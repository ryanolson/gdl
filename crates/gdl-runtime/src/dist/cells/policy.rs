//! Cell policies: how a chooser picks, once per attempt, the cell of a
//! class it sends to, from that class's entry table. A placement names
//! one per entered class (`[cells.<class>] policy`); the runtime provides
//! `home_first`.

use std::sync::Arc;

use gdl::{Error, ErrorKind, Result};
use gdl_cells::CellState;
use gdl_model::{CellPolicy, Layout};

use super::CellTable;

/// How a chooser picks a cell of a class it sends to.
pub(crate) trait CellChooser: Send + Sync + 'static {
    /// The index in `cells` of the cell to send this attempt to, or `None`
    /// when no cell may take it. `region` is the chooser's own; `excluded`
    /// holds the [`id`](CellTable::id)s of the cells that declined this
    /// request, never chosen again for it; `draw` is a random number drawn
    /// for this attempt.
    fn choose(
        &self,
        cells: &[Arc<CellTable>],
        region: &str,
        excluded: &[u64],
        draw: u64,
    ) -> Option<usize>;
}

/// The cell policy the placement names for `class`, refused, naming the
/// policy and the class, when the runtime does not provide it.
pub(crate) fn of(layout: &Layout, class: &str) -> Result<&'static dyn CellChooser> {
    let default;
    let policy = match layout.cell_policy(class) {
        Some(policy) => policy,
        None => {
            default = CellPolicy::default();
            &default
        }
    };
    match policy.policy.as_str() {
        "home_first" => Ok(&HomeFirst),
        name => Err(Error::new(
            ErrorKind::Placement,
            format!(
                "`[cells.{class}] policy = \"{name}\"`, but the runtime provides no cell policy of that name; it has `home_first`"
            ),
        )),
    }
}

/// `home_first`: a `ready` cell before a `degraded` one, and within a
/// state a cell of the chooser's own region before one elsewhere; among
/// the best, one at random. A cell is eligible only while it is `ready`
/// or `degraded`, has an entry instance to route to, and has not declined
/// the request.
///
/// Without a load summary every eligible cell looks alike, so the two
/// random choices of the design reduce to one: a deterministic pick would
/// send every chooser to the same cell.
pub(crate) struct HomeFirst;

impl HomeFirst {
    /// `cell`'s place in the order, lower first, or `None` when it is not
    /// eligible. A state outranks a region: a ready cell elsewhere comes
    /// before a degraded cell at home (ruling Q9).
    fn rank(cell: &CellTable, region: &str, excluded: &[u64]) -> Option<u8> {
        if cell.eligible.is_empty() || excluded.contains(&cell.id) {
            return None;
        }
        let state = match cell.state {
            CellState::Ready => 0,
            CellState::Degraded => 2,
            CellState::Draining | CellState::Unavailable => return None,
        };
        Some(state + u8::from(cell.region != region))
    }
}

impl CellChooser for HomeFirst {
    /// Two passes and no allocation: the best rank and how many cells
    /// hold it, then the drawn one of them.
    fn choose(
        &self,
        cells: &[Arc<CellTable>],
        region: &str,
        excluded: &[u64],
        draw: u64,
    ) -> Option<usize> {
        let mut best: Option<(u8, u64)> = None;
        for cell in cells {
            let Some(rank) = Self::rank(cell, region, excluded) else {
                continue;
            };
            best = match best {
                Some((at, count)) if at == rank => Some((at, count + 1)),
                Some((at, count)) if at < rank => Some((at, count)),
                _ => Some((rank, 1)),
            };
        }
        let (best, count) = best?;
        let mut nth = draw % count;
        cells.iter().position(|cell| {
            if Self::rank(cell, region, excluded) != Some(best) {
                return false;
            }
            if nth == 0 {
                return true;
            }
            nth -= 1;
            false
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use velo::InstanceId;

    use super::*;

    const HOME: &str = "us-east-1";
    const AWAY: &str = "us-west-2";

    fn cell(name: &str, region: &str, state: CellState, entries: usize) -> Arc<CellTable> {
        let eligible: Box<[InstanceId]> = (0..entries).map(|_| InstanceId::new_v4()).collect();
        Arc::new(CellTable {
            class: "model_x".into(),
            cell: name.into(),
            id: super::super::cell_id("model_x", name),
            generation: 1,
            region: region.into(),
            state,
            load: None,
            offered: eligible
                .iter()
                .map(|id| gdl::InstanceId::from_u128(id.as_u128()))
                .collect(),
            eligible,
            retired: Box::new([]),
            turn: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// The cells `home_first` chooses over many draws, by name.
    fn chosen(cells: &[Arc<CellTable>]) -> std::collections::BTreeSet<String> {
        (0..64)
            .map(|draw| {
                let at = HomeFirst.choose(cells, HOME, &[], draw).expect("a cell");
                cells[at].cell.clone()
            })
            .collect()
    }

    fn names(names: &[&str]) -> std::collections::BTreeSet<String> {
        names.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn its_own_region_first_within_a_state() {
        let cells = [
            cell("away", AWAY, CellState::Ready, 1),
            cell("home-1", HOME, CellState::Ready, 1),
            cell("home-2", HOME, CellState::Ready, 2),
        ];
        assert_eq!(chosen(&cells), names(&["home-1", "home-2"]));
        let cells = [
            cell("away", AWAY, CellState::Degraded, 1),
            cell("home", HOME, CellState::Degraded, 1),
        ];
        assert_eq!(chosen(&cells), names(&["home"]));
        // With no home cell, the cell elsewhere.
        let cells = [cell("away", AWAY, CellState::Ready, 1)];
        assert_eq!(chosen(&cells), names(&["away"]));
    }

    #[test]
    fn ready_before_degraded_whatever_the_region() {
        let cells = [
            cell("home", HOME, CellState::Degraded, 1),
            cell("away", AWAY, CellState::Ready, 1),
        ];
        assert_eq!(chosen(&cells), names(&["away"]));
        let cells = [
            cell("home", HOME, CellState::Degraded, 1),
            cell("away", AWAY, CellState::Degraded, 1),
        ];
        assert_eq!(chosen(&cells), names(&["home"]));
    }

    #[test]
    fn never_unavailable_or_draining_and_never_empty() {
        for state in [CellState::Unavailable, CellState::Draining] {
            let cells = [cell("home", HOME, state, 1)];
            assert_eq!(HomeFirst.choose(&cells, HOME, &[], 0), None, "{state:?}");
        }
        // A ready home cell with nothing to route to loses to a degraded
        // cell elsewhere that has.
        let cells = [
            cell("empty", HOME, CellState::Ready, 0),
            cell("away", AWAY, CellState::Degraded, 1),
        ];
        assert_eq!(chosen(&cells), names(&["away"]));
        let cells = [cell("empty", HOME, CellState::Ready, 0)];
        assert_eq!(HomeFirst.choose(&cells, HOME, &[], 3), None);
        assert_eq!(HomeFirst.choose(&[], HOME, &[], 3), None);
    }

    /// A cell that declined the request is never chosen for it again,
    /// whatever its rank; the next best is, and with every cell excluded,
    /// none.
    #[test]
    fn a_cell_that_declined_is_not_chosen_again() {
        let cells = [
            cell("home-1", HOME, CellState::Ready, 1),
            cell("home-2", HOME, CellState::Ready, 1),
            cell("away", AWAY, CellState::Degraded, 1),
        ];
        let excluded = |names: &[&str]| -> Vec<u64> {
            names
                .iter()
                .map(|name| super::super::cell_id("model_x", name))
                .collect()
        };
        let chosen_without = |names: &[&str]| -> std::collections::BTreeSet<String> {
            let excluded = excluded(names);
            (0..64)
                .filter_map(|draw| HomeFirst.choose(&cells, HOME, &excluded, draw))
                .map(|at| cells[at].cell.clone())
                .collect()
        };
        assert_eq!(chosen_without(&["home-1"]), names(&["home-2"]));
        assert_eq!(chosen_without(&["home-1", "home-2"]), names(&["away"]));
        assert!(chosen_without(&["home-1", "home-2", "away"]).is_empty());
        // The same cell in a table of another class is another cell.
        assert_ne!(
            super::super::cell_id("model_x", "home-1"),
            super::super::cell_id("model_y", "home-1")
        );
    }

    #[test]
    fn every_best_cell_is_drawn_alike() {
        let cells = [
            cell("a", HOME, CellState::Ready, 1),
            cell("other", AWAY, CellState::Ready, 1),
            cell("b", HOME, CellState::Ready, 1),
            cell("c", HOME, CellState::Ready, 1),
        ];
        let mut counts = [0usize; 4];
        for draw in 0..300 {
            counts[HomeFirst.choose(&cells, HOME, &[], draw).expect("a cell")] += 1;
        }
        assert_eq!(counts, [100, 0, 100, 100]);
    }

    #[test]
    fn a_placement_names_a_policy_the_runtime_has_or_is_refused() {
        let manifest = gdl_model::Manifest {
            name: "cells".into(),
            schema: 1,
            nodes: vec![
                gdl_model::NodeInfo {
                    name: "S".into(),
                    role: gdl_model::Role::Source,
                    binding: None,
                },
                gdl_model::NodeInfo {
                    name: "D".into(),
                    role: gdl_model::Role::Sink,
                    binding: Some("d".into()),
                },
            ],
            hops: vec![gdl_model::HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![gdl_model::TypeInfo::new("T", true)],
                backward_types: vec![gdl_model::TypeInfo::new("T", true)],
            }],
        };
        let placement = gdl_model::Placement::new()
            .segment("a", ["S"])
            .segment("d", ["D"])
            .class("global", ["a"])
            .class("model_x", ["d"]);
        let layout = gdl_model::layout(&manifest, &placement).expect("valid test fixture");
        assert!(of(&layout, "model_x").is_ok());
        let placement = placement.cell_policy(
            "model_x",
            CellPolicy {
                policy: "nearest".into(),
                ..CellPolicy::default()
            },
        );
        let layout = gdl_model::layout(&manifest, &placement).expect("valid test fixture");
        let Err(error) = of(&layout, "model_x") else {
            panic!("a policy the runtime does not have was taken");
        };
        let message = error.to_string();
        assert!(
            message.contains("nearest") && message.contains("model_x"),
            "{message}"
        );
    }
}
