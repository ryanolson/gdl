//! Cell policies: how a chooser picks, once per attempt, the cell of a
//! class it sends to, from that class's entry table. A placement names
//! one per entered class (`[cells.<class>] policy`); the runtime provides
//! `home_first`.

use std::sync::Arc;
use std::time::Duration;

use gdl::{Error, ErrorKind, Result};
use gdl_cells::{CellState, LoadSummary};
use gdl_model::{CellPolicy, Layout};
use tokio::time::Instant;

use super::CellTable;

/// How a chooser picks a cell of a class it sends to.
pub(crate) trait CellChooser: Send + Sync + 'static {
    /// The index in `cells` of the cell to send this attempt to, or `None`
    /// when no cell may take it. `region` is the chooser's own; `excluded`
    /// holds the [`id`](CellTable::id)s of the cells that declined this
    /// request, never chosen again for it; `now` is the chooser's clock,
    /// on which each cell's summary arrived; `draw` is a random number
    /// drawn for this attempt.
    fn choose(
        &self,
        cells: &[Arc<CellTable>],
        region: &str,
        excluded: &[u64],
        now: Instant,
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

/// `home_first`: by state, region, and admission, from each cell's load
/// summary.
///
/// A cell is eligible only while it is `ready` or `degraded`, has an
/// entry instance to route to, has not declined the request, and has a
/// summary no older than [`FRESH_PERIODS`] of its periods: a cell whose
/// load is not known is not sent to, since a summary steers only while it
/// is recent and a cell's refusal is the fast correction. Among the
/// eligible, the first non-empty tier:
///
/// 1. home cells (the chooser's region) that are `ready` and not
///    saturated;
/// 2. cells elsewhere that are `ready`, not saturated, and under the
///    spill bound;
/// 3. home cells that are `ready` and saturated;
/// 4. to 6. the `degraded` cells, in the same three tiers.
///
/// So a state outranks a region (ruling Q9), home cells take the work
/// until every one is saturated, and another region's cell takes it only
/// while it has room; a cell elsewhere at or over the spill bound is never
/// chosen. Within the tier, two cells drawn at random, and the one with
/// the lower ratio of admitted roots to admission limit: a deterministic
/// least-loaded pick would send every chooser to one cell between two
/// summaries. Admission counts roots, not engine work, so this is an
/// admission heuristic.
pub(crate) struct HomeFirst;

/// A cell is saturated once its admitted roots reach this share of its
/// admission limit, in percent, or its last summary period had refusals.
pub(crate) const SATURATED_PERCENT: u64 = 100;

/// A cell in another region is a candidate only while its admitted roots
/// are under this share of its admission limit, in percent: a cell near
/// its limit, behind a round trip between regions, is where a request
/// would most likely be declined.
pub(crate) const SPILL_PERCENT: u64 = 80;

/// A summary older than this many of its cell's summary periods is stale,
/// on the chooser's clock.
pub(crate) const FRESH_PERIODS: u32 = 3;

impl HomeFirst {
    /// `cell`'s tier, lower first, with its summary, or `None` when it is
    /// not eligible.
    fn tier<'c>(
        cell: &'c CellTable,
        region: &str,
        excluded: &[u64],
        now: Instant,
    ) -> Option<(u8, &'c LoadSummary)> {
        if cell.eligible.is_empty() || excluded.contains(&cell.id) {
            return None;
        }
        let state = match cell.state {
            CellState::Ready => 0,
            CellState::Degraded => 3,
            CellState::Draining | CellState::Unavailable => return None,
        };
        let (load, arrived) = cell.load.as_ref()?;
        let fresh = Duration::from_millis(load.period_ms).saturating_mul(FRESH_PERIODS);
        if now.saturating_duration_since(*arrived) > fresh || load.period_ms == 0 {
            return None;
        }
        let share = |percent: u64| u128::from(load.limit) * u128::from(percent);
        let admitted = u128::from(load.admitted) * 100;
        let saturated = load.refusals > 0 || admitted >= share(SATURATED_PERCENT);
        let tier = match (cell.region == region, saturated) {
            (true, false) => 0,
            (false, false) if admitted < share(SPILL_PERCENT) => 1,
            (true, true) => 2,
            (false, _) => return None,
        };
        Some((state + tier, load))
    }

    /// Whether `a` has a lower ratio of admitted roots to limit than `b`;
    /// a limit of zero is the highest.
    fn lighter(a: &LoadSummary, b: &LoadSummary) -> bool {
        match (a.limit, b.limit) {
            (0, _) => false,
            (_, 0) => true,
            _ => {
                u128::from(a.admitted) * u128::from(b.limit)
                    < u128::from(b.admitted) * u128::from(a.limit)
            }
        }
    }
}

impl CellChooser for HomeFirst {
    /// Two passes and no allocation: the best tier and how many cells hold
    /// it, then the two drawn ones of them.
    fn choose(
        &self,
        cells: &[Arc<CellTable>],
        region: &str,
        excluded: &[u64],
        now: Instant,
        draw: u64,
    ) -> Option<usize> {
        let mut best: Option<(u8, u64)> = None;
        for cell in cells {
            let Some((tier, _)) = Self::tier(cell, region, excluded, now) else {
                continue;
            };
            best = match best {
                Some((at, count)) if at == tier => Some((at, count + 1)),
                Some((at, count)) if at < tier => Some((at, count)),
                _ => Some((tier, 1)),
            };
        }
        let (best, count) = best?;
        // Two distinct members of the tier from one draw: the first by the
        // draw's remainder, the second by its quotient among the others.
        let first = draw % count;
        let second = match count {
            1 => first,
            _ => (first + 1 + (draw / count) % (count - 1)) % count,
        };
        let mut chosen: [Option<(usize, &LoadSummary)>; 2] = [None, None];
        let mut nth = 0;
        for (at, cell) in cells.iter().enumerate() {
            let Some((tier, load)) = Self::tier(cell, region, excluded, now) else {
                continue;
            };
            if tier != best {
                continue;
            }
            if nth == first {
                chosen[0] = Some((at, load));
            }
            if nth == second {
                chosen[1] = Some((at, load));
            }
            nth += 1;
        }
        let [Some((first, a)), Some((second, b))] = chosen else {
            return None;
        };
        // A tie goes to the first choice, which is uniform over the tier.
        Some(if Self::lighter(b, a) { second } else { first })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::hash::{BuildHasher, RandomState};
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use gdl_cells::LoadSummary;
    use velo::InstanceId;

    use super::*;

    const HOME: &str = "us-east-1";
    const AWAY: &str = "us-west-2";
    /// The summary period of every cell here.
    const PERIOD: Duration = Duration::from_secs(10);
    /// Each cell's admission limit.
    const LIMIT: u64 = 100;

    /// What a cell's table holds of its load: its summary and when it
    /// arrived, or nothing.
    #[derive(Clone, Copy)]
    enum Load {
        /// `admitted` of [`LIMIT`] roots and `refusals` in the last
        /// period, arrived at the given time.
        At {
            admitted: u64,
            refusals: u64,
            arrived: Instant,
        },
        None,
    }

    fn load(admitted: u64, refusals: u64, arrived: Instant) -> Load {
        Load::At {
            admitted,
            refusals,
            arrived,
        }
    }

    fn cell_loaded(
        name: &str,
        region: &str,
        state: CellState,
        entries: usize,
        load: Load,
    ) -> Arc<CellTable> {
        let eligible: Box<[InstanceId]> = (0..entries).map(|_| InstanceId::new_v4()).collect();
        let load = match load {
            Load::At {
                admitted,
                refusals,
                arrived,
            } => Some((
                LoadSummary {
                    admitted,
                    limit: LIMIT,
                    instances: u32::try_from(entries).expect("a few"),
                    refusals,
                    period_ms: u64::try_from(PERIOD.as_millis()).expect("short"),
                },
                arrived,
            )),
            Load::None => None,
        };
        Arc::new(CellTable {
            class: "model_x".into(),
            cell: name.into(),
            id: super::super::cell_id("model_x", name),
            generation: 1,
            region: region.into(),
            state,
            load,
            offered: eligible
                .iter()
                .map(|id| gdl::InstanceId::from_u128(id.as_u128()))
                .collect(),
            eligible,
            retired: Box::new([]),
            turn: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// An idle cell whose summary arrived at `now`.
    fn cell(
        name: &str,
        region: &str,
        state: CellState,
        entries: usize,
        now: Instant,
    ) -> Arc<CellTable> {
        cell_loaded(name, region, state, entries, load(0, 0, now))
    }

    /// A cell with `admitted` of [`LIMIT`] roots and `refusals`, whose
    /// summary arrived at `now`.
    fn busy(
        name: &str,
        region: &str,
        state: CellState,
        admitted: u64,
        refusals: u64,
        now: Instant,
    ) -> Arc<CellTable> {
        cell_loaded(name, region, state, 1, load(admitted, refusals, now))
    }

    /// Draws as a chooser makes them: a counter, hashed.
    fn draws(n: u64) -> impl Iterator<Item = u64> {
        let random = RandomState::new();
        (0..n).map(move |at| random.hash_one(at))
    }

    /// The cells `home_first` chooses at `now` over many draws, by name.
    fn chosen_at(cells: &[Arc<CellTable>], now: Instant) -> BTreeSet<String> {
        draws(256)
            .filter_map(|draw| HomeFirst.choose(cells, HOME, &[], now, draw))
            .map(|at| cells[at].cell.clone())
            .collect()
    }

    fn names(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn its_own_region_first_within_a_state() {
        let now = Instant::now();
        let cells = [
            cell("away", AWAY, CellState::Ready, 1, now),
            cell("home-1", HOME, CellState::Ready, 1, now),
            cell("home-2", HOME, CellState::Ready, 2, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["home-1", "home-2"]));
        let cells = [
            cell("away", AWAY, CellState::Degraded, 1, now),
            cell("home", HOME, CellState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["home"]));
        // With no home cell, the cell elsewhere, while it has room.
        let cells = [cell("away", AWAY, CellState::Ready, 1, now)];
        assert_eq!(chosen_at(&cells, now), names(&["away"]));
    }

    #[test]
    fn ready_before_degraded_whatever_the_region_or_load() {
        let now = Instant::now();
        let cells = [
            cell("home", HOME, CellState::Degraded, 1, now),
            cell("away", AWAY, CellState::Ready, 1, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["away"]));
        let cells = [
            cell("home", HOME, CellState::Degraded, 1, now),
            cell("away", AWAY, CellState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["home"]));
        // A saturated ready home cell before an idle degraded one.
        let cells = [
            cell("idle", HOME, CellState::Degraded, 1, now),
            busy("full", HOME, CellState::Ready, LIMIT, 0, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["full"]));
    }

    #[test]
    fn never_unavailable_or_draining_and_never_empty() {
        let now = Instant::now();
        for state in [CellState::Unavailable, CellState::Draining] {
            let cells = [cell("home", HOME, state, 1, now)];
            assert_eq!(
                HomeFirst.choose(&cells, HOME, &[], now, 0),
                None,
                "{state:?}"
            );
        }
        // A ready home cell with nothing to route to loses to a degraded
        // cell elsewhere that has.
        let cells = [
            cell("empty", HOME, CellState::Ready, 0, now),
            cell("away", AWAY, CellState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["away"]));
        let cells = [cell("empty", HOME, CellState::Ready, 0, now)];
        assert_eq!(HomeFirst.choose(&cells, HOME, &[], now, 3), None);
        assert_eq!(HomeFirst.choose(&[], HOME, &[], now, 3), None);
    }

    /// A cell with no summary, or one older than three of its periods, is
    /// not chosen, whatever its state and region: with no other cell,
    /// none is. A summary exactly three periods old is still fresh.
    #[test]
    fn a_cell_without_a_fresh_summary_is_not_chosen() {
        let now = Instant::now();
        let stale = now - PERIOD * 3 - Duration::from_millis(1);
        let cells = [
            cell_loaded("none", HOME, CellState::Ready, 1, Load::None),
            cell_loaded("stale", HOME, CellState::Ready, 1, load(0, 0, stale)),
            cell("away", AWAY, CellState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["away"]));
        assert_eq!(chosen_at(&cells[..2], now), names(&[]));
        let cells = [cell_loaded(
            "old",
            HOME,
            CellState::Ready,
            1,
            load(0, 0, now - PERIOD * 3),
        )];
        assert_eq!(chosen_at(&cells, now), names(&["old"]));
        // A summary that says nothing of its period is never fresh.
        let mut table =
            Arc::try_unwrap(cell("unknown", HOME, CellState::Ready, 1, now)).expect("one owner");
        if let Some((summary, _)) = &mut table.load {
            summary.period_ms = 0;
        }
        assert_eq!(chosen_at(&[Arc::new(table)], now), names(&[]));
    }

    /// Home cells are used until every one is saturated (its admitted
    /// roots at its limit, or refusals in its last period); a cell
    /// elsewhere is a candidate only while its admission is under 80% of
    /// its limit, and then before a saturated home cell.
    #[test]
    fn home_until_saturated_then_elsewhere_with_room() {
        let now = Instant::now();
        let room = LIMIT * 8 / 10 - 1;
        let bound = LIMIT * 8 / 10;
        // An unsaturated home cell, however busy, before any cell elsewhere.
        let cells = [
            busy("home", HOME, CellState::Ready, LIMIT - 1, 0, now),
            busy("away", AWAY, CellState::Ready, 0, 0, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["home"]));
        // Saturated at home, by admission or by refusals: elsewhere, if it
        // has room.
        for (admitted, refusals) in [(LIMIT, 0), (LIMIT + 5, 0), (1, 1)] {
            let cells = [
                busy("home", HOME, CellState::Ready, admitted, refusals, now),
                busy("away", AWAY, CellState::Ready, room, 0, now),
                busy("full-away", AWAY, CellState::Ready, bound, 0, now),
            ];
            assert_eq!(
                chosen_at(&cells, now),
                names(&["away"]),
                "{admitted} {refusals}"
            );
        }
        // Elsewhere at or over the spill bound, or refusing: the saturated
        // home cell.
        for (admitted, refusals) in [(bound, 0), (LIMIT, 0), (0, 1)] {
            let cells = [
                busy("home", HOME, CellState::Ready, LIMIT, 0, now),
                busy("away", AWAY, CellState::Ready, admitted, refusals, now),
            ];
            assert_eq!(
                chosen_at(&cells, now),
                names(&["home"]),
                "{admitted} {refusals}"
            );
            // With no home cell, nothing.
            assert_eq!(
                chosen_at(&cells[1..], now),
                names(&[]),
                "{admitted} {refusals}"
            );
        }
        // The degraded cells by the same order, after every ready one.
        let cells = [
            busy("home", HOME, CellState::Degraded, LIMIT, 0, now),
            busy("away", AWAY, CellState::Degraded, room, 0, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["away"]));
        let cells = [
            busy("home", HOME, CellState::Degraded, LIMIT, 0, now),
            busy("away", AWAY, CellState::Degraded, bound, 0, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["home"]));
        let cells = [
            busy("home", HOME, CellState::Degraded, 0, 0, now),
            busy("full", HOME, CellState::Ready, LIMIT, 1, now),
            busy("away", AWAY, CellState::Degraded, 0, 0, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["full"]));
    }

    /// Within a tier, two random choices: of two cells, the one with the
    /// lower ratio of admitted roots to limit, always; of three, the
    /// lightest more often than not but not always, the middle one
    /// sometimes, and the heaviest never, since it loses to either other.
    #[test]
    fn two_random_choices_by_admission() {
        let now = Instant::now();
        const N: u64 = 3000;
        let count = |cells: &[Arc<CellTable>]| -> Vec<u64> {
            let mut counts = vec![0; cells.len()];
            for draw in draws(N) {
                counts[HomeFirst
                    .choose(cells, HOME, &[], now, draw)
                    .expect("a cell")] += 1;
            }
            counts
        };
        let two = [
            busy("heavy", HOME, CellState::Ready, 70, 0, now),
            busy("light", HOME, CellState::Ready, 10, 0, now),
        ];
        assert_eq!(count(&two), [0, N]);
        let three = [
            busy("heavy", HOME, CellState::Ready, 70, 0, now),
            busy("light", HOME, CellState::Ready, 10, 0, now),
            busy("middle", HOME, CellState::Ready, 40, 0, now),
        ];
        let counts = count(&three);
        assert_eq!(counts[0], 0, "{counts:?}");
        assert!(counts[1] > N / 2 && counts[1] < N, "{counts:?}");
        assert!(counts[2] > 0, "{counts:?}");
        // Elsewhere, in its own tier, alike.
        let away = [
            busy("home", HOME, CellState::Ready, LIMIT, 0, now),
            busy("heavy", AWAY, CellState::Ready, 70, 0, now),
            busy("light", AWAY, CellState::Ready, 10, 0, now),
        ];
        assert_eq!(count(&away), [0, 0, N]);
    }

    /// A cell that declined the request is never chosen for it again,
    /// whatever its rank; the next best is, and with every cell excluded,
    /// none.
    #[test]
    fn a_cell_that_declined_is_not_chosen_again() {
        let now = Instant::now();
        let cells = [
            cell("home-1", HOME, CellState::Ready, 1, now),
            cell("home-2", HOME, CellState::Ready, 1, now),
            cell("away", AWAY, CellState::Degraded, 1, now),
        ];
        let excluded = |names: &[&str]| -> Vec<u64> {
            names
                .iter()
                .map(|name| super::super::cell_id("model_x", name))
                .collect()
        };
        let chosen_without = |names: &[&str]| -> BTreeSet<String> {
            let excluded = excluded(names);
            draws(64)
                .filter_map(|draw| HomeFirst.choose(&cells, HOME, &excluded, now, draw))
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

    /// Cells of one tier and one load are drawn alike: a tie goes to the
    /// first of the two choices, which is uniform.
    #[test]
    fn every_best_cell_is_drawn_alike() {
        let now = Instant::now();
        let cells = [
            cell("a", HOME, CellState::Ready, 1, now),
            cell("other", AWAY, CellState::Ready, 1, now),
            cell("b", HOME, CellState::Ready, 1, now),
            cell("c", HOME, CellState::Ready, 1, now),
        ];
        let mut counts = [0usize; 4];
        for draw in 0..300 {
            counts[HomeFirst
                .choose(&cells, HOME, &[], now, draw)
                .expect("a cell")] += 1;
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
