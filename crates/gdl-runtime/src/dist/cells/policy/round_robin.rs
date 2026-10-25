//! `round_robin`: each attempt takes the next eligible cell in table
//! order, by the crossing's count of attempts.

use std::sync::Arc;

use gdl_cells::CellState;
use tokio::time::Instant;

use super::super::CellTable;
use super::super::damping::Damped;
use super::CellChooser;

/// `round_robin`: the cells the chooser follows take turns, whatever their
/// load and region.
///
/// A cell is eligible only while it is `ready` or `degraded`, has an entry
/// instance to route to (a cell that is not offered, or unread, has none),
/// and has not declined the request. Damping comes first, then the rank:
///
/// 1. A cell the chooser damps is passed over as `home_first` passes it
///    over: at most half of the eligible cells, `ready` and `degraded`
///    together, rounded down, are passed over at once, those whose windows
///    end soonest being chosen as if they were not, so a lone cell is
///    never damped out.
/// 2. Of the cells left, the `ready` ones take every turn while any is
///    left, and the `degraded` ones only when none is.
///
/// So a damped `ready` cell that is passed over leaves its turns to a
/// `degraded` one: a cell that just declined does not fit (ruling Q9), and
/// damping stays as it is for every policy.
///
/// The attempt's turn is the crossing's count of the attempts before it:
/// the `turn`-th choosable cell, modulo how many there are, in the order
/// of the table, which is by class and cell. So the turns of one chooser's
/// hand-off go round its cells one by one, and a turn needs no lock and no
/// shared position but that count. "The next cell" is the next turn among
/// the cells still eligible, not the cell after the last one chosen: every
/// attempt takes a turn, a failover after a decline included, and a cell
/// the request excluded no longer counts. With cells A, B, and C, if A
/// declines the request at turn 0, its failover's turn 1 among B and C
/// goes to C. A cell that leaves or joins shifts which cell the next turn
/// lands on, which a rotation does not need to avoid.
pub(crate) struct RoundRobin;

impl RoundRobin {
    /// `cell`'s rank, `ready` before `degraded`, or `None` when it may not
    /// be chosen for this request.
    fn rank(cell: &CellTable, excluded: &[u64]) -> Option<usize> {
        if cell.eligible.is_empty() || excluded.contains(&cell.id) {
            return None;
        }
        match cell.state {
            CellState::Ready => Some(0),
            CellState::Degraded => Some(1),
            CellState::Draining | CellState::Unavailable => None,
        }
    }
}

impl CellChooser for RoundRobin {
    /// Two passes over the table and no allocation while no cell's damping
    /// window lasts: one counts the choosable cells of each rank, the other
    /// finds the turn's. While a window lasts, a pass first sets which
    /// damped cells are passed over, as in `home_first`. `damped` is one
    /// moment's view, so the passes agree whatever other requests decline
    /// or accept meanwhile.
    fn choose(
        &self,
        cells: &[Arc<CellTable>],
        _region: &str,
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
        turn: u64,
    ) -> Option<usize> {
        let lasts = damped.lasts(now);
        let cut = if lasts {
            super::cut(
                cells,
                |cell| Self::rank(cell, excluded).is_some(),
                damped,
                now,
            )
        } else {
            0
        };
        let rank = |cell: &CellTable| {
            Self::rank(cell, excluded).filter(|_| !lasts || damped.left(cell.id, now) <= cut)
        };
        let mut counts = [0u64; 2];
        for cell in cells {
            if let Some(rank) = rank(cell) {
                counts[rank] += 1;
            }
        }
        let best = usize::from(counts[0] == 0);
        let mut nth = turn.checked_rem(counts[best])?;
        for (at, cell) in cells.iter().enumerate() {
            if rank(cell) != Some(best) {
                continue;
            }
            if nth == 0 {
                return Some(at);
            }
            nth -= 1;
        }
        None
    }

    /// Whether `cell` is `ready` or `degraded`, has an entry instance, and
    /// has not declined the request: a damped cell, or a degraded one
    /// beside a ready one, is still eligible, since the rank and damping
    /// steer the next choices, not one already made.
    fn eligible(&self, cell: &CellTable, _region: &str, excluded: &[u64], _now: Instant) -> bool {
        Self::rank(cell, excluded).is_some()
    }

    fn random(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use velo::InstanceId;

    use super::super::super::cell_id;
    use super::super::super::damping::Damping;
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    /// A table of the cell `name`, in `state`, with `entries` eligible
    /// instances and no load summary: round robin reads none.
    fn cell(name: &str, state: CellState, entries: usize) -> Arc<CellTable> {
        let eligible: Box<[InstanceId]> = (0..entries).map(|_| InstanceId::new_v4()).collect();
        Arc::new(CellTable {
            class: "model_x".into(),
            cell: name.into(),
            id: cell_id("model_x", name),
            generation: 1,
            region: "anywhere".into(),
            state,
            load: None,
            offered: eligible
                .iter()
                .copied()
                .map(crate::dist::roster::selector_id)
                .collect(),
            eligible,
            retired: Box::new([]),
            unread: false,
            turn: Arc::new(AtomicUsize::new(0)),
            offering: true,
            revision: 1,
            leases: Arc::default(),
        })
    }

    fn ready(name: &str) -> Arc<CellTable> {
        cell(name, CellState::Ready, 1)
    }

    /// The names of the cells chosen for the turns `turns`, `-` for none.
    fn turns(
        cells: &[Arc<CellTable>],
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
        turns: std::ops::Range<u64>,
    ) -> Vec<String> {
        turns
            .map(|turn| {
                RoundRobin
                    .choose(cells, "here", excluded, damped, now, turn)
                    .map_or_else(|| "-".to_string(), |at| cells[at].cell.clone())
            })
            .collect()
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    /// Turn after turn, the next cell in table order, round and round,
    /// with no load summary and whatever the region.
    #[test]
    fn each_turn_takes_the_next_cell() {
        let cells = [ready("m1"), ready("m2"), ready("m3")];
        let now = Instant::now();
        assert_eq!(
            turns(&cells, &[], &Damped::default(), now, 0..7),
            names(&["m1", "m2", "m3", "m1", "m2", "m3", "m1"])
        );
        // A turn far along the count is still a turn: `u64::MAX` is a
        // multiple of 3.
        assert_eq!(
            turns(&cells, &[], &Damped::default(), now, u64::MAX - 3..u64::MAX),
            names(&["m1", "m2", "m3"])
        );
    }

    /// A cell that may not be chosen takes no turn: `unavailable`,
    /// `draining`, with nothing to route to (not offered, unread, or no
    /// entry instance), or excluded for the request.
    #[test]
    fn only_eligible_cells_take_turns() {
        let cells = [
            ready("m1"),
            cell("m2", CellState::Unavailable, 1),
            cell("m3", CellState::Draining, 1),
            cell("m4", CellState::Ready, 0),
            ready("m5"),
            ready("m6"),
        ];
        let now = Instant::now();
        let none = Damped::default();
        assert_eq!(
            turns(&cells, &[], &none, now, 0..6),
            names(&["m1", "m5", "m6", "m1", "m5", "m6"])
        );
        let excluded = [cell_id("model_x", "m5")];
        assert_eq!(
            turns(&cells, &excluded, &none, now, 0..4),
            names(&["m1", "m6", "m1", "m6"])
        );
        for at in [1, 2, 3] {
            assert!(!RoundRobin.eligible(&cells[at], "here", &[], now), "{at}");
        }
        assert!(RoundRobin.eligible(&cells[0], "here", &[], now));
        assert!(!RoundRobin.eligible(&cells[4], "here", &excluded, now));
        assert!(RoundRobin.eligible(&cell("d", CellState::Degraded, 1), "here", &[], now));
        let excluded = [
            cell_id("model_x", "m1"),
            cell_id("model_x", "m6"),
            cell_id("model_x", "m5"),
        ];
        assert_eq!(
            turns(&cells, &excluded, &none, now, 0..2),
            names(&["-", "-"])
        );
        assert_eq!(turns(&[], &[], &none, now, 0..1), names(&["-"]));
    }

    /// A `degraded` cell takes a turn only while no cell is `ready`.
    #[test]
    fn a_degraded_cell_only_while_no_cell_is_ready() {
        let now = Instant::now();
        let none = Damped::default();
        let mixed = [
            cell("d1", CellState::Degraded, 1),
            ready("m1"),
            cell("d2", CellState::Degraded, 1),
            ready("m2"),
        ];
        assert_eq!(
            turns(&mixed, &[], &none, now, 0..4),
            names(&["m1", "m2", "m1", "m2"])
        );
        let excluded = [cell_id("model_x", "m1"), cell_id("model_x", "m2")];
        assert_eq!(
            turns(&mixed, &excluded, &none, now, 0..4),
            names(&["d1", "d2", "d1", "d2"])
        );
        let degraded = [
            cell("d1", CellState::Degraded, 1),
            cell("m1", CellState::Ready, 0),
            cell("u1", CellState::Unavailable, 1),
        ];
        assert_eq!(
            turns(&degraded, &[], &none, now, 0..2),
            names(&["d1", "d1"])
        );
    }

    /// A damped cell takes no turn while its window lasts, but at most half
    /// of the eligible cells are passed over, so a lone cell, or one of
    /// two, is never damped out; the one whose window ends soonest is
    /// chosen as if it were not damped. A window that has passed passes
    /// nothing over.
    #[test]
    fn a_damped_cell_takes_no_turn_while_others_can() {
        let cells = [ready("m1"), ready("m2"), ready("m3")];
        let now = Instant::now();
        let damping = Damping::default();
        damping.decline(cells[1].id, now);
        assert_eq!(
            turns(&cells, &[], &damping.view(), now, 0..4),
            names(&["m1", "m3", "m1", "m3"])
        );
        assert_eq!(
            turns(&cells, &[], &damping.view(), now + SECOND * 2, 0..3),
            names(&["m1", "m2", "m3"]),
            "a passed window"
        );
        // Two of three damped: one may be passed over, the one whose
        // window lasts longest.
        damping.decline(cells[0].id, now + SECOND / 2);
        assert_eq!(
            turns(&cells, &[], &damping.view(), now + SECOND / 2, 0..4),
            names(&["m2", "m3", "m2", "m3"])
        );
        // A lone damped cell is still chosen.
        let lone = [ready("m2")];
        assert_eq!(
            turns(&lone, &[], &damping.view(), now, 0..2),
            names(&["m2", "m2"])
        );
        // A damped ready cell passed over leaves a degraded one its turns,
        // as it does in `home_first`, which caps over every eligible cell.
        let mixed = [ready("m2"), cell("d1", CellState::Degraded, 1)];
        assert_eq!(
            turns(&mixed, &[], &damping.view(), now, 0..2),
            names(&["d1", "d1"])
        );
    }
}
