use super::{Cell, CellChoice, CellPolicy, CellRequest, CellState, NoCell, cut};
use crate::select::Encoded;

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
/// `degraded` one: a cell that just declined does not fit, and
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
#[derive(Debug, Clone, Copy, Default)]
pub struct RoundRobin;

impl RoundRobin {
    /// `round_robin`'s choice among only the cells `keep` keeps, as if
    /// the others were not there: the index of the chosen cell in
    /// `cells`, or `None` when no kept cell is eligible. The turn goes
    /// round the kept cells, and the damping cap counts only them. A
    /// policy that filters the cells, by model for instance, takes turns
    /// over the rest with this, and allocates nothing. It reads no head,
    /// so it takes a request of any head.
    ///
    /// `keep` is called again for a cell within one choice, and must
    /// answer alike each time.
    ///
    /// Two passes over the table and no allocation while no cell's damping
    /// window lasts: one counts the choosable cells of each rank, the other
    /// finds the turn's. While a window lasts, a pass first sets which
    /// damped cells are passed over, as in `home_first`. The damping is one
    /// moment's view, so the passes agree whatever other requests decline
    /// or accept meanwhile.
    pub fn choose_where<H>(
        &self,
        request: CellRequest<'_, H>,
        cells: &[Cell],
        keep: impl Fn(&Cell) -> bool,
    ) -> Option<usize> {
        let (excluded, damped, now) = (request.excluded(), request.damped(), request.now());
        let lasts = damped.lasts(now);
        let cut = if lasts {
            let eligible = |cell: &Cell| Self::rank(cell, excluded).is_some() && keep(cell);
            cut(cells, eligible, damped, now)
        } else {
            0
        };
        let rank = |cell: &Cell| {
            Self::rank(cell, excluded)
                .filter(|_| keep(cell) && (!lasts || damped.left(cell.id, now) <= cut))
        };
        let mut counts = [0u64; 2];
        for cell in cells {
            if let Some(rank) = rank(cell) {
                counts[rank] += 1;
            }
        }
        let best = usize::from(counts[0] == 0);
        let mut nth = request.turn().checked_rem(counts[best])?;
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

    /// `cell`'s rank, `ready` before `degraded`, or `None` when it may not
    /// be chosen for this request.
    fn rank(cell: &Cell, excluded: &[u64]) -> Option<usize> {
        if cell.entries == 0 || excluded.contains(&cell.id) {
            return None;
        }
        match cell.state {
            CellState::Ready => Some(0),
            CellState::Degraded => Some(1),
            CellState::Draining | CellState::Unavailable => None,
        }
    }
}

impl CellPolicy for RoundRobin {
    /// It reads no request data.
    type Head = Encoded;

    /// [`choose_where`](RoundRobin::choose_where), keeping every cell, with
    /// no lease.
    fn choose(
        &self,
        request: CellRequest<'_, Encoded>,
        cells: &[Cell],
    ) -> Result<CellChoice, NoCell> {
        self.choose_where(request, cells, |_| true)
            .map(CellChoice::new)
            .ok_or_else(NoCell::new)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::super::{Damped, cell_id};
    use super::*;
    use crate::cx::Cx;

    const SECOND: Duration = Duration::from_secs(1);

    /// The cell `name`, in `state`, with `entries` entry instances and no
    /// load summary: round robin reads none.
    fn cell(name: &str, state: CellState, entries: usize) -> Cell {
        Cell::new("model_x", name, "anywhere", state).with_entries(entries)
    }

    fn ready(name: &str) -> Cell {
        cell(name, CellState::Ready, 1)
    }

    /// The names of the cells chosen for the turns `turns`, `-` for none.
    fn turns(
        cells: &[Cell],
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
        turns: std::ops::Range<u64>,
    ) -> Vec<String> {
        let cx = Cx::new();
        turns
            .map(|turn| {
                let request = CellRequest::new(&cx, &Encoded, "here", now)
                    .with_excluded(excluded)
                    .with_damped(damped)
                    .with_turn(turn);
                RoundRobin
                    .choose(request, cells)
                    .ok()
                    .map(|choice| choice.at())
                    .map_or_else(|| "-".to_string(), |at| cells[at].name.clone())
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
            assert!(!RoundRobin.eligible(&cells[at], "here", now), "{at}");
        }
        assert!(RoundRobin.eligible(&cells[0], "here", now));
        assert!(RoundRobin.eligible(&cell("d", CellState::Degraded, 1), "here", now));
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

    /// `choose_where` is `choose` over a copy of the cells it keeps, turn
    /// for turn, with its index into all of them: for every subset of
    /// cells of mixed state, with one damped and one excluded.
    #[test]
    fn choose_where_is_choose_over_the_kept_cells() {
        let cells = [
            ready("m1"),
            cell("d1", CellState::Degraded, 1),
            ready("m2"),
            cell("u1", CellState::Unavailable, 1),
            ready("m3"),
            cell("d2", CellState::Degraded, 1),
        ];
        let now = Instant::now();
        let damped = Damped::new([(cell_id("model_x", "m2"), now + SECOND)]);
        let cx = Cx::new();
        for excluded in [&[][..], &[cell_id("model_x", "m1")][..]] {
            for mask in 0u32..1 << cells.len() {
                let kept = |cell: &Cell| {
                    let at = cells.iter().position(|of| of.name == cell.name);
                    at.is_some_and(|at| mask & 1 << at != 0)
                };
                let copy: Vec<Cell> = cells.iter().filter(|cell| kept(cell)).cloned().collect();
                for turn in 0..16 {
                    let request = CellRequest::new(&cx, &Encoded, "here", now)
                        .with_excluded(excluded)
                        .with_damped(&damped)
                        .with_turn(turn);
                    let whole = RoundRobin
                        .choose_where(request, &cells, kept)
                        .map(|at| cells[at].name.clone());
                    let alone = RoundRobin
                        .choose(request, &copy)
                        .ok()
                        .map(|choice| choice.at())
                        .map(|at| copy[at].name.clone());
                    assert_eq!(whole, alone, "mask {mask:06b}, turn {turn}");
                }
            }
        }
    }

    /// The turns go round the kept cells only, and the damping cap counts
    /// only them: a lone kept cell is not damped out by cells the filter
    /// drops, and its index is into all the cells.
    #[test]
    fn choose_where_takes_turns_among_the_kept_cells_only() {
        let cells = [ready("b"), ready("c"), ready("a"), ready("e")];
        let now = Instant::now();
        let damped = Damped::new([(cell_id("model_x", "a"), now + SECOND)]);
        let cx = Cx::new();
        let chosen = |keep: &dyn Fn(&Cell) -> bool, turns: std::ops::Range<u64>| -> Vec<usize> {
            turns
                .filter_map(|turn| {
                    let request = CellRequest::new(&cx, &(), "here", now)
                        .with_damped(&damped)
                        .with_turn(turn);
                    RoundRobin.choose_where(request, &cells, keep)
                })
                .collect()
        };
        assert_eq!(chosen(&|cell| cell.name == "a", 0..3), [2, 2, 2]);
        assert_eq!(
            chosen(&|cell| cell.name != "c", 0..4),
            [0, 3, 0, 3],
            "the damped cell is passed over among three"
        );
        assert!(chosen(&|_| false, 0..2).is_empty());
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
        let damped = Damped::new([(cells[1].id(), now + SECOND)]);
        assert_eq!(
            turns(&cells, &[], &damped, now, 0..4),
            names(&["m1", "m3", "m1", "m3"])
        );
        assert_eq!(
            turns(&cells, &[], &damped, now + SECOND * 2, 0..3),
            names(&["m1", "m2", "m3"]),
            "a passed window"
        );
        // Two of three damped: one may be passed over, the one whose
        // window lasts longest.
        let damped = Damped::new([
            (cells[1].id(), now + SECOND),
            (cells[0].id(), now + SECOND * 3 / 2),
        ]);
        assert_eq!(
            turns(&cells, &[], &damped, now + SECOND / 2, 0..4),
            names(&["m2", "m3", "m2", "m3"])
        );
        // A lone damped cell is still chosen.
        let lone = [ready("m2")];
        assert_eq!(turns(&lone, &[], &damped, now, 0..2), names(&["m2", "m2"]));
        // A damped ready cell passed over leaves a degraded one its turns,
        // as it does in `home_first`, which caps over every eligible cell.
        let mixed = [ready("m2"), cell("d1", CellState::Degraded, 1)];
        assert_eq!(turns(&mixed, &[], &damped, now, 0..2), names(&["d1", "d1"]));
    }
}
