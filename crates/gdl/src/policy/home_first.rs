use tokio::time::Instant;

use super::{Cell, CellChoice, CellPolicy, CellRequest, CellState, LoadSummary, NoCell, cut, mix};
use crate::select::Encoded;

/// `home_first`: by state, region, and admission, from each cell's load
/// summary.
///
/// A cell is eligible only while it is `ready` or `degraded`, has an
/// entry instance to route to, has not declined the request, and has a
/// summary no older than [`FRESH_PERIODS`](super::Load::FRESH_PERIODS) of
/// its periods: a cell whose load is not known is not sent to, since a
/// summary steers only while it is recent and a cell's refusal is the
/// fast correction. Among the eligible, the first non-empty tier:
///
/// 1. home cells (the chooser's region) that are `ready` and not
///    saturated;
/// 2. cells elsewhere that are `ready`, not saturated, and under the
///    spill bound;
/// 3. home cells that are `ready` and saturated;
/// 4. to 6. the `degraded` cells, in the same three tiers.
///
/// So a state outranks a region, home cells take the work
/// until every one is saturated, and another region's cell takes it only
/// while it has room; a cell elsewhere at or over the spill bound is never
/// chosen. Within the tier, two cells drawn at random, and the one with
/// the lower ratio of admitted roots to admission limit: a deterministic
/// least-loaded pick would send every chooser to one cell between two
/// summaries. Admission counts roots, not engine work, so this is an
/// admission heuristic.
///
/// A cell the chooser damps ([`Damped`](super::Damped)) is not chosen
/// while its window lasts, but at most half of the eligible cells, rounded
/// down, are damped at once: when more are, those whose windows end
/// soonest are chosen as if they were not. So one bad spell cannot leave a
/// request no cell, and a lone cell is never damped.
#[derive(Debug, Clone, Copy, Default)]
pub struct HomeFirst;

impl HomeFirst {
    /// A cell is saturated once its admitted roots reach this share of its
    /// admission limit, in percent, or its last summary period had
    /// refusals.
    pub const SATURATED_PERCENT: u64 = 100;

    /// A cell in another region is a candidate only while its admitted
    /// roots are under this share of its admission limit, in percent: a
    /// cell near its limit, behind a round trip between regions, is where
    /// a request would most likely be declined.
    pub const SPILL_PERCENT: u64 = 80;

    /// `cell`'s tier, lower first, with its summary, or `None` when it is
    /// not eligible.
    fn tier<'c>(
        cell: &'c Cell,
        region: &str,
        excluded: &[u64],
        now: Instant,
    ) -> Option<(u8, &'c LoadSummary)> {
        if cell.entries == 0 || excluded.contains(&cell.id) {
            return None;
        }
        let state = match cell.state {
            CellState::Ready => 0,
            CellState::Degraded => 3,
            CellState::Draining | CellState::Unavailable => return None,
        };
        let load = cell.load.as_ref()?;
        if !load.is_fresh(now) {
            return None;
        }
        let load = load.summary();
        let share = |percent: u64| u128::from(load.limit) * u128::from(percent);
        let admitted = u128::from(load.admitted) * 100;
        let saturated = load.refusals > 0 || admitted >= share(Self::SATURATED_PERCENT);
        let tier = match (cell.region == region, saturated) {
            (true, false) => 0,
            (false, false) if admitted < share(Self::SPILL_PERCENT) => 1,
            (true, true) => 2,
            (false, _) => return None,
        };
        Some((state + tier, load))
    }

    /// `home_first`'s choice among only the cells `keep` keeps, as if
    /// the others were not there: the index of the chosen cell in
    /// `cells`, or `None` when no kept cell is eligible. The tiers, the
    /// two cells drawn, and the damping cap count only the kept cells. A
    /// policy that filters the cells, by model for instance, runs
    /// `home_first` over the rest with this, and allocates nothing. It
    /// reads no head, so it takes a request of any head.
    ///
    /// `keep` is called again for a cell within one choice, and must
    /// answer alike each time. A policy that chooses by it should also
    /// check a cell as [`eligible`](CellPolicy::eligible) does here.
    ///
    /// One pass and no allocation while no cell's damping window lasts:
    /// the best tier of the eligible cells, with two of them drawn as it
    /// goes, by reservoir sampling. While a window lasts, a pass first
    /// counts the eligible and the damped cells, which sets which damped
    /// cells are passed over, each a search of the short list of damped
    /// cells. The damping is one moment's view, so the cap and the cells
    /// drawn from agree whatever other requests decline or accept
    /// meanwhile, and a choice finds a cell whenever one is eligible.
    pub fn choose_where<H>(
        &self,
        request: CellRequest<'_, H>,
        cells: &[Cell],
        keep: impl Fn(&Cell) -> bool,
    ) -> Option<usize> {
        let (region, excluded) = (request.region(), request.excluded());
        let (damped, now) = (request.damped(), request.now());
        let lasts = damped.lasts(now);
        let cut = if lasts {
            let eligible =
                |cell: &Cell| Self::tier(cell, region, excluded, now).is_some() && keep(cell);
            cut(cells, eligible, damped, now)
        } else {
            0
        };
        let draw = request.draw();
        let mut best: Option<u8> = None;
        let mut count = 0u64;
        let mut drawn: [Option<(usize, &LoadSummary)>; 2] = [None, None];
        for (at, cell) in cells.iter().enumerate() {
            let Some((tier, load)) = Self::tier(cell, region, excluded, now) else {
                continue;
            };
            if !keep(cell) || lasts && damped.left(cell.id, now) > cut {
                continue;
            }
            match best {
                Some(of) if of < tier => continue,
                Some(of) if of == tier => {}
                _ => {
                    best = Some(tier);
                    count = 0;
                    drawn = [None, None];
                }
            }
            count += 1;
            // Each cell of the tier is one of the two drawn with the same
            // chance, 2 in `count` once `count` passes two.
            let slot = match count {
                1 => Some(0),
                2 => Some(1),
                _ => usize::try_from(mix(draw, count) % count)
                    .ok()
                    .filter(|slot| *slot < 2),
            };
            if let Some(slot) = slot {
                drawn[slot] = Some((at, load));
            }
        }
        match drawn {
            [Some((first, a)), Some((second, b))] => Some(if Self::lighter(b, a) {
                second
            } else if Self::lighter(a, b) {
                first
            } else if mix(draw, 0) & 1 == 0 {
                // A tie goes to either alike, so that every cell of a
                // tier of one load is chosen as often.
                first
            } else {
                second
            }),
            [Some((one, _)), None] | [None, Some((one, _))] => Some(one),
            [None, None] => None,
        }
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

impl CellPolicy for HomeFirst {
    /// It reads no request data.
    type Head = Encoded;

    /// [`choose_where`](HomeFirst::choose_where), keeping every cell, with
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

    /// Whether `cell` is in a tier: a damped cell is still eligible, since
    /// damping steers the next choices, not one already made.
    fn eligible(&self, cell: &Cell, region: &str, now: Instant) -> bool {
        Self::tier(cell, region, &[], now).is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::LazyLock;
    use std::time::Duration;

    use super::super::{Damped, Load};
    use super::*;
    use crate::cx::Cx;

    const SECOND: Duration = Duration::from_secs(1);

    /// The time every test counts from, and no later than any time a test
    /// passes.
    fn origin() -> Instant {
        static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
        *ORIGIN
    }

    const HOME: &str = "us-east-1";
    const AWAY: &str = "us-west-2";
    /// The summary period of every cell here.
    const PERIOD: Duration = Duration::from_secs(10);
    /// Each cell's admission limit.
    const LIMIT: u64 = 100;

    /// What a cell holds of its load: its summary and when it arrived, or
    /// nothing.
    #[derive(Clone, Copy)]
    enum Loaded {
        /// `admitted` of [`LIMIT`] roots and `refusals` in the last
        /// period, arrived at the given time.
        At {
            admitted: u64,
            refusals: u64,
            arrived: Instant,
        },
        None,
    }

    fn load(admitted: u64, refusals: u64, arrived: Instant) -> Loaded {
        Loaded::At {
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
        load: Loaded,
    ) -> Cell {
        let cell = Cell::new("model_x", name, region, state).with_entries(entries);
        match load {
            Loaded::At {
                admitted,
                refusals,
                arrived,
            } => cell.with_load(Load::new(
                LoadSummary {
                    admitted,
                    limit: LIMIT,
                    instances: u32::try_from(entries).expect("a few"),
                    refusals,
                    period_ms: u64::try_from(PERIOD.as_millis()).expect("short"),
                },
                arrived,
            )),
            Loaded::None => cell,
        }
    }

    /// An idle cell whose summary arrived at `now`.
    fn cell(name: &str, region: &str, state: CellState, entries: usize, now: Instant) -> Cell {
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
    ) -> Cell {
        cell_loaded(name, region, state, 1, load(admitted, refusals, now))
    }

    /// `home_first`'s choice among `cells` at `now` and turn `turn`, with
    /// `excluded` and `damped`.
    fn choose(
        cells: &[Cell],
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
        turn: u64,
    ) -> Option<usize> {
        let cx = Cx::new();
        let request = CellRequest::new(&cx, &Encoded, HOME, now)
            .with_excluded(excluded)
            .with_damped(damped)
            .with_turn(turn);
        HomeFirst
            .choose(request, cells)
            .ok()
            .map(|choice| choice.at())
    }

    /// The cells `home_first` chooses at `now` over many turns, by name.
    fn chosen_at(cells: &[Cell], now: Instant) -> BTreeSet<String> {
        chosen_damped(cells, &Damped::default(), now)
    }

    /// [`chosen_at`], with the cells in `damped` damped.
    fn chosen_damped(cells: &[Cell], damped: &Damped, now: Instant) -> BTreeSet<String> {
        (0..256)
            .filter_map(|turn| choose(cells, &[], damped, now, turn))
            .map(|at| cells[at].name.clone())
            .collect()
    }

    /// The [`id`](Cell::id) of the cell `name` here.
    fn id(name: &str) -> u64 {
        super::super::cell_id("model_x", name)
    }

    fn names(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn its_own_region_first_within_a_state() {
        let now = origin();
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
        let now = origin();
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
        let now = origin();
        let none = Damped::default();
        for state in [CellState::Unavailable, CellState::Draining] {
            let cells = [cell("home", HOME, state, 1, now)];
            assert_eq!(choose(&cells, &[], &none, now, 0), None, "{state:?}");
        }
        // A ready home cell with nothing to route to loses to a degraded
        // cell elsewhere that has.
        let cells = [
            cell("empty", HOME, CellState::Ready, 0, now),
            cell("away", AWAY, CellState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&cells, now), names(&["away"]));
        let cells = [cell("empty", HOME, CellState::Ready, 0, now)];
        assert_eq!(choose(&cells, &[], &none, now, 3), None);
        assert_eq!(choose(&[], &[], &none, now, 3), None);
    }

    /// A cell with no summary, or one older than three of its periods, is
    /// not chosen, whatever its state and region: with no other cell,
    /// none is. A summary exactly three periods old is still fresh.
    #[test]
    fn a_cell_without_a_fresh_summary_is_not_chosen() {
        let now = origin();
        let stale = now - PERIOD * 3 - Duration::from_millis(1);
        let cells = [
            cell_loaded("none", HOME, CellState::Ready, 1, Loaded::None),
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
        let unknown = Cell::new("model_x", "unknown", HOME, CellState::Ready)
            .with_entries(1)
            .with_load(Load::new(
                LoadSummary {
                    limit: LIMIT,
                    period_ms: 0,
                    ..LoadSummary::default()
                },
                now,
            ));
        assert_eq!(chosen_at(&[unknown], now), names(&[]));
    }

    /// Home cells are used until every one is saturated (its admitted
    /// roots at its limit, or refusals in its last period); a cell
    /// elsewhere is a candidate only while its admission is under 80% of
    /// its limit, and then before a saturated home cell.
    #[test]
    fn home_until_saturated_then_elsewhere_with_room() {
        let now = origin();
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
        let now = origin();
        const N: u64 = 3000;
        let count = |cells: &[Cell]| -> Vec<u64> {
            let mut counts = vec![0; cells.len()];
            for turn in 0..N {
                counts[choose(cells, &[], &Damped::default(), now, turn).expect("a cell")] += 1;
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
        let now = origin();
        let cells = [
            cell("home-1", HOME, CellState::Ready, 1, now),
            cell("home-2", HOME, CellState::Ready, 1, now),
            cell("away", AWAY, CellState::Degraded, 1, now),
        ];
        let chosen_without = |names: &[&str]| -> BTreeSet<String> {
            let excluded: Vec<u64> = names.iter().map(|name| id(name)).collect();
            (0..64)
                .filter_map(|turn| choose(&cells, &excluded, &Damped::default(), now, turn))
                .map(|at| cells[at].name.clone())
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

    /// A damped cell is not chosen while its window lasts, and is once it
    /// has passed. A damped home cell sends the request elsewhere, while
    /// elsewhere has room.
    #[test]
    fn a_damped_cell_is_not_chosen_while_its_window_lasts() {
        let now = origin();
        let cells = [
            cell("a", HOME, CellState::Ready, 1, now),
            cell("b", HOME, CellState::Ready, 1, now),
            cell("c", HOME, CellState::Ready, 1, now),
        ];
        let damped = Damped::new([(id("a"), now + SECOND)]);
        let just = Duration::from_millis(1);
        assert_eq!(chosen_damped(&cells, &damped, now), names(&["b", "c"]));
        assert_eq!(
            chosen_damped(&cells, &damped, now + SECOND - just),
            names(&["b", "c"])
        );
        assert_eq!(
            chosen_damped(&cells, &damped, now + SECOND),
            names(&["a", "b", "c"])
        );
        let cells = [
            cell("home", HOME, CellState::Ready, 1, now),
            cell("away", AWAY, CellState::Ready, 1, now),
        ];
        let damped = Damped::new([(id("home"), now + SECOND)]);
        assert_eq!(chosen_damped(&cells, &damped, now), names(&["away"]));
    }

    /// At most half of the eligible cells, rounded down, are damped at
    /// once; the others whose windows end soonest are chosen as if they
    /// were not. Of three damped cells, the latest to end stays damped; of
    /// two, the later; a lone cell is never damped, and neither is the
    /// last cell a request has not excluded.
    #[test]
    fn at_most_half_of_the_eligible_cells_are_damped() {
        let now = origin() + SECOND;
        let cells = [
            cell("soonest", HOME, CellState::Ready, 1, now),
            cell("sooner", HOME, CellState::Ready, 1, now),
            cell("latest", HOME, CellState::Ready, 1, now),
        ];
        let damped = Damped::new([
            (id("soonest"), now + SECOND / 2),
            (id("sooner"), now + SECOND * 3 / 4),
            (id("latest"), now + SECOND),
        ]);
        assert_eq!(
            chosen_damped(&cells, &damped, now),
            names(&["soonest", "sooner"])
        );
        assert_eq!(
            chosen_damped(&cells[..2], &damped, now),
            names(&["soonest"])
        );
        assert_eq!(chosen_damped(&cells[1..], &damped, now), names(&["sooner"]));
        assert_eq!(chosen_damped(&cells[2..], &damped, now), names(&["latest"]));
        let without = |excluded: &[u64]| -> BTreeSet<String> {
            (0..64)
                .filter_map(|turn| choose(&cells, excluded, &damped, now, turn))
                .map(|at| cells[at].name.clone())
                .collect()
        };
        assert_eq!(without(&[id("soonest")]), names(&["sooner"]));
        assert_eq!(without(&[id("soonest"), id("sooner")]), names(&["latest"]));
        // A cell that is not eligible counts for nothing: with only one
        // eligible cell, it is chosen, damped or not.
        let cells = [
            cell("damped", HOME, CellState::Ready, 1, now),
            cell("draining", HOME, CellState::Draining, 1, now),
            cell_loaded("unknown", HOME, CellState::Ready, 1, Loaded::None),
        ];
        let damped = Damped::new([(id("damped"), now + SECOND)]);
        assert_eq!(chosen_damped(&cells, &damped, now), names(&["damped"]));
    }

    /// Cells of one tier and one load are drawn alike: of the two drawn,
    /// a tie goes to either.
    #[test]
    fn every_best_cell_is_drawn_alike() {
        let now = origin();
        let cells = [
            cell("a", HOME, CellState::Ready, 1, now),
            cell("other", AWAY, CellState::Ready, 1, now),
            cell("b", HOME, CellState::Ready, 1, now),
            cell("c", HOME, CellState::Ready, 1, now),
        ];
        let mut counts = [0u64; 4];
        for turn in 0..3000 {
            counts[choose(&cells, &[], &Damped::default(), now, turn).expect("a cell")] += 1;
        }
        assert_eq!(counts[1], 0, "{counts:?}");
        for at in [0, 2, 3] {
            assert!(counts[at].abs_diff(1000) < 150, "{counts:?}");
        }
    }

    /// `choose_where` is `choose` over a copy of the cells it keeps, turn
    /// for turn, with its index into all of them: for every subset of
    /// cells of mixed state, region, and load, with one damped and one
    /// excluded.
    #[test]
    fn choose_where_is_choose_over_the_kept_cells() {
        let now = origin();
        let cells = [
            cell("home-1", HOME, CellState::Ready, 1, now),
            busy("full", HOME, CellState::Ready, LIMIT, 0, now),
            busy("away", AWAY, CellState::Ready, 10, 0, now),
            busy("home-2", HOME, CellState::Ready, 40, 0, now),
            cell("degraded", HOME, CellState::Degraded, 1, now),
            busy("home-3", HOME, CellState::Ready, 70, 0, now),
        ];
        let damped = Damped::new([(id("home-2"), now + SECOND)]);
        let cx = Cx::new();
        for excluded in [&[][..], &[id("home-1")][..]] {
            for mask in 0u32..1 << cells.len() {
                let kept = |cell: &Cell| {
                    let at = cells.iter().position(|of| of.name == cell.name);
                    at.is_some_and(|at| mask & 1 << at != 0)
                };
                let copy: Vec<Cell> = cells.iter().filter(|cell| kept(cell)).cloned().collect();
                for turn in 0..32 {
                    let request = CellRequest::new(&cx, &Encoded, HOME, now)
                        .with_excluded(excluded)
                        .with_damped(&damped)
                        .with_turn(turn);
                    let whole = HomeFirst
                        .choose_where(request, &cells, kept)
                        .map(|at| cells[at].name.clone());
                    let alone = HomeFirst
                        .choose(request, &copy)
                        .ok()
                        .map(|choice| choice.at())
                        .map(|at| copy[at].name.clone());
                    assert_eq!(whole, alone, "mask {mask:06b}, turn {turn}");
                }
            }
        }
    }

    /// The damping cap counts only the kept cells: a lone kept cell is
    /// not damped out by cells the filter drops, and its index is into
    /// all the cells.
    #[test]
    fn choose_where_damps_among_the_kept_cells_only() {
        let now = origin();
        let cells = [
            cell("b", HOME, CellState::Ready, 1, now),
            cell("c", HOME, CellState::Ready, 1, now),
            cell("a", HOME, CellState::Ready, 1, now),
        ];
        let damped = Damped::new([(id("a"), now + SECOND)]);
        let cx = Cx::new();
        let request = CellRequest::new(&cx, &(), HOME, now).with_damped(&damped);
        assert_eq!(
            HomeFirst.choose_where(request, &cells, |cell| cell.name == "a"),
            Some(2)
        );
        assert_eq!(HomeFirst.choose_where(request, &cells, |_| false), None);
    }

    /// A cell is eligible while it is in a tier, damped or not.
    #[test]
    fn eligible_is_in_a_tier() {
        let now = origin();
        assert!(HomeFirst.eligible(&cell("a", HOME, CellState::Ready, 1, now), HOME, now));
        assert!(HomeFirst.eligible(&cell("a", AWAY, CellState::Degraded, 1, now), HOME, now));
        assert!(!HomeFirst.eligible(&cell("a", HOME, CellState::Draining, 1, now), HOME, now));
        assert!(!HomeFirst.eligible(&cell("a", HOME, CellState::Ready, 0, now), HOME, now));
        let far = busy("far", AWAY, CellState::Ready, LIMIT, 0, now);
        assert!(!HomeFirst.eligible(&far, HOME, now));
        assert!(!HomeFirst.eligible(
            &cell("a", HOME, CellState::Ready, 1, now),
            HOME,
            now + PERIOD * 4
        ));
    }
}
