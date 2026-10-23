//! Cell policies: how a chooser picks, once per attempt, the cell of a
//! class it sends to, from that class's entry table. A placement names
//! one per entered class (`[cells.<class>] policy`); the runtime provides
//! `home_first`.

use std::sync::Arc;

use gdl::{Error, ErrorKind, Result};
use gdl_cells::{CellState, LoadSummary};
use gdl_model::{CellPolicy, Layout};
use tokio::time::Instant;

use super::CellTable;
use super::damping::Damped;

/// How a chooser picks a cell of a class it sends to.
pub(crate) trait CellChooser: Send + Sync + 'static {
    /// The index in `cells` of the cell to send this attempt to, or `None`
    /// when no cell may take it. `region` is the chooser's own; `excluded`
    /// holds the [`id`](CellTable::id)s of the cells that declined this
    /// request, never chosen again for it; `damped` the cells the chooser
    /// damps, as one moment's view; `now` is the chooser's clock, on which
    /// each cell's summary arrived; `draw` is a random number drawn for
    /// this attempt.
    fn choose(
        &self,
        cells: &[Arc<CellTable>],
        region: &str,
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
        draw: u64,
    ) -> Option<usize>;

    /// Whether `cell` may still be sent to at `now`, by the same rules as
    /// [`choose`](CellChooser::choose) but for damping: a choice that waited
    /// on a selector is checked again with it, since the cell's summary may
    /// have gone or gone stale meanwhile.
    fn eligible(&self, cell: &CellTable, region: &str, excluded: &[u64], now: Instant) -> bool;
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
///
/// A cell this chooser damps ([`Damping`](super::damping::Damping)) is
/// not chosen while its window lasts, but at most half of the eligible
/// cells, rounded down, are damped at once: when more are, those whose
/// windows end soonest are chosen as if they were not. So one bad spell
/// cannot leave a request no cell, and a lone cell is never damped.
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
        let load = cell.load.as_ref()?;
        if load.fresh_until.is_none_or(|until| now > until) {
            return None;
        }
        let load = &load.summary;
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

impl HomeFirst {
    /// The least `cut` such that no more than `allowed` of the eligible
    /// cells have more than `cut` milliseconds of their damping windows
    /// left in `damped`, when `longest` is the most any has: a binary
    /// search of counting passes, at most about 15 over a window of 30 s,
    /// each a search of the damped cells per cell, with no allocation.
    /// Taken only when more than `allowed` cells are damped.
    fn cut(
        cells: &[Arc<CellTable>],
        region: &str,
        excluded: &[u64],
        (damped, now): (&Damped, Instant),
        allowed: u64,
        longest: u64,
    ) -> u64 {
        let over = |cut: u64| -> u64 {
            let damped = cells.iter().filter(|cell| {
                Self::tier(cell, region, excluded, now).is_some() && damped.left(cell.id, now) > cut
            });
            u64::try_from(damped.count()).unwrap_or(u64::MAX)
        };
        let (mut low, mut high) = (0, longest);
        while low < high {
            let mid = low + (high - low) / 2;
            if over(mid) <= allowed {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        high
    }
}

/// A random number for the `nth` cell of a choice from its `draw`: the
/// splitmix64 finalizer, so that one draw gives each cell its own.
fn mix(draw: u64, nth: u64) -> u64 {
    let mut z = draw ^ nth.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl CellChooser for HomeFirst {
    /// One pass and no allocation while no cell's damping window lasts:
    /// the best tier of the eligible cells, with two of them drawn as it
    /// goes, by reservoir sampling. While a window lasts, a pass first
    /// counts the eligible and the damped cells, which sets which damped
    /// cells are passed over, each a search of the short list of damped
    /// cells. `damped` is one moment's view, so the cap and the cells drawn
    /// from agree whatever other requests decline or accept meanwhile, and
    /// a choice finds a cell whenever one is eligible.
    fn choose(
        &self,
        cells: &[Arc<CellTable>],
        region: &str,
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
        draw: u64,
    ) -> Option<usize> {
        let lasts = damped.lasts(now);
        let cut = if lasts {
            let (mut eligible, mut held, mut longest) = (0u64, 0u64, 0u64);
            for cell in cells {
                if Self::tier(cell, region, excluded, now).is_none() {
                    continue;
                }
                eligible += 1;
                let left = damped.left(cell.id, now);
                if left > 0 {
                    held += 1;
                    longest = longest.max(left);
                }
            }
            let allowed = eligible / 2;
            if held <= allowed {
                0
            } else {
                Self::cut(cells, region, excluded, (damped, now), allowed, longest)
            }
        } else {
            0
        };
        let mut best: Option<u8> = None;
        let mut count = 0u64;
        let mut drawn: [Option<(usize, &LoadSummary)>; 2] = [None, None];
        for (at, cell) in cells.iter().enumerate() {
            let Some((tier, load)) = Self::tier(cell, region, excluded, now) else {
                continue;
            };
            if lasts && damped.left(cell.id, now) > cut {
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

    /// Whether `cell` is in a tier: a damped cell is still eligible, since
    /// damping steers the next choices, not one already made.
    fn eligible(&self, cell: &CellTable, region: &str, excluded: &[u64], now: Instant) -> bool {
        Self::tier(cell, region, excluded, now).is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::hash::{BuildHasher, RandomState};
    use std::sync::LazyLock;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use gdl_cells::LoadSummary;
    use velo::InstanceId;

    use super::super::Load;
    use super::super::damping::{Damped, Damping};
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    /// The time every cell's damping counts from, and no later than any
    /// time a test passes.
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

    /// What a cell's table holds of its load: its summary and when it
    /// arrived, or nothing.
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
    ) -> Arc<CellTable> {
        let eligible: Box<[InstanceId]> = (0..entries).map(|_| InstanceId::new_v4()).collect();
        let load = match load {
            Loaded::At {
                admitted,
                refusals,
                arrived,
            } => Some(Load::new(
                LoadSummary {
                    admitted,
                    limit: LIMIT,
                    instances: u32::try_from(entries).expect("a few"),
                    refusals,
                    period_ms: u64::try_from(PERIOD.as_millis()).expect("short"),
                },
                arrived,
            )),
            Loaded::None => None,
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
            offering: true,
            revision: 1,
            leases: Arc::default(),
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
        chosen_damped(cells, &Damped::default(), now)
    }

    /// [`chosen_at`], with the cells in `damped` damped.
    fn chosen_damped(cells: &[Arc<CellTable>], damped: &Damped, now: Instant) -> BTreeSet<String> {
        draws(256)
            .filter_map(|draw| HomeFirst.choose(cells, HOME, &[], damped, now, draw))
            .map(|at| cells[at].cell.clone())
            .collect()
    }

    /// The [`id`](CellTable::id) of the cell `name` here.
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
        for state in [CellState::Unavailable, CellState::Draining] {
            let cells = [cell("home", HOME, state, 1, now)];
            assert_eq!(
                HomeFirst.choose(&cells, HOME, &[], &Damped::default(), now, 0),
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
        assert_eq!(
            HomeFirst.choose(&cells, HOME, &[], &Damped::default(), now, 3),
            None
        );
        assert_eq!(
            HomeFirst.choose(&[], HOME, &[], &Damped::default(), now, 3),
            None
        );
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
        let mut table =
            Arc::try_unwrap(cell("unknown", HOME, CellState::Ready, 1, now)).expect("one owner");
        if let Some(load) = &mut table.load {
            *load = Load::new(
                LoadSummary {
                    period_ms: 0,
                    ..load.summary
                },
                now,
            );
        }
        assert_eq!(chosen_at(&[Arc::new(table)], now), names(&[]));
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
        let count = |cells: &[Arc<CellTable>]| -> Vec<u64> {
            let mut counts = vec![0; cells.len()];
            for draw in draws(N) {
                counts[HomeFirst
                    .choose(cells, HOME, &[], &Damped::default(), now, draw)
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
        let now = origin();
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
                .filter_map(|draw| {
                    HomeFirst.choose(&cells, HOME, &excluded, &Damped::default(), now, draw)
                })
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

    /// A cell that declined is not chosen while its window lasts, here
    /// 1 s, and is once it has passed; a decline then doubles the window;
    /// an acceptance ends the damping at once. A damped home cell sends
    /// the request elsewhere, while elsewhere has room.
    #[test]
    fn a_cell_that_declined_is_not_chosen_while_its_window_lasts() {
        let now = origin();
        let damping = Damping::default();
        let cells = [
            cell("a", HOME, CellState::Ready, 1, now),
            cell("b", HOME, CellState::Ready, 1, now),
            cell("c", HOME, CellState::Ready, 1, now),
        ];
        let chosen = |at: Instant| chosen_damped(&cells, &damping.view(), at);
        let every = names(&["a", "b", "c"]);
        let just = Duration::from_millis(1);
        damping.decline(id("a"), now);
        assert_eq!(chosen(now), names(&["b", "c"]));
        assert_eq!(chosen(now + SECOND - just), names(&["b", "c"]));
        assert_eq!(chosen(now + SECOND), every);
        damping.decline(id("a"), now + SECOND);
        assert_eq!(chosen(now + SECOND * 3 - just), names(&["b", "c"]));
        assert_eq!(chosen(now + SECOND * 3), every);
        damping.decline(id("a"), now + SECOND * 3);
        assert_eq!(chosen(now + SECOND * 3), names(&["b", "c"]));
        damping.accept(id("a"));
        assert_eq!(chosen(now + SECOND * 3), every);

        let cells = [
            cell("home", HOME, CellState::Ready, 1, now),
            cell("away", AWAY, CellState::Ready, 1, now),
        ];
        damping.decline(id("home"), now);
        assert_eq!(
            chosen_damped(&cells, &damping.view(), now),
            names(&["away"])
        );
    }

    /// At most half of the eligible cells, rounded down, are damped at
    /// once; the others whose windows end soonest are chosen as if they
    /// were not. Of three damped cells, the latest to end stays damped; of
    /// two, the later; a lone cell is never damped, and neither is the
    /// last cell a request has not excluded.
    #[test]
    fn at_most_half_of_the_eligible_cells_are_damped() {
        let now = origin() + SECOND;
        let damping = Damping::default();
        let cells = [
            cell("soonest", HOME, CellState::Ready, 1, now),
            cell("sooner", HOME, CellState::Ready, 1, now),
            cell("latest", HOME, CellState::Ready, 1, now),
        ];
        damping.decline(id("soonest"), now - SECOND / 2);
        damping.decline(id("sooner"), now - SECOND / 4);
        damping.decline(id("latest"), now);
        let damped = damping.view();
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
            draws(64)
                .filter_map(|draw| HomeFirst.choose(&cells, HOME, excluded, &damped, now, draw))
                .map(|at| cells[at].cell.clone())
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
        damping.decline(id("damped"), now);
        assert_eq!(
            chosen_damped(&cells, &damping.view(), now),
            names(&["damped"])
        );
    }

    /// A cell damped or undamped while choices are made, as other
    /// requests' declines and acceptances do, never leaves a choice with
    /// no cell while another stays eligible, nor a lone cell unchosen:
    /// here the ready cell's damping changes on another thread throughout,
    /// beside a degraded cell that is always there to take the request,
    /// and alone. Each choice reads the damping as a chooser does, once.
    /// Counts the choices that found no cell.
    #[test]
    fn a_damping_that_changes_during_a_choice_leaves_it_a_cell() {
        let now = origin();
        let damping = Damping::default();
        let cells = [
            cell("ready", HOME, CellState::Ready, 1, now),
            cell("degraded", HOME, CellState::Degraded, 1, now),
        ];
        for choosable in [&cells[..], &cells[..1]] {
            let done = std::sync::atomic::AtomicBool::new(false);
            let empty = std::thread::scope(|scope| {
                scope.spawn(|| {
                    while !done.load(std::sync::atomic::Ordering::Relaxed) {
                        damping.decline(id("ready"), now);
                        damping.accept(id("ready"));
                    }
                });
                let empty = draws(100_000)
                    .filter(|draw| {
                        HomeFirst
                            .choose(choosable, HOME, &[], &damping.view(), now, *draw)
                            .is_none()
                    })
                    .count();
                done.store(true, std::sync::atomic::Ordering::Relaxed);
                empty
            });
            assert_eq!(
                empty,
                0,
                "of {} cells, choices that found none",
                choosable.len()
            );
        }
    }

    /// A choice sees every cell's damping as it was at one moment, so the
    /// cap and the two choices are made over the same cells: with three
    /// eligible cells, at most one of them damped, so at least two are
    /// drawn from, and the heaviest never wins a pair. Here another thread
    /// damps all three, their windows ending in turn, and ends their
    /// damping, throughout. Counts the choices of the heaviest.
    #[test]
    fn a_choice_reads_one_moment_of_the_damping() {
        let now = origin() + SECOND;
        let damping = Damping::default();
        let cells = [
            busy("heavy", HOME, CellState::Ready, 70, 0, now),
            busy("light", HOME, CellState::Ready, 10, 0, now),
            busy("middle", HOME, CellState::Ready, 40, 0, now),
        ];
        let done = std::sync::atomic::AtomicBool::new(false);
        let heaviest = std::thread::scope(|scope| {
            scope.spawn(|| {
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    damping.decline(id("heavy"), now - SECOND / 2);
                    damping.decline(id("light"), now - SECOND / 4);
                    damping.decline(id("middle"), now);
                    for name in ["heavy", "light", "middle"] {
                        damping.accept(id(name));
                    }
                }
            });
            let heaviest = draws(100_000)
                .filter(|draw| {
                    HomeFirst.choose(&cells, HOME, &[], &damping.view(), now, *draw) == Some(0)
                })
                .count();
            done.store(true, std::sync::atomic::Ordering::Relaxed);
            heaviest
        });
        assert_eq!(heaviest, 0, "choices of the heaviest cell");
    }

    /// The case without threads: a view read before three cells were
    /// damped, with windows ending 2, 4, and 8 s on, still holds none
    /// of them, and a choice by it never takes the heaviest; a choice by
    /// the view after leaves the two that end soonest to be drawn, and
    /// takes the lighter of them.
    #[test]
    fn a_view_read_before_a_change_is_chosen_by_as_it_was() {
        let now = origin() + SECOND * 8;
        let damping = Damping::default();
        let cells = [
            busy("heavy", HOME, CellState::Ready, 70, 0, now),
            busy("light", HOME, CellState::Ready, 10, 0, now),
            busy("middle", HOME, CellState::Ready, 40, 0, now),
        ];
        let before = damping.view();
        // A first decline damps for 1 s from it.
        for (name, ends) in [("heavy", 2), ("light", 4), ("middle", 8)] {
            damping.decline(id(name), now + SECOND * (ends - 1));
        }
        let after = damping.view();
        for (name, ends) in [("heavy", 2000), ("light", 4000), ("middle", 8000)] {
            assert_eq!(after.left(id(name), now), ends, "{name}");
        }
        assert!(!chosen_damped(&cells, &before, now).contains("heavy"));
        assert_eq!(chosen_damped(&cells, &after, now), names(&["light"]));
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
        for draw in draws(3000) {
            counts[HomeFirst
                .choose(&cells, HOME, &[], &Damped::default(), now, draw)
                .expect("a cell")] += 1;
        }
        assert_eq!(counts[1], 0, "{counts:?}");
        for at in [0, 2, 3] {
            assert!(counts[at].abs_diff(1000) < 150, "{counts:?}");
        }
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
