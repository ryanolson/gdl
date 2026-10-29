use tokio::time::Instant;

use super::{
    Damped, LoadSummary, NoPool, Pool, PoolChoice, PoolPolicy, PoolRequest, PoolState, cut, mix,
};
use crate::select::Encoded;

/// `home_first`: by state, region, and admission, from each pool's load
/// summary.
///
/// A pool is eligible only while it is `ready` or `degraded`, has an
/// entry instance to route to, has not declined the request nor declined
/// an offer as gone in the follow it is shown in
/// ([`Damped::is_gone`](super::Damped::is_gone)), and has a summary that
/// is fresh ([`Load::is_fresh`](super::Load::is_fresh), three of its
/// periods by default): a pool whose load is not known is not sent to, since a
/// summary steers only while it is recent and a pool's refusal is the
/// fast correction. Among the eligible, the first non-empty tier:
///
/// 1. home pools (the chooser's region) that are `ready` and not
///    saturated;
/// 2. pools elsewhere that are `ready`, not saturated, and under the
///    spill bound;
/// 3. home pools that are `ready` and saturated;
/// 4. to 6. the `degraded` pools, in the same three tiers.
///
/// So a state outranks a region, home pools take the work
/// until every one is saturated, and another region's pool takes it only
/// while it has room; a pool elsewhere at or over the spill bound is never
/// chosen. Within the tier, two pools drawn at random, and the one with
/// the lower ratio of admitted roots to admission limit: a deterministic
/// least-loaded pick would send every chooser to one pool between two
/// summaries. Admission counts roots, not engine work, so this is an
/// admission heuristic.
///
/// A pool the chooser damps ([`Damped`](super::Damped)) is not chosen
/// while its window lasts, but at most half of the eligible pools, rounded
/// down, are damped at once: when more are, those whose windows end
/// soonest are chosen as if they were not. So one bad spell cannot leave a
/// request no pool, and a lone pool is never damped.
///
/// `HomeFirst` chooses at the default thresholds,
/// [`SATURATED_PERCENT`](HomeFirst::SATURATED_PERCENT) and
/// [`SPILL_PERCENT`](HomeFirst::SPILL_PERCENT). [`HomeFirstAt`] chooses
/// alike at thresholds of its own, as a chooser's own `home_first` does
/// at its `[runtime]` settings.
#[derive(Debug, Clone, Copy, Default)]
pub struct HomeFirst;

impl HomeFirst {
    /// A pool is saturated once its admitted roots reach this share of its
    /// admission limit, in percent, or its last summary period had
    /// refusals, unless the thresholds say otherwise ([`HomeFirstAt`]).
    pub const SATURATED_PERCENT: u64 = 100;

    /// A pool in another region is a candidate only while its admitted
    /// roots are under this share of its admission limit, in percent,
    /// unless the thresholds say otherwise ([`HomeFirstAt`]).
    pub const SPILL_PERCENT: u64 = 80;

    /// [`HomeFirstAt::choose_where`] at the default thresholds.
    #[inline]
    pub fn choose_where<H>(
        &self,
        request: PoolRequest<'_, H>,
        pools: &[Pool],
        keep: impl Fn(&Pool) -> bool,
    ) -> Option<usize> {
        self.choose_in(request, pools, keep)
    }
}

impl PoolPolicy for HomeFirst {
    /// It reads no request data.
    type Head = Encoded;

    /// [`choose_where`](HomeFirst::choose_where), keeping every pool, with
    /// no lease.
    fn choose(
        &self,
        request: PoolRequest<'_, Encoded>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        self.choose_where(request, pools, |_| true)
            .map(PoolChoice::new)
            .ok_or_else(NoPool::new)
    }

    /// Whether `pool` is in a tier at the default thresholds.
    fn eligible(&self, pool: &Pool, region: &str, now: Instant) -> bool {
        self.tier(pool, region, &[], &Damped::EMPTY, now).is_some()
    }
}

/// `home_first` at thresholds of its own: what [`HomeFirst`] does at the
/// defaults. A chooser's own `home_first` is one, at the chooser's
/// `[runtime]` settings (`home_first_saturated_percent`,
/// `home_first_spill_percent`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeFirstAt {
    /// A pool is saturated once its admitted roots reach this share of its
    /// admission limit, in percent, or its last summary period had
    /// refusals.
    pub saturated_percent: u64,
    /// A pool in another region is a candidate only while its admitted
    /// roots are under this share of its admission limit, in percent: a
    /// pool near its limit, behind a round trip between regions, is where
    /// a request would most likely be declined.
    pub spill_percent: u64,
}

impl Default for HomeFirstAt {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl HomeFirstAt {
    /// The default thresholds, [`HomeFirst`]'s.
    pub const DEFAULT: Self = Self {
        saturated_percent: HomeFirst::SATURATED_PERCENT,
        spill_percent: HomeFirst::SPILL_PERCENT,
    };

    /// `home_first`'s choice among only the pools `keep` keeps, as if
    /// the others were not there: the index of the chosen pool in
    /// `pools`, or `None` when no kept pool is eligible. The tiers, the
    /// two pools drawn, and the damping cap count only the kept pools. A
    /// policy that filters the pools, by property for instance, runs
    /// `home_first` over the rest with this, and allocates nothing. It
    /// reads no head, so it takes a request of any head.
    ///
    /// `keep` must answer for a pool from that pool alone: alike each
    /// time, whatever other pools it was called for, and in any order. It
    /// is called for every pool, eligible or not, before the pool's tier
    /// is found, and again for a pool within one choice. A policy that
    /// chooses by it should also check a pool as
    /// [`eligible`](PoolPolicy::eligible) does here.
    ///
    /// One pass and no allocation while no pool's damping window lasts:
    /// the best tier of the eligible pools, with two of them drawn as it
    /// goes, by reservoir sampling. While a window lasts, a pass first
    /// counts the eligible and the damped pools, which sets which damped
    /// pools are passed over, each a search of the short list of damped
    /// pools. The damping is one moment's view, so the cap and the pools
    /// drawn from agree whatever other requests decline or accept
    /// meanwhile, and a choice finds a pool whenever one is eligible.
    pub fn choose_where<H>(
        &self,
        request: PoolRequest<'_, H>,
        pools: &[Pool],
        keep: impl Fn(&Pool) -> bool,
    ) -> Option<usize> {
        // At the default thresholds, by `HomeFirst`'s code, in which they
        // are constants: a chooser at its default settings pays one branch
        // for them, not two 128-bit products per pool.
        if *self == Self::DEFAULT {
            HomeFirst.choose_in(request, pools, keep)
        } else {
            self.choose_in(request, pools, keep)
        }
    }
}

impl PoolPolicy for HomeFirstAt {
    /// It reads no request data.
    type Head = Encoded;

    /// [`choose_where`](HomeFirstAt::choose_where), keeping every pool,
    /// with no lease.
    fn choose(
        &self,
        request: PoolRequest<'_, Encoded>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        self.choose_where(request, pools, |_| true)
            .map(PoolChoice::new)
            .ok_or_else(NoPool::new)
    }

    /// Whether `pool` is in a tier: a damped or gone pool is still
    /// eligible, since damping steers the next choices, not one already
    /// made.
    fn eligible(&self, pool: &Pool, region: &str, now: Instant) -> bool {
        self.tier(pool, region, &[], &Damped::EMPTY, now).is_some()
    }
}

/// `home_first`'s choice at a policy's thresholds. [`HomeFirst`]'s are
/// constants, so that its choice is built with them in the code, where a
/// saturated home pool is one compare; [`HomeFirstAt`]'s are data. A
/// generic over the two, rather than one function that takes the
/// thresholds, since the compiler does not inline that function into
/// every caller, and then reads the defaults as data too.
trait Thresholds: Copy {
    fn saturated_percent(self) -> u64;
    fn spill_percent(self) -> u64;

    /// `pool`'s tier, lower first, with its summary, or `None` when it is
    /// not eligible. Out of line: inlined into a pass whose `keep` is a
    /// call, it keeps more values live across that call, and a pick that
    /// filters by property took 8 ns more. The pass calls it only for the
    /// pools `keep` keeps, so a pick that filters by property pays the call
    /// for those alone (`docs/src/operations/feature-costs.md`).
    #[inline(never)]
    fn tier<'c>(
        self,
        pool: &'c Pool,
        region: &str,
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
    ) -> Option<(u8, &'c LoadSummary)> {
        if pool.entries == 0 || excluded.contains(&pool.id) || damped.is_gone(pool) {
            return None;
        }
        let state = match pool.state {
            PoolState::Ready => 0,
            PoolState::Degraded => 3,
            PoolState::Draining | PoolState::Unavailable => return None,
        };
        let load = pool.load.as_ref()?;
        if !load.is_fresh(now) {
            return None;
        }
        let load = load.summary();
        let share = |percent: u64| u128::from(load.limit) * u128::from(percent);
        let admitted = u128::from(load.admitted) * 100;
        let saturated = load.refusals > 0 || admitted >= share(self.saturated_percent());
        let tier = match (pool.region == region, saturated) {
            (true, false) => 0,
            (false, false) if admitted < share(self.spill_percent()) => 1,
            (true, true) => 2,
            (false, _) => return None,
        };
        Some((state + tier, load))
    }

    /// [`HomeFirstAt::choose_where`] at these thresholds.
    fn choose_in<H>(
        self,
        request: PoolRequest<'_, H>,
        pools: &[Pool],
        keep: impl Fn(&Pool) -> bool,
    ) -> Option<usize> {
        let (region, excluded) = (request.region(), request.excluded());
        let (damped, now) = (request.damped(), request.now());
        let lasts = damped.lasts(now);
        let cut = if lasts {
            let eligible = |pool: &Pool| {
                keep(pool) && self.tier(pool, region, excluded, damped, now).is_some()
            };
            cut(pools, eligible, damped, now)
        } else {
            0
        };
        let draw = request.draw();
        let mut best: Option<u8> = None;
        let mut count = 0u64;
        let mut drawn: [Option<(usize, &LoadSummary)>; 2] = [None, None];
        for (at, pool) in pools.iter().enumerate() {
            // The filter first: a pool it drops costs no call to `tier`.
            if !keep(pool) {
                continue;
            }
            let Some((tier, load)) = self.tier(pool, region, excluded, damped, now) else {
                continue;
            };
            if lasts && damped.left(pool.id, now) > cut {
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
            // Each pool of the tier is one of the two drawn with the same
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
            [Some((first, a)), Some((second, b))] => Some(if lighter(b, a) {
                second
            } else if lighter(a, b) {
                first
            } else if mix(draw, 0) & 1 == 0 {
                // A tie goes to either alike, so that every pool of a
                // tier of one load is chosen as often.
                first
            } else {
                second
            }),
            [Some((one, _)), None] | [None, Some((one, _))] => Some(one),
            [None, None] => None,
        }
    }
}

impl Thresholds for HomeFirst {
    #[inline(always)]
    fn saturated_percent(self) -> u64 {
        Self::SATURATED_PERCENT
    }

    #[inline(always)]
    fn spill_percent(self) -> u64 {
        Self::SPILL_PERCENT
    }
}

impl Thresholds for HomeFirstAt {
    #[inline(always)]
    fn saturated_percent(self) -> u64 {
        self.saturated_percent
    }

    #[inline(always)]
    fn spill_percent(self) -> u64 {
        self.spill_percent
    }
}

/// Whether `a` has a lower ratio of admitted roots to limit than `b`; a
/// limit of zero is the highest.
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
    /// The summary period of every pool here.
    const PERIOD: Duration = Duration::from_secs(10);
    /// Each pool's admission limit.
    const LIMIT: u64 = 100;

    /// What a pool holds of its load: its summary and when it arrived, or
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

    fn pool_loaded(
        name: &str,
        region: &str,
        state: PoolState,
        entries: usize,
        load: Loaded,
    ) -> Pool {
        let pool = Pool::new("model_x", name, region, state).with_entries(entries);
        match load {
            Loaded::At {
                admitted,
                refusals,
                arrived,
            } => pool.with_load(Load::new(
                LoadSummary {
                    admitted,
                    limit: LIMIT,
                    instances: u32::try_from(entries).expect("a few"),
                    refusals,
                    period_ms: u64::try_from(PERIOD.as_millis()).expect("short"),
                },
                arrived,
            )),
            Loaded::None => pool,
        }
    }

    /// An idle pool whose summary arrived at `now`.
    fn pool(name: &str, region: &str, state: PoolState, entries: usize, now: Instant) -> Pool {
        pool_loaded(name, region, state, entries, load(0, 0, now))
    }

    /// A pool with `admitted` of [`LIMIT`] roots and `refusals`, whose
    /// summary arrived at `now`.
    fn busy(
        name: &str,
        region: &str,
        state: PoolState,
        admitted: u64,
        refusals: u64,
        now: Instant,
    ) -> Pool {
        pool_loaded(name, region, state, 1, load(admitted, refusals, now))
    }

    /// `home_first`'s choice among `pools` at `now` and turn `turn`, with
    /// `excluded` and `damped`.
    fn choose(
        pools: &[Pool],
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
        turn: u64,
    ) -> Option<usize> {
        let cx = Cx::new();
        let request = PoolRequest::new(&cx, &Encoded, HOME, now)
            .with_excluded(excluded)
            .with_damped(damped)
            .with_turn(turn);
        HomeFirst
            .choose(request, pools)
            .ok()
            .map(|choice| choice.at())
    }

    /// The pools `home_first` chooses at `now` over many turns, by name.
    fn chosen_at(pools: &[Pool], now: Instant) -> BTreeSet<String> {
        chosen_damped(pools, &Damped::default(), now)
    }

    /// [`chosen_at`], with the pools in `damped` damped.
    fn chosen_damped(pools: &[Pool], damped: &Damped, now: Instant) -> BTreeSet<String> {
        (0..256)
            .filter_map(|turn| choose(pools, &[], damped, now, turn))
            .map(|at| pools[at].name.clone())
            .collect()
    }

    /// The [`id`](Pool::id) of the pool `name` here.
    fn id(name: &str) -> u64 {
        super::super::pool_id("model_x", name)
    }

    fn names(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn its_own_region_first_within_a_state() {
        let now = origin();
        let pools = [
            pool("away", AWAY, PoolState::Ready, 1, now),
            pool("home-1", HOME, PoolState::Ready, 1, now),
            pool("home-2", HOME, PoolState::Ready, 2, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["home-1", "home-2"]));
        let pools = [
            pool("away", AWAY, PoolState::Degraded, 1, now),
            pool("home", HOME, PoolState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["home"]));
        // With no home pool, the pool elsewhere, while it has room.
        let pools = [pool("away", AWAY, PoolState::Ready, 1, now)];
        assert_eq!(chosen_at(&pools, now), names(&["away"]));
    }

    #[test]
    fn ready_before_degraded_whatever_the_region_or_load() {
        let now = origin();
        let pools = [
            pool("home", HOME, PoolState::Degraded, 1, now),
            pool("away", AWAY, PoolState::Ready, 1, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["away"]));
        let pools = [
            pool("home", HOME, PoolState::Degraded, 1, now),
            pool("away", AWAY, PoolState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["home"]));
        // A saturated ready home pool before an idle degraded one.
        let pools = [
            pool("idle", HOME, PoolState::Degraded, 1, now),
            busy("full", HOME, PoolState::Ready, LIMIT, 0, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["full"]));
    }

    #[test]
    fn never_unavailable_or_draining_and_never_empty() {
        let now = origin();
        let none = Damped::default();
        for state in [PoolState::Unavailable, PoolState::Draining] {
            let pools = [pool("home", HOME, state, 1, now)];
            assert_eq!(choose(&pools, &[], &none, now, 0), None, "{state:?}");
        }
        // A ready home pool with nothing to route to loses to a degraded
        // pool elsewhere that has.
        let pools = [
            pool("empty", HOME, PoolState::Ready, 0, now),
            pool("away", AWAY, PoolState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["away"]));
        let pools = [pool("empty", HOME, PoolState::Ready, 0, now)];
        assert_eq!(choose(&pools, &[], &none, now, 3), None);
        assert_eq!(choose(&[], &[], &none, now, 3), None);
    }

    /// A pool with no summary, or one older than three of its periods, is
    /// not chosen, whatever its state and region: with no other pool,
    /// none is. A summary exactly three periods old is still fresh.
    #[test]
    fn a_pool_without_a_fresh_summary_is_not_chosen() {
        let now = origin();
        let stale = now - PERIOD * 3 - Duration::from_millis(1);
        let pools = [
            pool_loaded("none", HOME, PoolState::Ready, 1, Loaded::None),
            pool_loaded("stale", HOME, PoolState::Ready, 1, load(0, 0, stale)),
            pool("away", AWAY, PoolState::Degraded, 1, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["away"]));
        assert_eq!(chosen_at(&pools[..2], now), names(&[]));
        let pools = [pool_loaded(
            "old",
            HOME,
            PoolState::Ready,
            1,
            load(0, 0, now - PERIOD * 3),
        )];
        assert_eq!(chosen_at(&pools, now), names(&["old"]));
        // A summary that says nothing of its period is never fresh.
        let unknown = Pool::new("model_x", "unknown", HOME, PoolState::Ready)
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

    /// The thresholds are the policy's own: with saturation at 50% and the
    /// spill bound at 10%, a home pool at 60% is saturated, and a pool
    /// elsewhere at 20% is not a candidate, where the defaults, 100% and
    /// 80%, take both.
    #[test]
    fn its_thresholds_are_the_ones_it_is_given() {
        let now = origin();
        let defaults = HomeFirstAt::default();
        assert_eq!(
            (defaults.saturated_percent, defaults.spill_percent),
            (100, 80)
        );
        let tight = HomeFirstAt {
            saturated_percent: 50,
            spill_percent: 10,
        };
        let chosen = |policy: &HomeFirstAt, pools: &[Pool]| -> BTreeSet<String> {
            (0..256)
                .filter_map(|turn| {
                    let cx = Cx::new();
                    let request = PoolRequest::new(&cx, &Encoded, HOME, now).with_turn(turn);
                    policy.choose(request, pools).ok().map(|choice| choice.at())
                })
                .map(|at| pools[at].name.clone())
                .collect()
        };
        // At 60%, home is not saturated by default, so it comes first; at
        // 50%, it is, and the idle pool elsewhere comes first.
        let pools = [
            busy("home", HOME, PoolState::Ready, LIMIT * 6 / 10, 0, now),
            busy("away", AWAY, PoolState::Ready, 0, 0, now),
        ];
        assert_eq!(chosen(&defaults, &pools), names(&["home"]));
        assert_eq!(chosen(&tight, &pools), names(&["away"]));
        // Elsewhere at 20%: under the default bound, over the tight one.
        let away = [busy("away", AWAY, PoolState::Ready, LIMIT * 2 / 10, 0, now)];
        assert_eq!(chosen(&defaults, &away), names(&["away"]));
        assert_eq!(chosen(&tight, &away), names(&[]));
        assert!(defaults.eligible(&away[0], HOME, now));
        assert!(!tight.eligible(&away[0], HOME, now));
    }

    /// Home pools are used until every one is saturated (its admitted
    /// roots at its limit, or refusals in its last period); a pool
    /// elsewhere is a candidate only while its admission is under 80% of
    /// its limit, and then before a saturated home pool.
    #[test]
    fn home_until_saturated_then_elsewhere_with_room() {
        let now = origin();
        let room = LIMIT * 8 / 10 - 1;
        let bound = LIMIT * 8 / 10;
        // An unsaturated home pool, however busy, before any pool elsewhere.
        let pools = [
            busy("home", HOME, PoolState::Ready, LIMIT - 1, 0, now),
            busy("away", AWAY, PoolState::Ready, 0, 0, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["home"]));
        // Saturated at home, by admission or by refusals: elsewhere, if it
        // has room.
        for (admitted, refusals) in [(LIMIT, 0), (LIMIT + 5, 0), (1, 1)] {
            let pools = [
                busy("home", HOME, PoolState::Ready, admitted, refusals, now),
                busy("away", AWAY, PoolState::Ready, room, 0, now),
                busy("full-away", AWAY, PoolState::Ready, bound, 0, now),
            ];
            assert_eq!(
                chosen_at(&pools, now),
                names(&["away"]),
                "{admitted} {refusals}"
            );
        }
        // Elsewhere at or over the spill bound, or refusing: the saturated
        // home pool.
        for (admitted, refusals) in [(bound, 0), (LIMIT, 0), (0, 1)] {
            let pools = [
                busy("home", HOME, PoolState::Ready, LIMIT, 0, now),
                busy("away", AWAY, PoolState::Ready, admitted, refusals, now),
            ];
            assert_eq!(
                chosen_at(&pools, now),
                names(&["home"]),
                "{admitted} {refusals}"
            );
            // With no home pool, nothing.
            assert_eq!(
                chosen_at(&pools[1..], now),
                names(&[]),
                "{admitted} {refusals}"
            );
        }
        // The degraded pools by the same order, after every ready one.
        let pools = [
            busy("home", HOME, PoolState::Degraded, LIMIT, 0, now),
            busy("away", AWAY, PoolState::Degraded, room, 0, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["away"]));
        let pools = [
            busy("home", HOME, PoolState::Degraded, LIMIT, 0, now),
            busy("away", AWAY, PoolState::Degraded, bound, 0, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["home"]));
        let pools = [
            busy("home", HOME, PoolState::Degraded, 0, 0, now),
            busy("full", HOME, PoolState::Ready, LIMIT, 1, now),
            busy("away", AWAY, PoolState::Degraded, 0, 0, now),
        ];
        assert_eq!(chosen_at(&pools, now), names(&["full"]));
    }

    /// Within a tier, two random choices: of two pools, the one with the
    /// lower ratio of admitted roots to limit, always; of three, the
    /// lightest more often than not but not always, the middle one
    /// sometimes, and the heaviest never, since it loses to either other.
    #[test]
    fn two_random_choices_by_admission() {
        let now = origin();
        const N: u64 = 3000;
        let count = |pools: &[Pool]| -> Vec<u64> {
            let mut counts = vec![0; pools.len()];
            for turn in 0..N {
                counts[choose(pools, &[], &Damped::default(), now, turn).expect("a pool")] += 1;
            }
            counts
        };
        let two = [
            busy("heavy", HOME, PoolState::Ready, 70, 0, now),
            busy("light", HOME, PoolState::Ready, 10, 0, now),
        ];
        assert_eq!(count(&two), [0, N]);
        let three = [
            busy("heavy", HOME, PoolState::Ready, 70, 0, now),
            busy("light", HOME, PoolState::Ready, 10, 0, now),
            busy("middle", HOME, PoolState::Ready, 40, 0, now),
        ];
        let counts = count(&three);
        assert_eq!(counts[0], 0, "{counts:?}");
        assert!(counts[1] > N / 2 && counts[1] < N, "{counts:?}");
        assert!(counts[2] > 0, "{counts:?}");
        // Elsewhere, in its own tier, alike.
        let away = [
            busy("home", HOME, PoolState::Ready, LIMIT, 0, now),
            busy("heavy", AWAY, PoolState::Ready, 70, 0, now),
            busy("light", AWAY, PoolState::Ready, 10, 0, now),
        ];
        assert_eq!(count(&away), [0, 0, N]);
    }

    /// A pool that declined the request is never chosen for it again,
    /// whatever its rank; the next best is, and with every pool excluded,
    /// none.
    #[test]
    fn a_pool_that_declined_is_not_chosen_again() {
        let now = origin();
        let pools = [
            pool("home-1", HOME, PoolState::Ready, 1, now),
            pool("home-2", HOME, PoolState::Ready, 1, now),
            pool("away", AWAY, PoolState::Degraded, 1, now),
        ];
        let chosen_without = |names: &[&str]| -> BTreeSet<String> {
            let excluded: Vec<u64> = names.iter().map(|name| id(name)).collect();
            (0..64)
                .filter_map(|turn| choose(&pools, &excluded, &Damped::default(), now, turn))
                .map(|at| pools[at].name.clone())
                .collect()
        };
        assert_eq!(chosen_without(&["home-1"]), names(&["home-2"]));
        assert_eq!(chosen_without(&["home-1", "home-2"]), names(&["away"]));
        assert!(chosen_without(&["home-1", "home-2", "away"]).is_empty());
        // The same pool in a table of another class is another pool.
        assert_ne!(
            super::super::pool_id("model_x", "home-1"),
            super::super::pool_id("model_y", "home-1")
        );
    }

    /// A damped pool is not chosen while its window lasts, and is once it
    /// has passed. A damped home pool sends the request elsewhere, while
    /// elsewhere has room.
    #[test]
    fn a_damped_pool_is_not_chosen_while_its_window_lasts() {
        let now = origin();
        let pools = [
            pool("a", HOME, PoolState::Ready, 1, now),
            pool("b", HOME, PoolState::Ready, 1, now),
            pool("c", HOME, PoolState::Ready, 1, now),
        ];
        let damped = Damped::new([(id("a"), now + SECOND)]);
        let just = Duration::from_millis(1);
        assert_eq!(chosen_damped(&pools, &damped, now), names(&["b", "c"]));
        assert_eq!(
            chosen_damped(&pools, &damped, now + SECOND - just),
            names(&["b", "c"])
        );
        assert_eq!(
            chosen_damped(&pools, &damped, now + SECOND),
            names(&["a", "b", "c"])
        );
        let pools = [
            pool("home", HOME, PoolState::Ready, 1, now),
            pool("away", AWAY, PoolState::Ready, 1, now),
        ];
        let damped = Damped::new([(id("home"), now + SECOND)]);
        assert_eq!(chosen_damped(&pools, &damped, now), names(&["away"]));
    }

    /// At most half of the eligible pools, rounded down, are damped at
    /// once; the others whose windows end soonest are chosen as if they
    /// were not. Of three damped pools, the latest to end stays damped; of
    /// two, the later; a lone pool is never damped, and neither is the
    /// last pool a request has not excluded.
    #[test]
    fn at_most_half_of_the_eligible_pools_are_damped() {
        let now = origin() + SECOND;
        let pools = [
            pool("soonest", HOME, PoolState::Ready, 1, now),
            pool("sooner", HOME, PoolState::Ready, 1, now),
            pool("latest", HOME, PoolState::Ready, 1, now),
        ];
        let damped = Damped::new([
            (id("soonest"), now + SECOND / 2),
            (id("sooner"), now + SECOND * 3 / 4),
            (id("latest"), now + SECOND),
        ]);
        assert_eq!(
            chosen_damped(&pools, &damped, now),
            names(&["soonest", "sooner"])
        );
        assert_eq!(
            chosen_damped(&pools[..2], &damped, now),
            names(&["soonest"])
        );
        assert_eq!(chosen_damped(&pools[1..], &damped, now), names(&["sooner"]));
        assert_eq!(chosen_damped(&pools[2..], &damped, now), names(&["latest"]));
        let without = |excluded: &[u64]| -> BTreeSet<String> {
            (0..64)
                .filter_map(|turn| choose(&pools, excluded, &damped, now, turn))
                .map(|at| pools[at].name.clone())
                .collect()
        };
        assert_eq!(without(&[id("soonest")]), names(&["sooner"]));
        assert_eq!(without(&[id("soonest"), id("sooner")]), names(&["latest"]));
        // A pool that is not eligible counts for nothing: with only one
        // eligible pool, it is chosen, damped or not.
        let pools = [
            pool("damped", HOME, PoolState::Ready, 1, now),
            pool("draining", HOME, PoolState::Draining, 1, now),
            pool_loaded("unknown", HOME, PoolState::Ready, 1, Loaded::None),
        ];
        let damped = Damped::new([(id("damped"), now + SECOND)]);
        assert_eq!(chosen_damped(&pools, &damped, now), names(&["damped"]));
    }

    /// Pools of one tier and one load are drawn alike: of the two drawn,
    /// a tie goes to either.
    #[test]
    fn every_best_pool_is_drawn_alike() {
        let now = origin();
        let pools = [
            pool("a", HOME, PoolState::Ready, 1, now),
            pool("other", AWAY, PoolState::Ready, 1, now),
            pool("b", HOME, PoolState::Ready, 1, now),
            pool("c", HOME, PoolState::Ready, 1, now),
        ];
        let mut counts = [0u64; 4];
        for turn in 0..3000 {
            counts[choose(&pools, &[], &Damped::default(), now, turn).expect("a pool")] += 1;
        }
        assert_eq!(counts[1], 0, "{counts:?}");
        for at in [0, 2, 3] {
            assert!(counts[at].abs_diff(1000) < 150, "{counts:?}");
        }
    }

    /// `choose_where` is `choose` over a copy of the pools it keeps, turn
    /// for turn, with its index into all of them: for every subset of
    /// pools of mixed state, region, and load, with one damped and one
    /// excluded.
    #[test]
    fn choose_where_is_choose_over_the_kept_pools() {
        let now = origin();
        let pools = [
            pool("home-1", HOME, PoolState::Ready, 1, now),
            busy("full", HOME, PoolState::Ready, LIMIT, 0, now),
            busy("away", AWAY, PoolState::Ready, 10, 0, now),
            busy("home-2", HOME, PoolState::Ready, 40, 0, now),
            pool("degraded", HOME, PoolState::Degraded, 1, now),
            busy("home-3", HOME, PoolState::Ready, 70, 0, now),
        ];
        let damped = Damped::new([(id("home-2"), now + SECOND)]);
        let cx = Cx::new();
        for excluded in [&[][..], &[id("home-1")][..]] {
            for mask in 0u32..1 << pools.len() {
                let kept = |pool: &Pool| {
                    let at = pools.iter().position(|of| of.name == pool.name);
                    at.is_some_and(|at| mask & 1 << at != 0)
                };
                let copy: Vec<Pool> = pools.iter().filter(|pool| kept(pool)).cloned().collect();
                for turn in 0..32 {
                    let request = PoolRequest::new(&cx, &Encoded, HOME, now)
                        .with_excluded(excluded)
                        .with_damped(&damped)
                        .with_turn(turn);
                    let whole = HomeFirst
                        .choose_where(request, &pools, kept)
                        .map(|at| pools[at].name.clone());
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

    /// `HomeFirst`'s choice, with its thresholds as constants in the code,
    /// is the choice at the same thresholds read as data, turn for turn:
    /// for every subset of pools of mixed state, region, load, and
    /// freshness, with pools excluded, damped, gone, and gone in a follow
    /// since changed. `HomeFirstAt::DEFAULT` takes `HomeFirst`'s code, so
    /// the data path is reached here only through `choose_in`.
    #[test]
    fn the_defaults_choose_alike_as_constants_and_as_data() {
        let now = origin();
        let pools = [
            pool("home-1", HOME, PoolState::Ready, 1, now),
            busy("full", HOME, PoolState::Ready, LIMIT, 0, now),
            busy("refused", HOME, PoolState::Ready, 10, 1, now),
            busy("away", AWAY, PoolState::Ready, 10, 0, now),
            busy("away-full", AWAY, PoolState::Ready, LIMIT * 8 / 10, 0, now),
            busy("home-2", HOME, PoolState::Ready, 40, 0, now),
            pool("degraded", HOME, PoolState::Degraded, 1, now),
            pool_loaded(
                "stale",
                HOME,
                PoolState::Ready,
                1,
                load(0, 0, now - PERIOD * 4),
            ),
            pool("gone", HOME, PoolState::Ready, 1, now),
            pool("moved", AWAY, PoolState::Ready, 1, now).with_follow(0, 2),
        ];
        let at = |name: &str| pools.iter().position(|pool| pool.name == name);
        let damped = Damped::new([(id("home-2"), now + SECOND)])
            .with_gone(&pools[at("gone").expect("gone")])
            .with_gone(&pool("moved", AWAY, PoolState::Ready, 1, now).with_follow(0, 1));
        assert!(damped.is_gone(&pools[at("gone").expect("gone")]));
        assert!(!damped.is_gone(&pools[at("moved").expect("moved")]));
        let data = std::hint::black_box(HomeFirstAt::DEFAULT);
        let cx = Cx::new();
        for (excluded, damped) in [
            (&[][..], &Damped::EMPTY),
            (&[][..], &damped),
            (&[id("home-1")][..], &damped),
        ] {
            for mask in (0u32..1 << pools.len()).step_by(7) {
                let kept = |pool: &Pool| at(&pool.name).is_some_and(|at| mask & 1 << at != 0);
                for turn in 0..16 {
                    let request = PoolRequest::new(&cx, &Encoded, HOME, now)
                        .with_excluded(excluded)
                        .with_damped(damped)
                        .with_turn(turn);
                    let constants = HomeFirst.choose_in(request, &pools, kept);
                    assert_eq!(
                        data.choose_in(request, &pools, kept),
                        constants,
                        "mask {mask:010b}, turn {turn}"
                    );
                    assert_eq!(data.choose_where(request, &pools, kept), constants);
                }
            }
        }
        for pool in &pools {
            assert_eq!(
                HomeFirst.eligible(pool, HOME, now),
                data.eligible(pool, HOME, now)
            );
        }
    }

    /// The damping cap counts only the kept pools: a lone kept pool is
    /// not damped out by pools the filter drops, and its index is into
    /// all the pools.
    #[test]
    fn choose_where_damps_among_the_kept_pools_only() {
        let now = origin();
        let pools = [
            pool("b", HOME, PoolState::Ready, 1, now),
            pool("c", HOME, PoolState::Ready, 1, now),
            pool("a", HOME, PoolState::Ready, 1, now),
        ];
        let damped = Damped::new([(id("a"), now + SECOND)]);
        let cx = Cx::new();
        let request = PoolRequest::new(&cx, &(), HOME, now).with_damped(&damped);
        assert_eq!(
            HomeFirst.choose_where(request, &pools, |pool| pool.name == "a"),
            Some(2)
        );
        assert_eq!(HomeFirst.choose_where(request, &pools, |_| false), None);
    }

    /// A pool is eligible while it is in a tier, damped or not.
    #[test]
    fn eligible_is_in_a_tier() {
        let now = origin();
        assert!(HomeFirst.eligible(&pool("a", HOME, PoolState::Ready, 1, now), HOME, now));
        assert!(HomeFirst.eligible(&pool("a", AWAY, PoolState::Degraded, 1, now), HOME, now));
        assert!(!HomeFirst.eligible(&pool("a", HOME, PoolState::Draining, 1, now), HOME, now));
        assert!(!HomeFirst.eligible(&pool("a", HOME, PoolState::Ready, 0, now), HOME, now));
        let far = busy("far", AWAY, PoolState::Ready, LIMIT, 0, now);
        assert!(!HomeFirst.eligible(&far, HOME, now));
        assert!(!HomeFirst.eligible(
            &pool("a", HOME, PoolState::Ready, 1, now),
            HOME,
            now + PERIOD * 4
        ));
    }

    /// A home pool gone in its follow is not chosen, and the request goes
    /// to another region's pool with room, as for a home pool that cannot
    /// take it; a lone gone pool is not chosen at all. Once its follow
    /// changes, it is chosen again.
    #[test]
    fn a_gone_pool_is_not_chosen_until_its_follow_changes() {
        let now = Instant::now();
        let pools = [
            busy("home", HOME, PoolState::Ready, 0, 0, now),
            busy("away", AWAY, PoolState::Ready, 0, 0, now),
        ];
        let gone = Damped::EMPTY.with_gone(&pools[0]);
        assert_eq!(chosen_damped(&pools, &gone, now), names(&["away"]));
        assert_eq!(chosen_damped(&pools[..1], &gone, now), names(&[]));
        let changed = [pools[0].clone().with_follow(0, 1)];
        assert_eq!(chosen_damped(&changed, &gone, now), names(&["home"]));
    }
}
