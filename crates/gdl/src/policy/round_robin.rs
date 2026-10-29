use super::{Damped, NoPool, Pool, PoolChoice, PoolPolicy, PoolRequest, PoolState, cut};
use crate::select::Encoded;

/// `round_robin`: the pools the chooser follows take turns, whatever their
/// load and region.
///
/// A pool is eligible only while it is `ready` or `degraded`, has an entry
/// instance to route to (a pool that is not offered, or unread, has none),
/// has not declined the request, and has not declined an offer as gone in
/// the follow it is shown in ([`Damped::is_gone`]). Damping comes first,
/// then the rank:
///
/// 1. A pool the chooser damps is passed over as `home_first` passes it
///    over: at most half of the eligible pools, `ready` and `degraded`
///    together, rounded down, are passed over at once, those whose windows
///    end soonest being chosen as if they were not, so a lone pool is
///    never damped out.
/// 2. Of the pools left, the `ready` ones take every turn while any is
///    left, and the `degraded` ones only when none is.
///
/// So a damped `ready` pool that is passed over leaves its turns to a
/// `degraded` one: a pool that just declined does not fit, and
/// damping stays as it is for every policy.
///
/// The attempt's turn is the crossing's count of the attempts before it:
/// the `turn`-th choosable pool, modulo how many there are, in the order
/// of the table, which is by class and pool. So the turns of one chooser's
/// hand-off go round its pools one by one, and a turn needs no lock and no
/// shared position but that count. "The next pool" is the next turn among
/// the pools still eligible, not the pool after the last one chosen: every
/// attempt takes a turn, a failover after a decline included, and a pool
/// the request excluded no longer counts. With pools A, B, and C, if A
/// declines the request at turn 0, its failover's turn 1 among B and C
/// goes to C. A pool that leaves or joins shifts which pool the next turn
/// lands on, which a rotation does not need to avoid.
#[derive(Debug, Clone, Copy, Default)]
pub struct RoundRobin;

impl RoundRobin {
    /// `round_robin`'s choice among only the pools `keep` keeps, as if
    /// the others were not there: the index of the chosen pool in
    /// `pools`, or `None` when no kept pool is eligible. The turn goes
    /// round the kept pools, and the damping cap counts only them. A
    /// policy that filters the pools, by property for instance, takes turns
    /// over the rest with this, and allocates nothing. It reads no head,
    /// so it takes a request of any head.
    ///
    /// `keep` must answer for a pool from that pool alone: alike each
    /// time, whatever other pools it was called for, and in any order. It
    /// is called again for a pool within one choice.
    ///
    /// Two passes over the table and no allocation while no pool's damping
    /// window lasts: one counts the choosable pools of each rank, the other
    /// finds the turn's. While a window lasts, a pass first sets which
    /// damped pools are passed over, as in `home_first`. The damping is one
    /// moment's view, so the passes agree whatever other requests decline
    /// or accept meanwhile.
    pub fn choose_where<H>(
        &self,
        request: PoolRequest<'_, H>,
        pools: &[Pool],
        keep: impl Fn(&Pool) -> bool,
    ) -> Option<usize> {
        let (excluded, damped, now) = (request.excluded(), request.damped(), request.now());
        let lasts = damped.lasts(now);
        let cut = if lasts {
            let eligible = |pool: &Pool| Self::rank(pool, excluded, damped).is_some() && keep(pool);
            cut(pools, eligible, damped, now)
        } else {
            0
        };
        let rank = |pool: &Pool| {
            Self::rank(pool, excluded, damped)
                .filter(|_| keep(pool) && (!lasts || damped.left(pool.id, now) <= cut))
        };
        let mut counts = [0u64; 2];
        for pool in pools {
            if let Some(rank) = rank(pool) {
                counts[rank] += 1;
            }
        }
        let best = usize::from(counts[0] == 0);
        let mut nth = request.turn().checked_rem(counts[best])?;
        for (at, pool) in pools.iter().enumerate() {
            if rank(pool) != Some(best) {
                continue;
            }
            if nth == 0 {
                return Some(at);
            }
            nth -= 1;
        }
        None
    }

    /// `pool`'s rank, `ready` before `degraded`, or `None` when it may not
    /// be chosen for this request. Inlined into the choice's passes: out
    /// of line, a pick paid a call for each pool in each pass.
    #[inline]
    fn rank(pool: &Pool, excluded: &[u64], damped: &Damped) -> Option<usize> {
        if pool.entries == 0 || excluded.contains(&pool.id) || damped.is_gone(pool) {
            return None;
        }
        match pool.state {
            PoolState::Ready => Some(0),
            PoolState::Degraded => Some(1),
            PoolState::Draining | PoolState::Unavailable => None,
        }
    }
}

impl PoolPolicy for RoundRobin {
    /// It reads no request data.
    type Head = Encoded;

    /// [`choose_where`](RoundRobin::choose_where), keeping every pool, with
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::super::{Damped, pool_id};
    use super::*;
    use crate::cx::Cx;

    const SECOND: Duration = Duration::from_secs(1);

    /// The pool `name`, in `state`, with `entries` entry instances and no
    /// load summary: round robin reads none.
    fn pool(name: &str, state: PoolState, entries: usize) -> Pool {
        Pool::new("model_x", name, "anywhere", state).with_entries(entries)
    }

    fn ready(name: &str) -> Pool {
        pool(name, PoolState::Ready, 1)
    }

    /// The names of the pools chosen for the turns `turns`, `-` for none.
    fn turns(
        pools: &[Pool],
        excluded: &[u64],
        damped: &Damped,
        now: Instant,
        turns: std::ops::Range<u64>,
    ) -> Vec<String> {
        let cx = Cx::new();
        turns
            .map(|turn| {
                let request = PoolRequest::new(&cx, &Encoded, "here", now)
                    .with_excluded(excluded)
                    .with_damped(damped)
                    .with_turn(turn);
                RoundRobin
                    .choose(request, pools)
                    .ok()
                    .map(|choice| choice.at())
                    .map_or_else(|| "-".to_string(), |at| pools[at].name.clone())
            })
            .collect()
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    /// Turn after turn, the next pool in table order, round and round,
    /// with no load summary and whatever the region.
    #[test]
    fn each_turn_takes_the_next_pool() {
        let pools = [ready("m1"), ready("m2"), ready("m3")];
        let now = Instant::now();
        assert_eq!(
            turns(&pools, &[], &Damped::default(), now, 0..7),
            names(&["m1", "m2", "m3", "m1", "m2", "m3", "m1"])
        );
        // A turn far along the count is still a turn: `u64::MAX` is a
        // multiple of 3.
        assert_eq!(
            turns(&pools, &[], &Damped::default(), now, u64::MAX - 3..u64::MAX),
            names(&["m1", "m2", "m3"])
        );
    }

    /// A pool that may not be chosen takes no turn: `unavailable`,
    /// `draining`, with nothing to route to (not offered, unread, or no
    /// entry instance), or excluded for the request.
    #[test]
    fn only_eligible_pools_take_turns() {
        let pools = [
            ready("m1"),
            pool("m2", PoolState::Unavailable, 1),
            pool("m3", PoolState::Draining, 1),
            pool("m4", PoolState::Ready, 0),
            ready("m5"),
            ready("m6"),
        ];
        let now = Instant::now();
        let none = Damped::default();
        assert_eq!(
            turns(&pools, &[], &none, now, 0..6),
            names(&["m1", "m5", "m6", "m1", "m5", "m6"])
        );
        let excluded = [pool_id("model_x", "m5")];
        assert_eq!(
            turns(&pools, &excluded, &none, now, 0..4),
            names(&["m1", "m6", "m1", "m6"])
        );
        for at in [1, 2, 3] {
            assert!(!RoundRobin.eligible(&pools[at], "here", now), "{at}");
        }
        assert!(RoundRobin.eligible(&pools[0], "here", now));
        assert!(RoundRobin.eligible(&pool("d", PoolState::Degraded, 1), "here", now));
        let excluded = [
            pool_id("model_x", "m1"),
            pool_id("model_x", "m6"),
            pool_id("model_x", "m5"),
        ];
        assert_eq!(
            turns(&pools, &excluded, &none, now, 0..2),
            names(&["-", "-"])
        );
        assert_eq!(turns(&[], &[], &none, now, 0..1), names(&["-"]));
    }

    /// A `degraded` pool takes a turn only while no pool is `ready`.
    #[test]
    fn a_degraded_pool_only_while_no_pool_is_ready() {
        let now = Instant::now();
        let none = Damped::default();
        let mixed = [
            pool("d1", PoolState::Degraded, 1),
            ready("m1"),
            pool("d2", PoolState::Degraded, 1),
            ready("m2"),
        ];
        assert_eq!(
            turns(&mixed, &[], &none, now, 0..4),
            names(&["m1", "m2", "m1", "m2"])
        );
        let excluded = [pool_id("model_x", "m1"), pool_id("model_x", "m2")];
        assert_eq!(
            turns(&mixed, &excluded, &none, now, 0..4),
            names(&["d1", "d2", "d1", "d2"])
        );
        let degraded = [
            pool("d1", PoolState::Degraded, 1),
            pool("m1", PoolState::Ready, 0),
            pool("u1", PoolState::Unavailable, 1),
        ];
        assert_eq!(
            turns(&degraded, &[], &none, now, 0..2),
            names(&["d1", "d1"])
        );
    }

    /// `choose_where` is `choose` over a copy of the pools it keeps, turn
    /// for turn, with its index into all of them: for every subset of
    /// pools of mixed state, with one damped and one excluded.
    #[test]
    fn choose_where_is_choose_over_the_kept_pools() {
        let pools = [
            ready("m1"),
            pool("d1", PoolState::Degraded, 1),
            ready("m2"),
            pool("u1", PoolState::Unavailable, 1),
            ready("m3"),
            pool("d2", PoolState::Degraded, 1),
        ];
        let now = Instant::now();
        let damped = Damped::new([(pool_id("model_x", "m2"), now + SECOND)]);
        let cx = Cx::new();
        for excluded in [&[][..], &[pool_id("model_x", "m1")][..]] {
            for mask in 0u32..1 << pools.len() {
                let kept = |pool: &Pool| {
                    let at = pools.iter().position(|of| of.name == pool.name);
                    at.is_some_and(|at| mask & 1 << at != 0)
                };
                let copy: Vec<Pool> = pools.iter().filter(|pool| kept(pool)).cloned().collect();
                for turn in 0..16 {
                    let request = PoolRequest::new(&cx, &Encoded, "here", now)
                        .with_excluded(excluded)
                        .with_damped(&damped)
                        .with_turn(turn);
                    let whole = RoundRobin
                        .choose_where(request, &pools, kept)
                        .map(|at| pools[at].name.clone());
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

    /// The turns go round the kept pools only, and the damping cap counts
    /// only them: a lone kept pool is not damped out by pools the filter
    /// drops, and its index is into all the pools.
    #[test]
    fn choose_where_takes_turns_among_the_kept_pools_only() {
        let pools = [ready("b"), ready("c"), ready("a"), ready("e")];
        let now = Instant::now();
        let damped = Damped::new([(pool_id("model_x", "a"), now + SECOND)]);
        let cx = Cx::new();
        let chosen = |keep: &dyn Fn(&Pool) -> bool, turns: std::ops::Range<u64>| -> Vec<usize> {
            turns
                .filter_map(|turn| {
                    let request = PoolRequest::new(&cx, &(), "here", now)
                        .with_damped(&damped)
                        .with_turn(turn);
                    RoundRobin.choose_where(request, &pools, keep)
                })
                .collect()
        };
        assert_eq!(chosen(&|pool| pool.name == "a", 0..3), [2, 2, 2]);
        assert_eq!(
            chosen(&|pool| pool.name != "c", 0..4),
            [0, 3, 0, 3],
            "the damped pool is passed over among three"
        );
        assert!(chosen(&|_| false, 0..2).is_empty());
    }

    /// A damped pool takes no turn while its window lasts, but at most half
    /// of the eligible pools are passed over, so a lone pool, or one of
    /// two, is never damped out; the one whose window ends soonest is
    /// chosen as if it were not damped. A window that has passed passes
    /// nothing over.
    #[test]
    fn a_damped_pool_takes_no_turn_while_others_can() {
        let pools = [ready("m1"), ready("m2"), ready("m3")];
        let now = Instant::now();
        let damped = Damped::new([(pools[1].id(), now + SECOND)]);
        assert_eq!(
            turns(&pools, &[], &damped, now, 0..4),
            names(&["m1", "m3", "m1", "m3"])
        );
        assert_eq!(
            turns(&pools, &[], &damped, now + SECOND * 2, 0..3),
            names(&["m1", "m2", "m3"]),
            "a passed window"
        );
        // Two of three damped: one may be passed over, the one whose
        // window lasts longest.
        let damped = Damped::new([
            (pools[1].id(), now + SECOND),
            (pools[0].id(), now + SECOND * 3 / 2),
        ]);
        assert_eq!(
            turns(&pools, &[], &damped, now + SECOND / 2, 0..4),
            names(&["m2", "m3", "m2", "m3"])
        );
        // A lone damped pool is still chosen.
        let lone = [ready("m2")];
        assert_eq!(turns(&lone, &[], &damped, now, 0..2), names(&["m2", "m2"]));
        // A damped ready pool passed over leaves a degraded one its turns,
        // as it does in `home_first`, which caps over every eligible pool.
        let mixed = [ready("m2"), pool("d1", PoolState::Degraded, 1)];
        assert_eq!(turns(&mixed, &[], &damped, now, 0..2), names(&["d1", "d1"]));
    }

    /// A pool gone in its follow takes no turn, however many pools are,
    /// so a lone one is not chosen, and it does not count in the damping
    /// cap: a damped pool beside a gone one is the lone eligible pool, and
    /// is chosen. Once its follow changes, it is not gone, and takes its
    /// turns again, as it does once its chooser no longer holds it gone.
    #[test]
    fn a_gone_pool_takes_no_turn_until_its_follow_changes() {
        let pools = [ready("m1"), ready("m2"), ready("m3")];
        let now = Instant::now();
        let gone = Damped::EMPTY.with_gone(&pools[1]);
        assert!(gone.is_gone(&pools[1]) && !gone.is_damped(pools[1].id()));
        assert_eq!(
            turns(&pools, &[], &gone, now, 0..4),
            names(&["m1", "m3", "m1", "m3"])
        );
        let lone = [ready("m2")];
        assert_eq!(turns(&lone, &[], &gone, now, 0..2), names(&["-", "-"]));
        let both = Damped::new([(pools[0].id(), now + SECOND)]).with_gone(&pools[1]);
        assert_eq!(
            turns(&pools[..2], &[], &both, now, 0..2),
            names(&["m1", "m1"]),
            "the cap counted the gone pool"
        );
        let changed = [ready("m1"), ready("m2").with_follow(0, 1), ready("m3")];
        assert!(!gone.is_gone(&changed[1]));
        assert_eq!(
            turns(&changed, &[], &gone, now, 0..3),
            names(&["m1", "m2", "m3"])
        );
        let generation = [ready("m1"), ready("m2").with_follow(1, 0), ready("m3")];
        assert!(!gone.is_gone(&generation[1]));
        assert_eq!(
            turns(&generation, &[], &Damped::default(), now, 0..3),
            names(&["m1", "m2", "m3"])
        );
    }
}
