//! A chooser's damping of a pool that declined its offers: for a short
//! window after a decline, the pool is passed over, so that requests do not
//! each pay a round trip to a pool that is full until its summary says so.
//! The window starts at the chooser's first window (`pool_damping_first_ms`,
//! 1 s by default) and doubles, up to its longest (`pool_damping_max_ms`,
//! 30 s by default), with each further decline with no acceptance between;
//! an offer the pool accepts ends its damping. As with Envoy's outlier
//! ejection, whose ejection time grows with consecutive ejections, the
//! window grows while the pool keeps declining, and is capped so that a
//! pool is tried again within a bounded time.
//!
//! A decline that lands while a window lasts is part of the event that
//! began it, and leaves the window as it is: its offer was chosen before
//! the window began, or because the cap let a damped pool through. So the
//! declines of many requests that met one full pool at once are one
//! decline, and the window doubles only on a decline of an offer made
//! once the last window had passed. Extending the window on each of them
//! would put a pool that was full for a moment out for the longest window
//! after a burst of six.
//!
//! A chooser keeps the pools it damps as one immutable list, swapped whole
//! on each change, as its tables are: a choice reads the list once, so the
//! cap on how many pools are damped and the pools it draws from are judged
//! from one moment, whatever other requests decline or accept meanwhile.
//! Damping is rare, so the list is short, usually empty; a change makes a
//! new one, and only a decline, or an acceptance by a damped pool, makes
//! one.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use gdl::__runtime::Window;
use gdl::Damped;
use tokio::time::Instant;

use crate::RuntimeConfig;

/// One chooser instance's damping of the pools it sends to, by pool
/// [`id`](super::PoolTable::id): the same in every table of a pool, so it
/// outlives a new snapshot or generation of the pool, and apart from the
/// tables, which the follower makes.
///
/// A pool is damped from a decline until an acceptance: while its window
/// lasts it is passed over (at most half of the eligible pools are, so a
/// lone pool is still chosen), and once the window has passed it may be chosen
/// again, which is how it is offered the request whose acceptance ends its
/// damping, or whose decline doubles the window.
#[derive(Debug)]
pub(crate) struct Damping {
    view: ArcSwap<Damped>,
    /// The window after a pool's first decline.
    first: Duration,
    /// The longest window: a pool that keeps declining is tried again at
    /// least this often.
    max: Duration,
}

impl Default for Damping {
    fn default() -> Self {
        Self::new(
            RuntimeConfig::DEFAULT_POOL_DAMPING_FIRST,
            RuntimeConfig::DEFAULT_POOL_DAMPING_MAX,
        )
    }
}

impl Damping {
    /// No pool damped, with windows from `first` up to `max`.
    pub(crate) fn new(first: Duration, max: Duration) -> Self {
        Self {
            view: ArcSwap::default(),
            first,
            max,
        }
    }

    /// The damped pools now, for one choice: one load, no lock, no
    /// allocation.
    pub(crate) fn view(&self) -> Guard<Arc<Damped>> {
        self.view.load()
    }

    /// The pool `pool` declined an offer at `now`: damp it for the first
    /// window if it was not damped, for twice its last window (at most the
    /// longest) if that window has passed, and leave a window that still
    /// lasts as it is. A pool with a mark as gone keeps it: with no table
    /// of the pool to compare, the refusal cannot show that the mark is
    /// stale, and the follower forgets a stale one.
    pub(crate) fn decline(&self, pool: u64, now: Instant) {
        self.change(|damped| {
            if damped.holds_gone(pool) {
                return None;
            }
            self.declined(damped, pool, now)
        });
    }

    /// The pool `pool`, as the chooser's table shows it, declined an
    /// offer as full: as [`Damping::decline`], unless the pool is gone in
    /// that follow or a later one. A full refusal of an offer made before
    /// the pool said it is gone says nothing newer, and must not end the
    /// mark, even when it read an earlier table.
    pub(crate) fn decline_shown(&self, pool: &gdl::Pool, now: Instant) {
        self.change(|damped| {
            if damped.holds_gone_since(pool) {
                return None;
            }
            self.declined(damped, pool.id(), now)
        });
    }

    /// The pool `pool`, as a choice sees it now, declined an offer as
    /// gone: pass it over until its follow changes, in place of any
    /// window it had. A pool already gone in that follow, or a later one,
    /// writes nothing: a decline that read an earlier table must not put
    /// back an earlier stamp.
    pub(crate) fn gone(&self, pool: &gdl::Pool) {
        self.change(|damped| (!damped.holds_gone_since(pool)).then(|| damped.with_gone(pool)));
    }

    /// Forget each mark as gone that `keep` refuses, given the pool and
    /// the generation and count of changes it declined in: the follower
    /// refuses those whose follow changed or is no longer held. Such a
    /// mark already matches no pool; forgetting it keeps a choice on the
    /// path that reads nothing for gone pools. Nothing is written while
    /// no mark is refused.
    pub(crate) fn forget_gone(&self, keep: impl Fn(u64, u64, u64) -> bool) {
        self.change(|damped| damped.retain_gone(&keep));
    }

    /// The pool `pool` accepted an offer: its damping, or its mark as
    /// gone, ends. A pool that is neither writes nothing, and costs one
    /// load and a search of an empty or short list.
    pub(crate) fn accept(&self, pool: u64) {
        self.change(|damped| damped.without(pool));
    }

    /// Replace the list with what `change` makes of it, unless it makes
    /// nothing, as one compare-and-swap that is tried again when another
    /// change came between.
    fn change(&self, change: impl Fn(&Damped) -> Option<Damped>) {
        let mut current = self.view.load();
        loop {
            let Some(next) = change(&current) else {
                return;
            };
            let previous = self.view.compare_and_swap(&*current, Arc::new(next));
            if Arc::ptr_eq(&*previous, &*current) {
                wrote();
                return;
            }
            current = previous;
        }
    }

    /// The list once `pool` declined at `now`, or `None` when it does not
    /// change: the pool's window still lasts.
    fn declined(&self, damped: &Damped, pool: u64, now: Instant) -> Option<Damped> {
        let length = match damped.window(pool) {
            None => self.first,
            Some(window) if window.until > now => return None,
            Some(window) => (window.length * 2).min(self.max),
        };
        let window = Window {
            until: now + length,
            length,
        };
        Some(damped.with(pool, window))
    }
}

#[cfg(test)]
thread_local! {
    /// The changes of damping state this thread made, so that a test counts
    /// them.
    pub(super) static WRITES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn wrote() {
    #[cfg(test)]
    WRITES.with(|writes| writes.set(writes.get() + 1));
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);
    const POOL: u64 = 7;

    /// The window after each decline that falls when the last one passed:
    /// 1, 2, 4, 8, 16 s, then 30 s, the cap, from then on.
    #[test]
    fn the_window_doubles_with_each_decline_up_to_the_cap() {
        let start = Instant::now();
        let damping = Damping::default();
        assert!(!damping.view().is_damped(POOL));
        assert_eq!(damping.view().left(POOL, start), 0);
        let mut now = start;
        for window in [1, 2, 4, 8, 16, 30, 30] {
            damping.decline(POOL, now);
            assert!(damping.view().is_damped(POOL));
            assert_eq!(
                damping.view().left(POOL, now),
                window * 1000,
                "at {:?}",
                now - start
            );
            now += SECOND * u32::try_from(window).expect("short");
            assert_eq!(damping.view().left(POOL, now), 0);
            assert!(!damping.view().lasts(now));
            assert!(
                damping.view().is_damped(POOL),
                "a passed window ended the damping"
            );
        }
    }

    /// Windows set apart from the defaults start and stop where they say:
    /// 2 s, then 4 s, then 5 s, the cap, from then on.
    #[test]
    fn the_windows_are_the_ones_the_chooser_is_given() {
        let start = Instant::now();
        let damping = Damping::new(SECOND * 2, SECOND * 5);
        let mut now = start;
        for window in [2, 4, 5, 5] {
            damping.decline(POOL, now);
            assert_eq!(damping.view().left(POOL, now), window * 1000);
            now += SECOND * u32::try_from(window).expect("short");
        }
    }

    /// A decline while the window lasts leaves it; an acceptance ends the
    /// damping, and the next decline starts again at the first window.
    #[test]
    fn a_decline_within_the_window_leaves_it_and_an_acceptance_ends_it() {
        let start = Instant::now();
        let damping = Damping::default();
        damping.decline(POOL, start);
        damping.decline(POOL, start + SECOND / 2);
        assert_eq!(damping.view().left(POOL, start + SECOND / 2), 500);
        damping.decline(POOL, start + SECOND);
        assert_eq!(damping.view().left(POOL, start + SECOND), 2000);
        damping.accept(POOL);
        assert!(!damping.view().is_damped(POOL));
        assert_eq!(damping.view().left(POOL, start + SECOND), 0);
        damping.decline(POOL, start + SECOND * 2);
        assert_eq!(damping.view().left(POOL, start + SECOND * 2), 1000);
    }

    /// Declines that land while one window lasts, as when many requests
    /// in flight met one full pool at once, are one decline: the window
    /// ends where the first one put it, and the next decline, after it,
    /// doubles it once. Neither they nor an acceptance by a pool that is
    /// not damped change the list.
    #[test]
    fn declines_that_land_during_one_window_are_one_decline() {
        let writes = || WRITES.with(std::cell::Cell::get);
        let start = Instant::now();
        let damping = Damping::default();
        let wrote = writes();
        for at in 0..10 {
            damping.decline(POOL, start + SECOND / 10 * at);
        }
        damping.accept(POOL + 1);
        assert_eq!(writes() - wrote, 1);
        assert_eq!(damping.view().left(POOL, start), 1000);
        assert_eq!(damping.view().left(POOL, start + SECOND), 0);
        damping.decline(POOL, start + SECOND);
        assert_eq!(damping.view().left(POOL, start + SECOND), 2000);
    }

    /// A view is the list as it was when it was read: a change after it
    /// makes a new one, and leaves the view as it was.
    #[test]
    fn a_view_is_one_moment_of_the_damping() {
        let start = Instant::now();
        let damping = Damping::default();
        let before = damping.view();
        damping.decline(POOL, start);
        assert!(!before.is_damped(POOL));
        assert!(damping.view().is_damped(POOL));
        assert!(damping.view().lasts(start));
        damping.decline(POOL + 1, start + SECOND / 2);
        let both = damping.view();
        damping.accept(POOL);
        assert!(both.is_damped(POOL) && both.is_damped(POOL + 1));
        assert!(!damping.view().is_damped(POOL));
        assert!(damping.view().lasts(start + SECOND));
    }

    /// What `home_first` makes of the damping as the chooser keeps it,
    /// as declines and acceptances change it.
    mod chosen {
        use std::collections::BTreeSet;
        use std::sync::atomic::{AtomicBool, Ordering};

        use gdl::{Cx, Encoded, HomeFirst, Load, Pool, PoolPolicy, PoolRequest, PoolState};
        use gdl_pools::LoadSummary;

        use super::super::super::pool_id;
        use super::*;

        const HOME: &str = "us-east-1";
        const AWAY: &str = "us-west-2";

        /// A ready pool of `model_x` in `region` with `admitted` of 100
        /// roots, whose summary arrived at `now`.
        fn busy(name: &str, region: &str, admitted: u64, now: Instant) -> Pool {
            Pool::new("model_x", name, region, PoolState::Ready)
                .with_entries(1)
                .with_load(Load::new(
                    LoadSummary {
                        admitted,
                        limit: 100,
                        instances: 1,
                        refusals: 0,
                        period_ms: 10_000,
                    },
                    now,
                ))
        }

        fn pool(name: &str, now: Instant) -> Pool {
            busy(name, HOME, 0, now)
        }

        fn id(name: &str) -> u64 {
            pool_id("model_x", name)
        }

        fn choose(pools: &[Pool], damped: &Damped, now: Instant, turn: u64) -> Option<usize> {
            let cx = Cx::new();
            let request = PoolRequest::new(&cx, &Encoded, HOME, now)
                .with_damped(damped)
                .with_turn(turn);
            HomeFirst
                .choose(request, pools)
                .ok()
                .map(|choice| choice.at())
        }

        /// The pools chosen at `now` over many turns, by name.
        fn chosen(pools: &[Pool], damped: &Damped, now: Instant) -> BTreeSet<String> {
            (0..256)
                .filter_map(|turn| choose(pools, damped, now, turn))
                .map(|at| pools[at].name().to_string())
                .collect()
        }

        fn names(names: &[&str]) -> BTreeSet<String> {
            names.iter().map(ToString::to_string).collect()
        }

        /// A pool that declined is not chosen while its window lasts, here
        /// 1 s, and is once it has passed; a decline then doubles the
        /// window; an acceptance ends the damping at once. A damped home
        /// pool sends the request elsewhere, while elsewhere has room.
        #[test]
        fn a_pool_that_declined_is_not_chosen_while_its_window_lasts() {
            let now = Instant::now();
            let damping = Damping::default();
            let pools = [pool("a", now), pool("b", now), pool("c", now)];
            let at = |when: Instant| chosen(&pools, &damping.view(), when);
            let every = names(&["a", "b", "c"]);
            let just = Duration::from_millis(1);
            damping.decline(id("a"), now);
            assert_eq!(at(now), names(&["b", "c"]));
            assert_eq!(at(now + SECOND - just), names(&["b", "c"]));
            assert_eq!(at(now + SECOND), every);
            damping.decline(id("a"), now + SECOND);
            assert_eq!(at(now + SECOND * 3 - just), names(&["b", "c"]));
            assert_eq!(at(now + SECOND * 3), every);
            damping.decline(id("a"), now + SECOND * 3);
            assert_eq!(at(now + SECOND * 3), names(&["b", "c"]));
            damping.accept(id("a"));
            assert_eq!(at(now + SECOND * 3), every);

            let pools = [busy("home", HOME, 0, now), busy("away", AWAY, 0, now)];
            damping.decline(id("home"), now);
            assert_eq!(chosen(&pools, &damping.view(), now), names(&["away"]));
        }

        /// A pool damped or undamped while choices are made, as other
        /// requests' declines and acceptances do, never leaves a choice
        /// with no pool while another stays eligible, nor a lone pool
        /// unchosen: here the ready pool's damping changes on another
        /// thread throughout, beside a degraded pool that is always there
        /// to take the request, and alone. Each choice reads the damping
        /// as a chooser does, once. Counts the choices that found no pool.
        #[test]
        fn a_damping_that_changes_during_a_choice_leaves_it_a_pool() {
            let now = Instant::now();
            let damping = Damping::default();
            let degraded = Pool::new("model_x", "degraded", HOME, PoolState::Degraded)
                .with_entries(1)
                .with_load(*pool("x", now).load().expect("a load"));
            let pools = [pool("ready", now), degraded];
            for choosable in [&pools[..], &pools[..1]] {
                let done = AtomicBool::new(false);
                let empty = std::thread::scope(|scope| {
                    scope.spawn(|| {
                        while !done.load(Ordering::Relaxed) {
                            damping.decline(id("ready"), now);
                            damping.accept(id("ready"));
                        }
                    });
                    let empty = (0..100_000)
                        .filter(|turn| choose(choosable, &damping.view(), now, *turn).is_none())
                        .count();
                    done.store(true, Ordering::Relaxed);
                    empty
                });
                assert_eq!(
                    empty,
                    0,
                    "of {} pools, choices that found none",
                    choosable.len()
                );
            }
        }

        /// A choice sees every pool's damping as it was at one moment, so
        /// the cap and the two choices are made over the same pools: with
        /// three eligible pools, at most one of them damped, so at least
        /// two are drawn from, and the heaviest never wins a pair. Here
        /// another thread damps all three, their windows ending in turn,
        /// and ends their damping, throughout. Counts the choices of the
        /// heaviest.
        #[test]
        fn a_choice_reads_one_moment_of_the_damping() {
            let now = Instant::now() + SECOND;
            let damping = Damping::default();
            let pools = [
                busy("heavy", HOME, 70, now),
                busy("light", HOME, 10, now),
                busy("middle", HOME, 40, now),
            ];
            let done = AtomicBool::new(false);
            let heaviest = std::thread::scope(|scope| {
                scope.spawn(|| {
                    while !done.load(Ordering::Relaxed) {
                        damping.decline(id("heavy"), now - SECOND / 2);
                        damping.decline(id("light"), now - SECOND / 4);
                        damping.decline(id("middle"), now);
                        for name in ["heavy", "light", "middle"] {
                            damping.accept(id(name));
                        }
                    }
                });
                let heaviest = (0..100_000)
                    .filter(|turn| choose(&pools, &damping.view(), now, *turn) == Some(0))
                    .count();
                done.store(true, Ordering::Relaxed);
                heaviest
            });
            assert_eq!(heaviest, 0, "choices of the heaviest pool");
        }

        /// The case without threads: a view read before three pools were
        /// damped, with windows ending 2, 4, and 8 s on, still holds none
        /// of them, and a choice by it never takes the heaviest; a choice
        /// by the view after leaves the two that end soonest to be drawn,
        /// and takes the lighter of them.
        #[test]
        fn a_view_read_before_a_change_is_chosen_by_as_it_was() {
            let now = Instant::now() + SECOND * 8;
            let damping = Damping::default();
            let pools = [
                busy("heavy", HOME, 70, now),
                busy("light", HOME, 10, now),
                busy("middle", HOME, 40, now),
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
            assert!(!chosen(&pools, &before, now).contains("heavy"));
            assert_eq!(chosen(&pools, &after, now), names(&["light"]));
        }
    }
}
