use std::time::Duration;

use tokio::time::Instant;

use super::Pool;

/// The pools a chooser damps, at one moment: each pool that declined an
/// offer and has accepted none since, by [`id`](super::Pool::id), with the
/// end of its window, while which a policy passes it over. Sorted by id,
/// with the latest end of the windows, so that a choice made when every
/// window has passed reads nothing more.
///
/// It also holds the pools that declined an offer as gone: a draining pool,
/// or one whose entry refuses by its own policy. Such a pool has no window.
/// It is passed over, by every policy that honors
/// [`PoolRequest::is_excluded`](super::PoolRequest::is_excluded), until its
/// follow changes: a new generation, or a new state or offered state
/// ([`Damped::is_gone`]).
///
/// The runtime keeps one per chooser and swaps it whole on each change, so
/// that a choice reads every pool's damping as it was at one moment. A
/// policy crate makes one with [`Damped::new`] and [`Damped::with_gone`]
/// for its own tests.
#[derive(Debug, Clone, Default)]
pub struct Damped {
    pools: Vec<(u64, Mark)>,
    latest: Option<Instant>,
    /// Whether any pool is gone, so that a choice while none is reads
    /// nothing more.
    gone: bool,
}

/// One damped pool's window: when it ends, and how long it was, which the
/// runtime doubles at the pool's next decline.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub until: Instant,
    pub length: Duration,
}

/// What a pool's decline left: a window, or the follow it declined as gone
/// in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mark {
    Window(Window),
    Gone(Follow),
}

/// A pool's follow as a chooser's table shows it, and how many times the
/// relay changed its state or offered state: what a decline as gone holds
/// until either changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Follow {
    generation: u64,
    changes: u64,
}

impl Follow {
    fn of(pool: &Pool) -> Self {
        Self {
            generation: pool.generation,
            changes: pool.changes,
        }
    }
}

impl Damped {
    /// No pool damped.
    pub const EMPTY: Self = Self {
        pools: Vec::new(),
        latest: None,
        gone: false,
    };

    /// The pools in `windows` damped, each until the instant given with
    /// its [`id`](super::Pool::id).
    pub fn new(windows: impl IntoIterator<Item = (u64, Instant)>) -> Self {
        let mut pools: Vec<(u64, Mark)> = windows
            .into_iter()
            .map(|(pool, until)| {
                let window = Window {
                    until,
                    length: Duration::ZERO,
                };
                (pool, Mark::Window(window))
            })
            .collect();
        pools.sort_by_key(|(id, _)| *id);
        pools.dedup_by_key(|(id, _)| *id);
        Self::of(pools)
    }

    /// Whether any pool's window lasts at `now`.
    #[inline]
    pub fn lasts(&self, now: Instant) -> bool {
        self.latest.is_some_and(|latest| now < latest)
    }

    /// Whether `pool` is damped: it declined, and has accepted nothing
    /// since, whether or not its window has passed. A pool that declined
    /// as gone is not damped: it has no window ([`Damped::is_gone`]).
    #[inline]
    pub fn is_damped(&self, pool: u64) -> bool {
        self.window(pool).is_some()
    }

    /// How long `pool`'s window still lasts at `now`, in milliseconds: `0`
    /// once it has passed, or when the pool is not damped.
    #[inline]
    pub fn left(&self, pool: u64, now: Instant) -> u64 {
        self.window(pool).map_or(0, |window| {
            let left = window.until.saturating_duration_since(now);
            u64::try_from(left.as_millis()).unwrap_or(u64::MAX)
        })
    }

    /// Whether `pool` declined an offer as gone in the follow it is shown
    /// in now, and has accepted nothing since: the relay has shown no new
    /// generation of it, and no new state or offered state, since it
    /// declined. A pool whose follow changed since is no longer gone. One
    /// branch while no pool is gone.
    #[inline]
    pub fn is_gone(&self, pool: &Pool) -> bool {
        self.gone && self.gone_in(pool)
    }

    /// Whether `pool` has a mark as gone, in whatever follow.
    #[doc(hidden)]
    #[inline]
    pub fn holds_gone(&self, pool: u64) -> bool {
        self.gone && matches!(self.mark(pool), Some(Mark::Gone(_)))
    }

    /// Whether `pool` is gone in the follow it is shown in, or in a later
    /// one: a decline as gone made from an earlier view of the pool must
    /// not replace it.
    #[doc(hidden)]
    #[inline]
    pub fn holds_gone_since(&self, pool: &Pool) -> bool {
        let shown = Follow::of(pool);
        self.gone
            && matches!(self.mark(pool.id), Some(Mark::Gone(held))
                if (held.generation, held.changes) >= (shown.generation, shown.changes))
    }

    /// The list with `pool` gone in the follow it is shown in, in place of
    /// any window it had.
    pub fn with_gone(&self, pool: &Pool) -> Self {
        self.marked(pool.id, Mark::Gone(Follow::of(pool)))
    }

    /// The list without each gone mark that `keep` refuses, given the
    /// pool's id and the generation and count of changes it declined in,
    /// or `None` when it refuses none. Windows stay.
    #[doc(hidden)]
    pub fn retain_gone(&self, keep: impl Fn(u64, u64, u64) -> bool) -> Option<Self> {
        let stale = |(id, mark): &(u64, Mark)| match mark {
            Mark::Gone(follow) => !keep(*id, follow.generation, follow.changes),
            Mark::Window(_) => false,
        };
        if !self.gone || !self.pools.iter().any(stale) {
            return None;
        }
        let pools = self
            .pools
            .iter()
            .filter(|pool| !stale(pool))
            .copied()
            .collect();
        Some(Self::of(pools))
    }

    /// How many pools are gone, current or not, for a test that checks
    /// that the runtime forgets marks whose follow changed.
    #[doc(hidden)]
    pub fn gone_marks(&self) -> usize {
        self.pools
            .iter()
            .filter(|(_, mark)| matches!(mark, Mark::Gone(_)))
            .count()
    }

    /// `pool`'s window, if it is damped.
    #[doc(hidden)]
    #[inline]
    pub fn window(&self, pool: u64) -> Option<Window> {
        match self.mark(pool)? {
            Mark::Window(window) => Some(window),
            Mark::Gone(_) => None,
        }
    }

    /// The list with `pool` damped for `window`, in place of any mark it
    /// had.
    #[doc(hidden)]
    pub fn with(&self, pool: u64, window: Window) -> Self {
        self.marked(pool, Mark::Window(window))
    }

    /// The list without `pool`, or `None` when it was neither damped nor
    /// gone.
    #[doc(hidden)]
    #[inline]
    pub fn without(&self, pool: u64) -> Option<Self> {
        let at = self.pools.binary_search_by_key(&pool, |(id, _)| *id).ok()?;
        let mut pools = self.pools.clone();
        pools.remove(at);
        Some(Self::of(pools))
    }

    /// [`is_gone`](Damped::is_gone) once some pool is gone: a search of the
    /// list, out of line, so that a policy's pass over its pools inlines
    /// only the branch.
    #[cold]
    #[inline(never)]
    fn gone_in(&self, pool: &Pool) -> bool {
        self.mark(pool.id) == Some(Mark::Gone(Follow::of(pool)))
    }

    #[inline]
    fn mark(&self, pool: u64) -> Option<Mark> {
        self.pools
            .binary_search_by_key(&pool, |(id, _)| *id)
            .ok()
            .map(|at| self.pools[at].1)
    }

    fn marked(&self, pool: u64, mark: Mark) -> Self {
        let mut pools = self.pools.clone();
        match pools.binary_search_by_key(&pool, |(id, _)| *id) {
            Ok(at) => pools[at].1 = mark,
            Err(at) => pools.insert(at, (pool, mark)),
        }
        Self::of(pools)
    }

    fn of(pools: Vec<(u64, Mark)>) -> Self {
        let latest = pools
            .iter()
            .filter_map(|(_, mark)| match mark {
                Mark::Window(window) => Some(window.until),
                Mark::Gone(_) => None,
            })
            .max();
        let gone = pools.iter().any(|(_, mark)| matches!(mark, Mark::Gone(_)));
        Self {
            pools,
            latest,
            gone,
        }
    }
}
