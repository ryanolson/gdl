//! A chooser's damping of a cell that declined its offers: for a short
//! window after a decline, the cell is not chosen, so that requests do not
//! each pay a round trip to a cell that is full until its summary says so.
//! The window starts at [`FIRST_WINDOW`] and doubles, up to
//! [`MAX_WINDOW`], with each further decline with no acceptance between;
//! an offer the cell accepts ends its damping. As with Envoy's outlier
//! ejection, whose ejection time grows with consecutive ejections, the
//! window grows while the cell keeps declining, and is capped so that a
//! cell is tried again within a bounded time.
//!
//! A decline that lands while a window lasts is part of the event that
//! began it, and leaves the window as it is: its offer was chosen before
//! the window began, or because the cap let a damped cell through. So the
//! declines of many requests that met one full cell at once are one
//! decline, and the window doubles only on a decline of an offer made
//! once the last window had passed. Extending the window on each of them
//! would put a cell that was full for a moment out for [`MAX_WINDOW`]
//! after a burst of six.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

/// The window after a cell's first decline.
pub(crate) const FIRST_WINDOW: Duration = Duration::from_secs(1);

/// The longest window: a cell that keeps declining is tried again at least
/// this often.
pub(crate) const MAX_WINDOW: Duration = Duration::from_secs(30);

/// The bits of the state that hold the end of the window, in milliseconds
/// since the damping's origin; the bits above hold the window's length in
/// milliseconds. One word, so that a reader never sees the end of one
/// window with the length of another.
const UNTIL: u64 = (1 << 48) - 1;

/// One cell's damping, as one chooser instance keeps it: shared by every
/// table of the cell, of every entry segment and every follow of it, so
/// that it outlives a new snapshot or generation of the cell.
///
/// The cell is damped from a decline until an acceptance: while its window
/// lasts it is not chosen, and once the window has passed it may be chosen
/// again, which is how it is offered the request whose acceptance ends its
/// damping, or whose decline doubles the window. A decline while the window
/// lasts leaves it as it is: such an offer was made before the cell was
/// damped, or because too many cells were, and one moment of fullness seen
/// by many requests at once is one decline, not many.
///
/// Reading it costs one atomic load, and no clock arithmetic while the
/// cell is not damped; it is written only on a decline, and on an
/// acceptance by a damped cell.
#[derive(Debug)]
pub(crate) struct Damping {
    /// The time the state's milliseconds count from.
    origin: Instant,
    /// `0` while not damped; otherwise the window's length in its high
    /// bits and its end in [`UNTIL`].
    state: AtomicU64,
}

impl Damping {
    /// An undamped cell's damping, counting time from `origin`.
    pub(crate) fn new(origin: Instant) -> Self {
        Self {
            origin,
            state: AtomicU64::new(0),
        }
    }

    /// Whether the cell is damped: it declined, and has accepted nothing
    /// since, whether or not its window has passed.
    pub(crate) fn is_damped(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }

    /// How long the cell's window still lasts at `now`, in milliseconds:
    /// `0` once it has passed, or when the cell is not damped.
    pub(crate) fn left(&self, now: Instant) -> u64 {
        let state = self.state.load(Ordering::Relaxed);
        if state == 0 {
            return 0;
        }
        (state & UNTIL).saturating_sub(self.millis(now))
    }

    /// The cell declined an offer at `now`: damp it for [`FIRST_WINDOW`]
    /// if it was not damped, for twice its last window (at most
    /// [`MAX_WINDOW`]) if that window has passed, and leave a window that
    /// still lasts as it is.
    pub(crate) fn decline(&self, now: Instant) {
        let now = self.millis(now);
        let first = millis(FIRST_WINDOW);
        let max = millis(MAX_WINDOW);
        let changed = self
            .state
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |state| {
                let window = match state {
                    0 => first,
                    _ if state & UNTIL > now => return None,
                    _ => (state >> 48).saturating_mul(2).min(max),
                };
                Some(window << 48 | (now.saturating_add(window) & UNTIL))
            });
        if changed.is_ok() {
            wrote();
        }
    }

    /// The cell accepted an offer: its damping ends.
    pub(crate) fn accept(&self) {
        self.state.store(0, Ordering::Relaxed);
        wrote();
    }

    /// `now` as milliseconds since the origin.
    fn millis(&self, now: Instant) -> u64 {
        millis(now.saturating_duration_since(self.origin))
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
thread_local! {
    /// The writes of damping state this thread made, so that a test counts
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

    /// The window after each decline that falls when the last one passed:
    /// 1, 2, 4, 8, 16 s, then 30 s, the cap, from then on.
    #[test]
    fn the_window_doubles_with_each_decline_up_to_the_cap() {
        let start = Instant::now();
        let damping = Damping::new(start);
        assert!(!damping.is_damped());
        assert_eq!(damping.left(start), 0);
        let mut now = start;
        for window in [1, 2, 4, 8, 16, 30, 30] {
            damping.decline(now);
            assert!(damping.is_damped());
            assert_eq!(damping.left(now), window * 1000, "at {:?}", now - start);
            now += SECOND * u32::try_from(window).expect("short");
            assert_eq!(damping.left(now), 0);
            assert!(damping.is_damped(), "a passed window ended the damping");
        }
    }

    /// A decline while the window lasts leaves it; an acceptance ends the
    /// damping, and the next decline starts again at the first window.
    #[test]
    fn a_decline_within_the_window_leaves_it_and_an_acceptance_ends_it() {
        let start = Instant::now();
        let damping = Damping::new(start);
        damping.decline(start);
        damping.decline(start + SECOND / 2);
        assert_eq!(damping.left(start + SECOND / 2), 500);
        damping.decline(start + SECOND);
        assert_eq!(damping.left(start + SECOND), 2000);
        damping.accept();
        assert!(!damping.is_damped());
        assert_eq!(damping.left(start + SECOND), 0);
        damping.decline(start + SECOND * 2);
        assert_eq!(damping.left(start + SECOND * 2), 1000);
    }

    /// Declines that land while one window lasts, as when many requests
    /// in flight met one full cell at once, are one decline: the window
    /// ends where the first one put it, and the next decline, after it,
    /// doubles it once.
    #[test]
    fn declines_that_land_during_one_window_are_one_decline() {
        let start = Instant::now();
        let damping = Damping::new(start);
        for at in 0..10 {
            damping.decline(start + SECOND / 10 * at);
        }
        assert_eq!(damping.left(start), 1000);
        assert_eq!(damping.left(start + SECOND), 0);
        damping.decline(start + SECOND);
        assert_eq!(damping.left(start + SECOND), 2000);
    }
}
