//! A chooser's damping of a cell that declined its offers: for a short
//! window after a decline, the cell is passed over, so that requests do not
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
//!
//! A chooser keeps the cells it damps as one immutable list, swapped whole
//! on each change, as its tables are: a choice reads the list once, so the
//! cap on how many cells are damped and the cells it draws from are judged
//! from one moment, whatever other requests decline or accept meanwhile.
//! Damping is rare, so the list is short, usually empty; a change makes a
//! new one, and only a decline, or an acceptance by a damped cell, makes
//! one.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use tokio::time::Instant;

/// The window after a cell's first decline.
pub(crate) const FIRST_WINDOW: Duration = Duration::from_secs(1);

/// The longest window: a cell that keeps declining is tried again at least
/// this often.
pub(crate) const MAX_WINDOW: Duration = Duration::from_secs(30);

/// One chooser instance's damping of the cells it sends to, by cell
/// [`id`](super::CellTable::id): the same in every table of a cell, so it
/// outlives a new snapshot or generation of the cell, and apart from the
/// tables, which the follower makes.
///
/// A cell is damped from a decline until an acceptance: while its window
/// lasts it is passed over (at most half of the eligible cells are, so a
/// lone cell is still chosen), and once the window has passed it may be chosen
/// again, which is how it is offered the request whose acceptance ends its
/// damping, or whose decline doubles the window.
#[derive(Debug, Default)]
pub(crate) struct Damping {
    view: ArcSwap<Damped>,
}

impl Damping {
    /// The damped cells now, for one choice: one load, no lock, no
    /// allocation.
    pub(crate) fn view(&self) -> Guard<Arc<Damped>> {
        self.view.load()
    }

    /// The cell `cell` declined an offer at `now`: damp it for
    /// [`FIRST_WINDOW`] if it was not damped, for twice its last window (at
    /// most [`MAX_WINDOW`]) if that window has passed, and leave a window
    /// that still lasts as it is.
    pub(crate) fn decline(&self, cell: u64, now: Instant) {
        self.change(|damped| damped.declined(cell, now));
    }

    /// The cell `cell` accepted an offer: its damping ends. A cell that is
    /// not damped writes nothing, and costs one load and a search of an
    /// empty or short list.
    pub(crate) fn accept(&self, cell: u64) {
        self.change(|damped| damped.accepted(cell));
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
}

/// The cells a chooser damps, at one moment: sorted by id, with the latest
/// end of their windows, so that a choice made when every window has passed
/// reads nothing more.
#[derive(Debug, Default)]
pub(crate) struct Damped {
    cells: Box<[(u64, Window)]>,
    latest: Option<Instant>,
}

/// One damped cell's window.
#[derive(Debug, Clone, Copy)]
struct Window {
    until: Instant,
    length: Duration,
}

impl Damped {
    /// Whether any cell's window lasts at `now`.
    pub(crate) fn lasts(&self, now: Instant) -> bool {
        self.latest.is_some_and(|latest| now < latest)
    }

    /// Whether `cell` is damped: it declined, and has accepted nothing
    /// since, whether or not its window has passed.
    pub(crate) fn is_damped(&self, cell: u64) -> bool {
        self.find(cell).is_some()
    }

    /// How long `cell`'s window still lasts at `now`, in milliseconds: `0`
    /// once it has passed, or when the cell is not damped.
    pub(crate) fn left(&self, cell: u64, now: Instant) -> u64 {
        self.find(cell).map_or(0, |window| {
            let left = window.until.saturating_duration_since(now);
            u64::try_from(left.as_millis()).unwrap_or(u64::MAX)
        })
    }

    fn find(&self, cell: u64) -> Option<&Window> {
        self.cells
            .binary_search_by_key(&cell, |(id, _)| *id)
            .ok()
            .map(|at| &self.cells[at].1)
    }

    /// The list once `cell` declined at `now`, or `None` when it does not
    /// change: the cell's window still lasts.
    fn declined(&self, cell: u64, now: Instant) -> Option<Self> {
        let length = match self.find(cell) {
            None => FIRST_WINDOW,
            Some(window) if window.until > now => return None,
            Some(window) => (window.length * 2).min(MAX_WINDOW),
        };
        let window = Window {
            until: now + length,
            length,
        };
        let mut cells: Vec<(u64, Window)> = self.cells.to_vec();
        match cells.binary_search_by_key(&cell, |(id, _)| *id) {
            Ok(at) => cells[at].1 = window,
            Err(at) => cells.insert(at, (cell, window)),
        }
        Some(Self::of(cells))
    }

    /// The list once `cell` accepted, or `None` when it was not damped.
    fn accepted(&self, cell: u64) -> Option<Self> {
        let at = self.cells.binary_search_by_key(&cell, |(id, _)| *id).ok()?;
        let mut cells: Vec<(u64, Window)> = self.cells.to_vec();
        cells.remove(at);
        Some(Self::of(cells))
    }

    fn of(cells: Vec<(u64, Window)>) -> Self {
        let latest = cells.iter().map(|(_, window)| window.until).max();
        Self {
            cells: cells.into_boxed_slice(),
            latest,
        }
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
    const CELL: u64 = 7;

    /// The window after each decline that falls when the last one passed:
    /// 1, 2, 4, 8, 16 s, then 30 s, the cap, from then on.
    #[test]
    fn the_window_doubles_with_each_decline_up_to_the_cap() {
        let start = Instant::now();
        let damping = Damping::default();
        assert!(!damping.view().is_damped(CELL));
        assert_eq!(damping.view().left(CELL, start), 0);
        let mut now = start;
        for window in [1, 2, 4, 8, 16, 30, 30] {
            damping.decline(CELL, now);
            assert!(damping.view().is_damped(CELL));
            assert_eq!(
                damping.view().left(CELL, now),
                window * 1000,
                "at {:?}",
                now - start
            );
            now += SECOND * u32::try_from(window).expect("short");
            assert_eq!(damping.view().left(CELL, now), 0);
            assert!(!damping.view().lasts(now));
            assert!(
                damping.view().is_damped(CELL),
                "a passed window ended the damping"
            );
        }
    }

    /// A decline while the window lasts leaves it; an acceptance ends the
    /// damping, and the next decline starts again at the first window.
    #[test]
    fn a_decline_within_the_window_leaves_it_and_an_acceptance_ends_it() {
        let start = Instant::now();
        let damping = Damping::default();
        damping.decline(CELL, start);
        damping.decline(CELL, start + SECOND / 2);
        assert_eq!(damping.view().left(CELL, start + SECOND / 2), 500);
        damping.decline(CELL, start + SECOND);
        assert_eq!(damping.view().left(CELL, start + SECOND), 2000);
        damping.accept(CELL);
        assert!(!damping.view().is_damped(CELL));
        assert_eq!(damping.view().left(CELL, start + SECOND), 0);
        damping.decline(CELL, start + SECOND * 2);
        assert_eq!(damping.view().left(CELL, start + SECOND * 2), 1000);
    }

    /// Declines that land while one window lasts, as when many requests
    /// in flight met one full cell at once, are one decline: the window
    /// ends where the first one put it, and the next decline, after it,
    /// doubles it once. Neither they nor an acceptance by a cell that is
    /// not damped change the list.
    #[test]
    fn declines_that_land_during_one_window_are_one_decline() {
        let writes = || WRITES.with(std::cell::Cell::get);
        let start = Instant::now();
        let damping = Damping::default();
        let wrote = writes();
        for at in 0..10 {
            damping.decline(CELL, start + SECOND / 10 * at);
        }
        damping.accept(CELL + 1);
        assert_eq!(writes() - wrote, 1);
        assert_eq!(damping.view().left(CELL, start), 1000);
        assert_eq!(damping.view().left(CELL, start + SECOND), 0);
        damping.decline(CELL, start + SECOND);
        assert_eq!(damping.view().left(CELL, start + SECOND), 2000);
    }

    /// A view is the list as it was when it was read: a change after it
    /// makes a new one, and leaves the view as it was.
    #[test]
    fn a_view_is_one_moment_of_the_damping() {
        let start = Instant::now();
        let damping = Damping::default();
        let before = damping.view();
        damping.decline(CELL, start);
        assert!(!before.is_damped(CELL));
        assert!(damping.view().is_damped(CELL));
        assert!(damping.view().lasts(start));
        damping.decline(CELL + 1, start + SECOND / 2);
        let both = damping.view();
        damping.accept(CELL);
        assert!(both.is_damped(CELL) && both.is_damped(CELL + 1));
        assert!(!damping.view().is_damped(CELL));
        assert!(damping.view().lasts(start + SECOND));
    }
}
