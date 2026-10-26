use std::time::Duration;

use tokio::time::Instant;

/// The cells a chooser damps, at one moment: each cell that declined an
/// offer and has accepted none since, by [`id`](super::Cell::id), with the
/// end of its window, while which a policy passes it over. Sorted by id,
/// with the latest end of the windows, so that a choice made when every
/// window has passed reads nothing more.
///
/// The runtime keeps one per chooser and swaps it whole on each change, so
/// that a choice reads every cell's damping as it was at one moment. A
/// policy crate makes one with [`Damped::new`] for its own tests.
#[derive(Debug, Clone, Default)]
pub struct Damped {
    cells: Vec<(u64, Window)>,
    latest: Option<Instant>,
}

/// One damped cell's window: when it ends, and how long it was, which the
/// runtime doubles at the cell's next decline.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub until: Instant,
    pub length: Duration,
}

impl Damped {
    /// No cell damped.
    pub const EMPTY: Self = Self {
        cells: Vec::new(),
        latest: None,
    };

    /// The cells in `windows` damped, each until the instant given with
    /// its [`id`](super::Cell::id).
    pub fn new(windows: impl IntoIterator<Item = (u64, Instant)>) -> Self {
        let mut cells: Vec<(u64, Window)> = windows
            .into_iter()
            .map(|(cell, until)| {
                let window = Window {
                    until,
                    length: Duration::ZERO,
                };
                (cell, window)
            })
            .collect();
        cells.sort_by_key(|(id, _)| *id);
        cells.dedup_by_key(|(id, _)| *id);
        Self::of(cells)
    }

    /// Whether any cell's window lasts at `now`.
    #[inline]
    pub fn lasts(&self, now: Instant) -> bool {
        self.latest.is_some_and(|latest| now < latest)
    }

    /// Whether `cell` is damped: it declined, and has accepted nothing
    /// since, whether or not its window has passed.
    #[inline]
    pub fn is_damped(&self, cell: u64) -> bool {
        self.window(cell).is_some()
    }

    /// How long `cell`'s window still lasts at `now`, in milliseconds: `0`
    /// once it has passed, or when the cell is not damped.
    #[inline]
    pub fn left(&self, cell: u64, now: Instant) -> u64 {
        self.window(cell).map_or(0, |window| {
            let left = window.until.saturating_duration_since(now);
            u64::try_from(left.as_millis()).unwrap_or(u64::MAX)
        })
    }

    /// `cell`'s window, if it is damped.
    #[doc(hidden)]
    #[inline]
    pub fn window(&self, cell: u64) -> Option<Window> {
        self.cells
            .binary_search_by_key(&cell, |(id, _)| *id)
            .ok()
            .map(|at| self.cells[at].1)
    }

    /// The list with `cell` damped for `window`.
    #[doc(hidden)]
    pub fn with(&self, cell: u64, window: Window) -> Self {
        let mut cells = self.cells.clone();
        match cells.binary_search_by_key(&cell, |(id, _)| *id) {
            Ok(at) => cells[at].1 = window,
            Err(at) => cells.insert(at, (cell, window)),
        }
        Self::of(cells)
    }

    /// The list without `cell`, or `None` when it was not damped.
    #[doc(hidden)]
    #[inline]
    pub fn without(&self, cell: u64) -> Option<Self> {
        let at = self.cells.binary_search_by_key(&cell, |(id, _)| *id).ok()?;
        let mut cells = self.cells.clone();
        cells.remove(at);
        Some(Self::of(cells))
    }

    fn of(cells: Vec<(u64, Window)>) -> Self {
        let latest = cells.iter().map(|(_, window)| window.until).max();
        Self { cells, latest }
    }
}
