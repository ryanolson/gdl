use std::time::Duration;

use tokio::time::Instant;

use super::Cell;

/// The cells a chooser damps, at one moment: each cell that declined an
/// offer and has accepted none since, by [`id`](super::Cell::id), with the
/// end of its window, while which a policy passes it over. Sorted by id,
/// with the latest end of the windows, so that a choice made when every
/// window has passed reads nothing more.
///
/// It also holds the cells that declined an offer as gone: a draining cell,
/// or one whose entry refuses by its own policy. Such a cell has no window.
/// It is passed over, by every policy that honors
/// [`CellRequest::is_excluded`](super::CellRequest::is_excluded), until its
/// follow changes: a new generation, or a new state or offered state
/// ([`Damped::is_gone`]).
///
/// The runtime keeps one per chooser and swaps it whole on each change, so
/// that a choice reads every cell's damping as it was at one moment. A
/// policy crate makes one with [`Damped::new`] and [`Damped::with_gone`]
/// for its own tests.
#[derive(Debug, Clone, Default)]
pub struct Damped {
    cells: Vec<(u64, Mark)>,
    latest: Option<Instant>,
    /// Whether any cell is gone, so that a choice while none is reads
    /// nothing more.
    gone: bool,
}

/// One damped cell's window: when it ends, and how long it was, which the
/// runtime doubles at the cell's next decline.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub until: Instant,
    pub length: Duration,
}

/// What a cell's decline left: a window, or the follow it declined as gone
/// in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mark {
    Window(Window),
    Gone(Follow),
}

/// A cell's follow as a chooser's table shows it, and how many times the
/// relay changed its state or offered state: what a decline as gone holds
/// until either changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Follow {
    generation: u64,
    changes: u64,
}

impl Follow {
    fn of(cell: &Cell) -> Self {
        Self {
            generation: cell.generation,
            changes: cell.changes,
        }
    }
}

impl Damped {
    /// No cell damped.
    pub const EMPTY: Self = Self {
        cells: Vec::new(),
        latest: None,
        gone: false,
    };

    /// The cells in `windows` damped, each until the instant given with
    /// its [`id`](super::Cell::id).
    pub fn new(windows: impl IntoIterator<Item = (u64, Instant)>) -> Self {
        let mut cells: Vec<(u64, Mark)> = windows
            .into_iter()
            .map(|(cell, until)| {
                let window = Window {
                    until,
                    length: Duration::ZERO,
                };
                (cell, Mark::Window(window))
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
    /// since, whether or not its window has passed. A cell that declined
    /// as gone is not damped: it has no window ([`Damped::is_gone`]).
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

    /// Whether `cell` declined an offer as gone in the follow it is shown
    /// in now, and has accepted nothing since: the relay has shown no new
    /// generation of it, and no new state or offered state, since it
    /// declined. A cell whose follow changed since is no longer gone. One
    /// branch while no cell is gone.
    #[inline]
    pub fn is_gone(&self, cell: &Cell) -> bool {
        self.gone && self.mark(cell.id) == Some(Mark::Gone(Follow::of(cell)))
    }

    /// Whether `cell` has a mark as gone, in whatever follow.
    #[doc(hidden)]
    #[inline]
    pub fn holds_gone(&self, cell: u64) -> bool {
        self.gone && matches!(self.mark(cell), Some(Mark::Gone(_)))
    }

    /// Whether `cell` is gone in the follow it is shown in, or in a later
    /// one: a decline as gone made from an earlier view of the cell must
    /// not replace it.
    #[doc(hidden)]
    #[inline]
    pub fn holds_gone_since(&self, cell: &Cell) -> bool {
        let shown = Follow::of(cell);
        self.gone
            && matches!(self.mark(cell.id), Some(Mark::Gone(held))
                if (held.generation, held.changes) >= (shown.generation, shown.changes))
    }

    /// The list with `cell` gone in the follow it is shown in, in place of
    /// any window it had.
    pub fn with_gone(&self, cell: &Cell) -> Self {
        self.marked(cell.id, Mark::Gone(Follow::of(cell)))
    }

    /// The list without each gone mark that `keep` refuses, given the
    /// cell's id and the generation and count of changes it declined in,
    /// or `None` when it refuses none. Windows stay.
    #[doc(hidden)]
    pub fn retain_gone(&self, keep: impl Fn(u64, u64, u64) -> bool) -> Option<Self> {
        let stale = |(id, mark): &(u64, Mark)| match mark {
            Mark::Gone(follow) => !keep(*id, follow.generation, follow.changes),
            Mark::Window(_) => false,
        };
        if !self.gone || !self.cells.iter().any(stale) {
            return None;
        }
        let cells = self
            .cells
            .iter()
            .filter(|cell| !stale(cell))
            .copied()
            .collect();
        Some(Self::of(cells))
    }

    /// How many cells are gone, current or not, for a test that checks
    /// that the runtime forgets marks whose follow changed.
    #[doc(hidden)]
    pub fn gone_marks(&self) -> usize {
        self.cells
            .iter()
            .filter(|(_, mark)| matches!(mark, Mark::Gone(_)))
            .count()
    }

    /// `cell`'s window, if it is damped.
    #[doc(hidden)]
    #[inline]
    pub fn window(&self, cell: u64) -> Option<Window> {
        match self.mark(cell)? {
            Mark::Window(window) => Some(window),
            Mark::Gone(_) => None,
        }
    }

    /// The list with `cell` damped for `window`, in place of any mark it
    /// had.
    #[doc(hidden)]
    pub fn with(&self, cell: u64, window: Window) -> Self {
        self.marked(cell, Mark::Window(window))
    }

    /// The list without `cell`, or `None` when it was neither damped nor
    /// gone.
    #[doc(hidden)]
    #[inline]
    pub fn without(&self, cell: u64) -> Option<Self> {
        let at = self.cells.binary_search_by_key(&cell, |(id, _)| *id).ok()?;
        let mut cells = self.cells.clone();
        cells.remove(at);
        Some(Self::of(cells))
    }

    #[inline]
    fn mark(&self, cell: u64) -> Option<Mark> {
        self.cells
            .binary_search_by_key(&cell, |(id, _)| *id)
            .ok()
            .map(|at| self.cells[at].1)
    }

    fn marked(&self, cell: u64, mark: Mark) -> Self {
        let mut cells = self.cells.clone();
        match cells.binary_search_by_key(&cell, |(id, _)| *id) {
            Ok(at) => cells[at].1 = mark,
            Err(at) => cells.insert(at, (cell, mark)),
        }
        Self::of(cells)
    }

    fn of(cells: Vec<(u64, Mark)>) -> Self {
        let latest = cells
            .iter()
            .filter_map(|(_, mark)| match mark {
                Mark::Window(window) => Some(window.until),
                Mark::Gone(_) => None,
            })
            .max();
        let gone = cells.iter().any(|(_, mark)| matches!(mark, Mark::Gone(_)));
        Self {
            cells,
            latest,
            gone,
        }
    }
}
