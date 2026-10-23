use std::time::{Duration, Instant};

use gdl_cells::CellState;

/// Whether a cell can take work, from the usable instances of each segment
/// of its class and the cell's quorum of each: the rule a cell's hub
/// publishes by. The caller passes the time, so the rule is tested at
/// chosen instants and never waits.
///
/// - `unavailable` as soon as a segment has no usable instance;
/// - `degraded` as soon as a segment is below its quorum while every
///   segment has one;
/// - `ready` once every segment has met its quorum without a break for
///   `hold`. A drop below quorum during the hold restarts it, so that an
///   instance that flaps does not flap the cell into `ready`. During the
///   hold the cell is `degraded`: every segment has a usable instance,
///   which is what `degraded` promises a chooser, and `ready` it is not
///   yet.
///
/// A count changes nothing until the next [`Quorum::state`], so that
/// several counts taken together (a watch's first sets) are judged
/// together: a segment set to zero and back between two judgments does
/// not restart the hold.
#[derive(Debug, Clone)]
pub struct Quorum {
    segments: Vec<Segment>,
    hold: Duration,
    /// Since when every segment has met its quorum, without a break.
    met: Option<Instant>,
    state: CellState,
}

impl Quorum {
    /// How long the quorum must hold before a cell is `ready`, unless its
    /// hub is given another.
    pub const DEFAULT_HOLD: Duration = Duration::from_secs(2);

    /// The quorum of each segment of a class, in the order
    /// [`Quorum::segments`] reports them, with no usable instance counted
    /// yet: `unavailable`. A class has at least one segment; with none,
    /// every quorum would hold vacuously.
    pub fn new<S: Into<String>>(
        quorums: impl IntoIterator<Item = (S, u32)>,
        hold: Duration,
    ) -> Self {
        Self {
            segments: quorums
                .into_iter()
                .map(|(name, quorum)| Segment {
                    name: name.into(),
                    quorum,
                    usable: 0,
                })
                .collect(),
            hold,
            met: None,
            state: CellState::Unavailable,
        }
    }

    /// Count `usable` instances of `segment`, for the next judgment. A
    /// segment outside the class is ignored.
    pub fn set(&mut self, segment: &str, usable: u32) {
        if let Some(entry) = self.segments.iter_mut().find(|s| s.name == segment) {
            entry.usable = usable;
        }
    }

    /// Count `usable` instances of the segment at `index` in the class's
    /// order, for the next judgment: [`Quorum::set`] without the search.
    pub(crate) fn set_nth(&mut self, index: usize, usable: u32) {
        if let Some(entry) = self.segments.get_mut(index) {
            entry.usable = usable;
        }
    }

    /// The cell's state at `now`, from the counts set so far. Each call is
    /// a judgment: a segment below its quorum here ends the hold, even if
    /// a later count restores it.
    pub fn state(&mut self, now: Instant) -> CellState {
        let present = self.segments.iter().all(|s| s.usable > 0);
        let met = self.segments.iter().all(|s| s.usable >= s.quorum);
        self.state = if !present {
            self.met = None;
            CellState::Unavailable
        } else if !met {
            self.met = None;
            CellState::Degraded
        } else {
            let since = *self.met.get_or_insert(now);
            if now.saturating_duration_since(since) >= self.hold {
                CellState::Ready
            } else {
                CellState::Degraded
            }
        };
        self.state
    }

    /// When the state will change if no count does: the end of the hold,
    /// while one runs.
    pub fn next_change(&self) -> Option<Instant> {
        match (self.state, self.met) {
            (CellState::Ready, _) | (_, None) => None,
            (_, Some(since)) => Some(since + self.hold),
        }
    }

    /// Each segment of the class, with its usable instances and its
    /// quorum.
    pub fn segments(&self) -> impl Iterator<Item = (&str, u32, u32)> {
        self.segments
            .iter()
            .map(|s| (s.name.as_str(), s.usable, s.quorum))
    }
}

#[derive(Debug, Clone)]
struct Segment {
    name: String,
    quorum: u32,
    usable: u32,
}
