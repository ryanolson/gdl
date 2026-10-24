//! How a cell's hub paces itself.

use std::time::Duration;

use gdl_cells::CellRecord;

use crate::Quorum;

/// How a cell's hub paces its judgment, its record, and its watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellTiming {
    /// How long the quorum must hold before the cell is `ready`.
    pub hold: Duration,
    /// How often the record is published while the state does not change.
    pub heartbeat: Duration,
    /// How long a reader keeps the record without seeing it published
    /// again: three heartbeats by default.
    pub lease: Duration,
    /// How many events a watcher of the cell, or of what its hub follows,
    /// may fall behind by before its watch ends: at least this many,
    /// rounded up to a power of two.
    pub watch_buffer: usize,
    /// How long after a follow's link ends the hub first waits before it
    /// may follow the cell again: the wait doubles while the cell's links
    /// keep failing, up to [`CellTiming::REFOLLOW_CAP`], each drawn between
    /// half its step and the whole step.
    pub refollow: Duration,
    /// What a followed cell's watch may cost before its link ends it.
    pub watch: gdl_watch::WatchLimits,
}

impl Default for CellTiming {
    fn default() -> Self {
        Self {
            hold: Quorum::DEFAULT_HOLD,
            heartbeat: Duration::from_secs(10),
            lease: CellRecord::DEFAULT_LEASE,
            watch_buffer: 1024,
            refollow: Duration::from_millis(250),
            watch: gdl_watch::WatchLimits::default(),
        }
    }
}

impl CellTiming {
    /// The longest a hub waits before it follows a cell again after the
    /// cell's link ended, however often its links failed, unless
    /// [`CellTiming::refollow`] is longer. Longer than an instance's cap on
    /// its own hub's watches, since the children of every parent may fail
    /// at once and a parent's retries are spared a child's hub that is
    /// down; short enough that a child whose hub comes back is offered to
    /// again within seconds. A child whose hub restarted writes a record of
    /// a new epoch, which is followed at once, whatever the wait.
    pub const REFOLLOW_CAP: Duration = Duration::from_secs(5);
}
