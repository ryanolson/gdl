//! How a cell's hub paces its work, and how many watches it serves.

use std::sync::Arc;
use std::time::Duration;

use gdl_cells::CellRecord;

use crate::Quorum;
use crate::fanout::Places;

/// How a cell's hub paces its judgment, its record, and its watches, and
/// how many of its watches it serves at once.
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
    /// How long after a follow's link ends the hub waits before it may
    /// follow the cell again.
    pub refollow: Duration,
    /// How many watches each of the hub's watch routes serves at once.
    pub watch_caps: WatchCaps,
}

impl Default for CellTiming {
    fn default() -> Self {
        Self {
            hold: Quorum::DEFAULT_HOLD,
            heartbeat: Duration::from_secs(10),
            lease: CellRecord::DEFAULT_LEASE,
            watch_buffer: 1024,
            refollow: Duration::from_millis(250),
            watch_caps: WatchCaps::default(),
        }
    }
}

/// How many watches each watch route of a cell's hub serves at once. A
/// watch asked for beyond its route's cap is refused with 503 and a
/// `Retry-After`, before any snapshot is built or any subscription taken
/// for it; a watch that closes frees its place at once. The watches taken
/// in process ([`crate::CellReadiness::watch`],
/// [`crate::CellReadiness::follows`]) are not capped.
///
/// Each default is sized for a large healthy cell, of some hundreds of
/// instances: at least twice what a cell of 500 holds open, so that a
/// cell whose instances all restart at once, each counted by its old
/// connection until the hub sees it close and by its new one, is still
/// served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchCaps {
    /// The cell's watch (`GET watch`), opened by each of the cell's own
    /// instances and by its parent's hub, one per follow of it.
    pub cell: usize,
    /// The relay (`GET follows`), opened by each chooser instance of the
    /// cell.
    pub relay: usize,
    /// The watch of one follow (`GET follows/{class}/{cell}`), opened by a
    /// chooser for each followed cell it reads again after an event of a
    /// kind it does not know, and held until its next watch of the relay:
    /// one per chooser and per followed cell when an event of a new kind
    /// names every follow.
    pub follow: usize,
}

impl WatchCaps {
    /// The default cap of the cell's watch: one per instance of a cell of
    /// 500, and one per follow of its parent's hub, which holds a few at
    /// most, four times over.
    pub const CELL: usize = 2048;
    /// The default cap of the relay: one per chooser instance of a cell of
    /// 500, four times over.
    pub const RELAY: usize = 2048;
    /// The default cap of the watches of one follow: one per chooser of a
    /// cell of 500 and per followed cell, for a parent that follows 16
    /// cells, every one read again, twice over. Each costs one cell's
    /// snapshot, and its cell's channel is shared, so it is far cheaper
    /// than a watch of the relay.
    pub const FOLLOW: usize = 16384;

    /// Whether a route would refuse every watch.
    pub(super) fn has_zero(&self) -> bool {
        [self.cell, self.relay, self.follow].contains(&0)
    }

    /// The places of each route.
    pub(super) fn places(&self) -> RoutePlaces {
        RoutePlaces {
            cell: Places::new(self.cell),
            relay: Places::new(self.relay),
            follow: Places::new(self.follow),
        }
    }
}

impl Default for WatchCaps {
    fn default() -> Self {
        Self {
            cell: Self::CELL,
            relay: Self::RELAY,
            follow: Self::FOLLOW,
        }
    }
}

/// The places of each watch route of a cell's hub, as [`WatchCaps`] sizes
/// them.
pub(super) struct RoutePlaces {
    pub(super) cell: Arc<Places>,
    pub(super) relay: Arc<Places>,
    pub(super) follow: Arc<Places>,
}

impl RoutePlaces {
    pub(super) fn refused(&self) -> RefusedWatches {
        RefusedWatches {
            cell: self.cell.refused(),
            relay: self.relay.refused(),
            follow: self.follow.refused(),
        }
    }
}

/// How many watches each watch route of a cell's hub has refused, every
/// place it holds being taken ([`WatchCaps`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RefusedWatches {
    /// Of the cell's watch.
    pub cell: u64,
    /// Of the relay.
    pub relay: u64,
    /// Of the watches of one follow.
    pub follow: u64,
}
