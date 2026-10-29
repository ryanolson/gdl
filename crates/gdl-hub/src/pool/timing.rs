//! How a cell's hub paces its work, and how many watches it serves.

use std::sync::Arc;
use std::time::Duration;

use gdl_model::{HubTiming, WatchCaps};

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
    /// How long after a follow's link ends the hub first waits before it
    /// may follow the cell again: the wait doubles while the cell's links
    /// keep failing, up to [`CellTiming::REFOLLOW_CAP`], each drawn between
    /// half its step and the whole step.
    pub refollow: Duration,
    /// What a followed cell's watch may cost before its link ends it.
    pub watch: gdl_watch::WatchLimits,
    /// How long a chooser may have no watch of the relay open before the
    /// hub stops waiting for its acknowledgements
    /// ([`HubTiming::lost_chooser`]).
    pub lost_chooser: Duration,
    /// How many watches each of the hub's watch routes serves at once.
    pub watch_caps: WatchCaps,
}

impl Default for CellTiming {
    /// The pace of a hub whose settings set none of it.
    fn default() -> Self {
        Self::from_settings(&HubTiming::default())
    }
}

impl CellTiming {
    /// The pace the cell's settings give (`[cell]` or `GDL_CELL__*`), with
    /// what they do not set at its default: the hub binary's.
    pub fn from_settings(settings: &HubTiming) -> Self {
        Self {
            hold: settings.hold,
            heartbeat: settings.heartbeat,
            lease: settings.lease,
            watch_buffer: settings.watch_buffer,
            refollow: Duration::from_millis(250),
            watch: gdl_watch::WatchLimits::default(),
            lost_chooser: settings.lost_chooser,
            watch_caps: settings.watch_caps,
        }
    }

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

/// Whether a route would refuse every watch.
pub(super) fn has_zero(caps: &WatchCaps) -> bool {
    [caps.cell, caps.relay, caps.follow].contains(&0)
}

/// The places of each watch route of a cell's hub, as [`WatchCaps`] sizes
/// them.
pub(super) struct RoutePlaces {
    pub(super) cell: Arc<Places>,
    pub(super) relay: Arc<Places>,
    pub(super) follow: Arc<Places>,
}

impl RoutePlaces {
    /// The places of each route, as `caps` sizes them.
    pub(super) fn new(caps: &WatchCaps) -> Self {
        Self {
            cell: Places::new(caps.cell),
            relay: Places::new(caps.relay),
            follow: Places::new(caps.follow),
        }
    }

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
