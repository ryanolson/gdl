//! The cell's watch: what a cell's hub tells a hub that follows the cell,
//! as typed events, and the client that reads them.

use gdl_cells::{CellState, LoadSummary};
use serde::{Deserialize, Serialize};
use velo_hub::velo::{InstanceId, PeerInfo};

mod client;

pub use client::{CellWatchError, CellWatcher};

/// The name the cell's extension mounts its routes under, at
/// `/v1/ext/{EXTENSION}`.
pub(crate) const EXTENSION: &str = "gdl-cell";

/// The watch's route, under the extension's.
pub(crate) const ROUTE: &str = "/watch";

/// One event of a cell's watch, `GET /v1/ext/gdl-cell/watch` on the cell
/// hub's public listener: each server-sent event's data is one of these,
/// as JSON tagged by `event`.
///
/// A watch begins with a snapshot: [`CellWatchEvent::Cell`], then the
/// retired entry instances, then the ready ones that have not retired,
/// then [`CellWatchEvent::Synced`]. After it, each change as it happens.
/// The retired instances come first, so that a follower never makes an
/// instance routable that has already retired.
///
/// An entry instance is an instance of an entry segment: one that holds
/// the inner end of a hop crossing into the cell's class, which is all a
/// chooser in another class reaches. A class that no hop enters has none,
/// and its watch carries the cell's state alone.
///
/// The watch carries every entry instance whatever the cell's state: it is
/// the follower that exposes none of a cell's instances while the cell is
/// `draining` or `unavailable`, from the state it was last sent, so a
/// change of state never repeats the instances. The epoch does not change
/// within a watch: a hub that restarts ends every watch, and a watcher
/// that connects to the new process sees its epoch.
///
/// Retirement is final: an instance that leaves `retired`, or withdraws
/// `ready` without retiring (the runtime does neither), changes nothing on
/// the watch, and stays as it was sent until it leaves the hub. So a
/// snapshot is always what an earlier snapshot and every event since it
/// make.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CellWatchEvent {
    /// The cell as the watch begins: the first event of every watch.
    Cell {
        /// The cell's name.
        name: String,
        /// The cell's class.
        class: String,
        /// The epoch of the cell's record: the hub process's.
        epoch: u64,
        /// The cell's state.
        state: CellState,
        /// The load of the cell's entry segment: `None` until its
        /// instances report admission, as in the cell's record.
        load: Option<LoadSummary>,
    },
    /// The snapshot is complete: the events before this one are the cell
    /// as the watch began.
    Synced,
    /// The cell's state changed.
    State {
        /// The state it changed to.
        state: CellState,
    },
    /// An entry instance announced that it is ready, and has not retired.
    Ready {
        /// Its segment.
        segment: String,
        /// Who it is and where it is reached, as the hub's members name it.
        peer: PeerInfo,
    },
    /// An entry instance retired: no new work may be sent to it.
    Retired {
        /// Its segment.
        segment: String,
        /// The instance.
        instance: InstanceId,
    },
    /// An entry instance that a `ready` or `retired` event named left the
    /// hub.
    Left {
        /// Its segment.
        segment: String,
        /// The instance.
        instance: InstanceId,
    },
}
