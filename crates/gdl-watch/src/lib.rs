//! The watches a cell's hub serves, as typed events: the cell's watch,
//! what it tells a hub that follows the cell; the relay, what it follows,
//! for its own instances; and the client that reads both. Also the report
//! an entry instance sends its own hub of its admission ([`LoadReport`]),
//! from which the hub makes the cell's load summary, and the
//! acknowledgement a chooser sends its own hub once it no longer holds an
//! offer to a followed cell that is not offered ([`Unoffered`]).
//!
//! The hub (`gdl-hub`) serves them and a segment instance reads the relay
//! and its own cell's watch, for the cell's drain, and sends the report, so the wire types, the routes, and the client
//! live here, where both reach them without the runtime linking the hub's
//! server.

use gdl_cells::{CellState, LoadSummary};
use serde::{Deserialize, Serialize};
use velo::{InstanceId, PeerInfo};

mod client;
mod follows;

pub use client::{CellWatchError, CellWatcher};
pub use follows::FollowEvent;

/// The name the cell's extension mounts its routes under, at
/// `/v1/ext/{EXTENSION}`.
pub const EXTENSION: &str = "gdl-cell";

/// The cell watch's route, under the extension's. A follower names itself
/// in the query, `?class=<class>&cell=<cell>`, its own cell by its class
/// and its name within the class, and counts among the cell's followers
/// while its watch is open; a watch that names no follower counts for
/// nothing.
pub const WATCH_ROUTE: &str = "/watch";

/// A cell that follows another, as its watch of that cell names it: the
/// query of [`WATCH_ROUTE`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Follower {
    /// The follower's class.
    pub class: String,
    /// The follower's cell, named within its class.
    pub cell: String,
}

/// The relay's route, under the extension's.
pub const FOLLOWS_ROUTE: &str = "/follows";

/// The load report's route, under the extension's, on the hub's control
/// listener: `POST` with a [`LoadReport`], carrying the credential of the
/// instance the path names, which must be a usable entry instance of the
/// cell.
pub const LOAD_ROUTE: &str = "/instances/{instance_id}/load";

/// The drain's route, under the extension's, on the hub's control
/// listener: `POST`, with no body, starts the cell's drain and answers
/// `202 Accepted` with the cell's health once it shows the cell
/// `draining` and the directory has stored a record that says so, or
/// after 5 s without it; a drain already started is answered the same
/// way. A cell
/// whose class no hop enters is refused with `409 Conflict`.
pub const DRAIN_ROUTE: &str = "/drain";

/// [`LOAD_ROUTE`]'s path for `instance`.
pub fn load_path(instance: InstanceId) -> String {
    format!("/instances/{instance}/load")
}

/// The acknowledgement's route, under the extension's, on the hub's
/// control listener: `POST` with an [`Unoffered`], carrying the
/// credential of the instance the path names, which must be a member of
/// the hub whose segment chooses cells of the class it names. Answered
/// `204 No Content` once the hub has taken it, whether or not it still
/// answers the follow's current revision; one that does not counts for
/// nothing.
pub const UNOFFERED_ROUTE: &str = "/instances/{instance_id}/unoffered";

/// [`UNOFFERED_ROUTE`]'s path for `instance`.
pub fn unoffered_path(instance: InstanceId) -> String {
    format!("/instances/{instance}/unoffered")
}

/// What a chooser tells its own hub once it has applied a change of a
/// followed cell to not offered and holds no offer to that follow: the
/// follow, by the cell's class and name and its generation, and the
/// revision of the change. It answers that revision alone: a later change
/// of the follow, to offered or not, is not answered by it, so a hub
/// never counts it for one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Unoffered {
    /// The cell's class.
    pub class: String,
    /// The cell, named within its class.
    pub cell: String,
    /// The follow's generation.
    pub generation: u64,
    /// The revision of the change to not offered.
    pub revision: u64,
}

/// What an entry instance reports of its admission to its cell's hub,
/// once a load summary period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LoadReport {
    /// The roots the instance holds admitted now.
    pub admitted: u64,
    /// Its admission limit.
    pub limit: u64,
    /// The hand-offs it declined (no room, or retired) since the last
    /// report its hub took.
    pub refusals: u64,
}

/// One event of a cell's watch, `GET /v1/ext/gdl-cell/watch` on the cell
/// hub's public listener: each server-sent event's data is one of these,
/// as JSON tagged by `event`.
///
/// A watch begins with a snapshot: [`CellWatchEvent::Cell`], then
/// [`CellWatchEvent::Unfollowed`] if the cell has been told so, then the
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
        /// The load of the cell's entry segments, as in the cell's
        /// record: `None` while no usable entry instance has a report the
        /// hub counts, and always for a class that no hop enters.
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
    /// The cell's load summary: sent once a summary period while the cell
    /// has one, changed or not, so that a watcher can tell a quiet cell
    /// from a stale summary by its age, and once when it no longer has
    /// one.
    Load {
        /// The summary.
        load: Option<LoadSummary>,
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
    /// The cell drains, and no cell follows it any more: each that did has
    /// let go of it, which a cell's hub does only once none of its
    /// choosers holds an offer to it, so no offer from another cell is
    /// still on its way. Sent once, and in every snapshot after it.
    Unfollowed,
}
