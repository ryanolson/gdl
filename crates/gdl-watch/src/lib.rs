//! The watches a cell's hub serves, as typed events: the cell's watch,
//! what it tells a hub that follows the cell; the relay, what it follows,
//! for its own instances; and the client that reads both. Also the report
//! an entry instance sends its own hub of its admission ([`LoadReport`]),
//! from which the hub makes the cell's load summary, and the
//! acknowledgement a chooser sends its own hub once it no longer holds an
//! offer to a followed cell that is not offered ([`Unoffered`]). And the
//! acknowledgements of a followed cell's retired entry instance: the one a
//! chooser sends its own hub once its tables hold the instance retired
//! ([`Avoids`]), and the one its hub sends the cell's hub once every
//! chooser of its cell has ([`Avoided`]).
//!
//! The hub (`gdl-hub`) serves them and a segment instance reads the relay
//! and its own cell's watch, for the cell's drain, and sends the report, so the wire types, the routes, and the client
//! live here, where both reach them without the runtime linking the hub's
//! server.
//!
//! A reader ignores the fields of a known event that it does not know, and
//! fails closed for the cell that an event of a kind it does not know
//! concerns ([`Watched`]): it does not route to the cell until it has read
//! it again from a snapshot. So a later build adds what it has to say to
//! a known event as an optional field, which needs no lockstep, and adds
//! a kind of event only rarely, since on an earlier reader each one costs
//! its cell's eligibility until the cell is read again. An event of the
//! relay names its follow, by class, cell, and generation, so that a
//! reader knows which cell to fail closed for; one of a kind a reader does
//! not know that names none ends the relay's watch.

use gdl_cells::{CellState, LoadSummary};
use serde::{Deserialize, Serialize};
use velo::{InstanceId, PeerInfo};

mod client;
mod follows;
mod limits;
mod watched;

pub use client::{CellWatchError, CellWatcher, WatchItem, WatchStream};
pub use follows::FollowEvent;
pub use limits::{WatchFailure, WatchLimits};
pub use watched::{UnknownEvent, UnknownFollowEvent, Watched};

/// The name the cell's extension mounts its routes under, at
/// `/v1/ext/{EXTENSION}`.
pub const EXTENSION: &str = "gdl-cell";

/// The cell watch's route, under the extension's. A follower names its
/// watch in the query, `?class=<class>&cell=<cell>&run=<run>&generation=<generation>`
/// ([`Follower`]), and the watch counts among the cell's followers from
/// then until the follower releases it ([`RELEASE_ROUTE`]). A watch that
/// closes without a release still counts; once the cell's lease has passed
/// since it closed it no longer shows as a follower, but it is never taken
/// as a release, so a draining cell does not say it is unfollowed while
/// it is unreleased. A watch that names no follower counts for nothing.
pub const WATCH_ROUTE: &str = "/watch";

/// The route by which a follower lets go of a cell, under the
/// extension's, on the cell hub's public listener: `POST` with the
/// [`Follower`] its watch named, answered `204 No Content`. The follower
/// sends it once none of its choosers holds an offer to the cell in that
/// follow, so that the watch no longer counts, whether it is still open or
/// already closed.
pub const RELEASE_ROUTE: &str = "/release";

/// One watch by which a cell follows another: the follower's cell, by its
/// class and its name within the class, which is only a label, and the
/// follow it belongs to, by the run of the follower's hub and the follow's
/// generation in that run. The query of [`WATCH_ROUTE`], and the body of
/// [`RELEASE_ROUTE`]. Nothing checks the names: they are not
/// authenticated.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Follower {
    /// The follower's class.
    pub class: String,
    /// The follower's cell, named within its class.
    pub cell: String,
    /// A number the follower's hub draws once each time it starts, so that
    /// the watches of a hub that restarted are told from those of the run
    /// before, whose generations it numbers again from 1.
    pub run: u64,
    /// The follow's generation in that run.
    pub generation: u64,
}

/// The route by which a follower says that its whole cell avoids one of
/// the followed cell's retired entry instances, under the extension's, on
/// the cell hub's public listener, beside [`RELEASE_ROUTE`]: `POST` with
/// an [`Avoided`], answered `204 No Content`. The follower's hub sends it
/// once every chooser of its cell has acknowledged the retirement or left
/// ([`AVOIDS_ROUTE`]). The cell's hub counts it only from a watch it
/// counted among its followers, under the [`Follower`] it names, when the
/// instance retired; any other changes nothing. Like a release, it is not
/// authenticated: anything that can reach the cell's hub can end a
/// retiree's wait early, and a chooser that still picks the retiree once
/// it has stopped taking work is refused, and offers the request to
/// another cell.
pub const AVOIDED_ROUTE: &str = "/avoided";

/// What a follower tells a followed cell's hub once its whole cell avoids
/// one of the cell's retired entry instances: the watch of the follow, as
/// [`WATCH_ROUTE`]'s query named it, and the instance. The body of
/// [`AVOIDED_ROUTE`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Avoided {
    /// The follow's watch.
    #[serde(flatten)]
    pub follower: Follower,
    /// The retired entry instance.
    pub instance: InstanceId,
}

/// The relay's route, under the extension's.
pub const FOLLOWS_ROUTE: &str = "/follows";

/// The route of a watch of one follow of the relay, under the extension's:
/// `GET` answers with the follow of the cell `cell` of `class` as a watch
/// of the relay begins with it, then `synced`, then each event of the
/// relay that names the cell, of whichever generation, as server-sent
/// events, until the hub stops or the watcher falls behind; only `synced`
/// first when the hub has no such follow installed. Its snapshot and the
/// events after it are ordered as the relay's are, so a reader that met
/// an event of a kind it does not know for the cell reads the cell from
/// it from then ([`CellWatcher::follows_of`]).
pub const FOLLOWED_ROUTE: &str = "/follows/{class}/{cell}";

/// [`FOLLOWED_ROUTE`]'s path for the cell `cell` of `class`.
pub fn followed_path(class: &str, cell: &str) -> String {
    format!("/follows/{class}/{cell}")
}

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

/// The route by which a chooser says it runs a follower of its hub's
/// relay, under the extension's, on the hub's control listener: `POST`,
/// with no body, carrying the credential of the instance the path names,
/// which must be a member of the hub of a segment that chooses cells of
/// another class. Only from then does the hub wait for its
/// acknowledgements ([`UNOFFERED_ROUTE`]) before it closes a follow: a
/// member that runs no follower never makes an offer, and never
/// acknowledges. A chooser says so before it first watches the relay.
pub const CHOOSER_ROUTE: &str = "/instances/{instance_id}/chooser";

/// [`CHOOSER_ROUTE`]'s path for `instance`.
pub fn chooser_path(instance: InstanceId) -> String {
    format!("/instances/{instance}/chooser")
}

/// The acknowledgement's route, under the extension's, on the hub's
/// control listener: `POST` with an [`Unoffered`], carrying the
/// credential of the instance the path names, which must be a member of
/// the hub whose segment chooses cells of the class it names, and that has
/// said it runs a follower ([`CHOOSER_ROUTE`]). Answered
/// `204 No Content` once the hub has taken it, whether or not it still
/// answers the follow's current revision; one that does not counts for
/// nothing.
pub const UNOFFERED_ROUTE: &str = "/instances/{instance_id}/unoffered";

/// [`UNOFFERED_ROUTE`]'s path for `instance`.
pub fn unoffered_path(instance: InstanceId) -> String {
    format!("/instances/{instance}/unoffered")
}

/// The route by which a chooser tells its own hub that it avoids a
/// followed cell's retired entry instance, under the extension's, on the
/// hub's control listener: `POST` with an [`Avoids`], carrying the
/// credential of the instance the path names, which must be a member of
/// the hub whose segment chooses cells of the class it names, and that has
/// said it runs a follower ([`CHOOSER_ROUTE`]). Answered `204 No Content`
/// once the hub has taken it, whether or not the hub still waits for it;
/// one it does not wait for counts for nothing.
pub const AVOIDS_ROUTE: &str = "/instances/{instance_id}/avoids";

/// [`AVOIDS_ROUTE`]'s path for `instance`.
pub fn avoids_path(instance: InstanceId) -> String {
    format!("/instances/{instance}/avoids")
}

/// What a chooser tells its own hub once its tables hold a followed
/// cell's entry instance as retired, so that it never picks it again: the
/// follow, by the cell's class and name and its generation, and the
/// instance. It promises nothing about the hand-offs it sent the instance
/// before: the instance's setup timeout covers those.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Avoids {
    /// The cell's class.
    pub class: String,
    /// The cell, named within its class.
    pub cell: String,
    /// The follow's generation.
    pub generation: u64,
    /// The retired entry instance.
    pub instance: InstanceId,
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
/// A watch begins with a snapshot: [`CellWatchEvent::Cell`], then, if the
/// draining cell has been told that no cell follows it, its
/// [`CellWatchEvent::State`] again with `unfollowed`, then the retired
/// entry instances, then the ready ones that have not retired, then
/// [`CellWatchEvent::Synced`]. After it, each change as it happens.
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
///
/// Every event concerns the watched cell. A field a later build adds to
/// one is optional, and a reader of this build ignores it; an event of a
/// kind a reader does not know makes it fail closed for the cell until it
/// has read the cell again ([`Watched`]).
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
    /// The cell's state changed; or, sent again with `unfollowed`, the
    /// cell drains and no cell follows it any more.
    State {
        /// The state it changed to.
        state: CellState,
        /// The cell drains, and no cell follows it any more: each that did
        /// has let go of it, which a cell's hub does only once none of its
        /// choosers holds an offer to it, so no offer from another cell is
        /// still on its way. Sent once, and in every snapshot after it.
        ///
        /// A field of a known event, not a kind of its own, so that a reader
        /// that does not know it ignores it: it is for the cell's own
        /// instances, and says nothing about whether another cell is to
        /// offer the cell work, which a kind it did not know would have
        /// made it fail closed for, reading the cell again for as long as
        /// it drains. An earlier entry instance, which reads only the
        /// state, keeps its bound.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        unfollowed: bool,
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
    /// An entry instance retired: no new work may be sent to it. Sent
    /// again, with `acknowledged`, once every cell that followed this one
    /// when it retired avoids it.
    Retired {
        /// Its segment.
        segment: String,
        /// The instance.
        instance: InstanceId,
        /// Every cell that followed this one when the instance retired has
        /// acknowledged the retirement ([`AVOIDED_ROUTE`]), or let go of
        /// the cell ([`RELEASE_ROUTE`]): none of its choosers picks the
        /// instance again. With no such cell, the first `retired` says so.
        /// Sent once, and in every snapshot after it. A cell that starts to
        /// follow this one later is shown the instance retired, and never
        /// offers to it, so it is not waited for.
        ///
        /// A field of a known event, not a kind of its own, so that a
        /// reader that does not know it ignores it: it is for the retired
        /// instance, whose stop waits for it, and says nothing a follower
        /// needs, which a kind it did not know would have made it fail
        /// closed for, reading the cell again at every retirement. A
        /// follower that reads the event again finds the instance retired
        /// already, and changes nothing.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        acknowledged: bool,
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
