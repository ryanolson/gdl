//! The watches a pool's hub serves, as typed events: the pool's watch,
//! what it tells a hub that follows the pool; the relay, what it follows,
//! for its own instances; and the client that reads both. Also the report
//! an entry instance sends its own hub of its admission ([`LoadReport`]),
//! from which the hub makes the pool's load summary, and the
//! acknowledgement a chooser sends its own hub once it no longer holds an
//! offer to a followed pool that is not offered ([`Unoffered`]). And the
//! acknowledgements of a followed pool's retired entry instance: the one a
//! chooser sends its own hub once its tables hold the instance retired
//! ([`Avoids`]), and the one its hub sends the pool's hub once every
//! chooser of its pool has ([`Avoided`]). And, under the `relay`
//! directory backend, the report a child pool's hub sends its parent's
//! hub of its record ([`Report`]). And what an instance asks of its own
//! pool hub's keyed record table, and the hub's answer ([`KeyedAsk`],
//! [`KeyedReply`]).
//!
//! The hub (`gdl-hub`) serves them and a segment instance reads the relay
//! and its own pool's watch, for the pool's drain, and sends the report, so the wire types, the routes, and the client
//! live here, where both reach them without the runtime linking the hub's
//! server. So does how long a reader waits before it opens a watch again
//! ([`Backoff`]), which the hub's follows and the instance's watches share.
//!
//! A reader ignores the fields of a known event that it does not know, and
//! fails closed for the pool that an event of a kind it does not know
//! concerns ([`Watched`]): it does not route to the pool until it has read
//! it again from a snapshot. So a later build adds what it has to say to
//! a known event as an optional field, which needs no lockstep, and adds
//! a kind of event only rarely, since on an earlier reader each one costs
//! its pool's eligibility until the pool is read again. An event of the
//! relay names its follow, by class, pool, and generation, so that a
//! reader knows which pool to fail closed for; one of a kind a reader does
//! not know that names none ends the relay's watch.

use gdl_pools::{LoadSummary, PoolState};
use serde::{Deserialize, Serialize};
use velo::{InstanceId, PeerInfo};

mod backoff;
mod client;
mod command;
mod follows;
mod keyed;
mod limits;
mod report;
mod watched;

pub use backoff::{Backoff, WatchEnd};
pub use client::{PoolWatchError, PoolWatcher, WatchItem, WatchStream};
pub use command::{
    CHILD_SCALE_TO_ZERO_ROUTE, CHILD_WITHDRAW_ROUTE, LIFT_ROUTE, REVOKE_ROUTE,
    child_scale_to_zero_path, child_withdraw_path,
};
pub use follows::{FollowEvent, Listing};
pub use keyed::{
    Expected, KEYED_EXTENSION, KEYED_ROUTE, KeyedAnswer, KeyedAsk, KeyedOp, KeyedRecord,
    KeyedReply, keyed_path,
};
pub use limits::{WatchFailure, WatchLimits};
pub use report::{HELD_ROUTE, REPORT_ROUTE, Report, ReportRefusal, ReportReply, held_path};
pub use watched::{UnknownEvent, UnknownFollowEvent, Watched};

/// The name the pool's extension mounts its routes under, at
/// `/v1/ext/{EXTENSION}`.
pub const EXTENSION: &str = "gdl-pool";

/// The pool watch's route, under the extension's. A follower names its
/// watch in the query, `?class=<class>&pool=<pool>&run=<run>&generation=<generation>`
/// ([`Follower`]), and the watch counts among the pool's followers from
/// then until the follower releases it ([`RELEASE_ROUTE`]). A watch that
/// closes without a release still counts; once the pool's lease has passed
/// since it closed it no longer shows as a follower, but it is never taken
/// as a release, so a draining pool does not say it is unfollowed while
/// it is unreleased. A watch that names no follower counts for nothing.
pub const WATCH_ROUTE: &str = "/watch";

/// The route by which a follower lets go of a pool, under the
/// extension's, on the pool hub's public listener: `POST` with the
/// [`Follower`] its watch named, answered `204 No Content`. The follower
/// sends it once none of its choosers holds an offer to the pool in that
/// follow, so that the watch no longer counts, whether it is still open or
/// already closed.
pub const RELEASE_ROUTE: &str = "/release";

/// One watch by which a pool follows another: the follower's pool, by its
/// class and its name within the class, which is only a label, and the
/// follow it belongs to, by the run of the follower's hub and the follow's
/// generation in that run. The query of [`WATCH_ROUTE`], and the body of
/// [`RELEASE_ROUTE`]. Nothing checks the names: they are not
/// authenticated.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Follower {
    /// The follower's class.
    pub class: String,
    /// The follower's pool, named within its class.
    pub pool: String,
    /// A number the follower's hub draws once each time it starts, so that
    /// the watches of a hub that restarted are told from those of the run
    /// before, whose generations it numbers again from 1.
    pub run: u64,
    /// The follow's generation in that run.
    pub generation: u64,
}

/// The route by which a follower says that its whole pool avoids one of
/// the followed pool's retired entry instances, under the extension's, on
/// the pool hub's public listener, beside [`RELEASE_ROUTE`]: `POST` with
/// an [`Avoided`], answered `204 No Content`. The follower's hub sends it
/// once every chooser of its pool has acknowledged the retirement or left
/// ([`AVOIDS_ROUTE`]). The pool's hub counts it only from a watch it
/// counted among its followers, under the [`Follower`] it names, when the
/// instance retired; any other changes nothing. Like a release, it is not
/// authenticated: anything that can reach the pool's hub can end a
/// retiree's wait early, and a chooser that still picks the retiree once
/// it has stopped taking work is refused, and offers the request to
/// another pool.
pub const AVOIDED_ROUTE: &str = "/avoided";

/// What a follower tells a followed pool's hub once its whole pool avoids
/// one of the pool's retired entry instances: the watch of the follow, as
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

/// The relay's route, under the extension's. A chooser names itself in
/// the query ([`RelayWatcher`]), so that its hub knows whether it still
/// watches the relay; a watch that names no one is served the same.
pub const FOLLOWS_ROUTE: &str = "/follows";

/// The chooser that holds a watch of its hub's relay, as [`FOLLOWS_ROUTE`]'s
/// query names it (`?instance=<id>`). The hub stops waiting for the
/// acknowledgements ([`UNOFFERED_ROUTE`]) of a chooser that has had no
/// watch open for `[pool] lost_chooser_ms`, and waits for it again once it
/// watches again. The name is not authenticated: a watch in another's name
/// can only keep the hub waiting for that chooser, as it would if the
/// chooser watched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelayWatcher {
    /// The chooser instance.
    pub instance: InstanceId,
}

/// The route of a watch of one follow of the relay, under the extension's:
/// `GET` answers with the follow of the pool `pool` of `class` as a watch
/// of the relay begins with it, then `synced`, then each event of the
/// relay that names the pool, of whichever generation, as server-sent
/// events, until the hub stops or the watcher falls behind; only `synced`
/// first when the hub has no such follow installed. Its snapshot and the
/// events after it are ordered as the relay's are, so a reader that met
/// an event of a kind it does not know for the pool reads the pool from
/// it from then ([`PoolWatcher::follows_of`]).
pub const FOLLOWED_ROUTE: &str = "/follows/{class}/{pool}";

/// [`FOLLOWED_ROUTE`]'s path for the pool `pool` of `class`.
pub fn followed_path(class: &str, pool: &str) -> String {
    format!("/follows/{class}/{pool}")
}

/// The load report's route, under the extension's, on the hub's control
/// listener: `POST` with a [`LoadReport`], carrying the credential of the
/// instance the path names, which must be a usable entry instance of the
/// pool. A report that carries a `draining` property is answered once a
/// record that carries the drain is stored, or with 503 after half a
/// summary period; the instance forgets the drain only on the answer.
pub const LOAD_ROUTE: &str = "/instances/{instance_id}/load";

/// The drain's route, under the extension's, on the hub's control
/// listener: `POST`, with no body, starts the pool's drain and answers
/// `202 Accepted` with the pool's health once it shows the pool
/// `draining` and the directory has stored a record that says so, or
/// after 5 s without it; a drain already started is answered the same
/// way. A pool
/// whose class no hop enters is refused with `409 Conflict`.
pub const DRAIN_ROUTE: &str = "/drain";

/// The presence route, under the extension's, on the hub's control
/// listener: `POST` with a [`SetPresence`] tells the pool's hub whether the
/// pool serves or is scaled to 0 on purpose, and is answered
/// `204 No Content` once the directory has stored a record written after
/// the request that says so, or `503 Service Unavailable` when it has not
/// within 5 s, or a later request replaced it. A pool is told
/// `scaled_to_zero` before its instances stop, and `serving` as it scales
/// up again.
pub const PRESENCE_ROUTE: &str = "/presence";

/// The body of [`PRESENCE_ROUTE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetPresence {
    /// What the pool's record is to say.
    pub presence: gdl_pools::Presence,
}

/// [`LOAD_ROUTE`]'s path for `instance`.
pub fn load_path(instance: InstanceId) -> String {
    format!("/instances/{instance}/load")
}

/// The route by which a chooser says it runs a follower of its hub's
/// relay, under the extension's, on the hub's control listener: `POST`,
/// with no body, carrying the credential of the instance the path names,
/// which must be a member of the hub of a segment that chooses pools of
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
/// the hub whose segment chooses pools of the class it names, and that has
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
/// followed pool's retired entry instance, under the extension's, on the
/// hub's control listener: `POST` with an [`Avoids`], carrying the
/// credential of the instance the path names, which must be a member of
/// the hub whose segment chooses pools of the class it names, and that has
/// said it runs a follower ([`CHOOSER_ROUTE`]). Answered `204 No Content`
/// once the hub has taken it, whether or not the hub still waits for it;
/// one it does not wait for counts for nothing.
pub const AVOIDS_ROUTE: &str = "/instances/{instance_id}/avoids";

/// [`AVOIDS_ROUTE`]'s path for `instance`.
pub fn avoids_path(instance: InstanceId) -> String {
    format!("/instances/{instance}/avoids")
}

/// What a chooser tells its own hub once its tables hold a followed
/// pool's entry instance as retired, so that it never picks it again: the
/// follow, by the pool's class and name and its generation, and the
/// instance. It promises nothing about the hand-offs it sent the instance
/// before: the instance's setup timeout covers those.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Avoids {
    /// The pool's class.
    pub class: String,
    /// The pool, named within its class.
    pub pool: String,
    /// The follow's generation.
    pub generation: u64,
    /// The retired entry instance.
    pub instance: InstanceId,
}

/// What a chooser tells its own hub once it has applied a change of a
/// followed pool to not offered and holds no offer to that follow: the
/// follow, by the pool's class and name and its generation, and the
/// revision of the change. It answers that revision alone: a later change
/// of the follow, to offered or not, is not answered by it, so a hub
/// never counts it for one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Unoffered {
    /// The pool's class.
    pub class: String,
    /// The pool, named within its class.
    pub pool: String,
    /// The follow's generation.
    pub generation: u64,
    /// The revision of the change to not offered.
    pub revision: u64,
}

/// What an entry instance reports of its admission to its pool's hub,
/// once a load summary period, and of its properties, which it also reports
/// as soon as they change. A field a later build adds is ignored, so the
/// instances and their hub roll one at a time.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LoadReport {
    /// The roots the instance holds admitted now.
    pub admitted: u64,
    /// Its admission limit.
    pub limit: u64,
    /// The hand-offs it declined (no room, or retired) since the last
    /// report its hub took.
    pub refusals: u64,
    /// The properties the instance has, each with whether it can serve it
    /// now, in one list for each kind its nodes set: of a kind it has no
    /// list of, it serves the properties its pool's settings declare. An
    /// instance whose nodes set none, or of a build from before
    /// properties, sends none, which a hub never reads as an instance that
    /// serves nothing.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub properties: gdl_model::PropertyLists,
}

/// A pool's property entries, in the order of their keys, and the version of
/// the list ([`gdl_pools::PoolRecord::properties_version`]): a reader that
/// has the list from more than one path keeps the higher version.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PropertyList {
    /// The entries.
    #[serde(default)]
    pub entries: Vec<gdl_pools::PoolProperty>,
    /// The list's version.
    #[serde(default)]
    pub version: u64,
    /// The properties an operator revoked on the pool: none of them is in
    /// `entries` ([`gdl_pools::PoolRecord::revoked`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revoked: Vec<gdl_model::PropertyKey>,
}

/// One event of a pool's watch, `GET /v1/ext/gdl-pool/watch` on the pool
/// hub's public listener: each server-sent event's data is one of these,
/// as JSON tagged by `event`.
///
/// A watch begins with a snapshot: [`PoolWatchEvent::Pool`], then, if the
/// draining pool has been told that no pool follows it, its
/// [`PoolWatchEvent::State`] again with `unfollowed`, then the retired
/// entry instances, then the ready ones that have not retired, then
/// [`PoolWatchEvent::Synced`]. After it, each change as it happens.
/// The retired instances come first, so that a follower never makes an
/// instance routable that has already retired.
///
/// An entry instance is an instance of an entry segment: one that holds
/// the inner end of a hop crossing into the pool's class, which is all a
/// chooser in another class reaches. A class that no hop enters has none,
/// and its watch carries the pool's state alone.
///
/// The watch carries every entry instance whatever the pool's state: it is
/// the follower that exposes none of a pool's instances while the pool is
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
/// Every event concerns the watched pool. A field a later build adds to
/// one is optional, and a reader of this build ignores it; an event of a
/// kind a reader does not know makes it fail closed for the pool until it
/// has read the pool again ([`Watched`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum PoolWatchEvent {
    /// The pool as the watch begins: the first event of every watch.
    Pool {
        /// The pool's name.
        name: String,
        /// The pool's class.
        class: String,
        /// The epoch of the pool's record: the hub process's.
        epoch: u64,
        /// The pool's state.
        state: PoolState,
        /// The load of the pool's entry segments, as in the pool's
        /// record: `None` while no usable entry instance has a report the
        /// hub counts, and always for a class that no hop enters.
        load: Option<LoadSummary>,
        /// The pool's property entries and their version, as in its record.
        /// A field of a known event, so that a follower of an earlier
        /// build ignores it and reads the pool as it did: a kind of its
        /// own in every snapshot would withhold the pool from it on every
        /// watch. `None` from a hub of an earlier build.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        properties: Option<PropertyList>,
    },
    /// The pool's property entries changed: the whole list, at its new
    /// version, as the pool's record will carry it. A follower of an
    /// earlier build does not know this kind, and fails closed for the
    /// pool until it has read it again, from a snapshot, which carries the
    /// list as a field of [`PoolWatchEvent::Pool`] instead.
    Properties {
        /// The list.
        properties: PropertyList,
    },
    /// The snapshot is complete: the events before this one are the pool
    /// as the watch began.
    Synced,
    /// The pool's state changed; or, sent again with `unfollowed`, the
    /// pool drains and no pool follows it any more.
    State {
        /// The state it changed to.
        state: PoolState,
        /// The pool drains, and no pool follows it any more: each that did
        /// has let go of it, which a pool's hub does only once none of its
        /// choosers holds an offer to it, so no offer from another pool is
        /// still on its way. Sent once, and in every snapshot after it.
        ///
        /// A field of a known event, not a kind of its own, so that a reader
        /// that does not know it ignores it: it is for the pool's own
        /// instances, and says nothing about whether another pool is to
        /// offer the pool work, which a kind it did not know would have
        /// made it fail closed for, reading the pool again for as long as
        /// it drains. An earlier entry instance, which reads only the
        /// state, keeps its bound.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        unfollowed: bool,
    },
    /// The pool's load summary: sent once a summary period while the pool
    /// has one, changed or not, so that a watcher can tell a quiet pool
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
    /// again, with `acknowledged`, once every pool that followed this one
    /// when it retired avoids it.
    Retired {
        /// Its segment.
        segment: String,
        /// The instance.
        instance: InstanceId,
        /// Every pool that followed this one when the instance retired has
        /// acknowledged the retirement ([`AVOIDED_ROUTE`]), or let go of
        /// the pool ([`RELEASE_ROUTE`]): none of its choosers picks the
        /// instance again. With no such pool, the first `retired` says so.
        /// Sent once, and in every snapshot after it. A pool that starts to
        /// follow this one later is shown the instance retired, and never
        /// offers to it, so it is not waited for.
        ///
        /// A field of a known event, not a kind of its own, so that a
        /// reader that does not know it ignores it: it is for the retired
        /// instance, whose stop waits for it, and says nothing a follower
        /// needs, which a kind it did not know would have made it fail
        /// closed for, reading the pool again at every retirement. A
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
