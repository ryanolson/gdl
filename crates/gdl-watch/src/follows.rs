//! The relay of what a pool's hub follows, as typed events.

use gdl_pools::{LoadSummary, PoolCondition, PoolState};
use serde::{Deserialize, Serialize};
use velo::{InstanceId, PeerInfo};

/// One event of the relay of what a pool's hub follows,
/// `GET /v1/ext/gdl-pool/follows` on the hub's public listener: each
/// server-sent event's data is one of these, as JSON tagged by `event`.
/// The hub's own instances read it, to route to the pools their class
/// sends to.
///
/// A follow is one pool, followed over its pool watch, in one generation:
/// the follower numbers each follow of a pool, and a new one never carries
/// anything over from an earlier one. Every event but
/// [`FollowEvent::Synced`] names the pool, by its class and its name
/// within the class as the pool directory names it, and the generation it
/// belongs to, so a consumer applies each event to the generation it
/// names. A pool has at most one current follow, the latest generation,
/// whose link is under way; an earlier one whose link ended, or that a
/// later one replaced, closes only once every chooser has acknowledged
/// that it no longer holds an offer to it, and until then it is still
/// relayed, not offered, beside the current one. The relay never sends an
/// event of a generation after its [`FollowEvent::Dropped`] but
/// [`FollowEvent::Withdrawn`], and after a later generation of the same
/// pool, only the earlier one's [`FollowEvent::Offering`] and
/// [`FollowEvent::Dropped`].
///
/// A watch begins with a snapshot: for each follow, the earlier
/// generations that have yet to close first, [`FollowEvent::Follow`], its
/// [`FollowEvent::Offering`] if it is not offered, then its retired
/// instances, then its exposed instances;
/// then [`FollowEvent::Synced`], the relay's marker, which names no pool.
/// After it, each change as it happens. A follow appears only once its
/// pool watch's snapshot is complete, and with its state before any of
/// its instances, so a consumer knows the pool's state before it may route
/// to any of them, and knows every retirement before the instances it does
/// not retire.
///
/// An instance is exposed (routable) only while the state the pool last
/// sent is `ready` or `degraded`, and never once it retired. So a consumer
/// routes to an instance from its [`FollowEvent::Exposed`] until its
/// [`FollowEvent::Retired`] or [`FollowEvent::Left`], until a
/// [`FollowEvent::State`] that exposes nothing (`unavailable` or
/// `draining`), which hides every instance of the follow at once, or
/// until the follow is dropped. A state that exposes them again is
/// followed by an `exposed` event for each ready instance. A consumer
/// keeps every instance a follow has exposed reachable until the follow
/// is dropped or the instance leaves, whatever the state, since what it
/// offered to the pool may still be under way: an instance the relay sent
/// as exposed is sent its [`FollowEvent::Left`] also while a state hides
/// it. A watch begun while a state hides instances does not show them, so
/// a consumer may be sent `left` for an instance it does not hold, and
/// ignores it.
///
/// A follow is offered (its pool may be chosen) while its link lives and
/// its pool's state exposes its instances: [`FollowEvent::Follow`] is
/// offered at revision 1 unless an [`FollowEvent::Offering`] follows it,
/// and each change between offered and not offered is an `offering` of
/// the next revision. A consumer never offers a request to a follow it
/// holds as not offered, and acknowledges each not-offered revision it
/// applied once it holds no offer to the follow
/// ([`crate::Unoffered`]); the follow closes once every chooser of its
/// class has acknowledged its current revision, and the relay drops it.
///
/// A pool that drains withdraws its record from the pool directory once
/// its instances have left. When the hub's watch of the directory shows
/// that withdrawal, the relay sends [`FollowEvent::Withdrawn`], naming the
/// pool's latest follow, whether or not that follow has been dropped:
/// the pool was drained on purpose. Nothing else is a withdrawal: a
/// record that expires, a link that fails, and a directory snapshot that
/// no longer holds the record send none. A watch's snapshot holds no
/// withdrawal, so a consumer that begins after one never learns of it.
///
/// Every event but [`FollowEvent::Synced`] names its follow by its class,
/// pool, and generation, and an event a later build adds must too: a
/// reader that does not know its kind fails closed for that pool until it
/// has read it again (`GET /v1/ext/gdl-pool/follows/{class}/{pool}`), and
/// ends its watch at one that names no follow. A field a later build adds
/// to a known event is optional, and a reader of this build ignores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum FollowEvent {
    /// A follow is installed: its pool's snapshot is complete.
    Follow {
        /// The pool's class.
        class: String,
        /// The pool's name.
        pool: String,
        /// The follow's generation.
        generation: u64,
        /// The region the pool runs in, from its record.
        region: String,
        /// The pool's state, as its hub last sent it.
        state: PoolState,
        /// The load of the pool's entry segments, as its hub last sent it.
        load: Option<LoadSummary>,
        /// How long before this event the relay received `load`, in
        /// milliseconds: a consumer dates the summary back by it, so that
        /// a summary the relay has held for a while does not read as new
        /// on every watch that begins. An age, not an instant, since the
        /// relay's clock and the consumer's are not the same. Absent from
        /// a hub that does not send it: the summary then dates from when
        /// the event arrives.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        load_age_ms: Option<u64>,
        /// What the hub knows of the pool's record, in a snapshot, as a
        /// [`FollowEvent::Listed`] would say it: a field of a known event,
        /// so that a reader of an earlier build reads the follow as it did.
        /// `None` after the snapshot, where a `listed` event says it, and
        /// from a hub of an earlier build.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        listing: Option<Listing>,
    },
    /// The snapshot is complete: the events before this one are every
    /// follow as the watch began, and every pool the hub lists.
    Synced {
        /// Whether the hub's watch of the pool directory had completed its
        /// first snapshot of every class it sends to when this watch
        /// began: until then, a model the listings do not name may still
        /// be listed by a record the hub has not read. A hub ends each
        /// watch whose snapshot said `false` once it has, so that a reader
        /// learns it from the next watch's snapshot. `false` from a hub of
        /// an earlier build, which lists nothing.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        listed: bool,
    },
    /// A followed pool's state changed.
    State {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The follow's generation.
        generation: u64,
        /// The state it changed to.
        state: PoolState,
    },
    /// A followed pool's load summary, as its pool watch sent it: once a
    /// summary period while the pool has one, changed or not.
    Load {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The follow's generation.
        generation: u64,
        /// The summary.
        load: Option<LoadSummary>,
    },
    /// A follow changed between offered and not offered, or, in a
    /// snapshot, is not offered: its revision, which grows by one on every
    /// change, and whether it is offered. A follow shown in a snapshot
    /// without one is offered, at a revision the consumer takes as 1, so
    /// that it applies every later change. A consumer applies each in
    /// revision order, and ignores one older than the last it applied.
    ///
    /// A reader that does not know this kind fails closed for the pool
    /// until a snapshot of it shows the pool without one: it keeps from a
    /// follow that is not offered, which is what this event says, and
    /// takes the pool again once it is offered.
    Offering {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The follow's generation.
        generation: u64,
        /// The change's revision.
        revision: u64,
        /// Whether the follow is offered.
        offered: bool,
    },
    /// An entry instance of a followed pool is routable.
    Exposed {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The follow's generation.
        generation: u64,
        /// Its segment.
        segment: String,
        /// Who it is and where it is reached, as its pool's hub names it.
        peer: PeerInfo,
    },
    /// An entry instance of a followed pool retired: no new work may be
    /// sent to it.
    Retired {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The follow's generation.
        generation: u64,
        /// Its segment.
        segment: String,
        /// The instance.
        instance: InstanceId,
    },
    /// An entry instance that an `exposed` or `retired` event named left
    /// its pool's hub.
    Left {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The follow's generation.
        generation: u64,
        /// Its segment.
        segment: String,
        /// The instance.
        instance: InstanceId,
    },
    /// A follow closed: it was not offered, and every chooser of its class
    /// acknowledged its revision or left. None of its instances is
    /// routable, and a consumer lets go of them.
    Dropped {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The follow's generation.
        generation: u64,
    },
    /// What the hub knows of a pool's record changed: its region, its
    /// condition, and its models, for its choosers' model index. The hub
    /// lists every pool of a class it sends to whose record it has read
    /// and whose hashes are its own, followed or not, until the pool
    /// withdraws its record; `listing` is `None` once it has. A snapshot
    /// sends one for each listed pool that has no follow in it, before
    /// the marker, and gives a followed pool's on its
    /// [`FollowEvent::Follow`]. A watch of one follow
    /// (`GET /v1/ext/gdl-pool/follows/{class}/{pool}`) never sends one,
    /// so that a reader of an earlier build, which fails closed for the
    /// pool at this kind and reads it again from that watch, reads it
    /// whole.
    ///
    /// `generation` names the pool's latest follow, or 0 for a pool the
    /// hub never followed: it orders nothing, and is there for a reader
    /// that does not know this kind.
    Listed {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The generation of the pool's latest follow, or 0.
        generation: u64,
        /// What the hub knows, or `None` once the pool withdrew.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        listing: Option<Listing>,
    },
    /// The pool withdrew its record from the pool directory after its
    /// drain: it was drained on purpose. It may come after the follow's
    /// [`FollowEvent::Dropped`]. A later follow of the pool, which a hub
    /// begins only from a newer record, ends it.
    Withdrawn {
        /// The pool's class.
        class: String,
        /// The pool, named within its class.
        pool: String,
        /// The generation of the pool's latest follow.
        generation: u64,
    },
}

impl FollowEvent {
    /// The class and the pool of the follow the event names; `None` for
    /// the marker.
    pub fn pool(&self) -> Option<(&str, &str)> {
        match self {
            Self::Follow { class, pool, .. }
            | Self::State { class, pool, .. }
            | Self::Load { class, pool, .. }
            | Self::Exposed { class, pool, .. }
            | Self::Retired { class, pool, .. }
            | Self::Left { class, pool, .. }
            | Self::Offering { class, pool, .. }
            | Self::Dropped { class, pool, .. }
            | Self::Withdrawn { class, pool, .. }
            | Self::Listed { class, pool, .. } => Some((class, pool)),
            Self::Synced { .. } => None,
        }
    }
}

/// What a hub knows of one pool's record, for its choosers' model index:
/// the pool's region and condition, and its models, from the record or
/// the follow link, whichever has the higher version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listing {
    /// The region the pool runs in, from its record.
    pub region: String,
    /// Whether the pool is live, scaled to 0, or missing.
    pub condition: PoolCondition,
    /// The pool's model entries, with their version.
    #[serde(default)]
    pub models: crate::ModelList,
    /// How long the pool had been silent when the hub sent this, in
    /// milliseconds, while it is missing: since its record's last write
    /// when its lease passed, or since its follow's link was lost,
    /// whichever came first. A reader dates the silence back by it. An
    /// age, not an instant, since the hub's clock and the reader's are
    /// not the same. Absent for a pool that is not missing, and from a hub
    /// of an earlier build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing_age_ms: Option<u64>,
}
