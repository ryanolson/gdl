//! The relay of what a cell's hub follows, as typed events.

use gdl_cells::{CellState, LoadSummary};
use serde::{Deserialize, Serialize};
use velo::{InstanceId, PeerInfo};

/// One event of the relay of what a cell's hub follows,
/// `GET /v1/ext/gdl-cell/follows` on the hub's public listener: each
/// server-sent event's data is one of these, as JSON tagged by `event`.
/// The hub's own instances read it, to route to the cells their class
/// sends to.
///
/// A follow is one cell, followed over its cell watch, in one generation:
/// the follower numbers each follow of a cell, and a new one never carries
/// anything over from an earlier one. Every event but
/// [`FollowEvent::Synced`] names the cell, by its class and its name
/// within the class as the cell directory names it, and the generation it
/// belongs to, so a consumer drops what a generation it no longer holds
/// sends;
/// the relay itself never sends an event of a generation after its
/// [`FollowEvent::Dropped`], or after a later generation of the same
/// cell.
///
/// A watch begins with a snapshot: for each follow, [`FollowEvent::Follow`],
/// then its retired instances, then its exposed instances; then
/// [`FollowEvent::Synced`], the relay's marker, which names no cell. After
/// it, each change as it happens. A follow appears only once its cell
/// watch's snapshot is complete, and with its state before any of its
/// instances, so a consumer knows the cell's state before it may route to
/// any of them, and knows every retirement before the instances it does
/// not retire.
///
/// An instance is exposed (routable) only while the state the cell last
/// sent is `ready` or `degraded`, and never once it retired. So a consumer
/// routes to an instance from its [`FollowEvent::Exposed`] until its
/// [`FollowEvent::Retired`] or [`FollowEvent::Left`], until a
/// [`FollowEvent::State`] that exposes nothing (`unavailable` or
/// `draining`), which hides every instance of the follow at once, or
/// until the follow is dropped. A state that exposes them again is
/// followed by an `exposed` event for each ready instance.
///
/// Every event but [`FollowEvent::Synced`] names its follow by its class,
/// cell, and generation, and an event a later build adds must too: a
/// reader that does not know its kind fails closed for that cell until it
/// has read it again (`GET /v1/ext/gdl-cell/follows/{class}/{cell}`), and
/// ends its watch at one that names no follow. A field a later build adds
/// to a known event is optional, and a reader of this build ignores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum FollowEvent {
    /// A follow is installed: its cell's snapshot is complete.
    Follow {
        /// The cell's class.
        class: String,
        /// The cell's name.
        cell: String,
        /// The follow's generation.
        generation: u64,
        /// The region the cell runs in, from its record.
        region: String,
        /// The cell's state, as its hub last sent it.
        state: CellState,
        /// The load of the cell's entry segments, as its hub last sent it.
        load: Option<LoadSummary>,
    },
    /// The snapshot is complete: the events before this one are every
    /// follow as the watch began.
    Synced,
    /// A followed cell's state changed.
    State {
        /// The cell's class.
        class: String,
        /// The cell, named within its class.
        cell: String,
        /// The follow's generation.
        generation: u64,
        /// The state it changed to.
        state: CellState,
    },
    /// A followed cell's load summary, as its cell watch sent it: once a
    /// summary period while the cell has one, changed or not.
    Load {
        /// The cell's class.
        class: String,
        /// The cell, named within its class.
        cell: String,
        /// The follow's generation.
        generation: u64,
        /// The summary.
        load: Option<LoadSummary>,
    },
    /// An entry instance of a followed cell is routable.
    Exposed {
        /// The cell's class.
        class: String,
        /// The cell, named within its class.
        cell: String,
        /// The follow's generation.
        generation: u64,
        /// Its segment.
        segment: String,
        /// Who it is and where it is reached, as its cell's hub names it.
        peer: PeerInfo,
    },
    /// An entry instance of a followed cell retired: no new work may be
    /// sent to it.
    Retired {
        /// The cell's class.
        class: String,
        /// The cell, named within its class.
        cell: String,
        /// The follow's generation.
        generation: u64,
        /// Its segment.
        segment: String,
        /// The instance.
        instance: InstanceId,
    },
    /// An entry instance that an `exposed` or `retired` event named left
    /// its cell's hub.
    Left {
        /// The cell's class.
        class: String,
        /// The cell, named within its class.
        cell: String,
        /// The follow's generation.
        generation: u64,
        /// Its segment.
        segment: String,
        /// The instance.
        instance: InstanceId,
    },
    /// A follow ended: its cell watch ended or failed, or the cell's hub
    /// restarted. None of its instances is routable.
    Dropped {
        /// The cell's class.
        class: String,
        /// The cell, named within its class.
        cell: String,
        /// The follow's generation.
        generation: u64,
    },
}

impl FollowEvent {
    /// The class and the cell of the follow the event names; `None` for
    /// the marker.
    pub fn cell(&self) -> Option<(&str, &str)> {
        match self {
            Self::Follow { class, cell, .. }
            | Self::State { class, cell, .. }
            | Self::Load { class, cell, .. }
            | Self::Exposed { class, cell, .. }
            | Self::Retired { class, cell, .. }
            | Self::Left { class, cell, .. }
            | Self::Dropped { class, cell, .. } => Some((class, cell)),
            Self::Synced => None,
        }
    }
}
