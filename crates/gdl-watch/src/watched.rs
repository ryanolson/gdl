//! What a watch's client reads: an event of a kind this build knows, or
//! one of a kind it does not, with the cell it concerns.

use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::{CellWatchEvent, FollowEvent};

/// One item of a watch, as [`crate::CellWatcher`] reads it: an event, or
/// one of a kind this build does not know.
///
/// A reader fails closed for the cell an unknown event concerns: it does
/// not route to that cell until it has read the cell again from a
/// snapshot, since the event may have said what would keep the reader
/// from it. So a later build adds what it has to say as optional fields
/// of a known event, which an earlier reader ignores, and adds a kind of
/// event only rarely: on an earlier reader, each one costs its cell's
/// eligibility until the cell is read again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Watched<E, U> {
    /// An event of a kind this build knows.
    Event(E),
    /// An event of a kind it does not.
    Unknown(U),
}

impl<E, U> Watched<E, U> {
    /// The event, if it is of a kind this build knows.
    pub fn event(self) -> Option<E> {
        match self {
            Self::Event(event) => Some(event),
            Self::Unknown(_) => None,
        }
    }
}

/// An event of a cell's watch of a kind this build does not know: it
/// concerns the watched cell, whatever else it says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownEvent {
    /// Its `event` tag.
    pub kind: String,
}

/// An event of the relay of a kind this build does not know, and the
/// follow it names, as every event of the relay but `synced` names one.
/// The client ends the relay's watch at an unknown event that names no
/// follow, since it cannot tell which cell to fail closed for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownFollowEvent {
    /// Its `event` tag.
    pub kind: String,
    /// The cell's class.
    pub class: String,
    /// The cell, named within its class.
    pub cell: String,
    /// The follow's generation.
    pub generation: u64,
}

/// An event type a watch carries, and how the client reads one of its
/// kinds that this build does not know.
pub(crate) trait Classify: DeserializeOwned + Send + 'static {
    type Unknown: Send + 'static;

    /// The `event` tag of each kind this build knows.
    const KINDS: &'static [&'static str];

    /// The event of a kind this build does not know that `data` holds:
    /// `None` when `data` holds no such event, since it has no `event`
    /// tag, its kind is known (so its fields did not parse), or it names
    /// no cell where the watch needs one.
    fn unknown(data: &str) -> Option<Self::Unknown>;
}

/// Whether `kind` is one of `kinds`.
fn known(kinds: &[&str], kind: &str) -> bool {
    kinds.contains(&kind)
}

impl Classify for CellWatchEvent {
    type Unknown = UnknownEvent;

    const KINDS: &'static [&'static str] = &[
        "cell",
        "synced",
        "state",
        "load",
        "ready",
        "retired",
        "left",
        "unfollowed",
    ];

    fn unknown(data: &str) -> Option<UnknownEvent> {
        #[derive(Deserialize)]
        struct Probe {
            event: String,
        }
        let Probe { event } = serde_json::from_str(data).ok()?;
        (!known(Self::KINDS, &event)).then_some(UnknownEvent { kind: event })
    }
}

impl Classify for FollowEvent {
    type Unknown = UnknownFollowEvent;

    const KINDS: &'static [&'static str] = &[
        "follow", "synced", "state", "load", "exposed", "retired", "left", "offering", "dropped",
    ];

    fn unknown(data: &str) -> Option<UnknownFollowEvent> {
        #[derive(Deserialize)]
        struct Probe {
            event: String,
            class: Option<String>,
            cell: Option<String>,
            generation: Option<u64>,
        }
        let probe: Probe = serde_json::from_str(data).ok()?;
        if known(Self::KINDS, &probe.event) {
            return None;
        }
        Some(UnknownFollowEvent {
            kind: probe.event,
            class: probe.class?,
            cell: probe.cell?,
            generation: probe.generation?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{CellWatchEvent, Classify, FollowEvent};
    use gdl_cells::CellState;
    use velo::{InstanceId, PeerInfo, WorkerAddress};

    fn tag(event: &impl serde::Serialize) -> String {
        serde_json::to_value(event).expect("an event serializes")["event"]
            .as_str()
            .expect("a tag")
            .to_string()
    }

    /// The tags the client knows are every kind's, and no more: a kind
    /// left out would be read as unknown, and fail its cell closed.
    #[test]
    fn the_known_tags_are_every_kind() {
        let instance = InstanceId::new_v4();
        let peer = PeerInfo::new(instance, WorkerAddress::from_encoded(b"tcp://t".to_vec()));
        // One of each kind; the match below fails to build when a kind is
        // added, until it is listed here.
        let cell: Vec<CellWatchEvent> = vec![
            CellWatchEvent::Cell {
                name: "m".into(),
                class: "c".into(),
                epoch: 1,
                state: CellState::Ready,
                load: None,
            },
            CellWatchEvent::Synced,
            CellWatchEvent::State {
                state: CellState::Ready,
            },
            CellWatchEvent::Load { load: None },
            CellWatchEvent::Ready {
                segment: "f".into(),
                peer: peer.clone(),
            },
            CellWatchEvent::Retired {
                segment: "f".into(),
                instance,
            },
            CellWatchEvent::Left {
                segment: "f".into(),
                instance,
            },
            CellWatchEvent::Unfollowed,
        ];
        for event in &cell {
            match event {
                CellWatchEvent::Cell { .. }
                | CellWatchEvent::Synced
                | CellWatchEvent::State { .. }
                | CellWatchEvent::Load { .. }
                | CellWatchEvent::Ready { .. }
                | CellWatchEvent::Retired { .. }
                | CellWatchEvent::Left { .. }
                | CellWatchEvent::Unfollowed => {}
            }
        }
        let tags: Vec<String> = cell.iter().map(tag).collect();
        assert_eq!(tags, CellWatchEvent::KINDS);

        let (class, name) = (String::from("c"), String::from("m"));
        let relay: Vec<FollowEvent> = vec![
            FollowEvent::Follow {
                class: class.clone(),
                cell: name.clone(),
                generation: 1,
                region: "r".into(),
                state: CellState::Ready,
                load: None,
            },
            FollowEvent::Synced,
            FollowEvent::State {
                class: class.clone(),
                cell: name.clone(),
                generation: 1,
                state: CellState::Ready,
            },
            FollowEvent::Load {
                class: class.clone(),
                cell: name.clone(),
                generation: 1,
                load: None,
            },
            FollowEvent::Exposed {
                class: class.clone(),
                cell: name.clone(),
                generation: 1,
                segment: "f".into(),
                peer,
            },
            FollowEvent::Retired {
                class: class.clone(),
                cell: name.clone(),
                generation: 1,
                segment: "f".into(),
                instance,
            },
            FollowEvent::Left {
                class: class.clone(),
                cell: name.clone(),
                generation: 1,
                segment: "f".into(),
                instance,
            },
            FollowEvent::Offering {
                class: class.clone(),
                cell: name.clone(),
                generation: 1,
                revision: 2,
                offered: false,
            },
            FollowEvent::Dropped {
                class,
                cell: name,
                generation: 1,
            },
        ];
        for event in &relay {
            match event {
                FollowEvent::Follow { .. }
                | FollowEvent::Synced
                | FollowEvent::State { .. }
                | FollowEvent::Load { .. }
                | FollowEvent::Exposed { .. }
                | FollowEvent::Retired { .. }
                | FollowEvent::Left { .. }
                | FollowEvent::Offering { .. }
                | FollowEvent::Dropped { .. } => {}
            }
        }
        let tags: Vec<String> = relay.iter().map(tag).collect();
        assert_eq!(tags, FollowEvent::KINDS);
        // Every event of the relay but its marker names its follow, which
        // a reader fails closed for when it does not know the event.
        for event in relay.iter().filter(|event| **event != FollowEvent::Synced) {
            let value = serde_json::to_value(event).expect("an event serializes");
            for field in ["class", "cell", "generation"] {
                assert!(value.get(field).is_some(), "{event:?} names no {field}");
            }
        }
    }
}
