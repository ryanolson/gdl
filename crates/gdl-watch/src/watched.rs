//! What a watch's client reads: an event of a kind this build knows, or
//! one of a kind it does not, with the pool it concerns.

use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::{FollowEvent, PoolWatchEvent};

/// One item of a watch, as [`crate::PoolWatcher`] reads it: an event, or
/// one of a kind this build does not know.
///
/// A reader fails closed for the pool an unknown event concerns: it does
/// not route to that pool until it has read the pool again from a
/// snapshot, since the event may have said what would keep the reader
/// from it. So a later build adds what it has to say as optional fields
/// of a known event, which an earlier reader ignores, and adds a kind of
/// event only rarely: on an earlier reader, each one costs its pool's
/// eligibility until the pool is read again.
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

/// An event of a pool's watch of a kind this build does not know: it
/// concerns the watched pool, whatever else it says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownEvent {
    /// Its `event` tag.
    pub kind: String,
}

/// An event of the relay of a kind this build does not know, and the
/// follow it names, as every event of the relay but `synced` names one.
/// The client ends the relay's watch at an unknown event that names no
/// follow, since it cannot tell which pool to fail closed for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownFollowEvent {
    /// Its `event` tag.
    pub kind: String,
    /// The pool's class.
    pub class: String,
    /// The pool, named within its class.
    pub pool: String,
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
    /// no pool where the watch needs one.
    fn unknown(data: &str) -> Option<Self::Unknown>;

    /// Whether this is the marker that ends a snapshot.
    fn synced(&self) -> bool;
}

/// Whether `kind` is one of `kinds`.
fn known(kinds: &[&str], kind: &str) -> bool {
    kinds.contains(&kind)
}

impl Classify for PoolWatchEvent {
    type Unknown = UnknownEvent;

    const KINDS: &'static [&'static str] = &[
        "pool", "synced", "state", "load", "ready", "retired", "left", "models",
    ];

    fn unknown(data: &str) -> Option<UnknownEvent> {
        #[derive(Deserialize)]
        struct Probe {
            event: String,
        }
        let Probe { event } = serde_json::from_str(data).ok()?;
        (!known(Self::KINDS, &event)).then_some(UnknownEvent { kind: event })
    }

    fn synced(&self) -> bool {
        matches!(self, Self::Synced)
    }
}

impl Classify for FollowEvent {
    type Unknown = UnknownFollowEvent;

    const KINDS: &'static [&'static str] = &[
        "follow",
        "synced",
        "state",
        "load",
        "exposed",
        "retired",
        "left",
        "offering",
        "dropped",
        "withdrawn",
        "listed",
    ];

    fn unknown(data: &str) -> Option<UnknownFollowEvent> {
        #[derive(Deserialize)]
        struct Probe {
            event: String,
            class: Option<String>,
            pool: Option<String>,
            generation: Option<u64>,
        }
        let probe: Probe = serde_json::from_str(data).ok()?;
        if known(Self::KINDS, &probe.event) {
            return None;
        }
        Some(UnknownFollowEvent {
            kind: probe.event,
            class: probe.class?,
            pool: probe.pool?,
            generation: probe.generation?,
        })
    }

    fn synced(&self) -> bool {
        matches!(self, Self::Synced { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::{Classify, FollowEvent, PoolWatchEvent};
    use gdl_pools::PoolState;
    use velo::{InstanceId, PeerInfo, WorkerAddress};

    fn tag(event: &impl serde::Serialize) -> String {
        serde_json::to_value(event).expect("an event serializes")["event"]
            .as_str()
            .expect("a tag")
            .to_string()
    }

    /// The tags the client knows are every kind's, and no more: a kind
    /// left out would be read as unknown, and fail its pool closed.
    #[test]
    fn the_known_tags_are_every_kind() {
        let instance = InstanceId::new_v4();
        let peer = PeerInfo::new(instance, WorkerAddress::from_encoded(b"tcp://t".to_vec()));
        // One of each kind; the match below fails to build when a kind is
        // added, until it is listed here.
        let pool: Vec<PoolWatchEvent> = vec![
            PoolWatchEvent::Pool {
                models: None,
                name: "m".into(),
                class: "c".into(),
                epoch: 1,
                state: PoolState::Ready,
                load: None,
            },
            PoolWatchEvent::Synced,
            PoolWatchEvent::State {
                state: PoolState::Ready,
                unfollowed: false,
            },
            PoolWatchEvent::Load { load: None },
            PoolWatchEvent::Ready {
                segment: "f".into(),
                peer: peer.clone(),
            },
            PoolWatchEvent::Retired {
                segment: "f".into(),
                instance,
                acknowledged: false,
            },
            PoolWatchEvent::Left {
                segment: "f".into(),
                instance,
            },
            PoolWatchEvent::Models {
                models: crate::ModelList::default(),
            },
        ];
        for event in &pool {
            match event {
                PoolWatchEvent::Pool { .. }
                | PoolWatchEvent::Synced
                | PoolWatchEvent::State { .. }
                | PoolWatchEvent::Load { .. }
                | PoolWatchEvent::Ready { .. }
                | PoolWatchEvent::Retired { .. }
                | PoolWatchEvent::Left { .. }
                | PoolWatchEvent::Models { .. } => {}
            }
        }
        let tags: Vec<String> = pool.iter().map(tag).collect();
        assert_eq!(tags, PoolWatchEvent::KINDS);

        let (class, name) = (String::from("c"), String::from("m"));
        let relay: Vec<FollowEvent> = vec![
            FollowEvent::Follow {
                listing: None,
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
                region: "r".into(),
                state: PoolState::Ready,
                load: None,
                load_age_ms: None,
            },
            FollowEvent::Synced { listed: false },
            FollowEvent::State {
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
                state: PoolState::Ready,
            },
            FollowEvent::Load {
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
                load: None,
            },
            FollowEvent::Exposed {
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
                segment: "f".into(),
                peer,
            },
            FollowEvent::Retired {
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
                segment: "f".into(),
                instance,
            },
            FollowEvent::Left {
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
                segment: "f".into(),
                instance,
            },
            FollowEvent::Offering {
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
                revision: 2,
                offered: false,
            },
            FollowEvent::Dropped {
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
            },
            FollowEvent::Withdrawn {
                class: class.clone(),
                pool: name.clone(),
                generation: 1,
            },
            FollowEvent::Listed {
                class,
                pool: name,
                generation: 0,
                listing: None,
            },
        ];
        for event in &relay {
            match event {
                FollowEvent::Follow { .. }
                | FollowEvent::Synced { .. }
                | FollowEvent::State { .. }
                | FollowEvent::Load { .. }
                | FollowEvent::Exposed { .. }
                | FollowEvent::Retired { .. }
                | FollowEvent::Left { .. }
                | FollowEvent::Offering { .. }
                | FollowEvent::Dropped { .. }
                | FollowEvent::Withdrawn { .. }
                | FollowEvent::Listed { .. } => {}
            }
        }
        let tags: Vec<String> = relay.iter().map(tag).collect();
        assert_eq!(tags, FollowEvent::KINDS);
        // Every event of the relay but its marker names its follow, which
        // a reader fails closed for when it does not know the event.
        for event in relay.iter().filter(|event| !event.synced()) {
            let value = serde_json::to_value(event).expect("an event serializes");
            for field in ["class", "pool", "generation"] {
                assert!(value.get(field).is_some(), "{event:?} names no {field}");
            }
        }
    }

    /// A reader of the build before `unfollowed`, whose state event has no
    /// such field, reads the state that carries it as the state alone, and
    /// a state that does not carry it is sent as before.
    #[test]
    fn an_earlier_reader_reads_unfollowed_as_the_state_alone() {
        #[derive(Debug, PartialEq, serde::Deserialize)]
        #[serde(tag = "event", rename_all = "snake_case")]
        enum Earlier {
            State { state: PoolState },
        }
        let unfollowed = serde_json::to_string(&PoolWatchEvent::State {
            state: PoolState::Draining,
            unfollowed: true,
        })
        .expect("an event serializes");
        assert_eq!(
            serde_json::from_str::<Earlier>(&unfollowed).expect("an earlier reader reads it"),
            Earlier::State {
                state: PoolState::Draining
            }
        );
        let state = serde_json::to_value(PoolWatchEvent::State {
            state: PoolState::Draining,
            unfollowed: false,
        })
        .expect("an event serializes");
        assert_eq!(
            state,
            serde_json::json!({ "event": "state", "state": "draining" })
        );
    }

    /// A reader of the build before `acknowledged`, whose retirement has no
    /// such field, reads the acknowledged retirement as the retirement
    /// alone, a known kind, so that nothing withholds the pool; and a
    /// retirement not yet acknowledged is sent as before.
    #[test]
    fn an_earlier_reader_reads_an_acknowledged_retirement_as_the_retirement_alone() {
        #[derive(Debug, PartialEq, serde::Deserialize)]
        #[serde(tag = "event", rename_all = "snake_case")]
        enum Earlier {
            Retired {
                segment: String,
                instance: InstanceId,
            },
        }
        let instance = InstanceId::new_v4();
        let acknowledged = serde_json::to_string(&PoolWatchEvent::Retired {
            segment: "f".into(),
            instance,
            acknowledged: true,
        })
        .expect("an event serializes");
        assert_eq!(
            serde_json::from_str::<Earlier>(&acknowledged).expect("an earlier reader reads it"),
            Earlier::Retired {
                segment: "f".into(),
                instance
            }
        );
        assert!(
            PoolWatchEvent::unknown(&acknowledged).is_none(),
            "a known kind"
        );
        let retired = serde_json::to_value(PoolWatchEvent::Retired {
            segment: "f".into(),
            instance,
            acknowledged: false,
        })
        .expect("an event serializes");
        assert_eq!(
            retired,
            serde_json::json!({ "event": "retired", "segment": "f", "instance": instance })
        );
    }

    /// A reader of an earlier build reads the model index's fields of known
    /// events as the events it knew, and the new kinds as kinds it does not
    /// know: the relay's names its follow, so the reader fails closed for
    /// that pool alone, and reads it again.
    #[test]
    fn an_earlier_reader_reads_the_index_fields_as_it_did() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        #[serde(tag = "event", rename_all = "snake_case")]
        enum Earlier {
            Synced,
            Pool { name: String, epoch: u64 },
            Follow { pool: String, generation: u64 },
        }
        let read = |event: &serde_json::Value| -> Earlier {
            serde_json::from_value(event.clone()).expect("an earlier reader reads it")
        };
        let marker = serde_json::to_value(FollowEvent::Synced { listed: true }).expect("json");
        assert_eq!(read(&marker), Earlier::Synced);
        let pool = serde_json::to_value(PoolWatchEvent::Pool {
            name: "m".into(),
            class: "c".into(),
            epoch: 3,
            state: PoolState::Ready,
            load: None,
            models: Some(crate::ModelList {
                entries: Vec::new(),
                version: 9,
                revoked: Vec::new(),
            }),
        })
        .expect("json");
        assert_eq!(
            read(&pool),
            Earlier::Pool {
                name: "m".into(),
                epoch: 3
            }
        );
        let listing = crate::Listing {
            region: "r".into(),
            condition: gdl_pools::PoolCondition::Missing,
            models: crate::ModelList::default(),
            missing_age_ms: Some(4000),
        };
        // A listing's age is a field a reader of an earlier build ignores.
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct EarlierListing {
            region: String,
            condition: gdl_pools::PoolCondition,
        }
        let aged = serde_json::to_value(&listing).expect("json");
        assert_eq!(aged["missing_age_ms"], 4000);
        assert_eq!(
            serde_json::from_value::<EarlierListing>(aged).expect("an earlier reader reads it"),
            EarlierListing {
                region: "r".into(),
                condition: gdl_pools::PoolCondition::Missing,
            }
        );
        let follow = serde_json::to_value(FollowEvent::Follow {
            class: "c".into(),
            pool: "m".into(),
            generation: 2,
            region: "r".into(),
            state: PoolState::Ready,
            load: None,
            load_age_ms: None,
            listing: Some(listing.clone()),
        })
        .expect("json");
        assert_eq!(
            read(&follow),
            Earlier::Follow {
                pool: "m".into(),
                generation: 2
            }
        );
        // Each new kind is unknown to a reader that knew only the earlier
        // ones, and the relay's names the follow to fail closed for.
        let listed = FollowEvent::Listed {
            class: "c".into(),
            pool: "m".into(),
            generation: 2,
            listing: Some(listing),
        };
        let data = serde_json::to_string(&listed).expect("json");
        let probe: serde_json::Value = serde_json::from_str(&data).expect("json");
        assert_eq!(probe["event"], "listed");
        assert_eq!(probe["pool"], "m");
        assert_eq!(probe["generation"], 2);
        assert!(serde_json::from_str::<Earlier>(&data).is_err());
    }
}
