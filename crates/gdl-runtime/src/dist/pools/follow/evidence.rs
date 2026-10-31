//! What the hub has said of each pool, as the follower holds it: one
//! [`gdl::PoolEvidence`] per pool, the condition, drain, and withdrawal
//! dated by the epoch of the pool's hub they are about, apart from the
//! pool's property list ([`super::listings`]).
//!
//! Every source stamps what it says with the epoch of the hub it is about:
//! a listing with its `epoch`, a follow and its state changes with the
//! epoch of the hub the follow links to, a withdrawal with its own. The
//! follower keeps the value of the latest epoch ([`gdl::PoolEvidence::merged`]),
//! whichever watch it came on, and never dates one event by another's
//! arrival: the relay's watch and a pool's own watch are not ordered with
//! each other.
//!
//! It also keeps, per pool, the epochs of the hubs said to drain, by any
//! source: a drain lasts for its hub's life, so a follow of that hub that
//! says its word again, when the relay unlists a pool its own watch still
//! shows, says the drain whatever order the words came in. An epoch is
//! forgotten once neither the pool's evidence nor its shown follow is of
//! that hub, or the pool is forgotten: at most two epochs per pool.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use gdl::{PoolCondition, PoolEvidence};
use tokio::time::Instant;

use super::Key;

/// Each pool's evidence, with since when a missing pool has been silent.
#[derive(Default)]
pub(super) struct Evidence {
    held: BTreeMap<Key, (PoolEvidence, Option<Instant>)>,
    /// The epochs of each pool's hubs said to drain.
    drained: BTreeMap<Key, BTreeSet<u64>>,
}

impl Evidence {
    /// Take `incoming`, said of `key`'s pool after what is held, with since
    /// when it has been silent if it says the pool is missing: whether the
    /// held value changed.
    pub(super) fn take(
        &mut self,
        key: &Key,
        incoming: PoolEvidence,
        silent: Option<Instant>,
    ) -> bool {
        let silent = silent.filter(|_| incoming.condition() == PoolCondition::Missing);
        if incoming.is_draining() {
            self.drained
                .entry(key.clone())
                .or_default()
                .insert(incoming.epoch());
        }
        match self.held.get_mut(key) {
            None => {
                self.held.insert(key.clone(), (incoming, silent));
                true
            }
            Some(held) => {
                let merged = held.0.merged(incoming);
                // The silence goes with the condition it dates.
                let since = if merged.epoch() == incoming.epoch() {
                    silent
                } else {
                    held.1
                };
                let changed = *held != (merged, since);
                *held = (merged, since);
                changed
            }
        }
    }

    /// The withdrawal `key`'s pool's held value says lasts is ended: a
    /// later follow of the pool was shown, which only a newer record of it
    /// starts. For a hub of an earlier build, whose epochs are all 0.
    pub(super) fn unwithdraw(&mut self, key: &Key) -> bool {
        match self.held.get_mut(key) {
            Some((held, _)) if held.is_withdrawn() => {
                *held = held.with_withdrawn(false);
                true
            }
            _ => false,
        }
    }

    /// `key`'s pool is no longer listed: what was said of it is forgotten,
    /// as its list is, unless it withdrew.
    pub(super) fn unlist(&mut self, key: &Key) {
        if self.get(key).is_some_and(|held| !held.is_withdrawn()) {
            self.held.remove(key);
        }
    }

    /// Whether the hub of `epoch` of `key`'s pool was said to drain.
    pub(super) fn said_draining(&self, key: &Key, epoch: u64) -> bool {
        self.drained
            .get(key)
            .is_some_and(|drained| drained.contains(&epoch))
    }

    /// Keep only the drains of `key`'s pool's hubs a reader can still ask
    /// for: of the hub its evidence is of, and of the hub of `shown`, its
    /// follow shown, if any. So the record holds at most two epochs, however
    /// many hubs of the pool come and go.
    pub(super) fn settle(&mut self, key: &Key, shown: Option<u64>) {
        let held = self.get(key).map(|evidence| evidence.epoch());
        let Some(drained) = self.drained.get_mut(key) else {
            return;
        };
        drained.retain(|epoch| Some(*epoch) == held || Some(*epoch) == shown);
        if drained.is_empty() {
            self.drained.remove(key);
        }
    }

    /// The pools that hold evidence or drains.
    pub(super) fn pools(&self) -> impl Iterator<Item = &Key> {
        let held = self.held.keys();
        let drained = self
            .drained
            .keys()
            .filter(|key| !self.held.contains_key(*key));
        held.chain(drained)
    }

    /// What is held of `key`'s pool.
    pub(super) fn get(&self, key: &Key) -> Option<PoolEvidence> {
        self.held.get(key).map(|(evidence, _)| *evidence)
    }

    /// Since when `key`'s pool has been silent, if it is missing.
    pub(super) fn silent(&self, key: &Key) -> Option<Instant> {
        self.held.get(key).and_then(|(_, silent)| *silent)
    }

    /// This evidence, a snapshot's, takes the place of `held`: each pool
    /// the snapshot names takes its value after the held one. A pool it
    /// does not name keeps the held value if the snapshot is not `whole`,
    /// or if it withdrew, since a snapshot holds no withdrawal; it is
    /// forgotten otherwise, as its listing is.
    pub(super) fn carry(&mut self, held: Self, whole: bool) {
        for (key, drained) in held.drained {
            self.drained.entry(key).or_default().extend(drained);
        }
        for (key, entry) in held.held {
            match self.held.get_mut(&key) {
                Some(shown) => {
                    let merged = entry.0.merged(shown.0);
                    let since = if merged.epoch() == shown.0.epoch() {
                        shown.1
                    } else {
                        entry.1
                    };
                    *shown = (merged, since);
                }
                None if !whole || entry.0.is_withdrawn() => {
                    self.held.insert(key, entry);
                }
                None => {}
            }
        }
    }

    /// Take `key`'s value from `from`, the snapshot of a watch of the
    /// pool's own, after what is held: whether it held one.
    pub(super) fn take_from(&mut self, from: &mut Self, key: &Key) -> bool {
        if let Some(drained) = from.drained.remove(key) {
            self.drained.entry(key.clone()).or_default().extend(drained);
        }
        match from.held.remove(key) {
            Some((evidence, silent)) => {
                self.take(key, evidence, silent);
                true
            }
            None => false,
        }
    }

    /// `class`'s pools' evidence, by name.
    pub(super) fn of(&self, class: &str) -> Arc<[(Box<str>, PoolEvidence)]> {
        self.held
            .range((class.to_string(), String::new())..)
            .take_while(|((of, _), _)| of == class)
            .map(|((_, pool), (evidence, _))| (Box::from(pool.as_str()), *evidence))
            .collect()
    }
}

#[test]
fn reconnect_failures_do_not_retain_every_restarted_hubs_drain() {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use gdl_pools::PoolState;
    use gdl_watch::{FollowEvent, Listing, UnknownFollowEvent, WatchEnd};

    use super::{Follows, Wanted};
    use crate::dist::fabric::Registry;
    use crate::dist::pools::{Pools, damping::Damping};
    use crate::dist::roster::Roster;

    struct QuietRegistry;
    impl Registry for QuietRegistry {
        fn register(&self, _: velo::PeerInfo) -> gdl::Result<()> {
            panic!("no instance is exposed");
        }

        fn unregister(&self, _: velo::InstanceId) {
            panic!("no instance is held");
        }
    }

    let pools = Pools::new(
        "r",
        [("model_x", "f")],
        Damping::default(),
        gdl::PropertyKinds::new(),
    );
    let wanted = Wanted::from([("model_x".into(), BTreeSet::from(["f".into()]))]);
    let mut follows = Follows::new(
        pools,
        QuietRegistry,
        Arc::clone(Roster::new().live()),
        Duration::from_secs(10),
        wanted,
        3,
        None,
    );
    let mut now = Instant::now();
    let key: Key = ("model_x".into(), "m1".into());
    follows.take(
        FollowEvent::Follow {
            class: key.0.clone(),
            pool: key.1.clone(),
            generation: 1,
            epoch: 5,
            region: "r".into(),
            state: PoolState::Ready,
            load: None,
            load_age_ms: None,
            listing: None,
        },
        now,
    );
    follows.unknown(UnknownFollowEvent {
        kind: "paused".into(),
        class: key.0.clone(),
        pool: key.1.clone(),
        generation: 1,
    });
    let mut asked = follows.asked().pop().expect("the own watch is opened");
    let mut retained = Vec::new();
    for epoch in 6..=261 {
        now += Duration::from_secs(2);
        follows.take(
            FollowEvent::Listed {
                class: key.0.clone(),
                pool: key.1.clone(),
                generation: epoch,
                listing: Some(Listing {
                    region: "r".into(),
                    condition: PoolCondition::Live,
                    properties: gdl_watch::PropertyList::default(),
                    missing_age_ms: None,
                    draining: true,
                    epoch,
                }),
            },
            now,
        );
        follows.take(
            FollowEvent::Listed {
                class: key.0.clone(),
                pool: key.1.clone(),
                generation: epoch,
                listing: None,
            },
            now,
        );
        follows.take(
            FollowEvent::Withdrawn {
                class: key.0.clone(),
                pool: key.1.clone(),
                generation: epoch,
                epoch,
                properties_version: 0,
            },
            now,
        );
        follows.own(asked.key, asked.watch, Err(WatchEnd::Failed), now);
        asked = follows.asked().pop().expect("a failed watch is reopened");
        assert_eq!(follows.followed.follows[&key].epoch, 5);
        assert!(follows.followed.follows[&key].unread);
        // At most one epoch for the follow shown, and one for the evidence.
        let held = follows
            .followed
            .evidence
            .drained
            .get(&key)
            .map_or(0, BTreeSet::len);
        assert!(held <= 2, "{held} drain epochs held at hub {epoch}");
        if epoch == 133 || epoch == 261 {
            retained.push(held);
        }
    }
    now += Duration::from_secs(2);
    follows.own(
        asked.key,
        asked.watch,
        Ok(gdl_watch::Watched::Event(FollowEvent::Synced {
            listed: true,
        })),
        now,
    );
    assert!(!follows.followed.follows.contains_key(&key));
    assert!(
        follows
            .followed
            .evidence
            .drained
            .get(&key)
            .map_or(0, BTreeSet::len)
            <= 1,
        "no follow shown: at most the evidence's hub"
    );
    assert_eq!(
        retained[1], retained[0],
        "a fixed unread follow and failed own reconnects retain {retained:?} drain epochs after 128 and 256 completed hub lives"
    );
}
