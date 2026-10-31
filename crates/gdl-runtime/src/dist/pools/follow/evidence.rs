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
//! that hub or an earlier one, or the pool is forgotten.

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

    /// Forget the drains of `key`'s pool's hubs earlier than both its
    /// evidence and `shown`, the epoch of its follow shown, if any: every
    /// one once neither is held.
    pub(super) fn settle(&mut self, key: &Key, shown: Option<u64>) {
        let held = self.get(key).map(|evidence| evidence.epoch());
        let floor = match (held, shown) {
            (Some(held), Some(shown)) => Some(held.min(shown)),
            (held, shown) => held.or(shown),
        };
        match (floor, self.drained.get_mut(key)) {
            (Some(floor), Some(drained)) => drained.retain(|epoch| *epoch >= floor),
            (None, Some(_)) => {
                self.drained.remove(key);
            }
            (_, None) => {}
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
