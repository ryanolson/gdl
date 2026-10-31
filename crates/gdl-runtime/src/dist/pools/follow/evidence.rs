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
//! When the relay unlists a pool that a pool's own watch still shows, the
//! held value is kept only if it is of the hub the shown follow links to:
//! a drain and a condition stay with the hub that said them. Otherwise the
//! pool reads unknown until a later word: what is held is of another hub,
//! and a lagging watch can show a follow of any earlier hub, so no record
//! of each hub's word could stay complete without growing for each one.
//! Forgetting keeps the forgotten value's epoch as a fence: the pool reads
//! unknown, and a later word of an earlier hub is dropped, as the merge
//! rule drops it, so that forgetting never lowers the pool's newest hub.
//! The first word of the fenced hub or a later one ends the fence.

use std::collections::BTreeMap;
use std::sync::Arc;

use gdl::{PoolCondition, PoolEvidence};
use tokio::time::Instant;

use super::Key;

/// Each pool's evidence, with since when a missing pool has been silent.
#[derive(Default)]
pub(super) struct Evidence {
    held: BTreeMap<Key, (PoolEvidence, Option<Instant>)>,
    /// The epoch of each pool's forgotten value, while it reads unknown.
    fences: BTreeMap<Key, u64>,
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
        if self
            .fences
            .get(key)
            .is_some_and(|fence| incoming.epoch() < *fence)
        {
            return false;
        }
        self.fences.remove(key);
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
    /// as its list is, unless it withdrew, or it is of the hub of `shown`,
    /// the epoch of the pool's follow still shown, whose word it then is.
    /// What is forgotten leaves its epoch as the pool's fence.
    pub(super) fn unlist(&mut self, key: &Key, shown: Option<u64>) {
        if self
            .get(key)
            .is_some_and(|held| !held.is_withdrawn() && Some(held.epoch()) != shown)
            && let Some((held, _)) = self.held.remove(key)
        {
            self.fences.insert(key.clone(), held.epoch());
        }
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
    /// forgotten otherwise, as its listing is. A fenced pool stays fenced
    /// unless the snapshot names it at or past its fence, or forgets it.
    pub(super) fn carry(&mut self, held: Self, whole: bool) {
        for (key, fence) in held.fences {
            match self.held.get(&key) {
                Some((shown, _)) if shown.epoch() < fence => {
                    self.held.remove(&key);
                    self.fences.insert(key, fence);
                }
                Some(_) => {}
                None if !whole => {
                    self.fences.insert(key, fence);
                }
                None => {}
            }
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
