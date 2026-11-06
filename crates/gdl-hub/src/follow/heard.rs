//! When a pool's hub last heard each child's record change, on the hub's
//! own clock: what decides whether an operator's command for a child
//! whose hub is gone may be given (`docs/src/concepts/pools.md`).
//!
//! A record carries no time, since clocks across hosts differ, so the hub
//! measures the lease from when it first saw the record's version, as
//! every follower does ([`gdl_pools::PoolView`]). The follower writes here
//! each record it reads, and a command writes the record it read for
//! itself: a version the follower has not seen yet is heard as the
//! command reads it, so a follower that is behind never makes a live
//! child look silent.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use gdl_pools::{PoolRecord, Version};

#[cfg(test)]
mod tests;

/// Each child's newest version heard, by class, then by pool, and when it
/// was first heard.
#[derive(Debug, Default)]
pub(crate) struct Heard {
    children: Mutex<Children>,
}

type Children = HashMap<String, HashMap<String, (Version, Instant)>>;

/// A record whose lease has not passed since its version was heard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Fresh {
    /// How long ago the version was first heard.
    pub(crate) since: Duration,
    /// How long until the lease passes, unless a newer version is heard.
    pub(crate) ends_in: Duration,
    /// The record's lease.
    pub(crate) lease: Duration,
}

impl Heard {
    /// The follower read `record`, whose version it first saw at `at`.
    pub(crate) fn heard(&self, record: &PoolRecord, at: Instant) {
        Self::note(&mut self.lock(), record, at);
    }

    /// The child `pool` of `class` withdrew at `withdrawn`: it is
    /// forgotten, unless a later version of it was heard.
    pub(crate) fn forget(&self, class: &str, pool: &str, withdrawn: Version) {
        let mut children = self.lock();
        let Some(pools) = children.get_mut(class) else {
            return;
        };
        if pools
            .get(pool)
            .is_some_and(|(version, _)| *version <= withdrawn)
        {
            pools.remove(pool);
            if pools.is_empty() {
                children.remove(class);
            }
        }
    }

    /// Whether `last`, the record a command would be written past, is
    /// fresh at `now`: `serving`, and its lease not passed since its
    /// version, or a later one, was first heard. A version not heard
    /// before is heard now.
    pub(crate) fn fresh(&self, last: &PoolRecord, now: Instant) -> Option<Fresh> {
        let (_, heard) = Self::note(&mut self.lock(), last, now);
        if !last.leased() || !gdl_pools::lease_holds(heard, last.lease, now) {
            return None;
        }
        let since = now.saturating_duration_since(heard);
        let ends_in = last.lease - since;
        Some(Fresh {
            since,
            ends_in,
            lease: last.lease,
        })
    }

    /// Take `record`, heard at `at`, if its version is newer than the one
    /// held: the newest version held, and when it was first heard.
    fn note(children: &mut Children, record: &PoolRecord, at: Instant) -> (Version, Instant) {
        let version = record.version();
        // Looked up by reference before anything is keyed, so that a
        // heartbeat of a child already heard allocates nothing.
        if let Some(held) = children
            .get_mut(&record.class)
            .and_then(|pools| pools.get_mut(&record.name))
        {
            if version > held.0 {
                *held = (version, at);
            }
            return *held;
        }
        children
            .entry(record.class.clone())
            .or_default()
            .insert(record.name.clone(), (version, at));
        (version, at)
    }

    fn lock(&self) -> MutexGuard<'_, Children> {
        self.children.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
