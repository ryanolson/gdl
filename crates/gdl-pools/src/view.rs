use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Instant,
};

use crate::{PoolEvent, PoolRecord, Version, Withdrawal};

/// A reader's view of one class's pools, built from a watch's events: the
/// rules every consumer applies, whatever the backend.
///
/// - Per pool, the record with the highest [`Version`] seen wins. A copy
///   that is not newer is ignored, since a record may arrive by more than
///   one path.
/// - A withdrawal removes the record and keeps its version, so a late copy
///   cannot bring the record back, also across a [`PoolEvent::Resnapshot`].
/// - A record is expired once the reader's clock shows its `seq`
///   unchanged for longer than its lease, measured from when this view saw
///   the `seq` change, never from a time in the record. Only a record
///   that is `serving` has a lease ([`PoolRecord::leased`]): one scaled to
///   0 sends no heartbeat, and never expires. An expired record
///   is not offered, and a late copy of the same version does not make it
///   fresh again; a newer one does.
/// - A completed snapshot drops the pools it did not hold, keeping the
///   version of each as a withdrawal's.
///
/// The caller passes the time, so the view never reads a clock of its own.
#[derive(Debug, Default)]
pub struct PoolView {
    pools: BTreeMap<String, Seen>,
    withdrawn: HashMap<String, Version>,
    // The pools the snapshot under way has named, so that its end can drop
    // the others.
    snapshot: HashSet<String>,
}

#[derive(Debug)]
struct Seen {
    record: PoolRecord,
    changed: Instant,
}

impl PoolView {
    /// An empty view.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one event of a watch of the view's class, at `now`.
    pub fn apply(&mut self, event: PoolEvent, now: Instant) {
        match event {
            PoolEvent::Current(record) => {
                // Present in the snapshot even when the view already has
                // this version or a newer one.
                self.snapshot.insert(record.name.clone());
                self.put(record, now);
            }
            PoolEvent::Put { record, .. } => self.put(record, now),
            PoolEvent::Withdrawn { withdrawal, .. } => self.withdraw(&withdrawal),
            PoolEvent::Synced { snapshot, .. } => {
                if snapshot {
                    // A pool the snapshot does not hold was withdrawn while
                    // the watch could not see it. Its last version seen
                    // becomes its tombstone, so a late copy of it, by
                    // another path, does not bring it back.
                    let (held, withdrawn) = (&self.snapshot, &mut self.withdrawn);
                    self.pools.retain(|pool, seen| {
                        let keep = held.contains(pool);
                        if !keep {
                            let version = seen.record.version();
                            let tombstone = withdrawn.entry(pool.clone()).or_insert(version);
                            *tombstone = (*tombstone).max(version);
                        }
                        keep
                    });
                }
                self.snapshot.clear();
            }
            // The next watch's snapshot replaces the view when it
            // completes; until then the view keeps what it has.
            PoolEvent::Resnapshot => self.snapshot.clear(),
        }
    }

    /// The record of `pool`, unless it is withdrawn, unknown, or expired at
    /// `now`.
    pub fn get(&self, pool: &str, now: Instant) -> Option<&PoolRecord> {
        self.pools
            .get(pool)
            .filter(|seen| seen.fresh(now))
            .map(|seen| &seen.record)
    }

    /// The record of `pool`, whether or not its lease has passed, with when
    /// this view saw its `seq` change, from which its lease runs: `None`
    /// once it is withdrawn, or dropped by a completed snapshot, or never
    /// seen. For a reader that keeps a silent pool in view, as a model
    /// index does, where [`PoolView::get`] hides it.
    pub fn seen(&self, pool: &str) -> Option<(&PoolRecord, Instant)> {
        self.pools
            .get(pool)
            .map(|seen| (&seen.record, seen.changed))
    }

    /// The version of `pool`'s withdrawal, if the view holds it withdrawn:
    /// by a withdrawal it was shown, or by a completed snapshot that did
    /// not hold the pool.
    pub fn withdrawn(&self, pool: &str) -> Option<Version> {
        self.withdrawn.get(pool).copied()
    }

    /// The records that are not expired at `now`, by pool name.
    pub fn records(&self, now: Instant) -> impl Iterator<Item = &PoolRecord> {
        self.pools
            .values()
            .filter(move |seen| seen.fresh(now))
            .map(|seen| &seen.record)
    }

    fn put(&mut self, record: PoolRecord, now: Instant) {
        let version = record.version();
        if let Some(&tombstone) = self.withdrawn.get(&record.name) {
            if version <= tombstone {
                return;
            }
            self.withdrawn.remove(&record.name);
        }
        match self.pools.get_mut(&record.name) {
            Some(seen) if seen.record.version() >= version => {}
            Some(seen) => {
                seen.record = record;
                seen.changed = now;
            }
            None => {
                self.pools.insert(
                    record.name.clone(),
                    Seen {
                        record,
                        changed: now,
                    },
                );
            }
        }
    }

    fn withdraw(&mut self, withdrawal: &Withdrawal) {
        let version = withdrawal.version();
        if let Some(seen) = self.pools.get(&withdrawal.pool) {
            if seen.record.version() >= version {
                return;
            }
            self.pools.remove(&withdrawal.pool);
        }
        let tombstone = self
            .withdrawn
            .entry(withdrawal.pool.clone())
            .or_insert(version);
        *tombstone = (*tombstone).max(version);
    }
}

impl Seen {
    fn fresh(&self, now: Instant) -> bool {
        !self.record.leased() || now.saturating_duration_since(self.changed) <= self.record.lease
    }
}
