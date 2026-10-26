use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Instant,
};

use crate::{CellEvent, CellRecord, Version, Withdrawal};

/// A reader's view of one class's cells, built from a watch's events: the
/// rules every consumer applies, whatever the backend.
///
/// - Per cell, the record with the highest [`Version`] seen wins. A copy
///   that is not newer is ignored, since a record may arrive by more than
///   one path.
/// - A withdrawal removes the record and keeps its version, so a late copy
///   cannot bring the record back, also across a [`CellEvent::Resnapshot`].
/// - A record is expired once the reader's clock shows its `seq`
///   unchanged for longer than its lease, measured from when this view saw
///   the `seq` change, never from a time in the record. An expired record
///   is not offered, and a late copy of the same version does not make it
///   fresh again; a newer one does.
/// - A completed snapshot drops the cells it did not hold, keeping the
///   version of each as a withdrawal's.
///
/// The caller passes the time, so the view never reads a clock of its own.
#[derive(Debug, Default)]
pub struct CellView {
    cells: BTreeMap<String, Seen>,
    withdrawn: HashMap<String, Version>,
    // The cells the snapshot under way has named, so that its end can drop
    // the others.
    snapshot: HashSet<String>,
}

#[derive(Debug)]
struct Seen {
    record: CellRecord,
    changed: Instant,
}

impl CellView {
    /// An empty view.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one event of a watch of the view's class, at `now`.
    pub fn apply(&mut self, event: CellEvent, now: Instant) {
        match event {
            CellEvent::Current(record) => {
                // Present in the snapshot even when the view already has
                // this version or a newer one.
                self.snapshot.insert(record.name.clone());
                self.put(record, now);
            }
            CellEvent::Put { record, .. } => self.put(record, now),
            CellEvent::Withdrawn { withdrawal, .. } => self.withdraw(&withdrawal),
            CellEvent::Synced { snapshot, .. } => {
                if snapshot {
                    // A cell the snapshot does not hold was withdrawn while
                    // the watch could not see it. Its last version seen
                    // becomes its tombstone, so a late copy of it, by
                    // another path, does not bring it back.
                    let (held, withdrawn) = (&self.snapshot, &mut self.withdrawn);
                    self.cells.retain(|cell, seen| {
                        let keep = held.contains(cell);
                        if !keep {
                            let version = seen.record.version();
                            let tombstone = withdrawn.entry(cell.clone()).or_insert(version);
                            *tombstone = (*tombstone).max(version);
                        }
                        keep
                    });
                }
                self.snapshot.clear();
            }
            // The next watch's snapshot replaces the view when it
            // completes; until then the view keeps what it has.
            CellEvent::Resnapshot => self.snapshot.clear(),
        }
    }

    /// The record of `cell`, unless it is withdrawn, unknown, or expired at
    /// `now`.
    pub fn get(&self, cell: &str, now: Instant) -> Option<&CellRecord> {
        self.cells
            .get(cell)
            .filter(|seen| seen.fresh(now))
            .map(|seen| &seen.record)
    }

    /// The version of `cell`'s withdrawal, if the view holds it withdrawn:
    /// by a withdrawal it was shown, or by a completed snapshot that did
    /// not hold the cell.
    pub fn withdrawn(&self, cell: &str) -> Option<Version> {
        self.withdrawn.get(cell).copied()
    }

    /// The records that are not expired at `now`, by cell name.
    pub fn records(&self, now: Instant) -> impl Iterator<Item = &CellRecord> {
        self.cells
            .values()
            .filter(move |seen| seen.fresh(now))
            .map(|seen| &seen.record)
    }

    fn put(&mut self, record: CellRecord, now: Instant) {
        let version = record.version();
        if let Some(&tombstone) = self.withdrawn.get(&record.name) {
            if version <= tombstone {
                return;
            }
            self.withdrawn.remove(&record.name);
        }
        match self.cells.get_mut(&record.name) {
            Some(seen) if seen.record.version() >= version => {}
            Some(seen) => {
                seen.record = record;
                seen.changed = now;
            }
            None => {
                self.cells.insert(
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
        if let Some(seen) = self.cells.get(&withdrawal.cell) {
            if seen.record.version() >= version {
                return;
            }
            self.cells.remove(&withdrawal.cell);
        }
        let tombstone = self
            .withdrawn
            .entry(withdrawal.cell.clone())
            .or_insert(version);
        *tombstone = (*tombstone).max(version);
    }
}

impl Seen {
    fn fresh(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.changed) <= self.record.lease
    }
}
