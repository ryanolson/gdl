use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll},
    time::Duration,
};

use futures::Stream;
use tokio::time::Instant;

use crate::{
    CellDirectory, CellEvent, CellRecord, DirectoryError, MemoryDirectory, MemoryLimits,
    MemoryWatch, ResumeToken, Version, Withdrawal,
};

/// The directory of the `relay` backend: the records a parent cell's hub
/// holds for its children, which its children's hubs report to it, in
/// the hub's process. The hub's follow watches it as it would watch any
/// other directory.
///
/// It is a [`MemoryDirectory`] whose leases run on its own clock: a record
/// that no newer write has renewed for its lease is left out of a watch
/// that begins after that, its snapshot or the changes it resumes from, as
/// a reader that had watched all along would have dropped it
/// ([`crate::CellView`]). A record scaled to 0 has no lease, and never
/// expires ([`CellRecord::leased`]). Nothing is sent when a record expires, since
/// silence is not a withdrawal, and its version stays held, so an older
/// write is still refused.
///
/// So it does not keep the contract's first rule ([`CellDirectory`]): a
/// record whose lease has passed is gone from a new watch, and every
/// record is gone when the hub restarts.
///
/// Its writes are not checked beyond the trait's rules: the hub writes to
/// it only what its children's reports carry once it has verified them.
#[derive(Debug, Clone, Default)]
pub struct RelayDirectory {
    records: MemoryDirectory,
    writes: Arc<Mutex<HashMap<(String, String), Written>>>,
}

/// The latest write the directory took for one cell, and when.
#[derive(Debug, Clone, Copy)]
struct Written {
    version: Version,
    /// The record's lease, or `None` for a withdrawal.
    lease: Option<Duration>,
    at: Instant,
}

impl RelayDirectory {
    /// An empty directory with the default limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty directory with `limits`, as [`MemoryDirectory::with_limits`].
    pub fn with_limits(limits: MemoryLimits) -> Self {
        Self {
            records: MemoryDirectory::with_limits(limits),
            writes: Arc::default(),
        }
    }

    /// How many watches of `class` are open.
    pub fn watchers(&self, class: &str) -> usize {
        self.records.watchers(class)
    }

    /// The version of the latest write the directory took for `cell` of
    /// `class`, a record or a withdrawal, expired or not.
    pub fn held(&self, class: &str, cell: &str) -> Option<Version> {
        self.lock()
            .get(&(class.to_owned(), cell.to_owned()))
            .map(|written| written.version)
    }

    /// The record the directory holds for `cell` of `class`, expired or
    /// not: a restarted child reads its cell's history from it.
    pub fn record(&self, class: &str, cell: &str) -> Option<CellRecord> {
        self.records.record(class, cell)
    }

    /// Whether the latest write the directory took for `cell` of `class`
    /// is a record, expired or not, rather than a withdrawal.
    pub fn holds_record(&self, class: &str, cell: &str) -> bool {
        self.lock()
            .get(&(class.to_owned(), cell.to_owned()))
            .is_some_and(|written| written.lease.is_some())
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<(String, String), Written>> {
        // Each write replaces one entry in one step.
        self.writes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take the next write of `cell` of `class`, at `version`, if it is
    /// newer than the last: before the write reaches a watcher, so that
    /// what [`RelayDirectory::held`] says is never behind what a watcher
    /// has seen. The directory refuses only a write that is not newer, so
    /// once this takes one, the directory holds it or a later write.
    fn take(
        &self,
        class: &str,
        cell: &str,
        version: Version,
        lease: Option<Duration>,
    ) -> Result<(), DirectoryError> {
        let mut writes = self.lock();
        let key = (class.to_owned(), cell.to_owned());
        if let Some(held) = writes.get(&key)
            && version <= held.version
        {
            return Err(DirectoryError::NotNewer {
                class: class.to_owned(),
                cell: cell.to_owned(),
                held: held.version,
                offered: version,
            });
        }
        let at = Instant::now();
        writes.insert(key, Written { version, lease, at });
        Ok(())
    }

    /// Whether the latest write of `cell` of `class` is a record that no
    /// newer write has renewed for its lease, at `now`.
    fn expired(&self, record: &CellRecord, now: Instant) -> bool {
        self.lock()
            .get(&(record.class.clone(), record.name.clone()))
            .and_then(|written| written.lease.map(|lease| (written.at, lease)))
            .is_some_and(|(at, lease)| now.saturating_duration_since(at) > lease)
    }
}

impl CellDirectory for RelayDirectory {
    type Watch = RelayWatch;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        record.check()?;
        // A record with no lease is held as one whose lease never ends.
        let lease = if record.leased() {
            record.lease
        } else {
            Duration::MAX
        };
        self.take(&record.class, &record.name, record.version(), Some(lease))?;
        self.records.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        withdrawal.check()?;
        self.take(
            &withdrawal.class,
            &withdrawal.cell,
            withdrawal.version(),
            None,
        )?;
        self.records.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> RelayWatch {
        RelayWatch {
            watch: self.records.watch(class, from),
            directory: self.clone(),
        }
    }
}

/// A watch of one class of a [`RelayDirectory`]: a [`MemoryWatch`] that
/// leaves out the records whose lease has passed.
#[derive(Debug)]
pub struct RelayWatch {
    watch: MemoryWatch,
    directory: RelayDirectory,
}

impl Stream for RelayWatch {
    type Item = Result<CellEvent, DirectoryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // A change as it happens is fresh: only a snapshot, or the changes
        // a watch resumes from, can hold an expired record.
        loop {
            let event = std::task::ready!(Pin::new(&mut this.watch).poll_next(cx));
            match &event {
                Some(Ok(CellEvent::Current(record) | CellEvent::Put { record, .. }))
                    if this.directory.expired(record, Instant::now()) => {}
                _ => return Poll::Ready(event),
            }
        }
    }
}
