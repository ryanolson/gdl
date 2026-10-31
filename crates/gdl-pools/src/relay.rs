use std::{
    collections::BTreeSet,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use crate::{
    DirectoryError, LastWrite, MemoryDirectory, MemoryLimits, MemoryWatch, PoolDirectory,
    PoolRecord, ResumeToken, Version, Withdrawal,
    dir::file::{self, Files, Held},
};

/// The directory of the `relay` backend: the records a parent pool's hub
/// holds for its children, which its children's hubs report to it. The
/// hub's follow watches it as it would watch any other directory.
///
/// Its watches are a [`MemoryDirectory`]'s, in the hub's process. Opened
/// over a state directory ([`RelayDirectory::open`]), it also keeps each
/// record and withdrawal there, one file per pool,
/// `<root>/<class>/<pool>.json`, as the `dir` backend does, and reads them
/// back when it opens: so it keeps the contract's first rule
/// ([`PoolDirectory`]) across the hub's restart. A write is stored before
/// any watcher sees it, and a write that cannot be stored is refused with
/// [`DirectoryError::Unavailable`] and holds nothing, so the state never
/// lags what a watcher has seen. Without a state directory
/// ([`RelayDirectory::new`]) it keeps nothing across a restart.
///
/// A record whose lease has passed stays: silence is not a withdrawal, and
/// every reader applies the lease on its own clock ([`crate::PoolView`]).
/// A reader that starts after a record's last write, as a restarted hub's
/// follow does, takes it as fresh for one lease from when it reads it.
///
/// Its writes are not checked beyond the trait's rules: the hub writes to
/// it only what its children's reports carry once it has verified them.
#[derive(Debug, Clone, Default)]
pub struct RelayDirectory {
    records: MemoryDirectory,
    store: Option<Arc<Store>>,
}

/// Where a [`RelayDirectory`] keeps what it holds.
#[derive(Debug)]
struct Store {
    root: PathBuf,
    /// Held from a write's check of what is held to its arrival in
    /// [`RelayDirectory::records`], so that two writes of one pool reach
    /// the files and the watchers in one order. Writes come from the
    /// children's reports, at their heartbeat, never from the request
    /// path, so one lock for every pool costs nothing that matters.
    writes: Mutex<()>,
    /// The pools, by class and name, whose kept file did not decode when
    /// the directory opened: what they last wrote is unknown, so a read of
    /// it is an error, never none. A write that does not depend on what
    /// was read replaces the file, and the pool is known again.
    unknown: Mutex<BTreeSet<(String, String)>>,
}

impl RelayDirectory {
    /// An empty directory with the default limits, which keeps nothing
    /// across a restart.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty directory with `limits`, as [`MemoryDirectory::with_limits`],
    /// which keeps nothing across a restart.
    pub fn with_limits(limits: MemoryLimits) -> Self {
        Self {
            records: MemoryDirectory::with_limits(limits),
            store: None,
        }
    }

    /// The directory kept in `root`, with `limits`: what an earlier
    /// directory kept there, and each write from now on. `root` is made if
    /// it is missing.
    ///
    /// # Errors
    ///
    /// [`DirectoryError::Unavailable`] when `root` cannot be made or read,
    /// or a pool's file in it cannot be read: a directory that opened
    /// without a record it holds would read a silent pool as gone. A file
    /// that does not hold its pool's record or tombstone is passed over by
    /// its watches, as the `dir` backend passes it over, but its pool's
    /// last write is unknown ([`RelayDirectory::unknown`]): a read of it is
    /// an error, and a write numbered past a read is refused, until a write
    /// that depends on no read, or a withdrawal, replaces the file.
    pub fn open(root: impl Into<PathBuf>, limits: MemoryLimits) -> Result<Self, DirectoryError> {
        let root = root.into();
        let unavailable = |what: &str, path: &Path, error: std::io::Error| {
            DirectoryError::Unavailable(format!("{what} {}: {error}", path.display()))
        };
        std::fs::create_dir_all(&root).map_err(|error| unavailable("making", &root, error))?;
        // Positions of their own, so that a token of a directory opened
        // before over the same state is answered with `Resnapshot`.
        let records = MemoryDirectory::with_base(limits, crate::dir::base());
        let mut unknown = BTreeSet::new();
        let classes =
            std::fs::read_dir(&root).map_err(|error| unavailable("reading", &root, error))?;
        for class in classes {
            let class = class.map_err(|error| unavailable("reading", &root, error))?;
            let path = class.path();
            // Only what a write makes: a directory named for a class.
            let Some(name) = class
                .file_name()
                .to_str()
                .filter(|name| gdl_model::check_class_name(name).is_ok())
                .map(str::to_owned)
            else {
                continue;
            };
            match class.file_type() {
                Ok(kind) if kind.is_dir() => {}
                Ok(_) => continue,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(unavailable("reading", &path, error)),
            }
            let pools = file::scan(&path, &name, &Files::new())
                .map_err(|error| unavailable("reading", &path, error))?;
            for (pool, seen) in pools {
                let Some(entry) = seen.entry else {
                    unknown.insert((name.clone(), pool));
                    continue;
                };
                match (entry.record(), entry.withdrawal(&name)) {
                    (Some(record), _) => records.put(record.clone())?,
                    (None, Some(withdrawal)) => records.remove(withdrawal)?,
                    (None, None) => {}
                }
            }
        }
        Ok(Self {
            records,
            store: Some(Arc::new(Store {
                root,
                writes: Mutex::default(),
                unknown: Mutex::new(unknown),
            })),
        })
    }

    /// The state directory the directory keeps what it holds in, if it
    /// keeps it anywhere.
    pub fn root(&self) -> Option<&Path> {
        self.store.as_deref().map(|store| store.root.as_path())
    }

    /// How many watches of `class` are open.
    pub fn watchers(&self, class: &str) -> usize {
        self.records.watchers(class)
    }

    /// The version of the latest write the directory took for `pool` of
    /// `class`, a record or a withdrawal, expired or not.
    pub fn held(&self, class: &str, pool: &str) -> Option<Version> {
        self.records.held(class, pool).map(|(version, _)| version)
    }

    /// The record the directory holds for `pool` of `class`, expired or
    /// not: a restarted child reads its pool's history from it.
    pub fn record(&self, class: &str, pool: &str) -> Option<PoolRecord> {
        self.records.record(class, pool)
    }

    /// The last write the directory took for `pool` of `class`, its record
    /// or the tombstone of its withdrawal: a restarted child numbers past
    /// it.
    ///
    /// # Errors
    ///
    /// [`DirectoryError::Unavailable`] for a pool whose kept file did not
    /// decode when the directory opened, until a write replaces it.
    pub fn last(&self, class: &str, pool: &str) -> Result<Option<LastWrite>, DirectoryError> {
        if self.unknown(class, pool) {
            return Err(DirectoryError::Unavailable(format!(
                "the kept file of pool `{pool}` of class `{class}` does not decode"
            )));
        }
        Ok(self.records.last(class, pool))
    }

    /// Whether the kept file of `pool` of `class` did not decode when the
    /// directory opened, and no write has replaced it since: what the pool
    /// last wrote is unknown.
    pub fn unknown(&self, class: &str, pool: &str) -> bool {
        self.store.as_ref().is_some_and(|store| {
            store
                .unknown
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&(class.to_owned(), pool.to_owned()))
        })
    }

    /// Whether the latest write the directory took for `pool` of `class`
    /// is a record, expired or not, rather than a withdrawal.
    pub fn holds_record(&self, class: &str, pool: &str) -> bool {
        self.records
            .held(class, pool)
            .is_some_and(|(_, record)| record)
    }

    /// Take `write` of `pool` of `class`: store it, if the directory
    /// keeps what it holds, and then hand it to the watchers, unless it is
    /// not newer than what the directory holds. It runs on a blocking
    /// thread, whole, so a caller that stops waiting cannot leave a write
    /// stored and not taken.
    async fn write(
        &self,
        write: Write,
        after: Option<Option<Version>>,
    ) -> Result<(), DirectoryError> {
        let Some(store) = self.store.clone() else {
            return write.apply(&self.records, after);
        };
        let records = self.records.clone();
        tokio::task::spawn_blocking(move || {
            let _writes = store.writes.lock().unwrap_or_else(PoisonError::into_inner);
            let (class, pool) = write.pool();
            let key = (class.to_owned(), pool.to_owned());
            let mut unknown = store.unknown.lock().unwrap_or_else(PoisonError::into_inner);
            // A write numbered past a read cannot land on a write that is
            // unknown; one that depends on no read replaces it.
            if after.is_some() && unknown.contains(&key) {
                return Err(DirectoryError::Unavailable(format!(
                    "the kept file of pool `{pool}` of class `{class}` does not decode"
                )));
            }
            let held = records.held(class, pool).map(|(held, _)| held);
            if let Some(refused) =
                crate::directory::refused(class, pool, held, write.version(), after)
            {
                return Err(refused);
            }
            file::write(&store.root.join(class), class, pool, write.held(), None)?;
            write.apply(&records, after)?;
            unknown.remove(&key);
            Ok(())
        })
        .await
        .map_err(|error| DirectoryError::Unavailable(error.to_string()))?
    }
}

impl PoolDirectory for RelayDirectory {
    type Watch = MemoryWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        // Before any path is made of its names.
        record.check()?;
        self.write(Write::Record(record), None).await
    }

    async fn publish_after(
        &self,
        record: PoolRecord,
        last: Option<Version>,
    ) -> Result<(), DirectoryError> {
        record.check()?;
        self.write(Write::Record(record), Some(last)).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        withdrawal.check()?;
        self.write(Write::Withdrawal(withdrawal), None).await
    }

    async fn last_write(
        &self,
        class: &str,
        pool: &str,
    ) -> Result<Option<LastWrite>, DirectoryError> {
        self.last(class, pool)
    }

    /// A resume token is a place in this directory's watches: a directory
    /// opened again over the same state answers an earlier one's token
    /// with [`crate::PoolEvent::Resnapshot`].
    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.records.watch(class, from)
    }
}

/// A write of one pool.
// Made once a child's report and taken at once: boxing the record would
// add an allocation to each write for nothing.
#[allow(clippy::large_enum_variant)]
enum Write {
    Record(PoolRecord),
    Withdrawal(Withdrawal),
}

impl Write {
    fn pool(&self) -> (&str, &str) {
        match self {
            Write::Record(record) => (&record.class, &record.name),
            Write::Withdrawal(withdrawal) => (&withdrawal.class, &withdrawal.pool),
        }
    }

    fn version(&self) -> Version {
        match self {
            Write::Record(record) => record.version(),
            Write::Withdrawal(withdrawal) => withdrawal.version(),
        }
    }

    /// What the pool's file holds once the write is stored.
    fn held(&self) -> Held {
        match self {
            Write::Record(record) => Held::Record(Box::new(record.clone())),
            Write::Withdrawal(withdrawal) => Held::tombstone(withdrawal.clone()),
        }
    }

    /// Take the write into `records`, which hands it to their watchers.
    fn apply(
        self,
        records: &MemoryDirectory,
        after: Option<Option<Version>>,
    ) -> Result<(), DirectoryError> {
        match self {
            Write::Record(record) => records.put_after(record, after),
            Write::Withdrawal(withdrawal) => records.remove(withdrawal),
        }
    }
}
