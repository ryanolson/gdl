use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use crate::{
    CellDirectory, CellRecord, DirectoryError, MemoryDirectory, MemoryLimits, MemoryWatch,
    ResumeToken, Version, Withdrawal,
    dir::file::{self, Files, Held},
};

/// The directory of the `relay` backend: the records a parent cell's hub
/// holds for its children, which its children's hubs report to it. The
/// hub's follow watches it as it would watch any other directory.
///
/// Its watches are a [`MemoryDirectory`]'s, in the hub's process. Opened
/// over a state directory ([`RelayDirectory::open`]), it also keeps each
/// record and withdrawal there, one file per cell,
/// `<root>/<class>/<cell>.json`, as the `dir` backend does, and reads them
/// back when it opens: so it keeps the contract's first rule
/// ([`CellDirectory`]) across the hub's restart. A write is stored before
/// any watcher sees it, and a write that cannot be stored is refused with
/// [`DirectoryError::Unavailable`] and holds nothing, so the state never
/// lags what a watcher has seen. Without a state directory
/// ([`RelayDirectory::new`]) it keeps nothing across a restart.
///
/// A record whose lease has passed stays: silence is not a withdrawal, and
/// every reader applies the lease on its own clock ([`crate::CellView`]).
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
    /// [`RelayDirectory::records`], so that two writes of one cell reach
    /// the files and the watchers in one order. Writes come from the
    /// children's reports, at their heartbeat, never from the request
    /// path, so one lock for every cell costs nothing that matters.
    writes: Mutex<()>,
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
    /// or a cell's file in it cannot be read: a directory that opened
    /// without a record it holds would read a silent cell as gone. A file
    /// that does not hold its cell's record or tombstone is passed over, as
    /// the `dir` backend passes it over, and the cell's next write
    /// replaces it.
    pub fn open(root: impl Into<PathBuf>, limits: MemoryLimits) -> Result<Self, DirectoryError> {
        let root = root.into();
        let unavailable = |what: &str, path: &Path, error: std::io::Error| {
            DirectoryError::Unavailable(format!("{what} {}: {error}", path.display()))
        };
        std::fs::create_dir_all(&root).map_err(|error| unavailable("making", &root, error))?;
        // Positions of their own, so that a token of a directory opened
        // before over the same state is answered with `Resnapshot`.
        let records = MemoryDirectory::with_base(limits, crate::dir::base());
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
            let cells = file::scan(&path, &name, &Files::new())
                .map_err(|error| unavailable("reading", &path, error))?;
            for entry in cells.into_values().filter_map(|seen| seen.entry) {
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

    /// The version of the latest write the directory took for `cell` of
    /// `class`, a record or a withdrawal, expired or not.
    pub fn held(&self, class: &str, cell: &str) -> Option<Version> {
        self.records.held(class, cell).map(|(version, _)| version)
    }

    /// The record the directory holds for `cell` of `class`, expired or
    /// not: a restarted child reads its cell's history from it.
    pub fn record(&self, class: &str, cell: &str) -> Option<CellRecord> {
        self.records.record(class, cell)
    }

    /// Whether the latest write the directory took for `cell` of `class`
    /// is a record, expired or not, rather than a withdrawal.
    pub fn holds_record(&self, class: &str, cell: &str) -> bool {
        self.records
            .held(class, cell)
            .is_some_and(|(_, record)| record)
    }

    /// Take `write` of `cell` of `class`: store it, if the directory
    /// keeps what it holds, and then hand it to the watchers, unless it is
    /// not newer than what the directory holds. It runs on a blocking
    /// thread, whole, so a caller that stops waiting cannot leave a write
    /// stored and not taken.
    async fn write(&self, write: Write) -> Result<(), DirectoryError> {
        let Some(store) = self.store.clone() else {
            return write.apply(&self.records);
        };
        let records = self.records.clone();
        tokio::task::spawn_blocking(move || {
            let _writes = store.writes.lock().unwrap_or_else(PoisonError::into_inner);
            let (class, cell) = write.cell();
            let offered = write.version();
            if let Some((held, _)) = records.held(class, cell)
                && offered <= held
            {
                return Err(DirectoryError::NotNewer {
                    class: class.to_owned(),
                    cell: cell.to_owned(),
                    held,
                    offered,
                });
            }
            file::write(&store.root.join(class), class, cell, write.held())?;
            write.apply(&records)
        })
        .await
        .map_err(|error| DirectoryError::Unavailable(error.to_string()))?
    }
}

impl CellDirectory for RelayDirectory {
    type Watch = MemoryWatch;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        // Before any path is made of its names.
        record.check()?;
        self.write(Write::Record(record)).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        withdrawal.check()?;
        self.write(Write::Withdrawal(withdrawal)).await
    }

    /// A resume token is a place in this directory's watches: a directory
    /// opened again over the same state answers an earlier one's token
    /// with [`crate::CellEvent::Resnapshot`].
    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.records.watch(class, from)
    }
}

/// A write of one cell.
enum Write {
    Record(CellRecord),
    Withdrawal(Withdrawal),
}

impl Write {
    fn cell(&self) -> (&str, &str) {
        match self {
            Write::Record(record) => (&record.class, &record.name),
            Write::Withdrawal(withdrawal) => (&withdrawal.class, &withdrawal.cell),
        }
    }

    fn version(&self) -> Version {
        match self {
            Write::Record(record) => record.version(),
            Write::Withdrawal(withdrawal) => withdrawal.version(),
        }
    }

    /// What the cell's file holds once the write is stored.
    fn held(&self) -> Held {
        match self {
            Write::Record(record) => Held::Record(Box::new(record.clone())),
            Write::Withdrawal(withdrawal) => Held::tombstone(withdrawal.clone()),
        }
    }

    /// Take the write into `records`, which hands it to their watchers.
    fn apply(self, records: &MemoryDirectory) -> Result<(), DirectoryError> {
        match self {
            Write::Record(record) => records.put(record),
            Write::Withdrawal(withdrawal) => records.remove(withdrawal),
        }
    }
}
