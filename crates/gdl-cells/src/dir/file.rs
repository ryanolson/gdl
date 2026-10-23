//! A class's directory on disk: one file per cell, `<cell>.json`, each
//! replaced whole by a rename, and the scan that reads them.

use std::{
    collections::HashMap,
    fs::Metadata,
    io::{self, ErrorKind},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{CellEvent, CellRecord, DirectoryError, ResumeToken, Version, Withdrawal};

/// What a scan found in a class's directory, by cell.
pub(super) type Files = HashMap<String, Seen>;

/// A cell's file as a scan last found it.
#[derive(Debug, Clone)]
pub(super) struct Seen {
    stamp: Stamp,
    // When the file was read, on this host's clock; see `Seen::unchanged`.
    read: SystemTime,
    // None for a file that does not hold its cell's record or tombstone.
    pub(super) entry: Option<Arc<Entry>>,
}

/// A cell's file: its record or its withdrawal's tombstone, and where the
/// write falls among the writes of the process that made it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Entry {
    // A scan finds several changes at once and has no other way to order
    // them: modification times share the file system's tick. Wall-clock
    // nanoseconds, raised past the process's last, so one process's writes
    // are in order and different processes' writes, of different cells,
    // in about the order they were made.
    order: u64,
    #[serde(flatten)]
    held: Held,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Held {
    Record(CellRecord),
    Withdrawn(Tombstone),
}

/// What a withdrawal leaves in its cell's file, so that a late write
/// cannot bring the record back, from this process or another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Tombstone {
    cell: String,
    epoch: u64,
    seq: u64,
}

impl Entry {
    pub(super) fn record(&self) -> Option<&CellRecord> {
        match &self.held {
            Held::Record(record) => Some(record),
            Held::Withdrawn(_) => None,
        }
    }

    fn version(&self) -> Version {
        match &self.held {
            Held::Record(record) => record.version(),
            Held::Withdrawn(tombstone) => Version {
                epoch: tombstone.epoch,
                seq: tombstone.seq,
            },
        }
    }

    /// Whether `class/cell.json` may hold the entry: it is the record or
    /// tombstone of that cell, and a record a publish would accept.
    fn belongs(&self, class: &str, cell: &str) -> bool {
        match &self.held {
            Held::Record(record) => {
                record.name == cell && record.class == class && record.check().is_ok()
            }
            Held::Withdrawn(tombstone) => tombstone.cell == cell,
        }
    }

    /// The change this entry makes, as a watch reports it at `token`.
    pub(super) fn event(&self, class: &str, token: ResumeToken) -> CellEvent {
        match &self.held {
            Held::Record(record) => CellEvent::Put {
                record: record.clone(),
                token,
            },
            Held::Withdrawn(tombstone) => CellEvent::Withdrawn {
                withdrawal: Withdrawal {
                    class: class.to_owned(),
                    cell: tombstone.cell.clone(),
                    epoch: tombstone.epoch,
                    seq: tombstone.seq,
                },
                token,
            },
        }
    }

    /// Where the entry falls among the changes one scan finds.
    pub(super) fn order(&self) -> u64 {
        self.order
    }
}

impl Held {
    pub(super) fn tombstone(withdrawal: Withdrawal) -> Self {
        Held::Withdrawn(Tombstone {
            cell: withdrawal.cell,
            epoch: withdrawal.epoch,
            seq: withdrawal.seq,
        })
    }
}

/// The metadata a scan compares to tell that a file may have changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stamp {
    len: u64,
    modified: SystemTime,
    // On unix, the change time, which every write and rename sets and no
    // call sets back: a copy that preserves times restores a file's
    // modification time, but not this. Elsewhere, the modification time.
    changed: SystemTime,
    // On unix, the inode: a rename puts another in the cell's place.
    inode: u64,
}

impl Stamp {
    fn of(metadata: &Metadata) -> io::Result<Self> {
        let modified = metadata.modified()?;
        #[cfg(unix)]
        let (changed, inode) = {
            use std::os::unix::fs::MetadataExt;
            let since = Duration::new(
                u64::try_from(metadata.ctime()).unwrap_or(0),
                u32::try_from(metadata.ctime_nsec()).unwrap_or(0),
            );
            (UNIX_EPOCH + since, metadata.ino())
        };
        #[cfg(not(unix))]
        let (changed, inode) = (modified, 0);
        Ok(Self {
            len: metadata.len(),
            modified,
            changed,
            inode,
        })
    }
}

/// How long after a file's last change a scan still reads it, though its
/// stamp is unchanged. Two writes in one tick of the file system's clock,
/// which runs coarser than this host's and may lag it, can leave the stamp
/// as it was, since a freed inode is used again; past this, a later write
/// cannot share the stamp.
const SETTLED: Duration = Duration::from_secs(1);

impl Seen {
    fn unchanged(&self, stamp: Stamp) -> bool {
        self.stamp == stamp
            && stamp
                .changed
                .checked_add(SETTLED)
                .is_some_and(|settled| settled <= self.read)
    }
}

/// Read the class directory `dir` of `class`: each cell's file, reusing
/// what `previous` read of a file that has not changed since. A missing
/// directory holds no cells. A file that does not hold its cell's entry is
/// kept without one, and one that is not a cell's (a writer's temporary
/// file, say) is passed over; a cell's file that cannot be read is an
/// error, which a writer meets too.
pub(super) fn scan(dir: &Path, class: &str, previous: &Files) -> io::Result<Files> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Files::new()),
        Err(error) => return Err(error),
    };
    let mut files = Files::with_capacity(previous.len());
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(cell) = name.to_str().and_then(|name| name.strip_suffix(".json")) else {
            continue;
        };
        if gdl_model::check_cell_name(cell).is_err() {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            // Gone since the listing.
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !metadata.is_file() {
            continue;
        }
        let stamp = Stamp::of(&metadata)?;
        if let Some(seen) = previous.get(cell).filter(|seen| seen.unchanged(stamp)) {
            files.insert(cell.to_owned(), seen.clone());
            continue;
        }
        let read = SystemTime::now();
        let entry = match load(&entry.path(), class, cell) {
            Ok(entry) => entry.map(Arc::new),
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        files.insert(cell.to_owned(), Seen { stamp, read, entry });
    }
    Ok(files)
}

/// Write `held` as `cell`'s file in the class directory `dir`, if it is
/// newer than what the file holds: in a temporary file beside it, renamed
/// over it, so that a reader finds the old file or the new one, whole. A
/// file that does not hold the cell's entry is replaced.
///
/// The read and the rename are not one step: two processes writing one
/// cell is a misconfiguration this does not arbitrate.
pub(super) fn write(dir: &Path, class: &str, cell: &str, held: Held) -> Result<(), DirectoryError> {
    let path = dir.join(format!("{cell}.json"));
    let unavailable = |what: &str, error: io::Error| {
        DirectoryError::Unavailable(format!("{what} {}: {error}", path.display()))
    };
    let entry = Entry {
        order: next_order(),
        held,
    };
    match load(&path, class, cell) {
        Ok(Some(current)) if entry.version() <= current.version() => {
            return Err(DirectoryError::NotNewer {
                class: class.to_owned(),
                cell: cell.to_owned(),
                held: current.version(),
                offered: entry.version(),
            });
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        // Unreadable, as distinct from not parsing: the version it holds is
        // unknown, so it is not written over.
        Err(error) => return Err(unavailable("reading", error)),
    }
    let bytes = serde_json::to_vec(&entry).expect("an entry serializes");
    std::fs::create_dir_all(dir).map_err(|error| unavailable("making the directory of", error))?;
    // A name of the process and a count, so that two writers in one
    // process, or in two, never share one; the leading dot keeps it out
    // of every scan.
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let partial = dir.join(format!(
        ".{cell}.json.{}.{}.tmp",
        std::process::id(),
        WRITES.fetch_add(1, Ordering::Relaxed)
    ));
    let written = std::fs::write(&partial, bytes).and_then(|()| std::fs::rename(&partial, &path));
    written.map_err(|error| {
        let _ = std::fs::remove_file(&partial);
        unavailable("writing", error)
    })
}

/// The entry `path` holds for `class/cell`; None when it holds anything
/// else.
fn load(path: &Path, class: &str, cell: &str) -> io::Result<Option<Entry>> {
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice::<Entry>(&bytes)
        .ok()
        .filter(|entry| entry.belongs(class, cell)))
}

/// The order of a write made now: this host's wall clock in nanoseconds,
/// and always past the last write's, though the clock steps back.
fn next_order() -> u64 {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        });
    let previous = LAST
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
            Some(now.max(last.saturating_add(1)))
        })
        .expect("the update always succeeds");
    now.max(previous.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::{SETTLED, Seen, Stamp};

    /// The settling is measured from the change time, which nothing sets
    /// back, never from the modification time, which a copy restores.
    #[test]
    fn a_stamp_is_trusted_only_once_its_file_has_settled() {
        let changed = SystemTime::now();
        let stamp = Stamp {
            len: 10,
            modified: changed - Duration::from_secs(60),
            changed,
            inode: 7,
        };
        let read = |after: Duration| Seen {
            stamp,
            read: changed + after,
            entry: None,
        };
        assert!(!read(SETTLED / 2).unchanged(stamp), "read again");
        assert!(read(SETTLED).unchanged(stamp), "settled");
        let replaced = Stamp { inode: 8, ..stamp };
        assert!(!read(SETTLED * 60).unchanged(replaced), "another file");
    }
}
