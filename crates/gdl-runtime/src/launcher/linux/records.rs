//! The launcher's records on the shared file system: each file written
//! once, to a name of its own in the same directory, flushed, and then
//! linked to its name, which fails if the name is taken, so that a record
//! is never rewritten, and a reader on another client that opens it by
//! name sees all of it or nothing. A network file system shows a linked or
//! renamed file to an `open` by name on another client, with close-to-open
//! consistency; a name not yet seen may take as long as the client's
//! attribute cache to appear, which delays a reader and never misleads it.

use std::io::Write as _;
use std::path::{Path, PathBuf};

/// The records of one launcher, in its own directory.
pub(super) struct Records {
    dir: PathBuf,
}

impl Records {
    /// The records in `dir`, made if it does not exist.
    pub(super) fn make(dir: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        Ok(Self { dir })
    }

    /// Write record `name` once, holding `text`: a record already written
    /// stays as it is.
    pub(super) fn write(&self, name: &str, text: &str) -> Result<(), String> {
        write_once(&self.dir, name, text)
    }

    /// Record `name`, read by name: `None` when it is not there yet.
    pub(super) fn read(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(self.dir.join(name)).ok()
    }

    /// Whether record `name` is there.
    pub(super) fn exists(&self, name: &str) -> bool {
        self.dir.join(name).exists()
    }
}

/// Write `text` to `dir/name` once: to a name of this process's own,
/// flushed, then linked to `name`. A record is written once: a `name`
/// that another write took first stays as it is, and is said so on
/// stderr. A write cut short leaves only the staged file, which no reader
/// opens.
fn write_once(dir: &Path, name: &str, text: &str) -> Result<(), String> {
    let staged = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    let target = dir.join(name);
    let written = (|| {
        let mut file = std::fs::File::create(&staged)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()
    })();
    let linked = written.and_then(|()| std::fs::hard_link(&staged, &target));
    let settled = settle(&staged, &target, linked);
    let _ = std::fs::remove_file(&staged);
    match settled {
        Ok(true) => Ok(()),
        Ok(false) => {
            eprintln!(
                "launcher: {} held a record already, which stays",
                target.display()
            );
            Ok(())
        }
        Err(e) => Err(format!("{}: {e}", target.display())),
    }
}

/// Whether the link of `staged` to `target` made `target`, from what the
/// link returned. An NFS `LINK` is not idempotent: when its reply is
/// lost, the client sends it again, and the server can answer `EEXIST` to
/// a link that succeeded. So on `EEXIST`, `target` is this write's own
/// when it is `staged`'s inode; otherwise another write made it first.
pub(super) fn settle(
    staged: &Path,
    target: &Path,
    linked: std::io::Result<()>,
) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt as _;
    match linked {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let theirs = std::fs::metadata(target)?;
            Ok(std::fs::metadata(staged)
                .is_ok_and(|ours| ours.ino() == theirs.ino() && ours.dev() == theirs.dev()))
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `EEXIST` from a link that succeeded, as a retransmitted NFS `LINK`
    /// answers, is this write's own record; from another's record, it is
    /// not.
    #[test]
    fn eexist_after_a_link_that_made_the_record_is_ours() {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-launcher-link-")
            .tempdir()
            .expect("a scratch directory");
        let (staged, target) = (
            scratch.path().join(".1.run.tmp"),
            scratch.path().join("1.run"),
        );
        std::fs::write(&staged, "a1\n").expect("staged");
        std::fs::hard_link(&staged, &target).expect("linked");
        let eexist = || Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists));
        assert!(settle(&staged, &target, eexist()).expect("settled"));
        let other = scratch.path().join(".1.run.other.tmp");
        std::fs::write(&other, "b2\n").expect("another's");
        assert!(!settle(&other, &target, eexist()).expect("settled"));
        assert!(settle(&other, &target, Ok(())).expect("settled"));
    }
}
