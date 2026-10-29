use std::future::Future;

use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::{PoolRecord, Version, Withdrawal};

/// Where pool records are published and watched: one record per pool,
/// keyed by its class and name, each written only by its own pool's hub.
///
/// Publishing is how a pool becomes visible: its hub calls
/// [`PoolDirectory::publish`] as it starts, on each change, and as its
/// heartbeat. Where the record goes is the backend's business: a database
/// that every hub reads, the hub of a parent pool that the record is
/// reported to, a store that a top-level hub broadcasts, or a mix of
/// these. A backend can live in its own crate: `gdl_hub::hub_builder_with`
/// takes any directory.
///
/// A pool's writes, its publishes and its withdrawal, are ordered by
/// their [`Version`]: a write that is not newer than what the directory
/// holds for the pool is refused with [`DirectoryError::NotNewer`],
/// stores nothing, and reaches no watcher. A withdrawal leaves a
/// tombstone at its version, so a late publish cannot bring the record
/// back; a newer epoch can.
///
/// # The contract
///
/// A backend keeps four rules, and the conformance suite (the
/// `conformance` module, behind the feature of that name) holds each with
/// cases of its own:
///
/// 1. **Kept until withdrawn.** A published record stays until its pool
///    withdraws it, across the backend's own restart, and so does a
///    withdrawal. Nothing deletes a record by time: one whose lease has
///    passed is still in a new watch's snapshot.
/// 2. **Every record.** `watch(class)` yields every record of the class,
///    in every region, whatever its parent or hashes. The reader picks
///    the pools it follows.
/// 3. **Snapshot, then changes.** A watch gives a snapshot, then the
///    changes after it, and resumes from a token or says
///    [`PoolEvent::Resnapshot`] ([`PoolDirectory::watch`]). A backend may
///    fold several writes of one pool into the latest, as `dir` does
///    between two polls: a reader keeps only each pool's newest record,
///    so it misses no state.
/// 4. **Whole.** A record arrives as it was published, every field of it,
///    never with a part of another version. A backend that stores a part
///    apart, such as a list in a second table, installs a record only
///    once it holds the matching part.
///
/// The backends in this crate and the rules they keep today:
///
/// | Backend | 1 | 2 | 3 | 4 |
/// |---|---|---|---|---|
/// | [`crate::DirDirectory`] | yes | yes | yes | yes |
/// | [`crate::MemoryDirectory`] | no: it lives as long as its process | yes | yes | yes |
/// | relay: [`crate::RelayDirectory`], in a parent pool's hub | yes, opened over a state directory ([`crate::RelayDirectory::open`]); without one, it lives as long as its process | no: a hub holds only its own children's records | yes | yes |
///
/// Expiry is the reader's: every reader applies a record's lease on its
/// own clock ([`crate::PoolView`]), so the rule is one rule, whatever the
/// backend holds.
pub trait PoolDirectory: Send + Sync + 'static {
    /// A watch of one class: see [`PoolDirectory::watch`]. Dropping it
    /// releases what the backend holds for it.
    type Watch: Stream<Item = Result<PoolEvent, DirectoryError>> + Send + 'static;

    /// Store `record` for its pool, if it is well formed ([`PoolRecord::check`])
    /// and newer than what the directory holds for the pool.
    fn publish(
        &self,
        record: PoolRecord,
    ) -> impl Future<Output = Result<(), DirectoryError>> + Send;

    /// Remove a pool's record and leave a tombstone at the withdrawal's
    /// version, if it is newer than what the directory holds for the pool.
    /// A pool the directory has never held gets the tombstone all the same.
    fn withdraw(
        &self,
        withdrawal: Withdrawal,
    ) -> impl Future<Output = Result<(), DirectoryError>> + Send;

    /// Watch the records of `class`, and only those.
    ///
    /// From `None`: each current record ([`PoolEvent::Current`]), then
    /// [`PoolEvent::Synced`], then each change as it happens. From a token
    /// a change or a `Synced` carried: `Synced` with that token, then only
    /// the changes after it. When the backend no longer holds what follows
    /// the token, or the token is another class's, the watch yields
    /// [`PoolEvent::Resnapshot`] and ends. A watcher that falls further
    /// behind than the backend buffers gets `Resnapshot` and ends too: it
    /// is neither buffered without bound nor silently skipped. After
    /// `Resnapshot`, the caller watches again from `None`.
    ///
    /// An error ends the watch.
    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch;
}

/// What a watch of one class yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolEvent {
    /// A record the directory held when the watch began: part of the
    /// snapshot. It carries no token, since resuming in the middle of a
    /// snapshot would skip the rest of it.
    Current(PoolRecord),
    /// The watch is up to date as of `token`: the snapshot is complete, or
    /// a resumed watch starts here.
    Synced {
        /// Where a later watch resumes from.
        token: ResumeToken,
        /// Whether a snapshot came before it. A reader drops the pools a
        /// completed snapshot did not hold; a resumed watch drops nothing.
        snapshot: bool,
    },
    /// A record was stored.
    Put {
        /// The record.
        record: PoolRecord,
        /// Where a later watch resumes from: after this change.
        token: ResumeToken,
    },
    /// A pool withdrew its record.
    Withdrawn {
        /// The withdrawal, with its version.
        withdrawal: Withdrawal,
        /// Where a later watch resumes from: after this change.
        token: ResumeToken,
    },
    /// The watch cannot go on without missing changes, and ends: watch
    /// again from `None`.
    Resnapshot,
}

/// A place in one class's changes, from which a watch resumes. Opaque to
/// a reader: a backend makes it from the class and its own position in
/// that class's changes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResumeToken {
    class: String,
    position: u64,
}

impl ResumeToken {
    /// The token after the change at `position` in `class`'s changes.
    pub fn new(class: impl Into<String>, position: u64) -> Self {
        Self {
            class: class.into(),
            position,
        }
    }

    /// The class whose changes the token is a place in.
    pub fn class(&self) -> &str {
        &self.class
    }

    /// The backend's position in the class's changes.
    pub fn position(&self) -> u64 {
        self.position
    }
}

/// A write the directory refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DirectoryError {
    /// A record or withdrawal with a malformed name or a zero lease.
    #[error("{0}")]
    Malformed(String),
    /// A write that is not newer than what the directory holds for the
    /// pool: a record or the tombstone of its withdrawal.
    #[error("pool `{pool}` of class `{class}`: {offered} is not newer than {held}")]
    NotNewer {
        /// The pool's class.
        class: String,
        /// The pool.
        pool: String,
        /// The version the directory holds.
        held: Version,
        /// The version of the refused write.
        offered: Version,
    },
    /// The backend could not do what was asked: for the `dir` backend, a
    /// file or directory it could not read or write; for the `relay`
    /// backend, a parent's hub it could not reach.
    #[error("the directory is unavailable: {0}")]
    Unavailable(String),
    /// A well-formed write the backend refused for a rule of its own: the
    /// `relay` backend's parent hub refuses a record whose parent is
    /// another pool, or whose hashes are not its own.
    #[error("{0}")]
    Refused(String),
}
