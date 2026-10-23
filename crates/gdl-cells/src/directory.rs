use std::future::Future;

use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::{CellRecord, Version, Withdrawal};

/// Where cell records are published and watched: one record per cell,
/// keyed by its class and name, each written only by its own cell's hub.
///
/// A cell's writes, its publishes and its withdrawal, are ordered by
/// their [`Version`]: a write that is not newer than what the directory
/// holds for the cell is refused with [`DirectoryError::NotNewer`],
/// stores nothing, and reaches no watcher. A withdrawal leaves a
/// tombstone at its version, so a late publish cannot bring the record
/// back; a newer epoch can.
///
/// Expiry is not here: every reader enforces a record's lease on its own
/// clock ([`crate::CellView`]), whatever the backend does with TTLs, so
/// the rule is one rule.
pub trait CellDirectory: Send + Sync + 'static {
    /// A watch of one class: see [`CellDirectory::watch`]. Dropping it
    /// releases what the backend holds for it.
    type Watch: Stream<Item = Result<CellEvent, DirectoryError>> + Send + 'static;

    /// Store `record` for its cell, if it is well formed ([`CellRecord::check`])
    /// and newer than what the directory holds for the cell.
    fn publish(
        &self,
        record: CellRecord,
    ) -> impl Future<Output = Result<(), DirectoryError>> + Send;

    /// Remove a cell's record and leave a tombstone at the withdrawal's
    /// version, if it is newer than what the directory holds for the cell.
    /// A cell the directory has never held gets the tombstone all the same.
    fn withdraw(
        &self,
        withdrawal: Withdrawal,
    ) -> impl Future<Output = Result<(), DirectoryError>> + Send;

    /// Watch the records of `class`, and only those.
    ///
    /// From `None`: each current record ([`CellEvent::Current`]), then
    /// [`CellEvent::Synced`], then each change as it happens. From a token
    /// a change or a `Synced` carried: `Synced` with that token, then only
    /// the changes after it. When the backend no longer holds what follows
    /// the token, or the token is another class's, the watch yields
    /// [`CellEvent::Resnapshot`] and ends. A watcher that falls further
    /// behind than the backend buffers gets `Resnapshot` and ends too: it
    /// is neither buffered without bound nor silently skipped. After
    /// `Resnapshot`, the caller watches again from `None`.
    ///
    /// An error ends the watch.
    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch;
}

/// What a watch of one class yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellEvent {
    /// A record the directory held when the watch began: part of the
    /// snapshot. It carries no token, since resuming in the middle of a
    /// snapshot would skip the rest of it.
    Current(CellRecord),
    /// The watch is up to date as of `token`: the snapshot is complete, or
    /// a resumed watch starts here.
    Synced {
        /// Where a later watch resumes from.
        token: ResumeToken,
        /// Whether a snapshot came before it. A reader drops the cells a
        /// completed snapshot did not hold; a resumed watch drops nothing.
        snapshot: bool,
    },
    /// A record was stored.
    Put {
        /// The record.
        record: CellRecord,
        /// Where a later watch resumes from: after this change.
        token: ResumeToken,
    },
    /// A cell withdrew its record.
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
    /// cell: a record or the tombstone of its withdrawal.
    #[error("cell `{cell}` of class `{class}`: {offered} is not newer than {held}")]
    NotNewer {
        /// The cell's class.
        class: String,
        /// The cell.
        cell: String,
        /// The version the directory holds.
        held: Version,
        /// The version of the refused write.
        offered: Version,
    },
}
