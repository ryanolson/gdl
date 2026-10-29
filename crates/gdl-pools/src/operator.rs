use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;

use crate::{CellDirectory, CellEvent, CellRecord, DirectoryError, Presence, Version, Withdrawal};

/// What an operator says of a cell whose hub cannot say it: the hub is
/// gone, and silence never removes a cell or says that it was scaled
/// to 0.
///
/// A command is a write of the cell's record, at an epoch past the last
/// record's: a new writer, as a restarted hub is. So a late write of the
/// old epoch, a heartbeat that was on its way or a hub that was cut off
/// and comes back, is refused as not newer, and cannot undo the command.
/// A hub of the cell that starts again takes an epoch past the command's,
/// as it does past any record it finds, and writes over it. So does a hub
/// that still runs but none of whose writes was answered.
///
/// For a cell whose hub still answers, the hub's own drain and presence
/// say the same, and keep its epoch: a command fences that hub's writes
/// until it restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CellCommand {
    /// Remove the cell's record, and leave a tombstone at the command's
    /// version: its models leave every reader's list, and its parent's
    /// choosers hold it withdrawn.
    Withdraw,
    /// Write the cell's last record again, saying that it was scaled to 0
    /// on purpose ([`Presence::ScaledToZero`]): it has no lease, so a
    /// reader shows it as scaled to 0, not missing. Its state, models, and
    /// load stay as the last record said.
    ScaleToZero,
}

impl CellCommand {
    /// The write that carries the command for the cell whose last record
    /// is `last`, at `now`: at epoch `now` in milliseconds, or the epoch
    /// after `last`'s when that is later, and sequence number 1.
    pub fn write(self, last: &CellRecord, now: SystemTime) -> CommandWrite {
        let now = now.duration_since(UNIX_EPOCH).map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
        let (epoch, seq) = (now.max(last.epoch.saturating_add(1)), 1);
        match self {
            Self::Withdraw => CommandWrite::Withdrawal(Withdrawal {
                class: last.class.clone(),
                cell: last.name.clone(),
                epoch,
                seq,
            }),
            Self::ScaleToZero => CommandWrite::Record(Box::new(CellRecord {
                presence: Presence::ScaledToZero,
                epoch,
                seq,
                ..last.clone()
            })),
        }
    }

    /// Give the command for `cell` of `class` through `directory`: read the
    /// cell's last record, and write the command past it. The version
    /// written, or `None` when the directory holds no record of the cell,
    /// which leaves nothing to command and writes nothing.
    ///
    /// The record is read from a new watch's snapshot, which holds a
    /// record whose lease has passed, as the directory's first rule says.
    /// Under the `relay` backend, the records are a parent's hub's, and
    /// that hub gives the command itself.
    pub async fn give(
        self,
        directory: &impl CellDirectory,
        class: &str,
        cell: &str,
    ) -> Result<Option<Version>, DirectoryError> {
        let Some(last) = last_record(directory, class, cell).await? else {
            return Ok(None);
        };
        let write = self.write(&last, SystemTime::now());
        let version = write.version();
        write.to(directory).await?;
        Ok(Some(version))
    }
}

/// The write that carries a [`CellCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandWrite {
    /// A record: the cell scaled to 0.
    Record(Box<CellRecord>),
    /// The cell's withdrawal.
    Withdrawal(Withdrawal),
}

impl CommandWrite {
    /// The write's place in the cell's order of writes.
    pub fn version(&self) -> Version {
        match self {
            Self::Record(record) => record.version(),
            Self::Withdrawal(withdrawal) => withdrawal.version(),
        }
    }

    /// Write it to `directory`, which refuses it as it refuses any write.
    pub async fn to(self, directory: &impl CellDirectory) -> Result<(), DirectoryError> {
        match self {
            Self::Record(record) => directory.publish(*record).await,
            Self::Withdrawal(withdrawal) => directory.withdraw(withdrawal).await,
        }
    }
}

/// The record `directory` holds for `cell` of `class`, if any, from a new
/// watch's snapshot. A watch that ends or fails before its snapshot is
/// complete is an error, never a cell with no record.
pub async fn last_record(
    directory: &impl CellDirectory,
    class: &str,
    cell: &str,
) -> Result<Option<CellRecord>, DirectoryError> {
    let mut watch = std::pin::pin!(directory.watch(class, None));
    let mut held = None;
    loop {
        match watch.next().await {
            Some(Ok(CellEvent::Current(current))) if current.name == cell => held = Some(current),
            Some(Ok(CellEvent::Current(_))) => {}
            Some(Ok(CellEvent::Synced { .. })) => return Ok(held),
            Some(Err(error)) => return Err(error),
            Some(Ok(_)) | None => {
                return Err(DirectoryError::Unavailable(
                    "the directory's watch ended before its snapshot".into(),
                ));
            }
        }
    }
}
