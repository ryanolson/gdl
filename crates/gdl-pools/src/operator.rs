use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;

use crate::{DirectoryError, PoolDirectory, PoolEvent, PoolRecord, Presence, Version, Withdrawal};

/// What an operator says of a pool whose hub cannot say it: the hub is
/// gone, and silence never removes a pool or says that it was scaled
/// to 0.
///
/// A command is a write of the pool's record, at an epoch past the last
/// record's: a new writer, as a restarted hub is. So a late write of the
/// old epoch, a heartbeat that was on its way or a hub that was cut off
/// and comes back, is refused as not newer, and cannot undo the command.
/// A hub of the pool that starts again takes an epoch past the command's,
/// as it does past any record it finds, and writes over it. So does a hub
/// that still runs but none of whose writes was answered.
///
/// For a pool whose hub still answers, the hub's own drain and presence
/// say the same, and keep its epoch: a command fences that hub's writes
/// until it restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolCommand {
    /// Remove the pool's record, and leave a tombstone at the command's
    /// version: its models leave every reader's list, and its parent's
    /// choosers hold it withdrawn.
    Withdraw,
    /// Write the pool's last record again, saying that it was scaled to 0
    /// on purpose ([`Presence::ScaledToZero`]): it has no lease, so a
    /// reader shows it as scaled to 0, not missing. Its state, models, and
    /// load stay as the last record said.
    ScaleToZero,
}

impl PoolCommand {
    /// The write that carries the command for the pool whose last record
    /// is `last`, at `now`: at epoch `now` in milliseconds, or the epoch
    /// after `last`'s when that is later, and sequence number 1.
    pub fn write(self, last: &PoolRecord, now: SystemTime) -> CommandWrite {
        let now = now.duration_since(UNIX_EPOCH).map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
        let (epoch, seq) = (now.max(last.epoch.saturating_add(1)), 1);
        match self {
            Self::Withdraw => CommandWrite::Withdrawal(Withdrawal {
                class: last.class.clone(),
                pool: last.name.clone(),
                epoch,
                seq,
            }),
            Self::ScaleToZero => CommandWrite::Record(Box::new(PoolRecord {
                presence: Presence::ScaledToZero,
                epoch,
                seq,
                ..last.clone()
            })),
        }
    }

    /// Give the command for `pool` of `class` through `directory`: read the
    /// pool's last record, and write the command past it. The version
    /// written, or `None` when the directory holds no record of the pool,
    /// which leaves nothing to command and writes nothing.
    ///
    /// The record is read from a new watch's snapshot, which holds a
    /// record whose lease has passed, as the directory's first rule says.
    /// Under the `relay` backend, the records are a parent's hub's, and
    /// that hub gives the command itself.
    pub async fn give(
        self,
        directory: &impl PoolDirectory,
        class: &str,
        pool: &str,
    ) -> Result<Option<Version>, DirectoryError> {
        let Some(last) = last_record(directory, class, pool).await? else {
            return Ok(None);
        };
        let write = self.write(&last, SystemTime::now());
        let version = write.version();
        write.to(directory).await?;
        Ok(Some(version))
    }
}

/// The write that carries a [`PoolCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandWrite {
    /// A record: the pool scaled to 0.
    Record(Box<PoolRecord>),
    /// The pool's withdrawal.
    Withdrawal(Withdrawal),
}

impl CommandWrite {
    /// The write's place in the pool's order of writes.
    pub fn version(&self) -> Version {
        match self {
            Self::Record(record) => record.version(),
            Self::Withdrawal(withdrawal) => withdrawal.version(),
        }
    }

    /// Write it to `directory`, which refuses it as it refuses any write.
    pub async fn to(self, directory: &impl PoolDirectory) -> Result<(), DirectoryError> {
        match self {
            Self::Record(record) => directory.publish(*record).await,
            Self::Withdrawal(withdrawal) => directory.withdraw(withdrawal).await,
        }
    }
}

/// The record `directory` holds for `pool` of `class`, if any, from a new
/// watch's snapshot. A watch that ends or fails before its snapshot is
/// complete is an error, never a pool with no record.
pub async fn last_record(
    directory: &impl PoolDirectory,
    class: &str,
    pool: &str,
) -> Result<Option<PoolRecord>, DirectoryError> {
    let mut watch = std::pin::pin!(directory.watch(class, None));
    let mut held = None;
    loop {
        match watch.next().await {
            Some(Ok(PoolEvent::Current(current))) if current.name == pool => held = Some(current),
            Some(Ok(PoolEvent::Current(_))) => {}
            Some(Ok(PoolEvent::Synced { .. })) => return Ok(held),
            Some(Err(error)) => return Err(error),
            Some(Ok(_)) | None => {
                return Err(DirectoryError::Unavailable(
                    "the directory's watch ended before its snapshot".into(),
                ));
            }
        }
    }
}
