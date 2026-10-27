//! The cell's record, published on each change of the judged state, of
//! its presence, and of its models, and on the heartbeat, with the load
//! summary as it is when it is written, and withdrawn once the cell's
//! drain has ended. Under the `relay` backend the heartbeat is the cadence
//! the parent last answered with. Before its first write, the hub reads
//! the cell's last record, whose models keep their history.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gdl_cells::{
    CellDirectory, CellRecord, CellState, DirectoryError, LoadSummary, Presence, Withdrawal,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::drain::Drain;
use super::feed::Feed;
use super::models::ModelList;
use crate::HubDirectory;

/// The longest one read of the cell's last record may take before the
/// publisher starts: a read that fails or takes longer is made again.
const SNAPSHOT: Duration = Duration::from_secs(2);

/// How long the publisher waits before it reads the cell's last record
/// again, after a read that failed.
const REREAD: Duration = Duration::from_secs(1);

pub(super) struct Publisher<D> {
    pub(super) directory: Arc<D>,
    pub(super) record: CellRecord,
    pub(super) heartbeat: Duration,
    /// The cadence a parent asks the record to be written at, once it has
    /// answered: it replaces `heartbeat` from then.
    pub(super) cadence: Option<watch::Receiver<Option<Duration>>>,
    /// Told the epoch once a write is stored under it, and closed once
    /// the record is withdrawn.
    pub(super) feed: Arc<Feed>,
    /// The cell's drain, whose end withdraws the record.
    pub(super) drain: Arc<Drain>,
    /// The state and presence of the record last stored, for a drain or
    /// a change of presence to be answered once its record is.
    pub(super) stored: watch::Sender<Option<Stored>>,
    /// Under the `relay` backend, where the cell's last record is read:
    /// its parent's hub, since the hub's own directory holds its
    /// children's records, not its own.
    pub(super) parent: Option<HubDirectory>,
}

impl<D: CellDirectory> Publisher<D> {
    /// Publish the judged state, the presence, and the model entries,
    /// with the summary `loads` holds when each record is written, until
    /// `cancel`, or until the judgment ends. A change of the summary is
    /// not a write of its own: the heartbeat carries it, so that the
    /// directory's writes stay one a heartbeat.
    pub(super) async fn run(self, mut sources: Sources, cancel: CancellationToken) {
        tokio::select! {
            () = self.publish_until(&mut sources) => {}
            () = cancel.cancelled() => {}
        }
    }

    /// Publish at once, then each state the judgment changes to, each
    /// presence, and each change of the model entries, and, while none
    /// changes, on every heartbeat. A change while a write is in flight
    /// is written once that write ends; one replaced meanwhile by a later
    /// change is not written at all, since a reader keeps only the newest
    /// record. The first write waits for the cell's last record to be read,
    /// bounded, so that its models keep their history. Once the cell's drain has ended, the
    /// record is withdrawn, after a last write that says it is `draining`
    /// if the last did not, and nothing is written again: a heartbeat
    /// after the withdrawal would bring the record back. The cell's
    /// watches end then, so that the hubs that follow the cell drop it.
    async fn publish_until(&self, sources: &mut Sources) {
        let directory = &*self.directory;
        // A record written before the last one is read would lose the
        // history of the cell's models, so the hub writes none until it
        // has read it. Under the `relay` backend, a write could not land
        // while the parent cannot be read either.
        let (mut writer, held) = loop {
            let parent = self.parent.as_ref();
            match Writer::start(directory, parent, &self.record, self.heartbeat).await {
                (writer, Ok(held)) => break (writer, held),
                (_, Err(error)) => {
                    tracing::warn!(%error, "the cell's last record was not read");
                    tokio::time::sleep(REREAD).await;
                }
            }
        };
        let stored = &self.stored;
        let feed = &*self.feed;
        let held = held
            .as_ref()
            .map(|held| (&held.models[..], held.models_version, &held.revoked[..]));
        feed.restore(held, super::models::floor(writer.epoch));
        let mut written = sources.take();
        let mut state = written.state;
        writer
            .publish(directory, (feed, stored), &self.record, &written)
            .await;
        let mut cadence = self.cadence.clone();
        let mut heartbeat = beat(self.heartbeat);
        let ended = self.drain.ended();
        tokio::pin!(ended);
        loop {
            tokio::select! {
                biased;
                changed = sources.states.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    heartbeat.reset();
                }
                Ok(()) = sources.presence.changed() => heartbeat.reset(),
                Ok(()) = sources.models.changed() => heartbeat.reset(),
                () = &mut ended => break,
                period = answered(&mut cadence) => {
                    // A new cadence counts from the write it answered, and
                    // bounds each write, so that a stalled report is
                    // overtaken by the next within the record's lease.
                    heartbeat = beat(period);
                    writer.bound = period;
                    continue;
                }
                _ = heartbeat.tick() => {}
            }
            written = sources.take();
            state = written.state;
            writer
                .publish(directory, (feed, stored), &self.record, &written)
                .await;
        }
        if state != CellState::Draining {
            let last = Written {
                state: CellState::Draining,
                ..sources.take()
            };
            writer
                .publish(directory, (feed, stored), &self.record, &last)
                .await;
        }
        writer.withdraw(directory, &self.record).await;
        feed.close();
    }
}

/// What the record last stored says of the cell's state, presence, and
/// models, and the latest request for a presence that it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Stored {
    pub(super) state: CellState,
    pub(super) presence: Presence,
    pub(super) request: u64,
    pub(super) models: Arc<ModelList>,
}

/// The presence the hub was last told, and the number of that request:
/// a request is answered by a stored record that carries it or a later
/// one, never by one stored before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Asked {
    pub(super) presence: Presence,
    pub(super) request: u64,
}

/// What a write of the record says, beyond the record the hub was made
/// with.
struct Written {
    state: CellState,
    presence: Asked,
    load: Option<LoadSummary>,
    models: Arc<ModelList>,
}

/// The sources of what each write says: the judged state, the presence
/// the hub was told, the load summary, and the model entries.
pub(super) struct Sources {
    pub(super) states: watch::Receiver<CellState>,
    pub(super) presence: watch::Receiver<Asked>,
    pub(super) loads: watch::Receiver<Option<LoadSummary>>,
    pub(super) models: watch::Receiver<Arc<ModelList>>,
}

impl Sources {
    /// What a write says now, each change marked seen.
    fn take(&mut self) -> Written {
        Written {
            state: *self.states.borrow_and_update(),
            presence: *self.presence.borrow_and_update(),
            load: *self.loads.borrow(),
            models: Arc::clone(&self.models.borrow_and_update()),
        }
    }
}

/// A heartbeat of `period`, whose first tick is a period from now.
fn beat(period: Duration) -> tokio::time::Interval {
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat
}

/// The next cadence a parent answers with that differs from the last; it
/// never resolves without a parent, or once the answers end.
async fn answered(cadence: &mut Option<watch::Receiver<Option<Duration>>>) -> Duration {
    while let Some(answers) = cadence.as_mut() {
        if answers.changed().await.is_err() {
            *cadence = None;
        } else if let Some(period) = *answers.borrow_and_update() {
            return period;
        }
    }
    std::future::pending().await
}

/// The version of the cell's writes.
struct Writer {
    epoch: u64,
    seq: u64,
    /// The longest a write may take: one that takes longer is overtaken by
    /// the next, rather than holding up the heartbeat.
    bound: Duration,
    /// Whether a write has been stored: from then on the epoch is the
    /// one readers know this hub by, and it does not change.
    stored: bool,
}

impl Writer {
    /// The writer of a hub that starts now, with the record the directory
    /// holds for the cell, if it read one in time: its epoch is the time
    /// in milliseconds, raised past the held record's, so that a restart
    /// orders after its predecessor even when the predecessor's clock ran
    /// ahead. Under the `relay` backend, the record is read from the
    /// parent. A read that fails or does not end in time is an error.
    async fn start(
        directory: &impl CellDirectory,
        parent: Option<&HubDirectory>,
        record: &CellRecord,
        bound: Duration,
    ) -> (Self, Result<Option<CellRecord>, DirectoryError>) {
        let now = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let read = async {
            match parent {
                Some(parent) => parent.held(&record.class, &record.name).await,
                None => gdl_cells::last_record(directory, &record.class, &record.name).await,
            }
        };
        let held = tokio::time::timeout(SNAPSHOT, read)
            .await
            .unwrap_or_else(|_| {
                Err(DirectoryError::Unavailable(format!(
                    "the cell's last record was not read within {SNAPSHOT:?}"
                )))
            });
        let epoch = held
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map(|held| held.epoch);
        let writer = Self {
            epoch: epoch.map_or(now, |held| now.max(held.saturating_add(1))),
            seq: 0,
            bound,
            stored: false,
        };
        (writer, held)
    }

    async fn publish(
        &mut self,
        directory: &impl CellDirectory,
        (feed, stored): (&Feed, &watch::Sender<Option<Stored>>),
        record: &CellRecord,
        written: &Written,
    ) {
        self.seq += 1;
        let Written {
            state,
            presence,
            load,
            ref models,
        } = *written;
        let record = CellRecord {
            state,
            load,
            presence: presence.presence,
            models: models.entries.clone(),
            models_version: models.version,
            revoked: models.revoked.clone(),
            epoch: self.epoch,
            seq: self.seq,
            ..record.clone()
        };
        let Ok(written) = tokio::time::timeout(self.bound, directory.publish(record)).await else {
            tracing::warn!(bound = ?self.bound, "the cell's record was not published in time");
            return;
        };
        match written {
            Ok(()) => {
                // The watch carries the record's epoch, which is settled
                // once a write is stored under it.
                feed.settle(self.epoch);
                stored.send_replace(Some(Stored {
                    state,
                    presence: presence.presence,
                    request: presence.request,
                    models: Arc::clone(models),
                }));
                self.stored = true;
            }
            Err(DirectoryError::NotNewer { held, offered, .. }) => {
                tracing::warn!(%held, %offered, "the directory refused the cell's record as not newer");
                if held.epoch == self.epoch {
                    self.seq = held.seq;
                } else if held.epoch > self.epoch && !self.stored {
                    // Nothing has been stored under this epoch, so no reader
                    // knows it: it can still move past the one held. The
                    // models' versions follow it, so they stay above the
                    // ones published under the epoch held.
                    self.epoch = held.epoch.saturating_add(1);
                    self.seq = 0;
                    feed.raise_floor(super::models::floor(self.epoch));
                } else {
                    tracing::error!(
                        %held,
                        "another hub publishes this cell with a later epoch"
                    );
                }
            }
            Err(error) => tracing::warn!(%error, "the cell's record was not published"),
        }
    }
}

impl Writer {
    /// Withdraw the record, after every write before it, retried once a
    /// heartbeat until the directory takes it.
    async fn withdraw(&mut self, directory: &impl CellDirectory, record: &CellRecord) {
        loop {
            self.seq += 1;
            let withdrawal = Withdrawal {
                class: record.class.clone(),
                cell: record.name.clone(),
                epoch: self.epoch,
                seq: self.seq,
            };
            match tokio::time::timeout(self.bound, directory.withdraw(withdrawal)).await {
                Ok(Ok(())) => return,
                Ok(Err(DirectoryError::NotNewer { held, .. })) if held.epoch == self.epoch => {
                    self.seq = held.seq;
                    continue;
                }
                // A later hub of the cell holds its record now: it is that
                // hub's to withdraw.
                Ok(Err(DirectoryError::NotNewer { held, .. })) if held.epoch > self.epoch => {
                    tracing::warn!(%held, "another hub publishes this cell with a later epoch");
                    return;
                }
                Ok(Err(error)) => {
                    tracing::warn!(%error, "the drained cell's record was not withdrawn");
                }
                Err(_) => {
                    tracing::warn!(bound = ?self.bound, "the drained cell's record was not withdrawn in time");
                }
            }
            tokio::time::sleep(self.bound).await;
        }
    }
}
