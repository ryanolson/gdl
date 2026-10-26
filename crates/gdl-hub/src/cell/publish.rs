//! The cell's record, published on each change of the judged state and
//! on the heartbeat, with the load summary as it is when it is written,
//! and withdrawn once the cell's drain has ended. Under the `relay`
//! backend the heartbeat is the cadence the parent last answered with.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellState, DirectoryError, LoadSummary, Withdrawal,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::drain::Drain;
use super::feed::Feed;

/// The longest the publisher reads the directory for the cell's last
/// epoch before it starts: past it, the hub publishes from its clock
/// alone, and a refusal shows it what the directory holds.
const SNAPSHOT: Duration = Duration::from_secs(2);

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
    /// The state of the record last stored, for a drain to be answered
    /// once its record is.
    pub(super) stored: watch::Sender<Option<CellState>>,
}

impl<D: CellDirectory> Publisher<D> {
    /// Publish the judged state, with the summary `loads` holds when each
    /// record is written, until `cancel`, or until the judgment ends. A
    /// change of the summary is not a write of its own: the heartbeat
    /// carries it, so that the directory's writes stay one a heartbeat.
    pub(super) async fn run(
        self,
        mut states: watch::Receiver<CellState>,
        loads: watch::Receiver<Option<LoadSummary>>,
        cancel: CancellationToken,
    ) {
        tokio::select! {
            () = self.publish_until(&mut states, &loads) => {}
            () = cancel.cancelled() => {}
        }
    }

    /// Publish at once, then each state the judgment changes to and, while
    /// it does not change, on every heartbeat. A state that changes while
    /// a write is in flight is written once that write ends; one replaced
    /// meanwhile by a later state is not written at all, since a reader
    /// keeps only the newest record. Once the cell's drain has ended, the
    /// record is withdrawn, after a last write that says it is `draining`
    /// if the last did not, and nothing is written again: a heartbeat
    /// after the withdrawal would bring the record back. The cell's
    /// watches end then, so that the hubs that follow the cell drop it.
    async fn publish_until(
        &self,
        states: &mut watch::Receiver<CellState>,
        loads: &watch::Receiver<Option<LoadSummary>>,
    ) {
        let directory = &*self.directory;
        let mut writer = Writer::start(directory, &self.record, self.heartbeat).await;
        let stored = &self.stored;
        let feed = &*self.feed;
        let mut state = *states.borrow_and_update();
        let load = *loads.borrow();
        writer
            .publish(directory, (feed, stored), &self.record, state, load)
            .await;
        let mut cadence = self.cadence.clone();
        let mut heartbeat = beat(self.heartbeat);
        let ended = self.drain.ended();
        tokio::pin!(ended);
        loop {
            tokio::select! {
                biased;
                changed = states.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    heartbeat.reset();
                }
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
            state = *states.borrow_and_update();
            let load = *loads.borrow();
            writer
                .publish(directory, (feed, stored), &self.record, state, load)
                .await;
        }
        if state != CellState::Draining {
            let load = *loads.borrow();
            writer
                .publish(
                    directory,
                    (feed, stored),
                    &self.record,
                    CellState::Draining,
                    load,
                )
                .await;
        }
        writer.withdraw(directory, &self.record).await;
        feed.close();
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
    /// The writer of a hub that starts now: its epoch is the time in
    /// milliseconds, raised past any epoch the directory holds for the
    /// cell, so that a restart orders after its predecessor even when the
    /// predecessor's clock ran ahead.
    async fn start(directory: &impl CellDirectory, record: &CellRecord, bound: Duration) -> Self {
        let now = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let held = tokio::time::timeout(SNAPSHOT, held_epoch(directory, record))
            .await
            .ok()
            .flatten();
        Self {
            epoch: held.map_or(now, |held| now.max(held.saturating_add(1))),
            seq: 0,
            bound,
            stored: false,
        }
    }

    async fn publish(
        &mut self,
        directory: &impl CellDirectory,
        (feed, stored): (&Feed, &watch::Sender<Option<CellState>>),
        record: &CellRecord,
        state: CellState,
        load: Option<LoadSummary>,
    ) {
        self.seq += 1;
        let record = CellRecord {
            state,
            load,
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
                stored.send_replace(Some(state));
                self.stored = true;
            }
            Err(DirectoryError::NotNewer { held, offered, .. }) => {
                tracing::warn!(%held, %offered, "the directory refused the cell's record as not newer");
                if held.epoch == self.epoch {
                    self.seq = held.seq;
                } else if held.epoch > self.epoch && !self.stored {
                    // Nothing has been stored under this epoch, so no reader
                    // knows it: it can still move past the one held.
                    self.epoch = held.epoch.saturating_add(1);
                    self.seq = 0;
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

/// The epoch of the record the directory holds for the cell, if any.
async fn held_epoch(directory: &impl CellDirectory, record: &CellRecord) -> Option<u64> {
    let mut watch = std::pin::pin!(directory.watch(&record.class, None));
    let mut held = None;
    while let Some(Ok(event)) = watch.next().await {
        match event {
            CellEvent::Current(current) if current.name == record.name => {
                held = Some(current.epoch)
            }
            CellEvent::Current(_) => {}
            _ => break,
        }
    }
    held
}
