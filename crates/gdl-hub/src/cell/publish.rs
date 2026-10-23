//! The cell's state, judged as the counts change and as the hold runs
//! out, and its record, published on each change and on the heartbeat.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use gdl_cells::{CellDirectory, CellEvent, CellRecord, CellState, DirectoryError};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::CellHealth;
use super::count::Count;
use crate::Quorum;

/// The longest the publisher reads the directory for the cell's last
/// epoch before it starts: past it, the hub publishes from its clock
/// alone, and a refusal shows it what the directory holds.
const SNAPSHOT: Duration = Duration::from_secs(2);

pub(super) struct Publisher<D> {
    pub(super) directory: Arc<D>,
    pub(super) record: CellRecord,
    pub(super) quorum: Quorum,
    pub(super) heartbeat: Duration,
    pub(super) health: watch::Sender<CellHealth>,
}

impl<D: CellDirectory> Publisher<D> {
    /// Judge and publish until `cancel`, or until the counting ends.
    pub(super) async fn run(
        mut self,
        mut counts: mpsc::Receiver<Count>,
        cancel: CancellationToken,
    ) {
        tokio::select! {
            () = self.publish_until(&mut counts) => {}
            () = cancel.cancelled() => {}
        }
    }

    async fn publish_until(&mut self, counts: &mut mpsc::Receiver<Count>) {
        let mut writer = Writer::start(&*self.directory, &self.record, self.heartbeat).await;
        let mut state = self.judge();
        writer.publish(&*self.directory, &self.record, state).await;
        let mut heartbeat =
            tokio::time::interval_at(tokio::time::Instant::now() + self.heartbeat, self.heartbeat);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let held = self.quorum.next_change();
            let hold = async {
                match held {
                    Some(at) => tokio::time::sleep_until(at.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                count = counts.recv() => {
                    let Some(count) = count else { return };
                    self.count(count);
                }
                () = hold => {}
                _ = heartbeat.tick() => {
                    writer.publish(&*self.directory, &self.record, state).await;
                    continue;
                }
            }
            let judged = self.judge();
            if judged != state {
                state = judged;
                writer.publish(&*self.directory, &self.record, state).await;
                heartbeat.reset();
            }
        }
    }

    fn count(&mut self, count: Count) {
        match count {
            Count::Synced(usable) => {
                for (index, usable) in usable.into_iter().enumerate() {
                    self.quorum.set_nth(index, usable);
                }
            }
            Count::Changed(index, usable) => self.quorum.set_nth(index, usable),
            Count::Lost => {
                for index in 0..self.quorum.segments().count() {
                    self.quorum.set_nth(index, 0);
                }
            }
        }
    }

    /// The state now, shown on the health endpoint. The body is updated
    /// in place: a count costs no allocation.
    fn judge(&mut self) -> CellState {
        let state = self.quorum.state(Instant::now());
        let quorum = &self.quorum;
        self.health.send_if_modified(|health| {
            let mut modified = health.state != state;
            health.state = state;
            for (name, usable, _) in quorum.segments() {
                if let Some(segment) = health.segments.get_mut(name)
                    && segment.usable != usable
                {
                    segment.usable = usable;
                    modified = true;
                }
            }
            modified
        });
        state
    }
}

/// The version of the cell's writes.
struct Writer {
    epoch: u64,
    seq: u64,
    /// The longest a write may take: one that takes longer is overtaken by
    /// the next, rather than holding up the judgment and the heartbeat.
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
        record: &CellRecord,
        state: CellState,
    ) {
        self.seq += 1;
        let record = CellRecord {
            state,
            epoch: self.epoch,
            seq: self.seq,
            ..record.clone()
        };
        let Ok(written) = tokio::time::timeout(self.bound, directory.publish(record)).await else {
            tracing::warn!(bound = ?self.bound, "the cell's record was not published in time");
            return;
        };
        match written {
            Ok(()) => self.stored = true,
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
