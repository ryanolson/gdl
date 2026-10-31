//! The pool's record, published on each change of the judged state, of
//! its presence, and of its properties, and on the heartbeat, with the load
//! summary as it is when it is written, and withdrawn once the pool's
//! drain has ended. Under the `relay` backend the heartbeat is the cadence
//! the parent last answered with. Before its first write, the hub reads
//! the pool's last write: its record, whose properties keep their history,
//! or the tombstone of its withdrawal. The hub numbers past either.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gdl_pools::{
    DirectoryError, LastWrite, LoadSummary, PoolDirectory, PoolRecord, PoolState, Presence,
    Version, Withdrawal,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::drain::Drain;
use super::feed::Feed;
use super::health::PoolHealth;
use super::properties::PropertyList;
use crate::HubDirectory;

#[cfg(test)]
mod tests;

/// The longest one read of the pool's last record may take before the
/// publisher starts: a read that fails or takes longer is made again.
const SNAPSHOT: Duration = Duration::from_secs(2);

/// How long the publisher waits before it reads the pool's last record
/// again, after a read that failed.
const REREAD: Duration = Duration::from_secs(1);

pub(super) struct Publisher<D> {
    pub(super) directory: Arc<D>,
    pub(super) record: PoolRecord,
    pub(super) heartbeat: Duration,
    /// The cadence a parent asks the record to be written at, once it has
    /// answered: it replaces `heartbeat` from then.
    pub(super) cadence: Option<watch::Receiver<Option<Duration>>>,
    /// Told the epoch once a write is stored under it, and closed once
    /// the record is withdrawn.
    pub(super) feed: Arc<Feed>,
    /// The pool's drain, whose end withdraws the record.
    pub(super) drain: Arc<Drain>,
    /// The state and presence of the record last stored, for a drain or
    /// a change of presence to be answered once its record is.
    pub(super) stored: watch::Sender<Option<Stored>>,
    /// Under the `relay` backend, where the pool's last record is read:
    /// its parent's hub, since the hub's own directory holds its
    /// children's records, not its own.
    pub(super) parent: Option<HubDirectory>,
    /// The pool's health, which says `withdrawn` once the directory has
    /// stored the withdrawal.
    pub(super) health: watch::Sender<PoolHealth>,
}

impl<D: PoolDirectory> Publisher<D> {
    /// Publish the judged state, the presence, and the property entries,
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
    /// presence, and each change of the property entries, and, while none
    /// changes, on every heartbeat. A change while a write is in flight
    /// is written once that write ends; one replaced meanwhile by a later
    /// change is not written at all, since a reader keeps only the newest
    /// record. The first write waits for the pool's last record to be read,
    /// bounded, so that its properties keep their history. Once the pool's drain has ended, the
    /// record is withdrawn, after a last write that says it is `draining`
    /// if the last did not, and nothing is written again: a heartbeat
    /// after the withdrawal would bring the record back. The pool's
    /// watches end then, so that the hubs that follow the pool drop it.
    async fn publish_until(&self, sources: &mut Sources) {
        let directory = &*self.directory;
        // A record written before the last one is read would lose the
        // history of the pool's properties, so the hub writes none until it
        // has read it. Under the `relay` backend, a write could not land
        // while the parent cannot be read either.
        let (mut writer, last) = loop {
            let parent = self.parent.clone();
            match Writer::start(directory, parent, &self.record, self.heartbeat).await {
                (writer, Ok(last)) => break (writer, last),
                (_, Err(error)) => {
                    tracing::warn!(%error, "the pool's last write was not read");
                    tokio::time::sleep(REREAD).await;
                }
            }
        };
        let stored = &self.stored;
        let feed = &*self.feed;
        // The list counts past the last write's version, a withdrawal's
        // too, and keeps the history of the last record's entries.
        let floor = super::properties::floor_after(
            writer.epoch,
            last.as_ref().map(LastWrite::properties_version),
        );
        let held = last.as_ref().and_then(LastWrite::record).map(|held| {
            (
                &held.properties[..],
                held.properties_version,
                &held.revoked[..],
            )
        });
        feed.restore(held, floor);
        let mut written = sources.take();
        let mut state = written.state;
        writer
            .publish(directory, (feed, stored), &self.record, &written)
            .await;
        self.settled(&writer);
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
                Ok(()) = sources.properties.changed() => heartbeat.reset(),
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
            self.settled(&writer);
        }
        if state != PoolState::Draining {
            let last = Written {
                state: PoolState::Draining,
                ..sources.take()
            };
            writer
                .publish(directory, (feed, stored), &self.record, &last)
                .await;
        }
        // The tombstone keeps the list's version, which no list this hub
        // sent is above, so that a later hub of the pool numbers past it.
        let version = sources.properties.borrow().version;
        let stored = writer.withdraw(directory, &self.record, version).await;
        feed.close();
        // The withdrawal's success alone is no proof: a later hub of the
        // pool may have published since it was stored, and before its
        // reply. Only a read that finds no record of the pool is.
        if stored && self.absent(directory).await {
            self.health.send_modify(|health| health.withdrawn = true);
        }
    }
}

impl<D: PoolDirectory> Publisher<D> {
    /// Once `writer` has stored a write, the hub waits for the directory no
    /// longer: its health says so until then.
    fn settled(&self, writer: &Writer) {
        if writer.stored {
            self.health
                .send_if_modified(|health| health.waiting.take().is_some());
        }
    }

    /// Whether the directory holds no record of the pool, read as the
    /// pool's last record is read when the hub starts: from its parent's
    /// hub under the `relay` backend, else from the directory. A read that
    /// fails is made again a heartbeat later, since only a read answers.
    async fn absent(&self, directory: &D) -> bool {
        loop {
            let read = async {
                match &self.parent {
                    Some(parent) => parent
                        .held(&self.record.class, &self.record.name)
                        .await
                        .map(|last| last.and_then(LastWrite::into_record)),
                    None => {
                        gdl_pools::last_record(directory, &self.record.class, &self.record.name)
                            .await
                    }
                }
            };
            match tokio::time::timeout(SNAPSHOT, read).await {
                Ok(Ok(held)) => return held.is_none(),
                Ok(Err(error)) => {
                    tracing::warn!(%error, "the withdrawn pool's record was not read again");
                }
                Err(_) => {
                    tracing::warn!("the withdrawn pool's record was not read again in time");
                }
            }
            tokio::time::sleep(self.heartbeat).await;
        }
    }
}

/// What the record last stored says of the pool's state, presence, and
/// properties, and the latest request for a presence that it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Stored {
    pub(super) state: PoolState,
    pub(super) presence: Presence,
    pub(super) request: u64,
    pub(super) properties: Arc<PropertyList>,
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
    state: PoolState,
    presence: Asked,
    load: Option<LoadSummary>,
    properties: Arc<PropertyList>,
}

/// The sources of what each write says: the judged state, the presence
/// the hub was told, the load summary, and the property entries.
pub(super) struct Sources {
    pub(super) states: watch::Receiver<PoolState>,
    pub(super) presence: watch::Receiver<Asked>,
    pub(super) loads: watch::Receiver<Option<LoadSummary>>,
    pub(super) properties: watch::Receiver<Arc<PropertyList>>,
}

impl Sources {
    /// What a write says now, each change marked seen.
    fn take(&mut self) -> Written {
        Written {
            state: *self.states.borrow_and_update(),
            presence: *self.presence.borrow_and_update(),
            load: *self.loads.borrow(),
            properties: Arc::clone(&self.properties.borrow_and_update()),
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

/// The version of the pool's writes.
struct Writer {
    epoch: u64,
    seq: u64,
    /// Under the `relay` backend, where the pool's last write is read: its
    /// parent's hub.
    parent: Option<HubDirectory>,
    /// The version of the pool's last write the hub read and numbered past:
    /// until a write is stored, each lands only while that is still the
    /// last write ([`PoolDirectory::publish_after`]).
    read: Option<Version>,
    /// The longest a write may take: one that takes longer is overtaken by
    /// the next, rather than holding up the heartbeat.
    bound: Duration,
    /// Whether a write has been stored: from then on the epoch is the
    /// one readers know this hub by, and it does not change.
    stored: bool,
}

impl Writer {
    /// The writer of a hub that starts now, with the pool's last write the
    /// directory holds, its record or a withdrawal's tombstone, if it read
    /// one in time: its epoch is the time in milliseconds, raised past the
    /// last write's, so that a restart orders after its predecessor
    /// whatever the clocks say. Under the `relay` backend, the write is
    /// read from the parent. A read that fails or does not end in time is
    /// an error.
    async fn start(
        directory: &impl PoolDirectory,
        parent: Option<HubDirectory>,
        record: &PoolRecord,
        bound: Duration,
    ) -> (Self, Result<Option<LastWrite>, DirectoryError>) {
        let now = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let last = last_write(directory, parent.as_ref(), record).await;
        let epoch = last
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map(|last| last.version().epoch);
        let read = last
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map(LastWrite::version);
        let writer = Self {
            epoch: epoch.map_or(now, |last| now.max(last.saturating_add(1))),
            seq: 0,
            parent,
            read,
            bound,
            stored: false,
        };
        (writer, last)
    }

    async fn publish(
        &mut self,
        directory: &impl PoolDirectory,
        (feed, stored): (&Feed, &watch::Sender<Option<Stored>>),
        record: &PoolRecord,
        written: &Written,
    ) {
        self.seq += 1;
        let Written {
            state,
            presence,
            load,
            ref properties,
        } = *written;
        let write = PoolRecord {
            state,
            load,
            presence: presence.presence,
            properties: properties.entries.clone(),
            properties_version: properties.version,
            revoked: properties.revoked.clone(),
            epoch: self.epoch,
            seq: self.seq,
            ..record.clone()
        };
        // Until a write is stored, one lands only on the write the hub read:
        // a write another hub made since could carry a list version past
        // the one this hub numbered from.
        let after = (!self.stored).then_some(self.read);
        let publishing = async {
            match after {
                Some(last) => directory.publish_after(write, last).await,
                None => directory.publish(write).await,
            }
        };
        let Ok(written) = tokio::time::timeout(self.bound, publishing).await else {
            tracing::warn!(bound = ?self.bound, "the pool's record was not published in time");
            return;
        };
        match written {
            Ok(()) => {
                // A list in the block of the stored version may go over the
                // links, before a watch opens on the settled epoch.
                feed.stored(properties.version);
                // The watch carries the record's epoch, which is settled
                // once a write is stored under it.
                feed.settle(self.epoch);
                stored.send_replace(Some(Stored {
                    state,
                    presence: presence.presence,
                    request: presence.request,
                    properties: Arc::clone(properties),
                }));
                self.stored = true;
            }
            Err(DirectoryError::NotNewer { held, offered, .. }) => {
                tracing::warn!(%held, %offered, "the directory refused the pool's record as not newer");
                if held.epoch == self.epoch && self.stored {
                    self.seq = held.seq;
                } else if !self.stored {
                    // A later write than the one the hub read, at any epoch:
                    // nothing has been stored under this epoch, so no reader
                    // knows it, and it can still move past the one held. The
                    // properties' versions follow the write held, read
                    // again, so they stay above the ones it published. An
                    // equal epoch is a predecessor's that started in the
                    // same millisecond: catching up its sequence alone would
                    // publish a lower version than it did.
                    // A read that fails is unknown, never nothing: the hub
                    // reads again, and stores and sends nothing meanwhile,
                    // as before its first read.
                    let last = loop {
                        match last_write(directory, self.parent.as_ref(), record).await {
                            Ok(last) => break last,
                            Err(error) => {
                                tracing::warn!(%error, "the pool's last write was not read again");
                                tokio::time::sleep(REREAD).await;
                            }
                        }
                    };
                    let epoch = last
                        .as_ref()
                        .map_or(held.epoch, |last| held.epoch.max(last.version().epoch));
                    self.epoch = self.epoch.max(epoch.saturating_add(1));
                    self.seq = 0;
                    self.read = last.as_ref().map(LastWrite::version);
                    // The hub takes the write it read again whole, its
                    // revocations with its entries, not only its numbers.
                    let floor = super::properties::floor_after(
                        self.epoch,
                        last.as_ref().map(LastWrite::properties_version),
                    );
                    let held = last.as_ref().and_then(LastWrite::record).map(|held| {
                        (
                            &held.properties[..],
                            held.properties_version,
                            &held.revoked[..],
                        )
                    });
                    feed.adopt(held, floor);
                } else {
                    tracing::error!(
                        %held,
                        "another hub publishes this pool with a later epoch"
                    );
                }
            }
            Err(error) => tracing::warn!(%error, "the pool's record was not published"),
        }
    }
}

/// The pool's last write: read from its parent's hub under the `relay`
/// backend, else from `directory`, bounded.
async fn last_write(
    directory: &impl PoolDirectory,
    parent: Option<&HubDirectory>,
    record: &PoolRecord,
) -> Result<Option<LastWrite>, DirectoryError> {
    let read = async {
        match parent {
            Some(parent) => parent.held(&record.class, &record.name).await,
            None => directory.last_write(&record.class, &record.name).await,
        }
    };
    tokio::time::timeout(SNAPSHOT, read)
        .await
        .unwrap_or_else(|_| {
            Err(DirectoryError::Unavailable(format!(
                "the pool's last write was not read within {SNAPSHOT:?}"
            )))
        })
}

impl Writer {
    /// Withdraw the record, after every write before it, retried once a
    /// heartbeat until the directory takes it: whether this hub's
    /// withdrawal is stored. A later hub of the pool holds its record
    /// otherwise, which is that hub's to withdraw.
    async fn withdraw(
        &mut self,
        directory: &impl PoolDirectory,
        record: &PoolRecord,
        properties_version: u64,
    ) -> bool {
        loop {
            self.seq += 1;
            let withdrawal = Withdrawal {
                class: record.class.clone(),
                pool: record.name.clone(),
                epoch: self.epoch,
                seq: self.seq,
                properties_version,
            };
            match tokio::time::timeout(self.bound, directory.withdraw(withdrawal)).await {
                Ok(Ok(())) => return true,
                Ok(Err(DirectoryError::NotNewer { held, .. })) if held.epoch == self.epoch => {
                    self.seq = held.seq;
                    continue;
                }
                // A later hub of the pool holds its record now: it is that
                // hub's to withdraw.
                Ok(Err(DirectoryError::NotNewer { held, .. })) if held.epoch > self.epoch => {
                    tracing::warn!(%held, "another hub publishes this pool with a later epoch");
                    return false;
                }
                Ok(Err(error)) => {
                    tracing::warn!(%error, "the drained pool's record was not withdrawn");
                }
                Err(_) => {
                    tracing::warn!(bound = ?self.bound, "the drained pool's record was not withdrawn in time");
                }
            }
            tokio::time::sleep(self.bound).await;
        }
    }
}
