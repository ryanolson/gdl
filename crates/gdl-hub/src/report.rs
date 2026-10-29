//! The `relay` directory backend in a pool's hub: the hub reports its
//! pool's record to its parent pool's hub, and holds the records its own
//! children report to it, which its follow watches.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use gdl_pools::{
    DirectoryError, MemoryLimits, MemoryWatch, PoolDirectory, PoolRecord, RelayDirectory,
    Withdrawal,
};
use gdl_watch::{PoolWatcher, Report, ReportReply};
use tokio::sync::watch;

use crate::PoolError;

#[cfg(test)]
mod tests;

/// The shortest cadence a child takes from its parent's answer, so that a
/// parent's mistake cannot make a child report in a loop.
const MIN_CADENCE: Duration = gdl_model::HubTiming::MIN_RELAY_CADENCE;

/// A pool hub's directory under the `relay` backend
/// (`[pool] directory = "relay"`).
///
/// Its writes are the pool's own record and withdrawal, which it reports
/// to its parent pool's hub ([`gdl_watch::REPORT_ROUTE`]); a pool of a
/// class that nothing sends to has no parent, and reports nowhere. Its
/// watches read the records the pool's own children report to its hub,
/// which the hub holds in process and keeps in its state directory
/// ([`RelayDirectory`]), so a hub follows its children as it would from
/// any directory, and still holds their records after it restarts. A pool
/// in the middle of a chain of classes does both.
///
/// The parent answers each report with the interval at which it expects
/// the next, and the pool's hub reports at that cadence from then
/// ([`crate::PoolReadiness::with_reports`]), within a third of its
/// record's lease.
#[derive(Clone)]
pub struct HubDirectory {
    children: RelayDirectory,
    parent: Option<Arc<Parent>>,
    /// The cadence of the parent's last answer, once it has answered.
    cadence: Arc<watch::Sender<Option<Duration>>>,
}

struct Parent {
    hub: String,
    watcher: PoolWatcher,
}

impl HubDirectory {
    /// The directory of a pool whose hub reports to the parent's hub at
    /// `parent_hub`, `host:port`, or of a pool that reports nowhere; and
    /// that keeps its children's records in `state_dir`, reading back
    /// what an earlier hub kept there, or keeps them only in memory.
    ///
    /// # Errors
    ///
    /// A parent's hub address that is not `host:port`, or a state
    /// directory that cannot be made or read: a hub that started without
    /// the records it kept would read a silent child as gone.
    pub fn new(parent_hub: Option<String>, state_dir: Option<PathBuf>) -> Result<Self, PoolError> {
        let parent = parent_hub
            .map(|hub| {
                gdl_model::check_hub_address(&hub)
                    .map_err(|why| PoolError::new(format!("[pool] parent_hub: {why}")))?;
                let watcher = PoolWatcher::new().map_err(|e| PoolError::new(e.to_string()))?;
                Ok::<_, PoolError>(Arc::new(Parent { hub, watcher }))
            })
            .transpose()?;
        let children = match state_dir {
            Some(path) => RelayDirectory::open(path, MemoryLimits::default())
                .map_err(|e| PoolError::new(format!("[pool] state_dir: {e}")))?,
            None => RelayDirectory::new(),
        };
        Ok(Self {
            children,
            parent,
            cadence: Arc::new(watch::Sender::new(None)),
        })
    }

    /// The cadence of the parent's answers, as it changes: `None` until
    /// the parent has answered.
    pub(crate) fn cadence(&self) -> watch::Receiver<Option<Duration>> {
        self.cadence.subscribe()
    }

    /// Whether the pool's hub reports to a parent's hub.
    pub(crate) fn reports_up(&self) -> bool {
        self.parent.is_some()
    }

    /// The records the pool's children report, for the hub's gate alone.
    pub(crate) fn children(&self) -> &RelayDirectory {
        &self.children
    }

    /// The record the parent's hub holds for the pool `pool` of `class`,
    /// for a restarted hub to keep its pool's history: `None` for a pool
    /// that reports nowhere, or when the parent holds none, as a parent of
    /// a build that does not answer the route says too. A parent that
    /// cannot be reached is an error, never a pool with no record.
    pub(crate) async fn held(
        &self,
        class: &str,
        pool: &str,
    ) -> Result<Option<PoolRecord>, DirectoryError> {
        let Some(parent) = &self.parent else {
            return Ok(None);
        };
        parent.watcher.held(&parent.hub, class, pool).await
    }

    /// Report `report` to the parent: the interval of its answer.
    async fn report(&self, report: Report) -> Result<Option<Duration>, DirectoryError> {
        let Some(parent) = &self.parent else {
            return Ok(None);
        };
        parent.watcher.report(&parent.hub, &report).await.map(Some)
    }
}

impl PoolDirectory for HubDirectory {
    type Watch = MemoryWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        let lease = record.lease;
        if let Some(answer) = self.report(Report::Record(record)).await? {
            // Within a third of the record's lease whatever the parent
            // says, so that two lost reports do not drop the pool.
            let cadence = answer.clamp(MIN_CADENCE, (lease / 3).max(MIN_CADENCE));
            self.cadence.send_if_modified(|current| {
                let changed = *current != Some(cadence);
                *current = Some(cadence);
                changed
            });
        }
        Ok(())
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.report(Report::Withdrawal(withdrawal)).await.map(drop)
    }

    fn watch(&self, class: &str, from: Option<gdl_pools::ResumeToken>) -> MemoryWatch {
        self.children.watch(class, from)
    }
}

/// A child's report that its parent's hub has verified: the only way a
/// record or withdrawal reaches the records the hub holds for its
/// children. Only [`Reports::verify`] makes one, so nothing outside the
/// hub, and no report read from the wire, can be written unverified.
#[derive(Debug)]
pub struct VerifiedReport(Report);

/// The gate of a parent pool's hub: it verifies its children's reports
/// and writes what it verified into the records it holds for them.
pub(crate) struct Reports {
    children: RelayDirectory,
    /// The parent's own pool, which a child's record names as its `home`.
    pool: String,
    contract: u64,
    placement: u64,
    /// The interval at which the parent asks for reports, unless a
    /// record's lease asks for less.
    heartbeat: Duration,
    /// Held by each report from its check to the end of its write, taken
    /// in the order the reports arrive. A withdrawal is checked against
    /// what is held, so it must not be checked while an earlier report of
    /// its pool is still being stored.
    order: Arc<tokio::sync::Mutex<()>>,
}

impl Reports {
    /// The gate of the pool `record` describes, writing into `children`.
    pub(crate) fn new(children: RelayDirectory, record: &PoolRecord, heartbeat: Duration) -> Self {
        Self {
            children,
            pool: record.name.clone(),
            contract: record.contract,
            placement: record.placement,
            heartbeat,
            order: Arc::default(),
        }
    }

    /// The record held for the child `pool` of `class`, expired or not.
    pub(crate) fn held(&self, class: &str, pool: &str) -> Option<PoolRecord> {
        self.children.record(class, pool)
    }

    /// Verify `report`, write it, and answer with the interval at which
    /// the child's next report is expected. Reports are verified and
    /// written one at a time, in the order they arrive. Once its turn
    /// comes, a report runs to the end of its write in a task of its own,
    /// so a request that ends early cannot let the next report be checked
    /// before its write is done.
    pub(crate) async fn take(
        self: Arc<Self>,
        report: Report,
    ) -> Result<ReportReply, DirectoryError> {
        let interval = self.interval(&report);
        let turn = Arc::clone(&self.order).lock_owned().await;
        tokio::spawn(async move {
            let _turn = turn;
            match self.verify(report)? {
                Some(verified) => self.write(verified).await,
                None => Ok(()),
            }
        })
        .await
        .map_err(|error| DirectoryError::Unavailable(error.to_string()))??;
        Ok(ReportReply {
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
        })
    }

    /// Whether `report` may be written: a record of the right shape whose
    /// `home` is this pool, whose hashes are this pool's, since a parent
    /// and its children share one layout, and that is newer than what is
    /// held for the pool; or the newer withdrawal of a pool whose record is
    /// held. A withdrawal of a pool whose record is not held has nothing to
    /// withdraw, and writes nothing (`None`), so that a withdrawal can
    /// never stop a pool's records before it reports.
    pub(crate) fn verify(&self, report: Report) -> Result<Option<VerifiedReport>, DirectoryError> {
        let (class, pool) = report.pool();
        let (class, pool) = (class.to_owned(), pool.to_owned());
        let offered = match &report {
            Report::Record(record) => {
                record.check()?;
                if !record.dropped.is_empty() {
                    tracing::debug!(
                        class,
                        pool,
                        kinds = ?record.dropped,
                        "a child's record holds properties that do not check: they are left out"
                    );
                }
                if record.home.as_deref() != Some(self.pool.as_str()) {
                    return Err(DirectoryError::Refused(format!(
                        "pool `{pool}` names {} as its parent, not this pool, `{}`",
                        record
                            .home
                            .as_deref()
                            .map_or("no pool".to_string(), |home| format!("`{home}`")),
                        self.pool
                    )));
                }
                if (record.contract, record.placement) != (self.contract, self.placement) {
                    return Err(DirectoryError::Refused(format!(
                        "pool `{pool}` has contract hash {:016x} and placement hash {:016x}, and this pool {:016x} and {:016x}: a parent and its children share one layout",
                        record.contract, record.placement, self.contract, self.placement
                    )));
                }
                record.version()
            }
            Report::Withdrawal(withdrawal) => {
                withdrawal.check()?;
                if !self.children.holds_record(&class, &pool) {
                    return Ok(None);
                }
                withdrawal.version()
            }
        };
        if let Some(held) = self.children.held(&class, &pool)
            && offered <= held
        {
            return Err(DirectoryError::NotNewer {
                class,
                pool,
                held,
                offered,
            });
        }
        Ok(Some(VerifiedReport(report)))
    }

    /// Write what `verified` carries. The directory still refuses a write
    /// that is not newer, which settles two reports of one pool verified at
    /// once.
    pub(crate) async fn write(&self, verified: VerifiedReport) -> Result<(), DirectoryError> {
        match verified.0 {
            Report::Record(record) => self.children.publish(record).await,
            Report::Withdrawal(withdrawal) => self.children.withdraw(withdrawal).await,
        }
    }

    /// The interval at which the child's next report is expected: this
    /// hub's heartbeat, but never more than a third of the record's lease,
    /// so that a child that reports at it is not dropped while it reports,
    /// even when two of its reports are lost.
    fn interval(&self, report: &Report) -> Duration {
        match report {
            Report::Record(record) => self
                .heartbeat
                .min(record.lease / 3)
                .max(Duration::from_millis(1)),
            Report::Withdrawal(_) => self.heartbeat,
        }
    }
}
