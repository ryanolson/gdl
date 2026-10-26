//! The `relay` directory backend in a cell's hub: the hub reports its
//! cell's record to its parent cell's hub, and holds the records its own
//! children report to it, which its follow watches.

use std::sync::Arc;
use std::time::Duration;

use gdl_cells::{
    CellDirectory, CellRecord, DirectoryError, RelayDirectory, RelayWatch, Withdrawal,
};
use gdl_watch::{CellWatcher, Report, ReportReply};
use tokio::sync::watch;

use crate::CellError;

/// The shortest cadence a child takes from its parent's answer, so that a
/// parent's mistake cannot make a child report in a loop.
const MIN_CADENCE: Duration = Duration::from_millis(100);

/// A cell hub's directory under the `relay` backend
/// (`[cell] directory = "relay"`).
///
/// Its writes are the cell's own record and withdrawal, which it reports
/// to its parent cell's hub ([`gdl_watch::REPORT_ROUTE`]); a cell of a
/// class that nothing sends to has no parent, and reports nowhere. Its
/// watches read the records the cell's own children report to its hub,
/// which the hub holds in process ([`RelayDirectory`]), so a hub follows
/// its children as it would from any directory. A cell in the middle of a
/// chain of classes does both.
///
/// The parent answers each report with the interval at which it expects
/// the next, and the cell's hub reports at that cadence from then
/// ([`crate::CellReadiness::with_reports`]), within a third of its
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
    watcher: CellWatcher,
}

impl HubDirectory {
    /// The directory of a cell whose hub reports to the parent's hub at
    /// `parent_hub`, `host:port`, or of a cell that reports nowhere.
    pub fn new(parent_hub: Option<String>) -> Result<Self, CellError> {
        let parent = parent_hub
            .map(|hub| {
                gdl_model::check_hub_address(&hub)
                    .map_err(|why| CellError::new(format!("[cell] parent_hub: {why}")))?;
                let watcher = CellWatcher::new().map_err(|e| CellError::new(e.to_string()))?;
                Ok::<_, CellError>(Arc::new(Parent { hub, watcher }))
            })
            .transpose()?;
        Ok(Self {
            children: RelayDirectory::new(),
            parent,
            cadence: Arc::new(watch::Sender::new(None)),
        })
    }

    /// The cadence of the parent's answers, as it changes: `None` until
    /// the parent has answered.
    pub(crate) fn cadence(&self) -> watch::Receiver<Option<Duration>> {
        self.cadence.subscribe()
    }

    /// The records the cell's children report, for the hub's gate alone.
    pub(crate) fn children(&self) -> &RelayDirectory {
        &self.children
    }

    /// Report `report` to the parent: the interval of its answer.
    async fn report(&self, report: Report) -> Result<Option<Duration>, DirectoryError> {
        let Some(parent) = &self.parent else {
            return Ok(None);
        };
        parent.watcher.report(&parent.hub, &report).await.map(Some)
    }
}

impl CellDirectory for HubDirectory {
    type Watch = RelayWatch;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        let lease = record.lease;
        if let Some(answer) = self.report(Report::Record(record)).await? {
            // Within a third of the record's lease whatever the parent
            // says, so that two lost reports do not drop the cell.
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

    fn watch(&self, class: &str, from: Option<gdl_cells::ResumeToken>) -> RelayWatch {
        self.children.watch(class, from)
    }
}

/// A child's report that its parent's hub has verified: the only way a
/// record or withdrawal reaches the records the hub holds for its
/// children. Only [`Reports::verify`] makes one, so nothing outside the
/// hub, and no report read from the wire, can be written unverified.
#[derive(Debug)]
pub struct VerifiedReport(Report);

/// The gate of a parent cell's hub: it verifies its children's reports
/// and writes what it verified into the records it holds for them.
pub(crate) struct Reports {
    children: RelayDirectory,
    /// The parent's own cell, which a child's record names as its `home`.
    cell: String,
    contract: u64,
    placement: u64,
    /// The interval at which the parent asks for reports, unless a
    /// record's lease asks for less.
    heartbeat: Duration,
}

impl Reports {
    /// The gate of the cell `record` describes, writing into `children`.
    pub(crate) fn new(children: RelayDirectory, record: &CellRecord, heartbeat: Duration) -> Self {
        Self {
            children,
            cell: record.name.clone(),
            contract: record.contract,
            placement: record.placement,
            heartbeat,
        }
    }

    /// Verify `report`, write it, and answer with the interval at which
    /// the child's next report is expected.
    pub(crate) async fn take(&self, report: Report) -> Result<ReportReply, DirectoryError> {
        let interval = self.interval(&report);
        if let Some(verified) = self.verify(report)? {
            self.write(verified).await?;
        }
        Ok(ReportReply {
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
        })
    }

    /// Whether `report` may be written: a record of the right shape whose
    /// `home` is this cell, whose hashes are this cell's, since a parent
    /// and its children share one layout, and that is newer than what is
    /// held for the cell; or the newer withdrawal of a cell whose record is
    /// held. A withdrawal of a cell whose record is not held has nothing to
    /// withdraw, and writes nothing (`None`), so that a withdrawal can
    /// never stop a cell's records before it reports.
    pub(crate) fn verify(&self, report: Report) -> Result<Option<VerifiedReport>, DirectoryError> {
        let (class, cell) = report.cell();
        let (class, cell) = (class.to_owned(), cell.to_owned());
        let offered = match &report {
            Report::Record(record) => {
                record.check()?;
                if record.home.as_deref() != Some(self.cell.as_str()) {
                    return Err(DirectoryError::Refused(format!(
                        "cell `{cell}` names {} as its parent, not this cell, `{}`",
                        record
                            .home
                            .as_deref()
                            .map_or("no cell".to_string(), |home| format!("`{home}`")),
                        self.cell
                    )));
                }
                if (record.contract, record.placement) != (self.contract, self.placement) {
                    return Err(DirectoryError::Refused(format!(
                        "cell `{cell}` has contract hash {:016x} and placement hash {:016x}, and this cell {:016x} and {:016x}: a parent and its children share one layout",
                        record.contract, record.placement, self.contract, self.placement
                    )));
                }
                record.version()
            }
            Report::Withdrawal(withdrawal) => {
                withdrawal.check()?;
                if !self.children.holds_record(&class, &cell) {
                    return Ok(None);
                }
                withdrawal.version()
            }
        };
        if let Some(held) = self.children.held(&class, &cell)
            && offered <= held
        {
            return Err(DirectoryError::NotNewer {
                class,
                cell,
                held,
                offered,
            });
        }
        Ok(Some(VerifiedReport(report)))
    }

    /// Write what `verified` carries. The directory still refuses a write
    /// that is not newer, which settles two reports of one cell verified at
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
