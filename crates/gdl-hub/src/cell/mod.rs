use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use gdl_cells::{CellDirectory, CellRecord, CellState};
use gdl_model::{CellSettings, DeploymentSpec};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use velo_hub::axum::{Json, Router, http::StatusCode, routing::get};
use velo_hub::store::Removal;
use velo_hub::{Committed, Extension, RegistrationView, Rejection, StartContext, Txn};

use crate::Quorum;

mod count;
mod publish;

/// Whether a cell can take work, judged and published by the cell's own
/// hub: the `gdl-cell` extension, beside [`crate::GraphAdmission`].
///
/// It counts, per segment of the cell's class, the instances usable for
/// fresh work: those in the segment's `ready` service and not in its
/// `retired` one. Never `serving`, which an instance announces before the
/// routing barrier and keeps while it drains. An instance that leaves the
/// hub leaves both services. The counts are read from the hub's own
/// service watches, over loopback, since velo-hub keeps its service
/// directory to itself; one watch per service, whatever the number of
/// instances.
///
/// The state follows [`Quorum`]. Its routes, under `/v1/ext/gdl-cell/` on
/// the public listener: `GET health`, the cell health endpoint, which
/// answers 200 when the cell is `ready` and 503 otherwise, with a
/// [`CellHealth`] body.
///
/// It publishes the cell's [`CellRecord`] through `directory` at once, on
/// each change of state, and on every heartbeat, with an epoch fixed for
/// the life of the hub process and later than any the directory held for
/// the cell when it started, and a `seq` raised on every write. It does
/// not withdraw the record when the hub stops: a cell that stops without
/// draining leaves its record to expire on its readers' clocks.
pub struct CellReadiness<D: CellDirectory> {
    directory: Arc<D>,
    /// The record as published, but for its state and version.
    record: CellRecord,
    /// The class's segments, with the cell's quorum of each.
    quorums: Vec<(String, u32)>,
    timing: CellTiming,
    health: watch::Sender<CellHealth>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<D: CellDirectory> CellReadiness<D> {
    /// The extension for the cell `cell` of the deployment `spec`,
    /// publishing through `directory`. Settings from which no well-formed
    /// record of this deployment's cell can be published are refused here:
    /// a class the placement does not have, a `ready` key outside it, no
    /// hub address, a malformed name or region, a lease that is not whole
    /// milliseconds, or a heartbeat of zero.
    pub fn new(
        spec: &DeploymentSpec,
        cell: CellSettings,
        directory: D,
        timing: CellTiming,
    ) -> Result<Self, CellError> {
        let layout = gdl_model::layout(&spec.graph, &spec.placement)
            .map_err(|e| CellError::new(e.to_string()))?;
        cell.check_hub(&layout)
            .map_err(|e| CellError::new(e.to_string()))?;
        let hub = cell.hub.clone().ok_or_else(|| {
            CellError::new(
                "[cell] hub: a cell's hub publishes the address other cells' hubs reach it at, and none is set",
            )
        })?;
        if timing.heartbeat.is_zero() {
            return Err(CellError::new("the cell's heartbeat is zero"));
        }
        let record = CellRecord {
            name: cell.name.clone(),
            class: cell.class.clone(),
            region: cell.region.clone(),
            contract: layout.contract_hash(),
            placement: layout.placement_hash(),
            state: CellState::Unavailable,
            epoch: 0,
            seq: 0,
            hub,
            lease: timing.lease,
            load: None,
        };
        record.check().map_err(|e| CellError::new(e.to_string()))?;
        let quorums: Vec<(String, u32)> = spec
            .placement
            .classes()
            .find(|(class, _)| *class == cell.class)
            .map(|(_, segments)| segments)
            .unwrap_or_default()
            .iter()
            .map(|segment| (segment.clone(), cell.quorum(segment)))
            .collect();
        let health = CellHealth::of(
            &Quorum::new(quorums.iter().cloned(), timing.hold),
            CellState::Unavailable,
        );
        Ok(Self {
            directory: Arc::new(directory),
            record,
            quorums,
            timing,
            health: watch::Sender::new(health),
            tasks: Mutex::default(),
        })
    }

    /// The cell's health as it changes: its state, and each segment's
    /// usable instances and quorum.
    pub fn health(&self) -> watch::Receiver<CellHealth> {
        self.health.subscribe()
    }
}

impl<D: CellDirectory> Extension for CellReadiness<D> {
    const NAME: &'static str = "gdl-cell";
    type Registration = ();
    type Staged = ();

    fn start(&self, ctx: &StartContext<'_>) -> impl Future<Output = anyhow::Result<()>> + Send {
        // The hub serves only once every extension has started, so the
        // counting task connects in the background and the cell is
        // `unavailable` until it has.
        let cancel = ctx.cancellation().clone();
        let (counts, updates) = tokio::sync::mpsc::channel(count::BUFFER);
        let segments = self.quorums.iter().map(|(name, _)| name.clone()).collect();
        let counting = tokio::spawn(count::run(
            count::loopback(ctx.public_addr()),
            ctx.control_addr().port(),
            segments,
            counts,
            cancel.clone(),
        ));
        let publishing = tokio::spawn(
            publish::Publisher {
                directory: Arc::clone(&self.directory),
                record: self.record.clone(),
                quorum: Quorum::new(self.quorums.iter().cloned(), self.timing.hold),
                heartbeat: self.timing.heartbeat,
                health: self.health.clone(),
            }
            .run(updates, cancel),
        );
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend([counting, publishing]);
        async { Ok(()) }
    }

    async fn shutdown(&self) {
        // The hub cancels the tasks before it shuts its extensions down;
        // what is left is to see them end.
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }

    fn public_routes(self: Arc<Self>) -> Router {
        Router::new().route(
            "/health",
            get(move || {
                let health = self.health.borrow().clone();
                async move {
                    let status = match health.state {
                        CellState::Ready => StatusCode::OK,
                        _ => StatusCode::SERVICE_UNAVAILABLE,
                    };
                    (status, Json(health))
                }
            }),
        )
    }

    fn descriptor(&self) -> serde_json::Value {
        serde_json::json!({
            "cell": self.record.name,
            "class": self.record.class,
            "region": self.record.region,
        })
    }

    async fn prepare(&self, _: &Txn<'_>, _: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn admit(&self, _: &RegistrationView<'_>, _: Option<&()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, (): (), _: &Committed<'_>) {}

    fn on_removed(&self, _: Removal) {}
}

/// How a cell's hub paces its judgment and its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellTiming {
    /// How long the quorum must hold before the cell is `ready`.
    pub hold: Duration,
    /// How often the record is published while the state does not change.
    pub heartbeat: Duration,
    /// How long a reader keeps the record without seeing it published
    /// again: three heartbeats by default.
    pub lease: Duration,
}

impl Default for CellTiming {
    fn default() -> Self {
        Self {
            hold: Quorum::DEFAULT_HOLD,
            heartbeat: Duration::from_secs(10),
            lease: CellRecord::DEFAULT_LEASE,
        }
    }
}

/// The body of the cell health endpoint: the cell's state, and each
/// segment of its class with its usable instances and its quorum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellHealth {
    /// The cell's state.
    pub state: CellState,
    /// Each segment of the cell's class, by name.
    pub segments: BTreeMap<String, SegmentHealth>,
}

impl CellHealth {
    /// The body for `quorum`'s counts in `state`.
    pub(crate) fn of(quorum: &Quorum, state: CellState) -> Self {
        Self {
            state,
            segments: quorum
                .segments()
                .map(|(name, usable, quorum)| (name.to_string(), SegmentHealth { usable, quorum }))
                .collect(),
        }
    }
}

/// One segment of a cell's class, in [`CellHealth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentHealth {
    /// The instances usable for fresh work.
    pub usable: u32,
    /// The usable instances the cell needs to be ready.
    pub quorum: u32,
}

/// Why a cell's hub cannot start.
#[derive(Clone, PartialEq, Eq)]
pub struct CellError(String);

impl CellError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for CellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// The message alone, as `Display` gives it: a binary that returns the
// error from `main` prints its `Debug`, which would otherwise quote and
// escape the message.
impl std::fmt::Debug for CellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CellError {}
