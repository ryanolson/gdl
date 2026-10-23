use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use gdl_cells::{CellDirectory, CellRecord, CellState};
use gdl_model::{CellSettings, DeploymentSpec};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use velo_hub::axum::response::sse::{Event, KeepAlive, Sse};
use velo_hub::axum::response::{IntoResponse, Response};
use velo_hub::axum::{Json, Router, http::StatusCode, routing::get};
use velo_hub::store::Removal;
use velo_hub::{
    AttachContext, Committed, Extension, RegistrationView, Rejection, StartContext, Txn,
};

use crate::{CellWatchError, GraphAdmission, Quorum};
use feed::Feed;

/// How often a watch with nothing to say sends a keep-alive comment, as
/// velo-hub's service watch does, so that a watcher's idle timeout tells a
/// stalled connection from a quiet cell.
const KEEP_ALIVE: Duration = Duration::from_secs(10);

/// How long a watch that arrives before the cell's record is first stored
/// waits for it: a follower finds the cell by its record, so it can ask
/// in the moment between the write and the hub learning it was stored.
const SETTLE: Duration = Duration::from_secs(5);

mod count;
mod feed;
mod judge;
mod publish;

pub use feed::CellWatch;

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
/// [`CellHealth`] body; and `GET watch`, the cell's watch for the hubs that
/// follow it: server-sent events, each one [`crate::CellWatchEvent`] as
/// JSON, with a keep-alive every 10 s. The watch carries the cell's state
/// and epoch and its entry instances, ready and retired, with their
/// `PeerInfo` from [`GraphAdmission`]'s members, so the hub must run
/// admission too ([`Extension::requires`]). Who an entry instance is, and when it
/// leaves, comes from the hub's registration hooks; when it is ready or
/// retires, from the same loopback watches as the counts. Each watcher
/// holds only its place in a buffer of [`CellTiming::watch_buffer`] events
/// shared by all: one that falls further behind has its watch ended, and
/// watches again from a snapshot, so a slow watcher never slows the
/// counting, the judgment, the record, or another watcher. A watch begins
/// once the record is first stored, since its epoch is settled only then:
/// one that arrives earlier waits up to 5 s, then is refused (503). Every
/// watch ends when the hub stops.
///
/// It publishes the cell's [`CellRecord`] through `directory` at once, on
/// each change of state, and on every heartbeat, from a task of its own,
/// so that a slow directory never delays the judgment or the health
/// endpoint; a state that a later one replaces while a write is in flight
/// is not written, since a reader keeps only the newest. The record's
/// epoch is fixed for the life of the hub process and later than any the
/// directory held for the cell when it started, and its `seq` rises with
/// every write. It does not withdraw the record when the hub stops: a
/// cell that stops without draining leaves its record to expire on its
/// readers' clocks.
pub struct CellReadiness<D: CellDirectory> {
    directory: Arc<D>,
    /// The record as published, but for its state and version.
    record: CellRecord,
    /// The class's segments, with the cell's quorum of each.
    quorums: Vec<(String, u32)>,
    timing: CellTiming,
    health: watch::Sender<CellHealth>,
    /// The cell's watchers.
    feed: Arc<Feed>,
    /// The hub's admission, whose members say who an entry instance is.
    admission: OnceLock<Arc<GraphAdmission>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<D: CellDirectory> CellReadiness<D> {
    /// The extension for the cell `cell` of the deployment `spec`,
    /// publishing through `directory`. Settings from which no well-formed
    /// record of this deployment's cell can be published are refused here:
    /// a class the placement does not have, a `ready` key outside it, no
    /// hub address, a malformed name or region, a lease that is not whole
    /// milliseconds, a heartbeat of zero, or a watch buffer of zero.
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
        gdl_model::check_hub_address(&hub).map_err(CellError::new)?;
        if timing.heartbeat.is_zero() {
            return Err(CellError::new("the cell's heartbeat is zero"));
        }
        if timing.watch_buffer == 0 {
            return Err(CellError::new("the cell's watch buffer is zero"));
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
        // An entry segment holds the inner end of a hop that crosses into
        // the class: it is all a chooser in another class reaches.
        let entries: BTreeSet<&str> = layout
            .boundaries()
            .iter()
            .filter(|boundary| boundary.into == cell.class)
            .filter_map(|boundary| layout.hops()[boundary.hop].backward_runs.first())
            .map(|inner| inner.segment.as_str())
            .collect();
        let segments = quorums
            .iter()
            .map(|(segment, _)| (segment.clone(), entries.contains(segment.as_str())))
            .collect();
        let feed = Feed::new(
            record.name.clone(),
            record.class.clone(),
            segments,
            timing.watch_buffer,
        );
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
            feed: Arc::new(feed),
            admission: OnceLock::new(),
            tasks: Mutex::default(),
        })
    }

    /// The cell's health as it changes: its state, and each segment's
    /// usable instances and quorum.
    pub fn health(&self) -> watch::Receiver<CellHealth> {
        self.health.subscribe()
    }

    /// A watch of the cell, in process: what `GET watch` serves, as typed
    /// events. It begins once the cell's record is stored, since its epoch
    /// is settled only then; refused if the hub stops first.
    pub async fn watch(&self) -> Result<CellWatch, CellWatchError> {
        self.feed.watch().await
    }

    /// How many watches of the cell are open, as it changes.
    pub fn watchers(&self) -> watch::Receiver<usize> {
        self.feed.watchers()
    }
}

impl<D: CellDirectory> Extension for CellReadiness<D> {
    const NAME: &'static str = crate::watch::EXTENSION;
    type Registration = ();
    type Staged = ();

    fn requires() -> &'static [&'static str] {
        &[<GraphAdmission as Extension>::NAME]
    }

    async fn attach(&self, ctx: &AttachContext<'_>) -> anyhow::Result<()> {
        let _ = self.admission.set(ctx.extension::<GraphAdmission>()?);
        Ok(())
    }

    fn start(&self, ctx: &StartContext<'_>) -> impl Future<Output = anyhow::Result<()>> + Send {
        // The hub serves only once every extension has started, so the
        // counting task connects in the background and the cell is
        // `unavailable` until it has.
        let cancel = ctx.cancellation().clone();
        let (counts, updates) = tokio::sync::mpsc::channel(count::BUFFER);
        let segments = self.quorums.iter().map(|(name, _)| name.clone()).collect();
        let feed = Arc::clone(&self.feed);
        let counting = tokio::spawn(count::run(
            count::loopback(ctx.public_addr()),
            ctx.control_addr().port(),
            segments,
            counts,
            move |index, watched, event| feed.observe(index, watched, event),
            cancel.clone(),
        ));
        let (states, written) = watch::channel(CellState::Unavailable);
        let judging = tokio::spawn(
            judge::Judge {
                quorum: Quorum::new(self.quorums.iter().cloned(), self.timing.hold),
                health: self.health.clone(),
                states,
                feed: Arc::clone(&self.feed),
            }
            .run(updates, cancel.clone()),
        );
        // The writer is apart from the judgment, so that a directory that
        // is slow to take a write never leaves the health endpoint behind
        // the instances.
        let publishing = tokio::spawn(
            publish::Publisher {
                directory: Arc::clone(&self.directory),
                record: self.record.clone(),
                heartbeat: self.timing.heartbeat,
                feed: Arc::clone(&self.feed),
            }
            .run(written, cancel),
        );
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend([counting, judging, publishing]);
        async { Ok(()) }
    }

    async fn shutdown(&self) {
        self.feed.close();
        // The hub cancels the tasks before it shuts its extensions down;
        // what is left is to see them end.
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }

    fn public_routes(self: Arc<Self>) -> Router {
        let feed = Arc::clone(&self.feed);
        Router::new()
            .route(
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
            .route(
                crate::watch::ROUTE,
                get(move || {
                    let feed = Arc::clone(&feed);
                    async move {
                        let watch = tokio::time::timeout(SETTLE, feed.watch())
                            .await
                            .unwrap_or_else(|_| {
                                Err(CellWatchError::new(
                                    "the cell has not published its record yet, so its epoch is not settled",
                                ))
                            });
                        serve(watch)
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

    fn commit(&self, (): (), committed: &Committed<'_>) {
        // Admission committed first, as this extension requires it; its
        // lock is released before the feed's is taken.
        let member = self
            .admission
            .get()
            .and_then(|admission| admission.member(committed.owner));
        if let Some((segment, peer)) = member {
            self.feed.joined(&segment, committed.incarnation, peer);
        }
    }

    fn on_removed(&self, removal: Removal) {
        self.feed.removed(removal);
    }
}

/// The watch as server-sent events, each one JSON value; a hub that has
/// not stored its record in time, or stops first, answers 503.
fn serve(watch: Result<CellWatch, CellWatchError>) -> Response {
    match watch {
        Ok(watch) => {
            let events = futures::StreamExt::map(watch, |event| Event::default().json_data(event));
            Sse::new(events)
                .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
                .into_response()
        }
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response(),
    }
}

/// How a cell's hub paces its judgment, its record, and its watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellTiming {
    /// How long the quorum must hold before the cell is `ready`.
    pub hold: Duration,
    /// How often the record is published while the state does not change.
    pub heartbeat: Duration,
    /// How long a reader keeps the record without seeing it published
    /// again: three heartbeats by default.
    pub lease: Duration,
    /// How many events a watcher of the cell may fall behind by before its
    /// watch ends: at least this many, rounded up to a power of two.
    pub watch_buffer: usize,
}

impl Default for CellTiming {
    fn default() -> Self {
        Self {
            hold: Quorum::DEFAULT_HOLD,
            heartbeat: Duration::from_secs(10),
            lease: CellRecord::DEFAULT_LEASE,
            watch_buffer: 1024,
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
