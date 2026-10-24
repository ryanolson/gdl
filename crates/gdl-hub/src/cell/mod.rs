use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use futures::StreamExt;
use gdl_cells::{CellDirectory, CellRecord, CellState, LoadSummary};
use gdl_model::{CellSettings, DeploymentSpec};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use velo_hub::axum::extract::Path;
use velo_hub::axum::response::{IntoResponse, Response};
use velo_hub::axum::{
    Json, Router,
    http::StatusCode,
    routing::{get, post},
};
use velo_hub::protocol::ErrorCode;
use velo_hub::store::Removal;
use velo_hub::{
    AttachContext, Authorized, Committed, Extension, HubError, HubJson, RegistrationView,
    Rejection, StartContext, Txn,
};

use crate::follow::{Follower, FollowsWatch, Relay};
use crate::{GraphAdmission, Quorum};
use drain::Drain;
use feed::Feed;
use gdl_watch::{CellWatchError, CellWatcher, LoadReport};

/// How long a watch that arrives before the cell's record is first stored
/// waits for it: a follower finds the cell by its record, so it can ask
/// in the moment between the write and the hub learning it was stored.
const SETTLE: Duration = Duration::from_secs(5);

mod count;
mod drain;
mod feed;
mod judge;
mod load;
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
/// answers 200 when the cell is `ready` and, for each class its settings
/// require cells of, at least that many followed cells of the class have
/// a complete snapshot and last said they are `ready`, and 503 otherwise,
/// with a [`CellHealth`] body; and `GET watch`, the cell's watch for the hubs that
/// follow it, and for the cell's own instances, which learn of its drain
/// from it: server-sent events, each one [`gdl_watch::CellWatchEvent`] as
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
/// watch ends when the hub stops, or once a drained cell's record is
/// withdrawn.
///
/// A cell whose class sends to other classes follows their cells: for
/// each class it sends to, it watches `directory`, and follows each home
/// cell of the class (one of its own region and deployment, whose record
/// is `ready` or `degraded`) over the cell's watch, one link per cell. It
/// relays what it follows on `GET follows`, server-sent events, each one
/// [`gdl_watch::FollowEvent`] as JSON, with the same keep-alive and buffer as
/// the cell's watch: each followed cell's state, then its retired entry
/// instances, then those it exposes, the ready ones that have not retired
/// while its state is `ready` or `degraded`. A follow is dropped as soon
/// as its link ends, and followed again, as a new generation, while the
/// cell is still a home cell that takes work; a record that expires or is
/// withdrawn ends no follow, and a newer epoch in the directory ends the
/// follow of the hub it replaces. A follow is never closed while its link
/// lives.
///
/// Each usable entry instance of the cell reports its admission to the
/// hub once a load summary period (`[cell] summary_ms`), on the control
/// listener: `POST instances/{instance_id}/load` with a
/// [`gdl_watch::LoadReport`], authenticated as the instance it names by
/// velo-hub's credential check ([`velo_hub::Authority::authorize`]). A
/// report from anything else, a member of another segment, an entry
/// instance that is not ready, has retired, or has left, or one fenced off
/// by a later registration of the instance, is refused with 409, which
/// costs the reporter nothing of its registration. Once a period the hub
/// sums the latest report of each usable entry instance, dropping those
/// older than three periods, into the cell's [`LoadSummary`]: the roots
/// admitted and the admission limits summed, the usable entry instances
/// counted, and the refusals of each report's period summed; `None` while
/// no report is counted, and always for a class that no hop enters. The
/// watch sends it as a `load` event every period while there is one,
/// changed or not, so that a watcher's age of it says how fresh it is,
/// and once when there is none; the record carries it on its next write.
///
/// The cell drains when asked, on the control listener: `POST drain`, or
/// [`CellReadiness::drain`] in process. From then its state is `draining`
/// whatever its quorum, so its health endpoint answers 503, its record and
/// its watch say so, and no chooser chooses it; a drain cannot be
/// cancelled. A cell whose class no hop enters cannot be drained.
///
/// It publishes the cell's [`CellRecord`] through `directory` at once, on
/// each change of state, and on every heartbeat, from a task of its own,
/// so that a slow directory never delays the judgment or the health
/// endpoint; a state that a later one replaces while a write is in flight
/// is not written, since a reader keeps only the newest. The record's
/// epoch is fixed for the life of the hub process and later than any the
/// directory held for the cell when it started, and its `seq` rises with
/// every write. Once a drain has started and no instance of the cell's
/// class is a member of the hub, it withdraws the record, after a last
/// write that says the cell is `draining`, and writes it no more; then it
/// ends the cell's watches and refuses new ones, so that the hubs that
/// follow the cell drop it. A drained cell's hub serves the cell no more,
/// and refuses every registration from the moment the drain ends (409),
/// since an instance that joined would never learn of the drain: it is
/// started again to serve the cell. It does not withdraw the record when
/// the hub stops: a cell that stops without draining leaves its record to
/// expire on its readers' clocks.
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
    /// The classes the cell's class sends to, whose cells it follows.
    sends: Vec<String>,
    /// The load summary's period, when the class has an entry segment to
    /// summarize.
    summary: Option<Duration>,
    /// What the hub follows.
    relay: Arc<Relay>,
    /// The client of the followed cells' watches, once the hub follows
    /// any.
    watcher: OnceLock<CellWatcher>,
    /// The cell's drain.
    drain: Arc<Drain>,
    /// The state of the cell's record last stored in the directory.
    stored: watch::Sender<Option<CellState>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<D: CellDirectory> CellReadiness<D> {
    /// The extension for the cell `cell` of the deployment `spec`,
    /// publishing through `directory`. Settings from which no well-formed
    /// record of this deployment's cell can be published are refused here:
    /// a class the placement does not have, a `ready` key outside it, a
    /// `requires` key that is not a class it sends to or a count of 0, no
    /// hub address, a malformed name or region, a lease that is not whole
    /// milliseconds, a summary period shorter than 1 s, a heartbeat of
    /// zero, a watch buffer of zero, or a refollow delay of zero.
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
        gdl_model::check_summary(cell.summary).map_err(CellError::new)?;
        if timing.heartbeat.is_zero() {
            return Err(CellError::new("the cell's heartbeat is zero"));
        }
        if timing.watch_buffer == 0 {
            return Err(CellError::new("the cell's watch buffer is zero"));
        }
        if timing.refollow.is_zero() {
            // A hub that refuses at once would be dialled in a loop.
            return Err(CellError::new("the cell's refollow delay is zero"));
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
        let class_segments: Vec<String> =
            quorums.iter().map(|(segment, _)| segment.clone()).collect();
        let entries = layout.entry_segments(&cell.class);
        let segments = quorums
            .iter()
            .map(|(segment, _)| (segment.clone(), entries.contains(segment.as_str())))
            .collect();
        let feed = Feed::new(
            record.name.clone(),
            record.class.clone(),
            segments,
            cell.summary,
            timing.watch_buffer,
        );
        let sends: BTreeSet<String> = layout
            .boundaries()
            .iter()
            .filter(|boundary| boundary.from == cell.class)
            .map(|boundary| boundary.into.clone())
            .collect();
        let health = watch::Sender::new(CellHealth::of(
            &Quorum::new(quorums.iter().cloned(), timing.hold),
            CellState::Unavailable,
            &cell.requires,
        ));
        let relay = Arc::new(Relay::new(timing.watch_buffer, health.clone()));
        Ok(Self {
            directory: Arc::new(directory),
            record,
            quorums,
            timing,
            health,
            feed: Arc::new(feed),
            admission: OnceLock::new(),
            sends: sends.into_iter().collect(),
            summary: (!entries.is_empty()).then_some(cell.summary),
            relay,
            watcher: OnceLock::new(),
            drain: Arc::new(Drain::new(class_segments)),
            stored: watch::Sender::new(None),
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
    /// is settled only then; refused if the hub stops first, and once the
    /// drained cell's record is withdrawn.
    pub async fn watch(&self) -> Result<CellWatch, CellWatchError> {
        self.feed.watch().await
    }

    /// How many watches of the cell are open or waiting to begin, as it
    /// changes.
    pub fn watchers(&self) -> watch::Receiver<usize> {
        self.feed.watchers()
    }

    /// A watch of what the hub follows, in process: what `GET follows`
    /// serves, as typed events. It ends at once if the hub has stopped.
    pub fn follows(&self) -> FollowsWatch {
        self.relay.watch()
    }

    /// Start the cell's drain, as `POST drain` on the control listener
    /// does, and return the cell's health once it shows the cell
    /// `draining` and the directory has stored a record that says so: by
    /// then the cell's watchers have been sent the state too. A directory
    /// that takes more than 5 s to store the record leaves the drain to be
    /// answered without it, since the drain has started either way. From
    /// then the cell is `draining`
    /// whatever its quorum, for the life of the hub process; a drain
    /// already started is answered the same way. Refused for a cell whose
    /// class no hop enters, which has no entry instances to stop taking
    /// work at.
    pub async fn drain(&self) -> Result<CellHealth, CellError> {
        if self.summary.is_none() {
            return Err(CellError::new(
                "a cell whose class no hop enters cannot be drained",
            ));
        }
        self.drain.start();
        let (mut shown, mut stored) = (self.health.subscribe(), self.stored.subscribe());
        // The judgment takes the drain at once, and the writer writes it
        // next; the bound is for a hub whose judgment has ended, or a
        // directory that does not answer, and the drain is answered with
        // what the hub shows then.
        let _ = tokio::time::timeout(SETTLE, async {
            let _ = shown
                .wait_for(|health| health.state == CellState::Draining)
                .await;
            let _ = stored
                .wait_for(|state| *state == Some(CellState::Draining))
                .await;
        })
        .await;
        let health = shown.borrow().clone();
        Ok(health)
    }

    /// How many watches of what the hub follows are open, as it changes.
    pub fn follows_watchers(&self) -> watch::Receiver<usize> {
        self.relay.watchers()
    }

    /// How many events of a kind this build does not know the watches of
    /// the cells the hub follows have sent: each withheld its cell in the
    /// relay until the hub read the cell again.
    pub fn unknown_events(&self) -> u64 {
        self.watcher.get().map_or(0, CellWatcher::unknown_events)
    }

    /// The cells the hub has decided to follow, by class and name, each
    /// with the generation of its follow, as it changes: a cell is here
    /// from the moment its follow begins, before its snapshot is complete,
    /// and while its link is down and it waits to follow the cell again.
    pub fn following(&self) -> watch::Receiver<BTreeMap<(String, String), u64>> {
        self.relay.following()
    }
}

impl<D: CellDirectory> Extension for CellReadiness<D> {
    const NAME: &'static str = gdl_watch::EXTENSION;
    type Registration = ();
    /// A registration under way, which keeps the cell's drain from
    /// ending until it is committed or aborted.
    type Staged = drain::Joining;

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
        let (loads, summaries) = watch::channel(None::<LoadSummary>);
        let judging = tokio::spawn(
            judge::Judge {
                quorum: Quorum::new(self.quorums.iter().cloned(), self.timing.hold),
                health: self.health.clone(),
                states,
                feed: Arc::clone(&self.feed),
                draining: self.drain.started(),
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
                drain: Arc::clone(&self.drain),
                stored: self.stored.clone(),
            }
            .run(written, summaries, cancel.clone()),
        );
        let mut tasks = vec![counting, judging, publishing];
        // A class no hop enters has no entry instance to report, and so
        // no summary to make.
        if let Some(period) = self.summary {
            tasks.push(tokio::spawn(load::run(
                Arc::clone(&self.feed),
                period,
                loads,
                cancel,
            )));
        }
        let follows = if self.sends.is_empty() {
            Ok(())
        } else {
            match self.watcher.get() {
                Some(watcher) => Ok(watcher.clone()),
                None => {
                    CellWatcher::new().map(|watcher| self.watcher.get_or_init(|| watcher).clone())
                }
            }
            .map(|watcher| {
                for class in &self.sends {
                    let follower = Follower {
                        directory: Arc::clone(&self.directory),
                        relay: Arc::clone(&self.relay),
                        watcher: watcher.clone(),
                        refollow: self.timing.refollow,
                        class: class.clone(),
                        region: self.record.region.clone(),
                        contract: self.record.contract,
                        placement: self.record.placement,
                    };
                    tasks.push(tokio::spawn(follower.run(ctx.cancellation().clone())));
                }
            })
        };
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(tasks);
        async { follows.map_err(anyhow::Error::from) }
    }

    async fn shutdown(&self) {
        self.feed.close();
        self.relay.close();
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
        let relay = Arc::clone(&self.relay);
        Router::new()
            .route(
                "/health",
                get(move || {
                    let health = self.health.borrow().clone();
                    async move {
                        let status = if health.healthy() {
                            StatusCode::OK
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        };
                        (status, Json(health))
                    }
                }),
            )
            .route(
                gdl_watch::WATCH_ROUTE,
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
            .route(
                gdl_watch::FOLLOWS_ROUTE,
                get({
                    let relay = Arc::clone(&relay);
                    move || {
                        let watch = relay.watch();
                        async move { crate::fanout::serve(watch.into_events()) }
                    }
                }),
            )
            .route(
                gdl_watch::FOLLOWED_ROUTE,
                get(move |Path((class, cell)): Path<(String, String)>| {
                    let events = relay.followed(&class, &cell);
                    async move { crate::fanout::serve(futures::stream::iter(events).boxed()) }
                }),
            )
    }

    fn control_routes(self: Arc<Self>) -> Router {
        let draining = Arc::clone(&self);
        Router::new()
            .route(
                gdl_watch::DRAIN_ROUTE,
                post(move || async move {
                    draining
                        .drain()
                        .await
                        .map(|health| (StatusCode::ACCEPTED, Json(health)))
                        .map_err(|refused| HubError::new(ErrorCode::Conflict, refused.to_string()))
                }),
            )
            .route(
                gdl_watch::LOAD_ROUTE,
                post(
                    move |authorized: Authorized, HubJson(load): HubJson<LoadReport>| async move {
                        self.feed
                            .report(
                                authorized.owner,
                                authorized.lease,
                                load,
                                std::time::Instant::now(),
                            )
                            .map(|()| StatusCode::NO_CONTENT)
                            // Not 401 or 404: velo-hub's client drops its
                            // registration on those.
                            .map_err(|refused| HubError::new(ErrorCode::Conflict, refused))
                    },
                ),
            )
    }

    fn descriptor(&self) -> serde_json::Value {
        serde_json::json!({
            "cell": self.record.name,
            "class": self.record.class,
            "region": self.record.region,
        })
    }

    async fn prepare(&self, _: &Txn<'_>, _: Option<()>) -> Result<drain::Joining, Rejection> {
        self.drain.join().map_err(Rejection::conflict)
    }

    fn admit(&self, _: &RegistrationView<'_>, _: Option<&()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, joining: drain::Joining, committed: &Committed<'_>) {
        // Admission committed first, as this extension requires it; its
        // lock is released before the feed's is taken.
        let member = self
            .admission
            .get()
            .and_then(|admission| admission.member(committed.owner));
        if let Some((segment, peer)) = member {
            self.drain
                .joined(&segment, committed.owner, committed.incarnation);
            self.feed
                .joined(&segment, committed.incarnation, committed.epoch, peer);
        }
        // A member now, if it is one of the class's, before it stops
        // holding the drain as a registration under way.
        drop(joining);
    }

    fn on_removed(&self, removal: Removal) {
        self.drain.removed(&removal);
        self.feed.removed(removal);
    }
}

/// The watch as server-sent events, each one JSON value; a hub that has
/// not stored its record in time, or stops first, answers 503.
fn serve(watch: Result<CellWatch, CellWatchError>) -> Response {
    match watch {
        Ok(watch) => crate::fanout::serve(Box::pin(watch)),
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
    /// How many events a watcher of the cell, or of what its hub follows,
    /// may fall behind by before its watch ends: at least this many,
    /// rounded up to a power of two.
    pub watch_buffer: usize,
    /// How long after a follow's link ends the hub waits before it may
    /// follow the cell again.
    pub refollow: Duration,
}

impl Default for CellTiming {
    fn default() -> Self {
        Self {
            hold: Quorum::DEFAULT_HOLD,
            heartbeat: Duration::from_secs(10),
            lease: CellRecord::DEFAULT_LEASE,
            watch_buffer: 1024,
            refollow: Duration::from_millis(250),
        }
    }
}

/// The body of the cell health endpoint: the cell's state, each segment
/// of its class with its usable instances and its quorum, and, for a cell
/// whose settings require cells of the classes it sends to, each such
/// class with the count required and the count met.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellHealth {
    /// The cell's state, from its own quorum: what its record and its
    /// watch carry.
    pub state: CellState,
    /// Each segment of the cell's class, by name.
    pub segments: BTreeMap<String, SegmentHealth>,
    /// Each class the cell's settings require cells of, by name; absent
    /// from the body when there is none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requires: BTreeMap<String, RequiredHealth>,
}

impl CellHealth {
    /// The body for `quorum`'s counts in `state`, with none of the
    /// required cells met yet.
    pub(crate) fn of(quorum: &Quorum, state: CellState, requires: &BTreeMap<String, u32>) -> Self {
        Self {
            state,
            segments: quorum
                .segments()
                .map(|(name, usable, quorum)| (name.to_string(), SegmentHealth { usable, quorum }))
                .collect(),
            requires: requires
                .iter()
                .map(|(class, &required)| (class.clone(), RequiredHealth { required, met: 0 }))
                .collect(),
        }
    }

    /// Whether the endpoint answers 200: the cell is `ready`, and each
    /// required class has as many ready, followed cells as it requires.
    pub fn healthy(&self) -> bool {
        self.state == CellState::Ready
            && self
                .requires
                .values()
                .all(|required| required.met >= required.required)
    }
}

/// One class a cell requires cells of, in [`CellHealth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredHealth {
    /// The ready, followed cells of the class the cell needs to be
    /// healthy.
    pub required: u32,
    /// The followed cells of the class whose snapshot is complete and
    /// whose link last said they are `ready`.
    pub met: u32,
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
