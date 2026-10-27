use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use gdl_cells::{CellDirectory, CellRecord, CellState, LoadSummary, Presence};
use gdl_model::{CellSettings, DeploymentSpec};
use serde::Deserialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use velo_hub::axum::extract::Path;
use velo_hub::axum::response::{IntoResponse, Response};
use velo_hub::axum::{
    Json, Router,
    extract::Query,
    http::StatusCode,
    routing::{get, post},
};
use velo_hub::protocol::ErrorCode;
use velo_hub::store::Removal;
use velo_hub::{
    AttachContext, Authorized, Committed, Extension, HubError, HubJson, RegistrationView,
    Rejection, StartContext, Txn,
};

use crate::follow::{Choosers, Follower, FollowsWatch, Relay, Retirement};
use crate::report::Reports;
use crate::{GraphAdmission, HubDirectory, Quorum};
use drain::Drain;
use feed::Feed;
use gdl_watch::{CellWatchError, CellWatcher, LoadReport, Unoffered};

/// How long a watch that arrives before the cell's record is first stored
/// waits for it: a follower finds the cell by its record, so it can ask
/// in the moment between the write and the hub learning it was stored.
const SETTLE: Duration = Duration::from_secs(5);

mod count;
mod drain;
mod feed;
mod followers;
mod health;
mod judge;
mod load;
mod models;
mod publish;
mod retiring;
mod timing;

#[cfg(test)]
mod tests;

pub use feed::CellWatch;
pub use health::{CellHealth, RequiredHealth, SegmentHealth};
pub use timing::{CellTiming, RefusedWatches};

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
/// The state follows [`Quorum`], and the cell's record carries it, with
/// the cell's parent (`[cell] home`) unless no class sends to the cell's
/// class. Its routes, under `/v1/ext/gdl-cell/` on
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
/// withdrawn. Each watch route, `GET watch` and the relay's two below,
/// serves at most its cap of watches at once ([`CellTiming::watch_caps`]):
/// one beyond it, a waiting one included, is refused with 503 and a
/// `Retry-After`, before a snapshot is built or a subscription taken for
/// it, and a watch that closes frees its place at once. The watches taken
/// in process are not capped.
///
/// A cell whose class sends to other classes follows their cells: for
/// each class it sends to, it watches `directory`, and follows each child
/// of its cell in the class (a cell whose record names this cell as its
/// parent, `home`, has this deployment's contract and placement hashes,
/// is `ready` or `degraded`, and is not scaled to 0, whatever its region)
/// over the cell's
/// watch, one link per cell. It
/// relays what it follows on `GET follows`, server-sent events, each one
/// [`gdl_watch::FollowEvent`] as JSON, with the same keep-alive and buffer as
/// the cell's watch: each followed cell's state, then its retired entry
/// instances, then those it exposes, the ready ones that have not retired
/// while its state is `ready` or `degraded`. A follow is offered to the
/// hub's choosers while its link lives and its state is one of those, and
/// each change between offered and not offered is its next revision, which
/// the relay carries. One that is not offered closes once every chooser of
/// its class among the hub's members, the instances of the segments that
/// hold the outer end of a hop into the class, has acknowledged its
/// current revision or left: a member counts as a chooser once it says
/// that it runs a follower of the relay (`POST instances/{instance_id}/chooser`
/// on the control listener, with its own credential), since one that runs
/// none, as an instance without `[cell]`, never offers and never
/// acknowledges; and it acknowledges with `POST instances/{instance_id}/unoffered` on
/// the control listener, a [`gdl_watch::Unoffered`] with the chooser's own
/// credential, refused with 409 from any other instance. A chooser names
/// itself in its watch of the relay (`GET follows?instance=<id>`,
/// [`gdl_watch::RelayWatcher`]): one that has had no such watch open for
/// [`CellTiming::lost_chooser`], from when its last one closed or from when
/// it declared, is not waited for until it watches again, since it cannot
/// learn what to acknowledge. Then the relay drops it and its link ends. A retirement of a followed cell's entry
/// instance waits, in the same way, for the choosers whose hops enter the
/// instance's segment, as they are when the relay applies it: each says
/// that it avoids the instance once its tables hold it retired
/// (`POST instances/{instance_id}/avoids` on the control listener, a
/// [`gdl_watch::Avoids`] with its own credential, refused with 409 from any
/// other instance), and one that leaves counts as one that did. A chooser
/// that says it runs a follower later counts as one that did, since the
/// relay's snapshot shows it the instance retired; so does a retirement in
/// the snapshot a follow is installed from, which the cell's hub does not
/// wait for this follow for. Once every chooser it waited for avoids it,
/// the hub tells the cell's hub, once for its whole cell
/// ([`gdl_watch::Avoided`]); a follow that closes first is released
/// instead. A follow whose link ends, or that a newer
/// epoch in the directory replaces, is not offered from then, and is kept
/// beside the cell's later follows until it closes the same way; the cell
/// is followed again, as a new generation, while it is still a child that
/// takes work, and, after a follow closed, only from a later write
/// of its record. A record that expires or is withdrawn ends no follow.
///
/// Each usable entry instance of the cell reports its admission to the
/// hub once a load summary period (`[cell] summary_ms`), on the control
/// listener: `POST instances/{instance_id}/load` with a
/// [`gdl_watch::LoadReport`], authenticated as the instance it names by
/// velo-hub's credential check ([`velo_hub::Authority::authorize`]). A
/// report from anything else, a member of another segment, an entry
/// instance that is not ready, has retired, or has left, or one fenced off
/// by a later registration of the instance, is refused with 409, which
/// costs the reporter nothing of its registration. A report that carries a
/// model its instance drains is answered once a record that carries the
/// drain is stored, or with 503 after half a summary period. Once a period the hub
/// sums the latest report of each usable entry instance, dropping those
/// older than three periods, into the cell's [`LoadSummary`]: the roots
/// admitted and the admission limits summed, the usable entry instances
/// counted, and the refusals of each report's period summed; `None` while
/// no report is counted, and always for a class that no hop enters. The
/// watch sends it as a `load` event every period while there is one,
/// changed or not, so that a watcher's age of it says how fresh it is,
/// and once when there is none; the record carries it on its next write.
/// A report may carry the instance's models
/// ([`gdl_watch::LoadReport::models`]): from what its usable entry
/// instances last said, or the models its settings declare
/// (`[cell] models`) for one that says none, the hub makes the record's
/// model entries, each with a state that keeps its history, and writes the
/// record on each change of them.
///
/// The record says whether the cell serves or is scaled to 0 on purpose
/// (`POST presence` on the control listener, a
/// [`gdl_watch::SetPresence`], or [`CellReadiness::set_presence`] in
/// process), answered once the directory has stored a record that says
/// so. Before its first write, the hub reads the cell's last record, from
/// its parent's hub under the `relay` backend, so that a restart keeps the
/// models' history.
///
/// The cell drains when asked, on the control listener: `POST drain`, or
/// [`CellReadiness::drain`] in process. From then its state is `draining`
/// whatever its quorum, so its health endpoint answers 503, its record and
/// its watch say so, and no chooser chooses it; a drain cannot be
/// cancelled. A cell whose class no hop enters cannot be drained. A hub
/// that follows the cell names its watch in the query, its own cell and
/// the follow (`?class=<class>&cell=<cell>&run=<run>&generation=<generation>`,
/// [`gdl_watch::Follower`]), and the watch counts among the cell's
/// followers ([`CellReadiness::followers`]) until the follower releases it
/// (`POST release` on the public listener, with the same
/// [`gdl_watch::Follower`]), which it does once none of its choosers holds
/// an offer to the cell in that follow. A watch that closes without a
/// release still counts; once the cell's lease has passed since it closed,
/// looked at once a summary period, it no longer shows as a follower, but
/// it is never taken as a release. Once the cell drains and every watch
/// that counted has been released, or none did, its watch says so, once
/// (its [`gdl_watch::CellWatchEvent::State`] again, with `unfollowed`),
/// and its entry instances
/// stop taking work from other cells then rather than at their bound.
///
/// An entry instance's retirement waits for the cells that follow the cell
/// when it retires, in the same states: each follower's hub says, once
/// every chooser of its cell avoids the instance, that its whole cell does
/// (`POST avoided` on the public listener, with the [`gdl_watch::Avoided`]
/// that names its watch and the instance), and a release counts too, since
/// a follower that let go makes no new offer. A watch lost without a
/// release never counts; one that opens after the retirement is not waited
/// for, since its snapshot shows the instance retired. Once none is
/// waited for, the watch sends the instance's `retired` again with
/// `acknowledged`, once, as the first one does when no cell follows; the
/// retiree's stop waits for it, up to a bound of its own. Like a release,
/// `avoided` is not authenticated: anything that can reach the cell's hub
/// can end a retiree's wait early.
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
    /// The places of the watches each watch route serves.
    places: timing::RoutePlaces,
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
    /// The state and presence of the cell's record last stored in the
    /// directory.
    stored: watch::Sender<Option<publish::Stored>>,
    /// Whether the cell serves, or was scaled to 0 on purpose, as the hub
    /// was last told.
    presence: watch::Sender<publish::Asked>,
    /// A number drawn as the hub starts, which the watches it follows
    /// cells by carry, so that a cell's hub tells them from those of an
    /// earlier run of this hub.
    run: u64,
    /// The follows the relay closed, for their cells' hubs to be told,
    /// until the hub starts and takes them.
    released: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<crate::follow::Release>>>,
    /// Under the `relay` backend, the gate by which the cell's children
    /// report to it.
    reports: Option<Arc<Reports>>,
    /// Under the `relay` backend, the cadence of the parent's answers to
    /// the cell's reports, which the record is written at.
    cadence: Option<watch::Receiver<Option<Duration>>>,
    /// The class that sends to the cell's class, whose cell is its parent.
    sender: Option<String>,
    /// Under the `relay` backend, the directory that reads the cell's last
    /// record from its parent's hub, which alone holds it.
    parent: Option<HubDirectory>,
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
        cell.check_hub_given_directory(&layout)
            .map_err(|e| CellError::new(e.to_string()))?;
        let hub = cell.hub.clone().ok_or_else(|| {
            CellError::new(
                "[cell] hub: a cell's hub publishes the address other cells' hubs reach it at, and none is set",
            )
        })?;
        let at = |key: &str, why: String| {
            CellError::new(
                gdl_model::Problem::at(Some("cell".into()), Some(key.into()), why).to_string(),
            )
        };
        gdl_model::check_hub_address(&hub).map_err(|why| at("hub", why))?;
        gdl_model::check_summary(cell.summary).map_err(|why| at("summary_ms", why))?;
        if timing.heartbeat.is_zero() {
            return Err(CellError::new("the cell's heartbeat is zero"));
        }
        if timing.watch_buffer == 0 {
            return Err(CellError::new("the cell's watch buffer is zero"));
        }
        if timing::has_zero(&timing.watch_caps) {
            return Err(CellError::new("a watch route's cap is zero"));
        }
        if timing.refollow.is_zero() {
            // A hub that refuses at once would be dialled in a loop.
            return Err(CellError::new("the cell's refollow delay is zero"));
        }
        if timing.lost_chooser.is_zero() {
            return Err(at("lost_chooser_ms", "is 0; it is at least 1".into()));
        }
        // A cell of a class that nothing sends to has no parent to name.
        let sender = layout
            .boundaries()
            .iter()
            .find(|boundary| boundary.into == cell.class)
            .map(|boundary| boundary.from.clone());
        let entered = sender.is_some();
        let record = CellRecord {
            presence: gdl_cells::Presence::Serving,
            models: Vec::new(),
            models_version: 0,
            revoked: Vec::new(),
            name: cell.name.clone(),
            class: cell.class.clone(),
            region: cell.region.clone(),
            home: cell.home.clone().filter(|_| entered),
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
            timing.lease,
            cell.models.clone(),
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
            &cell.requirements(&layout),
        ));
        let (releases, released) = tokio::sync::mpsc::unbounded_channel();
        let relay = Arc::new(Relay::new(
            timing.watch_buffer,
            health.clone(),
            choosers(&layout, &cell.class),
            releases,
            timing.lost_chooser,
        ));
        // Known before any watch of the relay can begin, so that one begun
        // as the hub starts waits for the directory's first snapshots.
        relay.expect(sends.iter().cloned());
        Ok(Self {
            directory: Arc::new(directory),
            record,
            quorums,
            timing,
            health,
            feed: Arc::new(feed),
            places: timing::RoutePlaces::new(&timing.watch_caps),
            admission: OnceLock::new(),
            sends: sends.into_iter().collect(),
            summary: (!entries.is_empty()).then_some(cell.summary),
            relay,
            watcher: OnceLock::new(),
            drain: Arc::new(Drain::new(class_segments)),
            stored: watch::Sender::new(None),
            presence: watch::Sender::new(publish::Asked {
                presence: Presence::Serving,
                request: 0,
            }),
            run: velo_hub::velo::InstanceId::new_v4().as_u128() as u64,
            released: Mutex::new(Some(released)),
            reports: None,
            cadence: None,
            sender,
            parent: None,
            tasks: Mutex::default(),
        })
    }

    /// The extension under the `relay` backend, whose `directory` is the
    /// one it was made with or one in front of it: its hub takes its
    /// children's reports into `directory`
    /// (`POST /v1/ext/gdl-cell/report` on the public listener, a
    /// [`gdl_watch::Report`]), whether or not the cell is healthy, since
    /// its health waits for its children; and it writes its record at the
    /// cadence its parent answers with, from the first answer on, rather
    /// than at its own heartbeat. It answers the record it holds for a
    /// child (`GET /v1/ext/gdl-cell/report/{class}/{cell}`), and reads its
    /// own cell's last record from its parent the same way as it starts.
    /// Its children's reports carry no credential: the route trusts the
    /// private network, as registration and velo's traffic do, and writes
    /// only what it verifies ([`crate::VerifiedReport`]).
    ///
    /// Refuses a lease shorter than [`gdl_model::HubTiming::MIN_RELAY_LEASE`]:
    /// the parent asks for a report no more often than every
    /// [`gdl_model::HubTiming::MIN_RELAY_CADENCE`], so a shorter lease
    /// lapses at the parent between reports. Refuses a `directory` with no
    /// parent's hub for a cell of a class that another sends to, which
    /// would have nowhere to report its record, and one with no state
    /// directory for a cell of a class that sends to another, which would
    /// lose its children's records when it restarts.
    pub fn with_reports(mut self, directory: &HubDirectory) -> Result<Self, CellError> {
        if let (Some(sender), false) = (&self.sender, directory.reports_up()) {
            return Err(CellError::new(
                gdl_model::missing_parent_hub(sender, &self.record.class).to_string(),
            ));
        }
        if let (Some(child), None) = (self.sends.first(), directory.children().root()) {
            return Err(CellError::new(
                gdl_model::missing_state_dir(&self.record.class, child).to_string(),
            ));
        }
        gdl_model::check_relay_lease(self.timing.lease).map_err(|why| {
            CellError::new(
                gdl_model::Problem::at(Some("cell".into()), Some("lease_ms".into()), why)
                    .to_string(),
            )
        })?;
        self.reports = Some(Arc::new(Reports::new(
            directory.children().clone(),
            &self.record,
            self.timing.heartbeat,
        )));
        self.cadence = Some(directory.cadence());
        self.parent = Some(directory.clone());
        Ok(self)
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
    /// Not capped, as the route is ([`gdl_model::WatchCaps`]).
    pub async fn watch(&self) -> Result<CellWatch, CellWatchError> {
        self.feed.watch().await
    }

    /// How many watches of the cell are open or waiting to begin, as it
    /// changes.
    pub fn watchers(&self) -> watch::Receiver<usize> {
        self.feed.watchers()
    }

    /// The cells that follow this one, by class and name, as their open
    /// watches name them, as it changes.
    pub fn followers(&self) -> watch::Receiver<BTreeSet<(String, String)>> {
        self.feed.followers()
    }

    /// A watch of what the hub follows, in process: what `GET follows`
    /// serves, as typed events. It ends at once if the hub has stopped.
    /// Not capped, as the route is ([`gdl_model::WatchCaps`]).
    pub fn follows(&self) -> FollowsWatch {
        self.relay.watch(None)
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
                .wait_for(|stored| {
                    stored
                        .as_ref()
                        .is_some_and(|stored| stored.state == CellState::Draining)
                })
                .await;
        })
        .await;
        let health = shown.borrow().clone();
        Ok(health)
    }

    /// Say whether the cell serves, or is scaled to 0 on purpose, as
    /// `POST presence` on the control listener does, and return once the
    /// directory has stored a record that says so. A cell is told
    /// `scaled_to_zero` before its instances stop, so that its readers
    /// never hold it missing, and `serving` as it scales up again. A hub
    /// starts `serving`. Refused when the directory has not stored such a
    /// record within 5 s, or the hub has stopped: the presence holds all
    /// the same, and its next write carries it.
    pub async fn set_presence(&self, presence: Presence) -> Result<(), CellError> {
        let mut request = 0;
        self.presence.send_modify(|asked| {
            asked.presence = presence;
            asked.request += 1;
            request = asked.request;
        });
        // Only a record stored with this request or a later one answers
        // it: one stored before it may say the same, while a write of
        // another presence has landed and waits for its answer.
        let mut stored = self.stored.subscribe();
        let wait = stored.wait_for(|stored| {
            stored
                .as_ref()
                .is_some_and(|stored| stored.request >= request)
        });
        match tokio::time::timeout(SETTLE, wait).await {
            Ok(Ok(stored))
                if stored
                    .as_ref()
                    .is_some_and(|stored| stored.presence == presence) =>
            {
                Ok(())
            }
            Ok(Ok(_)) => Err(CellError::new(format!(
                "a later request replaced presence {presence:?} before it was stored"
            ))),
            _ => Err(CellError::new(format!(
                "the directory has not stored the cell's record with presence {presence:?} in time"
            ))),
        }
    }

    /// Revoke the cell's model `key`, or lift its revocation when
    /// `revoked` is false, as `POST models/revoke` and `POST models/lift`
    /// on the control listener do, and return once the directory has
    /// stored a record that says so. A revoked model leaves the record's
    /// models at once, whatever the instances say, until it is lifted; the
    /// record keeps the revocation, so a restarted hub keeps it too. A
    /// lifted model is listed again as the instances say, starting afresh.
    /// Refused for a malformed model, and when the directory has not stored
    /// such a record within 5 s, or a later request replaced it, or the hub
    /// has stopped: the revocation holds all the same, and the next write
    /// carries it.
    pub async fn revoke(&self, key: gdl_model::ModelKey, revoked: bool) -> Result<(), CellError> {
        key.check().map_err(CellError::new)?;
        let mut stored = self.stored.subscribe();
        // Taken at once: before the cell's last record is read, the table
        // keeps it to apply over the record's own revocations.
        let version = self.feed.revoke(key.clone(), revoked);
        let answered = tokio::time::timeout(SETTLE, async {
            stored
                .wait_for(|stored| {
                    stored
                        .as_ref()
                        .is_some_and(|stored| stored.models.version >= version)
                })
                .await
                .map(|stored| {
                    stored
                        .as_ref()
                        .is_some_and(|stored| stored.models.revoked.contains(&key) == revoked)
                })
                .map_err(drop)
        })
        .await;
        let doing = if revoked { "revoking" } else { "lifting" };
        match answered {
            Ok(Ok(true)) => Ok(()),
            Ok(Ok(false)) => Err(CellError::new(format!(
                "a later request replaced {doing} model `{}` before it was stored",
                key.model
            ))),
            _ => Err(CellError::new(format!(
                "the directory has not stored the cell's record {doing} model `{}` in time",
                key.model
            ))),
        }
    }

    /// Give an operator's `command` for the child `cell` of `class` whose
    /// hub is gone, as `POST children/{class}/{cell}/withdraw` and
    /// `POST children/{class}/{cell}/scale-to-zero` on the control listener
    /// do: read the child's last record, and write the command at an epoch
    /// past it ([`gdl_cells::CellCommand`]). The version written, or
    /// `None` when the hub's directory holds no record of a child of this
    /// cell by that name, of a class this cell sends to.
    ///
    /// Under the `relay` backend the hub holds its children's records
    /// itself, and writes the command into them as
    /// it writes a child's report; under any other backend it reads and
    /// writes the directory it watches. The directory refuses the write as
    /// it refuses any other, and an error is the directory's.
    pub async fn command_child(
        &self,
        class: &str,
        cell: &str,
        command: gdl_cells::CellCommand,
    ) -> Result<Option<gdl_cells::Version>, gdl_cells::DirectoryError> {
        if !self.sends.iter().any(|sent| sent == class) {
            return Ok(None);
        }
        let last = match &self.reports {
            Some(reports) => reports.held(class, cell),
            None => {
                let read = gdl_cells::last_record(&*self.directory, class, cell);
                tokio::time::timeout(SETTLE, read)
                    .await
                    .unwrap_or_else(|_| {
                        Err(gdl_cells::DirectoryError::Unavailable(format!(
                            "the cell's records were not read within {SETTLE:?}"
                        )))
                    })?
            }
        };
        let child = |last: &CellRecord| {
            last.home.as_deref() == Some(self.record.name.as_str())
                && (last.contract, last.placement) == (self.record.contract, self.record.placement)
        };
        let Some(last) = last.filter(child) else {
            return Ok(None);
        };
        let write = command.write(&last, std::time::SystemTime::now());
        let version = write.version();
        match (&self.reports, write) {
            (Some(reports), gdl_cells::CommandWrite::Record(record)) => {
                Arc::clone(reports)
                    .take(gdl_watch::Report::Record(*record))
                    .await?;
            }
            (Some(reports), gdl_cells::CommandWrite::Withdrawal(withdrawal)) => {
                Arc::clone(reports)
                    .take(gdl_watch::Report::Withdrawal(withdrawal))
                    .await?;
            }
            (None, write) => write.to(&*self.directory).await?,
        }
        Ok(Some(version))
    }

    /// How many watches of what the hub follows are open, as it changes.
    pub fn follows_watchers(&self) -> watch::Receiver<usize> {
        self.relay.watchers()
    }

    /// How many watches each watch route has refused, its cap being met.
    pub fn refused_watches(&self) -> RefusedWatches {
        self.places.refused()
    }

    /// How many events of a kind this build does not know the watches of
    /// the cells the hub follows have sent: each withheld its cell in the
    /// relay until the hub read the cell again.
    pub fn unknown_events(&self) -> u64 {
        self.watcher.get().map_or(0, CellWatcher::unknown_events)
    }

    /// The retirements of the followed cells' entry instances that the hub
    /// waits for its choosers to avoid before it tells each cell's hub, by
    /// the cell's class and name, the follow's generation, and the
    /// instance, as it changes.
    pub fn retirements(&self) -> watch::Receiver<HashSet<Retirement>> {
        self.relay.retirements()
    }

    /// The cells the hub has decided to follow, by class and name, each
    /// with the generation of its follow, as it changes: a cell is here
    /// from the moment its follow begins, before its snapshot is complete,
    /// and while its link is down and it waits to follow the cell again.
    pub fn following(&self) -> watch::Receiver<BTreeMap<(String, String), u64>> {
        self.relay.following()
    }

    /// Take an entry instance's load report, as `POST
    /// instances/{instance_id}/load` does. A report that carries a model the
    /// instance drains is answered only once a record that carries what it
    /// said is stored: the instance forgets the drain on the answer, and a
    /// hub that restarted before the write would read a record without it.
    /// Unanswered within half a summary period, so that the instance hears
    /// it before its own bound of a period, it is refused with 503, and the
    /// instance sends the drain again with its next report.
    async fn take_load(
        &self,
        authorized: Authorized,
        load: LoadReport,
    ) -> Result<StatusCode, HubError> {
        let mut stored = self.stored.subscribe();
        let taken = self
            .feed
            .report(
                authorized.owner,
                authorized.lease,
                load,
                std::time::Instant::now(),
            )
            // Not 401 or 404: velo-hub's client drops its registration on
            // those.
            .map_err(|refused| HubError::new(ErrorCode::Conflict, refused))?;
        let feed::Taken::Draining { version, load } = taken else {
            return Ok(StatusCode::NO_CONTENT);
        };
        let bound = self.summary.map_or(SETTLE, |period| period / 2);
        let wait = stored.wait_for(|stored| {
            stored
                .as_ref()
                .is_some_and(|stored| stored.models.version >= version)
        });
        match tokio::time::timeout(bound, wait).await {
            Ok(Ok(_)) => {
                let now = std::time::Instant::now();
                self.feed
                    .count(authorized.owner, authorized.lease, load, now);
                Ok(StatusCode::NO_CONTENT)
            }
            _ => Err(HubError::new(
                ErrorCode::Unavailable,
                "the cell's record that carries this drain is not stored yet",
            )),
        }
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
                cadence: self.cadence.clone(),
                feed: Arc::clone(&self.feed),
                drain: Arc::clone(&self.drain),
                stored: self.stored.clone(),
                parent: self.parent.clone(),
            }
            .run(
                publish::Sources {
                    states: written,
                    presence: self.presence.subscribe(),
                    loads: summaries,
                    models: self.feed.models(),
                },
                cancel.clone(),
            ),
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
                None => CellWatcher::with_limits(self.timing.watch)
                    .map(|watcher| self.watcher.get_or_init(|| watcher).clone()),
            }
            .map(|watcher| {
                let me = gdl_watch::Follower {
                    class: self.record.class.clone(),
                    cell: self.record.name.clone(),
                    run: self.run,
                    generation: 0,
                };
                let released = self
                    .released
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                if let Some(released) = released {
                    tasks.push(tokio::spawn(crate::follow::release(
                        released,
                        watcher.clone(),
                        me.clone(),
                        self.timing.lease,
                        ctx.cancellation().clone(),
                    )));
                }
                if !self.sends.is_empty() {
                    tasks.push(tokio::spawn(crate::follow::expire(
                        Arc::clone(&self.relay),
                        ctx.cancellation().clone(),
                    )));
                }
                for class in &self.sends {
                    let follower = Follower {
                        directory: Arc::clone(&self.directory),
                        relay: Arc::clone(&self.relay),
                        watcher: watcher.clone(),
                        refollow: self.timing.refollow,
                        me: me.clone(),
                        class: class.clone(),
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
        let releasing = Arc::clone(&self.feed);
        let avoiding = Arc::clone(&self.feed);
        let relay = Arc::clone(&self.relay);
        let watching = Arc::clone(&self.places.cell);
        let relaying = Arc::clone(&self.places.relay);
        let following = Arc::clone(&self.places.follow);
        let reports = self.reports.clone().map_or_else(Router::new, |reports| {
            let holding = Arc::clone(&reports);
            Router::new()
                .route(
                    gdl_watch::HELD_ROUTE,
                    get(
                        move |Path((class, cell)): Path<(String, String)>| async move {
                            match holding.held(&class, &cell) {
                                Some(record) => Json(record).into_response(),
                                None => StatusCode::NOT_FOUND.into_response(),
                            }
                        },
                    ),
                )
                .route(
                    gdl_watch::REPORT_ROUTE,
                    post(move |Json(report): Json<gdl_watch::Report>| async move {
                        match Arc::clone(&reports).take(report).await {
                            Ok(reply) => Json(reply).into_response(),
                            Err(refused) => {
                                let status = match refused {
                                    gdl_cells::DirectoryError::Malformed(_) => {
                                        StatusCode::BAD_REQUEST
                                    }
                                    _ => StatusCode::CONFLICT,
                                };
                                let refusal = gdl_watch::ReportRefusal::of(refused);
                                (status, Json(refusal)).into_response()
                            }
                        }
                    }),
                )
        });
        Router::new()
            .merge(reports)
            .route(
                gdl_watch::AVOIDED_ROUTE,
                post(move |Json(avoided): Json<gdl_watch::Avoided>| async move {
                    let gdl_watch::Avoided { follower, instance } = avoided;
                    let gdl_watch::Follower {
                        class,
                        cell,
                        run,
                        generation,
                    } = follower;
                    avoiding.avoided(&((class, cell), run, generation), instance);
                    StatusCode::NO_CONTENT
                }),
            )
            .route(
                gdl_watch::RELEASE_ROUTE,
                post(move |Json(follower): Json<gdl_watch::Follower>| async move {
                    let gdl_watch::Follower {
                        class,
                        cell,
                        run,
                        generation,
                    } = follower;
                    releasing.release(&((class, cell), run, generation));
                    StatusCode::NO_CONTENT
                }),
            )
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
                get(move |Query(query): Query<WatchQuery>| {
                    let (feed, place) = (Arc::clone(&feed), watching.take());
                    async move {
                        let place = match place {
                            Ok(place) => place,
                            Err(full) => return full.into_response(),
                        };
                        let follower = (query.class, query.cell, query.run, query.generation);
                        let watch = match follower {
                            (Some(class), Some(cell), Some(run), Some(generation)) => {
                                let key = ((class, cell), run, generation);
                                tokio::time::timeout(SETTLE, feed.follow(key)).await
                            }
                            _ => tokio::time::timeout(SETTLE, feed.watch()).await,
                        };
                        let watch = watch
                            .unwrap_or_else(|_| {
                                Err(CellWatchError::new(
                                    "the cell has not published its record yet, so its epoch is not settled",
                                ))
                            });
                        serve(watch, place)
                    }
                }),
            )
            .route(
                gdl_watch::FOLLOWS_ROUTE,
                get({
                    let relay = Arc::clone(&relay);
                    move |Query(query): Query<RelayQuery>| {
                        let watch = relaying
                            .take()
                            .map(|place| (relay.watch(query.instance), place));
                        async move { serve_follows(watch) }
                    }
                }),
            )
            .route(
                gdl_watch::FOLLOWED_ROUTE,
                get(move |Path((class, cell)): Path<(String, String)>| {
                    let watch = (following.take()).map(|place| (relay.watch_of(&class, &cell), place));
                    async move { serve_follows(watch) }
                }),
            )
    }

    fn control_routes(self: Arc<Self>) -> Router {
        let draining = Arc::clone(&self);
        let scaling = Arc::clone(&self);
        let revoking = Arc::clone(&self);
        let lifting = Arc::clone(&self);
        let withdrawing = Arc::clone(&self);
        let zeroing = Arc::clone(&self);
        let relay = Arc::clone(&self.relay);
        let declared = Arc::clone(&self.relay);
        let avoiding = Arc::clone(&self.relay);
        Router::new()
            .route(
                gdl_watch::AVOIDS_ROUTE,
                post(
                    move |authorized: Authorized, HubJson(avoids): HubJson<gdl_watch::Avoids>| async move {
                        avoiding
                            .avoids(authorized.owner, authorized.lease.epoch, &avoids)
                            .map(|()| StatusCode::NO_CONTENT)
                            // Not 401 or 404: velo-hub's client drops its
                            // registration on those.
                            .map_err(|refused| HubError::new(ErrorCode::Conflict, refused))
                    },
                ),
            )
            .route(
                gdl_watch::CHOOSER_ROUTE,
                post(move |authorized: Authorized| async move {
                    declared
                        .declare(authorized.owner, authorized.lease.epoch)
                        .map(|()| StatusCode::NO_CONTENT)
                        .map_err(|refused| HubError::new(ErrorCode::Conflict, refused))
                }),
            )
            .route(
                gdl_watch::UNOFFERED_ROUTE,
                post(
                    move |authorized: Authorized, HubJson(unoffered): HubJson<Unoffered>| async move {
                        relay
                            .acknowledge(authorized.owner, authorized.lease.epoch, &unoffered)
                            .map(|()| StatusCode::NO_CONTENT)
                            // Not 401 or 404: velo-hub's client drops its
                            // registration on those.
                            .map_err(|refused| HubError::new(ErrorCode::Conflict, refused))
                    },
                ),
            )
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
                gdl_watch::PRESENCE_ROUTE,
                post(move |Json(change): Json<gdl_watch::SetPresence>| async move {
                    match scaling.set_presence(change.presence).await {
                        Ok(()) => StatusCode::NO_CONTENT.into_response(),
                        Err(error) => {
                            (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response()
                        }
                    }
                }),
            )
            .route(
                gdl_watch::CHILD_WITHDRAW_ROUTE,
                post(
                    move |Path((class, cell)): Path<(String, String)>| async move {
                        let command = gdl_cells::CellCommand::Withdraw;
                        command_child(&withdrawing, &class, &cell, command).await
                    },
                ),
            )
            .route(
                gdl_watch::CHILD_SCALE_TO_ZERO_ROUTE,
                post(
                    move |Path((class, cell)): Path<(String, String)>| async move {
                        let command = gdl_cells::CellCommand::ScaleToZero;
                        command_child(&zeroing, &class, &cell, command).await
                    },
                ),
            )
            .route(
                gdl_watch::REVOKE_ROUTE,
                post(move |Json(key): Json<gdl_model::ModelKey>| async move {
                    revoke(&revoking, key, true).await
                }),
            )
            .route(
                gdl_watch::LIFT_ROUTE,
                post(move |Json(key): Json<gdl_model::ModelKey>| async move {
                    revoke(&lifting, key, false).await
                }),
            )
            .route(
                gdl_watch::LOAD_ROUTE,
                post(
                    move |authorized: Authorized, HubJson(load): HubJson<LoadReport>| async move {
                        self.take_load(authorized, load).await
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
            self.relay.joined(
                &segment,
                committed.owner,
                committed.incarnation,
                committed.epoch,
            );
            self.feed
                .joined(&segment, committed.incarnation, committed.epoch, peer);
        }
        // A member now, if it is one of the class's, before it stops
        // holding the drain as a registration under way.
        drop(joining);
    }

    fn on_removed(&self, removal: Removal) {
        self.drain.removed(&removal);
        self.relay.removed(&removal);
        self.feed.removed(removal);
    }
}

/// The segments of `class` that choose cells of the classes it sends to,
/// each with those classes and the entry segments its hops enter: the
/// segments holding the outer end of a hop that crosses out of the class,
/// whose instances offer requests to the cells this hub follows.
fn choosers(layout: &gdl_model::Layout, class: &str) -> Choosers {
    type Chosen = (BTreeSet<String>, BTreeSet<String>);
    let mut segments: BTreeMap<String, Chosen> = BTreeMap::new();
    for boundary in layout.boundaries() {
        if boundary.from != class {
            continue;
        }
        let hop = &layout.hops()[boundary.hop];
        if let Some(outer) = hop.forward_runs.first() {
            let (classes, entries) = segments.entry(outer.segment.clone()).or_default();
            classes.insert(boundary.into.clone());
            entries.extend(hop.backward_runs.first().map(|inner| inner.segment.clone()));
        }
    }
    Choosers::new(segments.into_iter().map(|(segment, (classes, entries))| {
        (
            segment,
            classes.into_iter().collect(),
            entries.into_iter().collect(),
        )
    }))
}

/// The query of a watch: the follower's watch it names, if it names one
/// ([`gdl_watch::Follower`]); one that leaves out any of its parts names
/// none.
#[derive(Deserialize)]
struct WatchQuery {
    class: Option<String>,
    cell: Option<String>,
    run: Option<u64>,
    generation: Option<u64>,
}

/// The relay's query: the chooser that holds the watch, if it names one
/// ([`gdl_watch::RelayWatcher`]).
#[derive(Deserialize)]
struct RelayQuery {
    instance: Option<velo_hub::velo::InstanceId>,
}

/// Give `command` for the child `name` of `class` on `cell`'s hub, and
/// answer: a child the hub holds no record of is not found (404), a write
/// the directory refused conflicts (409), and a directory that cannot be
/// read or written is unavailable (503).
async fn command_child<D: CellDirectory>(
    cell: &CellReadiness<D>,
    class: &str,
    name: &str,
    command: gdl_cells::CellCommand,
) -> Response {
    match cell.command_child(class, name, command).await {
        Ok(Some(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("this cell holds no record of a child cell `{name}` of class `{class}`"),
        )
            .into_response(),
        Err(error @ gdl_cells::DirectoryError::Unavailable(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response()
        }
        Err(error) => (StatusCode::CONFLICT, error.to_string()).into_response(),
    }
}

/// Revoke `key` on `cell`, or lift its revocation, and answer: a
/// malformed model is the caller's (400), and anything else is the
/// directory's (503).
async fn revoke<D: CellDirectory>(
    cell: &CellReadiness<D>,
    key: gdl_model::ModelKey,
    revoked: bool,
) -> Response {
    if let Err(why) = key.check() {
        return (StatusCode::BAD_REQUEST, why).into_response();
    }
    match cell.revoke(key, revoked).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response(),
    }
}

/// The watch as server-sent events, each one JSON value, in `place`; a hub
/// that has not stored its record in time, or stops first, answers 503.
fn serve(watch: Result<CellWatch, CellWatchError>, place: crate::fanout::Place) -> Response {
    match watch {
        Ok(watch) => crate::fanout::serve(Box::pin(watch), place),
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response(),
    }
}

/// A watch of what the hub follows as server-sent events, in its place, or
/// its refusal.
fn serve_follows(
    watch: Result<(FollowsWatch, crate::fanout::Place), crate::fanout::Full>,
) -> Response {
    match watch {
        Ok((watch, place)) => crate::fanout::serve(watch.into_events(), place),
        Err(full) => full.into_response(),
    }
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
