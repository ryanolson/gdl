use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use gdl_model::{DeploymentSpec, PoolSettings};
use gdl_pools::{LoadSummary, PoolDirectory, PoolRecord, PoolState, Presence};
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
use gdl_watch::{LoadReport, PoolWatchError, PoolWatcher, Unoffered};

/// How long a watch that arrives before the pool's record is first stored
/// waits for it: a follower finds the pool by its record, so it can ask
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

pub use feed::PoolWatch;
pub use health::{PoolHealth, RequiredHealth, SegmentHealth};
pub use timing::{PoolTiming, RefusedWatches};

/// Whether a pool can take work, judged and published by the pool's own
/// hub: the `gdl-pool` extension, beside [`crate::GraphAdmission`].
///
/// It counts, per segment of the pool's class, the instances usable for
/// fresh work: those in the segment's `ready` service and not in its
/// `retired` one. Never `serving`, which an instance announces before the
/// routing barrier and keeps while it drains. An instance that leaves the
/// hub leaves both services. The counts are read from the hub's own
/// service watches, over loopback, since velo-hub keeps its service
/// directory to itself; one watch per service, whatever the number of
/// instances.
///
/// The state follows [`Quorum`], and the pool's record carries it, with
/// the pool's parent (`[pool] home`) unless no class sends to the pool's
/// class. Its routes, under `/v1/ext/gdl-pool/` on
/// the public listener: `GET health`, the pool health endpoint, which
/// answers 200 when the pool is `ready` and, for each class its settings
/// require pools of, at least that many followed pools of the class have
/// a complete snapshot and last said they are `ready`, and 503 otherwise,
/// with a [`PoolHealth`] body; and `GET watch`, the pool's watch for the hubs that
/// follow it, and for the pool's own instances, which learn of its drain
/// from it: server-sent events, each one [`gdl_watch::PoolWatchEvent`] as
/// JSON, with a keep-alive every 10 s. The watch carries the pool's state
/// and epoch and its entry instances, ready and retired, with their
/// `PeerInfo` from [`GraphAdmission`]'s members, so the hub must run
/// admission too ([`Extension::requires`]). Who an entry instance is, and when it
/// leaves, comes from the hub's registration hooks; when it is ready or
/// retires, from the same loopback watches as the counts. Each watcher
/// holds only its place in a buffer of [`PoolTiming::watch_buffer`] events
/// shared by all: one that falls further behind has its watch ended, and
/// watches again from a snapshot, so a slow watcher never slows the
/// counting, the judgment, the record, or another watcher. A watch begins
/// once the record is first stored, since its epoch is settled only then:
/// one that arrives earlier waits up to 5 s, then is refused (503). Every
/// watch ends when the hub stops, or once a drained pool's record is
/// withdrawn. Each watch route, `GET watch` and the relay's two below,
/// serves at most its cap of watches at once ([`PoolTiming::watch_caps`]):
/// one beyond it, a waiting one included, is refused with 503 and a
/// `Retry-After`, before a snapshot is built or a subscription taken for
/// it, and a watch that closes frees its place at once. The watches taken
/// in process are not capped.
///
/// A pool whose class sends to other classes follows their pools: for
/// each class it sends to, it watches `directory`, and follows each child
/// of its pool in the class (a pool whose record names this pool as its
/// parent, `home`, has this deployment's contract and placement hashes,
/// is `ready` or `degraded`, and is not scaled to 0, whatever its region)
/// over the pool's
/// watch, one link per pool. It
/// relays what it follows on `GET follows`, server-sent events, each one
/// [`gdl_watch::FollowEvent`] as JSON, with the same keep-alive and buffer as
/// the pool's watch: each followed pool's state, then its retired entry
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
/// none, as an instance without `[pool]`, never offers and never
/// acknowledges; and it acknowledges with `POST instances/{instance_id}/unoffered` on
/// the control listener, a [`gdl_watch::Unoffered`] with the chooser's own
/// credential, refused with 409 from any other instance. A chooser names
/// itself in its watch of the relay (`GET follows?instance=<id>`,
/// [`gdl_watch::RelayWatcher`]): one that has had no such watch open for
/// [`PoolTiming::lost_chooser`], from when its last one closed or from when
/// it declared, is not waited for until it watches again, since it cannot
/// learn what to acknowledge. Then the relay drops it and its link ends. A retirement of a followed pool's entry
/// instance waits, in the same way, for the choosers whose hops enter the
/// instance's segment, as they are when the relay applies it: each says
/// that it avoids the instance once its tables hold it retired
/// (`POST instances/{instance_id}/avoids` on the control listener, a
/// [`gdl_watch::Avoids`] with its own credential, refused with 409 from any
/// other instance), and one that leaves counts as one that did. A chooser
/// that says it runs a follower later counts as one that did, since the
/// relay's snapshot shows it the instance retired; so does a retirement in
/// the snapshot a follow is installed from, which the pool's hub does not
/// wait for this follow for. Once every chooser it waited for avoids it,
/// the hub tells the pool's hub, once for its whole pool
/// ([`gdl_watch::Avoided`]); a follow that closes first is released
/// instead. A follow whose link ends, or that a newer
/// epoch in the directory replaces, is not offered from then, and is kept
/// beside the pool's later follows until it closes the same way; the pool
/// is followed again, as a new generation, while it is still a child that
/// takes work, and, after a follow closed, only from a later write
/// of its record. A record that expires or is withdrawn ends no follow.
///
/// Each usable entry instance of the pool reports its admission to the
/// hub once a load summary period (`[pool] summary_ms`), on the control
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
/// older than three periods, into the pool's [`LoadSummary`]: the roots
/// admitted and the admission limits summed, the usable entry instances
/// counted, and the refusals of each report's period summed; `None` while
/// no report is counted, and always for a class that no hop enters. The
/// watch sends it as a `load` event every period while there is one,
/// changed or not, so that a watcher's age of it says how fresh it is,
/// and once when there is none; the record carries it on its next write.
/// A report may carry the instance's models
/// ([`gdl_watch::LoadReport::models`]): from what its usable entry
/// instances last said, or the models its settings declare
/// (`[pool] models`) for one that says none, the hub makes the record's
/// model entries, each with a state that keeps its history, and writes the
/// record on each change of them.
///
/// The record says whether the pool serves or is scaled to 0 on purpose
/// (`POST presence` on the control listener, a
/// [`gdl_watch::SetPresence`], or [`PoolReadiness::set_presence`] in
/// process), answered once the directory has stored a record that says
/// so. Before its first write, the hub reads the pool's last record, from
/// its parent's hub under the `relay` backend, so that a restart keeps the
/// models' history.
///
/// The pool drains when asked, on the control listener: `POST drain`, or
/// [`PoolReadiness::drain`] in process. From then its state is `draining`
/// whatever its quorum, so its health endpoint answers 503, its record and
/// its watch say so, and no chooser chooses it; a drain cannot be
/// cancelled. A pool whose class no hop enters cannot be drained. A hub
/// that follows the pool names its watch in the query, its own pool and
/// the follow (`?class=<class>&pool=<pool>&run=<run>&generation=<generation>`,
/// [`gdl_watch::Follower`]), and the watch counts among the pool's
/// followers ([`PoolReadiness::followers`]) until the follower releases it
/// (`POST release` on the public listener, with the same
/// [`gdl_watch::Follower`]), which it does once none of its choosers holds
/// an offer to the pool in that follow. A watch that closes without a
/// release still counts; once the pool's lease has passed since it closed,
/// looked at once a summary period, it no longer shows as a follower, but
/// it is never taken as a release. Once the pool drains and every watch
/// that counted has been released, or none did, its watch says so, once
/// (its [`gdl_watch::PoolWatchEvent::State`] again, with `unfollowed`),
/// and its entry instances
/// stop taking work from other pools then rather than at their bound.
///
/// An entry instance's retirement waits for the pools that follow the pool
/// when it retires, in the same states: each follower's hub says, once
/// every chooser of its pool avoids the instance, that its whole pool does
/// (`POST avoided` on the public listener, with the [`gdl_watch::Avoided`]
/// that names its watch and the instance), and a release counts too, since
/// a follower that let go makes no new offer. A watch lost without a
/// release never counts; one that opens after the retirement is not waited
/// for, since its snapshot shows the instance retired. Once none is
/// waited for, the watch sends the instance's `retired` again with
/// `acknowledged`, once, as the first one does when no pool follows; the
/// retiree's stop waits for it, up to a bound of its own. Like a release,
/// `avoided` is not authenticated: anything that can reach the pool's hub
/// can end a retiree's wait early.
///
/// It publishes the pool's [`PoolRecord`] through `directory` at once, on
/// each change of state, and on every heartbeat, from a task of its own,
/// so that a slow directory never delays the judgment or the health
/// endpoint; a state that a later one replaces while a write is in flight
/// is not written, since a reader keeps only the newest. The record's
/// epoch is fixed for the life of the hub process and later than any the
/// directory held for the pool when it started, and its `seq` rises with
/// every write. Once a drain has started and no instance of the pool's
/// class is a member of the hub, it withdraws the record, after a last
/// write that says the pool is `draining`, and writes it no more; then it
/// ends the pool's watches and refuses new ones, so that the hubs that
/// follow the pool drop it. A drained pool's hub serves the pool no more,
/// and refuses every registration from the moment the drain ends (409),
/// since an instance that joined would never learn of the drain: it is
/// started again to serve the pool. It does not withdraw the record when
/// the hub stops: a pool that stops without draining leaves its record to
/// expire on its readers' clocks.
pub struct PoolReadiness<D: PoolDirectory> {
    directory: Arc<D>,
    /// The record as published, but for its state and version.
    record: PoolRecord,
    /// The class's segments, with the pool's quorum of each.
    quorums: Vec<(String, u32)>,
    timing: PoolTiming,
    health: watch::Sender<PoolHealth>,
    /// The pool's watchers.
    feed: Arc<Feed>,
    /// The places of the watches each watch route serves.
    places: timing::RoutePlaces,
    /// The hub's admission, whose members say who an entry instance is.
    admission: OnceLock<Arc<GraphAdmission>>,
    /// The classes the pool's class sends to, whose pools it follows.
    sends: Vec<String>,
    /// The load summary's period, when the class has an entry segment to
    /// summarize.
    summary: Option<Duration>,
    /// What the hub follows.
    relay: Arc<Relay>,
    /// The client of the followed pools' watches, once the hub follows
    /// any.
    watcher: OnceLock<PoolWatcher>,
    /// The pool's drain.
    drain: Arc<Drain>,
    /// The state and presence of the pool's record last stored in the
    /// directory.
    stored: watch::Sender<Option<publish::Stored>>,
    /// Whether the pool serves, or was scaled to 0 on purpose, as the hub
    /// was last told.
    presence: watch::Sender<publish::Asked>,
    /// A number drawn as the hub starts, which the watches it follows
    /// pools by carry, so that a pool's hub tells them from those of an
    /// earlier run of this hub.
    run: u64,
    /// The follows the relay closed, for their pools' hubs to be told,
    /// until the hub starts and takes them.
    released: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<crate::follow::Release>>>,
    /// Under the `relay` backend, the gate by which the pool's children
    /// report to it.
    reports: Option<Arc<Reports>>,
    /// Under the `relay` backend, the cadence of the parent's answers to
    /// the pool's reports, which the record is written at.
    cadence: Option<watch::Receiver<Option<Duration>>>,
    /// The class that sends to the pool's class, whose pool is its parent.
    sender: Option<String>,
    /// Under the `relay` backend, the directory that reads the pool's last
    /// record from its parent's hub, which alone holds it.
    parent: Option<HubDirectory>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<D: PoolDirectory> PoolReadiness<D> {
    /// The extension for the pool `pool` of the deployment `spec`,
    /// publishing through `directory`. Settings from which no well-formed
    /// record of this deployment's pool can be published are refused here:
    /// a class the placement does not have, a `ready` key outside it, a
    /// `requires` key that is not a class it sends to or a count of 0, no
    /// hub address, a malformed name or region, a lease that is not whole
    /// milliseconds, a summary period shorter than 1 s, a heartbeat of
    /// zero, a watch buffer of zero, or a refollow delay of zero.
    pub fn new(
        spec: &DeploymentSpec,
        pool: PoolSettings,
        directory: D,
        timing: PoolTiming,
    ) -> Result<Self, PoolError> {
        let layout = gdl_model::layout(&spec.graph, &spec.placement)
            .map_err(|e| PoolError::new(e.to_string()))?;
        pool.check_hub_given_directory(&layout)
            .map_err(|e| PoolError::new(e.to_string()))?;
        let hub = pool.hub.clone().ok_or_else(|| {
            PoolError::new(
                "[pool] hub: a pool's hub publishes the address other pools' hubs reach it at, and none is set",
            )
        })?;
        let at = |key: &str, why: String| {
            PoolError::new(
                gdl_model::Problem::at(Some("pool".into()), Some(key.into()), why).to_string(),
            )
        };
        gdl_model::check_hub_address(&hub).map_err(|why| at("hub", why))?;
        gdl_model::check_summary(pool.summary).map_err(|why| at("summary_ms", why))?;
        if timing.heartbeat.is_zero() {
            return Err(PoolError::new("the pool's heartbeat is zero"));
        }
        if timing.watch_buffer == 0 {
            return Err(PoolError::new("the pool's watch buffer is zero"));
        }
        if timing::has_zero(&timing.watch_caps) {
            return Err(PoolError::new("a watch route's cap is zero"));
        }
        if timing.refollow.is_zero() {
            // A hub that refuses at once would be dialled in a loop.
            return Err(PoolError::new("the pool's refollow delay is zero"));
        }
        if timing.lost_chooser.is_zero() {
            return Err(at("lost_chooser_ms", "is 0; it is at least 1".into()));
        }
        // A pool of a class that nothing sends to has no parent to name.
        let sender = layout
            .boundaries()
            .iter()
            .find(|boundary| boundary.into == pool.class)
            .map(|boundary| boundary.from.clone());
        let entered = sender.is_some();
        let record = PoolRecord {
            presence: gdl_pools::Presence::Serving,
            models: Vec::new(),
            models_version: 0,
            revoked: Vec::new(),
            name: pool.name.clone(),
            class: pool.class.clone(),
            region: pool.region.clone(),
            home: pool.home.clone().filter(|_| entered),
            contract: layout.contract_hash(),
            placement: layout.placement_hash(),
            state: PoolState::Unavailable,
            epoch: 0,
            seq: 0,
            hub,
            lease: timing.lease,
            load: None,
        };
        record.check().map_err(|e| PoolError::new(e.to_string()))?;
        let quorums: Vec<(String, u32)> = spec
            .placement
            .classes()
            .find(|(class, _)| *class == pool.class)
            .map(|(_, segments)| segments)
            .unwrap_or_default()
            .iter()
            .map(|segment| (segment.clone(), pool.quorum(segment)))
            .collect();
        let class_segments: Vec<String> =
            quorums.iter().map(|(segment, _)| segment.clone()).collect();
        let entries = layout.entry_segments(&pool.class);
        let segments = quorums
            .iter()
            .map(|(segment, _)| (segment.clone(), entries.contains(segment.as_str())))
            .collect();
        let feed = Feed::new(
            record.name.clone(),
            record.class.clone(),
            segments,
            pool.summary,
            timing.watch_buffer,
            timing.lease,
            pool.models.clone(),
        );
        let sends: BTreeSet<String> = layout
            .boundaries()
            .iter()
            .filter(|boundary| boundary.from == pool.class)
            .map(|boundary| boundary.into.clone())
            .collect();
        let health = watch::Sender::new(PoolHealth::of(
            &Quorum::new(quorums.iter().cloned(), timing.hold),
            PoolState::Unavailable,
            &pool.requirements(&layout),
        ));
        let (releases, released) = tokio::sync::mpsc::unbounded_channel();
        let relay = Arc::new(Relay::new(
            timing.watch_buffer,
            health.clone(),
            choosers(&layout, &pool.class),
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
            summary: (!entries.is_empty()).then_some(pool.summary),
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
    /// (`POST /v1/ext/gdl-pool/report` on the public listener, a
    /// [`gdl_watch::Report`]), whether or not the pool is healthy, since
    /// its health waits for its children; and it writes its record at the
    /// cadence its parent answers with, from the first answer on, rather
    /// than at its own heartbeat. It answers the record it holds for a
    /// child (`GET /v1/ext/gdl-pool/report/{class}/{pool}`), and reads its
    /// own pool's last record from its parent the same way as it starts.
    /// Its children's reports carry no credential: the route trusts the
    /// private network, as registration and velo's traffic do, and writes
    /// only what it verifies ([`crate::VerifiedReport`]).
    ///
    /// Refuses a lease shorter than [`gdl_model::HubTiming::MIN_RELAY_LEASE`]:
    /// the parent asks for a report no more often than every
    /// [`gdl_model::HubTiming::MIN_RELAY_CADENCE`], so a shorter lease
    /// lapses at the parent between reports. Refuses a `directory` with no
    /// parent's hub for a pool of a class that another sends to, which
    /// would have nowhere to report its record, and one with no state
    /// directory for a pool of a class that sends to another, which would
    /// lose its children's records when it restarts.
    pub fn with_reports(mut self, directory: &HubDirectory) -> Result<Self, PoolError> {
        if let (Some(sender), false) = (&self.sender, directory.reports_up()) {
            return Err(PoolError::new(
                gdl_model::missing_parent_hub(sender, &self.record.class).to_string(),
            ));
        }
        if let (Some(child), None) = (self.sends.first(), directory.children().root()) {
            return Err(PoolError::new(
                gdl_model::missing_state_dir(&self.record.class, child).to_string(),
            ));
        }
        gdl_model::check_relay_lease(self.timing.lease).map_err(|why| {
            PoolError::new(
                gdl_model::Problem::at(Some("pool".into()), Some("lease_ms".into()), why)
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

    /// The pool's health as it changes: its state, and each segment's
    /// usable instances and quorum.
    pub fn health(&self) -> watch::Receiver<PoolHealth> {
        self.health.subscribe()
    }

    /// A watch of the pool, in process: what `GET watch` serves, as typed
    /// events. It begins once the pool's record is stored, since its epoch
    /// is settled only then; refused if the hub stops first, and once the
    /// drained pool's record is withdrawn.
    /// Not capped, as the route is ([`gdl_model::WatchCaps`]).
    pub async fn watch(&self) -> Result<PoolWatch, PoolWatchError> {
        self.feed.watch().await
    }

    /// How many watches of the pool are open or waiting to begin, as it
    /// changes.
    pub fn watchers(&self) -> watch::Receiver<usize> {
        self.feed.watchers()
    }

    /// The pools that follow this one, by class and name, as their open
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

    /// Start the pool's drain, as `POST drain` on the control listener
    /// does, and return the pool's health once it shows the pool
    /// `draining` and the directory has stored a record that says so: by
    /// then the pool's watchers have been sent the state too. A directory
    /// that takes more than 5 s to store the record leaves the drain to be
    /// answered without it, since the drain has started either way. From
    /// then the pool is `draining`
    /// whatever its quorum, for the life of the hub process; a drain
    /// already started is answered the same way. Refused for a pool whose
    /// class no hop enters, which has no entry instances to stop taking
    /// work at.
    pub async fn drain(&self) -> Result<PoolHealth, PoolError> {
        if self.summary.is_none() {
            return Err(PoolError::new(
                "a pool whose class no hop enters cannot be drained",
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
                .wait_for(|health| health.state == PoolState::Draining)
                .await;
            let _ = stored
                .wait_for(|stored| {
                    stored
                        .as_ref()
                        .is_some_and(|stored| stored.state == PoolState::Draining)
                })
                .await;
        })
        .await;
        let health = shown.borrow().clone();
        Ok(health)
    }

    /// Say whether the pool serves, or is scaled to 0 on purpose, as
    /// `POST presence` on the control listener does, and return once the
    /// directory has stored a record that says so. A pool is told
    /// `scaled_to_zero` before its instances stop, so that its readers
    /// never hold it missing, and `serving` as it scales up again. A hub
    /// starts `serving`. Refused when the directory has not stored such a
    /// record within 5 s, or the hub has stopped: the presence holds all
    /// the same, and its next write carries it.
    pub async fn set_presence(&self, presence: Presence) -> Result<(), PoolError> {
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
            Ok(Ok(_)) => Err(PoolError::new(format!(
                "a later request replaced presence {presence:?} before it was stored"
            ))),
            _ => Err(PoolError::new(format!(
                "the directory has not stored the pool's record with presence {presence:?} in time"
            ))),
        }
    }

    /// Revoke the pool's model `key`, or lift its revocation when
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
    pub async fn revoke(&self, key: gdl_model::ModelKey, revoked: bool) -> Result<(), PoolError> {
        key.check().map_err(PoolError::new)?;
        let mut stored = self.stored.subscribe();
        // Taken at once: before the pool's last record is read, the table
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
            Ok(Ok(false)) => Err(PoolError::new(format!(
                "a later request replaced {doing} model `{}` before it was stored",
                key.model
            ))),
            _ => Err(PoolError::new(format!(
                "the directory has not stored the pool's record {doing} model `{}` in time",
                key.model
            ))),
        }
    }

    /// Give an operator's `command` for the child `pool` of `class` whose
    /// hub is gone, as `POST children/{class}/{pool}/withdraw` and
    /// `POST children/{class}/{pool}/scale-to-zero` on the control listener
    /// do: read the child's last record, and write the command at an epoch
    /// past it ([`gdl_pools::PoolCommand`]). The version written, or
    /// `None` when the hub's directory holds no record of a child of this
    /// pool by that name, of a class this pool sends to.
    ///
    /// Under the `relay` backend the hub holds its children's records
    /// itself, and writes the command into them as
    /// it writes a child's report; under any other backend it reads and
    /// writes the directory it watches. The directory refuses the write as
    /// it refuses any other, and an error is the directory's.
    pub async fn command_child(
        &self,
        class: &str,
        pool: &str,
        command: gdl_pools::PoolCommand,
    ) -> Result<Option<gdl_pools::Version>, gdl_pools::DirectoryError> {
        if !self.sends.iter().any(|sent| sent == class) {
            return Ok(None);
        }
        let last = match &self.reports {
            Some(reports) => reports.held(class, pool),
            None => {
                let read = gdl_pools::last_record(&*self.directory, class, pool);
                tokio::time::timeout(SETTLE, read)
                    .await
                    .unwrap_or_else(|_| {
                        Err(gdl_pools::DirectoryError::Unavailable(format!(
                            "the pool's records were not read within {SETTLE:?}"
                        )))
                    })?
            }
        };
        let child = |last: &PoolRecord| {
            last.home.as_deref() == Some(self.record.name.as_str())
                && (last.contract, last.placement) == (self.record.contract, self.record.placement)
        };
        let Some(last) = last.filter(child) else {
            return Ok(None);
        };
        let write = command.write(&last, std::time::SystemTime::now());
        let version = write.version();
        match (&self.reports, write) {
            (Some(reports), gdl_pools::CommandWrite::Record(record)) => {
                Arc::clone(reports)
                    .take(gdl_watch::Report::Record(*record))
                    .await?;
            }
            (Some(reports), gdl_pools::CommandWrite::Withdrawal(withdrawal)) => {
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
    /// the pools the hub follows have sent: each withheld its pool in the
    /// relay until the hub read the pool again.
    pub fn unknown_events(&self) -> u64 {
        self.watcher.get().map_or(0, PoolWatcher::unknown_events)
    }

    /// The retirements of the followed pools' entry instances that the hub
    /// waits for its choosers to avoid before it tells each pool's hub, by
    /// the pool's class and name, the follow's generation, and the
    /// instance, as it changes.
    pub fn retirements(&self) -> watch::Receiver<HashSet<Retirement>> {
        self.relay.retirements()
    }

    /// The pools the hub has decided to follow, by class and name, each
    /// with the generation of its follow, as it changes: a pool is here
    /// from the moment its follow begins, before its snapshot is complete,
    /// and while its link is down and it waits to follow the pool again.
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
                "the pool's record that carries this drain is not stored yet",
            )),
        }
    }
}

impl<D: PoolDirectory> Extension for PoolReadiness<D> {
    const NAME: &'static str = gdl_watch::EXTENSION;
    type Registration = ();
    /// A registration under way, which keeps the pool's drain from
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
        // counting task connects in the background and the pool is
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
        let (states, written) = watch::channel(PoolState::Unavailable);
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
                None => PoolWatcher::with_limits(self.timing.watch)
                    .map(|watcher| self.watcher.get_or_init(|| watcher).clone()),
            }
            .map(|watcher| {
                let me = gdl_watch::Follower {
                    class: self.record.class.clone(),
                    pool: self.record.name.clone(),
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
        let watching = Arc::clone(&self.places.pool);
        let relaying = Arc::clone(&self.places.relay);
        let following = Arc::clone(&self.places.follow);
        let reports = self.reports.clone().map_or_else(Router::new, |reports| {
            let holding = Arc::clone(&reports);
            Router::new()
                .route(
                    gdl_watch::HELD_ROUTE,
                    get(
                        move |Path((class, pool)): Path<(String, String)>| async move {
                            match holding.held(&class, &pool) {
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
                                    gdl_pools::DirectoryError::Malformed(_) => {
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
                        pool,
                        run,
                        generation,
                    } = follower;
                    avoiding.avoided(&((class, pool), run, generation), instance);
                    StatusCode::NO_CONTENT
                }),
            )
            .route(
                gdl_watch::RELEASE_ROUTE,
                post(move |Json(follower): Json<gdl_watch::Follower>| async move {
                    let gdl_watch::Follower {
                        class,
                        pool,
                        run,
                        generation,
                    } = follower;
                    releasing.release(&((class, pool), run, generation));
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
                        let follower = (query.class, query.pool, query.run, query.generation);
                        let watch = match follower {
                            (Some(class), Some(pool), Some(run), Some(generation)) => {
                                let key = ((class, pool), run, generation);
                                tokio::time::timeout(SETTLE, feed.follow(key)).await
                            }
                            _ => tokio::time::timeout(SETTLE, feed.watch()).await,
                        };
                        let watch = watch
                            .unwrap_or_else(|_| {
                                Err(PoolWatchError::new(
                                    "the pool has not published its record yet, so its epoch is not settled",
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
                get(move |Path((class, pool)): Path<(String, String)>| {
                    let watch = (following.take()).map(|place| (relay.watch_of(&class, &pool), place));
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
                    move |Path((class, pool)): Path<(String, String)>| async move {
                        let command = gdl_pools::PoolCommand::Withdraw;
                        command_child(&withdrawing, &class, &pool, command).await
                    },
                ),
            )
            .route(
                gdl_watch::CHILD_SCALE_TO_ZERO_ROUTE,
                post(
                    move |Path((class, pool)): Path<(String, String)>| async move {
                        let command = gdl_pools::PoolCommand::ScaleToZero;
                        command_child(&zeroing, &class, &pool, command).await
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
            "pool": self.record.name,
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

/// The segments of `class` that choose pools of the classes it sends to,
/// each with those classes and the entry segments its hops enter: the
/// segments holding the outer end of a hop that crosses out of the class,
/// whose instances offer requests to the pools this hub follows.
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
    pool: Option<String>,
    run: Option<u64>,
    generation: Option<u64>,
}

/// The relay's query: the chooser that holds the watch, if it names one
/// ([`gdl_watch::RelayWatcher`]).
#[derive(Deserialize)]
struct RelayQuery {
    instance: Option<velo_hub::velo::InstanceId>,
}

/// Give `command` for the child `name` of `class` on `pool`'s hub, and
/// answer: a child the hub holds no record of is not found (404), a write
/// the directory refused conflicts (409), and a directory that cannot be
/// read or written is unavailable (503).
async fn command_child<D: PoolDirectory>(
    pool: &PoolReadiness<D>,
    class: &str,
    name: &str,
    command: gdl_pools::PoolCommand,
) -> Response {
    match pool.command_child(class, name, command).await {
        Ok(Some(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("this pool holds no record of a child pool `{name}` of class `{class}`"),
        )
            .into_response(),
        Err(error @ gdl_pools::DirectoryError::Unavailable(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response()
        }
        Err(error) => (StatusCode::CONFLICT, error.to_string()).into_response(),
    }
}

/// Revoke `key` on `pool`, or lift its revocation, and answer: a
/// malformed model is the caller's (400), and anything else is the
/// directory's (503).
async fn revoke<D: PoolDirectory>(
    pool: &PoolReadiness<D>,
    key: gdl_model::ModelKey,
    revoked: bool,
) -> Response {
    if let Err(why) = key.check() {
        return (StatusCode::BAD_REQUEST, why).into_response();
    }
    match pool.revoke(key, revoked).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response(),
    }
}

/// The watch as server-sent events, each one JSON value, in `place`; a hub
/// that has not stored its record in time, or stops first, answers 503.
fn serve(watch: Result<PoolWatch, PoolWatchError>, place: crate::fanout::Place) -> Response {
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

/// Why a pool's hub cannot start.
#[derive(Clone, PartialEq, Eq)]
pub struct PoolError(String);

impl PoolError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// The message alone, as `Display` gives it: a binary that returns the
// error from `main` prints its `Debug`, which would otherwise quote and
// escape the message.
impl std::fmt::Debug for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PoolError {}
