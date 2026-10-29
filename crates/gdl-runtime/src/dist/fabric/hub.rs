use crate::Roster;
use crate::dist::Pools;
use crate::dist::roster::Live;
use futures::StreamExt;
use futures::stream::{BoxStream, SelectAll};
use gdl::{Error, ErrorKind, Result};
use gdl_model::{SegmentPhase, SegmentRegistration, segment_service};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use velo::discovery::{ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard};
use velo::{InstanceId, PeerInfo, Velo};
use velo_hub::client::{ClientError, HubClient, HubServiceRegistration};

/// A segment's hub registration, kept alive from the moment it is made,
/// and the phases it announces: `loaded`, once its nodes are ready to work;
/// `serving`, once its handlers are installed; `ready`, once it routes;
/// `drained`, once a source has finished its requests; `retired`, once it
/// has been stopped; and `detach`, once an instance of a draining pool will
/// send no more work into it. Peers take loaded instances into their rosters,
/// send only once those are serving, add a joiner when it is ready, and
/// stop routing to one that retired.
pub struct HubSession {
    client: HubClient,
    registration: SegmentRegistration,
    loaded: Option<HubServiceRegistration>,
    serving: Option<HubServiceRegistration>,
    ready: Option<HubServiceRegistration>,
    drained: Option<HubServiceRegistration>,
    detached: Option<HubServiceRegistration>,
    retired: Option<HubServiceRegistration>,
    lease: tokio::task::JoinHandle<()>,
    lost: watch::Receiver<Option<String>>,
    /// Follows membership into the roster, from just before `loaded`.
    follower: Option<tokio::task::JoinHandle<()>>,
}

impl HubSession {
    /// Register the compiled graph identity, and from then on keep the
    /// registration alive with a heartbeat every `heartbeat`: the hub
    /// knows the instance is live from here, whether or not its nodes are
    /// ready, and a node that takes longer to load than the hub's lease is
    /// not reaped meanwhile. The hub admits the instance only if
    /// `registration` names the deployment it serves.
    pub async fn register(
        host: &str,
        public_port: u16,
        control_port: u16,
        node: &Velo,
        registration: SegmentRegistration,
        heartbeat: Duration,
    ) -> Result<Self> {
        let client = HubClient::builder()
            .host(host)
            .public_port(public_port)
            .control_port(control_port)
            .build()
            .map_err(failure)?;
        let payload = serde_json::to_value(&registration).map_err(failure)?;
        client
            .register(node, BTreeMap::from([("gdl".to_string(), payload)]))
            .await
            .map_err(failure)?;
        let (report, lost) = watch::channel(None);
        let lease = tokio::spawn(keep_lease(client.clone(), heartbeat, report));
        Ok(Self {
            client,
            registration,
            loaded: None,
            serving: None,
            ready: None,
            drained: None,
            detached: None,
            retired: None,
            lease,
            lost,
            follower: None,
        })
    }

    /// Announce that this instance's nodes are ready, then wait until at
    /// least `min_replicas` loaded instances of each segment exist, and
    /// return them: the instances this one will route to. How many
    /// instances a segment has is not fixed, so more is never an error.
    /// Every instance announces before it waits, so none waits for another
    /// that is waiting for it. A hub call that fails is retried until
    /// `wait` runs out.
    pub async fn roster(
        &mut self,
        min_replicas: &[(&str, usize)],
        wait: Duration,
    ) -> Result<Roster> {
        let deadline = tokio::time::Instant::now() + wait;
        if self.loaded.is_none() {
            self.loaded = Some(self.announce(SegmentPhase::Loaded, deadline).await?);
        }
        let mut last = String::new();
        loop {
            self.lost()?;
            match tokio::time::timeout_at(deadline, self.loaded_members()).await {
                // A poll cut short by the deadline says nothing an earlier
                // answer did not; the shortfall it reported stands.
                Err(_) => {
                    if last.is_empty() {
                        last = "the hub did not answer in time".into();
                    }
                }
                Ok(Ok(roster)) => {
                    if min_replicas
                        .iter()
                        .all(|(segment, want)| roster.instances(segment).len() >= *want)
                    {
                        return Ok(roster);
                    }
                    let short: Vec<String> = min_replicas
                        .iter()
                        .filter(|(segment, want)| roster.instances(segment).len() < *want)
                        .map(|(segment, want)| {
                            format!("{segment} ({} of {want})", roster.instances(segment).len())
                        })
                        .collect();
                    last = format!("loaded instances missing at the hub: {}", short.join(", "));
                }
                Ok(Err(error)) => last = error.to_string(),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failure(last));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Announce that this instance's handlers are installed, then wait until
    /// every instance in `roster` of the segments this one sends to
    /// (`inner`) has announced the same. The announcement comes first: a
    /// forward path that leaves a segment and comes back makes two segments
    /// send to each other, and each would otherwise wait for the other. A
    /// request that reaches this instance before its wait is over waits at
    /// its held roster, so a peer's announcement is enough to route to it.
    /// The wait follows membership: an instance that left the hub will
    /// never announce, so it is dropped from the roster and from the wait,
    /// and the wait fails at once only when a segment this one sends to has
    /// no instance left.
    pub async fn ready(&mut self, roster: &Roster, inner: &[&str], wait: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + wait;
        if self.serving.is_none() {
            self.serving = Some(self.announce(SegmentPhase::Serving, deadline).await?);
        }
        let live = roster.live();
        let mut last = String::new();
        loop {
            self.lost()?;
            // Who to judge is fixed before the hub is asked: an instance the
            // follower adds meanwhile is missing from the answer, and must
            // not be taken for one that left.
            let candidates: Vec<(&str, Vec<InstanceId>)> = inner
                .iter()
                .map(|segment| (*segment, live.members(segment)))
                .collect();
            match tokio::time::timeout_at(deadline, self.serving_members(inner)).await {
                // A poll cut short by the deadline says nothing an earlier
                // answer did not; the shortfall it reported stands.
                Err(_) => {
                    if last.is_empty() {
                        last = "the hub did not answer in time".into();
                    }
                }
                Ok(Ok((members, serving))) => {
                    let mut missing = Vec::new();
                    for (segment, ids) in &candidates {
                        let segment = *segment;
                        let mut gone = Vec::new();
                        for id in ids.iter().copied() {
                            if !members.contains(&id) {
                                live.leave(segment, id);
                                gone.push(format!("{segment}/{id}"));
                            } else if !serving.contains(&id) {
                                missing.push(format!("{segment}/{id}"));
                            }
                        }
                        if live.members(segment).is_empty() {
                            return Err(failure(format!(
                                "segment `{segment}` has no instance left to route to: {} left the hub before serving",
                                if gone.is_empty() {
                                    "every instance".to_string()
                                } else {
                                    gone.join(", ")
                                }
                            )));
                        }
                    }
                    if missing.is_empty() {
                        return Ok(());
                    }
                    last = format!("instances not serving: {}", missing.join(", "));
                }
                Ok(Err(error)) => last = error.to_string(),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failure(last));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Announce that this instance routes: the instances already serving
    /// add it now. Retried until `deadline`.
    pub(crate) async fn announce_ready(&mut self, deadline: tokio::time::Instant) -> Result<()> {
        if self.ready.is_none() {
            self.ready = Some(self.persist(SegmentPhase::Ready, deadline).await?);
        }
        Ok(())
    }

    /// Announce that this instance retired, then withdraw `ready` and
    /// `loaded`, each answered by the hub before the next: a
    /// roster read afterwards leaves it out, and a peer that follows
    /// membership afterwards never adds it. Bounded by `deadline`; what the
    /// hub did not answer is left to the drain's bound.
    pub(crate) async fn retire(&mut self, deadline: tokio::time::Instant) -> Result<()> {
        if self.retired.is_none() {
            self.retired = Some(self.persist(SegmentPhase::Retired, deadline).await?);
        }
        for mut guard in [self.ready.take(), self.loaded.take()]
            .into_iter()
            .flatten()
        {
            // A lost lease took the registration these belong to: there is
            // nothing left to withdraw, and nobody to wait for.
            let withdrawn = tokio::time::timeout_at(deadline, guard.unregister());
            tokio::select! {
                biased;
                lost = self.maintain() => return lost,
                withdrawn = withdrawn => withdrawn
                    .map_err(|_| failure("the hub did not answer in time"))?
                    .map_err(failure)?,
            }
        }
        Ok(())
    }

    /// Whether every member of `senders` that announced `loaded` or
    /// `retired` avoids `me`: it has promised never to route to it again.
    /// A member that left is no longer one. A member that announced neither
    /// reads its roster, and follows membership, only after this instance's
    /// retirement, so it never routes here.
    pub(crate) async fn all_avoid(&self, me: InstanceId, senders: &[&str]) -> Result<bool> {
        let members = self.members().await?;
        let mut routing = HashSet::new();
        for segment in senders {
            routing.extend(self.instances(segment, SegmentPhase::Loaded).await?);
            routing.extend(self.instances(segment, SegmentPhase::Retired).await?);
        }
        let avoiding: HashSet<InstanceId> = self
            .client
            .services()
            .get_instances(&avoids(me))
            .await
            .map_err(failure)?
            .into_iter()
            .collect();
        Ok(members
            .iter()
            .filter(|m| senders.contains(&m.registration.segment.as_str()))
            .map(|m| m.peer.instance_id())
            .filter(|id| *id != me && routing.contains(id))
            .all(|id| avoiding.contains(&id)))
    }

    /// Follow membership into `live`, the state of the roster this instance
    /// routes with, until the session ends: one task for the instance, not
    /// one per request or per peer. `segments` are every
    /// segment of the deployment, `inner` those this instance sends to.
    /// See [`follow`].
    pub(crate) fn follow(
        &mut self,
        live: Arc<Live>,
        node: Velo,
        segments: &[&str],
        inner: &[&str],
        drain: Duration,
        pools: Option<Pools>,
    ) {
        let follower = self.follower(live, node, segments, inner, drain, pools);
        if let Some(earlier) = self.follower.replace(tokio::spawn(follower.run())) {
            earlier.abort();
        }
    }

    /// The follower [`follow`](Self::follow) runs.
    fn follower(
        &self,
        live: Arc<Live>,
        node: Velo,
        segments: &[&str],
        inner: &[&str],
        drain: Duration,
        pools: Option<Pools>,
    ) -> Follower {
        Follower {
            client: self.client.clone(),
            registration: self.registration.clone(),
            me: node.instance_id(),
            live,
            node,
            segments: segments.iter().map(ToString::to_string).collect(),
            inner: inner.iter().map(ToString::to_string).collect(),
            drain,
            acknowledged: HashMap::new(),
            unsent: Vec::new(),
            forget: Vec::new(),
            unresolved: Vec::new(),
            pools,
            #[cfg(test)]
            members_gate: None,
        }
    }

    /// Announce that this instance, a source's, has drained the requests it
    /// held, retried until `deadline`.
    pub(crate) async fn drained(&mut self, deadline: tokio::time::Instant) -> Result<()> {
        if self.drained.is_none() {
            self.drained = Some(self.persist(SegmentPhase::Drained, deadline).await?);
        }
        Ok(())
    }

    /// Announce `phase`, retried until `deadline`: an announcement the hub
    /// refused is gone with its registration guard, and peers would wait
    /// for it until their bound. Each attempt is bounded, since a hub can
    /// stop answering without closing its connections.
    async fn persist(
        &self,
        phase: SegmentPhase,
        deadline: tokio::time::Instant,
    ) -> Result<HubServiceRegistration> {
        loop {
            // A lost lease took the registration this would belong to.
            self.lost()?;
            let attempt = (tokio::time::Instant::now() + POLL * 10).min(deadline);
            match self.announce(phase, attempt).await {
                Ok(registration) => return Ok(registration),
                Err(error) if tokio::time::Instant::now() + POLL >= deadline => {
                    return Err(error);
                }
                Err(_) => tokio::time::sleep(POLL).await,
            }
        }
    }

    /// Announce that this instance, of a draining pool, will send no more
    /// work into its pool, retried until `deadline`.
    pub(crate) async fn detached(&mut self, deadline: tokio::time::Instant) -> Result<()> {
        if self.detached.is_none() {
            self.detached = Some(self.persist(SegmentPhase::Detached, deadline).await?);
        }
        Ok(())
    }

    /// Whether every current member of `segments` has announced that it
    /// detached: it sends no more work into its draining pool. A member of
    /// one of `entries` may have announced that it drained instead, which
    /// an entry of a draining pool does only once it holds no request; a
    /// source elsewhere drains once its serving body ends, while its
    /// segment may still hand work on. A member that left is no longer
    /// one, so it is not waited for.
    pub(crate) async fn all_detached(&self, segments: &[&str], entries: &[&str]) -> Result<bool> {
        let members = self.members().await?;
        let mut detached = HashSet::new();
        for segment in segments {
            detached.extend(self.instances(segment, SegmentPhase::Detached).await?);
            if entries.contains(segment) {
                detached.extend(self.instances(segment, SegmentPhase::Drained).await?);
            }
        }
        Ok(members
            .iter()
            .filter(|m| segments.contains(&m.registration.segment.as_str()))
            .all(|m| detached.contains(&m.peer.instance_id())))
    }

    /// Whether every current member of `segments` has announced `drained`.
    /// A member that left is no longer one, so it is not waited for.
    pub(crate) async fn all_drained(&self, segments: &[&str]) -> Result<bool> {
        all_drained(&self.client, &self.registration, segments).await
    }

    /// What a wait beside the drain asks the hub, apart from the session,
    /// so that the wait borrows none of it.
    pub(crate) fn asker(&self) -> HubAsker {
        HubAsker {
            client: self.client.clone(),
            registration: self.registration.clone(),
            lost: self.lost.clone(),
        }
    }

    /// Leave the hub at once, rather than when the lease runs out, so that
    /// no peer's drain waits for this instance.
    pub(crate) async fn leave(&self) {
        let _ = tokio::time::timeout(POLL * 50, self.client.unregister()).await;
    }

    /// Whether the lease is lost.
    pub(crate) fn is_lost(&self) -> bool {
        self.lost().is_err()
    }

    /// How long to wait between two questions to the hub.
    pub(crate) fn poll() -> Duration {
        POLL
    }

    /// Resolves when the lease is lost: the hub dropped the registration,
    /// or stopped answering for longer than it keeps one. The process is
    /// then invisible to new peers and will be probed as dead, so it should
    /// drain and stop, and let the deployment start it again.
    pub async fn maintain(&self) -> Result<()> {
        let mut lost = self.lost.clone();
        let lost = lost
            .wait_for(|lost| lost.is_some())
            .await
            .map_err(|_| failure("the lease task ended"))?;
        Err(failure(lost.as_deref().unwrap_or("lease lost")))
    }

    fn lost(&self) -> Result<()> {
        match &*self.lost.borrow() {
            Some(reason) => Err(failure(reason)),
            None => Ok(()),
        }
    }

    async fn announce(
        &self,
        phase: SegmentPhase,
        deadline: tokio::time::Instant,
    ) -> Result<HubServiceRegistration> {
        let name = segment_service(&self.registration.segment, phase);
        let announced = self.client.register_service(&name);
        tokio::time::timeout_at(deadline, announced)
            .await
            .map_err(|_| failure("the hub did not answer in time"))?
            .map_err(failure)
    }

    /// The members of this deployment, from the hub.
    async fn members(&self) -> Result<Vec<Member>> {
        members(&self.client, &self.registration).await
    }

    /// The instances of `segment` in `phase`.
    async fn instances(&self, segment: &str, phase: SegmentPhase) -> Result<HashSet<InstanceId>> {
        Ok(self
            .client
            .services()
            .get_instances(&segment_service(segment, phase))
            .await
            .map_err(failure)?
            .into_iter()
            .collect())
    }

    /// The members that have announced `loaded` and not `retired`, as a
    /// roster. `loaded` is read first: an instance that retires between
    /// the two reads is left out, and one that retired before them has
    /// withdrawn `loaded` or is in `retired`. One query per segment and
    /// phase: the hub answers for a segment, not for an instance.
    async fn loaded_members(&self) -> Result<Roster> {
        let members = self.members().await?;
        let segments: BTreeSet<String> = members
            .iter()
            .map(|m| m.registration.segment.clone())
            .collect();
        let mut loaded = BTreeMap::new();
        for segment in &segments {
            let instances = self.instances(segment, SegmentPhase::Loaded).await?;
            loaded.insert(segment.clone(), instances);
        }
        let mut retired = HashSet::new();
        for segment in &segments {
            retired.extend(self.instances(segment, SegmentPhase::Retired).await?);
        }
        let mut roster = Roster::new();
        for member in members {
            let id = member.peer.instance_id();
            if loaded[&member.registration.segment].contains(&id) && !retired.contains(&id) {
                roster.add(member.registration.segment, member.peer);
            }
        }
        Ok(roster)
    }

    /// The members, and those of them in `segments` that have announced
    /// `serving`.
    async fn serving_members(
        &self,
        segments: &[&str],
    ) -> Result<(HashSet<InstanceId>, HashSet<InstanceId>)> {
        let members = self.members().await?;
        let mut serving = HashSet::new();
        for segment in segments {
            serving.extend(self.instances(segment, SegmentPhase::Serving).await?);
        }
        let members = members.iter().map(|m| m.peer.instance_id()).collect();
        Ok((members, serving))
    }
}

impl Drop for HubSession {
    fn drop(&mut self) {
        self.lease.abort();
        if let Some(follower) = &self.follower {
            follower.abort();
        }
    }
}

/// The members of the deployment `registration` belongs to, from the hub.
/// The questions a wait beside an instance's drain asks its hub, held
/// apart from its [`HubSession`].
pub(crate) struct HubAsker {
    client: HubClient,
    registration: SegmentRegistration,
    lost: watch::Receiver<Option<String>>,
}

impl HubAsker {
    /// Whether the session's lease is lost, so that no hub answers.
    pub(crate) fn is_lost(&self) -> bool {
        self.lost.borrow().is_some()
    }

    /// [`HubSession::all_drained`].
    pub(crate) async fn all_drained(&self, segments: &[&str]) -> Result<bool> {
        all_drained(&self.client, &self.registration, segments).await
    }
}

/// Whether every current member of `segments` has announced `drained`. A
/// member that left is no longer one, so it is not waited for.
async fn all_drained(
    client: &HubClient,
    registration: &SegmentRegistration,
    segments: &[&str],
) -> Result<bool> {
    let members = members(client, registration).await?;
    let mut drained = HashSet::new();
    for segment in segments {
        let name = segment_service(segment, SegmentPhase::Drained);
        let instances = client
            .services()
            .get_instances(&name)
            .await
            .map_err(failure)?;
        drained.extend(instances);
    }
    Ok(members
        .iter()
        .filter(|m| segments.contains(&m.registration.segment.as_str()))
        .all(|m| drained.contains(&m.peer.instance_id())))
}

async fn members(client: &HubClient, registration: &SegmentRegistration) -> Result<Vec<Member>> {
    let members: Vec<Member> = client.ext("gdl").get("/members").await.map_err(failure)?;
    for member in &members {
        if member.registration.contract != registration.contract
            || member.registration.placement != registration.placement
        {
            return Err(failure("hub returned a different deployment"));
        }
    }
    Ok(members)
}

/// Applies the hub's membership to the roster an instance routes with.
/// It watches, for each segment this instance sends to, the
/// instances that retired (marked retired, never made routable by it), and
/// for every segment the instances that are ready (registered with velo and
/// added to routing) and those that serve (one removed has left: dropped at
/// once, and unregistered from velo after the drain's bound, since one
/// whose lease was lost may still be draining). After each change it
/// acknowledges each retired instance it no longer routes to.
struct Follower {
    client: HubClient,
    registration: SegmentRegistration,
    me: InstanceId,
    live: Arc<Live>,
    node: Velo,
    segments: Vec<String>,
    inner: Vec<String>,
    drain: Duration,
    /// The promise to avoid each retired instance, served while it lives.
    acknowledged: HashMap<InstanceId, HubServiceRegistration>,
    /// Acknowledgements the hub has not taken yet: retried each tick.
    unsent: Vec<InstanceId>,
    /// Instances that left, to unregister from velo once their time comes.
    forget: Vec<(tokio::time::Instant, InstanceId)>,
    /// Ready instances not added yet, by segment, because the hub did not
    /// say who they are: retried each tick, so that one failed question
    /// does not leave a joiner out for good.
    unresolved: Vec<(String, InstanceId)>,
    /// A chooser's tables, whose registrations this follower leaves alone.
    pools: Option<Pools>,
    /// Holds the follower once the hub has said who the members are, so a
    /// test can change membership in between.
    #[cfg(test)]
    members_gate: Option<Arc<Gate>>,
}

/// A point the follower stops at until a test lets it go.
#[cfg(test)]
#[derive(Default)]
struct Gate {
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

/// Which service a watch follows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Watched {
    Retired,
    Ready,
    Serving,
}

type Watch = BoxStream<'static, (Watched, String, ServiceEvent)>;

impl Follower {
    async fn run(mut self) {
        let mut tick = tokio::time::interval(POLL * 10);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // Watched again from scratch after any stream ends: marks are
            // kept for good, so seeing everything again changes nothing.
            if let Some(mut watches) = self.watch().await {
                loop {
                    tokio::select! {
                        event = watches.next() => match event {
                            Some((_, _, ServiceEvent::Disconnected)) | None => break,
                            Some((watched, segment, event)) => {
                                self.apply(watched, &segment, event).await;
                                self.acknowledge().await;
                            }
                        },
                        _ = tick.tick() => self.tick().await,
                    }
                }
            }
            tokio::select! {
                () = tokio::time::sleep(POLL) => {}
                _ = tick.tick() => self.tick().await,
            }
        }
    }

    /// Open every watch, applying their first sets: the retirements before
    /// any readiness, so that an instance retired before this instance
    /// looked is never routed to first. Then drop the instances that left
    /// before the watches began. `None` if the hub did not answer.
    async fn watch(&mut self) -> Option<SelectAll<Watch>> {
        let mut watches = SelectAll::new();
        let retired = self.inner.iter().map(|s| (Watched::Retired, s.clone()));
        let rest = self
            .segments
            .iter()
            .flat_map(|s| [(Watched::Ready, s.clone()), (Watched::Serving, s.clone())]);
        let wanted: Vec<(Watched, String)> = retired.chain(rest).collect();
        for (watched, segment) in wanted {
            let phase = match watched {
                Watched::Retired => SegmentPhase::Retired,
                Watched::Ready => SegmentPhase::Ready,
                Watched::Serving => SegmentPhase::Serving,
            };
            let services = self.client.services();
            let opened = tokio::time::timeout(
                POLL * 50,
                services.watch_instances(&segment_service(&segment, phase)),
            );
            let mut stream = match opened.await {
                Ok(Ok(stream)) => stream,
                _ => return None,
            };
            // The first event is the set as it is now.
            match tokio::time::timeout(POLL * 50, stream.next()).await {
                Ok(Some(first @ ServiceEvent::Initial(_))) => {
                    self.apply(watched, &segment, first).await;
                }
                _ => return None,
            }
            let tagged = segment.clone();
            watches.push(
                stream
                    .map(move |event| (watched, tagged.clone(), event))
                    .boxed(),
            );
        }
        // Who to judge is fixed before the hub is asked: an instance added
        // to routing meanwhile (the launch merging a newer roster) is
        // missing from the answer, and must not be taken for one that left.
        let candidates: Vec<(String, InstanceId)> = self
            .segments
            .iter()
            .flat_map(|segment| {
                self.live
                    .members(segment)
                    .into_iter()
                    .map(move |id| (segment.clone(), id))
            })
            .filter(|(_, id)| *id != self.me)
            .collect();
        // Without the members, an instance that left while nothing watched
        // would stay routable: start again rather than go on without them.
        let Ok(members) = members(&self.client, &self.registration).await else {
            return None;
        };
        #[cfg(test)]
        if let Some(gate) = &self.members_gate {
            gate.arrived.notify_one();
            gate.release.notified().await;
        }
        {
            let present: HashSet<InstanceId> =
                members.iter().map(|m| m.peer.instance_id()).collect();
            let gone = candidates
                .into_iter()
                .filter(|(_, id)| !present.contains(id));
            for (segment, id) in gone {
                self.left(&segment, id);
            }
        }
        self.acknowledge().await;
        Some(watches)
    }

    async fn apply(&mut self, watched: Watched, segment: &str, event: ServiceEvent) {
        let (added, removed) = match event {
            ServiceEvent::Initial(ids) => (ids, Vec::new()),
            ServiceEvent::Added(id) => (vec![id], Vec::new()),
            ServiceEvent::Removed(id) => (Vec::new(), vec![id]),
            ServiceEvent::Disconnected => return,
        };
        match watched {
            Watched::Retired => {
                for id in added.into_iter().filter(|id| *id != self.me) {
                    self.live.retire(segment, id);
                }
            }
            Watched::Ready => {
                for id in added.into_iter().filter(|id| *id != self.me) {
                    if !self.unresolved.iter().any(|(_, known)| *known == id) {
                        self.unresolved.push((segment.to_string(), id));
                    }
                }
                self.resolve().await;
            }
            Watched::Serving => {
                let me = self.me;
                for id in removed.into_iter().filter(|id| *id != me) {
                    self.left(segment, id);
                }
            }
        }
    }

    /// Add the ready instances not added yet, as the hub names them. One
    /// that is no longer a member has left, and is dropped; one the hub
    /// could not be asked about stays for the next tick.
    async fn resolve(&mut self) {
        let live = &self.live;
        self.unresolved.retain(|(_, id)| !live.routes_to(*id));
        if self.unresolved.is_empty() {
            return;
        }
        let Ok(members) = members(&self.client, &self.registration).await else {
            return;
        };
        for (segment, id) in std::mem::take(&mut self.unresolved) {
            let Some(member) = members.iter().find(|m| m.peer.instance_id() == id) else {
                continue;
            };
            if member.registration.segment != segment {
                continue;
            }
            // Registered before it is routable: a call addressed by
            // instance finds no peer it was not told of.
            let (node, live) = (&self.node, &self.live);
            let registered = Pools::exclusive_of(self.pools.as_ref(), |_| {
                let registered = super::register_peer(node, member.peer.clone());
                if registered.is_ok() {
                    live.join(&segment, id);
                }
                registered
            });
            if registered.is_err() {
                self.unresolved.push((segment, id));
            }
        }
    }

    fn left(&mut self, segment: &str, id: InstanceId) {
        let live = &self.live;
        Pools::leaving(self.pools.as_ref(), id, || live.leave(segment, id));
        self.unresolved.retain(|(_, unresolved)| *unresolved != id);
        // Dropping the promise ends it at the hub, in the background.
        self.acknowledged.remove(&id);
        self.unsent.retain(|unsent| *unsent != id);
        if !self.forget.iter().any(|(_, known)| *known == id) {
            self.forget
                .push((tokio::time::Instant::now() + self.drain, id));
        }
    }

    /// Promise to avoid each retired instance this instance no longer
    /// routes to: marked first, then served.
    async fn acknowledge(&mut self) {
        self.unsent.extend(self.live.take_acknowledgements());
        let mut unsent = Vec::new();
        for id in std::mem::take(&mut self.unsent) {
            let name = avoids(id);
            let promised = tokio::time::timeout(POLL * 10, self.client.register_service(&name));
            match promised.await {
                Ok(Ok(registration)) => {
                    self.acknowledged.insert(id, registration);
                }
                Ok(Err(ClientError::AlreadyServing(_))) => {}
                _ => unsent.push(id),
            }
        }
        self.unsent = unsent;
    }

    async fn tick(&mut self) {
        let now = tokio::time::Instant::now();
        let (node, pools) = (&self.node, &self.pools);
        self.forget.retain(|(at, id)| {
            if *at > now {
                return true;
            }
            // A peer a chooser's table holds is the chooser's to forget.
            Pools::drained(pools.as_ref(), *id, || {
                super::unregister_peer(node, *id);
            });
            false
        });
        // An instance added here can displace a retired one, which is then
        // owed its promise, as after an event.
        if !self.unresolved.is_empty() || !self.unsent.is_empty() {
            self.resolve().await;
            self.acknowledge().await;
        }
    }
}

/// Keep a registration alive: a heartbeat every `heartbeat`, each bounded
/// by the same, retried through failures the hub may recover from (a
/// reset connection, a `busy` answer, an answer that never comes) until
/// `grace` has passed since the last one the hub acknowledged. The lease
/// is reported lost then, or at once when the hub says it holds no
/// registration. The deployment's hub keeps registrations in memory, so
/// one that restarted holds none, and the next heartbeat ends the lease:
/// a segment does not outlive its hub, it drains and stops, and the
/// deployment starts it again.
async fn keep_lease(client: HubClient, heartbeat: Duration, report: watch::Sender<Option<String>>) {
    let grace = heartbeat * 5;
    let mut acknowledged = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(heartbeat).await;
        let result = match tokio::time::timeout(heartbeat, client.send_heartbeat()).await {
            Ok(result) => result.map_err(|e| (is_final(&e), e.to_string())),
            Err(_) => Err((false, format!("no answer within {heartbeat:?}"))),
        };
        match result {
            Ok(()) => acknowledged = tokio::time::Instant::now(),
            Err((final_loss, error)) => {
                if final_loss {
                    let _ = report.send(Some(error));
                    return;
                }
                if acknowledged.elapsed() >= grace {
                    let _ = report.send(Some(format!(
                        "the hub has not answered for {grace:?}: {error}"
                    )));
                    return;
                }
                tracing::warn!(%error, "hub heartbeat failed; retrying");
            }
        }
    }
}

/// Whether a failed hub call means the registration is gone, rather than
/// the hub being momentarily unreachable.
fn is_final(error: &ClientError) -> bool {
    match error {
        ClientError::NotRegistered | ClientError::AlreadyRegistered(_) => true,
        ClientError::Hub { status, .. } => status.as_u16() == 404 || status.as_u16() == 401,
        _ => false,
    }
}

const POLL: Duration = Duration::from_millis(100);

/// The hub service an instance serves to promise that it never routes to
/// `retired` again.
fn avoids(retired: InstanceId) -> String {
    format!("gdl-avoids-{:032x}", retired.as_u128())
}

fn failure(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Transport, error.to_string())
}

#[derive(Clone, Deserialize)]
struct Member {
    registration: SegmentRegistration,
    peer: PeerInfo,
}

/// Bind this instance's velo node under the hub at `host`: at `listen`,
/// or, for an unspecified address, at the address the container's
/// `HOSTNAME` resolves to, on `listen`'s port (binding to an unspecified
/// address would advertise `0.0.0.0` to peers). The node resolves a peer it
/// has not registered through the hub; the client that does
/// so holds no registration.
pub(crate) async fn bind_under_hub(
    listen: std::net::SocketAddr,
    host: &str,
    public_port: u16,
    control_port: u16,
) -> Result<Velo> {
    let client = HubClient::builder()
        .host(host)
        .public_port(public_port)
        .control_port(control_port)
        .build()
        .map_err(failure)?;
    let address = if listen.ip().is_unspecified() {
        container_address(std::env::var("HOSTNAME").ok().as_deref(), listen.port()).await?
    } else {
        listen
    };
    super::build_node(address, Some(Arc::new(client.discovery()))).await
}

/// This container's routable address: the one its `HOSTNAME`, `host`,
/// resolves to, on `port`. The caller reads the variable, so that a test
/// can pass a name without changing the environment other tests share.
async fn container_address(host: Option<&str>, port: u16) -> Result<std::net::SocketAddr> {
    let host = host.ok_or_else(|| {
        failure("HOSTNAME is not set; under a hub a segment binds the address its container's hostname resolves to")
    })?;
    tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| failure(format!("resolving HOSTNAME {host:?}: {e}")))?
        .find(|address| address.is_ipv4())
        .ok_or_else(|| failure(format!("HOSTNAME {host:?} has no IPv4 address")))
}

#[cfg(test)]
mod tests {
    use gdl_model::{SegmentPhase, segment_service};

    mod following {
        use super::super::{Gate, HubSession};
        use crate::Roster;
        use gdl_model::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
        use std::sync::Arc;
        use std::time::Duration;

        fn spec() -> DeploymentSpec {
            let graph = Manifest {
                name: "follow-test".into(),
                schema: 1,
                nodes: vec![
                    NodeInfo {
                        name: "Source".into(),
                        role: Role::Source,
                        binding: None,
                    },
                    NodeInfo {
                        name: "Echo".into(),
                        role: Role::Sink,
                        binding: Some("echo".into()),
                    },
                ],
                hops: vec![HopInfo {
                    outer: "Source".into(),
                    inner: "Echo".into(),
                    forward: vec![],
                    backward: vec![],
                    forward_types: vec![TypeInfo::new("Request", true, Default::default())],
                    backward_types: vec![TypeInfo::new("Reply", true, Default::default())],
                }],
            };
            let placement = Placement::new()
                .segment("front", ["Source"])
                .segment("sink", ["Echo"]);
            DeploymentSpec::new(graph, placement).expect("valid test fixture")
        }

        async fn hub() -> velo_hub::Hub {
            let mut config = velo_hub::HubConfig::default();
            config.server.public_port = 0;
            config.server.control_port = 0;
            velo_hub::Hub::builder(config)
                .extension(gdl_hub::GraphAdmission::new(spec()))
                .serve()
                .await
                .expect("valid test fixture")
        }

        async fn node() -> velo::Velo {
            crate::dist::bind_node("127.0.0.1:0".parse().expect("valid test fixture"))
                .await
                .expect("node")
        }

        async fn session(hub: &velo_hub::Hub, segment: &str, node: &velo::Velo) -> HubSession {
            HubSession::register(
                "127.0.0.1",
                hub.public_addr().port(),
                hub.control_addr().port(),
                node,
                spec().registration(segment).expect("valid test fixture"),
                Duration::from_secs(1),
            )
            .await
            .expect("registered")
        }

        /// The follower compares the hub's members with the instances it
        /// routes to. An instance added to routing after the hub answered
        /// (the launch merging a newer roster) is not in that answer, and
        /// must not be taken for one that left.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn an_instance_added_after_the_members_were_read_has_not_left() {
            let hub = hub().await;
            let front = node().await;
            let front_session = session(&hub, "front", &front).await;
            let live = Arc::clone(Roster::new().live());
            let mut follower = front_session.follower(
                Arc::clone(&live),
                front.clone(),
                &["front", "sink"],
                &["sink"],
                Duration::from_secs(30),
                None,
            );
            let gate = Arc::new(Gate::default());
            follower.members_gate = Some(Arc::clone(&gate));
            let watching = tokio::spawn(async move { follower.watch().await.is_some() });
            tokio::time::timeout(Duration::from_secs(5), gate.arrived.notified())
                .await
                .expect("the follower read the members");
            // A sink joins the hub, and the launch's roster takes it, after
            // the follower's answer.
            let sink = node().await;
            let _sink_session = session(&hub, "sink", &sink).await;
            live.join("sink", sink.instance_id());
            gate.release.notify_one();
            assert!(
                watching.await.expect("the follower's task"),
                "the watches opened"
            );
            assert_eq!(
                live.members("sink"),
                [sink.instance_id()],
                "a sink that joined after the answer was taken for one that left"
            );
            hub.shutdown().await.expect("valid test fixture");
        }

        /// A tick that adds a ready instance the hub could not name before
        /// can make a retired one ineligible: the follower then promises to
        /// avoid it, as it does after an event.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_tick_that_adds_an_instance_acknowledges_what_it_displaces() {
            let hub = hub().await;
            let front = node().await;
            let front_session = session(&hub, "front", &front).await;
            let live = Arc::clone(Roster::new().live());
            let mut follower = front_session.follower(
                Arc::clone(&live),
                front.clone(),
                &["front", "sink"],
                &["sink"],
                Duration::from_secs(30),
                None,
            );
            // The only sink routed to has retired; a second is ready, but
            // the hub could not say who it was when its event came.
            let (retired, joiner) = (node().await, node().await);
            live.join("sink", retired.instance_id());
            live.retire("sink", retired.instance_id());
            let _joiner_session = session(&hub, "sink", &joiner).await;
            follower
                .unresolved
                .push(("sink".to_string(), joiner.instance_id()));
            follower.tick().await;
            assert_eq!(live.members("sink").len(), 2, "the joiner was added");
            let promised = follower.acknowledged.contains_key(&retired.instance_id())
                || follower.unsent.contains(&retired.instance_id());
            assert!(promised, "the retired sink was displaced without a promise");
            hub.shutdown().await.expect("valid test fixture");
        }
    }

    /// Under a hub, a segment told to listen on an unspecified address
    /// binds what its `HOSTNAME` resolves to, and fails to start when that
    /// gives no IPv4 address. The names are IP literals, which resolve
    /// without DNS.
    mod container {
        use super::super::container_address;

        #[tokio::test]
        async fn an_unset_hostname_fails() {
            let error = container_address(None, 7000)
                .await
                .expect_err("no name gives no address");
            assert!(error.to_string().contains("HOSTNAME is not set"), "{error}");
        }

        #[tokio::test]
        async fn a_hostname_with_no_ipv4_address_fails() {
            let error = container_address(Some("::1"), 7000)
                .await
                .expect_err("an IPv6 literal has no IPv4 address");
            assert!(
                error
                    .to_string()
                    .contains(r#"HOSTNAME "::1" has no IPv4 address"#),
                "{error}"
            );
        }

        #[tokio::test]
        async fn a_hostname_with_an_ipv4_address_gives_it_on_the_port() {
            let address = container_address(Some("127.0.0.1"), 7000).await;
            assert_eq!(
                address.expect("an IPv4 literal resolves"),
                "127.0.0.1:7000".parse().expect("valid test fixture")
            );
        }
    }

    /// A node bound under a hub streams to an anchor minted by a peer it was
    /// never told of, as an engine does to a joiner's anchor before its
    /// follower has seen the joiner: velo asks the hub who the peer is. A
    /// node bound without the hub cannot, which shows what the hub adds;
    /// that absence takes a bounded wait of two seconds to show.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_under_a_hub_reaches_a_peer_it_was_never_told_of() {
        use super::super::{Fabric, Frame, Outgoing, bind_node, bind_under_hub};
        use futures::StreamExt;
        use std::time::Duration;

        let mut config = velo_hub::HubConfig::default();
        config.server.public_port = 0;
        config.server.control_port = 0;
        let hub = velo_hub::Hub::builder(config)
            .serve()
            .await
            .expect("valid test fixture");
        let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
        let local = "127.0.0.1:0".parse().expect("valid test fixture");
        let owner = bind_node(local).await.expect("node");
        let client = velo_hub::client::HubClient::builder()
            .host("127.0.0.1")
            .public_port(public)
            .control_port(control)
            .build()
            .expect("valid test fixture");
        client
            .register(&owner, Default::default())
            .await
            .expect("the owner registers");
        let fabric = |node| Fabric::new(node, None, None, Duration::from_secs(30));
        let owner = fabric(owner);
        let bound = |under_hub: bool| async move {
            match under_hub {
                true => bind_under_hub(local, "127.0.0.1", public, control).await,
                false => bind_node(local).await,
            }
            .expect("node")
        };

        // A ticket opens without asking the owner; the stream's batches are
        // what go to its worker, so delivery is the witness.
        let delivered = |attacher: Fabric| {
            let (mut inbound, reply) = owner.mint();
            async move {
                let sent = async {
                    let out = attacher.attach(&reply).await.ok()?;
                    out.send(Outgoing::Item(super::super::EncodedItem::of(&[7])))
                        .await
                        .ok()?;
                    out.finish().await;
                    Some(())
                };
                let wait = Duration::from_secs(2);
                let _ = tokio::time::timeout(wait, sent).await;
                matches!(
                    tokio::time::timeout(wait, inbound.next()).await,
                    Ok(Some(Frame::Item(item))) if item[..] == [7]
                )
            }
        };
        assert!(
            !delivered(fabric(bound(false).await)).await,
            "a node that knows no hub reached a peer it was never told of"
        );
        assert!(
            delivered(fabric(bound(true).await)).await,
            "a node under the hub did not reach a peer it was never told of"
        );
        drop(client);
        hub.shutdown().await.expect("valid test fixture");
    }

    /// The segment name limit is set by the hub's service names: the
    /// longest name allowed fits, and one byte more does not.
    #[test]
    fn the_segment_name_limit_fits_the_hubs_service_names() {
        let fits = "s".repeat(gdl_model::SEGMENT_NAME_LIMIT);
        assert!(velo_hub::protocol::is_valid_service_name(&segment_service(
            &fits,
            SegmentPhase::Serving
        )));
        for phase in [
            SegmentPhase::Loaded,
            SegmentPhase::Ready,
            SegmentPhase::Drained,
            SegmentPhase::Retired,
            SegmentPhase::Detached,
        ] {
            assert!(velo_hub::protocol::is_valid_service_name(&segment_service(
                &fits, phase
            )));
        }
        let over = "s".repeat(gdl_model::SEGMENT_NAME_LIMIT + 1);
        assert!(!velo_hub::protocol::is_valid_service_name(
            &segment_service(&over, SegmentPhase::Serving)
        ));
    }
}

mod chooser;
#[cfg(test)]
mod drains;
mod load;
mod pool;
mod records;

pub(crate) use chooser::ChooserHub;
pub(crate) use pool::{Parent, PoolDrain};
