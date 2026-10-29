//! The fixtures the pool hub's tests share: a deployment of two classes,
//! its model pool, a hub of it, instances registered as segments register,
//! and the pool's records as a reader sees them.

// Each test target uses a part of these.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use gdl_hub::{GraphAdmission, PoolHealth, PoolReadiness, PoolTiming};
use gdl_model::{
    DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, PoolSettings, Role, SegmentPhase,
    TypeInfo, segment_service,
};
use gdl_pools::{
    DirectoryError, MemoryDirectory, MemoryWatch, PoolDirectory, PoolEvent, PoolRecord, PoolState,
    ResumeToken,
};
use gdl_watch::{Avoids, KeyedAsk, KeyedOp, KeyedReply, LoadReport, Unoffered};
use tokio::sync::watch;
use velo_hub::client::{ClientError, HubClient, HubServiceRegistration};
use velo_hub::velo::discovery::ServiceRegistrationGuard;
use velo_hub::velo::{InstanceId, Velo};
use velo_hub::{Hub, HubConfig};

/// How long any one observation may take before the test fails.
pub const WAIT: Duration = Duration::from_secs(5);

/// `S => F { D }` in two classes: `global` runs `a` (S), and `model_x`
/// runs `f` (F) and `d` (D); the boundary is the inner end of S's hop.
pub fn spec() -> DeploymentSpec {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let hop = |outer: &str, inner: &str| HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![TypeInfo::new("T", true, Default::default())],
        backward_types: vec![TypeInfo::new("T", true, Default::default())],
    };
    let graph = Manifest {
        name: "pools".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("F", Role::Operator),
            node("D", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("F", "D")],
    };
    let placement = Placement::new()
        .segment("a", ["S"])
        .segment("f", ["F"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

pub const POOL: &str = "model-x-01-us-east-1";

/// The model pool, a child of [`GLOBAL`]: `d` needs two usable instances,
/// `f` one.
pub fn settings() -> PoolSettings {
    toml::from_str(&format!(
        "name = '{POOL}'\nclass = 'model_x'\nregion = 'us-east-1'\nhome = '{GLOBAL}'\n\
         hub = 'model-x-01-hub:1337'\ndirectory = 'memory'\n[ready]\nd = 2\n"
    ))
    .expect("valid test fixture")
}

/// A hold long enough to observe, short enough to wait out; no heartbeat
/// in the length of a test, so every record seen is a change of state.
pub fn timing() -> PoolTiming {
    PoolTiming {
        hold: Duration::from_millis(300),
        heartbeat: Duration::from_secs(600),
        lease: Duration::from_secs(30),
        ..PoolTiming::default()
    }
}

/// A hub of the deployment with the pool's extension beside admission.
pub async fn pool_hub<D: PoolDirectory>(
    directory: D,
    timing: PoolTiming,
) -> (Hub, Arc<PoolReadiness<D>>) {
    hub_of(settings(), directory, timing).await
}

/// A hub of the deployment with the extension of the pool `pool`.
pub async fn hub_of<D: PoolDirectory>(
    pool: PoolSettings,
    directory: D,
    timing: PoolTiming,
) -> (Hub, Arc<PoolReadiness<D>>) {
    hub_of_spec(spec(), pool, directory, timing).await
}

/// A hub of the deployment `spec` with the extension of the pool `pool`.
pub async fn hub_of_spec<D: PoolDirectory>(
    spec: DeploymentSpec,
    pool: PoolSettings,
    directory: D,
    timing: PoolTiming,
) -> (Hub, Arc<PoolReadiness<D>>) {
    let pool =
        Arc::new(PoolReadiness::new(&spec, pool, directory, timing).expect("valid test fixture"));
    let hub = Hub::builder(config())
        .extension(GraphAdmission::new(spec))
        .extension(Arc::clone(&pool))
        .serve()
        .await
        .expect("the hub serves");
    (hub, pool)
}

pub fn config() -> HubConfig {
    let mut config = HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    config
}

/// One instance of a segment, registered with the hub as a segment
/// registers, and announcing its phases.
pub struct Instance {
    client: HubClient,
    segment: &'static str,
    phases: HashMap<SegmentPhase, HubServiceRegistration>,
    node: Velo,
}

impl Instance {
    pub async fn join(hub: &Hub, segment: &'static str) -> Self {
        Self::join_spec(hub, &spec(), segment).await
    }

    /// Join as an instance of `segment` of the deployment `spec`.
    pub async fn join_spec(hub: &Hub, spec: &DeploymentSpec, segment: &'static str) -> Self {
        Self::try_join(hub, spec, segment)
            .await
            .expect("the hub admits the instance")
    }

    /// Ask to join as an instance of `segment` of the deployment `spec`:
    /// the instance, or the hub's refusal.
    pub async fn try_join(
        hub: &Hub,
        spec: &DeploymentSpec,
        segment: &'static str,
    ) -> Result<Self, ClientError> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
        let tcp = velo_hub::velo::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("valid test fixture")
            .build()
            .expect("valid test fixture");
        let node = Velo::builder()
            .add_transport(Arc::new(tcp))
            .build()
            .await
            .expect("a velo node");
        let client = HubClient::builder()
            .host("127.0.0.1")
            .public_port(hub.public_addr().port())
            .control_port(hub.control_addr().port())
            .build()
            .expect("valid test fixture");
        let registration = spec.registration(segment).expect("valid test fixture");
        let payload = serde_json::to_value(&registration).expect("valid test fixture");
        client
            .register(&node, BTreeMap::from([("gdl".to_string(), payload)]))
            .await?;
        Ok(Self {
            client,
            segment,
            phases: HashMap::new(),
            node,
        })
    }

    pub fn id(&self) -> InstanceId {
        self.node.instance_id()
    }

    pub async fn announce(&mut self, phase: SegmentPhase) {
        let name = segment_service(self.segment, phase);
        let registration = self
            .client
            .register_service(&name)
            .await
            .expect("the hub takes the service");
        self.phases.insert(phase, registration);
    }

    pub async fn withdraw(&mut self, phase: SegmentPhase) {
        let mut registration = self.phases.remove(&phase).expect("announced");
        registration
            .unregister()
            .await
            .expect("the hub drops the service");
    }

    /// Report `load` to the hub as this instance's admission.
    pub async fn report(&self, load: LoadReport) -> Result<(), ClientError> {
        self.client
            .ext(gdl_watch::EXTENSION)
            .control_post(&gdl_watch::load_path(self.id()), &load)
            .await
    }

    /// Join as an instance of `a`, the global's segment that chooses
    /// pools of `model_x`, and say, as a chooser does before it watches
    /// its hub's relay, that it runs a follower: from then the hub waits
    /// for its acknowledgements.
    pub async fn chooser(hub: &Hub) -> Self {
        let instance = Self::join(hub, "a").await;
        instance
            .client
            .ext(gdl_watch::EXTENSION)
            .control_post::<_, ()>(&gdl_watch::chooser_path(instance.id()), &())
            .await
            .expect("the hub takes the chooser");
        instance
    }

    /// Tell the hub, as this instance, that it holds no offer to the
    /// follow `unoffered` names, at its revision.
    pub async fn acknowledge(&self, unoffered: &Unoffered) -> Result<(), ClientError> {
        self.client
            .ext(gdl_watch::EXTENSION)
            .control_post(&gdl_watch::unoffered_path(self.id()), unoffered)
            .await
    }

    /// Tell the hub, as this instance, that it avoids the retired entry
    /// instance `avoids` names.
    pub async fn avoid(&self, avoids: &Avoids) -> Result<(), ClientError> {
        self.client
            .ext(gdl_watch::EXTENSION)
            .control_post(&gdl_watch::avoids_path(self.id()), avoids)
            .await
    }

    /// Ask the hub's keyed record table, as this instance, to apply
    /// `ops`, in order.
    pub async fn keyed(&self, ops: Vec<KeyedOp>) -> Result<KeyedReply, ClientError> {
        self.client
            .ext(gdl_watch::KEYED_EXTENSION)
            .control_post(&gdl_watch::keyed_path(self.id()), &KeyedAsk { ops })
            .await
    }

    /// The credential of this instance's registration.
    pub fn credential(&self) -> velo_hub::protocol::Credential {
        self.client
            .registration()
            .expect("the instance is registered")
            .credential
    }

    /// Whether the hub still holds this instance's registration: a call
    /// that needs its credential is answered.
    pub async fn registered(&self) -> bool {
        let mut probe = match self.client.register_service("probe").await {
            Ok(probe) => probe,
            Err(_) => return false,
        };
        probe.unregister().await.is_ok()
    }

    /// Leave the hub, as an instance whose lease ends does.
    pub async fn leave(self) {
        self.client
            .unregister()
            .await
            .expect("the hub drops the instance");
    }
}

/// Wait until the extension's health satisfies `ready`, and return it.
pub async fn health<D: PoolDirectory>(
    pool: &PoolReadiness<D>,
    what: &str,
    ready: impl FnMut(&PoolHealth) -> bool,
) -> PoolHealth {
    let mut status = pool.health();
    let waited = tokio::time::timeout(WAIT, async {
        status.wait_for(ready).await.map(|health| health.clone())
    })
    .await;
    match waited {
        Ok(health) => health.expect("the extension runs"),
        Err(_) => panic!("timed out waiting for {what}: {:?}", *status.borrow()),
    }
}

pub fn usable(health: &PoolHealth, segment: &str) -> u32 {
    health.segments[segment].usable
}

/// The pool's records, as a reader of its class sees them.
pub struct Records {
    watch: MemoryWatch,
    pool: String,
}

impl Records {
    /// The records of the model pool.
    pub fn new(directory: &MemoryDirectory) -> Self {
        Self::of(directory, "model_x", POOL)
    }

    /// The records of the pool `pool` of `class`.
    pub fn of(directory: &MemoryDirectory, class: &str, pool: &str) -> Self {
        Self {
            watch: directory.watch(class, None),
            pool: pool.into(),
        }
    }

    /// The next record of the pool.
    pub async fn next(&mut self) -> PoolRecord {
        let next = async {
            loop {
                match self.watch.next().await {
                    Some(Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. }))
                        if record.name == self.pool =>
                    {
                        return record;
                    }
                    Some(Ok(_)) => {}
                    other => panic!("the watch ended: {other:?}"),
                }
            }
        };
        tokio::time::timeout(WAIT, next)
            .await
            .expect("timed out waiting for a record")
    }

    /// The next record of the pool in `state`, and when it was seen.
    pub async fn until(&mut self, state: PoolState) -> (PoolRecord, Instant) {
        loop {
            let record = self.next().await;
            if record.state == state {
                return (record, Instant::now());
            }
        }
    }
}

pub fn millis_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after 1970")
            .as_millis(),
    )
    .expect("valid test fixture")
}

/// Bring `f` to one ready instance and `d` to two: the pool's quorum.
/// Returns the instances, and when the last announcement began.
pub async fn meet_the_quorum(hub: &Hub) -> (Vec<Instance>, Instant) {
    let mut instances = Vec::new();
    for segment in ["f", "d"] {
        let mut instance = Instance::join(hub, segment).await;
        instance.announce(SegmentPhase::Ready).await;
        instances.push(instance);
    }
    let mut last = Instance::join(hub, "d").await;
    let began = Instant::now();
    last.announce(SegmentPhase::Ready).await;
    instances.push(last);
    (instances, began)
}

/// `GET` on the hub's public listener: the status and the body.
pub async fn get(hub: &Hub, path: &str) -> (u16, String) {
    let url = format!("http://127.0.0.1:{}{path}", hub.public_addr().port());
    let response = tokio::time::timeout(WAIT, reqwest::get(&url))
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    let status = response.status().as_u16();
    (status, response.text().await.expect("a body"))
}

/// A directory whose writes a test can hold: while it is closed, a
/// publish waits, and the test sees how many wait.
#[derive(Clone)]
pub struct Gated {
    pub inner: MemoryDirectory,
    open: Arc<watch::Sender<bool>>,
    held: Arc<watch::Sender<usize>>,
}

impl Gated {
    pub fn new() -> Self {
        Self {
            inner: MemoryDirectory::new(),
            open: Arc::new(watch::Sender::new(true)),
            held: Arc::new(watch::Sender::new(0)),
        }
    }

    pub fn close(&self) {
        self.open.send_replace(false);
    }

    pub fn open(&self) {
        self.open.send_replace(true);
    }

    /// Wait until a write is held at the gate.
    pub async fn holding(&self) {
        let mut held = self.held.subscribe();
        tokio::time::timeout(WAIT, held.wait_for(|held| *held > 0))
            .await
            .expect("a write reached the gate in time")
            .expect("the gate lives");
    }

    pub fn holds(&self) -> usize {
        *self.held.borrow()
    }
}

impl PoolDirectory for Gated {
    type Watch = MemoryWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        let mut open = self.open.subscribe();
        if !*open.borrow_and_update() {
            self.held.send_modify(|held| *held += 1);
            let _ = open.wait_for(|open| *open).await;
            self.held.send_modify(|held| *held -= 1);
        }
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: gdl_pools::Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.inner.watch(class, from)
    }
}

/// A directory that names each pool's hub as DNS would: a record's `hub`
/// is a name, which a test resolves to the address the hub serves at once
/// it serves, since a hub's port is assigned only then. A publish waits
/// until its hub's name is resolved, then stores the record with the
/// address in its place.
#[derive(Clone)]
pub struct Resolving {
    pub inner: MemoryDirectory,
    names: Arc<watch::Sender<HashMap<String, String>>>,
}

impl Resolving {
    pub fn new() -> Self {
        Self {
            inner: MemoryDirectory::new(),
            names: Arc::new(watch::Sender::new(HashMap::new())),
        }
    }

    /// The hub named `name` serves as `hub`.
    pub fn resolve(&self, name: &str, hub: &Hub) {
        let address = format!("127.0.0.1:{}", hub.public_addr().port());
        self.names.send_modify(|names| {
            names.insert(name.to_string(), address);
        });
    }
}

impl PoolDirectory for Resolving {
    type Watch = MemoryWatch;

    async fn publish(&self, mut record: PoolRecord) -> Result<(), DirectoryError> {
        let mut names = self.names.subscribe();
        let address = names
            .wait_for(|names| names.contains_key(&record.hub))
            .await
            .expect("the names live")[&record.hub]
            .clone();
        record.hub = address;
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: gdl_pools::Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.inner.watch(class, from)
    }
}

/// A hub of the deployment with the extension of the pool `pool`, its hub
/// name resolved in `directory` once it serves.
pub async fn serve_pool(
    pool: PoolSettings,
    directory: &Resolving,
    timing: PoolTiming,
) -> (Hub, Arc<PoolReadiness<Resolving>>) {
    let name = pool.hub.clone().expect("a pool's hub has a name");
    let (hub, readiness) = hub_of(pool, directory.clone(), timing).await;
    directory.resolve(&name, &hub);
    (hub, readiness)
}

/// A model pool named `name` in `region`, a child of [`GLOBAL`], whose hub
/// is named `hub`: `d` needs two usable instances, `f` one.
pub fn model(name: &str, region: &str, hub: &str) -> PoolSettings {
    toml::from_str(&format!(
        "name = '{name}'\nclass = 'model_x'\nregion = '{region}'\nhome = '{GLOBAL}'\n\
         hub = '{hub}'\ndirectory = 'memory'\n[ready]\nd = 2\n"
    ))
    .expect("valid test fixture")
}

pub const GLOBAL: &str = "global-01-us-east-1";

/// The global pool in us-east-1, whose class runs `a` and sends to
/// `model_x`, with `requires` unsaid: its health requires one ready property
/// pool, the default.
pub fn global_default() -> PoolSettings {
    toml::from_str(&format!(
        "name = '{GLOBAL}'\nclass = 'global'\nregion = 'us-east-1'\nhub = 'global-01-hub:1337'\n\
         directory = 'memory'\n"
    ))
    .expect("valid test fixture")
}

/// [`global_default`], requiring `requires` ready model pools.
pub fn global_requiring(requires: u32) -> PoolSettings {
    PoolSettings {
        requires: [("model_x".to_string(), requires)].into(),
        ..global_default()
    }
}

/// Another directory with its pool's hub named as DNS would name it: the
/// record's `hub` is a name, which the test resolves once the hub serves,
/// since a hub's port is assigned only then. A publish waits until its
/// hub's name is resolved, and goes on with the address in its place.
#[derive(Clone)]
pub struct Named<D> {
    pub inner: D,
    names: Arc<watch::Sender<HashMap<String, String>>>,
}

impl<D: PoolDirectory> Named<D> {
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            names: Arc::new(watch::Sender::new(HashMap::new())),
        }
    }

    /// The hub named `name` serves as `hub`.
    pub fn resolve(&self, name: &str, hub: &Hub) {
        let address = format!("127.0.0.1:{}", hub.public_addr().port());
        self.names.send_modify(|names| {
            names.insert(name.to_string(), address);
        });
    }
}

impl<D: PoolDirectory> PoolDirectory for Named<D> {
    type Watch = D::Watch;

    async fn publish(&self, mut record: PoolRecord) -> Result<(), DirectoryError> {
        let mut names = self.names.subscribe();
        let address = names
            .wait_for(|names| names.contains_key(&record.hub))
            .await
            .expect("the names live")[&record.hub]
            .clone();
        record.hub = address;
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: gdl_pools::Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> D::Watch {
        self.inner.watch(class, from)
    }
}

/// The public listener of `hub`, as another hub reaches it.
pub fn address(hub: &Hub) -> String {
    format!("127.0.0.1:{}", hub.public_addr().port())
}

/// `event` as a hub without the property index sent it: the follow without
/// its listing, and the marker without its flag. `None` for a `listed`
/// event, which only the property index reads ([`crate`]'s `listed` tests).
pub fn plain_follow(event: gdl_watch::FollowEvent) -> Option<gdl_watch::FollowEvent> {
    use gdl_watch::FollowEvent;
    match event {
        FollowEvent::Listed { .. } => None,
        FollowEvent::Synced { .. } => Some(FollowEvent::Synced { listed: false }),
        FollowEvent::Follow {
            class,
            pool,
            generation,
            region,
            state,
            load,
            load_age_ms,
            ..
        } => Some(FollowEvent::Follow {
            class,
            pool,
            generation,
            region,
            state,
            load,
            load_age_ms,
            listing: None,
        }),
        event => Some(event),
    }
}

/// `event` as a hub without the property index sent it: the pool without its
/// properties. `None` for a `properties` event.
pub fn plain_pool(event: gdl_watch::PoolWatchEvent) -> Option<gdl_watch::PoolWatchEvent> {
    use gdl_watch::PoolWatchEvent;
    match event {
        PoolWatchEvent::Properties { .. } => None,
        PoolWatchEvent::Pool {
            name,
            class,
            epoch,
            state,
            load,
            ..
        } => Some(PoolWatchEvent::Pool {
            name,
            class,
            epoch,
            state,
            load,
            properties: None,
        }),
        event => Some(event),
    }
}
