//! A deployment of two classes for the pool tests, and its instances'
//! settings: `global` runs `a` (S, the chooser), and `model_x` runs `f`
//! (F, the pool entry) and `d` (D). The boundary is the inner end of S's
//! hop.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use figment::providers::Format as _;
use gdl_hub::{GraphAdmission, PoolReadiness, PoolTiming};
use gdl_model::{PoolSettings, SegmentPhase, segment_service};
use gdl_pools::{
    DirectoryError, MemoryDirectory, MemoryWatch, PoolDirectory, PoolRecord, ResumeToken,
};
use gdl_runtime::velo::{InstanceId, Velo};
use gdl_runtime::{
    DeploymentSpec, FollowedPool, HopInfo, Hosted, Launch, Manifest, NodeInfo, Placement, Pools,
    Process, ProcessConfig, Role, TypeInfo,
};
use serde::Deserialize;
use tokio::sync::watch;
use velo_hub::Hub;
use velo_hub::client::{HubClient, HubServiceRegistration};

use super::WAIT;

/// Every segment of [`spec`].
pub const SEGMENTS: &[&str] = &["a", "f", "d"];

/// The region every pool of these tests runs in.
pub const REGION: &str = "us-east-1";

/// The global pool: the parent of every model pool here unless a test
/// says otherwise, since a global's hub follows only its children.
pub const GLOBAL: &str = "global-01-us-east-1";

/// `S => F { D }`: `a` in `global`, `f` and `d` in `model_x`.
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

/// The `[pool]` table of an instance of the global pool.
pub fn global_pool() -> String {
    format!(
        "[pool]\nname = '{GLOBAL}'\nclass = 'global'\nregion = '{REGION}'\ndirectory = 'memory'\n"
    )
}

/// The `[pool]` table of an instance of the model pool `name`.
pub fn model_pool(name: &str) -> String {
    format!(
        "[pool]\nname = '{name}'\nclass = 'model_x'\nregion = '{REGION}'\ndirectory = 'memory'\n"
    )
}

/// The settings of an instance at the hub on these ports, heartbeating
/// every 200 ms, waiting for its peers for `rendezvous_ms`, with `extra`
/// appended (a `[pool]` table, `[min_replicas]`, `[runtime]`).
pub fn settings(
    public: u16,
    control: u16,
    rendezvous_ms: u64,
    extra: &str,
) -> ProcessConfig<Nothing> {
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {public}, control_port = {control}, heartbeat_ms = 200 }}\nrendezvous_ms = {rendezvous_ms}\n{extra}"
    );
    ProcessConfig::from_figment(figment::Figment::from(figment::providers::Toml::string(
        &settings,
    )))
    .expect("settings")
}

/// A hub that admits [`spec`]'s segments, on OS-assigned ports, without a
/// pool's extension.
pub async fn admission_hub() -> velo_hub::Hub {
    velo_hub::Hub::builder(hub_config())
        .extension(gdl_hub::GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("the hub serves")
}

/// A hub's configuration on OS-assigned ports.
pub fn hub_config() -> velo_hub::HubConfig {
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    config
}

/// Launch an instance of `segment` with `config`, in a process of its own.
pub async fn launch(
    config: &ProcessConfig<Nothing>,
    segment: &'static str,
) -> gdl::Result<(Process, Launch)> {
    let process = Process::start(config).await?;
    let launch = config
        .launch(&process, segment, SEGMENTS, &spec(), async { Ok(()) })
        .await?;
    Ok((process, launch))
}

/// Hosts nothing.
#[derive(Deserialize)]
pub struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NAMES: &'static [&'static str] = &[];
}

/// The settings of the hub of the model pool `name`, a child of
/// [`GLOBAL`], which its peers' hubs reach under the name `hub`: `f` and
/// `d` each need one usable instance.
pub fn model_hub(name: &str, hub: &str) -> PoolSettings {
    toml::from_str(&format!(
        "name = '{name}'\nclass = 'model_x'\nregion = '{REGION}'\nhome = '{GLOBAL}'\n\
         hub = '{hub}'\ndirectory = 'memory'\n[ready]\nf = 1\nd = 1\n"
    ))
    .expect("valid test fixture")
}

/// The settings of the global pool's hub, reached under `global-hub:1337`.
pub fn global_hub() -> PoolSettings {
    toml::from_str(&format!(
        "name = '{GLOBAL}'\nclass = 'global'\nregion = '{REGION}'\nhub = 'global-hub:1337'\n\
         directory = 'memory'\n"
    ))
    .expect("valid test fixture")
}

/// A hold long enough to be a hold and short enough to wait out, and no
/// heartbeat within a test.
pub fn timing() -> PoolTiming {
    PoolTiming {
        hold: Duration::from_millis(300),
        heartbeat: Duration::from_secs(600),
        lease: Duration::from_secs(30),
        ..PoolTiming::default()
    }
}

/// A pool's hub of [`spec`]: admission and the pool's extension, its hub
/// name resolved in `directory` once it serves.
pub async fn pool_hub(
    pool: PoolSettings,
    directory: &Resolving,
) -> (Hub, Arc<PoolReadiness<Resolving>>) {
    pool_hub_of(&spec(), pool, directory).await
}

/// [`pool_hub`] for the deployment `spec`.
pub async fn pool_hub_of(
    spec: &DeploymentSpec,
    pool: PoolSettings,
    directory: &Resolving,
) -> (Hub, Arc<PoolReadiness<Resolving>>) {
    pool_hub_timed(spec, pool, directory, timing()).await
}

/// [`pool_hub_of`] paced by `timing`.
pub async fn pool_hub_timed(
    spec: &DeploymentSpec,
    pool: PoolSettings,
    directory: &Resolving,
    timing: PoolTiming,
) -> (Hub, Arc<PoolReadiness<Resolving>>) {
    let name = pool.hub.clone().expect("a pool's hub has a name");
    let (hub, readiness) = unresolved_pool_hub_of(spec, pool, directory, timing).await;
    directory.resolve(&name, &format!("127.0.0.1:{}", hub.public_addr().port()));
    (hub, readiness)
}

/// [`pool_hub_timed`], whose name resolves to a proxy in front of its
/// public listener: the hubs that follow the pool reach it through the
/// proxy, and its own instances, which are given its ports, do not.
pub async fn pool_hub_proxied(
    spec: &DeploymentSpec,
    pool: PoolSettings,
    directory: &Resolving,
    timing: PoolTiming,
) -> (Hub, Arc<PoolReadiness<Resolving>>, super::proxy::Proxy) {
    let name = pool.hub.clone().expect("a pool's hub has a name");
    let (hub, readiness) = unresolved_pool_hub_of(spec, pool, directory, timing).await;
    let proxy = super::proxy::Proxy::new(hub.public_addr()).await;
    directory.resolve(&name, &format!("127.0.0.1:{}", proxy.port));
    (hub, readiness, proxy)
}

/// [`pool_hub`], with its name left for the test to resolve: its record
/// is published only once the test has.
pub async fn unresolved_pool_hub(
    pool: PoolSettings,
    directory: &Resolving,
) -> (Hub, Arc<PoolReadiness<Resolving>>) {
    unresolved_pool_hub_of(&spec(), pool, directory, timing()).await
}

async fn unresolved_pool_hub_of(
    spec: &DeploymentSpec,
    pool: PoolSettings,
    directory: &Resolving,
    timing: PoolTiming,
) -> (Hub, Arc<PoolReadiness<Resolving>>) {
    let readiness = Arc::new(
        PoolReadiness::new(spec, pool, directory.clone(), timing).expect("valid test fixture"),
    );
    // A pool's hub runs the record service too, as `hub_builder` mounts it.
    let hub = Hub::builder(hub_config())
        .extension(GraphAdmission::new(spec.clone()))
        .extension(Arc::clone(&readiness))
        .extension(gdl_hub::KeyedRecords::new(1024).expect("a capacity"))
        .serve()
        .await
        .expect("the hub serves");
    (hub, readiness)
}

/// A directory that names each pool's hub as DNS would: a record's `hub`
/// is a name, which a test resolves to the address the hub serves at once
/// it serves, since a hub's port is assigned only then. A publish waits
/// until its hub's name is resolved, then stores the record with the
/// address in its place.
#[derive(Clone)]
pub struct Resolving {
    inner: MemoryDirectory,
    names: Arc<watch::Sender<HashMap<String, String>>>,
}

impl Resolving {
    pub fn new() -> Self {
        Self {
            inner: MemoryDirectory::new(),
            names: Arc::new(watch::Sender::new(HashMap::new())),
        }
    }

    /// The hub named `name` serves at `address`, `host:port`.
    pub fn resolve(&self, name: &str, address: &str) {
        self.names.send_modify(|names| {
            names.insert(name.to_string(), address.to_string());
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

/// One instance of a segment of [`spec`], registered with a hub as a
/// segment registers and announcing its phases by hand, on a velo node of
/// its own that answers `ping`.
pub struct Instance {
    client: HubClient,
    segment: &'static str,
    phases: HashMap<SegmentPhase, HubServiceRegistration>,
    node: Velo,
}

impl Instance {
    /// Join `hub` as an instance of `segment`.
    pub async fn join(hub: &Hub, segment: &'static str) -> Self {
        let node = gdl_runtime::bind_node("127.0.0.1:0".parse().expect("valid test fixture"))
            .await
            .expect("a velo node");
        let ping = gdl_runtime::velo::Handler::unary_handler_async(
            "ping".to_string(),
            |_: gdl_runtime::velo::Context| async { Ok(Some(b"pong".to_vec().into())) },
        )
        .build();
        node.register_handler(ping).expect("valid test fixture");
        Self::join_as(hub, segment, node).await
    }

    /// Join `hub` as an instance of `segment` too, on this instance's node:
    /// one velo instance that is a member of two hubs.
    pub async fn also(&self, hub: &Hub, segment: &'static str) -> Self {
        Self::join_as(hub, segment, self.node.clone()).await
    }

    async fn join_as(hub: &Hub, segment: &'static str, node: Velo) -> Self {
        let client = HubClient::builder()
            .host("127.0.0.1")
            .public_port(hub.public_addr().port())
            .control_port(hub.control_addr().port())
            .build()
            .expect("valid test fixture");
        let registration = spec().registration(segment).expect("valid test fixture");
        let payload = serde_json::to_value(&registration).expect("valid test fixture");
        client
            .register(&node, BTreeMap::from([("gdl".to_string(), payload)]))
            .await
            .expect("the hub admits the instance");
        Self {
            client,
            segment,
            phases: HashMap::new(),
            node,
        }
    }

    /// Join `hub` as an instance of `segment` that is ready.
    pub async fn ready(hub: &Hub, segment: &'static str) -> Self {
        let mut instance = Self::join(hub, segment).await;
        instance.announce(SegmentPhase::Ready).await;
        instance
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

    /// Leave the hub, as an instance whose lease ends does. Its node stays
    /// up, so that whether a peer can still reach it shows whether the peer
    /// still has it registered.
    pub async fn leave(self) -> Velo {
        self.client
            .unregister()
            .await
            .expect("the hub drops the instance");
        self.node
    }
}

/// Whether `from` reaches `instance` by its instance id with a `ping`: a
/// call by instance goes only to a peer `from` registered.
pub async fn pings(from: &Velo, instance: InstanceId) -> bool {
    let call = from
        .unary("ping")
        .expect("valid test fixture")
        .raw_payload(Vec::new().into())
        .instance(instance)
        .send();
    matches!(tokio::time::timeout(WAIT, call).await, Ok(Ok(_)))
}

/// The followed pool `pool` of `segment`, if its table holds it.
pub fn followed(pools: &Pools, segment: &str, pool: &str) -> Option<FollowedPool> {
    pools
        .followed(segment)
        .into_iter()
        .find(|followed| followed.pool == pool)
}

/// Wait until `done` holds of `pools`, checking at each change, and fail
/// with `what` and the tables after [`WAIT`].
pub async fn until(pools: &Pools, what: &str, mut done: impl FnMut(&Pools) -> bool) {
    let mut changes = pools.changes();
    let waited = tokio::time::timeout(WAIT, async {
        loop {
            changes.borrow_and_update();
            if done(pools) {
                return;
            }
            changes.changed().await.expect("the tables live");
        }
    })
    .await;
    if waited.is_err() {
        panic!("timed out waiting for {what}: {:?}", pools.followed("f"));
    }
}

/// The ids of `instances`, sorted as a table orders them.
pub fn sorted(instances: &[&Instance]) -> Vec<InstanceId> {
    let mut ids: Vec<InstanceId> = instances.iter().map(|instance| instance.id()).collect();
    ids.sort_by_key(|id| id.as_u128());
    ids
}
