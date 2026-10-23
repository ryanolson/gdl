//! A deployment of two classes for the cell tests, and its instances'
//! settings: `global` runs `a` (S, the chooser), and `model_x` runs `f`
//! (F, the cell entry) and `d` (D). The boundary is the inner end of S's
//! hop.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use figment::providers::Format as _;
use gdl_cells::{
    CellDirectory, CellRecord, DirectoryError, MemoryDirectory, MemoryWatch, ResumeToken,
};
use gdl_hub::{CellReadiness, CellTiming, GraphAdmission};
use gdl_model::{CellSettings, SegmentPhase, segment_service};
use gdl_runtime::velo::{InstanceId, Velo};
use gdl_runtime::{
    Cells, DeploymentSpec, FollowedCell, HopInfo, Hosted, Launch, Manifest, NodeInfo, Placement,
    Process, ProcessConfig, Role, TypeInfo,
};
use serde::Deserialize;
use tokio::sync::watch;
use velo_hub::Hub;
use velo_hub::client::{HubClient, HubServiceRegistration};

use super::WAIT;

/// Every segment of [`spec`].
pub const SEGMENTS: &[&str] = &["a", "f", "d"];

/// The region every cell of these tests runs in: a global cell follows
/// only the model cells of its own region.
pub const REGION: &str = "us-east-1";

/// The global cell.
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
        forward_types: vec![TypeInfo::new("T", true)],
        backward_types: vec![TypeInfo::new("T", true)],
    };
    let graph = Manifest {
        name: "cells".into(),
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

/// The `[cell]` table of an instance of the global cell.
pub fn global_cell() -> String {
    format!(
        "[cell]\nname = '{GLOBAL}'\nclass = 'global'\nregion = '{REGION}'\ndirectory = 'memory'\n"
    )
}

/// The `[cell]` table of an instance of the model cell `name`.
pub fn model_cell(name: &str) -> String {
    format!(
        "[cell]\nname = '{name}'\nclass = 'model_x'\nregion = '{REGION}'\ndirectory = 'memory'\n"
    )
}

/// The settings of an instance at the hub on these ports, heartbeating
/// every 200 ms, waiting for its peers for `rendezvous_ms`, with `extra`
/// appended (a `[cell]` table, `[min_replicas]`, `[runtime]`).
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
/// cell's extension.
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

/// The settings of the hub of the model cell `name`, which its peers' hubs
/// reach under the name `hub`: `f` and `d` each need one usable instance.
pub fn model_hub(name: &str, hub: &str) -> CellSettings {
    toml::from_str(&format!(
        "name = '{name}'\nclass = 'model_x'\nregion = '{REGION}'\nhub = '{hub}'\n\
         directory = 'memory'\n[ready]\nf = 1\nd = 1\n"
    ))
    .expect("valid test fixture")
}

/// The settings of the global cell's hub, reached under `global-hub:1337`.
pub fn global_hub() -> CellSettings {
    toml::from_str(&format!(
        "name = '{GLOBAL}'\nclass = 'global'\nregion = '{REGION}'\nhub = 'global-hub:1337'\n\
         directory = 'memory'\n"
    ))
    .expect("valid test fixture")
}

/// A hold long enough to be a hold and short enough to wait out, and no
/// heartbeat within a test.
pub fn timing() -> CellTiming {
    CellTiming {
        hold: Duration::from_millis(300),
        heartbeat: Duration::from_secs(600),
        lease: Duration::from_secs(30),
        ..CellTiming::default()
    }
}

/// A cell's hub of [`spec`]: admission and the cell's extension, its hub
/// name resolved in `directory` once it serves.
pub async fn cell_hub(
    cell: CellSettings,
    directory: &Resolving,
) -> (Hub, Arc<CellReadiness<Resolving>>) {
    let name = cell.hub.clone().expect("a cell's hub has a name");
    let readiness = Arc::new(
        CellReadiness::new(&spec(), cell, directory.clone(), timing()).expect("valid test fixture"),
    );
    let hub = Hub::builder(hub_config())
        .extension(GraphAdmission::new(spec()))
        .extension(Arc::clone(&readiness))
        .serve()
        .await
        .expect("the hub serves");
    directory.resolve(&name, &format!("127.0.0.1:{}", hub.public_addr().port()));
    (hub, readiness)
}

/// A directory that names each cell's hub as DNS would: a record's `hub`
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

impl CellDirectory for Resolving {
    type Watch = MemoryWatch;

    async fn publish(&self, mut record: CellRecord) -> Result<(), DirectoryError> {
        let mut names = self.names.subscribe();
        let address = names
            .wait_for(|names| names.contains_key(&record.hub))
            .await
            .expect("the names live")[&record.hub]
            .clone();
        record.hub = address;
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: gdl_cells::Withdrawal) -> Result<(), DirectoryError> {
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

/// The followed cell `cell` of `segment`, if its table holds it.
pub fn followed(cells: &Cells, segment: &str, cell: &str) -> Option<FollowedCell> {
    cells
        .followed(segment)
        .into_iter()
        .find(|followed| followed.cell == cell)
}

/// Wait until `done` holds of `cells`, checking at each change, and fail
/// with `what` and the tables after [`WAIT`].
pub async fn until(cells: &Cells, what: &str, mut done: impl FnMut(&Cells) -> bool) {
    let mut changes = cells.changes();
    let waited = tokio::time::timeout(WAIT, async {
        loop {
            changes.borrow_and_update();
            if done(cells) {
                return;
            }
            changes.changed().await.expect("the tables live");
        }
    })
    .await;
    if waited.is_err() {
        panic!("timed out waiting for {what}: {:?}", cells.followed("f"));
    }
}

/// The ids of `instances`, sorted as a table orders them.
pub fn sorted(instances: &[&Instance]) -> Vec<InstanceId> {
    let mut ids: Vec<InstanceId> = instances.iter().map(|instance| instance.id()).collect();
    ids.sort_by_key(|id| id.as_u128());
    ids
}
