//! The fixtures the cell hub's tests share: a deployment of two classes,
//! its model cell, a hub of it, instances registered as segments register,
//! and the cell's records as a reader sees them.

// Each test target uses a part of these.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use gdl_cells::{CellDirectory, CellEvent, CellRecord, CellState, MemoryDirectory, MemoryWatch};
use gdl_hub::{CellHealth, CellReadiness, CellTiming, GraphAdmission};
use gdl_model::{
    CellSettings, DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, SegmentPhase,
    TypeInfo, segment_service,
};
use velo_hub::client::{HubClient, HubServiceRegistration};
use velo_hub::velo::Velo;
use velo_hub::velo::discovery::ServiceRegistrationGuard;
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

pub const CELL: &str = "model-x-01-us-east-1";

/// The model cell: `d` needs two usable instances, `f` one.
pub fn settings() -> CellSettings {
    toml::from_str(&format!(
        "name = '{CELL}'\nclass = 'model_x'\nregion = 'us-east-1'\nhub = 'model-x-01-hub:1337'\n\
         directory = 'memory'\n[ready]\nd = 2\n"
    ))
    .expect("valid test fixture")
}

/// A hold long enough to observe, short enough to wait out; no heartbeat
/// in the length of a test, so every record seen is a change of state.
pub fn timing() -> CellTiming {
    CellTiming {
        hold: Duration::from_millis(300),
        heartbeat: Duration::from_secs(600),
        lease: Duration::from_secs(30),
    }
}

/// A hub of the deployment with the cell's extension beside admission.
pub async fn cell_hub<D: CellDirectory>(
    directory: D,
    timing: CellTiming,
) -> (Hub, Arc<CellReadiness<D>>) {
    let cell = Arc::new(
        CellReadiness::new(&spec(), settings(), directory, timing).expect("valid test fixture"),
    );
    let hub = Hub::builder(config())
        .extension(GraphAdmission::new(spec()))
        .extension(Arc::clone(&cell))
        .serve()
        .await
        .expect("the hub serves");
    (hub, cell)
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
    _node: Velo,
}

impl Instance {
    pub async fn join(hub: &Hub, segment: &'static str) -> Self {
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
            _node: node,
        }
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

    /// Leave the hub, as an instance whose lease ends does.
    pub async fn leave(self) {
        self.client
            .unregister()
            .await
            .expect("the hub drops the instance");
    }
}

/// Wait until the extension's health satisfies `ready`, and return it.
pub async fn health<D: CellDirectory>(
    cell: &CellReadiness<D>,
    what: &str,
    ready: impl FnMut(&CellHealth) -> bool,
) -> CellHealth {
    let mut status = cell.health();
    let waited = tokio::time::timeout(WAIT, async {
        status.wait_for(ready).await.map(|health| health.clone())
    })
    .await;
    match waited {
        Ok(health) => health.expect("the extension runs"),
        Err(_) => panic!("timed out waiting for {what}: {:?}", *status.borrow()),
    }
}

pub fn usable(health: &CellHealth, segment: &str) -> u32 {
    health.segments[segment].usable
}

/// The cell's records, as a reader of its class sees them.
pub struct Records {
    watch: MemoryWatch,
}

impl Records {
    pub fn new(directory: &MemoryDirectory) -> Self {
        Self {
            watch: directory.watch("model_x", None),
        }
    }

    /// The next record of the cell.
    pub async fn next(&mut self) -> CellRecord {
        let next = async {
            loop {
                match self.watch.next().await {
                    Some(Ok(CellEvent::Current(record) | CellEvent::Put { record, .. }))
                        if record.name == CELL =>
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

    /// The next record of the cell in `state`, and when it was seen.
    pub async fn until(&mut self, state: CellState) -> (CellRecord, Instant) {
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

/// Bring `f` to one ready instance and `d` to two: the cell's quorum.
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
