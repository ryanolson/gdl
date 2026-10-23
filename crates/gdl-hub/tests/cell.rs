//! A cell's hub: the `gdl-cell` extension counts the usable instances of
//! each segment of the cell's class through its own hub, judges the cell's
//! state, serves it on the cell health endpoint, and publishes the cell's
//! record. Every hub here runs in process on OS-assigned ports, and every
//! wait is for something observed: a status, a record, a response.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use gdl_cells::{CellDirectory, CellEvent, CellRecord, CellState, MemoryDirectory, MemoryWatch};
use gdl_hub::{CellHealth, CellReadiness, CellTiming, GraphAdmission, SegmentHealth};
use gdl_model::{
    CellSettings, DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, SegmentPhase,
    TypeInfo, segment_service,
};
use velo_hub::client::{HubClient, HubServiceRegistration};
use velo_hub::velo::Velo;
use velo_hub::velo::discovery::ServiceRegistrationGuard;
use velo_hub::{Hub, HubConfig};

/// How long any one observation may take before the test fails.
const WAIT: Duration = Duration::from_secs(5);

/// `S => F { D }` in two classes: `global` runs `a` (S), and `model_x`
/// runs `f` (F) and `d` (D); the boundary is the inner end of S's hop.
fn spec() -> DeploymentSpec {
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

const CELL: &str = "model-x-01-us-east-1";

/// The model cell: `d` needs two usable instances, `f` one.
fn settings() -> CellSettings {
    toml::from_str(&format!(
        "name = '{CELL}'\nclass = 'model_x'\nregion = 'us-east-1'\nhub = 'model-x-01-hub:1337'\n\
         directory = 'memory'\n[ready]\nd = 2\n"
    ))
    .expect("valid test fixture")
}

/// A hold long enough to observe, short enough to wait out; no heartbeat
/// in the length of a test, so every record seen is a change of state.
fn timing() -> CellTiming {
    CellTiming {
        hold: Duration::from_millis(300),
        heartbeat: Duration::from_secs(600),
        lease: Duration::from_secs(30),
    }
}

/// A hub of the deployment with the cell's extension beside admission.
async fn cell_hub(
    directory: MemoryDirectory,
    timing: CellTiming,
) -> (Hub, Arc<CellReadiness<MemoryDirectory>>) {
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

fn config() -> HubConfig {
    let mut config = HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    config
}

/// One instance of a segment, registered with the hub as a segment
/// registers, and announcing its phases.
struct Instance {
    client: HubClient,
    segment: &'static str,
    phases: HashMap<SegmentPhase, HubServiceRegistration>,
    _node: Velo,
}

impl Instance {
    async fn join(hub: &Hub, segment: &'static str) -> Self {
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

    async fn announce(&mut self, phase: SegmentPhase) {
        let name = segment_service(self.segment, phase);
        let registration = self
            .client
            .register_service(&name)
            .await
            .expect("the hub takes the service");
        self.phases.insert(phase, registration);
    }

    async fn withdraw(&mut self, phase: SegmentPhase) {
        let mut registration = self.phases.remove(&phase).expect("announced");
        registration
            .unregister()
            .await
            .expect("the hub drops the service");
    }

    /// Leave the hub, as an instance whose lease ends does.
    async fn leave(self) {
        self.client
            .unregister()
            .await
            .expect("the hub drops the instance");
    }
}

/// Wait until the extension's health satisfies `ready`, and return it.
async fn health(
    cell: &CellReadiness<MemoryDirectory>,
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

fn usable(health: &CellHealth, segment: &str) -> u32 {
    health.segments[segment].usable
}

/// The usable instances of each segment, as the extension counts them,
/// once it has counted `d` at `count`.
async fn counted(cell: &CellReadiness<MemoryDirectory>, count: u32) -> CellHealth {
    health(cell, &format!("d at {count}"), |h| usable(h, "d") == count).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_ready_instances_that_have_not_retired_are_counted() {
    let (hub, cell) = cell_hub(MemoryDirectory::new(), timing()).await;

    // Ready counts.
    let mut first = Instance::join(&hub, "d").await;
    first.announce(SegmentPhase::Loaded).await;
    first.announce(SegmentPhase::Serving).await;
    first.announce(SegmentPhase::Ready).await;
    counted(&cell, 1).await;
    // Ready and retired does not.
    first.announce(SegmentPhase::Retired).await;
    counted(&cell, 0).await;

    // Serving alone does not: a second instance serves, and a third that
    // is ready is the only one counted.
    let mut serving = Instance::join(&hub, "d").await;
    serving.announce(SegmentPhase::Loaded).await;
    serving.announce(SegmentPhase::Serving).await;
    let mut third = Instance::join(&hub, "d").await;
    third.announce(SegmentPhase::Ready).await;
    let health = counted(&cell, 1).await;
    assert_eq!(usable(&health, "f"), 0, "f has no instance");
    // The retired one withdrawing `ready` changes nothing; the serving one
    // turning ready counts.
    first.withdraw(SegmentPhase::Ready).await;
    serving.announce(SegmentPhase::Ready).await;
    counted(&cell, 2).await;

    // One that leaves the hub stops counting.
    third.leave().await;
    counted(&cell, 1).await;
    serving.leave().await;
    counted(&cell, 0).await;
    hub.shutdown().await.expect("the hub stops");
}

/// The cell's records, as a reader of its class sees them.
struct Records {
    watch: MemoryWatch,
}

impl Records {
    fn new(directory: &MemoryDirectory) -> Self {
        Self {
            watch: directory.watch("model_x", None),
        }
    }

    /// The next record of the cell.
    async fn next(&mut self) -> CellRecord {
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
    async fn until(&mut self, state: CellState) -> (CellRecord, Instant) {
        loop {
            let record = self.next().await;
            if record.state == state {
                return (record, Instant::now());
            }
        }
    }
}

fn millis_now() -> u64 {
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
async fn meet_the_quorum(hub: &Hub) -> (Vec<Instance>, Instant) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cell_publishes_its_record_and_each_change_of_state() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let before = millis_now();
    let (hub, cell) = cell_hub(directory.clone(), timing()).await;

    // Published at once, before any instance: unavailable.
    let first = records.next().await;
    let layout = gdl_model::layout(&spec().graph, &spec().placement).expect("valid test fixture");
    assert_eq!(
        first,
        CellRecord {
            name: CELL.into(),
            class: "model_x".into(),
            region: "us-east-1".into(),
            contract: layout.contract_hash(),
            placement: layout.placement_hash(),
            state: CellState::Unavailable,
            epoch: first.epoch,
            seq: first.seq,
            hub: "model-x-01-hub:1337".into(),
            lease: Duration::from_secs(30),
            load: None,
        }
    );
    assert!(first.epoch >= before, "the epoch is the start's time");

    // Ready only once the quorum has held for the hold, degraded meanwhile.
    let (mut instances, began) = meet_the_quorum(&hub).await;
    let (held, _) = records.until(CellState::Degraded).await;
    let (ready, at) = records.until(CellState::Ready).await;
    assert!(
        at.duration_since(began) >= timing().hold,
        "ready {:?} after the quorum was met",
        at.duration_since(began)
    );
    assert_eq!(ready.epoch, first.epoch, "one epoch for the hub's life");
    assert!(ready.seq > held.seq && held.seq > first.seq);
    assert_eq!(
        health(&cell, "ready", |h| h.state == CellState::Ready)
            .await
            .segments,
        BTreeMap::from([
            (
                "d".into(),
                SegmentHealth {
                    usable: 2,
                    quorum: 2
                }
            ),
            (
                "f".into(),
                SegmentHealth {
                    usable: 1,
                    quorum: 1
                }
            ),
        ])
    );

    // A drop is published at once: no heartbeat falls in this test.
    let last = instances.pop().expect("a d instance");
    last.leave().await;
    assert_eq!(records.next().await.state, CellState::Degraded);
    let f = instances.remove(0);
    f.leave().await;
    assert_eq!(records.next().await.state, CellState::Unavailable);
    hub.shutdown().await.expect("the hub stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_record_is_republished_on_the_heartbeat() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let timing = CellTiming {
        heartbeat: Duration::from_millis(100),
        ..timing()
    };
    let (hub, _cell) = cell_hub(directory.clone(), timing).await;
    let first = records.next().await;
    for _ in 0..3 {
        let beat = records.next().await;
        assert_eq!(beat.state, CellState::Unavailable, "no change of state");
        assert_eq!(beat.epoch, first.epoch);
        assert!(beat.seq > first.seq);
    }
    hub.shutdown().await.expect("the hub stops");
}

/// A hub that starts again for the same cell takes a later epoch: later
/// than its predecessor's, and later than any the directory holds, even
/// one ahead of this host's clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_that_restarts_publishes_with_a_later_epoch() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, _cell) = cell_hub(directory.clone(), timing()).await;
    let before = records.next().await;
    hub.shutdown().await.expect("the hub stops");
    let (hub, _cell) = cell_hub(directory.clone(), timing()).await;
    let after = records.next().await;
    assert!(after.epoch > before.epoch, "{after:?} after {before:?}");
    hub.shutdown().await.expect("the hub stops");

    // A predecessor whose clock ran an hour ahead.
    let ahead = CellRecord {
        epoch: millis_now() + 3_600_000,
        seq: 1,
        ..after.clone()
    };
    directory.publish(ahead.clone()).await.expect("stored");
    assert_eq!(records.next().await, ahead);
    let (hub, _cell) = cell_hub(directory.clone(), timing()).await;
    let next = records.next().await;
    assert!(next.epoch > ahead.epoch, "{next:?} after {ahead:?}");
    hub.shutdown().await.expect("the hub stops");
}

/// A write the directory refuses as not newer is reported, and the next
/// write moves past what the directory holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_write_is_passed_by_the_next() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let timing = CellTiming {
        heartbeat: Duration::from_millis(100),
        ..timing()
    };
    let (hub, _cell) = cell_hub(directory.clone(), timing).await;
    let first = records.next().await;
    let ahead = CellRecord {
        seq: first.seq + 100,
        ..first.clone()
    };
    directory.publish(ahead.clone()).await.expect("stored");
    loop {
        let record = records.next().await;
        if record.seq > ahead.seq {
            assert_eq!(record.epoch, first.epoch);
            break;
        }
    }
    hub.shutdown().await.expect("the hub stops");
}

/// `GET` on the hub's public listener: the status and the body.
async fn get(hub: &Hub, path: &str) -> (u16, String) {
    let url = format!("http://127.0.0.1:{}{path}", hub.public_addr().port());
    let response = tokio::time::timeout(WAIT, reqwest::get(&url))
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    let status = response.status().as_u16();
    (status, response.text().await.expect("a body"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cell_health_endpoint_answers_ready_only_once_the_cell_is() {
    let (hub, cell) = cell_hub(MemoryDirectory::new(), timing()).await;
    let (status, body) = get(&hub, "/v1/ext/gdl-cell/health").await;
    assert_eq!(status, 503, "{body}");
    let body: serde_json::Value = serde_json::from_str(&body).expect("JSON");
    assert_eq!(
        body,
        serde_json::json!({
            "state": "unavailable",
            "segments": {
                "d": { "usable": 0, "quorum": 2 },
                "f": { "usable": 0, "quorum": 1 },
            },
        })
    );
    let (_instances, _) = meet_the_quorum(&hub).await;
    health(&cell, "ready", |h| h.state == CellState::Ready).await;
    let (status, body) = get(&hub, "/v1/ext/gdl-cell/health").await;
    assert_eq!(status, 200, "{body}");
    let body: CellHealth = serde_json::from_str(&body).expect("JSON");
    assert_eq!(body.state, CellState::Ready);
    assert_eq!(usable(&body, "d"), 2);
    hub.shutdown().await.expect("the hub stops");
}

/// A hub given no cell settings runs as before: admission alone, and no
/// cell health endpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_without_a_cell_has_no_cell_endpoint() {
    let hub = Hub::builder(config())
        .extension(GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("the hub serves");
    let (status, _) = get(&hub, "/v1/ext/gdl-cell/health").await;
    assert_eq!(status, 404);
    let instance = Instance::join(&hub, "d").await;
    let (status, members) = get(&hub, "/v1/ext/gdl/members").await;
    assert_eq!(status, 200);
    assert!(members.contains("\"segment\":\"d\""), "{members}");
    instance.leave().await;
    hub.shutdown().await.expect("the hub stops");
}

/// Settings that cannot publish a well-formed record of this deployment's
/// cell are refused when the extension is made, not on every heartbeat.
#[test]
fn settings_a_cells_hub_cannot_publish_are_refused() {
    type Change = fn(&mut CellSettings);
    let cases: [(&str, Change, CellTiming); 6] = [
        ("[cell] hub", |cell| cell.hub = None, timing()),
        (
            "the placement has no class `model_y`",
            |cell| cell.class = "model_y".into(),
            timing(),
        ),
        (
            "`a` is not a segment of class `model_x`",
            |cell| {
                cell.ready.insert("a".into(), 1);
            },
            timing(),
        ),
        (
            "cell name `Model_X`",
            |cell| cell.name = "Model_X".into(),
            timing(),
        ),
        (
            "the lease",
            |_| {},
            CellTiming {
                lease: Duration::ZERO,
                ..timing()
            },
        ),
        (
            "heartbeat",
            |_| {},
            CellTiming {
                heartbeat: Duration::ZERO,
                ..timing()
            },
        ),
    ];
    for (expected, change, timing) in cases {
        let mut cell = settings();
        change(&mut cell);
        let Err(error) = CellReadiness::new(&spec(), cell, MemoryDirectory::new(), timing) else {
            panic!("accepted settings without {expected}");
        };
        assert!(error.to_string().contains(expected), "{error}");
    }
}
