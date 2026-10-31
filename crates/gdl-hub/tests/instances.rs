//! A pool hub's list of its members: each one's segment, velo peer,
//! furthest phase, and, for a usable entry instance, its latest load
//! report, read from what the hub holds when it is asked. Instances here
//! register and report by hand, as the runtime does. Every hub runs in
//! process on OS-assigned ports; every wait is for something observed.

mod common;

use std::collections::HashMap;

use common::{Instance, WAIT, control_get, hub_of, model};
use futures::StreamExt;
use gdl_hub::PoolWatch;
use gdl_model::SegmentPhase;
use gdl_pools::MemoryDirectory;
use gdl_watch::{
    INSTANCES_ROUTE, InstanceLoad, InstancePhase, Instances, LoadReport, PoolInstance,
    PoolWatchEvent,
};
use velo_hub::velo::{InstanceId, PeerInfo};

fn report(admitted: u64, limit: u64, refusals: u64) -> LoadReport {
    LoadReport {
        dropped: Vec::new(),
        properties: gdl_model::PropertyLists::new(),
        admitted,
        limit,
        refusals,
    }
}

/// A watch of the pool, its snapshot read.
async fn watch(pool: &gdl_hub::PoolReadiness<MemoryDirectory>) -> PoolWatch {
    let mut watch = tokio::time::timeout(WAIT, pool.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    let synced = tokio::time::timeout(WAIT, async {
        while watch.next().await.expect("the watch goes on") != PoolWatchEvent::Synced {}
    })
    .await;
    assert!(synced.is_ok(), "no snapshot in time");
    watch
}

/// Read `watch` until `wanted` says it saw what it waits for.
async fn until(watch: &mut PoolWatch, what: &str, mut wanted: impl FnMut(&PoolWatchEvent) -> bool) {
    let waited = tokio::time::timeout(WAIT, async {
        while !wanted(&watch.next().await.expect("the watch goes on")) {}
    })
    .await;
    assert!(waited.is_ok(), "no {what} in time");
}

/// The hub's list, by instance.
async fn list(hub: &velo_hub::Hub) -> HashMap<InstanceId, PoolInstance> {
    let (status, body) = control_get(hub, INSTANCES_ROUTE).await;
    assert_eq!(status, 200, "{body}");
    let listed: Instances = serde_json::from_str(&body).expect("an instance list");
    listed
        .instances
        .into_iter()
        .map(|instance| (instance.peer.instance_id, instance))
        .collect()
}

/// Whether two peers are one: the same instance, with the same entry for
/// each transport. Two encodings of one address can order the transports
/// differently.
fn same_peer(listed: &PeerInfo, registered: &PeerInfo) -> bool {
    let entries = |peer: &PeerInfo| {
        let address = peer.worker_address();
        let mut keys = address
            .available_transports()
            .expect("a well-formed address");
        keys.sort();
        keys.into_iter()
            .map(|key| {
                let entry = address.get_entry(&key).expect("a well-formed address");
                (key, entry)
            })
            .collect::<Vec<_>>()
    };
    listed.instance_id == registered.instance_id && entries(listed) == entries(registered)
}

/// The figures of a load, without its age.
fn figures(load: Option<InstanceLoad>) -> Option<(u64, u64, u64)> {
    load.map(|load| (load.admitted, load.limit, load.refusals))
}

/// Two ready entry instances that report different loads are listed with
/// their own reports; an entry instance that retired is listed `retired`,
/// without the report it made before; one that has not announced `ready`
/// is `joining`; one that announced `drained` after `retired` is
/// `drained`, and one of a draining pool that detached is `detached`; an
/// interior instance is listed without a load; and each with its segment,
/// the peer it registered with, and the address its velo node listens on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_hub_lists_its_members_with_their_phase_and_latest_load() {
    let (hub, readiness) = hub_of(
        model(common::POOL, "us-east-1", "model-x-01-hub:1337"),
        MemoryDirectory::new(),
        common::timing(),
    )
    .await;
    let mut watch = watch(&readiness).await;
    let mut entries = Vec::new();
    for _ in 0..3 {
        let mut instance = Instance::join(&hub, "f").await;
        instance.announce(SegmentPhase::Ready).await;
        entries.push(instance);
    }
    let mut ready = 0;
    until(&mut watch, "three ready entry instances", |event| {
        ready += usize::from(matches!(event, PoolWatchEvent::Ready { .. }));
        ready == 3
    })
    .await;
    let [a, b, mut c] = <[Instance; 3]>::try_from(entries).ok().expect("three");
    let mut interior = Instance::join(&hub, "d").await;
    interior.announce(SegmentPhase::Ready).await;
    let joining = Instance::join(&hub, "f").await;
    let mut detached = Instance::join(&hub, "d").await;
    detached.announce(SegmentPhase::Ready).await;
    detached.announce(SegmentPhase::Detached).await;

    a.report(report(3, 8, 1)).await.expect("the hub takes it");
    b.report(report(1, 8, 0)).await.expect("the hub takes it");
    c.report(report(5, 8, 0)).await.expect("the hub takes it");
    c.announce(SegmentPhase::Retired).await;
    let mut drained = Instance::join(&hub, "f").await;
    drained.announce(SegmentPhase::Retired).await;
    drained.announce(SegmentPhase::Drained).await;
    let retired = c.id();
    until(
        &mut watch,
        "the retirement",
        |event| matches!(event, PoolWatchEvent::Retired { instance, .. } if *instance == retired),
    )
    .await;

    let listed = list(&hub).await;
    assert_eq!(listed.len(), 7, "{listed:?}");
    let entry = |instance: &Instance| &listed[&instance.id()];
    assert_eq!(figures(entry(&a).load), Some((3, 8, 1)));
    assert_eq!(figures(entry(&b).load), Some((1, 8, 0)));
    let age = entry(&a).load.expect("a's report").age_ms;
    assert!(
        u128::from(age) <= WAIT.as_millis(),
        "a report taken in this test is {age} ms old"
    );
    for instance in [&a, &b] {
        assert_eq!(entry(instance).phase, InstancePhase::Ready);
    }
    assert_eq!(entry(&c).phase, InstancePhase::Retired);
    assert_eq!(entry(&c).load, None, "a retired instance's report is shown");
    assert_eq!(entry(&joining).phase, InstancePhase::Joining);
    assert_eq!(entry(&joining).load, None);
    assert_eq!(entry(&interior).phase, InstancePhase::Ready);
    assert_eq!(
        entry(&interior).load,
        None,
        "an interior instance has a load"
    );
    assert_eq!(entry(&detached).phase, InstancePhase::Detached);
    assert_eq!(entry(&drained).phase, InstancePhase::Drained);
    for (instance, segment) in [(&a, "f"), (&c, "f"), (&joining, "f"), (&interior, "d")] {
        assert_eq!(entry(instance).segment, segment);
        assert_eq!(entry(instance).addresses, [instance.listening()]);
        assert!(
            same_peer(&entry(instance).peer, &instance.peer()),
            "{:?} is not {:?}",
            entry(instance).peer,
            instance.peer()
        );
    }
    hub.shutdown().await.expect("the hub stops");
}

/// A transport's key can differ from its protocol name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "F1: TCP addresses under a custom transport key are omitted"]
async fn tcp_addresses_under_a_custom_transport_key_are_listed() {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use velo_hub::client::HubClient;
    use velo_hub::velo::Velo;
    use velo_hub::velo::transports::tcp::TcpTransportBuilder;

    let (hub, _) = hub_of(
        model(common::POOL, "us-east-1", "model-x-01-hub:1337"),
        MemoryDirectory::new(),
        common::timing(),
    )
    .await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let listening = listener.local_addr().expect("a bound port");
    let tcp = TcpTransportBuilder::new()
        .key("pool-tcp".into())
        .from_listener(listener)
        .expect("a listener")
        .build()
        .expect("a TCP transport");
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
        .expect("a hub client");
    let registration = common::spec().registration("f").expect("segment f");
    client
        .register_peer(
            node.peer_info(),
            BTreeMap::from([(
                "gdl".to_string(),
                serde_json::to_value(registration).expect("a registration"),
            )]),
        )
        .await
        .expect("the hub admits the instance");

    let listed = list(&hub).await;
    assert_eq!(listed.len(), 1);
    let instance = &listed[&node.instance_id()];
    assert!(same_peer(&instance.peer, &node.peer_info()));
    assert_eq!(instance.addresses, [listening]);
    hub.shutdown().await.expect("the hub stops");
}
