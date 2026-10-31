use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use gdl_hub::{GraphAdmission, PoolReadiness, PoolTiming};
use gdl_model::{
    DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, PoolSettings, Role, TypeInfo,
};
use gdl_pools::{MemoryDirectory, PoolDirectory, PoolEvent, PoolRecord, PoolState};
use gdl_watch::{FollowEvent, PoolWatcher, UnknownFollowEvent, WatchEnd, WatchStream, Watched};
use tokio::time::Instant;
use velo_hub::{Hub, HubConfig};

use super::{follower, read};

const POOL: &str = "model-x-01-us-east-1";
const GLOBAL: &str = "global-01-us-east-1";
const WAIT: Duration = Duration::from_secs(5);

fn spec() -> DeploymentSpec {
    let graph = Manifest {
        name: "epoch".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "F".into(),
                role: Role::Sink,
                binding: Some("f".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            inner: "F".into(),
            forward: vec![],
            backward: vec![],
            forward_types: vec![TypeInfo::new("T", true, Default::default())],
            backward_types: vec![TypeInfo::new("T", true, Default::default())],
        }],
        properties: vec![],
    };
    DeploymentSpec::new(
        graph,
        Placement::new()
            .segment("a", ["S"])
            .segment("f", ["F"])
            .class("global", ["a"])
            .class("model_x", ["f"]),
    )
    .expect("a deployment")
}

async fn serve(spec: &DeploymentSpec, directory: &MemoryDirectory, model: bool) -> Hub {
    let settings = if model {
        format!(
            "name = '{POOL}'\nclass = 'model_x'\nregion = 'us-east-1'\nhome = '{GLOBAL}'\nhub = '127.0.0.1:1'\ndirectory = 'memory'\n"
        )
    } else {
        format!(
            "name = '{GLOBAL}'\nclass = 'global'\nregion = 'us-east-1'\nhub = '127.0.0.1:1'\ndirectory = 'memory'\n"
        )
    };
    let pool: PoolSettings = toml::from_str(&settings).expect("settings");
    let readiness = Arc::new(
        PoolReadiness::new(
            spec,
            pool,
            directory.clone(),
            PoolTiming {
                hold: Duration::from_millis(20),
                heartbeat: Duration::from_secs(600),
                lease: Duration::from_secs(30),
                ..PoolTiming::default()
            },
        )
        .expect("a pool"),
    );
    let mut config = HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    Hub::builder(config)
        .extension(GraphAdmission::new(spec.clone()))
        .extension(readiness)
        .serve()
        .await
        .expect("the hub serves")
}

async fn ready(
    hub: &Hub,
    spec: &DeploymentSpec,
) -> (
    velo::Velo,
    velo_hub::client::HubClient,
    velo_hub::client::HubServiceRegistration,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let tcp = velo::transports::tcp::TcpTransportBuilder::new()
        .from_listener(listener)
        .expect("a listener")
        .build()
        .expect("a transport");
    let node = velo::Velo::builder()
        .add_transport(Arc::new(tcp))
        .mux_only()
        .build()
        .await
        .expect("a node");
    let client = velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("a client");
    let registration =
        serde_json::to_value(spec.registration("f").expect("registration")).expect("JSON");
    client
        .register(&node, BTreeMap::from([("gdl".into(), registration)]))
        .await
        .expect("registered");
    let service = client
        .register_service(&gdl_model::segment_service(
            "f",
            gdl_model::SegmentPhase::Ready,
        ))
        .await
        .expect("ready");
    (node, client, service)
}

async fn record(directory: &MemoryDirectory, past: u64) -> PoolRecord {
    tokio::time::timeout(WAIT, async {
        let mut watch = directory.watch("model_x", None);
        loop {
            match watch
                .next()
                .await
                .expect("the directory lives")
                .expect("an event")
            {
                PoolEvent::Current(record) | PoolEvent::Put { record, .. }
                    if record.name == POOL
                        && record.epoch > past
                        && record.state == PoolState::Ready =>
                {
                    return record;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("a ready record")
}

type Events = WatchStream<FollowEvent, UnknownFollowEvent>;

async fn next(events: &mut Events) -> FollowEvent {
    match tokio::time::timeout(WAIT, events.next())
        .await
        .expect("an event in time")
        .expect("the watch lives")
        .expect("a valid event")
    {
        Watched::Event(event) => event,
        Watched::Unknown(_) => panic!("a known event"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "F2: a live follow on a pool's own watch installs the successor row before the whole relay's listing supplies its epoch"]
async fn a_reconnected_own_watch_dates_a_live_successor_before_the_relay_catches_up() {
    let spec = spec();
    let model_directory = MemoryDirectory::new();
    let relay_directory = MemoryDirectory::new();
    let predecessor = serve(&spec, &model_directory, true).await;
    let _entry = ready(&predecessor, &spec).await;
    let mut before = record(&model_directory, 0).await;
    before.hub = format!("127.0.0.1:{}", predecessor.public_addr().port());
    relay_directory
        .publish(before.clone())
        .await
        .expect("the relay sees the record");
    let relay = serve(&spec, &relay_directory, false).await;
    let address = format!("127.0.0.1:{}", relay.public_addr().port());
    let watcher = PoolWatcher::new().expect("a watcher");
    let mut all = watcher
        .follows(&address, None)
        .await
        .expect("the relay watch");
    let (mut follows, pools, _, _) = follower();
    let mut snapshot = follows.snapshot();
    loop {
        let event = next(&mut all).await;
        let synced = matches!(event, FollowEvent::Synced { .. });
        snapshot.apply(event, Instant::now());
        if synced {
            break;
        }
    }
    follows.resnapshot(snapshot, Instant::now());
    while super::super::table(&pools, POOL).is_none() {
        follows.take(next(&mut all).await, Instant::now());
    }
    assert_eq!(read(&pools, POOL), (false, Some(before.epoch)));
    let generation = super::super::table(&pools, POOL)
        .expect("followed")
        .generation;
    follows.unknown(UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        pool: POOL.into(),
        generation,
    });
    let first = follows.asked().pop().expect("a recovery watch");
    let mut own = watcher
        .follows_of(&address, "model_x", POOL)
        .await
        .expect("a pool watch");
    loop {
        let event = next(&mut own).await;
        let synced = matches!(event, FollowEvent::Synced { .. });
        follows.own(
            first.key.clone(),
            first.watch,
            Ok(Watched::Event(event)),
            Instant::now(),
        );
        if synced {
            break;
        }
    }
    drop(own);
    follows.own(
        first.key.clone(),
        first.watch,
        Err(WatchEnd::Closed),
        Instant::now(),
    );
    let reconnect = follows.asked().pop().expect("a replacement watch");
    let mut own = watcher
        .follows_of(&address, "model_x", POOL)
        .await
        .expect("the reconnected watch");
    loop {
        let event = next(&mut own).await;
        let synced = matches!(event, FollowEvent::Synced { .. });
        follows.own(
            reconnect.key.clone(),
            reconnect.watch,
            Ok(Watched::Event(event)),
            Instant::now(),
        );
        if synced {
            break;
        }
    }

    // The relay socket is delayed while the reconnected pool socket reads on.
    let successor = serve(&spec, &model_directory, true).await;
    let _successor_entry = ready(&successor, &spec).await;
    let mut after = record(&model_directory, before.epoch).await;
    assert!(
        after.epoch > before.epoch,
        "the successor owns a later epoch"
    );
    after.hub = format!("127.0.0.1:{}", successor.public_addr().port());
    relay_directory
        .publish(after.clone())
        .await
        .expect("the later record");
    loop {
        let event = next(&mut own).await;
        if let FollowEvent::Follow {
            generation: fresh,
            listing,
            ..
        } = &event
            && *fresh > generation
        {
            assert!(listing.is_none(), "a live follow carries no listing");
        }
        follows.own(
            reconnect.key.clone(),
            reconnect.watch,
            Ok(Watched::Event(event)),
            Instant::now(),
        );
        if super::super::table(&pools, POOL)
            .is_some_and(|row| row.generation > generation && !row.eligible.is_empty())
        {
            break;
        }
    }
    let row = super::super::table(&pools, POOL).expect("the successor row");
    assert!(row.generation > generation);
    assert_eq!(row.state, PoolState::Ready);
    let observed = read(&pools, POOL);

    // The hub sent the listing first, but on the other socket.
    let mut listed_first = false;
    loop {
        let event = next(&mut all).await;
        let done =
            matches!(&event, FollowEvent::Follow { generation: fresh, .. } if *fresh > generation);
        match &event {
            FollowEvent::Listed {
                listing: Some(listing),
                ..
            } if listing.epoch == after.epoch => listed_first = true,
            _ => {}
        }
        follows.take(event, Instant::now());
        if done {
            break;
        }
    }
    assert!(
        listed_first,
        "the hub orders the full relay watch correctly"
    );
    assert_eq!(
        read(&pools, POOL),
        (false, Some(after.epoch)),
        "the delayed listing repairs the epoch once it arrives"
    );
    for hub in [successor, predecessor, relay] {
        hub.shutdown().await.expect("the hub stops");
    }
    assert_eq!(
        observed,
        (false, Some(after.epoch)),
        "a live successor row must not read the predecessor's epoch after an own-watch reconnect"
    );
}
