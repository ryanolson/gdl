//! The two owners of a peer's velo registration, driven together: the
//! instance's membership follower, as it runs, and a chooser's follower of
//! other cells, on an explicit clock.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use gdl_cells::CellState;
use gdl_model::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use gdl_watch::FollowEvent;
use tokio::time::Instant;

use super::HubSession;
use crate::Roster;
use crate::dist::cells::{Cells, Damping, Follows};

const DRAIN: Duration = Duration::from_millis(300);

fn spec() -> DeploymentSpec {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let graph = Manifest {
        name: "drains-test".into(),
        schema: 1,
        nodes: vec![node("Source", Role::Source), node("Echo", Role::Sink)],
        hops: vec![HopInfo {
            outer: "Source".into(),
            inner: "Echo".into(),
            forward: vec![],
            backward: vec![],
            forward_types: vec![TypeInfo::new("T", true, Default::default())],
            backward_types: vec![TypeInfo::new("T", true, Default::default())],
        }],
    };
    let placement = Placement::new()
        .segment("front", ["Source"])
        .segment("sink", ["Echo"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// A velo node that answers `ping`.
async fn answering() -> velo::Velo {
    let node = crate::dist::bind_node("127.0.0.1:0".parse().expect("valid test fixture"))
        .await
        .expect("node");
    let ping = velo::Handler::unary_handler_async("ping".to_string(), |_: velo::Context| async {
        Ok(Some(b"pong".to_vec().into()))
    })
    .build();
    node.register_handler(ping).expect("valid test fixture");
    node
}

/// Whether `from` reaches `to` by its instance id: a call by instance goes
/// only to a peer `from` registered.
async fn pings(from: &velo::Velo, to: velo::InstanceId) -> bool {
    let call = from
        .unary("ping")
        .expect("valid test fixture")
        .raw_payload(Vec::new().into())
        .instance(to)
        .send();
    matches!(
        tokio::time::timeout(Duration::from_secs(5), call).await,
        Ok(Ok(_))
    )
}

/// A peer leaves a followed cell's table, and then this instance's own
/// membership: the chooser's drain ends first and leaves the peer
/// registered, and the membership's, which began later, unregisters it.
/// The two followers are the ones the runtime runs; the membership's is
/// driven by its own calls, so each step is applied before the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_later_drain_unregisters_a_peer_both_owners_held() {
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    let hub = velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("the hub serves");
    let front = crate::dist::bind_node("127.0.0.1:0".parse().expect("valid test fixture"))
        .await
        .expect("node");
    let session = HubSession::register(
        "127.0.0.1",
        hub.public_addr().port(),
        hub.control_addr().port(),
        &front,
        spec().registration("front").expect("valid test fixture"),
        Duration::from_secs(1),
    )
    .await
    .expect("registered");
    let live = Arc::clone(Roster::new().live());
    let cells = Cells::new("r", [("model_x", "sink")], Damping::default());
    let wanted = BTreeMap::from([("model_x".to_string(), BTreeSet::from(["sink".to_string()]))]);
    let mut chooser = Follows::new(
        cells.clone(),
        front.clone(),
        Arc::clone(&live),
        DRAIN,
        wanted,
        gdl::Load::FRESH_PERIODS,
    );
    let mut membership = session.follower(
        Arc::clone(&live),
        front.clone(),
        &["front", "sink"],
        &["sink"],
        DRAIN,
        Some(cells.clone()),
    );

    // The peer is an entry of a followed cell, and a member this instance
    // routes to.
    let peer = answering().await;
    let id = peer.instance_id();
    let start = Instant::now();
    for event in [
        FollowEvent::Follow {
            class: "model_x".into(),
            cell: "m1".into(),
            generation: 1,
            region: "r".into(),
            state: CellState::Ready,
            load: None,
            load_age_ms: None,
        },
        FollowEvent::Exposed {
            class: "model_x".into(),
            cell: "m1".into(),
            generation: 1,
            segment: "sink".into(),
            peer: peer.peer_info(),
        },
    ] {
        chooser.apply(event, start);
    }
    live.join("sink", id);
    assert!(pings(&front, id).await);

    // It leaves the cell's table, then the membership.
    chooser.apply(
        FollowEvent::Left {
            class: "model_x".into(),
            cell: "m1".into(),
            generation: 1,
            segment: "sink".into(),
            instance: id,
        },
        start,
    );
    membership.left("sink", id);
    let left = Instant::now();
    chooser.forget(start + DRAIN);
    assert!(!cells.is_registered(id), "the chooser kept what it gave up");
    assert!(
        pings(&front, id).await,
        "the chooser's drain cut the membership's short"
    );

    // The membership's drain ends, and its next tick unregisters the peer.
    tokio::time::sleep_until(left + DRAIN).await;
    membership.tick().await;
    assert!(!pings(&front, id).await, "no owner unregistered the peer");
    drop(membership);
    drop(session);
    hub.shutdown().await.expect("valid test fixture");
}
