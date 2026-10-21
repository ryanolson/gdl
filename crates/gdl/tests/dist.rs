//! The reference graph split across velo nodes in one process.

mod common;

use common::selecting::{Counting, counted};
use common::*;
use gdl::{Cx, Placement, RuntimeConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_three_segment_placement_matches_the_local_run() {
    let nodes = vec![velo_node().await, velo_node().await, velo_node().await];
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    let events = Events::new();
    let cluster = deploy(&nodes, &placement, &events, RuntimeConfig::default());
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let out = cluster
        .segment("front")
        .call(Cx::new(), prompt("p", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..3));
    events.wait_for("D:done:c(p)#0", WAIT).await;
}

/// About 15 s: one request through each of the 877 placements, on velo
/// nodes started once. Each placement splits the hops differently, so none
/// can stand in for another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_placement_of_the_reference_graph_matches_the_local_run() {
    let mut nodes = Vec::new();
    for _ in 0..REFERENCE_NODES.len() {
        nodes.push(velo_node().await);
    }
    let all = partitions(&REFERENCE_NODES);
    assert_eq!(all.len(), 877);
    let events = Events::new();
    for (case, blocks) in all.iter().enumerate() {
        // Every other case routes each crossing through a selector, whose
        // leases must all be given back: an outer end's, a relay's, and a
        // hand-on's each end on their own path.
        let counting = Counting::default();
        let selected = case % 2 == 1;
        let placement = placement_of(blocks);
        let placement = match selected {
            true => {
                let layout =
                    gdl::layout(&reference_manifest(|_| true), &placement).expect("layout");
                counted(placement, &layout)
            }
            false => placement,
        };
        let config = match selected {
            true => counting.config(),
            false => RuntimeConfig::default(),
        };
        let cluster = deploy(&nodes, &placement, &events, config);
        let plan = Plan {
            tokens: 3,
            ..Plan::default()
        };
        let text = format!("m{case}");
        let out = cluster
            .segment(&source_segment(&placement))
            .call(Cx::new(), prompt(&text, plan))
            .await
            .unwrap_or_else(|e| panic!("{blocks:?}: {e}"));
        assert_eq!(collect(out).await, expected(0..3), "{blocks:?}");
        events.wait_for(&format!("D:done:c({text})#0"), WAIT).await;
        assert!(
            settles(&cluster).await,
            "{blocks:?} did not release its resources"
        );

        // The work really was split as the layout says: one forward hand-off
        // per forward crossing, one prepared run per provisioned run.
        let layout = gdl::layout(&reference_manifest(|_| true), &placement).expect("layout");
        let want = layout.hops().iter().fold((0, 0), |(f, r), hop| {
            if hop.local {
                (f, r)
            } else {
                (f + hop.forward_runs.len() - 1, r + hop.provisioning.len())
            }
        });
        let got = cluster.segments.iter().fold((0, 0), |(f, r), (_, s)| {
            (f + s.served().forwards, r + s.served().runs)
        });
        assert_eq!(got, want, "{blocks:?}");
        if selected {
            assert_eq!(
                counting.taken(),
                want.0,
                "{blocks:?}: a hand-off chose no instance"
            );
            assert_eq!(counting.live(), 0, "{blocks:?}: a lease was kept");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_routers_keep_concurrent_responses_on_their_origin_anchors() {
    use gdl::RequestRouter;
    let mut nodes = Vec::new();
    for _ in 0..5 {
        nodes.push(velo_node().await);
    }
    let instances = vec![
        ("front".into(), nodes[0].clone()),
        ("front".into(), nodes[1].clone()),
        ("engine".into(), nodes[2].clone()),
        ("engine".into(), nodes[3].clone()),
        ("post".into(), nodes[4].clone()),
    ];
    for router in [RequestRouter::RoundRobin, RequestRouter::Random] {
        let placement = Placement::new()
            .segment("front", ["A", "B", "C", "G"])
            .segment("engine", ["D"])
            .segment("post", ["F", "E"])
            .route("D", router.clone());
        let events = Events::new();
        let cluster = deploy_instances(&instances, &placement, &events, RuntimeConfig::default());
        let calls = (0..12u32).map(|i| {
            let segment = &cluster.segments[i as usize % 2].1;
            async move {
                let plan = Plan {
                    tokens: i + 1,
                    ..Plan::default()
                };
                let output = segment
                    .call(Cx::new(), prompt(&format!("r{i}"), plan))
                    .await
                    .expect("routing fixture");
                assert_eq!(collect(output).await, expected(0..i + 1));
            }
        });
        futures::future::join_all(calls).await;
        let served: Vec<_> = cluster
            .segments
            .iter()
            .filter(|(name, _)| name == "engine")
            .map(|(_, s)| s.served().forwards)
            .collect();
        assert_eq!(served.iter().sum::<usize>(), 12);
        if router == RequestRouter::RoundRobin {
            assert_eq!(served, vec![6, 6]);
        }
        assert!(settles(&cluster).await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_routing_keeps_request_items_on_one_replica() {
    use common::streamed::{Script, ask, replies, words};
    let mut nodes = Vec::new();
    for _ in 0..4 {
        nodes.push(velo_node().await);
    }
    let instances = vec![
        ("front".into(), nodes[0].clone()),
        ("engine".into(), nodes[1].clone()),
        ("engine".into(), nodes[2].clone()),
        ("post".into(), nodes[3].clone()),
    ];
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"])
        .route("D", gdl::RequestRouter::Random);
    let events = Events::new();
    let cluster =
        common::streamed::deploy(&instances, &placement, &events, RuntimeConfig::default());
    let calls = (0..8).map(|i| {
        let cluster = &cluster;
        let events = &events;
        async move {
            let text = format!("stream-{i}");
            let input = words(events, &["one", "two", "three"], false);
            let output = cluster
                .segment("front")
                .call(Cx::new(), ask(&text, Script::default(), input))
                .await
                .expect("stream dispatch");
            assert_eq!(
                collect(output).await,
                replies(&text, &["one", "two", "three"])
            );
        }
    });
    futures::future::join_all(calls).await;
    assert_eq!(
        cluster
            .segments
            .iter()
            .filter(|(name, _)| name == "engine")
            .map(|(_, s)| s.served().forwards)
            .sum::<usize>(),
        8
    );
    assert!(settles(&cluster).await);
}
