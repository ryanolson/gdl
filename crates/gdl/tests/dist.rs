//! The reference graph split across velo nodes in one process.

mod common;

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

/// Wait until every segment has released everything the requests took.
async fn settles<H>(cluster: &Cluster<H>) -> bool {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let busy = cluster
            .segments
            .iter()
            .any(|(_, s)| s.admitted() > 0 || s.attempting() > 0 || s.active_anchors() > 0);
        if !busy {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(ms(5)).await;
    }
}

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
        let placement = placement_of(blocks);
        let cluster = deploy(&nodes, &placement, &events, RuntimeConfig::default());
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
            (f + s.served().0, r + s.served().1)
        });
        assert_eq!(got, want, "{blocks:?}");
    }
}
