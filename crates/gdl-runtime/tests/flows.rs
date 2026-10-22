//! Streamed requests split across velo nodes in one process: every
//! placement must give the local run's answer.

mod common;

use std::sync::Arc;

use common::selecting::{Counting, counted};
use common::streamed::{self, Brief, Note, Script, ask, replies, words};
use common::{
    Cluster, Events, WAIT, collect, partitions, placement_of, settles, source_segment, velo_node,
};
use gdl::{Cx, Streamed, items};
use gdl_runtime::{Layout, Placement, RuntimeConfig};

async fn nodes(count: usize) -> Vec<velo::Velo> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(velo_node().await);
    }
    nodes
}

/// Forward hand-offs, prepared runs, and input anchors the layout says one
/// request takes.
fn expected_work(layout: &Layout) -> (usize, usize, usize) {
    layout.hops().iter().filter(|hop| !hop.local).fold(
        (0, 0, 0),
        |(forwards, runs, inputs), hop| {
            (
                forwards + hop.forward_runs.len() - 1,
                runs + hop.provisioning.len(),
                inputs + hop.request_anchors(),
            )
        },
    )
}

fn served_work<H>(cluster: &Cluster<H>) -> (usize, usize, usize) {
    cluster
        .segments
        .iter()
        .fold((0, 0, 0), |(forwards, runs, inputs), (_, segment)| {
            let served = segment.served();
            (
                forwards + served.forwards,
                runs + served.runs,
                inputs + served.inputs,
            )
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_three_segment_placement_streams_its_request() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = streamed::deploy(
        &streamed::instances(&nodes, &placement),
        &placement,
        &events,
        RuntimeConfig::default(),
    );
    let input = words(&events, &["x", "y", "z"], false);
    let out = cluster
        .segment("front")
        .call(Cx::new(), ask("p", Script::default(), input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, replies("p", &["x", "y", "z"]));
    assert!(settles(&cluster).await);
    assert_eq!(served_work(&cluster).2, 1, "one input anchor, on engine");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_streams_the_request_through_the_middle_segment() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "G"])
        .segment("mid", ["C"])
        .segment("back", ["D", "F", "E"]);
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = streamed::deploy(
        &streamed::instances(&nodes, &placement),
        &placement,
        &events,
        RuntimeConfig::default(),
    );
    let input = words(&events, &["x", "y"], false);
    let out = cluster
        .segment("front")
        .call(Cx::new(), ask("p", Script::default(), input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, replies("p", &["x", "y"]));
    assert!(settles(&cluster).await);
    let served = cluster.segment("mid").served();
    assert_eq!((served.forwards, served.inputs), (1, 1), "{served:?}");
}

/// About 15 s: one request through each of the 877 placements, on velo
/// nodes started once. Each placement splits the hops differently, so none
/// can stand in for another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_placement_of_the_streamed_reference_graph_matches_the_local_run() {
    const NODES: [&str; 7] = ["A", "B", "C", "D", "E", "F", "G"];
    let nodes = nodes(NODES.len()).await;
    let all = partitions(&NODES);
    assert_eq!(all.len(), 877);
    let events = Events::new();
    let manifest = streamed::contract::manifest();
    for (case, blocks) in all.iter().enumerate() {
        // Every other case routes each crossing through a selector, whose
        // leases must all be given back, a relay's included.
        let counting = Counting::default();
        let selected = case % 2 == 1;
        let placement = placement_of(blocks);
        let placement = match selected {
            true => {
                let layout = gdl_runtime::layout(&manifest, &placement).expect("layout");
                counted(placement, &layout)
            }
            false => placement,
        };
        let config = match selected {
            true => counting.config(),
            false => RuntimeConfig::default(),
        };
        let cluster = streamed::deploy(
            &streamed::instances(&nodes, &placement),
            &placement,
            &events,
            config,
        );
        let text = format!("m{case}");
        let input = words(&events, &["x", "y"], false);
        let out = cluster
            .segment(&source_segment(&placement))
            .call(Cx::new(), ask(&text, Script::default(), input))
            .await
            .unwrap_or_else(|e| panic!("{blocks:?}: {e}"));
        assert_eq!(
            collect(out).await,
            replies(&text, &["x", "y"]),
            "{blocks:?}"
        );
        events
            .wait_for(&format!("D:done:c(b({text}))#0"), WAIT)
            .await;
        assert!(
            settles(&cluster).await,
            "{blocks:?} did not release its resources"
        );
        // The work really was split as the layout says, including one input
        // anchor per crossing of a streamed edge.
        let layout = gdl_runtime::layout(&manifest, &placement).expect("layout");
        assert_eq!(served_work(&cluster), expected_work(&layout), "{blocks:?}");
        if selected {
            assert_eq!(counting.taken(), expected_work(&layout).0, "{blocks:?}");
            assert_eq!(counting.live(), 0, "{blocks:?}: a lease was kept");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_placement_of_the_expanding_and_collapsing_graphs_matches_the_local_run() {
    let nodes = nodes(3).await;
    let brief = |text: &str| Brief {
        text: text.to_string(),
        attempt: 0,
        script: Script::default(),
    };
    let events = Events::new();
    for blocks in partitions(&["A", "X", "D"]) {
        let placement = placement_of(&blocks);
        let events = Arc::clone(&events);
        let cluster = streamed::deploy_graph(
            &streamed::instances(&nodes, &placement),
            &streamed::expanding::manifest(),
            &placement,
            &|_| RuntimeConfig::default(),
            move |deployment| {
                let bindings =
                    streamed::expanding::Bindings::new()
                        .x(streamed::NodeX)
                        .d(streamed::NodeD {
                            events: Arc::clone(&events),
                        });
                streamed::expanding::assemble(deployment, bindings)
            },
        );
        let out = cluster
            .segment(&source_segment(&placement))
            .call(Cx::new(), brief("one two"))
            .await
            .unwrap_or_else(|e| panic!("{blocks:?}: {e}"));
        let notes = ["one two:one", "one two:two", "one two:end:2"];
        let want: Vec<_> = notes.iter().map(|n| Ok(Note(n.to_string()))).collect();
        assert_eq!(collect(out).await, want, "{blocks:?}");
        assert!(settles(&cluster).await, "{blocks:?}");
    }
    for blocks in partitions(&["A", "Y", "S"]) {
        let placement = placement_of(&blocks);
        let cluster = streamed::deploy_graph(
            &streamed::instances(&nodes, &placement),
            &streamed::collapsing::manifest(),
            &placement,
            &|_| RuntimeConfig::default(),
            |deployment| {
                let bindings = streamed::collapsing::Bindings::new()
                    .y(streamed::NodeY)
                    .s(streamed::NodeS);
                streamed::collapsing::assemble(deployment, bindings)
            },
        );
        let input = items(futures::stream::iter([
            Ok("a".to_string()),
            Ok("b".to_string()),
        ]));
        let out = cluster
            .segment(&source_segment(&placement))
            .call(Cx::new(), Streamed::new(brief("t"), input))
            .await
            .unwrap_or_else(|e| panic!("{blocks:?}: {e}"));
        assert_eq!(
            collect(out).await,
            vec![Ok(Note("t:a+b".to_string()))],
            "{blocks:?}"
        );
        assert!(settles(&cluster).await, "{blocks:?}");
    }
}
