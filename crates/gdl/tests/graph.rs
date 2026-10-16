//! The `graph!` macro over the reference graph: what it generates must match
//! the hand-written structure the other suites use.

mod common;

use std::sync::Arc;

use common::*;
use gdl::{Cx, Deployment, Placement, Roster, RuntimeConfig};

mod contract {
    use super::common::{Chunk, Out, Pre, Prompt, Raw, Tok};

    gdl::graph! {
        name = "reference", schema = 1;
        source   A: Prompt -> stream Chunk;
        operator B: (Prompt -> stream Chunk) over (Pre -> stream Out);
        forward  C: Pre -> Pre;
        sink     D: Pre -> stream Raw;
        backward E: Tok -> Out;
        backward F: Raw -> Tok;
        backward G: Chunk -> Chunk;
        topology: A => B { C => D => F => E } => G;
    }
}

fn full_bindings(
    events: &Arc<Events>,
) -> contract::Bindings<NodeB, NodeC, NodeD, NodeE, NodeF, NodeG> {
    contract::Bindings::new()
        .b(NodeB {
            events: Arc::clone(events),
        })
        .c(NodeC {
            events: Arc::clone(events),
        })
        .d(NodeD {
            events: Arc::clone(events),
        })
        .e(NodeE)
        .f(NodeF)
        .g(NodeG)
}

#[test]
fn the_generated_manifest_is_the_reference_manifest() {
    let generated = contract::manifest();
    assert_eq!(generated, reference_manifest(|_| true));
    assert_eq!(
        generated.contract_hash(),
        reference_manifest(|_| true).contract_hash()
    );
}

#[test]
fn the_manifest_survives_its_toml_form() {
    let manifest = contract::manifest();
    let text = manifest.to_toml();
    assert_eq!(gdl::Manifest::from_toml(&text).expect("parse"), manifest);
}

#[test]
fn the_source_types_are_named_by_the_contract() {
    // Compiles only if the aliases are the source's request and item types.
    let request: contract::SourceReq = prompt("t", Plan::default());
    let item: Option<contract::SourceItem> = None::<Chunk>;
    assert_eq!(request.text, "t");
    assert!(item.is_none());
}

#[tokio::test]
async fn the_generated_local_graph_matches_the_hand_written_one() {
    let events = Events::new();
    let generated = contract::local(full_bindings(&events));
    let hand = local_graph(&events);
    for (text, plan) in [
        (
            "g1",
            Plan {
                tokens: 3,
                ..Plan::default()
            },
        ),
        (
            "g2",
            Plan {
                tokens: 2,
                fail_attempts: 1,
                mode: Mode::Retry { max: 3 },
                ..Plan::default()
            },
        ),
    ] {
        let want = collect(
            hand.call(Cx::new(), prompt(text, plan.clone()))
                .await
                .expect("hand"),
        )
        .await;
        let got = collect(
            generated
                .call(Cx::new(), prompt(text, plan))
                .await
                .expect("generated"),
        )
        .await;
        assert_eq!(got, want, "{text}");
    }
}

/// Every placement through the generated `assemble`, every slot bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_placement_runs_through_the_generated_assembly() {
    let mut nodes = Vec::new();
    for _ in 0..REFERENCE_NODES.len() {
        nodes.push(velo_node().await);
    }
    let manifest = contract::manifest();
    let events = Events::new();
    for (case, blocks) in partitions(&REFERENCE_NODES).iter().enumerate() {
        let placement = placement_of(blocks);
        let names: Vec<String> = placement.segments().map(|(n, _)| n.to_string()).collect();
        let mut roster = Roster::new();
        for (name, node) in names.iter().zip(&nodes) {
            roster.add(name.clone(), node.peer_info());
        }
        let segments: Vec<_> = names
            .iter()
            .zip(&nodes)
            .map(|(name, node)| {
                let deployment = Deployment::new(
                    Arc::clone(node),
                    &manifest,
                    &placement,
                    name,
                    &roster,
                    RuntimeConfig::default(),
                )
                .expect("deployment");
                let root =
                    contract::assemble(&deployment, full_bindings(&events)).expect("assemble");
                (name.clone(), deployment.start(root).expect("start"))
            })
            .collect();
        let source = source_segment(&placement);
        let (_, front) = segments.iter().find(|(n, _)| *n == source).expect("source");
        let text = format!("x{case}");
        let plan = Plan {
            tokens: 3,
            ..Plan::default()
        };
        let out = front
            .call(Cx::new(), prompt(&text, plan))
            .await
            .unwrap_or_else(|e| panic!("{blocks:?}: {e}"));
        assert_eq!(collect(out).await, expected(0..3), "{blocks:?}");
        events.wait_for(&format!("D:done:c({text})#0"), WAIT).await;
    }
}

/// The three-segment placement with each segment binding only the nodes it
/// hosts; every other slot stays `Absent`, so no segment names another's
/// implementation types.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segments_that_bind_only_their_own_nodes_run_the_graph() {
    let nodes = [velo_node().await, velo_node().await, velo_node().await];
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    let manifest = contract::manifest();
    let mut roster = Roster::new();
    for (name, node) in ["front", "engine", "post"].iter().zip(&nodes) {
        roster.add(*name, node.peer_info());
    }
    let deployment = |index: usize, name: &str| {
        Deployment::new(
            Arc::clone(&nodes[index]),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment")
    };
    let events = Events::new();

    let front = deployment(0, "front");
    let front_root = contract::assemble(
        &front,
        contract::Bindings::new()
            .b(NodeB {
                events: Arc::clone(&events),
            })
            .c(NodeC {
                events: Arc::clone(&events),
            })
            .g(NodeG),
    )
    .expect("front");
    let front = front.start(front_root).expect("front");

    let engine = deployment(1, "engine");
    let engine_root = contract::assemble(
        &engine,
        contract::Bindings::new().d(NodeD {
            events: Arc::clone(&events),
        }),
    )
    .expect("engine");
    let _engine = engine.start(engine_root).expect("engine");

    let post = deployment(2, "post");
    let post_root =
        contract::assemble(&post, contract::Bindings::new().f(NodeF).e(NodeE)).expect("post");
    let _post = post.start(post_root).expect("post");

    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let out = front
        .call(Cx::new(), prompt("own", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..3));
}

/// A slot may stay `Absent` only for a node another segment hosts: a
/// segment that leaves one of its own nodes unbound is refused at assembly,
/// not on its first request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_that_leaves_a_hosted_node_unbound_is_refused() {
    let nodes = [velo_node().await, velo_node().await];
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "F", "E", "G"])
        .segment("engine", ["D"]);
    let manifest = contract::manifest();
    let mut roster = Roster::new();
    for (name, node) in ["front", "engine"].iter().zip(&nodes) {
        roster.add(*name, node.peer_info());
    }
    let deployment = |index: usize, name: &str| {
        Deployment::new(
            Arc::clone(&nodes[index]),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment")
    };
    let events = Events::new();

    let engine = deployment(1, "engine");
    match contract::assemble(&engine, contract::Bindings::new()) {
        Ok(_) => panic!("an engine without D was assembled"),
        Err(error) => {
            assert_eq!(error.kind(), gdl::ErrorKind::Placement);
            assert!(error.to_string().contains("`D`"), "{error}");
        }
    }

    let front = deployment(0, "front");
    let without_c = contract::Bindings::new()
        .b(NodeB {
            events: Arc::clone(&events),
        })
        .e(NodeE)
        .f(NodeF)
        .g(NodeG);
    match contract::assemble(&front, without_c) {
        Ok(_) => panic!("a front without C was assembled"),
        Err(error) => assert!(error.to_string().contains("`C`"), "{error}"),
    }

    // D is hosted elsewhere, so the front may leave it absent.
    let front = deployment(0, "front");
    let without_d = contract::Bindings::new()
        .b(NodeB {
            events: Arc::clone(&events),
        })
        .c(NodeC {
            events: Arc::clone(&events),
        })
        .e(NodeE)
        .f(NodeF)
        .g(NodeG);
    assert!(contract::assemble(&front, without_d).is_ok());
}

mod twins {
    gdl::graph! {
        name = "twins", schema = 1;
        source  A: u32 -> stream u32;
        forward C: u32 -> u32;
        forward E: u32 -> u32;
        sink    D: u32 -> stream u32;
        topology: A => C => E => D;
    }
}

struct Echo;

struct Same;

impl gdl::Forward for Same {
    type In = u32;
    type Out = u32;

    async fn forward(&self, _cx: &Cx, req: u32) -> gdl::Result<u32> {
        Ok(req)
    }
}

impl gdl::Sink for Echo {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, _cx: Cx, req: u32) -> gdl::Result<gdl::Items<u32>> {
        Ok(gdl::items(futures::stream::iter([Ok(req)])))
    }
}

/// The check is by slot, not by the placeholder's name: `C` and `E` have the
/// same port types, so `E`'s placeholder fits `C`'s slot, and `C` is still
/// unbound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hosted_slot_holding_another_nodes_placeholder_is_refused() {
    let nodes = [velo_node().await, velo_node().await];
    let placement = Placement::new()
        .segment("front", ["A", "C", "D"])
        .segment("side", ["E"]);
    let manifest = twins::manifest();
    let mut roster = Roster::new();
    for (name, node) in ["front", "side"].iter().zip(&nodes) {
        roster.add(*name, node.peer_info());
    }
    let front = Deployment::new(
        Arc::clone(&nodes[0]),
        &manifest,
        &placement,
        "front",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("deployment");
    let bindings = twins::Bindings::new()
        .c(gdl::Absent::<twins::nodes::E>::new())
        .d(Echo);
    match twins::assemble(&front, bindings) {
        Ok(_) => panic!("a front with no C was assembled"),
        Err(error) => assert!(error.to_string().contains("`C`"), "{error}"),
    }
}

mod twins_v2 {
    gdl::graph! {
        name = "twins", schema = 2;
        source  A: u32 -> stream u32;
        forward C: u32 -> u32;
        forward E: u32 -> u32;
        sink    D: u32 -> stream u32;
        topology: A => C => E => D;
    }
}

/// A deployment carries the contract it was made for; assembling another
/// contract's graph on it is refused, even when the two have the same shape
/// (here, the same graph at another schema), so its envelopes never claim a
/// contract that its stages do not run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assembling_another_contracts_graph_is_refused() {
    let node = velo_node().await;
    let placement = Placement::new().segment("all", ["A", "C", "E", "D"]);
    let mut roster = Roster::new();
    roster.add("all", node.peer_info());
    let deployment = Deployment::new(
        Arc::clone(&node),
        &twins::manifest(),
        &placement,
        "all",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("deployment");
    let bindings = twins_v2::Bindings::new().c(Same).e(Same).d(Echo);
    match twins_v2::assemble(&deployment, bindings) {
        Ok(_) => panic!("the schema 2 graph was assembled on a schema 1 deployment"),
        Err(error) => assert!(error.to_string().contains("contract"), "{error}"),
    }
}
