//! Request router validation and deployment identity.

use gdl_model::{HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo, layout};

fn manifest() -> Manifest {
    Manifest {
        name: "routed".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "D".into(),
                role: Role::Sink,
                binding: Some("d".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("Request", true)],
            backward_types: vec![TypeInfo::new("Reply", true)],
        }],
    }
}
const SPLIT: &str = "[segment.front]\nnodes=['S']\n[segment.worker]\nnodes=['D']\n";

#[test]
fn request_routing_survives_export_and_changes_deployment_identity() {
    let plain = Placement::from_toml(SPLIT).expect("routing fixture");
    let random = Placement::from_toml(&format!("{SPLIT}\n[routing]\nD='random'\n"))
        .expect("routing fixture");
    assert_ne!(plain.hash(), random.hash());
    assert!(
        toml::to_string(&random)
            .expect("routing fixture")
            .contains("random")
    );
    let routed = layout(&manifest(), &random).expect("routing fixture");
    assert_eq!(
        routed.request_router(0, 1),
        gdl_model::RequestRouter::Random
    );
    assert_eq!(
        routed.request_router(0, 0),
        gdl_model::RequestRouter::RoundRobin
    );
}

#[test]
fn routers_must_target_request_boundaries() {
    for target in ["S", "Missing"] {
        let placement = Placement::from_toml(&format!("{SPLIT}\n[routing]\n{target}='random'\n"))
            .expect("routing fixture");
        assert!(
            layout(&manifest(), &placement).is_err(),
            "accepted {target}"
        );
    }
    let local = Placement::from_toml("[segment.all]\nnodes=['S','D']\n[routing]\nD='random'")
        .expect("routing fixture");
    assert!(layout(&manifest(), &local).is_err());
    assert!(Placement::from_toml(&format!("{SPLIT}\n[routing]\nD='custom'\n")).is_err());
}

/// The default policy spelled out is the same placement as the default
/// left unsaid: the composer writes it for every crossing, a hand-written
/// spec does not, and both must register with one hub.
#[test]
fn an_explicit_default_router_does_not_change_the_placement() {
    let unsaid = Placement::new()
        .segment("front", ["S"])
        .segment("sink", ["D"]);
    let spelled = Placement::new()
        .segment("front", ["S"])
        .segment("sink", ["D"])
        .route("D", gdl_model::RequestRouter::RoundRobin);
    assert_eq!(unsaid.hash(), spelled.hash());
    let parsed = Placement::from_toml(
        "[segment.front]\nnodes = [\"S\"]\n[segment.sink]\nnodes = [\"D\"]\n[routing]\nD = \"round_robin\"\n",
    )
    .expect("parses");
    assert_eq!(parsed.hash(), unsaid.hash());
    let random = Placement::new()
        .segment("front", ["S"])
        .segment("sink", ["D"])
        .route("D", gdl_model::RequestRouter::Random);
    assert_ne!(random.hash(), unsaid.hash());
}

/// Who a segment sends to follows the forward path and the backward runs
/// an outer end opens; never the other way, so readiness can follow it.
#[test]
fn a_segment_sends_to_the_next_run_and_the_runs_it_opens() {
    let graph = Manifest {
        name: "chain".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "F".into(),
                role: Role::Forward,
                binding: Some("f".into()),
            },
            NodeInfo {
                name: "D".into(),
                role: Role::Sink,
                binding: Some("d".into()),
            },
            NodeInfo {
                name: "B".into(),
                role: Role::Backward,
                binding: Some("b".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec!["F".into()],
            inner: "D".into(),
            backward: vec!["B".into()],
            forward_types: vec![
                TypeInfo::new("Request", true),
                TypeInfo::new("Request", true),
            ],
            backward_types: vec![TypeInfo::new("Reply", true), TypeInfo::new("Reply", true)],
        }],
    };
    let placement = Placement::new()
        .segment("front", ["S"])
        .segment("fwd", ["F"])
        .segment("sink", ["D"])
        .segment("bwd", ["B"]);
    let layout = layout(&graph, &placement).expect("a layout");
    let names = |segment: &str| -> Vec<String> { layout.sends_to(segment).into_iter().collect() };
    assert_eq!(names("front"), ["bwd", "fwd"]);
    assert_eq!(names("fwd"), ["sink"]);
    assert!(names("sink").is_empty());
    assert!(names("bwd").is_empty());
    // Reaching follows sends: the sink through `fwd`, never back up, and
    // no segment reaches itself without a path that returns.
    assert!(layout.reaches("front", "sink"));
    assert!(layout.reaches("front", "bwd"));
    assert!(!layout.reaches("sink", "front"));
    assert!(!layout.reaches("fwd", "bwd"));
    assert!(!layout.reaches("front", "front"));
}

/// A forward path that leaves a segment and comes back makes the two
/// segments send to each other, so readiness cannot be ordered by this
/// relation alone.
#[test]
fn a_forward_path_that_returns_sends_both_ways() {
    let graph = Manifest {
        name: "back".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "F".into(),
                role: Role::Forward,
                binding: Some("f".into()),
            },
            NodeInfo {
                name: "D".into(),
                role: Role::Sink,
                binding: Some("d".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec!["F".into()],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![
                TypeInfo::new("Request", true),
                TypeInfo::new("Request", true),
            ],
            backward_types: vec![TypeInfo::new("Reply", true)],
        }],
    };
    let placement = Placement::new()
        .segment("front", ["S", "D"])
        .segment("mid", ["F"]);
    let layout = layout(&graph, &placement).expect("a layout");
    let names = |segment: &str| -> Vec<String> { layout.sends_to(segment).into_iter().collect() };
    assert_eq!(names("front"), ["mid"]);
    assert_eq!(names("mid"), ["front"]);
    // Each reaches itself through the other.
    assert!(layout.reaches("front", "front"));
    assert!(layout.reaches("mid", "mid"));
}
