//! Placement validation and run partitioning for the reference graph.

mod common;

use common::*;
use gdl::{Layout, Placement, PlacementError, Role, WireReason, layout};

fn all_wire() -> gdl::Manifest {
    reference_manifest(|_| true)
}

fn ok(placement: &Placement) -> Layout {
    layout(&all_wire(), placement).unwrap_or_else(|e| panic!("{e}"))
}

fn errors(manifest: &gdl::Manifest, placement: &Placement) -> Vec<PlacementError> {
    match layout(manifest, placement) {
        Ok(_) => panic!("placement should be rejected"),
        Err(errors) => errors.0,
    }
}

fn stages(runs: &[gdl::Run]) -> Vec<(String, Vec<String>)> {
    runs.iter()
        .map(|run| (run.segment.clone(), run.stages.clone()))
        .collect()
}

fn run(segment: &str, nodes: &[&str]) -> (String, Vec<String>) {
    (
        segment.to_string(),
        nodes.iter().map(|s| s.to_string()).collect(),
    )
}

#[test]
fn the_three_segment_placement_splits_hop_one_into_three_backward_runs() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    let layout = ok(&placement);
    let [hop0, hop1] = layout.hops() else {
        panic!("two hops")
    };

    assert!(hop0.local);
    assert_eq!(hop0.anchors(), 0);

    assert!(!hop1.local);
    assert_eq!(
        stages(&hop1.forward_runs),
        vec![run("front", &["B", "C"]), run("engine", &["D"])]
    );
    assert_eq!(
        stages(&hop1.backward_runs),
        vec![
            run("engine", &["D"]),
            run("post", &["F", "E"]),
            run("front", &["B"])
        ]
    );
    assert_eq!(hop1.provisioning, vec![1]);
    assert_eq!(hop1.anchors(), 2);
    assert_eq!(hop1.crossings(), 3);
    assert_eq!(layout.report().crossings(), 3);
    assert_eq!(layout.report().anchors(), 2);
}

#[test]
fn moving_g_out_makes_the_outer_hop_remote() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "C"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"])
        .segment("tail", ["G"]);
    let layout = ok(&placement);
    let hop0 = &layout.hops()[0];
    assert!(!hop0.local);
    assert_eq!(
        stages(&hop0.backward_runs),
        vec![
            run("front", &["B"]),
            run("tail", &["G"]),
            run("front", &["A"])
        ]
    );
    assert_eq!(hop0.provisioning, vec![1]);
    assert_eq!(layout.report().crossings(), 5);
}

#[test]
fn a_backward_path_that_revisits_the_outer_segment_is_provisioned_last_to_first() {
    // D@s2 -> F@s1 -> E@s3 -> B@s1
    let placement = Placement::new()
        .segment("s1", ["A", "B", "C", "F", "G"])
        .segment("s2", ["D"])
        .segment("s3", ["E"]);
    let hop1 = ok(&placement).hops()[1].clone();
    assert_eq!(
        stages(&hop1.backward_runs),
        vec![
            run("s2", &["D"]),
            run("s1", &["F"]),
            run("s3", &["E"]),
            run("s1", &["B"])
        ]
    );
    assert_eq!(hop1.provisioning, vec![2, 1]);
    assert_eq!(hop1.anchors(), 3);
}

#[test]
fn a_forward_excursion_carries_the_response_through_an_anchor() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "D", "F", "E", "G"])
        .segment("side", ["C"]);

    let hop1 = ok(&placement).hops()[1].clone();
    assert!(!hop1.local);
    assert_eq!(hop1.backward_runs.len(), 1);
    assert_eq!(hop1.anchors(), 1);

    let manifest = reference_manifest(|ty| ty != "Raw");
    assert_eq!(
        errors(&manifest, &placement),
        vec![PlacementError::NotWire {
            from: "D".into(),
            to: "F".into(),
            ty: "Raw".into(),
            reason: WireReason::AnchorCarried,
        }]
    );
}

#[test]
fn a_crossing_over_a_non_wire_edge_is_rejected_and_a_local_one_is_not() {
    let manifest = reference_manifest(|ty| ty != "Tok");
    let crossing = Placement::new()
        .segment("a", ["A", "B", "C", "D", "F", "G"])
        .segment("b", ["E"]);
    assert_eq!(
        errors(&manifest, &crossing),
        vec![PlacementError::NotWire {
            from: "F".into(),
            to: "E".into(),
            ty: "Tok".into(),
            reason: WireReason::Crossing,
        }]
    );
    let local = Placement::new().segment("a", ["A", "B", "C", "D", "F", "E", "G"]);
    assert!(layout(&manifest, &local).is_ok());
}

#[test]
fn every_placement_problem_is_reported_together() {
    let placement = Placement::new()
        .segment("a", ["A", "B", "C", "X"])
        .segment("b", ["D", "C"])
        .segment("empty", Vec::<String>::new());
    let found = errors(&all_wire(), &placement);
    for expected in [
        PlacementError::EmptySegment {
            segment: "empty".into(),
        },
        PlacementError::UnknownNode {
            segment: "a".into(),
            node: "X".into(),
        },
        PlacementError::PlacedTwice {
            node: "C".into(),
            segments: vec!["a".into(), "b".into()],
        },
        PlacementError::Unplaced { node: "E".into() },
        PlacementError::Unplaced { node: "F".into() },
        PlacementError::Unplaced { node: "G".into() },
    ] {
        assert!(
            found.contains(&expected),
            "missing {expected:?} in {found:?}"
        );
    }
    assert_eq!(found.len(), 6, "{found:?}");
}

#[test]
fn inconsistent_manifests_are_rejected() {
    let single = Placement::new().segment("a", REFERENCE_NODES);

    let mut wrong_role = all_wire();
    wrong_role.nodes[2].role = Role::Backward;
    assert!(matches!(
        errors(&wrong_role, &single)[..],
        [PlacementError::InvalidManifest(_), ..]
    ));

    let mut missing_type = all_wire();
    missing_type.hops[1].backward_types.pop();
    assert!(matches!(
        errors(&missing_type, &single)[..],
        [PlacementError::InvalidManifest(_)]
    ));

    let mut broken_nesting = all_wire();
    broken_nesting.hops[1].outer = "A".into();
    assert!(!errors(&broken_nesting, &single).is_empty());

    let mut duplicate = all_wire();
    duplicate.hops[1].backward.push("G".into());
    duplicate.hops[1]
        .backward_types
        .push(gdl::TypeInfo::new("Chunk", true));
    assert!(!errors(&duplicate, &single).is_empty());
}

#[test]
fn placement_files_parse_their_segment_tables_only() {
    let text = r#"
        contract = { path = "../contract" }

        [segment.front]
        nodes = ["A", "B", "C", "G"]
        [segment.engine]
        nodes = ["D"]
        [segment.post]
        nodes = ["F", "E"]

        [bind]
        D = { crate = "engine", type = "engine::D" }
    "#;
    let parsed = Placement::from_toml(text).expect("parse");
    let built = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"]);
    assert_eq!(parsed, built);
    assert!(Placement::from_toml("[segment.x]\nnodes = 3").is_err());
}

#[test]
fn the_placement_hash_tracks_moves_but_not_listing_order() {
    let base = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D", "E", "F"]);
    let reordered = Placement::new()
        .segment("front", ["G", "C", "B", "A"])
        .segment("engine", ["F", "E", "D"]);
    let moved = Placement::new()
        .segment("front", ["A", "B", "G"])
        .segment("engine", ["C", "D", "E", "F"]);
    let renamed = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("gpu", ["D", "E", "F"]);
    assert_eq!(base.hash(), reordered.hash());
    assert_ne!(base.hash(), moved.hash());
    assert_ne!(base.hash(), renamed.hash());
}

#[test]
fn the_contract_hash_tracks_schema_topology_and_types() {
    let base = all_wire();
    let mut schema = all_wire();
    schema.schema = 2;
    let mut ty = all_wire();
    ty.hops[1].backward_types[1].name = "Token".into();
    let mut topology = all_wire();
    topology.hops[1].backward.swap(0, 1);
    for changed in [schema, ty, topology] {
        assert_ne!(base.contract_hash(), changed.contract_hash());
    }
    // Wire capability is a property of the types, not of the contract.
    assert_eq!(
        base.contract_hash(),
        reference_manifest(|_| false).contract_hash()
    );
}

/// Invariants over every placement of the reference graph.
#[test]
fn every_partition_of_the_reference_graph_has_a_consistent_layout() {
    let manifest = all_wire();
    let all = partitions(&REFERENCE_NODES);
    assert_eq!(all.len(), 877);
    for blocks in &all {
        let placement = placement_of(blocks);
        let layout = layout(&manifest, &placement).unwrap_or_else(|e| panic!("{blocks:?}: {e}"));
        for (hop, info) in layout.hops().iter().zip(&manifest.hops) {
            for (runs, path) in [
                (&hop.forward_runs, info.forward_path().collect::<Vec<_>>()),
                (&hop.backward_runs, info.backward_path().collect::<Vec<_>>()),
            ] {
                // Runs tile the path exactly, in order.
                let flat: Vec<&str> = runs
                    .iter()
                    .flat_map(|run| run.stages.iter().map(String::as_str))
                    .collect();
                assert_eq!(flat, path, "{blocks:?}");
                for pair in runs.windows(2) {
                    assert_ne!(pair[0].segment, pair[1].segment, "{blocks:?}");
                    assert_eq!(pair[1].start, pair[0].start + pair[0].stages.len());
                }
                for run in runs.iter() {
                    for stage in &run.stages {
                        assert_eq!(layout.segment_of(stage), Some(run.segment.as_str()));
                    }
                }
            }
            let outer = layout.segment_of(&info.outer);
            let all_with_outer = info
                .forward_path()
                .chain(info.backward_path())
                .all(|node| layout.segment_of(node) == outer);
            assert_eq!(hop.local, all_with_outer, "{blocks:?}");
            if hop.local {
                assert!(hop.provisioning.is_empty());
                assert_eq!((hop.crossings(), hop.anchors()), (0, 0));
            } else {
                let k = hop.backward_runs.len() - 1;
                let expected: Vec<usize> = (1..k).rev().collect();
                assert_eq!(hop.provisioning, expected, "{blocks:?}");
                assert!(hop.anchors() >= 1);
            }
        }
    }
}

#[test]
fn an_operator_may_own_only_one_downstream_hop() {
    let single = Placement::new().segment("a", REFERENCE_NODES);

    // B -> B: an operator nested under itself.
    let mut self_nested = all_wire();
    self_nested.hops.insert(
        1,
        gdl::HopInfo {
            outer: "B".into(),
            forward: vec![],
            inner: "B".into(),
            backward: vec![],
            forward_types: vec![gdl::TypeInfo::new("Pre", true)],
            backward_types: vec![gdl::TypeInfo::new("Out", true)],
        },
    );
    assert!(matches!(
        errors(&self_nested, &single)[..],
        [PlacementError::InvalidManifest(_), ..]
    ));

    // B reappears further down: A -> B, B -> X, X -> B, B -> D.
    let mut repeated = all_wire();
    repeated.nodes.push(gdl::NodeInfo {
        name: "X".into(),
        role: Role::Operator,
    });
    let hop = |outer: &str, inner: &str| gdl::HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![gdl::TypeInfo::new("Pre", true)],
        backward_types: vec![gdl::TypeInfo::new("Out", true)],
    };
    repeated.hops.insert(1, hop("B", "X"));
    repeated.hops.insert(2, hop("X", "B"));
    let with_x = Placement::new().segment("a", ["A", "B", "C", "D", "E", "F", "G", "X"]);
    assert!(matches!(
        errors(&repeated, &with_x)[..],
        [PlacementError::InvalidManifest(_), ..]
    ));
}
