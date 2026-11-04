//! An around node's two halves run where one task runs them both, or the
//! placement is refused. Its value never crosses a segment, and being in
//! one segment is not enough: two runs in that segment can be two tasks on
//! two instances.

use gdl_model::{HopInfo, Manifest, NodeInfo, Placement, PlacementError, Role, TypeInfo, layout};

/// `Src => Pair { Sink } => After`: one hop whose forward chain is
/// `[Pair]` and whose backward chain is `[Pair, After]`, with a forward
/// node `Before` ahead of the pair when `before` is set.
fn graph(before: bool) -> Manifest {
    let ty = || TypeInfo::new("T", true, Default::default());
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
        calls: Vec::new(),
        unary: None,
    };
    let mut forward = vec!["Pair".to_string()];
    let mut nodes = vec![
        node("Src", Role::Source),
        node("Pair", Role::Around),
        node("Sink", Role::Sink),
        node("After", Role::Backward),
    ];
    if before {
        forward.insert(0, "Before".into());
        nodes.push(node("Before", Role::Forward));
    }
    let backward = vec!["Pair".to_string(), "After".to_string()];
    Manifest {
        name: "around".into(),
        schema: 1,
        nodes,
        hops: vec![HopInfo {
            outer: "Src".into(),
            forward_types: (0..=forward.len()).map(|_| ty()).collect(),
            backward_types: (0..=backward.len()).map(|_| ty()).collect(),
            forward,
            inner: "Sink".into(),
            backward,
        }],
        properties: Vec::new(),
    }
}

fn split(errors: &[PlacementError]) -> Vec<&PlacementError> {
    errors
        .iter()
        .filter(|e| matches!(e, PlacementError::AroundSplit { .. }))
        .collect()
}

#[test]
fn a_pair_in_one_task_is_placed() {
    let accepted = [
        // A local hop.
        Placement::new().segment("all", ["Src", "Pair", "Sink", "After"]),
        // The outer end's task: the forward walk to the hand-off, and the
        // backward chain after the response returns.
        Placement::new()
            .segment("front", ["Src", "Pair", "After"])
            .segment("engine", ["Sink"]),
        // The inner end's task: the hand-off, the inner end, and the
        // backward run that starts at the inner end.
        Placement::new()
            .segment("front", ["Src", "After"])
            .segment("engine", ["Pair", "Sink"]),
    ];
    for placement in accepted {
        if let Err(errors) = layout(&graph(false), &placement) {
            panic!("refused: {errors}");
        }
    }
}

#[test]
fn a_pair_split_across_segments_is_refused() {
    let placement = Placement::new()
        .segment("front", ["Src", "After"])
        .segment("mid", ["Pair"])
        .segment("engine", ["Sink"]);
    let errors = layout(&graph(false), &placement).expect_err("refused").0;
    let found = split(&errors);
    assert_eq!(found.len(), 1, "{errors:?}");
    let text = found[0].to_string();
    assert!(text.contains("`Pair`"), "{text}");
    assert!(text.contains("hop 0"), "{text}");
}

/// The forward path leaves the outer end's segment and comes back: the
/// pair's forward half runs in the outer end's task, and its backward half
/// in a run that a hand-off started, which can be another instance.
#[test]
fn a_pair_whose_halves_are_in_one_segment_but_two_tasks_is_refused() {
    let placement = Placement::new()
        .segment("front", ["Src", "Pair", "Sink", "After"])
        .segment("side", ["Before"]);
    let errors = layout(&graph(true), &placement).expect_err("refused").0;
    assert_eq!(split(&errors).len(), 1, "{errors:?}");
}

/// The forward path stays in the outer end's segment and the backward path
/// leaves it: the inner end's run is a task of its own, so a pair whose
/// backward half is in it is refused.
#[test]
fn a_pair_whose_backward_half_runs_beside_a_spawned_inner_end_is_refused() {
    let placement = Placement::new()
        .segment("front", ["Src", "Pair", "Sink"])
        .segment("post", ["After"]);
    let errors = layout(&graph(false), &placement).expect_err("refused").0;
    assert_eq!(split(&errors).len(), 1, "{errors:?}");
}

#[test]
fn an_around_node_in_one_chain_only_is_an_invalid_manifest() {
    let mut manifest = graph(false);
    manifest.hops[0].backward = vec!["After".into()];
    manifest.hops[0].backward_types.pop();
    let placement = Placement::new().segment("all", ["Src", "Pair", "Sink", "After"]);
    let errors = layout(&manifest, &placement).expect_err("refused").0;
    assert!(
        errors.iter().any(|e| e.to_string().contains("`Pair`")),
        "{errors:?}"
    );
}

#[test]
fn around_nodes_that_cross_are_an_invalid_manifest() {
    let ty = || TypeInfo::new("T", true, Default::default());
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
        calls: Vec::new(),
        unary: None,
    };
    let manifest = Manifest {
        name: "crossed".into(),
        schema: 1,
        nodes: vec![
            node("Src", Role::Source),
            node("P", Role::Around),
            node("Q", Role::Around),
            node("Sink", Role::Sink),
        ],
        hops: vec![HopInfo {
            outer: "Src".into(),
            forward: vec!["P".into(), "Q".into()],
            inner: "Sink".into(),
            backward: vec!["P".into(), "Q".into()],
            forward_types: (0..3).map(|_| ty()).collect(),
            backward_types: (0..3).map(|_| ty()).collect(),
        }],
        properties: Vec::new(),
    };
    let placement = Placement::new().segment("all", ["Src", "P", "Q", "Sink"]);
    let errors = layout(&manifest, &placement).expect_err("refused").0;
    assert!(
        errors.iter().any(|e| e.to_string().contains("nest")),
        "{errors:?}"
    );
}
