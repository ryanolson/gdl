//! Response codec requirements when a forward path returns or stays remote.

use gdl_model::{
    HopInfo, Manifest, NodeInfo, Placement, PlacementError, Role, TypeInfo, WireReason, layout,
};

fn manifest() -> Manifest {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    Manifest {
        name: "response".into(),
        schema: 1,
        nodes: vec![
            node("A", Role::Source),
            node("C", Role::Forward),
            node("D", Role::Sink),
        ],
        hops: vec![HopInfo {
            outer: "A".into(),
            forward: vec!["C".into()],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("Request", true, Default::default()); 2],
            backward_types: vec![TypeInfo::new("LocalReply", false, Default::default())],
        }],
    }
}

#[test]
fn a_local_reply_works_without_a_detour_and_needs_a_codec_with_one() {
    let graph = manifest();
    let local = Placement::new().segment("front", ["A", "C", "D"]);
    layout(&graph, &local).expect("the reply needs no codec when the hop stays local");

    let detour = Placement::new()
        .segment("front", ["A", "D"])
        .segment("mid", ["C"]);
    let errors = layout(&graph, &detour).expect_err("the detour uses a reply anchor");
    assert!(errors.0.iter().any(|error| matches!(
        error,
        PlacementError::NotWire {
            reason: WireReason::AnchorCarried,
            ..
        }
    )));
}

/// A cut whose forward path does not come back still needs the reply's
/// codec: the response crosses back to the outer end. Only a hop kept in
/// one segment, as above, needs none (`docs/src/operations/limitations.md`).
#[test]
fn a_cut_that_does_not_come_back_still_needs_a_reply_codec() {
    let graph = manifest();
    let placement = Placement::new()
        .segment("front", ["A"])
        .segment("mid", ["C", "D"]);
    let mut wire_graph = graph.clone();
    wire_graph.hops[0].backward_types[0].wire = true;
    let routed = layout(&wire_graph, &placement).expect("the placement is otherwise valid");
    assert_eq!(routed.hops()[0].forward_runs.len(), 2);
    assert_eq!(routed.hops()[0].forward_runs[1].stages, ["C", "D"]);

    let errors = layout(&graph, &placement).expect_err("the reply crosses back to A");
    assert!(errors.0.iter().any(|error| matches!(
        error,
        PlacementError::NotWire {
            reason: WireReason::Crossing,
            ..
        }
    )));
}
