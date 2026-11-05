//! Unary nodes and `calls`: what a graph and a placement may not say about
//! them. A unary node is not in the topology; a node reaches it through
//! its `calls` clause, in its own segment or in another one. Each refusal
//! names the node and the clause, and the table and key to change.

use gdl_model::{
    Encoding, HopInfo, Manifest, NodeInfo, Placement, PlacementError, PlacementErrors, Problem,
    RequestRouter, Role, TypeInfo, UnaryInfo, WireReason, layout,
};

fn ty(name: &str) -> TypeInfo {
    TypeInfo::new(name, true, Encoding::default())
}

fn node(name: &str, role: Role) -> NodeInfo {
    NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
        calls: vec![],
        unary: None,
    }
}

fn unary(name: &str, req: TypeInfo, resp: TypeInfo) -> NodeInfo {
    NodeInfo {
        unary: Some(UnaryInfo {
            req,
            resp,
            idempotent: false,
        }),
        ..node(name, Role::Unary)
    }
}

fn calls(mut node: NodeInfo, services: &[&str]) -> NodeInfo {
    node.calls = services.iter().map(|s| s.to_string()).collect();
    node
}

fn hop(outer: &str, inner: &str) -> HopInfo {
    HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![ty("Prompt")],
        backward_types: vec![ty("Chunk")],
    }
}

/// `S => R { D }`, where R `calls Tok`, and `Ov` calls `Tok` too.
fn manifest(nodes: Vec<NodeInfo>) -> Manifest {
    Manifest {
        name: "called".into(),
        schema: 1,
        nodes,
        hops: vec![hop("S", "R"), hop("R", "D")],
        properties: vec![],
    }
}

fn nodes() -> Vec<NodeInfo> {
    vec![
        node("S", Role::Source),
        calls(node("R", Role::Operator), &["Tok"]),
        node("D", Role::Sink),
        unary("Tok", ty("Text"), ty("Tokens")),
        calls(unary("Ov", ty("Blocks"), ty("Scores")), &["Tok"]),
    ]
}

fn placement(text: &str) -> Placement {
    Placement::from_toml(text).expect("a placement")
}

/// Every node in one segment.
const ONE: &str = "[segment.all]\nnodes = ['S', 'R', 'D', 'Tok', 'Ov']\n";

fn refused(manifest: &Manifest, text: &str) -> PlacementErrors {
    match layout(manifest, &placement(text)) {
        Ok(_) => panic!("laid out {text}"),
        Err(errors) => errors,
    }
}

/// The one error `errors` holds, with its problem.
fn only(errors: &PlacementErrors) -> (&PlacementError, Problem) {
    assert_eq!(errors.0.len(), 1, "{errors}");
    (&errors.0[0], errors.0[0].problem())
}

#[test]
fn a_graph_with_calls_lays_out_in_one_segment() {
    let laid = layout(&manifest(nodes()), &placement(ONE)).expect("laid out");
    assert_eq!(laid.segment_of("R"), Some("all"));
    // A unary node may be in several segments, so it has no one segment.
    assert_eq!(laid.segment_of("Tok"), None);
    assert_eq!(laid.service_segments("Tok"), ["all"]);
}

#[test]
fn a_calls_name_that_no_unary_declares_is_refused() {
    let mut nodes = nodes();
    nodes[1] = calls(node("R", Role::Operator), &["Tok", "Missing"]);
    let errors = refused(&manifest(nodes), ONE);
    let (error, problem) = only(&errors);
    assert_eq!(
        error,
        &PlacementError::UnknownService {
            node: "R".into(),
            service: "Missing".into(),
        }
    );
    let text = error.to_string();
    assert!(text.contains("node `R`"), "{text}");
    assert!(text.contains("`calls Missing`"), "{text}");
    assert!(text.contains("unary Missing"), "{text}");
    // The graph's to change: no placement table.
    assert_eq!((problem.table, problem.key), (None, None));
}

#[test]
fn a_cycle_of_calls_is_refused() {
    let mut cycled = nodes();
    cycled[3] = calls(unary("Tok", ty("Text"), ty("Tokens")), &["Ov"]);
    let errors = refused(&manifest(cycled), ONE);
    let (error, problem) = only(&errors);
    assert_eq!(
        error,
        &PlacementError::CallCycle {
            nodes: vec!["Ov".into(), "Tok".into()],
        }
    );
    let text = error.to_string();
    assert!(
        text.contains("`Ov` calls `Tok`, and `Tok` calls `Ov`"),
        "{text}"
    );
    assert_eq!((problem.table, problem.key), (None, None));

    // A node that calls itself is a cycle of one.
    let mut own = nodes();
    own[3] = calls(unary("Tok", ty("Text"), ty("Tokens")), &["Tok"]);
    let errors = refused(&manifest(own), ONE);
    assert_eq!(
        only(&errors).0,
        &PlacementError::CallCycle {
            nodes: vec!["Tok".into()],
        }
    );
    assert!(errors.to_string().contains("`Tok` calls `Tok`"), "{errors}");
}

#[test]
fn a_unary_node_in_the_topology_is_refused() {
    let mut on_hop = manifest(nodes());
    on_hop.hops[1].forward = vec!["Tok".into()];
    on_hop.hops[1].forward_types.push(ty("Prompt"));
    let errors = refused(&on_hop, ONE);
    let (error, _) = only(&errors);
    assert_eq!(
        error,
        &PlacementError::UnaryInTopology { node: "Tok".into() }
    );
    let text = error.to_string();
    assert!(text.contains("unary node `Tok`"), "{text}");
    assert!(text.contains("`calls Tok`"), "{text}");
}

#[test]
fn a_topology_node_in_calls_is_refused() {
    let mut nodes = nodes();
    nodes[1] = calls(node("R", Role::Operator), &["Tok", "D"]);
    let errors = refused(&manifest(nodes), ONE);
    let (error, problem) = only(&errors);
    assert_eq!(
        error,
        &PlacementError::CallsPipelineNode {
            node: "R".into(),
            service: "D".into(),
            role: Role::Sink,
        }
    );
    let text = error.to_string();
    assert!(text.contains("node `R`"), "{text}");
    assert!(text.contains("`calls D`"), "{text}");
    assert!(text.contains("sink"), "{text}");
    assert_eq!((problem.table, problem.key), (None, None));
}

#[test]
fn a_pipeline_node_in_two_segments_is_refused_and_a_unary_node_is_not() {
    let errors = refused(
        &manifest(nodes()),
        "[segment.a]\nnodes = ['S', 'R', 'Tok', 'Ov']\n[segment.b]\nnodes = ['D', 'R']\n",
    );
    let (error, problem) = only(&errors);
    assert_eq!(
        error,
        &PlacementError::PlacedTwice {
            node: "R".into(),
            segments: vec!["a".into(), "b".into()],
        }
    );
    let text = error.to_string();
    assert!(text.contains("only a unary node"), "{text}");
    assert_eq!(problem.table.as_deref(), Some("segment"));

    let laid = layout(
        &manifest(nodes()),
        &placement(
            "[segment.a]\nnodes = ['S', 'R', 'Tok', 'Ov']\n[segment.b]\nnodes = ['D', 'Tok']\n",
        ),
    )
    .expect("a unary node in two segments");
    assert_eq!(laid.service_segments("Tok"), ["a", "b"]);
    assert_eq!(laid.service_segments("Ov"), ["a"]);
}

#[test]
fn a_remote_call_needs_wire_types_and_a_local_one_does_not() {
    let mut nodes = nodes();
    nodes[3] = unary(
        "Tok",
        TypeInfo::new("Text", false, Encoding::default()),
        TypeInfo::new("Tokens", false, Encoding::default()),
    );
    // In the caller's segment the call is a function call: nothing is
    // encoded.
    layout(&manifest(nodes.clone()), &placement(ONE)).expect("a local call");

    let errors = refused(
        &manifest(nodes),
        "[segment.front]\nnodes = ['S', 'R', 'D', 'Ov']\n[segment.tok]\nnodes = ['Tok']\n",
    );
    let not_wire = |from: &str, ty: &str| PlacementError::NotWire {
        from: from.into(),
        to: "Tok".into(),
        ty: ty.into(),
        reason: WireReason::Call,
    };
    assert_eq!(
        errors.0,
        vec![
            not_wire("Ov", "Text"),
            not_wire("Ov", "Tokens"),
            not_wire("R", "Text"),
            not_wire("R", "Tokens"),
        ],
        "{errors}"
    );
    let text = errors.0[2].to_string();
    assert!(
        text.contains("`R` calls `Tok` in another segment"),
        "{text}"
    );
    assert!(text.contains("`Text` is not serializable"), "{text}");
}

#[test]
fn a_call_across_classes_is_refused() {
    let segments = "[segment.front]\nnodes = ['S', 'R', 'Ov', 'Tok']\n\
        [segment.engine]\nnodes = ['D']\n[segment.tok]\nnodes = ['Tok']\n";
    // Ov has a copy beside it; R's copy is in its own segment too. Only a
    // caller with no local copy calls across.
    let classes =
        "[class.global]\nsegments = ['front']\n[class.model_x]\nsegments = ['engine', 'tok']\n";
    layout(
        &manifest(nodes()),
        &placement(&format!("{segments}{classes}")),
    )
    .expect("every call is local");

    let segments = "[segment.front]\nnodes = ['S', 'R', 'Ov']\n\
        [segment.engine]\nnodes = ['D']\n[segment.tok]\nnodes = ['Tok']\n";
    let errors = refused(&manifest(nodes()), &format!("{segments}{classes}"));
    let across = |node: &str| PlacementError::CallAcrossClasses {
        node: node.into(),
        segment: "front".into(),
        service: "Tok".into(),
        from: "global".into(),
        into: "model_x".into(),
    };
    assert_eq!(errors.0, vec![across("Ov"), across("R")], "{errors}");
    let text = errors.0[1].to_string();
    assert!(text.contains("node `R`"), "{text}");
    assert!(text.contains("`calls Tok`"), "{text}");
    assert!(text.contains("class `global`"), "{text}");
    assert!(text.contains("class `model_x`"), "{text}");
    let problem = errors.0[1].problem();
    assert_eq!(problem.table.as_deref(), Some("segment.front"));
    assert_eq!(problem.key.as_deref(), Some("nodes"));

    // In the same class, the call crosses segments and is allowed.
    let same =
        "[class.global]\nsegments = ['front', 'tok']\n[class.model_x]\nsegments = ['engine']\n";
    layout(&manifest(nodes()), &placement(&format!("{segments}{same}")))
        .expect("a call inside one class");
}

#[test]
fn a_caller_with_no_local_copy_and_several_remote_ones_is_refused() {
    let errors = refused(
        &manifest(nodes()),
        "[segment.front]\nnodes = ['S', 'R', 'D', 'Ov', 'Tok']\n\
         [segment.lone]\nnodes = ['Ov']\n\
         [segment.a]\nnodes = ['Tok']\n[segment.b]\nnodes = ['Tok']\n",
    );
    // Ov's copy in `front` has Tok beside it; the copy in `lone` does not.
    let (error, problem) = only(&errors);
    assert_eq!(
        error,
        &PlacementError::CallAmbiguous {
            node: "Ov".into(),
            segment: "lone".into(),
            service: "Tok".into(),
            segments: vec!["a".into(), "b".into(), "front".into()],
        }
    );
    let text = error.to_string();
    assert!(text.contains("node `Ov`"), "{text}");
    assert!(text.contains("`calls Tok`"), "{text}");
    assert!(text.contains("segment `lone`"), "{text}");
    assert_eq!(problem.table.as_deref(), Some("segment.lone"));
    assert_eq!(problem.key.as_deref(), Some("nodes"));

    // One remote copy is the one to call.
    layout(
        &manifest(nodes()),
        &placement(
            "[segment.front]\nnodes = ['S', 'R', 'D', 'Ov']\n[segment.tok]\nnodes = ['Tok']\n",
        ),
    )
    .expect("one remote copy");
}

#[test]
fn a_routing_key_may_name_a_unary_node() {
    let split = "[segment.front]\nnodes = ['S', 'R', 'D', 'Ov']\n[segment.tok]\nnodes = ['Tok']\n";
    let laid = layout(
        &manifest(nodes()),
        &placement(&format!("{split}[routing]\nTok = 'random'\n")),
    )
    .expect("a router for a unary node");
    assert_eq!(laid.call_router("Tok"), &RequestRouter::Random);
    assert_eq!(laid.call_router("Ov"), &RequestRouter::RoundRobin);
    // Spelling the default names the node as any router does.
    layout(
        &manifest(nodes()),
        &placement(&format!("{split}[routing]\nTok = 'round_robin'\n")),
    )
    .expect("a default router for a unary node");

    // R starts no remote forward run and is no unary node.
    for router in ["'random'", "'round_robin'"] {
        let errors = refused(
            &manifest(nodes()),
            &format!("{split}[routing]\nR = {router}\n"),
        );
        let (error, problem) = only(&errors);
        assert_eq!(error, &PlacementError::InvalidRouter("R".into()));
        let text = error.to_string();
        assert!(text.contains("a unary node"), "{text}");
        assert_eq!(problem.table.as_deref(), Some("routing"));
        assert_eq!(problem.key.as_deref(), Some("R"));
    }
}

#[test]
fn a_manifest_must_give_each_unary_node_its_types() {
    let mut untyped = nodes();
    untyped[3].unary = None;
    let errors = refused(&manifest(untyped), ONE);
    assert!(
        errors
            .to_string()
            .contains("unary node `Tok` has no request and response types"),
        "{errors}"
    );

    let mut typed_sink = nodes();
    typed_sink[2].unary = nodes()[3].unary.clone();
    let errors = refused(&manifest(typed_sink), ONE);
    assert!(
        errors
            .to_string()
            .contains("node `D` is a sink, but has a unary node's types"),
        "{errors}"
    );

    let mut streamed = nodes();
    streamed[3] = unary(
        "Tok",
        TypeInfo::streamed(
            "Head",
            true,
            Encoding::default(),
            "Text",
            true,
            Encoding::default(),
        ),
        ty("Tokens"),
    );
    let errors = refused(&manifest(streamed), ONE);
    assert!(
        errors
            .to_string()
            .contains("unary node `Tok`: its request `Head + stream Text` is streamed"),
        "{errors}"
    );

    let mut twice = nodes();
    twice[1] = calls(node("R", Role::Operator), &["Tok", "Tok"]);
    let errors = refused(&manifest(twice), ONE);
    assert!(
        errors
            .to_string()
            .contains("node `R` has `calls Tok` twice"),
        "{errors}"
    );
}

#[test]
fn a_manifest_with_calls_reads_back_and_one_without_writes_none() {
    let with = manifest(nodes());
    let text = with.to_toml();
    assert_eq!(Manifest::from_toml(&text).expect("read back"), with);

    // A graph without unary nodes writes what it wrote before them.
    let plain = Manifest {
        nodes: vec![
            node("S", Role::Source),
            node("R", Role::Operator),
            node("D", Role::Sink),
        ],
        ..manifest(vec![])
    };
    let text = plain.to_toml();
    assert!(!text.contains("calls"), "{text}");
    assert!(!text.contains("unary"), "{text}");
    assert_eq!(Manifest::from_toml(&text).expect("read back"), plain);
}

/// A diagram of a graph with unary nodes fails on nothing. Without a
/// layout it draws every node; with one, each segment's nodes of the
/// topology, and no unary node, which may be in several segments.
#[test]
fn a_graph_with_unary_nodes_draws() {
    let manifest = manifest(nodes());
    let laid = layout(
        &manifest,
        &placement(
            "[segment.front]\nnodes = ['S', 'R', 'D', 'Ov']\n[segment.tok]\nnodes = ['Tok']\n",
        ),
    )
    .expect("laid out");
    for layout in [None, Some(&laid)] {
        let dot = manifest
            .to_dot(layout, gdl_model::Direction::LeftToRight)
            .expect("DOT");
        assert_eq!(dot.contains("Tok"), layout.is_none(), "{dot}");
        let mermaid = manifest
            .to_mermaid(layout, gdl_model::Direction::LeftToRight)
            .expect("Mermaid");
        assert_eq!(mermaid.contains("Tok"), layout.is_none(), "{mermaid}");
    }
}

/// A unary node provides and needs no property: a manifest that lists one
/// as a provider or a reader is refused, naming the node and the clause.
#[test]
fn a_unary_node_cannot_provide_or_need_a_property() {
    let with = |providers: Vec<String>, needs: Vec<gdl_model::PropertyNeed>| Manifest {
        properties: vec![gdl_model::PropertyInfo {
            kind: "model".into(),
            ty: ty("Model"),
            providers,
            needs,
        }],
        ..manifest(nodes())
    };
    let errors = refused(&with(vec!["Tok".into()], vec![]), ONE);
    assert!(
        errors.to_string().contains(
            "unary node `Tok` has `provides model`; a unary node cannot provide or need a property"
        ),
        "{errors}"
    );
    let need = gdl_model::PropertyNeed {
        node: "Ov".into(),
        class: "sets".into(),
    };
    let errors = refused(&with(vec![], vec![need]), ONE);
    assert!(
        errors.to_string().contains(
            "unary node `Ov` has `needs model from sets`; a unary node cannot provide or need a property"
        ),
        "{errors}"
    );
}

/// A call to a service in another segment is a send: the caller's segment
/// sends to the service's, so the caller's serving barrier waits for it,
/// its retirement counts it, and work reaches through it. A call to the
/// copy in the caller's own segment is no send.
#[test]
fn a_remote_call_is_a_send_from_the_callers_segment_to_the_services() {
    let split = "[segment.front]\nnodes = ['S', 'R', 'D']\n\
                 [segment.tok]\nnodes = ['Tok']\n\
                 [segment.ov]\nnodes = ['Ov']\n";
    let laid = layout(&manifest(nodes()), &placement(split)).expect("laid out");
    assert_eq!(
        laid.sends_to("front").into_iter().collect::<Vec<_>>(),
        ["tok"]
    );
    // A service that calls another sends to it too.
    assert_eq!(laid.sends_to("ov").into_iter().collect::<Vec<_>>(), ["tok"]);
    assert!(laid.sends_to("tok").is_empty());
    assert!(laid.reaches("front", "tok"));
    assert!(!laid.reaches("tok", "front"));
    assert_eq!(
        laid.calls_from("front").collect::<Vec<_>>(),
        [("Tok", "tok")]
    );
    assert_eq!(laid.calls_from("ov").collect::<Vec<_>>(), [("Tok", "tok")]);
    assert_eq!(laid.calls_from("tok").count(), 0);
    assert!(laid.called_in("Tok", "tok"));
    assert!(!laid.called_in("Ov", "ov"));
    assert!(!laid.called_in("Tok", "front"));

    // With a copy beside each caller, nothing is sent.
    let beside = "[segment.front]\nnodes = ['S', 'R', 'D', 'Tok']\n\
                  [segment.ov]\nnodes = ['Ov', 'Tok']\n";
    let laid = layout(&manifest(nodes()), &placement(beside)).expect("laid out");
    assert!(laid.sends_to("front").is_empty());
    assert!(laid.sends_to("ov").is_empty());
    assert_eq!(laid.calls_from("front").count(), 0);
}
