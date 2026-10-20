//! Independent pipeline roots and explicit topology ownership.

use gdl_syntax::Graph;

#[test]
fn independent_roots_keep_identical_contracts_separate() {
    let graph = Graph::parse(
        r#"
        name = "branches", schema = 1;
        source Chat: Request -> stream Reply;
        source Responses: Request -> stream Reply;
        sink ChatWorker: Request -> stream Reply;
        sink ResponsesWorker: Request -> stream Reply;
        topology: Chat => ChatWorker;
        topology: Responses => ResponsesWorker;
    "#
        .parse()
        .expect("tokens"),
    )
    .expect("independent roots");
    assert_eq!(graph.hops.len(), 2);
    assert_eq!(graph.hops[0].inner, "ChatWorker");
    assert_eq!(graph.hops[1].inner, "ResponsesWorker");
    let manifest = graph
        .manifest(&|name| Some((format!("protocol::{name}"), true)))
        .expect("manifest");
    let placement = gdl_model::Placement::from_toml("[segment.front]\nnodes=['Chat','Responses']\n[segment.chat]\nnodes=['ChatWorker']\n[segment.responses]\nnodes=['ResponsesWorker']").expect("placement");
    gdl_model::layout(&manifest, &placement).expect("independent segment placement");
}

#[test]
fn explicit_sink_references_share_one_node() {
    let graph = Graph::parse(
        r#"
        name = "branches", schema = 1;
        source Chat: Request -> stream Reply;
        source Responses: Request -> stream Reply;
        sink Worker: Request -> stream Reply;
        topology: Chat => Worker;
        topology: Responses => Worker;
    "#
        .parse()
        .expect("tokens"),
    )
    .expect("explicit shared sink");
    assert_eq!(graph.nodes.len(), 3);
    let manifest = graph
        .manifest(&|name| Some((format!("protocol::{name}"), true)))
        .expect("manifest");
    let placement = gdl_model::Placement::new()
        .segment("front", ["Chat", "Responses"])
        .segment("worker", ["Worker"]);
    let layout = gdl_model::layout(&manifest, &placement).expect("shared sink placement");
    assert!(
        layout
            .report()
            .to_string()
            .contains("one request per root: 4 crossings")
    );
}

#[test]
fn identity_operators_have_independent_placement() {
    let graph = Graph::parse(
        r#"
        name = "four", schema = 1;
        source Chat: Request -> stream Reply;
        source Responses: Request -> stream Reply;
        identity ChatProcessing: Request -> stream Reply;
        identity ResponsesProcessing: Request -> stream Reply;
        sink Worker: Request -> stream Reply;
        topology: Chat => ChatProcessing { Worker };
        topology: Responses => ResponsesProcessing { Worker };
    "#
        .parse()
        .expect("valid identity fixture"),
    )
    .expect("pipeline syntax");
    let manifest = graph
        .manifest(&|name| Some((format!("protocol::{name}"), true)))
        .expect("valid identity fixture");
    assert_eq!(
        manifest
            .node("ChatProcessing")
            .expect("valid identity fixture")
            .role,
        gdl_model::Role::Operator
    );
    assert!(
        manifest
            .node("ChatProcessing")
            .expect("valid identity fixture")
            .binding
            .is_none()
    );
    let placement = gdl_model::Placement::new()
        .segment("front", ["Chat", "Responses"])
        .segment("chat", ["ChatProcessing"])
        .segment("responses", ["ResponsesProcessing"])
        .segment("sink", ["Worker"]);
    let layout = gdl_model::layout(&manifest, &placement).expect("four segments");
    assert_eq!(layout.report().crossings(), 8);
}

#[test]
fn identity_ports_cannot_change_message_types_in_a_manifest() {
    let graph = Graph::parse(
        r#"name="identity", schema=1;
        source Input: Req -> stream Reply;
        identity F: request Req;
        identity O: Req -> stream Reply;
        identity B: response Reply;
        sink End: Req -> stream Reply;
        topology: Input => F => O { End } => B;
    "#
        .parse()
        .expect("valid identity fixture"),
    )
    .expect("valid identity fixture");
    let manifest = graph
        .manifest(&|name| Some((name.into(), true)))
        .expect("valid identity fixture");
    let placement = gdl_model::Placement::new().segment("all", ["Input", "F", "O", "B", "End"]);
    gdl_model::layout(&manifest, &placement).expect("valid identity fixture");
    for (hop, forward, index) in [(0, true, 0), (0, false, 1), (1, true, 0), (1, false, 0)] {
        let mut broken = manifest.clone();
        let types = if forward {
            &mut broken.hops[hop].forward_types
        } else {
            &mut broken.hops[hop].backward_types
        };
        types[index].name = "Different".into();
        assert!(
            gdl_model::layout(&broken, &placement).is_err(),
            "identity changed its ports"
        );
    }
}
