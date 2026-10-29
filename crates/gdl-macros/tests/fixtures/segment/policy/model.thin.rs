
//! Segment `model` of graph `policy`, hosting `Model`.
//!
//! [`start`] deploys it inside a program; the `policy-model` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::policy_contract;
    package: "policy-model";
    segment: "model";
    segments: ["front", "model"];
    placement: r#"[segment.front]
nodes = ["Front"]

[segment.model]
nodes = ["Model"]

[class.global]
segments = ["front"]

[class.model]
segments = ["model"]

[pools.model]
policy = "by_model"
"#;
    graph_config: "[Model]\n";
    roots: [Front];
    crossings: [
        crosses Front_to_Model = Model SinkDecl Req;
        crosses Model_to_Front = Front SourceDecl Item;
    ];
    feeds: [];
    nodes: [
        Model model = ::policy_nodes::Model;
    ];
    policies: [];
    serve: none;
}
