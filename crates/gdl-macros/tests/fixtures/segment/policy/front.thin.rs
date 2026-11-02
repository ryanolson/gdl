
// Async request paths nest deeply, and rustc lays out each future with those
// it holds. The limit costs compile time, never run time. See
// docs/src/development/generation-design.md in gdl.
#![recursion_limit = "256"]

//! Segment `front` of graph `policy`, hosting `Front`.
//!
//! [`start`] deploys it inside a program; the `policy-front` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::policy_contract;
    package: "policy-front";
    segment: "front";
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
    nodes: [];
    policies: [
        "by_model" = ::policy_router::ByModel;
    ];
    serve: json_lines;
}
