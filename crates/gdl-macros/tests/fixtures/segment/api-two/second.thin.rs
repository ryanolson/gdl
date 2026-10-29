
//! Segment `second` of graph `api`, hosting `SecondEngine`.
//!
//! [`start`] deploys it inside a program; the `api-second` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::api_contract;
    package: "api-second";
    segment: "second";
    segments: ["engine", "front", "second"];
    placement: r#"[segment.engine]
nodes = ["Engine"]

[segment.front]
nodes = ["Http", "SecondPipeline"]

[segment.second]
nodes = ["SecondEngine"]
"#;
    graph_config: "[Engine]\n\n[Http]\nenabled_anthropic = true\nenabled_chat = true\nenabled_responses = true\n\n[Http_anthropic_op]\n\n[Http_chat_b0]\n\n[Http_chat_f0]\n\n[Http_responses_op]\n\n[SecondEngine]\n";
    roots: [Http, SecondPipeline];
    crossings: [
        crosses SecondPipeline_to_SecondEngine = SecondEngine SinkDecl Req;
        crosses SecondEngine_to_SecondPipeline = SecondPipeline SourceDecl Item;
    ];
    feeds: [];
    nodes: [
        SecondEngine second_engine = ::api_frontend::Echo;
    ];
    policies: [];
    serve: none;
}
