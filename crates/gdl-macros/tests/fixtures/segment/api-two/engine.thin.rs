
//! Segment `engine` of graph `api`, hosting `Engine`.
//!
//! [`start`] deploys it inside a program; the `api-engine` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::api_contract;
    package: "api-engine";
    segment: "engine";
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
        crosses Http_to_Engine = Engine SinkDecl Req;
        crosses Engine_to_Http = Http SourceDecl Item;
    ];
    feeds: [];
    nodes: [
        Engine engine = ::api_frontend::Echo;
    ];
    policies: [];
    serve: none;
}
