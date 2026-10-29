
//! Segment `engine` of graph `api`, hosting `Engine`.
//!
//! [`start`] deploys it inside a program; the `api-engine` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::api_contract;
    package: "api-engine";
    segment: "engine";
    segments: ["engine", "front"];
    placement: r#"[segment.engine]
nodes = ["Engine"]

[segment.front]
nodes = ["Http"]
"#;
    graph_config: "[Engine]\n\n[Http]\nenabled_anthropic = false\nenabled_chat = true\nenabled_responses = false\n\n[Http_chat_b0]\n\n[Http_chat_f0]\n";
    roots: [Http];
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
