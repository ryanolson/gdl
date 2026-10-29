
//! Segment `engine` of graph `chat`, hosting `Engine`.
//!
//! [`start`] deploys it inside a program; the `chat-engine` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::chat_contract;
    package: "chat-engine";
    segment: "engine";
    segments: ["engine", "front", "post"];
    placement: r#"[segment.engine]
nodes = ["Engine"]

[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number"]

[segment.post]
nodes = ["Detokenize", "Format"]
"#;
    graph_config: "[Detokenize]\n\n[Engine]\n\n[Format]\n\n[Number]\n\n[Preprocess]\n\n[Truncate]\n";
    roots: [Frontend];
    crossings: [
        crosses Truncate_to_Engine = Engine SinkDecl Req;
        crosses Engine_to_Detokenize = Detokenize BackwardDecl In;
    ];
    feeds: [];
    nodes: [
        Engine engine = ::chat_engine_mock::Engine;
    ];
    policies: [];
    serve: none;
}
