
//! Segment `engine` of graph `trio`, hosting `Engine`, `Spare`.
//!
//! [`start`] deploys it inside a program; the `trio-regional-engine` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::trio_contract;
    package: "trio-regional-engine";
    segment: "engine";
    segments: ["engine", "front", "side"];
    placement: r#"[segment.engine]
nodes = ["Engine", "Spare"]

[segment.front]
nodes = ["Ask", "Preprocess", "Truncate", "Detokenize", "Format", "Number"]

[segment.side]
nodes = ["Tell"]
"#;
    graph_config: "[Detokenize]\n\n[Engine]\n\n[Format]\n\n[Number]\n\n[Preprocess]\n\n[Spare]\n\n[Truncate]\n";
    roots: [Ask, Tell];
    crossings: [
        crosses Truncate_to_Engine = Engine SinkDecl Req;
        crosses Engine_to_Detokenize = Detokenize BackwardDecl In;
    ];
    feeds: [];
    nodes: [
        Engine engine = ::chat_engine_mock::Engine;
        Spare spare = ::chat_engine_mock::Engine;
    ];
    policies: [];
    serve: none;
}
