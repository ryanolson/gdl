
//! Segment `core` of graph `chat`, hosting `Frontend`, `Preprocess`, `Engine`, `Detokenize`, `Format`, `Number`.
//!
//! [`start`] deploys it inside a program; the `chat-core` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::chat_contract;
    package: "chat-core";
    segment: "core";
    segments: ["core", "trunc"];
    placement: r#"[segment.core]
nodes = ["Frontend", "Preprocess", "Engine", "Detokenize", "Format", "Number"]

[segment.trunc]
nodes = ["Truncate"]
"#;
    graph_config: "[Detokenize]\n\n[Engine]\n\n[Format]\n\n[Number]\n\n[Preprocess]\n\n[Truncate]\n";
    roots: [Frontend];
    crossings: [
        crosses Preprocess_to_Truncate = Truncate ForwardDecl In;
        anchors Engine_to_Detokenize = Detokenize BackwardDecl In;
    ];
    feeds: [reads_json, writes_json];
    nodes: [
        Preprocess preprocess = ::chat_front_nodes::Preprocess;
        Engine engine = ::chat_engine_mock::Engine;
        Detokenize detokenize = ::chat_post_nodes::Detokenize;
        Format format = ::chat_post_nodes::Format;
        Number number = ::chat_front_nodes::Number;
    ];
    policies: [];
    serve: json_lines;
}
