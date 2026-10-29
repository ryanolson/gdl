
//! Segment `trunc` of graph `chat`, hosting `Truncate`.
//!
//! [`start`] deploys it inside a program; the `chat-trunc` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::chat_contract;
    package: "chat-trunc";
    segment: "trunc";
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
    ];
    feeds: [];
    nodes: [
        Truncate truncate = ::chat_front_nodes::Truncate;
    ];
    policies: [];
    serve: none;
}
