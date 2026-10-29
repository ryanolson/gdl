
//! Segment `post` of graph `chat`, hosting `Detokenize`, `Format`.
//!
//! [`start`] deploys it inside a program; the `chat-post` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::chat_contract;
    package: "chat-post";
    segment: "post";
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
        crosses Engine_to_Detokenize = Detokenize BackwardDecl In;
        crosses Format_to_Preprocess = Preprocess OperatorDecl DownItem;
    ];
    feeds: [];
    nodes: [
        Detokenize detokenize = ::chat_post_nodes::Detokenize;
        Format format = ::chat_post_nodes::Format;
    ];
    policies: [];
    serve: none;
}
