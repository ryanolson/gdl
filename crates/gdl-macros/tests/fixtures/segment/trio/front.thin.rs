
//! Segment `front` of graph `trio`, hosting `Ask`, `Tell`, `Preprocess`, `Truncate`, `Detokenize`, `Format`, `Number`.
//!
//! [`start`] deploys it inside a program; the `trio-front` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::trio_contract;
    package: "trio-front";
    segment: "front";
    segments: ["engine", "front"];
    placement: r#"[segment.engine]
nodes = ["Engine", "Spare"]

[segment.front]
nodes = ["Ask", "Tell", "Preprocess", "Truncate", "Detokenize", "Format", "Number"]
"#;
    graph_config: "[Detokenize]\n\n[Engine]\n\n[Format]\n\n[Number]\n\n[Preprocess]\n\n[Spare]\n\n[Truncate]\n";
    roots: [Ask, Tell];
    crossings: [
        crosses Truncate_to_Engine = Engine SinkDecl Req;
        crosses Engine_to_Detokenize = Detokenize BackwardDecl In;
    ];
    feeds: [];
    nodes: [
        Preprocess preprocess = ::chat_front_nodes::Preprocess;
        Truncate truncate = ::chat_front_nodes::Truncate;
        Detokenize detokenize = ::chat_post_nodes::Detokenize;
        Format format = ::chat_post_nodes::Format;
        Number number = ::chat_front_nodes::Number;
    ];
    policies: [];
    serve: none;
}
