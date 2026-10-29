
//! Segment `front` of graph `chat`, hosting `Frontend`, `Preprocess`, `Truncate`, `Number`.
//!
//! [`start`] deploys it inside a program; the `chat-front` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::chat_contract;
    package: "chat-front";
    segment: "front";
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
        crosses Format_to_Preprocess = Preprocess OperatorDecl DownItem;
    ];
    feeds: [reads_json, writes_json];
    nodes: [
        Preprocess preprocess = ::chat_front_nodes::Preprocess;
        Truncate truncate = ::chat_front_nodes::Truncate;
        Number number = ::chat_front_nodes::Number;
    ];
    policies: [];
    serve: json_lines;
}
