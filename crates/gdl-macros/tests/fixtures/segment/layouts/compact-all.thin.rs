
// Async request paths nest deeply, and rustc lays out each future with those
// it holds. The limit costs compile time, never run time. See
// docs/src/development/generation-design.md in gdl.
#![recursion_limit = "256"]

//! Segment `all` of graph `trio`, hosting `Ask`, `Tell`, `Preprocess`, `Truncate`, `Detokenize`, `Format`, `Number`, `Engine`, `Spare`.
//!
//! [`start`] deploys it inside a program; the `trio-compact-all` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::trio_contract;
    package: "trio-compact-all";
    segment: "all";
    segments: ["all"];
    placement: r#"[segment.all]
nodes = ["Ask", "Tell", "Preprocess", "Truncate", "Detokenize", "Format", "Number", "Engine", "Spare"]
"#;
    graph_config: "[Detokenize]\n\n[Engine]\n\n[Format]\n\n[Number]\n\n[Preprocess]\n\n[Spare]\n\n[Truncate]\n";
    roots: [Ask, Tell];
    crossings: [];
    feeds: [];
    nodes: [
        Preprocess preprocess = ::chat_front_nodes::Preprocess;
        Truncate truncate = ::chat_front_nodes::Truncate;
        Engine engine = ::chat_engine_mock::Engine;
        Spare spare = ::chat_engine_mock::Engine;
        Detokenize detokenize = ::chat_post_nodes::Detokenize;
        Format format = ::chat_post_nodes::Format;
        Number number = ::chat_front_nodes::Number;
    ];
    policies: [];
    serve: none;
}
