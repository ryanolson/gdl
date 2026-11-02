
// Async request paths nest deeply, and rustc lays out each future with those
// it holds. The limit costs compile time, never run time. See
// docs/src/development/generation-design.md in gdl.
#![recursion_limit = "256"]

//! Segment `front` of graph `asr`, hosting `Client`, `Gateway`.
//!
//! [`start`] deploys it inside a program; the `asr-front` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::asr_contract;
    package: "asr-front";
    segment: "front";
    segments: ["dsp", "engine", "front"];
    placement: r#"[segment.dsp]
nodes = ["Resample"]

[segment.engine]
nodes = ["Recognizer", "Punctuate"]

[segment.front]
nodes = ["Client", "Gateway"]
"#;
    graph_config: "[Gateway]\n\n[Punctuate]\n\n[Recognizer]\n\n[Resample]\n";
    roots: [Client];
    crossings: [
        crosses Gateway_to_Resample_head = Resample ForwardDecl In head;
        crosses Gateway_to_Resample_item = Resample ForwardDecl In item;
        crosses Punctuate_to_Gateway = Gateway OperatorDecl DownItem;
    ];
    feeds: [];
    nodes: [
        Gateway gateway = ::asr_front_nodes::Gateway;
    ];
    policies: [];
    serve: none;
}
