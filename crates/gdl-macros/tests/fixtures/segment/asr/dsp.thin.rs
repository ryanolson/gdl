
//! Segment `dsp` of graph `asr`, hosting `Resample`.
//!
//! [`start`] deploys it inside a program; the `asr-dsp` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::asr_contract;
    package: "asr-dsp";
    segment: "dsp";
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
        crosses Resample_to_Recognizer_item = Recognizer SinkDecl Req item;
    ];
    feeds: [];
    nodes: [
        Resample resample = ::asr_dsp_nodes::Resample;
    ];
    policies: [];
    serve: none;
}
