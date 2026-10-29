
//! Segment `engine` of graph `asr`, hosting `Recognizer`, `Punctuate`.
//!
//! [`start`] deploys it inside a program; the `asr-engine` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::asr_contract;
    package: "asr-engine";
    segment: "engine";
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
        crosses Resample_to_Recognizer_head = Recognizer SinkDecl Req head;
        crosses Resample_to_Recognizer_item = Recognizer SinkDecl Req item;
        crosses Punctuate_to_Gateway = Gateway OperatorDecl DownItem;
    ];
    feeds: [];
    nodes: [
        Recognizer recognizer = ::asr_engine_mock::Recognizer;
        Punctuate punctuate = ::asr_engine_mock::Punctuate;
    ];
    policies: [];
    serve: none;
}
