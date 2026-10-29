
//! Segment `side` of graph `trio`, hosting `Tell`.
//!
//! [`start`] deploys it inside a program; the `trio-regional-side` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::trio_contract;
    package: "trio-regional-side";
    segment: "side";
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
        crosses Tell_to_Spare = Spare SinkDecl Req;
        crosses Spare_to_Tell = Tell SourceDecl Item;
    ];
    feeds: [];
    nodes: [];
    policies: [];
    serve: none;
}
