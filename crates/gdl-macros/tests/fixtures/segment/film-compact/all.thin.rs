
// Async request paths nest deeply, and rustc lays out each future with those
// it holds. The limit costs compile time, never run time. See
// docs/src/development/generation-design.md in gdl.
#![recursion_limit = "256"]

//! Segment `all` of graph `film`, hosting `Http`, `Engine`, `UsageTap`.
//!
//! [`start`] deploys it inside a program; the `film-all` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::film_contract;
    package: "film-all";
    segment: "all";
    segments: ["all"];
    placement: r#"[segment.all]
nodes = ["Http", "Engine", "UsageTap"]
"#;
    graph_config: "[Engine]\nmodel = \"film-engine\"\nuppercase = true\n\n[Http]\n\n[UsageTap]\nenabled = true\ntenant = \"acme-film\"\n";
    roots: [Http];
    crossings: [];
    feeds: [];
    nodes: [
        Engine engine = ::gdl_demo_nodes::MockEngine;
        UsageTap usage_tap = ::acme_gdl_nodes::UsageTap;
    ];
    policies: [];
    serve: frontend Http = ::gdl_demo_nodes::HttpFrontend;
}
