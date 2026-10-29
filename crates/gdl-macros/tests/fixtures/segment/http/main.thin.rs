
//! Segment `main` of graph `http`, hosting `Http`, `Engine`.
//!
//! [`start`] deploys it inside a program; the `http-main` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::http_contract;
    package: "http-main";
    segment: "main";
    segments: ["main"];
    placement: r#"[segment.main]
nodes = ["Http", "Engine"]
"#;
    graph_config: "[Engine]\n\n[Http]\n";
    roots: [Http];
    crossings: [];
    feeds: [];
    nodes: [
        Engine engine = ::http_frontend::Echo;
    ];
    policies: [];
    serve: frontend Http = ::http_frontend::HttpFrontend;
}
