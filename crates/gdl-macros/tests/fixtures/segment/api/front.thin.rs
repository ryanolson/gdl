
//! Segment `front` of graph `api`, hosting `Http`.
//!
//! [`start`] deploys it inside a program; the `api-front` binary deploys it on
//! its own, through [`Binary`].

gdl_runtime::segment! {
    contract: ::api_contract;
    package: "api-front";
    segment: "front";
    segments: ["engine", "front"];
    placement: r#"[segment.engine]
nodes = ["Engine"]

[segment.front]
nodes = ["Http"]
"#;
    graph_config: "[Engine]\n\n[Http]\nenabled_anthropic = true\nenabled_chat = true\nenabled_responses = true\n\n[Http_anthropic_op]\n\n[Http_chat_b0]\n\n[Http_chat_f0]\n\n[Http_responses_op]\n";
    roots: [Http];
    crossings: [
        crosses Http_to_Engine = Engine SinkDecl Req;
        crosses Engine_to_Http = Http SourceDecl Item;
    ];
    feeds: [];
    nodes: [];
    policies: [];
    serve: service {
        frontend: Http = ::api_frontend::ApiFrontend;
        clients: ::api_frontend::ApiClients;
        adapters: [
            "Http_chat_f0" entry_487474705f636861745f6630 = ::api_frontend::ExtractChat;
            "Http_chat_b0" entry_487474705f636861745f6230 = ::api_frontend::FormatChat;
            "Http_responses_op" entry_487474705f726573706f6e7365735f6f70 = ::api_frontend::ResponsesAdapter;
            "Http_anthropic_op" entry_487474705f616e7468726f7069635f6f70 = ::api_frontend::AnthropicAdapter;
        ];
        pipelines: [
            pipeline_48747470 = 0;
        ];
        entries: [
            chat = gdl_runtime::gdl::Hop::new((std::sync::Arc::new(entry_487474705f636861745f6630), ()), gdl_runtime::gdl::Join::new(pipeline_48747470.clone()), (std::sync::Arc::new(entry_487474705f636861745f6230), ()));
            responses = gdl_runtime::gdl::Hop::new((), gdl_runtime::gdl::OperatorStage::new(std::sync::Arc::new(entry_487474705f726573706f6e7365735f6f70), gdl_runtime::gdl::Hop::new((), gdl_runtime::gdl::Join::new(pipeline_48747470.clone()), ())), ());
            anthropic = gdl_runtime::gdl::Hop::new((), gdl_runtime::gdl::OperatorStage::new(std::sync::Arc::new(entry_487474705f616e7468726f7069635f6f70), gdl_runtime::gdl::Hop::new((), gdl_runtime::gdl::Join::new(pipeline_48747470.clone()), ())), ());
        ];
    };
}
