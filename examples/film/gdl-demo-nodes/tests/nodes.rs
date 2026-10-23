use futures::StreamExt;
use gdl::{Build, ConfigSchema, Cx, Frontend, Metrics, Next, Sink};
use gdl_demo_nodes::{
    Chunk, EngineConfig, EngineMetrics, FrontendConfig, HttpFrontend, MockEngine, Prompt, Usage,
};
use std::{sync::Arc, time::Duration};

fn engine_config(uppercase: bool) -> EngineConfig {
    EngineConfig {
        model: "film-model".into(),
        uppercase,
        delay_ms: 0,
    }
}

#[tokio::test]
async fn engine_emits_deterministic_text_terminal_usage_and_custom_metrics() {
    let registry = gdl::prometheus::Registry::new();
    let metrics = EngineMetrics::register(&registry, "Engine").expect("metrics");
    let engine = MockEngine::build(engine_config(true), metrics)
        .await
        .expect("engine");
    let output: Vec<_> = engine
        .generate(
            Cx::new(),
            Prompt {
                text: "Hello graph".into(),
            },
        )
        .await
        .expect("stream")
        .map(|item| item.expect("chunk"))
        .collect()
        .await;
    assert_eq!(
        output,
        vec![
            Chunk::Text {
                index: 0,
                text: "HELLO".into()
            },
            Chunk::Text {
                index: 1,
                text: "GRAPH".into()
            },
            Chunk::Usage(Usage {
                model: "film-model".into(),
                input_tokens: 2,
                output_tokens: 2
            })
        ]
    );
    let count = |name| {
        registry
            .gather()
            .into_iter()
            .find(|metric| metric.name() == name)
            .expect("metric")
            .get_metric()[0]
            .get_counter()
            .get_value()
    };
    assert_eq!(count("mock_requests_total"), 1.0);
    assert_eq!(count("mock_chunks_total"), 3.0);
}

#[tokio::test]
async fn an_empty_prompt_still_emits_one_zero_usage_record() {
    let registry = gdl::prometheus::Registry::new();
    let engine = MockEngine::build(
        engine_config(false),
        EngineMetrics::register(&registry, "Engine").expect("metrics"),
    )
    .await
    .expect("engine");
    let output: Vec<_> = engine
        .generate(Cx::new(), Prompt { text: "  ".into() })
        .await
        .expect("stream")
        .map(|item| item.expect("chunk"))
        .collect()
        .await;
    assert_eq!(
        output,
        vec![Chunk::Usage(Usage {
            model: "film-model".into(),
            input_tokens: 0,
            output_tokens: 0
        })]
    );
}

#[test]
fn only_graph_settings_are_exposed_and_protocols_are_wire_messages() {
    assert_eq!(
        EngineConfig::graph_fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["model", "uppercase"]
    );
    assert!(FrontendConfig::graph_fields().is_empty());
    let catalog = gdl::Catalogue::linked();
    for name in ["Prompt", "Chunk", "Usage"] {
        assert!(
            catalog
                .messages
                .iter()
                .any(|message| message.name == name && message.wire),
            "wire message {name}"
        );
    }
}

#[tokio::test]
async fn frontend_serves_the_same_typed_stream_over_http_and_drains() {
    let registry = gdl::prometheus::Registry::new();
    let engine = MockEngine::build(
        engine_config(false),
        EngineMetrics::register(&registry, "Engine").expect("metrics"),
    )
    .await
    .expect("engine");
    let frontend = HttpFrontend::build(
        FrontendConfig {
            listen: "127.0.0.1:0".parse().expect("address"),
        },
        (),
    )
    .await
    .expect("frontend");
    let address = frontend.local_addr().expect("bound listener");
    let (stop, shutdown) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(frontend.serve(EngineNext(Arc::new(engine)), async {
        let _ = shutdown.await;
    }));
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client")
        .post(format!("http://{address}/generate"))
        .json(&Prompt {
            text: "hello film".into(),
        })
        .send()
        .await
        .expect("HTTP response");
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "application/x-ndjson");
    let body = response.text().await.expect("body");
    let chunks: Vec<Chunk> = body
        .lines()
        .map(|line| serde_json::from_str(line).expect("typed chunk"))
        .collect();
    assert_eq!(
        chunks,
        vec![
            Chunk::Text {
                index: 0,
                text: "hello".into()
            },
            Chunk::Text {
                index: 1,
                text: "film".into()
            },
            Chunk::Usage(Usage {
                model: "film-model".into(),
                input_tokens: 2,
                output_tokens: 2
            })
        ]
    );
    stop.send(()).expect("shutdown signal");
    tokio::time::timeout(Duration::from_secs(2), serving)
        .await
        .expect("shutdown deadline")
        .expect("task")
        .expect("serve");
}

#[derive(Clone)]
struct EngineNext(Arc<MockEngine>);
impl Next<Prompt, Chunk> for EngineNext {
    async fn generate(&self, cx: Cx, request: Prompt) -> gdl::Result<gdl::Items<Chunk>> {
        self.0.generate(cx, request).await
    }
}
