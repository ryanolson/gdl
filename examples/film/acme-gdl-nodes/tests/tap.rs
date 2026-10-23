use acme_gdl_nodes::{TapConfig, TapMetrics, UsageEvent, UsageTap};
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use futures::StreamExt;
use gdl::{Backward, Build, ConfigSchema, Cx, Metrics};
use gdl_demo_nodes::{Chunk, Usage};
use std::time::Duration;

#[tokio::test]
async fn accounting_preserves_responses_and_sends_at_most_one_known_event() {
    let receiver = Receiver::start(StatusCode::ACCEPTED).await;
    let usage = Usage {
        model: "film-model".into(),
        input_tokens: 2,
        output_tokens: 2,
    };
    let text = Chunk::Text {
        index: 0,
        text: "unchanged".into(),
    };
    let cases = [
        (true, vec![text.clone(), Chunk::Usage(usage.clone())], true),
        (
            true,
            vec![
                text.clone(),
                Chunk::Usage(usage.clone()),
                Chunk::Usage(usage.clone()),
            ],
            true,
        ),
        (true, vec![text.clone()], false),
        (
            false,
            vec![text.clone(), Chunk::Usage(usage.clone())],
            false,
        ),
    ];
    let mut events = receiver.events;
    for (enabled, input, expected_event) in cases {
        let registry = gdl::prometheus::Registry::new();
        let tap = UsageTap::build(
            TapConfig {
                tenant: "acme-film".into(),
                enabled,
                accounting_url: receiver.url.clone(),
            },
            TapMetrics::register(&registry, "Usage").expect("metrics"),
        )
        .await
        .expect("tap");
        let output: Vec<_> = tap
            .backward(
                &Cx::new(),
                gdl::items(futures::stream::iter(input.clone().into_iter().map(Ok))),
            )
            .map(|item| item.expect("unchanged chunk"))
            .collect()
            .await;
        assert_eq!(output, input);
        if expected_event {
            let event = events.try_recv().expect("completed accounting POST");
            assert_eq!(
                event,
                UsageEvent {
                    tenant: "acme-film".into(),
                    usage: usage.clone()
                }
            );
        }
        assert!(
            events.try_recv().is_err(),
            "no duplicate, absent, or disabled usage event"
        );
        assert_eq!(
            counter(&registry, "accounting_events_total"),
            f64::from(u8::from(expected_event))
        );
        assert_eq!(counter(&registry, "accounting_errors_total"), 0.0);
    }
    receiver.stop.send(()).expect("stop");
    tokio::time::timeout(Duration::from_secs(2), receiver.task)
        .await
        .expect("shutdown deadline")
        .expect("task");
}

#[tokio::test]
async fn accounting_failure_preserves_output_and_counts_one_error_without_retry() {
    let receiver = Receiver::start(StatusCode::INTERNAL_SERVER_ERROR).await;
    let registry = gdl::prometheus::Registry::new();
    let tap = UsageTap::build(
        TapConfig {
            tenant: "acme-film".into(),
            enabled: true,
            accounting_url: receiver.url.clone(),
        },
        TapMetrics::register(&registry, "Usage").expect("metrics"),
    )
    .await
    .expect("tap");
    let usage = Chunk::Usage(Usage {
        model: "film-model".into(),
        input_tokens: 3,
        output_tokens: 3,
    });
    let input = vec![
        Chunk::Text {
            index: 0,
            text: "still delivered".into(),
        },
        usage.clone(),
        usage,
    ];
    let output: Vec<_> = tap
        .backward(
            &Cx::new(),
            gdl::items(futures::stream::iter(input.clone().into_iter().map(Ok))),
        )
        .map(|item| item.expect("preserved response"))
        .collect()
        .await;
    assert_eq!(output, input);
    let mut events = receiver.events;
    assert!(events.try_recv().is_ok());
    assert!(events.try_recv().is_err());
    assert_eq!(counter(&registry, "accounting_events_total"), 0.0);
    assert_eq!(counter(&registry, "accounting_errors_total"), 1.0);
    receiver.stop.send(()).expect("stop");
    tokio::time::timeout(Duration::from_secs(2), receiver.task)
        .await
        .expect("shutdown deadline")
        .expect("task");
}

#[tokio::test]
async fn upstream_errors_are_preserved_without_accounting() {
    let mut receiver = Receiver::start(StatusCode::ACCEPTED).await;
    let registry = gdl::prometheus::Registry::new();
    let tap = UsageTap::build(
        TapConfig {
            tenant: "acme-film".into(),
            enabled: true,
            accounting_url: receiver.url.clone(),
        },
        TapMetrics::register(&registry, "Usage").expect("metrics"),
    )
    .await
    .expect("tap");
    let input = gdl::items(futures::stream::iter([Err(gdl::Error::node(
        "upstream stopped",
    ))]));
    let mut output = tap.backward(&Cx::new(), input);
    assert_eq!(
        output
            .next()
            .await
            .expect("error item")
            .expect_err("same error")
            .to_string(),
        gdl::Error::node("upstream stopped").to_string()
    );
    assert!(output.next().await.is_none());
    assert_eq!(counter(&registry, "accounting_events_total"), 0.0);
    assert_eq!(counter(&registry, "accounting_errors_total"), 0.0);
    assert!(receiver.events.try_recv().is_err());
    receiver.stop.send(()).expect("stop");
    tokio::time::timeout(Duration::from_secs(2), receiver.task)
        .await
        .expect("shutdown deadline")
        .expect("task");
}

#[test]
fn endpoint_is_runtime_only_and_reexported_chunks_keep_the_reference_identity() {
    assert_eq!(
        TapConfig::graph_fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["tenant", "enabled"]
    );
    assert_eq!(
        <acme_gdl_nodes::Chunk as gdl::Message>::IDENTITY,
        <Chunk as gdl::Message>::IDENTITY
    );
}

fn counter(registry: &gdl::prometheus::Registry, name: &str) -> f64 {
    registry
        .gather()
        .into_iter()
        .find(|metric| metric.name() == name)
        .expect("metric")
        .get_metric()[0]
        .get_counter()
        .get_value()
}

struct Receiver {
    url: String,
    events: tokio::sync::mpsc::UnboundedReceiver<UsageEvent>,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Receiver {
    async fn start(status: StatusCode) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let url = format!(
            "http://{}/usage",
            listener.local_addr().expect("bound address")
        );
        let (sender, events) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new()
            .route("/usage", post(receive))
            .with_state((sender, status));
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown.await;
                })
                .await
                .expect("receiver");
        });
        Self {
            url,
            events,
            stop,
            task,
        }
    }
}
async fn receive(
    State((sender, status)): State<(tokio::sync::mpsc::UnboundedSender<UsageEvent>, StatusCode)>,
    Json(event): Json<UsageEvent>,
) -> StatusCode {
    sender.send(event).expect("event receiver");
    status
}
