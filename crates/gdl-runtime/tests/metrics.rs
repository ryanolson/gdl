//! A node's own metrics: `derive(gdl::Metrics)` builds each field, labels
//! it with the node, and registers it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use gdl::prometheus::{Encoder, Registry, TextEncoder};
use gdl::{MetricField, MetricKind, Metrics};

/// Engine metrics, of every kind.
#[derive(gdl::Metrics)]
struct EngineMetrics {
    /// Tokens generated.
    tokens_total: gdl::prometheus::IntCounter,
    /// Requests refused, by reason.
    #[gdl(labels = ["reason"])]
    refused_total: gdl::prometheus::IntCounterVec,
    /// Sequences in the batch.
    active_sequences: gdl::prometheus::IntGauge,
    /// Time to the first token.
    #[gdl(buckets = [0.01, 0.1, 1.0])]
    time_to_first_token_seconds: gdl::prometheus::Histogram,
}

fn text(registry: &Registry) -> String {
    let mut out = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut out)
        .expect("text format");
    String::from_utf8(out).expect("utf-8")
}

#[test]
fn definitions_follow_the_fields() {
    let definitions = EngineMetrics::definitions();
    assert_eq!(
        definitions[0],
        MetricField {
            name: "tokens_total".into(),
            help: "Tokens generated.".into(),
            kind: MetricKind::Counter,
            labels: vec![],
            buckets: vec![],
        }
    );
    assert_eq!(definitions[1].labels, ["reason"]);
    assert_eq!(definitions[2].kind, MetricKind::Gauge);
    assert_eq!(definitions[3].buckets, [0.01, 0.1, 1.0]);
    assert!(<() as Metrics>::definitions().is_empty());
}

#[test]
fn registered_metrics_carry_the_node_and_count() {
    let registry = Registry::new();
    let engine = EngineMetrics::register(&registry, "Engine").expect("registered");
    engine.tokens_total.inc_by(3);
    engine.refused_total.with_label_values(&["full"]).inc();
    engine.active_sequences.set(2);
    engine.time_to_first_token_seconds.observe(0.05);
    let scraped = text(&registry);
    for line in [
        "# HELP tokens_total Tokens generated.",
        "# TYPE tokens_total counter",
        "tokens_total{dynamo_endpoint=\"Engine\"} 3",
        "refused_total{dynamo_endpoint=\"Engine\",reason=\"full\"} 1",
        "active_sequences{dynamo_endpoint=\"Engine\"} 2",
        "time_to_first_token_seconds_bucket{dynamo_endpoint=\"Engine\",le=\"0.1\"} 1",
        "time_to_first_token_seconds_count{dynamo_endpoint=\"Engine\"} 1",
    ] {
        assert!(scraped.contains(line), "{line:?} missing from:\n{scraped}");
    }
    // Two nodes of one type register side by side, by their names.
    let other = EngineMetrics::register(&registry, "Other").expect("a second node");
    other.tokens_total.inc();
    let scraped = text(&registry);
    assert!(
        scraped.contains("tokens_total{dynamo_endpoint=\"Other\"} 1"),
        "{scraped}"
    );
    // The same node twice is refused: its series would collide.
    assert!(EngineMetrics::register(&registry, "Engine").is_err());
}

/// A model gate's metrics: a gauge it fills from its own state at each
/// scrape.
#[derive(gdl::Metrics)]
struct GateMetrics {
    /// 1 for each model's status, 0 for its other statuses.
    #[gdl(labels = ["model", "status"])]
    model_status: gdl::ScrapeGaugeVec,
}

#[test]
fn a_scrape_gauge_reads_as_a_gauge_with_its_labels() {
    assert_eq!(
        GateMetrics::definitions(),
        [MetricField {
            name: "model_status".into(),
            help: "1 for each model's status, 0 for its other statuses.".into(),
            kind: MetricKind::Gauge,
            labels: vec!["model".into(), "status".into()],
            buckets: vec![],
        }]
    );
}

#[test]
fn a_scrape_gauge_scrapes_what_its_closure_sets() {
    let registry = Registry::new();
    let gate = GateMetrics::register(&registry, "Gate").expect("registered");
    // Registered, with nothing to read yet: no series.
    assert!(
        !text(&registry).contains("model_status"),
        "{}",
        text(&registry)
    );
    // The closure is set on the struct the node holds; the registry holds
    // a clone, which must read the same closure.
    let models = Arc::new(Mutex::new(vec![("llama", "live")]));
    let state = models.clone();
    gate.model_status.fill_with(move |gauges| {
        for (model, status) in state.lock().expect("models").iter() {
            for each in ["live", "down"] {
                let value = if each == *status { 1.0 } else { 0.0 };
                gauges.set(&[model, each], value).expect("two labels");
            }
        }
    });
    let scraped = text(&registry);
    for line in [
        "# HELP model_status 1 for each model's status, 0 for its other statuses.",
        "# TYPE model_status gauge",
        "model_status{dynamo_endpoint=\"Gate\",model=\"llama\",status=\"live\"} 1",
        "model_status{dynamo_endpoint=\"Gate\",model=\"llama\",status=\"down\"} 0",
    ] {
        assert!(scraped.contains(line), "{line:?} missing from:\n{scraped}");
    }
    // Each scrape reads the state anew: a model that left has no series.
    *models.lock().expect("models") = vec![("qwen", "down")];
    let scraped = text(&registry);
    assert!(
        scraped.contains("model_status{dynamo_endpoint=\"Gate\",model=\"qwen\",status=\"down\"} 1"),
        "{scraped}"
    );
    assert!(!scraped.contains("llama"), "{scraped}");
}

#[test]
fn a_later_closure_replaces_the_earlier() {
    let registry = Registry::new();
    let gate = GateMetrics::register(&registry, "Gate").expect("registered");
    gate.model_status.fill_with(|gauges| {
        gauges.set(&["first", "live"], 1.0).expect("two labels");
    });
    gate.model_status.fill_with(|gauges| {
        gauges.set(&["second", "live"], 1.0).expect("two labels");
    });
    let scraped = text(&registry);
    assert!(scraped.contains("model=\"second\""), "{scraped}");
    assert!(!scraped.contains("model=\"first\""), "{scraped}");
}

#[test]
fn a_wrong_label_count_is_refused_without_ending_the_scrape() {
    let registry = Registry::new();
    let gate = GateMetrics::register(&registry, "Gate").expect("registered");
    let refused = Arc::new(AtomicUsize::new(0));
    let count = refused.clone();
    gate.model_status.fill_with(move |gauges| {
        if gauges.set(&["llama"], 1.0).is_err() {
            count.fetch_add(1, Ordering::Relaxed);
        }
        gauges.set(&["llama", "live"], 1.0).expect("two labels");
    });
    let scraped = text(&registry);
    assert_eq!(refused.load(Ordering::Relaxed), 1);
    assert!(
        scraped
            .contains("model_status{dynamo_endpoint=\"Gate\",model=\"llama\",status=\"live\"} 1"),
        "{scraped}"
    );
}

/// A node that answers each request and fills its gauge at each scrape.
struct Gate {
    metrics: GateMetrics,
}

impl gdl::Build for Gate {
    type Config = ();
    type Metrics = GateMetrics;
    async fn build((): (), metrics: GateMetrics) -> gdl::Result<Self> {
        Ok(Self { metrics })
    }
}

impl gdl::Sink for Gate {
    type Req = u32;
    type Item = u32;
    async fn generate(&self, _cx: gdl::Cx, req: u32) -> gdl::Result<gdl::Items<u32>> {
        Ok(gdl::items(futures::stream::iter([Ok(req)])))
    }
}

#[tokio::test]
async fn a_scrape_gauge_runs_its_closure_per_scrape_not_per_request() {
    use futures::StreamExt;
    use gdl::{Build, Next};

    let registry = Registry::new();
    let metrics = GateMetrics::register(&registry, "Gate").expect("registered");
    let gate = Arc::new(Gate::build((), metrics).await.expect("built"));
    let runs = Arc::new(AtomicUsize::new(0));
    let count = runs.clone();
    gate.metrics.model_status.fill_with(move |gauges| {
        count.fetch_add(1, Ordering::Relaxed);
        gauges.set(&["llama", "live"], 1.0).expect("two labels");
    });
    let graph = gdl::Source::new(gdl::Hop::new((), gdl::SinkStage::new(gate), ()));
    for req in 0..100 {
        let items: Vec<u32> = graph
            .generate(gdl::Cx::new(), req)
            .await
            .expect("a stream")
            .map(|item| item.expect("an item"))
            .collect()
            .await;
        assert_eq!(items, [req]);
    }
    assert_eq!(runs.load(Ordering::Relaxed), 0, "a request ran the closure");
    text(&registry);
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    text(&registry);
    assert_eq!(runs.load(Ordering::Relaxed), 2);
}
