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

/// One field of each type a metrics struct may have.
#[derive(gdl::Metrics)]
struct EveryKind {
    /// An int counter.
    whole_total: gdl::prometheus::IntCounter,
    /// A counter.
    part_total: gdl::prometheus::Counter,
    /// An int gauge.
    whole_sequences: gdl::prometheus::IntGauge,
    /// A gauge.
    part_sequences: gdl::prometheus::Gauge,
    /// A histogram.
    #[gdl(buckets = [0.5, 1.5])]
    single_seconds: gdl::prometheus::Histogram,
    /// Int counters.
    #[gdl(labels = ["reason"])]
    whole_refused_total: gdl::prometheus::IntCounterVec,
    /// Counters.
    #[gdl(labels = ["reason"])]
    part_refused_total: gdl::prometheus::CounterVec,
    /// Int gauges.
    #[gdl(labels = ["queue"])]
    whole_queued_sequences: gdl::prometheus::IntGaugeVec,
    /// Gauges.
    #[gdl(labels = ["queue"])]
    part_queued_sequences: gdl::prometheus::GaugeVec,
    /// Histograms.
    #[gdl(labels = ["model"], buckets = [0.5, 1.5])]
    each_seconds: gdl::prometheus::HistogramVec,
    /// A gauge read at scrape time.
    #[gdl(labels = ["model"])]
    model_ready: gdl::ScrapeGaugeVec,
}

/// A segment's metrics text is what dashboards and scrapers read, so this
/// holds it byte for byte. The registry adds a prefix and one label, as a
/// segment's does: it adds its labels in hash order, so more of them would
/// not give the same bytes from run to run.
#[test]
fn a_nodes_text_stays_the_same() {
    let labels = [("worker_id".to_string(), "7".to_string())];
    let registry = Registry::new_custom(Some("dynamo_component".to_string()), Some(labels.into()))
        .expect("registry");
    let metrics = EveryKind::register(&registry, "Engine").expect("registered");
    metrics.whole_total.inc();
    metrics.part_total.inc_by(1.5);
    metrics.whole_sequences.set(2);
    metrics.part_sequences.set(2.5);
    metrics.single_seconds.observe(1.0);
    metrics
        .whole_refused_total
        .with_label_values(&["full"])
        .inc();
    metrics
        .part_refused_total
        .with_label_values(&["full"])
        .inc_by(0.5);
    metrics
        .whole_queued_sequences
        .with_label_values(&["prefill"])
        .set(3);
    metrics
        .part_queued_sequences
        .with_label_values(&["prefill"])
        .set(3.5);
    metrics
        .each_seconds
        .with_label_values(&["llama"])
        .observe(1.0);
    metrics.model_ready.fill_with(|gauges| {
        gauges.set(&["llama"], 1.0).expect("one label");
    });
    let expected = r#"# HELP dynamo_component_each_seconds Histograms.
# TYPE dynamo_component_each_seconds histogram
dynamo_component_each_seconds_bucket{dynamo_endpoint="Engine",model="llama",worker_id="7",le="0.5"} 0
dynamo_component_each_seconds_bucket{dynamo_endpoint="Engine",model="llama",worker_id="7",le="1.5"} 1
dynamo_component_each_seconds_bucket{dynamo_endpoint="Engine",model="llama",worker_id="7",le="+Inf"} 1
dynamo_component_each_seconds_sum{dynamo_endpoint="Engine",model="llama",worker_id="7"} 1
dynamo_component_each_seconds_count{dynamo_endpoint="Engine",model="llama",worker_id="7"} 1
# HELP dynamo_component_model_ready A gauge read at scrape time.
# TYPE dynamo_component_model_ready gauge
dynamo_component_model_ready{dynamo_endpoint="Engine",model="llama",worker_id="7"} 1
# HELP dynamo_component_part_queued_sequences Gauges.
# TYPE dynamo_component_part_queued_sequences gauge
dynamo_component_part_queued_sequences{dynamo_endpoint="Engine",queue="prefill",worker_id="7"} 3.5
# HELP dynamo_component_part_refused_total Counters.
# TYPE dynamo_component_part_refused_total counter
dynamo_component_part_refused_total{dynamo_endpoint="Engine",reason="full",worker_id="7"} 0.5
# HELP dynamo_component_part_sequences A gauge.
# TYPE dynamo_component_part_sequences gauge
dynamo_component_part_sequences{dynamo_endpoint="Engine",worker_id="7"} 2.5
# HELP dynamo_component_part_total A counter.
# TYPE dynamo_component_part_total counter
dynamo_component_part_total{dynamo_endpoint="Engine",worker_id="7"} 1.5
# HELP dynamo_component_single_seconds A histogram.
# TYPE dynamo_component_single_seconds histogram
dynamo_component_single_seconds_bucket{dynamo_endpoint="Engine",worker_id="7",le="0.5"} 0
dynamo_component_single_seconds_bucket{dynamo_endpoint="Engine",worker_id="7",le="1.5"} 1
dynamo_component_single_seconds_bucket{dynamo_endpoint="Engine",worker_id="7",le="+Inf"} 1
dynamo_component_single_seconds_sum{dynamo_endpoint="Engine",worker_id="7"} 1
dynamo_component_single_seconds_count{dynamo_endpoint="Engine",worker_id="7"} 1
# HELP dynamo_component_whole_queued_sequences Int gauges.
# TYPE dynamo_component_whole_queued_sequences gauge
dynamo_component_whole_queued_sequences{dynamo_endpoint="Engine",queue="prefill",worker_id="7"} 3
# HELP dynamo_component_whole_refused_total Int counters.
# TYPE dynamo_component_whole_refused_total counter
dynamo_component_whole_refused_total{dynamo_endpoint="Engine",reason="full",worker_id="7"} 1
# HELP dynamo_component_whole_sequences An int gauge.
# TYPE dynamo_component_whole_sequences gauge
dynamo_component_whole_sequences{dynamo_endpoint="Engine",worker_id="7"} 2
# HELP dynamo_component_whole_total An int counter.
# TYPE dynamo_component_whole_total counter
dynamo_component_whole_total{dynamo_endpoint="Engine",worker_id="7"} 1
"#;
    assert_eq!(text(&registry), expected);
}
