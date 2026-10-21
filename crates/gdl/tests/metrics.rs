//! A node's own metrics: `derive(gdl::Metrics)` builds each field, labels
//! it with the node, and registers it.

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
