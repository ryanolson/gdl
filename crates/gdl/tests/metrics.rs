//! A host's own labels on a node's metrics: `Metrics::register_with` lets a
//! registry that already adds the identity labels keep each of them once.

use gdl::Metrics;
use gdl::prometheus::{Error, Registry, TextEncoder};

/// The labels a dynamo registry adds to each series.
const IDENTITY: [&str; 4] = [
    "dynamo_namespace",
    "dynamo_component",
    "dynamo_endpoint",
    "worker_id",
];

#[derive(gdl::Metrics)]
struct EngineMetrics {
    /// Tokens generated.
    tokens_total: gdl::prometheus::IntCounter,
    /// Requests refused, by reason.
    #[gdl(labels = ["reason"])]
    refused_total: gdl::prometheus::IntCounterVec,
    /// Time to the first token.
    #[gdl(buckets = [0.5, 1.5])]
    time_to_first_token_seconds: gdl::prometheus::Histogram,
    /// Sequences waiting, by queue.
    #[gdl(labels = ["queue"])]
    queued_sequences: gdl::ScrapeGaugeVec,
}

impl EngineMetrics {
    /// Give each field a series, so that each shows in the text.
    fn record(&self) {
        self.tokens_total.inc();
        self.refused_total.with_label_values(&["full"]).inc();
        self.time_to_first_token_seconds.observe(1.0);
        self.queued_sequences.fill_with(|gauges| {
            gauges.set(&["prefill"], 1.0).expect("one label");
        });
    }
}

fn host_registry() -> Registry {
    let labels = IDENTITY.map(|name| (name.to_string(), "host".to_string()));
    Registry::new_custom(Some("dynamo_component".to_string()), Some(labels.into()))
        .expect("registry")
}

fn text(registry: &Registry) -> String {
    let mut out = String::new();
    TextEncoder::new()
        .encode_utf8(&registry.gather(), &mut out)
        .expect("text format");
    out
}

/// The sample lines of `text`, with how often each one names `label`.
fn label_counts<'a>(text: &'a str, label: &str) -> Vec<(&'a str, usize)> {
    let name = format!("{label}=\"");
    text.lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| (line, line.matches(&name).count()))
        .collect()
}

#[test]
fn a_hosts_identity_labels_stay_once_each() {
    let registry = host_registry();
    EngineMetrics::register_with(&registry, &[])
        .expect("registered")
        .record();
    let text = text(&registry);
    // A sample of 3 fields, and the 5 series of the histogram.
    assert_eq!(label_counts(&text, IDENTITY[0]).len(), 8, "{text}");
    for label in IDENTITY {
        for (line, count) in label_counts(&text, label) {
            assert_eq!(count, 1, "{label} in {line}");
        }
    }
}

/// The control: the count above sees the defect `register_with` avoids, so
/// the test above cannot pass for want of a duplicate to find.
#[test]
fn a_nodes_label_doubles_the_hosts() {
    let registry = host_registry();
    EngineMetrics::register(&registry, "Engine")
        .expect("registered")
        .record();
    let text = text(&registry);
    for (line, count) in label_counts(&text, "dynamo_endpoint") {
        assert_eq!(count, 2, "{line}");
    }
}

#[test]
fn register_is_register_with_the_endpoint_label() {
    let plain = Registry::new();
    EngineMetrics::register(&plain, "Engine")
        .expect("registered")
        .record();
    let labelled = Registry::new();
    EngineMetrics::register_with(&labelled, &[("dynamo_endpoint", "Engine")])
        .expect("registered")
        .record();
    assert_eq!(text(&plain), text(&labelled));
}

#[test]
fn a_hosts_labels_reach_every_field() {
    let registry = Registry::new();
    EngineMetrics::register_with(&registry, &[("pool", "a"), ("shard", "3")])
        .expect("registered")
        .record();
    let text = text(&registry);
    let lines = label_counts(&text, "pool");
    assert_eq!(lines.len(), 8, "{text}");
    for (line, _) in lines {
        assert!(line.contains(r#"pool="a""#), "{line}");
        assert!(line.contains(r#"shard="3""#), "{line}");
    }
}

/// Prometheus keeps a const label and a field's own label of one name
/// apart, so without a refusal each series would carry the name twice.
#[test]
fn a_label_a_field_uses_is_refused() {
    for label in ["reason", "queue"] {
        let registry = Registry::new();
        let Err(Error::Msg(message)) = EngineMetrics::register_with(&registry, &[(label, "a")])
        else {
            panic!("{label} is a field's label");
        };
        assert!(message.contains(label), "{message}");
        assert_eq!(text(&registry), "", "a refusal registers nothing");
    }
}

/// Prometheus takes const labels as a map, where the later value would
/// replace the earlier without a word.
#[test]
fn a_label_given_twice_is_refused() {
    let registry = Registry::new();
    let Err(Error::Msg(message)) =
        EngineMetrics::register_with(&registry, &[("pool", "a"), ("pool", "b")])
    else {
        panic!("pool is given twice");
    };
    assert!(message.contains("pool"), "{message}");
    assert_eq!(text(&registry), "", "a refusal registers nothing");
}

#[test]
fn a_node_without_metrics_takes_any_labels() {
    let registry = Registry::new();
    <() as Metrics>::register_with(&registry, &[]).expect("no metrics");
    <() as Metrics>::register_with(&registry, &[("pool", "a")]).expect("no metrics");
}

/// An impl from before `register_with`, which implements what it had to
/// then. The seal is what keeps this out of a node's reach, not the trait.
struct ByHand;

impl gdl::__private::MetricsDerived for ByHand {}

impl Metrics for ByHand {
    fn definitions() -> Vec<gdl::MetricField> {
        Vec::new()
    }

    fn register(registry: &Registry, node: &str) -> gdl::prometheus::Result<Self> {
        let opts = gdl::prometheus::Opts::new("by_hand_total", "By hand.")
            .const_label("dynamo_endpoint", node);
        let counter = gdl::prometheus::IntCounter::with_opts(opts)?;
        registry.register(Box::new(counter))?;
        Ok(Self)
    }
}

#[test]
fn an_impl_without_register_with_serves_the_endpoint_label() {
    let plain = Registry::new();
    ByHand::register(&plain, "Node").expect("registered");
    let labelled = Registry::new();
    ByHand::register_with(&labelled, &[("dynamo_endpoint", "Node")]).expect("registered");
    assert_eq!(text(&plain), text(&labelled));
    assert!(!text(&labelled).is_empty());
}

/// Such an impl cannot apply other labels, and registering its metrics
/// without them would serve series the host did not ask for.
#[test]
fn an_impl_without_register_with_refuses_other_labels() {
    for labels in [&[][..], &[("pool", "a")][..]] {
        let registry = Registry::new();
        let Err(Error::Msg(message)) = ByHand::register_with(&registry, labels) else {
            panic!("{labels:?} is not what `register` can apply");
        };
        assert!(message.contains("ByHand"), "{message}");
        assert_eq!(text(&registry), "", "a refusal registers nothing");
    }
}
