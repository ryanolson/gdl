use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// One metric a node reports, as `derive(gdl::Metrics)` defines it: the
/// catalogue's view of a field of the node's metrics type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricField {
    /// The metric's name, without the deployment's prefix.
    pub name: String,
    /// Help text from the field's documentation.
    pub help: String,
    /// What kind of metric it is.
    pub kind: MetricKind,
    /// A vector's label names, in order; empty for a single metric.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    /// A histogram's bucket upper bounds; empty for any other kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<f64>,
}

// The derive refuses a bound that is not finite, so `==` on the buckets is
// an equivalence, and the catalogue's entries, which hold metric fields,
// can be compared and sorted as wholes.
impl Eq for MetricField {}

impl PartialOrd for MetricField {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MetricField {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.name, &self.help, self.kind, &self.labels)
            .cmp(&(&other.name, &other.help, other.kind, &other.labels))
            .then_with(|| {
                let (mut a, mut b) = (self.buckets.iter(), other.buckets.iter());
                loop {
                    match (a.next(), b.next()) {
                        (Some(x), Some(y)) if x.total_cmp(y).is_ne() => return x.total_cmp(y),
                        (Some(_), Some(_)) => {}
                        (None, None) => return Ordering::Equal,
                        (None, Some(_)) => return Ordering::Less,
                        (Some(_), None) => return Ordering::Greater,
                    }
                }
            })
    }
}

/// The kind of a [`MetricField`], as Prometheus names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    /// Only ever increases.
    Counter,
    /// Goes up and down.
    Gauge,
    /// Counts observations into buckets.
    Histogram,
}

impl MetricKind {
    /// The kind's name, as Prometheus spells it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// The metrics the runtime records for each segment, and dynamo's
/// `request_duration_seconds`, which it reserves. A node's own metrics may
/// not take these names: they would collide in the registry, or, for the
/// reserved one, a dashboard would read a node's figure as a handler's.
pub const RUNTIME_METRICS: [&str; 17] = [
    "requests_total",
    "errors_total",
    "cancellation_total",
    "inflight_requests",
    "request_path_duration_seconds",
    "responses_total",
    "response_errors_total",
    "response_cancellation_total",
    "response_time_to_first_item_seconds",
    "response_time_after_first_item_seconds",
    "end_to_end_requests_total",
    "end_to_end_errors_total",
    "end_to_end_cancellation_total",
    "end_to_end_time_to_first_item_seconds",
    "end_to_end_duration_seconds",
    "uptime_seconds",
    "request_duration_seconds",
];

/// The deployment a series comes from: the graph's name and the first eight
/// hex digits of the placement hash.
pub const NAMESPACE_LABEL: &str = "dynamo_namespace";
/// The segment a series comes from.
pub const COMPONENT_LABEL: &str = "dynamo_component";
/// The node a series is about.
pub const ENDPOINT_LABEL: &str = "dynamo_endpoint";
/// The segment instance a series comes from: its velo instance id in hex.
pub const WORKER_LABEL: &str = "worker_id";

/// The labels the runtime adds to every series it serves, dynamo's
/// identity labels. A node's own metrics may not use them as label names.
pub const RUNTIME_LABELS: [&str; 4] = [
    NAMESPACE_LABEL,
    COMPONENT_LABEL,
    ENDPOINT_LABEL,
    WORKER_LABEL,
];
