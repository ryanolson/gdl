//! A PodMonitor, for a cluster that runs the Prometheus operator: each
//! segment's system port at `/metrics` and each hub's public port at its
//! pool's route, the two named ports no other pod of a fleet has.

use serde_json::{Map, Value, json};

use crate::labels::label_value;
use crate::pool::Pool;
use crate::render::{HUB_METRICS, PUBLIC, SEGMENT_METRICS, SYSTEM};

/// The labels a PodMonitor copies from each pod onto every series it
/// scrapes there, each as the target label it becomes: the same as the
/// chapter's scrape job without the operator gives. Not `pool`, `class`
/// or `region`: a chooser labels its own series with the pool it offered
/// to, its class and its region, and a target label of the same name would
/// move those aside.
const MONITORED: [(&str, &str); 6] = [
    ("gdl.dev/fleet", "fleet"),
    ("gdl.dev/cluster", "cluster"),
    ("gdl.dev/pool", "in_pool"),
    ("gdl.dev/class", "in_class"),
    ("gdl.dev/region", "in_region"),
    ("gdl.dev/segment", "segment"),
];

/// The PodMonitor `name` in `namespace` of the pods that carry each of
/// `labels`, a key and its whole value, which it carries too. Each copied
/// label is read from the pod's label, then from the annotation that holds
/// its whole value when the label holds a shortened one ([`label_value`]).
pub fn pod_monitor(name: &str, namespace: &str, labels: &[(&str, &str)]) -> Value {
    let labels: Map<String, Value> = labels
        .iter()
        .map(|(key, value)| (key.to_string(), label_value(value).into()))
        .collect();
    let relabelings: Vec<Value> = MONITORED
        .iter()
        .flat_map(|(label, target)| {
            let meta = prometheus_name(label);
            [
                json!({
                    "sourceLabels": [format!("__meta_kubernetes_pod_label_{meta}")],
                    "targetLabel": target,
                }),
                json!({
                    "sourceLabels": [format!("__meta_kubernetes_pod_annotation_{meta}")],
                    "regex": "(.+)",
                    "targetLabel": target,
                }),
            ]
        })
        .collect();
    let endpoint =
        |port: &str, path: &str| json!({"port": port, "path": path, "relabelings": relabelings});
    json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PodMonitor",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "selector": {"matchLabels": labels},
            "podMetricsEndpoints": [
                endpoint(SYSTEM, SEGMENT_METRICS),
                endpoint(PUBLIC, HUB_METRICS),
            ],
        },
    })
}

/// The PodMonitor of pool `pool`, named `name` after it, in `namespace`:
/// the API server keeps its name the pool's own there.
pub fn pool_monitor(name: &str, namespace: &str, pool: &Pool) -> Value {
    pod_monitor(
        name,
        namespace,
        &[
            ("gdl.dev/fleet", &pool.fleet),
            ("gdl.dev/cluster", &pool.cluster),
            ("gdl.dev/pool", name),
        ],
    )
}

/// A Kubernetes label's key as Prometheus' discovery names it: each
/// character that a label name cannot hold becomes `_`.
fn prometheus_name(key: &str) -> String {
    key.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}
