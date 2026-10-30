//! Where a cluster's pods run, and what they ask for: a node selector,
//! tolerations, and CPU and memory requests and limits, a default for every
//! pod of the cluster and each segment's own, which a pool may override.
//! A cluster gives resources to every pod or to none.

use std::collections::BTreeMap;

use gdl_model::{Quantity, Resources, Toleration};

use super::{fleet, laid, problems};

/// One cluster that places its pods, a global pool and a model pool in it,
/// and the model pool's `e` given more memory than the cluster gives `e`.
const PLACED: &str = r#"
[fleet]
name = "llm"

[cluster.use1]
context = "kind-use1"
namespace = "gdl-llm"
region = "us-east-1"
node_selector = { "gdl.dev/node-group" = "demo" }
tolerations = [
  { key = "dedicated", operator = "Equal", value = "gdl", effect = "NoSchedule" },
  { key = "spot", operator = "Exists", effect = "NoExecute", toleration_seconds = 30 },
]

[cluster.use1.resources.default]
requests = { cpu = "250m", memory = "256Mi" }
limits = { cpu = "1", memory = "1Gi" }

[cluster.use1.resources.segment.e]
requests = { cpu = "1" }
limits = { cpu = "2", memory = "2Gi" }

[cluster.use1.resources.segment.hub]
limits = { memory = "512Mi" }

[class.global]
ready = { a = 1, b = 1, c = 1 }

[class.model_x]
ready = { d = 1, e = 1, f = 1 }

[pool.global-us]
class = "global"
cluster = "use1"

[pool.model-x-us]
class = "model_x"
cluster = "use1"
home = "global-us"

[pool.model-x-us.resources.segment.e]
limits = { memory = "4Gi" }
"#;

fn resources(cpu: (&str, &str), memory: (&str, &str)) -> Resources {
    Resources::new(cpu, memory)
}

#[test]
fn a_cluster_places_every_pod_it_runs() {
    let fleet = fleet(PLACED);
    fleet.check(Some(&laid())).expect("a sound fleet");
    let cluster = fleet.cluster("use1").expect("a cluster");
    assert_eq!(
        cluster.node_selector,
        BTreeMap::from([("gdl.dev/node-group".to_string(), "demo".to_string())])
    );
    assert_eq!(
        cluster.tolerations,
        [
            Toleration {
                key: "dedicated".into(),
                operator: "Equal".into(),
                value: "gdl".into(),
                effect: "NoSchedule".into(),
                toleration_seconds: None,
            },
            Toleration {
                key: "spot".into(),
                operator: "Exists".into(),
                value: String::new(),
                effect: "NoExecute".into(),
                toleration_seconds: Some(30),
            },
        ]
    );
    // Field by field: the pool's own, then the cluster's for the segment,
    // then the cluster's default.
    let pod = |pool: Option<&str>, pod: Option<&str>| {
        fleet
            .pod_resources("use1", pool, pod)
            .expect("the cluster gives resources")
    };
    assert_eq!(
        pod(Some("model-x-us"), Some("e")),
        resources(("1", "2"), ("256Mi", "4Gi"))
    );
    assert_eq!(
        pod(Some("global-us"), Some("a")),
        resources(("250m", "1"), ("256Mi", "1Gi"))
    );
    assert_eq!(
        pod(Some("global-us"), Some("hub")),
        resources(("250m", "1"), ("256Mi", "512Mi"))
    );
    // A pod of no pool, as the DynamoDB Local, takes the default.
    assert_eq!(pod(None, None), resources(("250m", "1"), ("256Mi", "1Gi")));
}

/// A cluster without these keys places its pods as before: no selector of
/// its own, no toleration, no resources.
#[test]
fn a_cluster_without_placement_gives_nothing() {
    let bare = r#"
[cluster.use1]
context = "kind-use1"
namespace = "gdl-llm"
region = "us-east-1"
[pool.g]
class = "global"
cluster = "use1"
"#;
    let fleet = fleet(bare);
    let cluster = fleet.cluster("use1").expect("a cluster");
    assert!(cluster.node_selector.is_empty() && cluster.tolerations.is_empty());
    assert_eq!(fleet.pod_resources("use1", Some("g"), Some("a")), None);
}

/// Quantities compare by value, exactly: a CPU of `1` is `1000m`, `0.5` is
/// `500m`, `1e3` is `1k`, and `1Gi` is more than `1G`.
#[test]
fn quantities_compare_by_value() {
    let q = |text: &str| Quantity::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"));
    assert_eq!(q("1"), q("1000m"));
    assert_eq!(q("0.5"), q("500m"));
    assert_eq!(q("1e3"), q("1k"));
    assert_eq!(q("1Ki"), q("1024"));
    assert!(q("1Gi") > q("1G"));
    assert!(q("128Mi") < q("1Gi"));
    for refused in ["", "1.5.5", "12Q", "-1", "m", "1 Gi", "0.0000000001", "1e"] {
        assert!(Quantity::parse(refused).is_err(), "{refused}");
    }
}

#[test]
fn a_long_zero_quantity_does_not_panic_during_fleet_check() {
    let check = |cpu: &str| {
        let text = changed(r#"cpu = "250m""#, &format!("cpu = {cpu:?}"));
        fleet(&text).check(Some(&laid()))
    };
    check("0").expect("a zero CPU request is valid");
    assert!(check("fast").is_err(), "malformed quantities are refused");
    let zero = format!("0.{}", "0".repeat(137));
    assert!(
        std::panic::catch_unwind(|| check(&zero)).is_ok(),
        "Fleet::check must return success or diagnostics for a long quantity"
    );
}

#[test]
#[ignore = "F2: Fleet::check does not cap binary quantities as Kubernetes does"]
fn binary_quantity_caps_cannot_hide_a_limit_below_its_request() {
    let resources = |request: &str, limit: &str| {
        changed(
            r#"limits = { memory = "4Gi" }"#,
            &format!("requests = {{ memory = {request:?} }}\nlimits = {{ memory = {limit:?} }}"),
        )
    };
    fleet(&resources("1Gi", "2Gi"))
        .check(Some(&laid()))
        .expect("ordinary binary resources fit");
    assert!(
        fleet(&resources("10E", "9223372036854775807"))
            .check(Some(&laid()))
            .is_err(),
        "the capped limit is below the decimal request"
    );
    assert!(
        fleet(&resources("10E", "10Ei"))
            .check(Some(&laid()))
            .is_err(),
        "Kubernetes caps 10Ei at 9223372036854775807, below the 10E request"
    );
}

#[test]
fn fractional_binary_quantities_are_scaled_before_precision_is_checked() {
    let q = |text: &str| Quantity::parse(text).unwrap_or_else(|error| panic!("{text}: {error}"));
    assert_eq!(q("0.5Gi"), q("512Mi"));
    assert_eq!(q("0.0009765625Gi"), q("1Mi"));
}

/// [`PLACED`] with `from` replaced by `to`, which must be in it.
fn changed(from: &str, to: &str) -> String {
    assert!(PLACED.contains(from), "{from}");
    PLACED.replacen(from, to, 1)
}

/// The problems of `text`, which must have one naming each of `parts`.
fn refused(text: &str, parts: &[&str]) {
    let problems = problems(text);
    let all = problems.join("\n");
    for part in parts {
        assert!(all.contains(part), "{part} in:\n{all}");
    }
}

#[test]
fn malformed_tolerations_are_refused() {
    for (from, to, part) in [
        (
            r#"operator = "Equal""#,
            r#"operator = "Matches""#,
            "operator",
        ),
        (r#"effect = "NoSchedule""#, r#"effect = "NoRun""#, "effect"),
        (
            r#"operator = "Exists", effect"#,
            r#"operator = "Exists", value = "x", effect"#,
            "`Exists`",
        ),
        (r#"key = "dedicated", "#, r#"key = "", "#, "empty key"),
        (
            r#"effect = "NoExecute", toleration_seconds = 30"#,
            r#"effect = "NoSchedule", toleration_seconds = 30"#,
            "toleration_seconds",
        ),
    ] {
        refused(&changed(from, to), &["[cluster.use1] tolerations", part]);
    }
}

#[test]
fn malformed_node_selectors_are_refused() {
    let from = r#"node_selector = { "gdl.dev/node-group" = "demo" }"#;
    for (to, part) in [
        (
            r#"node_selector = { "gdl.dev/" = "demo" }"#,
            "not a label key",
        ),
        (
            r#"node_selector = { "group" = "this value has spaces" }"#,
            "not a label value",
        ),
        (
            r#"node_selector = { "kubernetes.io/arch" = "arm64" }"#,
            "platform",
        ),
    ] {
        refused(&changed(from, to), &["[cluster.use1] node_selector", part]);
    }
}

#[test]
fn malformed_resources_are_refused() {
    // A quantity that does not parse.
    refused(
        &changed(r#"cpu = "250m""#, r#"cpu = "fast""#),
        &["[cluster.use1] resources.default: requests.cpu `fast` is not a quantity"],
    );
    // A limit below its request: in the default, in a segment's resources
    // over the default, and in a pool's over its cluster's.
    refused(
        &changed(
            r#"limits = { cpu = "1", memory = "1Gi" }"#,
            r#"limits = { cpu = "100m", memory = "1Gi" }"#,
        ),
        &[
            "[cluster.use1] resources.default",
            "cpu limit `100m` is below its request `250m`",
        ],
    );
    refused(
        &changed(r#"requests = { cpu = "1" }"#, r#"requests = { cpu = "3" }"#),
        &[
            "[cluster.use1] resources.segment.e",
            "cpu limit `2` is below its request `3`",
        ],
    );
    refused(
        &changed(
            r#"limits = { memory = "4Gi" }"#,
            r#"limits = { memory = "128Mi" }"#,
        ),
        &[
            "[pool.model-x-us] resources.segment.e",
            "memory limit `128Mi` is below its request `256Mi`",
        ],
    );
    // A default that does not give all four.
    refused(
        &changed(
            r#"limits = { cpu = "1", memory = "1Gi" }"#,
            r#"limits = { memory = "1Gi" }"#,
        ),
        &["[cluster.use1] resources.default", "cpu limit"],
    );
    // A segment the class does not have, for the cluster and for a pool.
    refused(
        &changed(
            "[cluster.use1.resources.segment.e]",
            "[cluster.use1.resources.segment.z]",
        ),
        &["[cluster.use1] resources.segment.z", "not a segment"],
    );
    refused(
        &changed(
            "[pool.model-x-us.resources.segment.e]",
            "[pool.model-x-us.resources.segment.a]",
        ),
        &["[pool.model-x-us] resources.segment.a", "not a segment"],
    );
    // A pool's resources in a cluster that gives none.
    let start = PLACED
        .find("[cluster.use1.resources.default]")
        .expect("the default");
    let end = PLACED.find("[class.global]").expect("the classes");
    let without = format!("{}{}", &PLACED[..start], &PLACED[end..]);
    refused(
        &without,
        &["[pool.model-x-us] resources", "gives no resources"],
    );
}
