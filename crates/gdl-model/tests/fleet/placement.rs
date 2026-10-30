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

/// Edge quantities, each with its value in thousandths of its unit, or
/// `None` where it is refused. Each outcome is what apimachinery v0.34.1's
/// `resource.ParseQuantity` (`quantity.go`) gives, run on these inputs, with
/// a quantity refused where Kubernetes would change it: finer than `1m`,
/// which it rounds up, or above 2^63 - 1 units, which it caps (it parses
/// `8Ei` as 9223372036854775807, and keeps `10E` above it only until it
/// reads its value). It refuses no negative quantity; a pod's resources
/// cannot be negative, so `-1` is refused here.
#[test]
fn edge_quantities_parse_as_kubernetes_keeps_them() {
    const MAX: u128 = 9_223_372_036_854_775_807;
    let zeros = format!("0.{}", "0".repeat(137));
    let table: &[(&str, Option<u128>)] = &[
        // Every suffix.
        ("1n", None),
        ("1000000n", Some(1)),
        ("999999n", None),
        ("1000u", Some(1)),
        ("999u", None),
        ("1m", Some(1)),
        ("0.1m", None),
        ("1", Some(1_000)),
        ("1k", Some(1_000_000)),
        ("1M", Some(1_000_000_000)),
        ("1G", Some(1_000_000_000_000)),
        ("1T", Some(1_000_000_000_000_000)),
        ("1P", Some(1_000_000_000_000_000_000)),
        ("1E", Some(1_000_000_000_000_000_000_000)),
        ("1Ki", Some(1_024_000)),
        ("1Mi", Some(1_048_576_000)),
        ("1Gi", Some(1_073_741_824_000)),
        ("1Ti", Some(1_099_511_627_776_000)),
        ("1Pi", Some(1_125_899_906_842_624_000)),
        ("1Ei", Some(1_152_921_504_606_846_976_000)),
        ("1e3", Some(1_000_000)),
        ("1E3", Some(1_000_000)),
        ("1e-3", Some(1)),
        ("1000000e-9", Some(1)),
        // The smallest accepted, and the first refused below it.
        ("0", Some(0)),
        ("0.", Some(0)),
        (&zeros, Some(0)),
        ("0.001", Some(1)),
        ("0.0001", None),
        ("1e-4", None),
        ("1e-9", None),
        ("0.001Ki", Some(1_024)),
        ("0.0001Ki", None),
        // The largest accepted, and the first refused above it.
        ("9223372036854775807", Some(MAX * 1000)),
        ("9223372036854775807000m", Some(MAX * 1000)),
        ("9223372036854775.807k", Some(MAX * 1000)),
        ("9223372036854775807001m", None),
        ("9223372036854775808", None),
        ("9E", Some(9_000_000_000_000_000_000_000)),
        ("10E", None),
        ("1e18", Some(1_000_000_000_000_000_000_000)),
        ("1e19", None),
        ("7Ei", Some(8_070_450_532_247_928_832_000)),
        ("8Ei", None),
        // Fractions of a binary unit, scaled before their precision.
        ("0.5Gi", Some(536_870_912_000)),
        ("0.0009765625Gi", Some(1_048_576_000)),
        ("0.5Ki", Some(512_000)),
        // Forms of a number.
        ("+1", Some(1_000)),
        ("1.", Some(1_000)),
        (".5", Some(500)),
        ("1.5", Some(1_500)),
        ("-1", None),
        // Not quantities.
        ("", None),
        ("12Q", None),
        ("1 Gi", None),
        ("fast", None),
        ("1e", None),
        ("1.5.5", None),
        ("0x10", None),
    ];
    for (text, expected) in table {
        let parsed = Quantity::parse(text).map(|q| q.millis()).ok();
        assert_eq!(parsed, *expected, "{text:?}");
    }
}
