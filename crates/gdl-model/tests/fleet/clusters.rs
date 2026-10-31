//! The fleet's clusters: where each pool runs, and the region it takes
//! from there.

use std::collections::BTreeMap;

use gdl_model::{Cluster, Fleet};

use super::{FLEET, fleet, laid, problems};

/// The design's fleet with each region a cluster, every key of a cluster
/// set in the first.
const CLUSTERED: &str = r#"
[fleet]
name = "llm-proof"

[cluster.use1]
context = "kind-use1"
namespace = "gdl-llm"
region = "us-east-1"
registry = "registry.example/gdl"
platform = "linux/amd64"
service_account_annotations = { "eks.amazonaws.com/role-arn" = "arn:aws:iam::0:role/gdl" }
secrets = { sequence-key = "llm-sequence-key" }
env = { SET_RECORDS_TABLE = "llm-pins-use1" }
pins_local = true
scaler = true

[cluster.usw2]
context = "kind-usw2"
namespace = "gdl-llm"
region = "us-west-2"

[cluster.euw1]
context = "kind-euw1"
namespace = "gdl-llm"
region = "eu-west-1"

[class.global]
ready = { a = 2, b = 2, c = 2 }
requires = { model_x = 1 }

[class.model_x]
ready = { d = 1, e = 4, f = 2 }

[pool.global-us-east-1]
class = "global"
cluster = "use1"

[pool.global-us-west-2]
class = "global"
cluster = "usw2"

[pool.global-eu-west-1]
class = "global"
cluster = "euw1"

[pool.model-x-01-us-east-1]
class = "model_x"
cluster = "use1"
region = "us-east-1"
home = "global-us-east-1"

[pool.model-x-02-us-east-1]
class = "model_x"
cluster = "use1"
home = "global-us-east-1"
ready = { e = 8 }

[pool.model-x-01-us-west-2]
class = "model_x"
cluster = "usw2"
home = "global-us-west-2"

[pool.model-x-01-eu-west-1]
class = "model_x"
cluster = "euw1"
home = "global-eu-west-1"
"#;

/// Two clusters `a` and `b`, in one region, before any pool.
const TWO: &str = "[cluster.a]\ncontext = 'ctx-a'\nnamespace = 'gdl'\nregion = 'r'\n\
                   [cluster.b]\ncontext = 'ctx-b'\nnamespace = 'gdl'\nregion = 'r'\n";

/// A global pool `g` in cluster `a`, with its child there.
const G_IN_A: &str = "[pool.g]\nclass = 'global'\ncluster = 'a'\n\
                      [pool.g-child]\nclass = 'model_x'\ncluster = 'a'\nhome = 'g'\n";

/// A pool takes its region from its cluster, and its settings, which a
/// generator writes, are those of the same pool with the region written
/// out.
#[test]
fn a_pool_takes_its_region_from_its_cluster() {
    let clustered = fleet(CLUSTERED);
    clustered.check(Some(&laid())).expect("a sound fleet");
    clustered
        .check(None)
        .expect("a sound fleet, without a layout");
    let flat = fleet(FLEET);
    assert_eq!(
        clustered.pools().collect::<Vec<_>>(),
        flat.pools().collect::<Vec<_>>()
    );
}

#[test]
fn a_fleets_clusters_are_read_whole() {
    let fleet = fleet(CLUSTERED);
    assert_eq!(fleet.name(), Some("llm-proof"));
    let use1 = fleet.cluster("use1").expect("a cluster");
    assert_eq!(
        use1,
        &Cluster {
            context: "kind-use1".into(),
            namespace: "gdl-llm".into(),
            region: "us-east-1".into(),
            registry: Some("registry.example/gdl".into()),
            platform: Some("linux/amd64".into()),
            service_account_annotations: BTreeMap::from([(
                "eks.amazonaws.com/role-arn".into(),
                "arn:aws:iam::0:role/gdl".into()
            )]),
            secrets: BTreeMap::from([("sequence-key".into(), "llm-sequence-key".into())]),
            env: BTreeMap::from([("SET_RECORDS_TABLE".into(), "llm-pins-use1".into())]),
            pins_local: true,
            node_selector: BTreeMap::new(),
            tolerations: Vec::new(),
            resources: None,
            scaler: true,
        }
    );
    // Every key but the three that place it is optional.
    let usw2 = fleet.cluster("usw2").expect("a cluster");
    assert_eq!(
        usw2,
        &Cluster {
            context: "kind-usw2".into(),
            namespace: "gdl-llm".into(),
            region: "us-west-2".into(),
            registry: None,
            platform: None,
            service_account_annotations: BTreeMap::new(),
            secrets: BTreeMap::new(),
            env: BTreeMap::new(),
            pins_local: false,
            node_selector: BTreeMap::new(),
            tolerations: Vec::new(),
            resources: None,
            scaler: false,
        }
    );
    let names: Vec<&str> = fleet.clusters().map(|(name, _)| name).collect();
    assert_eq!(names, ["euw1", "use1", "usw2"]);
    assert_eq!(
        fleet.cluster_of("model-x-02-us-east-1"),
        Some(("use1", use1))
    );
    assert!(fleet.cluster("nope").is_none());
    assert!(fleet.cluster_of("nope").is_none());
    // A fleet without clusters has no name and places no pool.
    let flat = super::fleet(FLEET);
    assert_eq!(flat.name(), None);
    assert_eq!(flat.clusters().count(), 0);
    assert!(flat.cluster_of("global-us-east-1").is_none());
}

#[test]
fn a_fleet_with_clusters_survives_toml() {
    let fleet = fleet(CLUSTERED);
    let text = toml::to_string(&fleet).expect("toml");
    assert_eq!(Fleet::from_toml(&text).expect("reads what it wrote"), fleet);
    // A fleet without clusters writes none of the new tables.
    let flat = toml::to_string(&super::fleet(FLEET)).expect("toml");
    assert!(!flat.contains("[fleet]"), "{flat}");
    assert!(!flat.contains("cluster"), "{flat}");
}

/// Each refusal of `cargo gdl check` that clusters bring names the pool or
/// the cluster it is about.
#[test]
fn each_cluster_refusal_names_its_pool_or_cluster() {
    let rule = "must be lowercase letters, digits, and `-`, start with a letter, end with a letter or digit, and be at most 63 characters";
    let cases = [
        (
            format!("{TWO}{G_IN_A}").replacen("cluster = 'a'", "cluster = 'nope'", 1),
            "[pool.g] cluster: the fleet has no cluster `nope`".to_string(),
        ),
        (
            format!("{TWO}{G_IN_A}[pool.m]\nclass = 'model_x'\ncluster = 'a'\nregion = 'eu'\nhome = 'g'\n"),
            "[pool.m] region: `eu` is not the region of cluster `a`, which is `r`; a pool's region is its cluster's".into(),
        ),
        (
            format!("{TWO}{G_IN_A}").replacen("cluster = 'a'\n", "", 1),
            "[pool.g] region: missing; a pool names its region, or a cluster whose region it takes".into(),
        ),
        (
            format!("{TWO}{G_IN_A}").replace("ctx-b", "ctx-a"),
            "[cluster.b]: cluster `a` has the same context `ctx-a` and namespace `gdl`; each cluster deploys to a namespace of its own".into(),
        ),
        (
            format!("{TWO}{G_IN_A}").replace("[cluster.b]", "[cluster.B]"),
            format!("[cluster.B]: cluster name `B` {rule}"),
        ),
        (
            format!("{TWO}{G_IN_A}").replace("region = 'r'\n[cluster.b]", "region = 'R'\n[cluster.b]"),
            format!("[cluster.a] region: region name `R` {rule}"),
        ),
        (
            format!("{TWO}{G_IN_A}").replace("namespace = 'gdl'\nregion = 'r'\n[cluster.b]", "namespace = 'Gdl'\nregion = 'r'\n[cluster.b]"),
            format!("[cluster.a] namespace: `Gdl` {NAMESPACE_RULE}"),
        ),
        (
            format!("[fleet]\nname = 'Proof'\n{TWO}{G_IN_A}"),
            format!("[fleet] name: fleet name `Proof` {rule}"),
        ),
        (
            format!("{TWO}{G_IN_A}[cluster.c]\ncontext = 'c'\nnamespace = 'gdl'\nregion = 'r'\nsecrets = {{ key = 'Not_A_Secret' }}\n"),
            "[cluster.c] secrets.key: `Not_A_Secret` is not a Kubernetes Secret's name: lowercase letters, digits, `-`, and `.`, starting and ending with a letter or digit, at most 253 characters".into(),
        ),
        (
            format!("{TWO}{G_IN_A}[cluster.c]\ncontext = 'c'\nnamespace = 'gdl'\nregion = 'r'\nenv = {{ 1BAD = 'x' }}\n"),
            "[cluster.c] env.1BAD: not a variable's name: letters, digits, and `_`, not starting with a digit".into(),
        ),
        (
            format!("{TWO}{G_IN_A}[cluster.c]\ncontext = ''\nnamespace = 'gdl'\nregion = 'r'\n"),
            "[cluster.c] context: is empty; a cluster names the kube context that reaches it".into(),
        ),
    ];
    for (text, expected) in cases {
        assert_eq!(problems(&text), std::slice::from_ref(&expected), "{text}");
        // None needs the spec's layouts.
        let Err(errors) = fleet(&text).check(None) else {
            panic!("accepted without a layout:\n{text}");
        };
        let errors: Vec<String> = errors.0.iter().map(ToString::to_string).collect();
        assert_eq!(errors, [expected], "{text}");
    }
}

/// How a refused namespace is described.
const NAMESPACE_RULE: &str = "is not a Kubernetes namespace: lowercase letters, digits, and `-`, starting and ending with a letter or digit, at most 63 characters";

/// A namespace is a Kubernetes namespace's name, an RFC 1123 label, which
/// may start with a digit, unlike a pool's or a cluster's name.
#[test]
fn a_namespace_is_an_rfc_1123_label() {
    let longest = format!("0{}", "-0".repeat(31));
    assert_eq!(longest.len(), 63);
    let with = |namespace: &str| {
        format!("{TWO}{G_IN_A}").replacen(
            "namespace = 'gdl'",
            &format!("namespace = '{namespace}'"),
            1,
        )
    };
    for good in ["123-gdl", "0", "gdl", "a1", longest.as_str()] {
        fleet(&with(good)).check(Some(&laid())).expect(good);
    }
    let too_long = format!("{longest}0");
    for bad in [
        "Gdl",
        "-gdl",
        "gdl-",
        "gdl.x",
        "gdl_x",
        "",
        too_long.as_str(),
    ] {
        assert_eq!(
            problems(&with(bad)),
            [format!("[cluster.a] namespace: `{bad}` {NAMESPACE_RULE}")],
            "{bad}"
        );
    }
}

/// A service account annotation's key is Kubernetes' annotation key: an
/// optional DNS subdomain and `/`, then a name of at most 63 characters.
#[test]
fn an_annotation_key_is_a_kubernetes_annotation_key() {
    let with = |key: &str| {
        format!(
            "{TWO}{G_IN_A}[cluster.c]\ncontext = 'c'\nnamespace = 'gdl'\nregion = 'r'\n\
             service_account_annotations = {{ \"{key}\" = 'v' }}\n"
        )
    };
    let longest = format!("A{}", "_b".repeat(31));
    assert_eq!(longest.len(), 63);
    let prefixed = format!("eks.amazonaws.com/{longest}");
    for good in [
        "eks.amazonaws.com/role-arn",
        "role-arn",
        "a/B_c.d-e",
        "0.example/9",
        longest.as_str(),
        prefixed.as_str(),
    ] {
        fleet(&with(good)).check(Some(&laid())).expect(good);
    }
    let too_long = format!("{longest}b");
    let long_prefix = format!("{}/x", "a".repeat(254));
    for bad in [
        "",
        "/x",
        "x/",
        "Eks.com/x",
        "a/b/c",
        "-x",
        "x-",
        "has space",
        "a_b.com/x",
        too_long.as_str(),
        long_prefix.as_str(),
    ] {
        assert_eq!(
            problems(&with(bad)),
            [format!(
                "[cluster.c] service_account_annotations.{bad}: not an annotation key: an optional DNS subdomain and `/`, then at most 63 letters, digits, `-`, `_`, and `.`, starting and ending with a letter or digit"
            )],
            "{bad}"
        );
    }
}

/// The controls: a pool may write its cluster's region out, and pools of
/// one fleet may name a cluster or a region alone.
#[test]
fn a_pool_may_write_its_clusters_region_or_name_a_region_alone() {
    for text in [
        format!(
            "{TWO}{G_IN_A}[pool.m]\nclass = 'model_x'\ncluster = 'b'\nregion = 'r'\nhome = 'g'\n"
        ),
        format!("{TWO}{G_IN_A}[pool.m]\nclass = 'model_x'\nregion = 'eu'\nhome = 'g'\n"),
        // Two clusters may share a context in other namespaces, or a
        // namespace in other contexts.
        format!("{TWO}{G_IN_A}")
            .replace("ctx-b'\nnamespace = 'gdl'", "ctx-a'\nnamespace = 'gdl-b'"),
    ] {
        fleet(&text).check(Some(&laid())).expect("a sound fleet");
    }
}

/// An unknown cluster is one problem: the pool's region is not compared
/// with a cluster that is not there. A cluster's bad region is reported
/// at the cluster, not again at each pool that takes it.
#[test]
fn a_problem_of_a_cluster_is_reported_once() {
    assert_eq!(
        problems(&format!(
            "{TWO}{G_IN_A}[pool.m]\nclass = 'model_x'\ncluster = 'nope'\nregion = 'eu'\nhome = 'g'\n"
        )),
        ["[pool.m] cluster: the fleet has no cluster `nope`"]
    );
    // A bad region of the pool's own hides no unknown cluster.
    assert_eq!(
        problems(&format!(
            "{TWO}{G_IN_A}[pool.m]\nclass = 'model_x'\ncluster = 'nope'\nregion = 'EU'\nhome = 'g'\n"
        )),
        [
            "[pool.m] region: region name `EU` must be lowercase letters, digits, and `-`, start with a letter, end with a letter or digit, and be at most 63 characters",
            "[pool.m] cluster: the fleet has no cluster `nope`",
        ]
    );
    let text =
        format!("{TWO}{G_IN_A}").replace("region = 'r'\n[cluster.b]", "region = 'R'\n[cluster.b]");
    assert_eq!(problems(&text).len(), 1, "{:?}", problems(&text));
}

/// Three clusters that share a context and a namespace are two problems,
/// each at the later cluster in name order, naming the first.
#[test]
fn a_shared_namespace_is_reported_at_each_later_cluster() {
    let text =
        format!("{TWO}{G_IN_A}[cluster.c]\ncontext = 'ctx-a'\nnamespace = 'gdl'\nregion = 'r'\n")
            .replace("ctx-b", "ctx-a");
    let tail = "has the same context `ctx-a` and namespace `gdl`; each cluster deploys to a namespace of its own";
    assert_eq!(
        problems(&text),
        [
            format!("[cluster.b]: cluster `a` {tail}"),
            format!("[cluster.c]: cluster `a` {tail}"),
        ]
    );
}

#[test]
fn unknown_keys_are_refused_in_the_new_tables() {
    for bad in [
        "[fleet]\nname = 'x'\nzone = 'a'\n",
        "[cluster.a]\ncontext = 'c'\nnamespace = 'n'\nregion = 'r'\nzone = 'a'\n",
        "[cluster.a]\nnamespace = 'n'\nregion = 'r'\n",
        "[cluster.a]\ncontext = 'c'\nregion = 'r'\n",
        "[cluster.a]\ncontext = 'c'\nnamespace = 'n'\n",
        "[cluster.a]\ncontext = 'c'\nnamespace = 'n'\nregion = 'r'\npins_local = 'yes'\n",
        "[cluster.a]\ncontext = 'c'\nnamespace = 'n'\nregion = 'r'\nsecrets = { key = 1 }\n",
    ] {
        assert!(Fleet::from_toml(bad).is_err(), "accepted:\n{bad}");
    }
}

/// A DynamoDB directory's tables, without `local` or `cluster`.
const DIRECTORY: &str = "[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\n";

/// A directory in DynamoDB Local runs in one cluster, which `[directory]
/// cluster` names: required when the fleet names clusters, so that every
/// region uses one directory rather than one Local each, and a cluster of
/// the fleet. It is read back as written.
#[test]
fn a_local_directory_names_the_cluster_that_runs_it() {
    let text = format!("{CLUSTERED}{DIRECTORY}local = true\ncluster = \"use1\"\n");
    let named = fleet(&text);
    named.check(Some(&laid())).expect("a sound fleet");
    assert_eq!(
        named.directory().expect("a directory").cluster.as_deref(),
        Some("use1")
    );
    let written = toml::to_string(&named).expect("toml");
    assert_eq!(Fleet::from_toml(&written).expect("read back"), named);

    assert_eq!(
        problems(&format!("{CLUSTERED}{DIRECTORY}local = true\n")),
        [
            "[directory] cluster: missing; the fleet names clusters, so a directory in DynamoDB Local names the one cluster that runs it, for every cluster to reach"
        ]
    );
    assert_eq!(
        problems(&format!(
            "{CLUSTERED}{DIRECTORY}local = true\ncluster = \"apse1\"\n"
        )),
        ["[directory] cluster: the fleet has no cluster `apse1`"]
    );
}

/// `cluster` names where DynamoDB Local runs, so it is refused without
/// `local = true`, and in a fleet without clusters, where it names none.
/// A fleet without clusters needs no `cluster` for its Local.
#[test]
fn a_directory_cluster_is_refused_where_it_names_nothing() {
    assert_eq!(
        problems(&format!("{CLUSTERED}{DIRECTORY}cluster = \"use1\"\n")),
        [
            "[directory] cluster: \"use1\" names the cluster that runs DynamoDB Local, so it needs local = true"
        ]
    );
    assert_eq!(
        problems(&format!(
            "{FLEET}{DIRECTORY}local = true\ncluster = \"use1\"\n"
        )),
        ["[directory] cluster: the fleet has no cluster `use1`"]
    );
    fleet(&format!("{FLEET}{DIRECTORY}local = true\n"))
        .check(Some(&laid()))
        .expect("a Local without clusters");
}
