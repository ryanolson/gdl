//! A fleet's Kubernetes objects, per cluster: pinned file for file, read
//! against the fleet's Compose bundle, and refused, with the reason named,
//! where the fleet cannot run on Kubernetes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::{Mirror, SPLIT, chat_spec, placed};

/// The api example's fleet over two clusters, each a parent pool and its
/// child: under the relay directory a child's hub reaches its parent's by
/// a Service name, which resolves only in its own cluster.
const FLEET: &str = "tests/fixtures/k8s/fleet.toml";

/// The pinned objects of [`FLEET`], one directory per cluster: what
/// `scripts/kubeconform.sh` validates by default.
const PINNED: &str = "tests/fixtures/k8s/out";

/// A file whose text is [`MARKER`]: the fleet's secret on the host, which
/// no generated file may hold.
const SECRET: &str = "tests/fixtures/k8s/secret";

/// The text of [`SECRET`].
pub(super) const MARKER: &str = "gdl-k8s-secret-marker-5f1c";

/// The tag the pinned objects name: the default tag digests what each
/// image is built from, which holds the test mirror's URL and commit.
const TAG: &str = "pinned";

fn manifest_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

pub(super) fn fleet_text() -> String {
    std::fs::read_to_string(manifest_dir().join(FLEET)).expect("the fleet")
}

pub(super) fn fleet(text: &str) -> gdl_model::Fleet {
    gdl_model::Fleet::from_toml(text).expect("a fleet")
}

/// The api example in [`SPLIT`]'s two classes, with what a Kubernetes
/// deployment of it reads from the spec: a published port, a secret, and
/// a drain bound.
pub(super) fn spec(mirror: &Mirror) -> gdl_gen::Spec {
    placed(mirror, &format!("{SPLIT}{}", deployment()))
}

/// The `[deployment]` tables of [`spec`].
pub(super) fn deployment() -> String {
    let secret = manifest_dir().join(SECRET);
    assert_eq!(
        std::fs::read_to_string(&secret).expect("the secret").trim(),
        MARKER
    );
    format!(
        "[deployment.secrets.fingerprint]\nfile = {:?}\n\
         [deployment.segment.front]\nports = [\"8080\"]\nsecrets = [\"fingerprint\"]\n\
         [deployment.segment.engine.config.runtime]\ndrain_ms = 90000\n",
        secret.display().to_string()
    )
}

/// The objects of `fleet` for `spec`, tagged [`TAG`].
fn render(
    mirror: &Mirror,
    spec: &gdl_gen::Spec,
    fleet: &gdl_model::Fleet,
) -> Result<gdl_gen::Files, gdl_gen::Error> {
    gdl_gen::k8s_files(
        spec,
        fleet,
        &mirror.target(),
        Some(TAG),
        None,
        gdl_gen::Sources::Fetched,
    )
}

/// The objects of a file the render writes: one JSON document each, in a
/// YAML stream.
fn documents(text: &str) -> Vec<Value> {
    text.split("---\n")
        .filter(|doc| !doc.trim().is_empty())
        .map(|doc| serde_json::from_str(doc).expect("a JSON object"))
        .collect()
}

/// Every object of `files`, by its kind and name. A pool's objects are
/// named for it, and so for one cluster; each cluster's base objects are
/// named `<cluster>/<name>` here.
pub(super) fn objects(files: &gdl_gen::Files) -> BTreeMap<(String, String), Value> {
    let mut objects = BTreeMap::new();
    for (path, text) in files {
        if path.extension().is_none_or(|e| e != "yaml") {
            continue;
        }
        let cluster = path.iter().next().expect("a cluster").to_string_lossy();
        for object in documents(text) {
            let kind = object["kind"].as_str().expect("a kind").to_string();
            let name = object["metadata"]["name"].as_str().expect("a name");
            let name = match kind.as_str() {
                "Namespace" | "ServiceAccount" | "PodMonitor" => format!("{cluster}/{name}"),
                _ => name.to_string(),
            };
            let key = (kind, name);
            assert!(
                objects.insert(key.clone(), object).is_none(),
                "{key:?} twice"
            );
        }
    }
    objects
}

/// A container's literal variables, by name; one taken from the pod has
/// its field path as its value.
fn env(container: &Value) -> BTreeMap<String, String> {
    container["env"]
        .as_array()
        .expect("env")
        .iter()
        .map(|var| {
            let value = match var.get("value") {
                Some(value) => value.as_str().expect("a string").to_string(),
                None => format!(
                    "fieldRef:{}",
                    var["valueFrom"]["fieldRef"]["fieldPath"]
                        .as_str()
                        .expect("a field")
                ),
            };
            (var["name"].as_str().expect("a name").to_string(), value)
        })
        .collect()
}

/// The one container of a Deployment.
pub(super) fn container(deployment: &Value) -> &Value {
    let containers = deployment["spec"]["template"]["spec"]["containers"]
        .as_array()
        .expect("containers");
    assert_eq!(containers.len(), 1, "{deployment}");
    &containers[0]
}

/// The render's files as the pinned directory holds them: the test
/// mirror's URL and commit, which move, as `<url>` and `<rev>`.
fn pinned_text(mirror: &Mirror, text: &str) -> String {
    text.replace(&mirror.url, "<url>")
        .replace(&mirror.rev, "<rev>")
}

/// `files` are the files pinned in `pinned`, a directory under the crate,
/// file for file. `GDL_UPDATE_FIXTURES=1` writes them again.
pub(super) fn assert_pinned(mirror: &Mirror, files: &gdl_gen::Files, pinned: &str) {
    let dir = manifest_dir().join(pinned);
    if std::env::var_os("GDL_UPDATE_FIXTURES").is_some() {
        let _ = std::fs::remove_dir_all(&dir);
        for (path, text) in files {
            let path = dir.join(path);
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("a directory");
            std::fs::write(path, pinned_text(mirror, text)).expect("a pinned file");
        }
        return;
    }
    let mut on_disk = Vec::new();
    let mut pending = vec![dir.clone()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("the pinned objects") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                on_disk.push(path.strip_prefix(&dir).expect("inside").to_path_buf());
            }
        }
    }
    on_disk.sort();
    let written: Vec<PathBuf> = files.keys().cloned().collect();
    assert_eq!(written, on_disk, "the files differ from {pinned}");
    for (path, text) in files {
        let expected = std::fs::read_to_string(dir.join(path)).expect("a pinned file");
        let found = pinned_text(mirror, text);
        for (found, expected) in found.lines().zip(expected.lines()) {
            assert_eq!(found, expected, "{} differs", path.display());
        }
        assert_eq!(found, expected, "{} differs", path.display());
    }
}

/// Every file the render writes for [`FLEET`] is pinned, so that a change
/// to any object is seen in review. `GDL_UPDATE_FIXTURES=1` writes them
/// again.
#[test]
fn a_fleets_objects_are_pinned() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("objects");
    assert_pinned(&mirror, &files, PINNED);
    // One directory per cluster, each with its base and one file per pool.
    for path in [
        "README.md",
        "euw1/base.yaml",
        "euw1/pools/global-eu.yaml",
        "euw1/pools/model-x-eu.yaml",
        "use1/base.yaml",
        "use1/pools/global-us.yaml",
        "use1/pools/model-x-us.yaml",
    ] {
        assert!(files.contains_key(Path::new(path)), "{path}");
    }
}

/// Each object carries the fleet's label, and each of a pool's objects
/// its pool's, class's, region's, and segment's; a Deployment's selector picks its
/// own pods only, by the labels that name them, and a hub's is never a
/// segment's.
#[test]
fn every_object_is_labelled_and_selects_its_own_pods() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("objects");
    let objects = objects(&files);
    let mut selectors = Vec::new();
    for ((kind, name), object) in &objects {
        let labels = &object["metadata"]["labels"];
        assert_eq!(labels["gdl.dev/fleet"], "api", "{kind} {name}");
        if matches!(kind.as_str(), "Namespace" | "ServiceAccount" | "PodMonitor") {
            continue;
        }
        for key in ["gdl.dev/pool", "gdl.dev/class", "gdl.dev/region"] {
            assert!(labels[key].is_string(), "{kind} {name}: {key}");
        }
        // A pool's settings are all its segments'.
        assert_eq!(
            labels["gdl.dev/segment"].is_string(),
            kind != "ConfigMap",
            "{kind} {name}"
        );
        // A selector names its pods by fleet, pool, and segment only: a
        // Deployment's selector cannot change, and a class's name can.
        // Where the operator scales the segments, an entry Service also
        // selects only the pods that take connections.
        let selector = match kind.as_str() {
            "Deployment" => &object["spec"]["selector"]["matchLabels"],
            "Service" => &object["spec"]["selector"],
            _ => continue,
        };
        let keys: Vec<&String> = selector.as_object().expect("a selector").keys().collect();
        let scaled = labels["gdl.dev/cluster"] == "use1";
        let entry = kind == "Service" && labels["gdl.dev/segment"] != "hub";
        let mut expected = vec!["gdl.dev/fleet", "gdl.dev/pool", "gdl.dev/segment"];
        if scaled && entry {
            expected.insert(2, "gdl.dev/routable");
            assert_eq!(selector["gdl.dev/routable"], "true", "{kind} {name}");
        }
        assert_eq!(keys, expected, "{kind} {name}");
        if kind == "Deployment" {
            let template = &object["spec"]["template"]["metadata"]["labels"];
            for (key, value) in selector.as_object().expect("a selector") {
                assert_eq!(&template[key], value, "{name}: {key}");
            }
            selectors.push((
                object["metadata"]["namespace"].clone(),
                selector["gdl.dev/pool"].clone(),
                selector["gdl.dev/segment"].clone(),
            ));
        }
    }
    let unique: std::collections::BTreeSet<String> =
        selectors.iter().map(|s| format!("{s:?}")).collect();
    assert_eq!(unique.len(), selectors.len(), "{selectors:?}");
}

/// A pool's hub and segments get the variables its Compose service gets,
/// from the same function: they differ only in the addresses each network
/// spells, and a hub advertises its pod's IP.
#[test]
fn the_pool_variables_are_composes_except_addresses() {
    let mirror = Mirror::new();
    let spec = spec(&mirror);
    let fleet = fleet(&fleet_text());
    let objects = objects(&render(&mirror, &spec, &fleet).expect("objects"));
    let temp = tempfile::Builder::new()
        .prefix("gdl-k8s-compose-")
        .tempdir()
        .expect("a directory");
    let compose = gdl_gen::compose_files(
        &spec,
        Some(&fleet),
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    let compose: Value =
        serde_json::from_str(&compose[Path::new("compose.yaml")]).expect("compose.yaml");
    let addresses = ["GDL_POOL__HUB", "GDL_POOL__PARENT_HUB", "GDL_HUB__HOST"];
    // Set by the cluster, which Compose does not read.
    let cluster = ["RUST_LOG"];
    let mut compared = 0;
    for (service, settings) in compose["services"].as_object().expect("services") {
        let deployment = service.replace('_', "-");
        let object = &objects[&("Deployment".to_string(), deployment.clone())];
        let ours = env(container(object));
        let theirs: BTreeMap<String, String> = settings["environment"]
            .as_object()
            .expect("environment")
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().expect("a string").to_string()))
            .collect();
        let strip = |vars: &BTreeMap<String, String>| {
            vars.iter()
                .filter(|(name, _)| {
                    name.as_str() != "GDL_POD_IP" && !cluster.contains(&name.as_str())
                })
                .map(|(name, value)| {
                    let value = match addresses.contains(&name.as_str()) {
                        true => "<address>".to_string(),
                        false => value.clone(),
                    };
                    (name.clone(), value)
                })
                .collect::<BTreeMap<_, _>>()
        };
        assert_eq!(strip(&ours), strip(&theirs), "{service}");
        compared += 1;
        let is_hub = !service.contains('_');
        if is_hub {
            // The hub's record names the hub at its pod's IP, which only
            // the pod knows.
            assert_eq!(ours["GDL_POD_IP"], "fieldRef:status.podIP", "{service}");
            assert_eq!(ours["GDL_POOL__HUB"], "$(GDL_POD_IP):1337", "{service}");
            let names: Vec<&str> = container(object)["env"]
                .as_array()
                .expect("env")
                .iter()
                .map(|v| v["name"].as_str().expect("a name"))
                .collect();
            let ip = names.iter().position(|n| *n == "GDL_POD_IP");
            let hub = names.iter().position(|n| *n == "GDL_POOL__HUB");
            assert!(ip < hub, "{names:?}: a $(VAR) reads only what is above it");
        } else {
            let pool = service.split_once('_').expect("a pool").0;
            assert_eq!(ours["GDL_HUB__HOST"], pool, "{service}");
            assert_eq!(ours["GDL_POOL__HUB"], format!("{pool}:1337"), "{service}");
        }
        if let Some(parent) = ours.get("GDL_POOL__PARENT_HUB") {
            // A child's parent's hub, by its Service, in its own cluster.
            assert_eq!(
                Some(parent),
                theirs.get("GDL_POOL__PARENT_HUB"),
                "{service}"
            );
        }
    }
    assert_eq!(compared, 8, "four pools, each a hub and one segment");
}

/// The fleet's secret is named, never read: the file that the spec names
/// holds a marker, and no file the render writes holds it.
#[test]
fn a_secrets_value_appears_in_no_object() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("objects");
    for (path, text) in &files {
        assert!(!text.contains(MARKER), "{}", path.display());
        assert!(
            !text.contains(SECRET),
            "{}: names the host file",
            path.display()
        );
    }
    // The secret is mounted where Compose mounts it, from the Secret that
    // the cluster names, under the spec's name for it.
    let objects = objects(&files);
    let front = &objects[&("Deployment".to_string(), "global-us-front".to_string())];
    let pod = &front["spec"]["template"]["spec"];
    let volume = pod["volumes"]
        .as_array()
        .expect("volumes")
        .iter()
        .find(|v| v["name"] == "secrets")
        .expect("the secrets volume");
    assert_eq!(
        volume["projected"]["sources"],
        serde_json::json!([{"secret": {"name": "api-fingerprint", "items": [{"key": "fingerprint", "path": "fingerprint"}]}}])
    );
    let mount = container(front)["volumeMounts"]
        .as_array()
        .expect("mounts")
        .iter()
        .find(|m| m["name"] == "secrets")
        .expect("the secrets mount")
        .clone();
    assert_eq!(mount["mountPath"], "/run/secrets");
    // The engine mounts none.
    let engine = &objects[&("Deployment".to_string(), "model-x-us-engine".to_string())];
    assert!(!engine.to_string().contains("api-fingerprint"), "{engine}");
}

/// What a pool's pods are told: replicas from the pool's quorum, the
/// drain bound's grace, probes on the system port, a hub of one replica
/// that is never two at once, and a ready probe on the hub's own health,
/// never the pool's, which a parent answers 503 until its children are
/// ready.
#[test]
fn a_pools_pods_are_sized_and_probed_as_its_settings_say() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("objects");
    assert!(!files.values().any(|text| text.contains("gdl-pool/health")));
    let objects = objects(&files);
    let deployment = |name: &str| &objects[&("Deployment".to_string(), name.to_string())];
    for (name, replicas, grace) in [
        ("global-us-front", 2, 630),
        ("global-eu-front", 2, 630),
        ("model-x-us-engine", 2, 120),
        ("model-x-eu-engine", 1, 120),
    ] {
        let object = deployment(name);
        // Where the operator scales the segments (use1), the Deployment
        // leaves `replicas` to the scaler, whose minimum is the quorum.
        match objects.get(&("GdlScaler".to_string(), name.to_string())) {
            Some(scaler) => {
                assert!(object["spec"].get("replicas").is_none(), "{name}");
                assert_eq!(scaler["spec"]["minReplicas"], replicas, "{name}");
                assert!(scaler["spec"].get("replicas").is_none(), "{name}");
            }
            None => assert_eq!(object["spec"]["replicas"], replicas, "{name}"),
        }
        let pod = &object["spec"]["template"]["spec"];
        assert_eq!(pod["terminationGracePeriodSeconds"], grace, "{name}");
        let container = container(object);
        assert_eq!(container["livenessProbe"]["httpGet"]["path"], "/live");
        assert_eq!(container["livenessProbe"]["httpGet"]["port"], 9090);
        assert_eq!(container["readinessProbe"]["httpGet"]["path"], "/health");
        assert_eq!(container["readinessProbe"]["httpGet"]["port"], 9090);
        let segment = name.rsplit_once('-').expect("a segment").1;
        assert_eq!(
            container["command"],
            serde_json::json!([
                "/usr/local/bin/segment",
                format!("/etc/gdl/fleet/{segment}.toml")
            ]),
            "{name}: the pool's ConfigMap is what the pod reads"
        );
    }
    for pool in ["global-us", "model-x-us", "global-eu", "model-x-eu"] {
        let hub = deployment(pool);
        assert_eq!(hub["spec"]["replicas"], 1, "{pool}");
        assert_eq!(hub["spec"]["strategy"]["type"], "Recreate", "{pool}");
        let container = container(hub);
        for probe in ["livenessProbe", "readinessProbe"] {
            assert_eq!(container[probe]["httpGet"]["path"], "/v1/health", "{pool}");
            assert_eq!(container[probe]["httpGet"]["port"], 1337, "{pool}");
        }
        let config = &objects[&("ConfigMap".to_string(), pool.to_string())];
        assert!(config["data"]["deployment.json"].is_string(), "{pool}");
    }
    // Only a parent keeps state, on a claim of its own, writable by the
    // image's user.
    let claims: Vec<&String> = objects
        .keys()
        .filter(|(kind, _)| kind == "PersistentVolumeClaim")
        .map(|(_, name)| name)
        .collect();
    assert_eq!(claims, ["global-eu-state", "global-us-state"]);
    let parent = &deployment("global-us")["spec"]["template"]["spec"];
    assert_eq!(parent["securityContext"]["fsGroup"], 65532);
    assert_eq!(
        env(container(deployment("global-us")))["GDL_POOL__STATE_DIR"],
        "/var/lib/gdl-hub"
    );
    // A cluster's platform picks its nodes, and its registry its images.
    let eu = &deployment("global-eu-front")["spec"]["template"]["spec"];
    assert_eq!(
        eu["nodeSelector"],
        serde_json::json!({"kubernetes.io/arch": "arm64", "kubernetes.io/os": "linux"})
    );
    assert_eq!(
        container(deployment("global-eu-front"))["image"],
        "registry.example/gdl-eu/api-front:pinned"
    );
    assert_eq!(
        container(deployment("global-us"))["image"],
        "registry.example/gdl/api-hub:pinned"
    );
    // The entry port is a ClusterIP Service of each pool that runs it.
    for pool in ["global-us", "global-eu"] {
        let service = &objects[&("Service".to_string(), format!("{pool}-front"))];
        assert_eq!(service["spec"]["type"], "ClusterIP");
        assert_eq!(service["spec"]["ports"][0]["port"], 8080);
    }
    assert!(!objects.contains_key(&("Service".to_string(), "model-x-us-engine".to_string())));
    // The cluster's role reaches its pods through the service account.
    let account = &objects[&("ServiceAccount".to_string(), "use1/api".to_string())];
    assert_eq!(account["metadata"]["namespace"], "gdl-api");
}

/// `k8s` writes the objects into a fresh directory, and refuses one that
/// exists.
#[test]
fn the_objects_are_written_into_a_fresh_directory() {
    let mirror = Mirror::new();
    let temp = tempfile::Builder::new()
        .prefix("gdl-k8s-out-")
        .tempdir()
        .expect("a directory");
    let out = temp.path().join("k8s");
    let spec = spec(&mirror);
    let fleet = fleet(&fleet_text());
    gdl_gen::k8s(
        &spec,
        &fleet,
        &out,
        &mirror.target(),
        Some(TAG),
        None,
        gdl_gen::Sources::Fetched,
    )
    .expect("written");
    assert!(out.join("use1/base.yaml").is_file());
    assert!(out.join("euw1/pools/model-x-eu.yaml").is_file());
    let again = gdl_gen::k8s(
        &spec,
        &fleet,
        &out,
        &mirror.target(),
        Some(TAG),
        None,
        gdl_gen::Sources::Fetched,
    )
    .expect_err("the directory exists");
    assert!(again.to_string().contains("already exists"), "{again}");
}

/// The render's refusal of `fleet`, which it names.
fn refused(mirror: &Mirror, spec: &gdl_gen::Spec, fleet: &str) -> String {
    render(mirror, spec, &self::fleet(fleet))
        .expect_err("refused")
        .to_string()
}

/// Each fleet that cannot run on Kubernetes is refused, with what is
/// missing named at its table. `check` accepts each: it does not see the
/// spec's secrets, and a fleet for Compose needs none of these.
#[test]
fn a_fleet_that_cannot_run_on_kubernetes_is_refused() {
    let mirror = Mirror::new();
    let spec = spec(&mirror);
    let base = fleet_text();
    let cases: Vec<(String, &[&str])> = vec![
        (
            base.replace("[fleet]\nname = \"api\"\n", ""),
            &["`[fleet] name`"],
        ),
        (
            base.replace("registry = \"registry.example/gdl-eu\"\n", ""),
            &["[cluster.euw1]", "`registry`"],
        ),
        (
            base.replace("platform = \"linux/arm64\"\n", ""),
            &["[cluster.euw1]", "`platform`"],
        ),
        (
            base.replace(
                "class = \"model_x\"\ncluster = \"euw1\"\n",
                "class = \"model_x\"\nregion = \"eu-west-1\"\n",
            ),
            &["[pool.model-x-eu]", "`cluster`"],
        ),
        (
            base.replace(
                "secrets = { fingerprint = \"api-fingerprint\" }",
                "secrets = { fingerprint = \"api-fingerprint\", other = \"api-other\" }",
            ),
            &["[cluster.use1]", "`other`", "[deployment.secrets"],
        ),
        (
            base.replace("RUST_LOG = \"info\"", "GDL_POOL__NAME = \"x\""),
            &["[cluster.use1]", "`GDL_POOL__NAME`"],
        ),
        (
            base.replace("RUST_LOG = \"info\"", "gdl_hub__host = \"x\""),
            &["[cluster.use1]", "`gdl_hub__host`"],
        ),
        (
            base.replace("RUST_LOG = \"info\"", "GDL_POD_IP = \"x\""),
            &["[cluster.use1]", "`GDL_POD_IP`"],
        ),
        (
            base.replace("platform = \"linux/arm64\"", "platform = \"arm64\""),
            &["[cluster.euw1]", "`arm64`"],
        ),
    ];
    for (fleet, named) in cases {
        let checked = gdl_model::Fleet::from_toml(&fleet)
            .expect("a fleet")
            .check(None);
        assert!(checked.is_ok(), "{checked:?}\n{fleet}");
        let refusal = refused(&mirror, &spec, &fleet);
        for name in named {
            assert!(refusal.contains(name), "{name} in {refusal}");
        }
    }
}

/// Under the relay directory a child's hub reaches its parent's by its
/// Service name, which does not resolve in another cluster: a fleet whose
/// child and parent are in two clusters is refused, naming both pools.
#[test]
fn a_relay_fleet_across_clusters_is_refused() {
    let mirror = Mirror::new();
    let crossed = fleet_text()
        .replace("home = \"global-us\"", "home = \"global-xx\"")
        .replace("home = \"global-eu\"", "home = \"global-us\"")
        .replace("home = \"global-xx\"", "home = \"global-eu\"");
    let refusal = refused(&mirror, &spec(&mirror), &crossed);
    for name in [
        "[pool.model-x-eu]",
        "`global-us`",
        "`use1`",
        "`euw1`",
        "[pool.model-x-us]",
        "`global-eu`",
        "relay",
    ] {
        assert!(refusal.contains(name), "{name} in {refusal}");
    }
}

/// A pool's segment that mounts a secret its cluster does not map could
/// never start: refused, naming the pool, the segment, and the cluster.
#[test]
fn a_secret_the_cluster_does_not_map_is_refused() {
    let mirror = Mirror::new();
    let unmapped = fleet_text().replace("secrets = { fingerprint = \"api-fingerprint-eu\" }\n", "");
    let refusal = refused(&mirror, &spec(&mirror), &unmapped);
    for name in [
        "[pool.global-eu]",
        "`front`",
        "`fingerprint`",
        "[cluster.euw1]",
    ] {
        assert!(refusal.contains(name), "{name} in {refusal}");
    }
}

/// What a Kubernetes object cannot name or publish is refused rather than
/// renamed: a segment whose name is not a DNS label, and a port range.
#[test]
fn a_segment_kubernetes_cannot_name_or_publish_is_refused() {
    let mirror = Mirror::new();
    let fleet = fleet_text();
    // No secrets: these specs mount none.
    let fleet = fleet
        .replace("secrets = { fingerprint = \"api-fingerprint\" }\n", "")
        .replace("secrets = { fingerprint = \"api-fingerprint-eu\" }\n", "");
    let underscored = placed(&mirror, &SPLIT.replace("engine", "big_engine"));
    let refusal = refused(
        &mirror,
        &underscored,
        &fleet.replace("engine = ", "big_engine = "),
    );
    assert!(refusal.contains("`big_engine`"), "{refusal}");
    let ranged = placed(
        &mirror,
        &format!("{SPLIT}[deployment.segment.front]\nports = [\"8080-8081:8080-8081\"]\n"),
    );
    let refusal = refused(&mirror, &ranged, &fleet);
    assert!(refusal.contains("`8080-8081:8080-8081`"), "{refusal}");
}

/// [`FLEET`] with no secrets, for a spec that mounts none.
fn unsecret() -> String {
    fleet_text()
        .replace("secrets = { fingerprint = \"api-fingerprint\" }\n", "")
        .replace("secrets = { fingerprint = \"api-fingerprint-eu\" }\n", "")
}

/// A pod's probes ask the segment's system listener, so a segment whose
/// settings turn it off is refused at the key that does.
#[test]
fn a_segment_without_a_system_port_is_refused() {
    let mirror = Mirror::new();
    let off = placed(
        &mirror,
        &format!("{SPLIT}[deployment.segment.engine.config.system]\nport = -1\n"),
    );
    let refusal = refused(&mirror, &off, &unsecret());
    assert!(
        refusal.contains("[deployment.segment.engine.config.system] port"),
        "{refusal}"
    );
}

/// A Service's name is a DNS label: a pool whose entry Service would be
/// longer is refused, naming the Service, rather than renamed.
#[test]
fn a_service_name_longer_than_a_label_is_refused() {
    let mirror = Mirror::new();
    // A pool name of 59 characters, and a Service name of 65.
    let long = format!("global-{}", "u".repeat(52));
    let fleet = unsecret().replace("global-us", &long);
    let spec = placed(
        &mirror,
        &format!("{SPLIT}[deployment.segment.front]\nports = [\"8080\"]\n"),
    );
    let refusal = refused(&mirror, &spec, &fleet);
    assert!(refusal.contains(&format!("[pool.{long}]")), "{refusal}");
    assert!(refusal.contains(&format!("`{long}-front`")), "{refusal}");
}

/// Two pools' objects of one kind never share a name in a cluster: pool
/// `global`'s segment `us` and pool `global-us`'s hub would both be
/// Deployment `global-us`.
#[test]
fn two_objects_of_one_name_are_refused() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, &SPLIT.replace("front", "us"));
    let fleet = "[fleet]\nname = \"api\"\n\
        [cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-api\"\nregion = \"us-east-1\"\n\
        registry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n\
        [pool.global]\nclass = \"global\"\ncluster = \"use1\"\n\
        [pool.global-us]\nclass = \"global\"\ncluster = \"use1\"\n\
        [pool.model-a]\nclass = \"model_x\"\ncluster = \"use1\"\nhome = \"global\"\n\
        [pool.model-b]\nclass = \"model_x\"\ncluster = \"use1\"\nhome = \"global-us\"\n";
    let refusal = refused(&mirror, &spec, fleet);
    for name in ["Deployment `global-us`", "`global`", "[pool.global-us]"] {
        assert!(refusal.contains(name), "{name} in {refusal}");
    }
}

/// A source that reads its requests on stdin has none in a pod: refused,
/// naming the source.
#[test]
fn a_source_that_reads_stdin_is_refused() {
    let mirror = Mirror::new();
    let spec = chat_spec(
        &mirror,
        "\n[class.chat]\nsegments = [\"front\", \"engine\", \"post\"]\n",
    );
    let fleet = "[fleet]\nname = \"chat\"\n\
        [cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-chat\"\nregion = \"us-east-1\"\n\
        registry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n\
        [pool.chat]\nclass = \"chat\"\ncluster = \"use1\"\n";
    let refusal = refused(&mirror, &spec, fleet);
    assert!(
        refusal.contains("source `Frontend` reads its requests on stdin"),
        "{refusal}"
    );
}

#[path = "k8s/credentials.rs"]
mod credentials;
#[path = "k8s/dynamodb.rs"]
mod dynamodb;
#[path = "k8s/images.rs"]
mod images;
#[path = "k8s/labels.rs"]
mod labels;
#[path = "k8s/placement.rs"]
mod placement;
#[path = "k8s/ports.rs"]
mod ports;
#[path = "k8s/probes.rs"]
mod probes;
#[path = "k8s/rollout.rs"]
mod rollout;
#[path = "k8s/scrape.rs"]
mod scrape;
#[path = "k8s/secrets.rs"]
mod secrets;
#[path = "k8s/variables.rs"]
mod variables;
