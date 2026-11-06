//! A fleet under the etcd directory on Kubernetes: every hub publishes to
//! one etcd, with none of the relay's state claims or parent's hubs, so a
//! child may run in another cluster than its parents. The etcd runs in
//! the directory's cluster alone, with the backend's compaction and quota,
//! its data on a claim of its own: its cluster's hubs reach it at its
//! Service, and the other clusters' hubs at its pod's IP, which only
//! `deploy` can read, so the render writes a placeholder there.

use super::*;

const ETCD_FLEET: &str = "tests/fixtures/k8s/etcd/fleet.toml";
const ETCD_PINNED: &str = "tests/fixtures/k8s/etcd/out";

/// The endpoints of the hubs outside the directory's cluster until
/// `deploy` writes etcd's pod IP in its place.
const PLACEHOLDER: &str = "[\"http://ETCD_POD_IP:2379\"]";

fn etcd_text() -> String {
    std::fs::read_to_string(manifest_dir().join(ETCD_FLEET)).expect("the fleet")
}

fn variables(objects: &BTreeMap<(String, String), Value>, name: &str) -> BTreeMap<String, String> {
    env(container(
        &objects[&("Deployment".to_string(), name.to_string())],
    ))
}

#[test]
fn an_etcd_fleets_objects_are_pinned() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&etcd_text())).expect("objects");
    assert_pinned(&mirror, &files, ETCD_PINNED);
    assert!(files.contains_key(Path::new("use1/base.yaml")));
    assert!(!files.keys().any(|path| path.ends_with("tables.yaml")));
    assert!(!files.contains_key(Path::new("dynamodb/tables.json")));
}

/// Every hub and segment publishes to etcd: the directory's cluster at its
/// Service, the other at the placeholder. None reports to a parent's hub
/// or keeps a state claim, so the shared pool in `euw1`, whose parents run
/// in both clusters, renders.
#[test]
fn every_pool_publishes_to_etcd() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&etcd_text())).expect("objects");
    let objects = objects(&files);
    let home = "[\"http://etcd:2379\"]";
    for (deployment, endpoints) in [
        ("global-us", home),
        ("global-us-front", home),
        ("model-x-us", home),
        ("model-x-us-engine", home),
        ("global-eu", PLACEHOLDER),
        ("model-x-shared", PLACEHOLDER),
        ("model-x-shared-engine", PLACEHOLDER),
    ] {
        let env = variables(&objects, deployment);
        for (name, value) in [
            ("GDL_POOL__DIRECTORY", "etcd"),
            ("GDL_POOL__ENDPOINTS", endpoints),
        ] {
            assert_eq!(
                env.get(name).map(String::as_str),
                Some(value),
                "{deployment}: {name}"
            );
        }
        for other in [
            "GDL_POOL__PARENT_HUB",
            "GDL_POOL__STATE_DIR",
            "GDL_POOL__RECORDS_TABLE",
            "GDL_POOL__ENDPOINT",
        ] {
            assert!(!env.contains_key(other), "{deployment}: {other}");
        }
    }
    let readme = &files[Path::new("README.md")];
    assert!(readme.contains("ETCD_POD_IP"), "{readme}");
}

/// One etcd runs in the directory's cluster, from the pinned image, with
/// the backend's flags, advertising its pod's IP; its data is on a claim
/// of its own, which a Recreate rollout hands from one pod to the next. A
/// Service of its name fronts it. The other cluster runs none of it, and
/// nothing of DynamoDB is rendered.
#[test]
fn one_etcd_runs_in_the_directorys_cluster_on_a_claim() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&etcd_text())).expect("objects");
    let objects = objects(&files);
    let etcd = &objects[&("Deployment".to_string(), "etcd".to_string())];
    assert_eq!(etcd["metadata"]["labels"]["gdl.dev/cluster"], "use1");
    assert_eq!(etcd["metadata"]["labels"]["gdl.dev/directory"], "etcd");
    assert_eq!(etcd["spec"]["replicas"], 1);
    assert_eq!(etcd["spec"]["strategy"]["type"], "Recreate");
    let pod = &etcd["spec"]["template"]["spec"];
    assert_eq!(
        pod["volumes"],
        serde_json::json!([{"name": "data", "persistentVolumeClaim": {"claimName": "etcd-data"}}])
    );
    let main = container(etcd);
    assert!(
        main["image"]
            .as_str()
            .expect("an image")
            .starts_with("gcr.io/etcd-development/etcd@sha256:"),
        "{main}"
    );
    let args: Vec<&str> = main["args"]
        .as_array()
        .expect("args")
        .iter()
        .map(|arg| arg.as_str().expect("a word"))
        .collect();
    for flag in [
        "--auto-compaction-mode=periodic",
        "--auto-compaction-retention=1h",
        "--quota-backend-bytes=8589934592",
    ] {
        assert!(args.contains(&flag), "{flag}: {args:?}");
    }
    let advertise = args
        .iter()
        .position(|arg| *arg == "--advertise-client-urls")
        .expect("an advertised address");
    assert_eq!(args[advertise + 1], "http://$(POD_IP):2379");
    assert_eq!(
        main["env"],
        serde_json::json!([{"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}])
    );
    assert_eq!(
        main["volumeMounts"],
        serde_json::json!([{"name": "data", "mountPath": "/var/lib/etcd"}])
    );
    let claim = &objects[&("PersistentVolumeClaim".to_string(), "etcd-data".to_string())];
    assert_eq!(claim["metadata"]["labels"]["gdl.dev/cluster"], "use1");
    assert_eq!(
        claim["spec"]["accessModes"],
        serde_json::json!(["ReadWriteOnce"])
    );
    let service = &objects[&("Service".to_string(), "etcd".to_string())];
    assert_eq!(service["spec"]["ports"][0]["port"], 2379);
    assert!(!files[Path::new("euw1/base.yaml")].contains("\"etcd\""));
    assert!(
        !objects
            .keys()
            .any(|(_, name)| name.starts_with("dynamodb-")),
        "{:?}",
        objects.keys()
    );
}

/// A pool cannot take the name of the etcd's Deployment and Service.
#[test]
fn a_pool_named_for_the_etcd_objects_is_refused() {
    let mirror = Mirror::new();
    let spec = spec(&mirror);
    let refusal = refused(&mirror, &spec, &etcd_text().replace("model-x-us", "etcd"));
    assert!(refusal.contains("`etcd`"), "{refusal}");
}
