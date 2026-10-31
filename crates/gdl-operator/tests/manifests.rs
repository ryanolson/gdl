//! The objects a cluster's owner applies to run the operator.
//!
//! The CRD is pinned in `tests/fixtures/crds.yaml`, so that a change to its
//! schema shows in review. Run with `GDL_UPDATE_FIXTURES=1` to write it
//! again.

use std::path::Path;

use serde_json::Value;

#[test]
fn the_crd_is_the_pinned_one() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/crds.yaml");
    let text = gdl_operator::stream(&gdl_operator::crds());
    if std::env::var_os("GDL_UPDATE_FIXTURES").is_some() {
        std::fs::write(&path, &text).expect("write the fixture");
    }
    let pinned = std::fs::read_to_string(&path).expect("read the fixture");
    assert!(
        pinned == text,
        "the CRD changed; run with GDL_UPDATE_FIXTURES=1 to pin it again"
    );
}

/// The scale subresource is what an HPA targets: it maps the spec's and
/// the status's replicas and the status's selector, which the HPA needs as
/// text.
#[test]
fn the_crd_serves_scale_for_an_hpa() {
    let crds = gdl_operator::crds();
    let crd = &crds[0];
    assert_eq!(crd["spec"]["scope"], "Namespaced");
    let version = &crd["spec"]["versions"][0];
    assert_eq!(version["name"], "v1alpha1");
    assert_eq!(
        version["subresources"]["scale"],
        serde_json::json!({
            "specReplicasPath": ".spec.replicas",
            "statusReplicasPath": ".status.replicas",
            "labelSelectorPath": ".status.selector",
        })
    );
    assert!(version["subresources"]["status"].is_object());
    let status = &version["schema"]["openAPIV3Schema"]["properties"]["status"]["properties"];
    assert_eq!(status["selector"]["type"], "string");
    assert_eq!(status["replicas"]["type"], "integer");
}

/// The schema refuses what the scaler cannot do: a retire mode other than
/// `signal`, a suspend other than `zero`, a minimum below 1, and a
/// duration it does not read.
#[test]
fn the_schema_refuses_what_the_scaler_cannot_do() {
    let crds = gdl_operator::crds();
    let spec = &crds[0]["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"];
    assert_eq!(spec["retire"]["enum"], serde_json::json!(["signal"]));
    assert_eq!(spec["suspend"]["enum"], serde_json::json!(["zero"]));
    assert_eq!(spec["minReplicas"]["minimum"], 1.0);
    assert_eq!(spec["maxConcurrentDrains"]["minimum"], 1.0);
    assert_eq!(spec["replicas"]["minimum"], 0.0);
    for field in ["drainBound", "routingGrace"] {
        assert_eq!(spec[field]["pattern"], "^[0-9]+(ms|s|m|h)$", "{field}");
    }
    let required: Vec<&str> = crds[0]["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
        ["properties"]["spec"]["required"]
        .as_array()
        .expect("required fields")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert_eq!(required, ["pool", "segment", "targetRef"]);
}

/// The Role grants only what the operator uses, in its namespace: a
/// pool's objects, which it makes and deletes; pods, which it reads and
/// labels but never deletes; and events. Nothing on Secrets or
/// ServiceAccounts, no Lease, and no service proxy.
#[test]
fn the_role_grants_only_what_the_operator_uses() {
    let objects = gdl_operator::namespaced("gdl-api", "registry/gdl-operator:tag");
    let kinds: Vec<&str> = objects
        .iter()
        .filter_map(|object| object["kind"].as_str())
        .collect();
    assert_eq!(
        kinds,
        [
            "ServiceAccount",
            "Role",
            "RoleBinding",
            "Service",
            "StatefulSet"
        ]
    );
    for object in &objects {
        assert_eq!(object["metadata"]["namespace"], "gdl-api");
    }
    let rules = objects[1]["rules"].as_array().expect("rules");
    let mut granted = Vec::new();
    for rule in rules {
        let group = rule["apiGroups"][0].as_str().expect("a group");
        let verbs: Vec<&str> = rule["verbs"]
            .as_array()
            .expect("verbs")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        for resource in rule["resources"].as_array().expect("resources") {
            let resource = resource.as_str().expect("a resource");
            granted.push((format!("{group}/{resource}"), verbs.join(",")));
        }
    }
    for (resource, verbs) in &granted {
        assert!(!verbs.contains('*'), "{resource} has every verb");
        for refused in [
            "/secrets",
            "/serviceaccounts",
            "/services/proxy",
            "/namespaces",
            "coordination.k8s.io/leases",
        ] {
            assert_ne!(resource, refused);
        }
    }
    let write = "get,list,watch,create,update,patch,delete";
    let granted: Vec<(&str, &str)> = granted
        .iter()
        .map(|(resource, verbs)| (resource.as_str(), verbs.as_str()))
        .collect();
    assert_eq!(
        granted,
        [
            ("gdl.dev/gdlpools", "get,list,watch,update,patch"),
            ("gdl.dev/gdlpools/status", "get,update,patch"),
            ("gdl.dev/gdlpools/finalizers", "update"),
            ("gdl.dev/gdlscalers", write),
            ("gdl.dev/gdlscalers/status", "get,update,patch"),
            ("apps/deployments", write),
            ("/services", write),
            ("/configmaps", write),
            ("/persistentvolumeclaims", write),
            ("/pods", "get,list,watch,patch"),
            ("apps/replicasets", "get,list,watch"),
            ("discovery.k8s.io/endpointslices", "get,list,watch"),
            ("monitoring.coreos.com/podmonitors", write),
            ("events.k8s.io/events", "create,patch"),
        ]
    );
}

/// A `GdlPool` is namespaced, has a status of its own and no scale, and
/// its schema requires what a pool's objects are rendered from.
#[test]
fn the_pool_crd_holds_a_resolved_pool() {
    let crds = gdl_operator::crds();
    let crd = &crds[1];
    assert_eq!(crd["spec"]["names"]["kind"], "GdlPool");
    assert_eq!(crd["spec"]["scope"], "Namespaced");
    let version = &crd["spec"]["versions"][0];
    assert!(version["subresources"]["status"].is_object());
    assert!(version["subresources"].get("scale").is_none());
    let schema = &version["schema"]["openAPIV3Schema"]["properties"]["spec"];
    let required: Vec<&str> = schema["required"]
        .as_array()
        .expect("required fields")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert_eq!(
        required,
        [
            "class",
            "cluster",
            "deployment",
            "fleet",
            "hub",
            "region",
            "segments",
            "serviceAccountName"
        ]
    );
    let spec = &schema["properties"];
    assert_eq!(
        spec["presence"]["enum"],
        serde_json::json!(["serving", "scaledToZero"])
    );
    assert_eq!(spec["parent"]["properties"]["home"]["type"], "string");
    assert_eq!(spec["parent"]["properties"]["parents"]["type"], "string");
    assert_eq!(spec["allowPoolRestart"]["type"], "boolean");
    let status = &version["schema"]["openAPIV3Schema"]["properties"]["status"]["properties"];
    assert_eq!(
        status["stop"]["properties"]["step"]["enum"],
        serde_json::json!(["drain", "segments", "hub", "release"])
    );
}

/// The operator is a StatefulSet of one replica: Kubernetes starts its
/// replacement only once the old pod is confirmed gone, so two never run
/// at once. Its headless Service is the StatefulSet's governing Service.
#[test]
fn the_operator_runs_as_one_stateful_replica() {
    let objects = gdl_operator::namespaced("gdl-api", "registry/gdl-operator:tag");
    let service = &objects[3];
    let set = &objects[4];
    assert_eq!(service["spec"]["clusterIP"], "None");
    assert_eq!(set["spec"]["serviceName"], service["metadata"]["name"]);
    assert_eq!(set["spec"]["replicas"], 1);
    assert_eq!(
        service["spec"]["selector"],
        set["spec"]["selector"]["matchLabels"]
    );
    let container = &set["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(container["image"], "registry/gdl-operator:tag");
    assert_eq!(
        container["args"],
        serde_json::json!(["run", "--namespace", "gdl-api"])
    );
}
