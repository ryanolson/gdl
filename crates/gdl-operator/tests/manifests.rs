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

/// The Role grants only what the scaler uses, in its namespace: nothing
/// on Secrets or ServiceAccounts, no delete, and no service proxy.
#[test]
fn the_role_grants_only_what_the_scaler_uses() {
    let objects = gdl_operator::namespaced("gdl-api", "registry/gdl-operator:tag");
    let kinds: Vec<&str> = objects
        .iter()
        .filter_map(|object| object["kind"].as_str())
        .collect();
    assert_eq!(
        kinds,
        ["ServiceAccount", "Role", "RoleBinding", "Deployment"]
    );
    for object in &objects {
        assert_eq!(object["metadata"]["namespace"], "gdl-api");
    }
    let rules = objects[1]["rules"].as_array().expect("rules");
    let mut granted = Vec::new();
    for rule in rules {
        for resource in rule["resources"].as_array().expect("resources") {
            let verbs: Vec<&str> = rule["verbs"]
                .as_array()
                .expect("verbs")
                .iter()
                .filter_map(Value::as_str)
                .collect();
            granted.push((resource.as_str().expect("a resource").to_string(), verbs));
        }
    }
    for (resource, verbs) in &granted {
        assert!(!verbs.contains(&"delete"), "{resource} may be deleted");
        assert!(!verbs.contains(&"*"), "{resource} has every verb");
        for refused in ["secrets", "serviceaccounts", "services/proxy", "namespaces"] {
            assert_ne!(resource, refused);
        }
    }
    let resources: Vec<&str> = granted
        .iter()
        .map(|(resource, _)| resource.as_str())
        .collect();
    assert_eq!(
        resources,
        [
            "gdlscalers",
            "gdlscalers/status",
            "deployments",
            "pods",
            "endpointslices"
        ]
    );
    let container = &objects[3]["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(container["image"], "registry/gdl-operator:tag");
    assert_eq!(
        container["args"],
        serde_json::json!(["run", "--namespace", "gdl-api"])
    );
    assert_eq!(objects[3]["spec"]["strategy"]["type"], "Recreate");
}
