//! What a cluster's owner applies to run the operator in one namespace:
//! the `GdlScaler` CRD, the operator's ServiceAccount, its Role and
//! RoleBinding there, and its Deployment. The CRD is the one cluster-scoped
//! object. The Role grants only what the scaler uses, in its namespace:
//! no Secrets, no ServiceAccounts, nothing cluster-wide, and no service
//! proxy, since the operator reaches hubs and pods from inside the cluster.

use kube::CustomResourceExt;
use serde_json::{Value, json};

use crate::crd::GdlScaler;

/// The name of the operator's ServiceAccount, Role, RoleBinding and
/// Deployment.
pub const NAME: &str = "gdl-operator";

/// The label every object here carries, so that its owner can find and
/// remove them.
const SETUP: &str = "gdl.dev/setup";

/// The CRDs, cluster-scoped: installed once per cluster by its owner.
pub fn crds() -> Vec<Value> {
    let mut crd = serde_json::to_value(GdlScaler::crd()).expect("a CRD encodes as JSON");
    crd["metadata"]["labels"] = json!({SETUP: NAME});
    vec![crd]
}

/// The operator's objects in `namespace`, its image `image`: the
/// ServiceAccount, the Role, the RoleBinding, and a StatefulSet of one
/// replica with its headless Service. A StatefulSet starts the replacement
/// of its pod only once the old one is confirmed gone, so two operators
/// never run at once, unless a person force-deletes the pod or removes its
/// node while the pod still runs.
pub fn namespaced(namespace: &str, image: &str) -> Vec<Value> {
    let meta = || json!({"name": NAME, "namespace": namespace, "labels": {SETUP: NAME}});
    let rule = |group: &str, resources: &[&str], verbs: &[&str]| json!({"apiGroups": [group], "resources": resources, "verbs": verbs});
    let read = ["get", "list", "watch"];
    let role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": meta(),
        "rules": [
            rule("gdl.dev", &["gdlscalers"], &["get", "list", "watch", "update", "patch"]),
            rule("gdl.dev", &["gdlscalers/status"], &["get", "update", "patch"]),
            rule("apps", &["deployments"], &["get", "list", "watch", "patch"]),
            rule("", &["pods"], &["get", "list", "watch", "patch"]),
            rule("discovery.k8s.io", &["endpointslices"], &read),
        ],
    });
    let binding = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": meta(),
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": NAME},
        "subjects": [{"kind": "ServiceAccount", "name": NAME, "namespace": namespace}],
    });
    let labels = json!({"app.kubernetes.io/name": NAME});
    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": meta(),
        "spec": {"clusterIP": "None", "selector": labels},
    });
    let set = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": meta(),
        "spec": {
            "replicas": 1,
            "serviceName": NAME,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "serviceAccountName": NAME,
                    "enableServiceLinks": false,
                    "containers": [{
                        "name": "operator",
                        "image": image,
                        "args": ["run", "--namespace", namespace],
                        "env": [{"name": "RUST_LOG", "value": "info"}],
                    }],
                },
            },
        },
    });
    vec![
        json!({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": meta()}),
        role,
        binding,
        service,
        set,
    ]
}

/// `objects` as a YAML stream, each a JSON document.
pub fn stream(objects: &[Value]) -> String {
    objects
        .iter()
        .map(|object| {
            format!(
                "---\n{}\n",
                serde_json::to_string_pretty(object).expect("JSON encodes")
            )
        })
        .collect()
}
