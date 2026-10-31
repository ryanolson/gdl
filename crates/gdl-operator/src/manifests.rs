//! What a cluster's owner applies to run the operator in one namespace:
//! the `GdlScaler` and `GdlPool` CRDs, the operator's ServiceAccount, its
//! Role and RoleBinding there, and its StatefulSet. The CRDs are the only
//! cluster-scoped objects. The Role grants only what the operator uses,
//! in its namespace: a pool's objects, which it makes and deletes, its
//! pods, which it reads and labels, and events. No Secrets, no
//! ServiceAccounts, nothing cluster-wide, and no service proxy, since the
//! operator reaches hubs and pods from inside the cluster. PodMonitors are
//! granted whether or not the cluster has their CRD: a rule for a kind the
//! cluster does not serve grants nothing.

use kube::CustomResourceExt;
use serde_json::{Value, json};

use crate::crd::GdlScaler;
use crate::pool::GdlPool;

/// The name of the operator's ServiceAccount, Role, RoleBinding and
/// Deployment.
pub const NAME: &str = "gdl-operator";

/// The label every object here carries, so that its owner can find and
/// remove them.
const SETUP: &str = "gdl.dev/setup";

/// The CRDs, cluster-scoped: installed once per cluster by its owner.
pub fn crds() -> Vec<Value> {
    [
        serde_json::to_value(GdlScaler::crd()),
        serde_json::to_value(GdlPool::crd()),
    ]
    .into_iter()
    .map(|crd| {
        let mut crd = crd.expect("a CRD encodes as JSON");
        crd["metadata"]["labels"] = json!({SETUP: NAME});
        crd
    })
    .collect()
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
    let write = [
        "get", "list", "watch", "create", "update", "patch", "delete",
    ];
    let role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": meta(),
        "rules": [
            rule("gdl.dev", &["gdlpools"], &["get", "list", "watch", "update", "patch"]),
            rule("gdl.dev", &["gdlpools/status"], &["get", "update", "patch"]),
            rule("gdl.dev", &["gdlpools/finalizers"], &["update"]),
            rule("gdl.dev", &["gdlscalers"], &write),
            rule("gdl.dev", &["gdlscalers/status"], &["get", "update", "patch"]),
            rule("apps", &["deployments"], &write),
            rule("", &["services", "configmaps", "persistentvolumeclaims"], &write),
            rule("", &["pods"], &["get", "list", "watch", "patch"]),
            rule("apps", &["replicasets"], &read),
            rule("discovery.k8s.io", &["endpointslices"], &read),
            rule("monitoring.coreos.com", &["podmonitors"], &write),
            rule("events.k8s.io", &["events"], &["create", "patch"]),
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
