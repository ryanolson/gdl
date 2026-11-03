//! What [`KubePools`] sends the API server: each write's fence, its field
//! manager, whether it is forced, and a delete's propagation.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use http::{Request, Response};
use kube::client::Body;
use tower::service_fn;

use super::*;

#[path = "tests/kind.rs"]
mod kind;

/// A request: its method, its path and query, and its body.
type Seen = (String, String, Value);

/// A client whose every request is recorded and answered with an object
/// of the path's kind, named and with a uid.
fn recording() -> (KubePools, Arc<Mutex<Vec<Seen>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    let service = service_fn(move |request: Request<Body>| {
        let log = Arc::clone(&log);
        async move {
            let method = request.method().to_string();
            let uri = request.uri().to_string();
            let bytes = request.into_body().collect_bytes().await.expect("body");
            let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            log.lock().expect("log").push((method, uri.clone(), body));
            let reply = if uri.contains("/gdlpools/") {
                json!({
                    "apiVersion": "gdl.dev/v1alpha1", "kind": "GdlPool",
                    "metadata": {"name": "p", "namespace": "ns", "uid": "pool", "resourceVersion": "8"},
                    "spec": serde_json::to_value(crate::pool::tests::spec()).expect("spec"),
                })
            } else {
                json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"name": "p", "namespace": "ns", "uid": "u", "resourceVersion": "9"},
                })
            };
            Ok::<_, Infallible>(
                Response::builder()
                    .status(200)
                    .body(Body::from(reply.to_string().into_bytes()))
                    .expect("response"),
            )
        }
    });
    let pools = KubePools::new(Client::new(service, "ns"), "ns", false, false);
    (pools, seen)
}

#[tokio::test]
async fn a_delete_is_in_the_background_and_fenced_by_uid() {
    let (pools, seen) = recording();
    pools
        .delete(Kind::Deployment, "p-engine", "u1")
        .await
        .expect("a delete");
    let (method, uri, body) = seen.lock().expect("log")[0].clone();
    assert_eq!(method, "DELETE");
    assert_eq!(uri, "/apis/apps/v1/namespaces/ns/deployments/p-engine?");
    assert_eq!(body["propagationPolicy"], "Background");
    assert_eq!(body["preconditions"]["uid"], "u1");
}

#[tokio::test]
async fn the_finalizers_and_the_status_carry_the_version_read() {
    let (pools, seen) = recording();
    pools
        .finalizers("p", &["gdl.dev/stop".into()], "7")
        .await
        .expect("finalizers");
    pools
        .status("p", &crate::GdlPoolStatus::default(), "8")
        .await
        .expect("status");
    let seen = seen.lock().expect("log").clone();
    assert_eq!(
        seen[0].1,
        "/apis/gdl.dev/v1alpha1/namespaces/ns/gdlpools/p?"
    );
    assert_eq!(
        seen[0].2,
        json!({"metadata": {"resourceVersion": "7", "finalizers": ["gdl.dev/stop"]}})
    );
    assert_eq!(
        seen[1].1,
        "/apis/gdl.dev/v1alpha1/namespaces/ns/gdlpools/p/status?"
    );
    assert_eq!(seen[1].2["metadata"]["resourceVersion"], "8");
    // An absent field is named, so the merge patch removes it.
    for field in ["stop", "held", "observedGeneration", "phase"] {
        assert!(seen[1].2["status"][field].is_null(), "{field}");
        assert!(seen[1].2["status"].get(field).is_some(), "{field}");
    }
}

#[tokio::test]
async fn an_apply_is_forced_under_gdl_and_a_bootstrap_is_not() {
    let (pools, seen) = recording();
    let object = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "p", "namespace": "ns"}, "data": {},
    });
    let read = Live::of(
        Kind::ConfigMap,
        &json!({"metadata": {"name": "p", "uid": "u2"}}),
    )
    .expect("a view");
    pools.apply(&object, Some(&read)).await.expect("an apply");
    pools
        .write(
            Kind::GdlScaler,
            "p-engine",
            "gdl-bootstrap",
            json!({"replicas": 2}),
            "u3",
            Some("5"),
            false,
        )
        .await
        .expect("a bootstrap");
    pools
        .write(
            Kind::GdlScaler,
            "p-engine",
            "gdl-pool",
            json!({"suspend": "zero"}),
            "u3",
            None,
            true,
        )
        .await
        .expect("a suspend");
    let seen = seen.lock().expect("log").clone();
    assert_eq!(
        seen[0].1,
        "/api/v1/namespaces/ns/configmaps/p?&force=true&fieldManager=gdl"
    );
    assert_eq!(seen[0].2["metadata"]["uid"], "u2");
    assert_eq!(
        seen[1].1,
        "/apis/gdl.dev/v1alpha1/namespaces/ns/gdlscalers/p-engine?&fieldManager=gdl-bootstrap"
    );
    assert_eq!(
        seen[1].2["metadata"],
        json!({"name": "p-engine", "uid": "u3", "resourceVersion": "5"})
    );
    assert_eq!(
        seen[2].1,
        "/apis/gdl.dev/v1alpha1/namespaces/ns/gdlscalers/p-engine?&force=true&fieldManager=gdl-pool"
    );
    assert_eq!(
        seen[2].2["metadata"],
        json!({"name": "p-engine", "uid": "u3"})
    );
}

#[tokio::test]
async fn a_pool_stop_does_not_wait_for_terminal_pods() {
    for phase in ["Running", "Pending", "Failed", "Succeeded"] {
        let service = service_fn(move |request: Request<Body>| async move {
            let (kind, items) = if request.uri().path().ends_with("/replicasets") {
                (
                    "ReplicaSetList",
                    vec![json!({
                        "metadata": {"name": "p-engine-rs", "uid": "rs-uid", "labels": {"app": "engine"},
                            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "p-engine", "uid": "deployment-uid", "controller": true}]},
                    })],
                )
            } else {
                assert!(request.uri().path().ends_with("/pods"));
                (
                    "PodList",
                    vec![json!({
                        "metadata": {"name": "engine", "uid": "pod-uid", "labels": {"app": "engine"},
                            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "p-engine-rs", "uid": "rs-uid", "controller": true}]},
                        "status": {"phase": phase},
                    })],
                )
            };
            Ok::<_, Infallible>(
                Response::builder()
                    .status(200)
                    .body(Body::from(
                        json!({"apiVersion": "v1", "kind": kind, "items": items})
                            .to_string()
                            .into_bytes(),
                    ))
                    .expect("response"),
            )
        });
        let pools = KubePools::new(Client::new(service, "ns"), "ns", false, false);
        let deployment = Live::of(
            Kind::Deployment,
            &json!({
                "metadata": {"name": "p-engine", "uid": "deployment-uid"},
                "spec": {"replicas": 0, "selector": {"matchLabels": {"app": "engine"}}},
            }),
        )
        .expect("deployment");
        let pods = pools.pods(&deployment).await.expect("owned pods");
        let terminal = matches!(phase, "Failed" | "Succeeded");
        assert_eq!(
            pods.is_empty(),
            terminal,
            "phase {phase}: the stop uses an empty list as proof that no pod still runs: {pods:?}"
        );
    }
}

/// A DGD's pods are asked by the DGD's label alone. A segment's are those
/// of a component the DGD runs a segment in, and those that carry
/// `gdl.dev/config` in another component; a worker's, and an ended one,
/// are not.
#[tokio::test]
async fn a_dgds_segment_pods_are_found_by_component_or_annotation() {
    let service = service_fn(|request: Request<Body>| async move {
        assert!(request.uri().path().ends_with("/pods"));
        let query = request.uri().query().unwrap_or_default().to_string();
        assert_eq!(
            query,
            "&labelSelector=nvidia.com%2Fdynamo-graph-deployment-name%3Dp"
        );
        let pod = |name: &str, component: &str, marked: bool, phase: &str| {
            let mut pod = json!({
                "metadata": {"name": name, "uid": name, "labels": {
                    "nvidia.com/dynamo-graph-deployment-name": "p",
                    "nvidia.com/dynamo-component": component,
                }},
                "status": {"phase": phase},
            });
            if marked {
                pod["metadata"]["annotations"] = json!({"gdl.dev/config": "c"});
            }
            pod
        };
        let items = vec![
            pod("frontend", "Frontend", false, "Running"),
            pod("moved", "Worker", true, "Running"),
            pod("worker", "Worker", false, "Running"),
            pod("ended", "Frontend", true, "Succeeded"),
        ];
        Ok::<_, Infallible>(
            Response::builder()
                .status(200)
                .body(Body::from(
                    json!({"apiVersion": "v1", "kind": "PodList", "items": items})
                        .to_string()
                        .into_bytes(),
                ))
                .expect("response"),
        )
    });
    let pools = KubePools::new(Client::new(service, "ns"), "ns", false, true);
    let pods = pools
        .segment_pods("p", &["Frontend".to_string()])
        .await
        .expect("pods");
    let names: Vec<&str> = pods.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["frontend", "moved"]);
}
