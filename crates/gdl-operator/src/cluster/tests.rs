use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use http::{Request, Response, StatusCode};
use kube::client::Body;
use serde_json::Value;
use tower::service_fn;

use super::*;
use crate::probe::{Asked, Instances, Probe};
use crate::scaler::{Context, reconcile};
use crate::{GdlScalerSpec, TargetRef, Victims};

struct ApiState {
    deployment: Value,
    recreate: bool,
}

fn pod(n: u32) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": format!("pod-{n}"), "uid": format!("uid-{n}"),
            "creationTimestamp": format!("1970-01-01T00:00:0{n}Z"),
        },
        "spec": {"containers": [{"name": "front", "ports": [{"name": "system", "containerPort": 9090}]}]},
        "status": {"phase": "Running", "podIP": format!("10.0.0.{n}"), "conditions": [{"type": "Ready", "status": "True"}]},
    })
}

struct Quiet;

impl Probe for Quiet {
    async fn instances(&self, _: &str) -> Result<Instances, String> {
        Err("no hub needed".into())
    }

    async fn metrics(&self, _: &str, _: u16) -> Result<String, String> {
        Ok("dynamo_component_admission_admitted 0\n".into())
    }

    async fn retire(&self, _: &str, _: &str, _: u64) -> Result<Asked, String> {
        Err("no hub needed".into())
    }
}

async fn reconcile_across_recreation(recreate: bool) -> Value {
    let state = Arc::new(Mutex::new(ApiState {
        deployment: json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "global-front", "uid": "deployment-a", "resourceVersion": "10", "generation": 1},
            "spec": {"replicas": 3, "selector": {"matchLabels": {"app": "front"}}, "template": {}},
            "status": {"replicas": 3, "updatedReplicas": 3, "observedGeneration": 1},
        }),
        recreate,
    }));
    let spec = GdlScalerSpec {
        pool: "global".into(),
        segment: "front".into(),
        target_ref: TargetRef {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            name: "global-front".into(),
        },
        replicas: Some(2),
        min_replicas: None,
        suspend: None,
        max_concurrent_drains: None,
        victim: None,
        retire: None,
        drain_bound: None,
        routing_grace: None,
        service: None,
        metrics_prefix: None,
    };
    let scaler = GdlScaler::new("global-front", spec);
    let reply_scaler = serde_json::to_value(&scaler).expect("scaler JSON");
    let server_state = Arc::clone(&state);
    let service = service_fn(move |request: Request<Body>| {
        let state = Arc::clone(&server_state);
        let reply_scaler = reply_scaler.clone();
        async move {
            let method = request.method().clone();
            let path = request.uri().path().to_string();
            let bytes = request.into_body().collect_bytes().await.expect("body");
            let patch: Value = if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes).expect("JSON patch")
            };
            let mut state = state.lock().expect("API state");
            let mut code = StatusCode::OK;
            let body = if path.ends_with("/deployments/global-front") {
                if method == http::Method::PATCH {
                    let fenced = ["uid", "resourceVersion"].into_iter().any(|field| {
                        !patch["metadata"][field].is_null()
                            && patch["metadata"][field] != state.deployment["metadata"][field]
                    });
                    if fenced {
                        code = StatusCode::CONFLICT;
                        json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "Conflict", "message": "the Deployment was recreated", "code": 409})
                    } else {
                        state.deployment["spec"]["replicas"] = patch["spec"]["replicas"].clone();
                        state.deployment.clone()
                    }
                } else {
                    state.deployment.clone()
                }
            } else if path.ends_with("/pods") {
                json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": [pod(1), pod(2), pod(3)]})
            } else if path.ends_with("/pods/pod-3") && method == http::Method::PATCH {
                assert_eq!(patch["metadata"]["uid"], "uid-3");
                if state.recreate {
                    // Recreation wins after the cost patch, before the replica apply.
                    state.deployment["metadata"]["uid"] = "deployment-b".into();
                    state.deployment["metadata"]["resourceVersion"] = "20".into();
                    state.deployment["spec"]["replicas"] = 7.into();
                }
                pod(3)
            } else if path.ends_with("/gdlscalers/global-front/status") {
                reply_scaler
            } else {
                panic!("unexpected request: {method} {path}");
            };
            Ok::<_, Infallible>(
                Response::builder()
                    .status(code)
                    .body(Body::from(body.to_string().into_bytes()))
                    .expect("response"),
            )
        }
    });
    let cluster = KubeCluster::new(Client::new(service, "test"), "test");
    let victims = Victims::default();
    let cx = Context {
        cluster: &cluster,
        probe: &Quiet,
        victims: &victims,
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        reconcile(&cx, &scaler, 0),
    )
    .await
    .expect("reconcile completes")
    .ok();
    state.lock().expect("API state").deployment.clone()
}

#[tokio::test]
async fn an_unchanged_deployment_scales_normally() {
    let deployment = reconcile_across_recreation(false).await;
    assert_eq!(deployment["metadata"]["uid"], "deployment-a");
    assert_eq!(deployment["spec"]["replicas"], 2);
}

#[tokio::test]
async fn a_recreated_deployment_keeps_its_new_replica_count() {
    let deployment = reconcile_across_recreation(true).await;
    assert_eq!(deployment["metadata"]["uid"], "deployment-b");
    assert_eq!(
        deployment["spec"]["replicas"], 7,
        "the removal belongs to deployment-a"
    );
}

#[tokio::test]
async fn a_pod_patch_refuses_a_stale_version_and_a_recreated_uid() {
    let mut initial = pod(1);
    initial["metadata"]["resourceVersion"] = json!("10");
    initial["metadata"]["labels"] = json!({ROUTABLE: "true"});
    initial["metadata"]["annotations"] = json!({});
    let read =
        PodView::of(&serde_json::from_value(initial.clone()).expect("pod JSON")).expect("pod view");
    let state = Arc::new(Mutex::new(initial));
    let server_state = Arc::clone(&state);
    let service = service_fn(move |request: Request<Body>| {
        let state = Arc::clone(&server_state);
        async move {
            assert_eq!(request.method(), http::Method::PATCH);
            assert_eq!(request.uri().path(), "/api/v1/namespaces/test/pods/pod-1");
            let bytes = request
                .into_body()
                .collect_bytes()
                .await
                .expect("patch body");
            let patch: Value = serde_json::from_slice(&bytes).expect("patch JSON");
            let mut pod = state.lock().expect("pod state");
            let metadata = &patch["metadata"];
            let refused = ["uid", "resourceVersion"].into_iter().any(|field| {
                metadata
                    .get(field)
                    .is_some_and(|v| *v != pod["metadata"][field])
            });
            let (code, body) = if refused {
                (
                    StatusCode::CONFLICT,
                    json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "Conflict", "message": "the pod changed", "code": 409}),
                )
            } else {
                let before = pod.clone();
                for field in ["labels", "annotations"] {
                    if let Some(fields) = metadata[field].as_object() {
                        for (key, value) in fields {
                            let stored = pod["metadata"][field]
                                .as_object_mut()
                                .expect("pod metadata map");
                            if value.is_null() {
                                stored.remove(key);
                            } else {
                                stored.insert(key.clone(), value.clone());
                            }
                        }
                    }
                }
                if *pod != before {
                    let version = before["metadata"]["resourceVersion"]
                        .as_str()
                        .expect("pod version")
                        .parse::<u64>()
                        .expect("numeric version");
                    pod["metadata"]["resourceVersion"] = json!((version + 1).to_string());
                }
                (StatusCode::OK, pod.clone())
            };
            Ok::<_, Infallible>(
                Response::builder()
                    .status(code)
                    .body(Body::from(body.to_string().into_bytes()))
                    .expect("pod patch succeeds"),
            )
        }
    });
    let cluster = KubeCluster::new(Client::new(service, "test"), "test");
    let labelled = cluster
        .routable(&read, "10", false)
        .await
        .expect("pod patch succeeds");
    assert_eq!(labelled, "11");
    let priced = cluster
        .cost(&read, &labelled, Some(i32::MIN))
        .await
        .expect("pod patch succeeds");
    assert_eq!(priced, "12");
    let unchanged = cluster
        .cost(&read, &priced, Some(i32::MIN))
        .await
        .expect("pod patch succeeds");
    assert_eq!(unchanged, priced);
    assert!(matches!(
        cluster.cost(&read, &labelled, None).await,
        Err(Refused::Stale(_))
    ));
    {
        let mut pod = state.lock().expect("pod state");
        pod["metadata"]["uid"] = json!("replacement");
        pod["metadata"]["resourceVersion"] = json!("20");
    }
    assert!(matches!(
        cluster.routable(&read, "20", true).await,
        Err(Refused::Stale(_))
    ));
    assert_eq!(
        state.lock().expect("pod state")["metadata"]["labels"][ROUTABLE],
        "false"
    );
}

/// A pod's delete carries the uid and resourceVersion it was decided on,
/// as preconditions, so that a pod made again under its name, or changed
/// since, refuses it: the API server's 409 reads as stale.
#[tokio::test]
async fn a_pod_delete_is_fenced_by_its_uid_and_version() {
    let seen: Arc<Mutex<Vec<Value>>> = Arc::default();
    let bodies = Arc::clone(&seen);
    let service = service_fn(move |request: Request<Body>| {
        let bodies = Arc::clone(&bodies);
        async move {
            assert_eq!(request.method(), http::Method::DELETE);
            assert!(request.uri().path().ends_with("/pods/pod-3"));
            let bytes = request.into_body().collect_bytes().await.expect("body");
            let body: Value = serde_json::from_slice(&bytes).expect("delete options");
            let current = body["preconditions"]["resourceVersion"] == "10";
            bodies.lock().expect("bodies").push(body);
            let (code, reply) = if current {
                (StatusCode::OK, pod(3))
            } else {
                (
                    StatusCode::CONFLICT,
                    json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "Conflict", "message": "Precondition failed", "code": 409}),
                )
            };
            Ok::<_, Infallible>(
                Response::builder()
                    .status(code)
                    .body(Body::from(reply.to_string().into_bytes()))
                    .expect("response"),
            )
        }
    });
    let cluster = KubeCluster::new(Client::new(service, "test"), "test");
    let read = PodView::of(&serde_json::from_value(pod(3)).expect("a pod")).expect("a view");
    cluster.delete(&read, "10").await.expect("deleted");
    assert!(matches!(
        cluster.delete(&read, "9").await,
        Err(Refused::Stale(_))
    ));
    let seen = seen.lock().expect("bodies");
    assert_eq!(seen[0]["preconditions"]["uid"], "uid-3");
    assert_eq!(seen[1]["preconditions"]["resourceVersion"], "9");
}

/// A status write is a merge patch, which merges `last` key by key: a
/// removal without a handoff names it null, so that the last removal's
/// handoff does not stay.
#[tokio::test]
async fn a_status_write_clears_a_handoff_the_last_removal_had() {
    let seen: Arc<Mutex<Vec<Value>>> = Arc::default();
    let bodies = Arc::clone(&seen);
    let service = service_fn(move |request: Request<Body>| {
        let bodies = Arc::clone(&bodies);
        async move {
            assert!(
                request
                    .uri()
                    .path()
                    .ends_with("/gdlscalers/global-front/status")
            );
            let bytes = request.into_body().collect_bytes().await.expect("body");
            bodies
                .lock()
                .expect("bodies")
                .push(serde_json::from_slice(&bytes).expect("a merge patch"));
            let reply = json!({
                "apiVersion": "gdl.dev/v1alpha1", "kind": "GdlScaler",
                "metadata": {"name": "global-front", "resourceVersion": "2"},
                "spec": {"pool": "p", "segment": "s",
                         "targetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "d"}},
            });
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(reply.to_string().into_bytes()))
                    .expect("response"),
            )
        }
    });
    let cluster = KubeCluster::new(Client::new(service, "test"), "test");
    let status = GdlScalerStatus {
        last: Some(crate::Last {
            pod: "pod-2".into(),
            chosen: "uid-2".into(),
            deleted: "uid-2".into(),
            reason: String::new(),
            duration_ms: 1,
            outcome: crate::Outcome::Removed,
            handoff: None,
        }),
        ..GdlScalerStatus::default()
    };
    cluster
        .status("global-front", &status, Some("1"))
        .await
        .expect("written");
    let seen = seen.lock().expect("bodies");
    assert_eq!(seen[0]["status"]["last"]["handoff"], Value::Null);
    assert!(
        seen[0]["status"]["last"]
            .as_object()
            .expect("last")
            .contains_key("handoff"),
        "named, so the merge clears it: {}",
        seen[0]
    );
}
