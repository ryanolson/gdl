use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use http::{Request, Response, StatusCode};
use kube::client::Body;
use serde_json::Value;
use tower::service_fn;

use super::*;
use crate::probe::{Instances, Probe};
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
#[ignore = "F4: a stale forced apply changes a recreated Deployment with a new UID"]
async fn a_recreated_deployment_keeps_its_new_replica_count() {
    let deployment = reconcile_across_recreation(true).await;
    assert_eq!(deployment["metadata"]["uid"], "deployment-b");
    assert_eq!(
        deployment["spec"]["replicas"], 7,
        "the removal belongs to deployment-a"
    );
}
