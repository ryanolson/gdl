use std::convert::Infallible;
use std::sync::Mutex;

use http::{Request, Response};
use kube::client::Body;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tower::service_fn;

use super::*;

struct ApiState {
    lease: Value,
    deployment: Value,
    scaler: Value,
    writes: Vec<(String, i32)>,
}

async fn scale_with_lease(stall_renewal: bool) -> Vec<(String, i32)> {
    let scaler: GdlScaler = serde_json::from_value(json!({
        "apiVersion": "gdl.dev/v1alpha1", "kind": "GdlScaler",
        "metadata": {"name": "global-front", "resourceVersion": "20", "generation": 1},
        "spec": {
            "pool": "global", "segment": "front", "replicas": 3,
            "targetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "global-front"},
        },
    }))
    .expect("scaler");
    let state = Arc::new(Mutex::new(ApiState {
        lease: json!({
            "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
            "metadata": {"name": "gdl-operator", "resourceVersion": "1"},
            "spec": {},
        }),
        deployment: json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "global-front", "uid": "deployment-a", "resourceVersion": "10", "generation": 1},
            "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "front"}}, "template": {}},
            "status": {"replicas": 2, "updatedReplicas": 2, "observedGeneration": 1},
        }),
        scaler: serde_json::to_value(&scaler).expect("scaler JSON"),
        writes: Vec::new(),
    }));
    let renewal = Arc::new(Notify::new());
    let server_state = Arc::clone(&state);
    let server_renewal = Arc::clone(&renewal);
    let service = service_fn(move |request: Request<Body>| {
        let state = Arc::clone(&server_state);
        let renewal = Arc::clone(&server_renewal);
        async move {
            let method = request.method().clone();
            let path = request.uri().path().to_string();
            let bytes = request.into_body().collect_bytes().await.expect("body");
            let patch: Value = if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes).expect("patch")
            };
            if stall_renewal
                && path.ends_with("/leases/gdl-operator")
                && method == http::Method::PATCH
                && state.lock().expect("API state").lease["spec"]["holderIdentity"] == "operator-a"
                && patch["spec"]["renewTime"].is_string()
                && patch["spec"].get("holderIdentity").is_none()
            {
                renewal.notify_one();
                futures::future::pending::<()>().await;
            }
            let mut state = state.lock().expect("API state");
            let body = if path.ends_with("/leases/gdl-operator") {
                if method == http::Method::PATCH {
                    for (field, value) in patch["spec"].as_object().expect("lease spec") {
                        state.lease["spec"][field] = value.clone();
                    }
                }
                state.lease.clone()
            } else if path.ends_with("/gdlscalers/global-front") {
                state.scaler.clone()
            } else if path.ends_with("/gdlscalers/global-front/status") {
                state.scaler["status"] = patch["status"].clone();
                state.scaler["metadata"]["resourceVersion"] = json!("21");
                state.scaler.clone()
            } else if path.ends_with("/deployments/global-front") {
                if method == http::Method::PATCH {
                    assert_eq!(
                        patch["metadata"]["uid"],
                        state.deployment["metadata"]["uid"]
                    );
                    assert_eq!(
                        patch["metadata"]["resourceVersion"],
                        state.deployment["metadata"]["resourceVersion"]
                    );
                    let holder = state.lease["spec"]["holderIdentity"]
                        .as_str()
                        .expect("holder")
                        .to_string();
                    let replicas = patch["spec"]["replicas"].as_i64().expect("count") as i32;
                    state.writes.push((holder, replicas));
                    state.deployment["spec"]["replicas"] = patch["spec"]["replicas"].clone();
                    state.deployment["metadata"]["resourceVersion"] = json!("11");
                }
                state.deployment.clone()
            } else if path.ends_with("/pods") {
                json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": []})
            } else {
                panic!("unexpected request: {method} {path}");
            };
            Ok::<_, Infallible>(Response::new(Body::from(body.to_string().into_bytes())))
        }
    });
    let client = Client::new(service, "test");
    // The shorter lease exercises expiration without waiting the production lease's 15 seconds.
    let elector = |identity: &str| {
        crate::lease::Elector::new(
            client.clone(),
            "test",
            crate::NAME,
            identity,
            Duration::from_secs(2),
        )
    };
    let first = elector("operator-a");
    tokio::time::timeout(Duration::from_secs(3), first.acquire())
        .await
        .expect("first leader responds")
        .expect("first leader");
    let clock = first.clock();
    let holding = tokio::spawn(async move { first.hold().await });
    if stall_renewal {
        tokio::time::timeout(Duration::from_secs(3), renewal.notified())
            .await
            .expect("renewal reaches the API");
        let renewed = state.lock().expect("API state").lease["spec"]["renewTime"]
            .as_str()
            .expect("renewTime")
            .parse::<k8s_openapi::jiff::Timestamp>()
            .expect("timestamp");
        let expires = renewed.as_millisecond() + 2_000;
        let remaining =
            (expires - k8s_openapi::jiff::Timestamp::now().as_millisecond()).max(0) as u64;
        tokio::time::sleep(Duration::from_millis(remaining + 1)).await;
        assert!(k8s_openapi::jiff::Timestamp::now().as_millisecond() >= expires);
        // The successor takes the lease once it has seen the record
        // unchanged for a whole lease, by its own clock.
        tokio::time::timeout(Duration::from_secs(5), elector("operator-b").acquire())
            .await
            .expect("successor responds")
            .expect("successor leads");
        assert_eq!(
            state.lock().expect("API state").lease["spec"]["holderIdentity"],
            "operator-b"
        );
    }
    let shared = Arc::new(Shared {
        lease: Arc::clone(&clock),
        scalers: Api::namespaced(client.clone(), "test"),
        cluster: KubeCluster::new(client, "test").leased(clock),
        probe: HttpProbe::new("test").expect("probe"),
        victims: Victims::default(),
    });
    tokio::time::timeout(Duration::from_secs(3), reconcile(Arc::new(scaler), shared))
        .await
        .expect("reconcile responds")
        .expect("reconcile");
    holding.abort();
    let _ = holding.await;
    std::mem::take(&mut state.lock().expect("API state").writes)
}

#[tokio::test]
async fn a_current_lease_holder_scales() {
    assert_eq!(scale_with_lease(false).await, [("operator-a".into(), 3)]);
}

/// About 4 s: the successor waits out a 2 s lease after the stalled
/// renewal, and then sees the record unchanged for another.
#[tokio::test]
async fn a_lease_holder_stops_writing_when_renewal_stalls_past_expiration() {
    let writes = scale_with_lease(true).await;
    assert!(
        writes.is_empty(),
        "the former leader writes while the successor holds the lease: {writes:?}"
    );
}
