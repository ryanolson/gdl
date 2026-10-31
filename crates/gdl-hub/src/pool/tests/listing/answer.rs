use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use gdl_model::{SegmentPhase, segment_service};
use gdl_watch::InstancePhase;
use tokio::sync::Semaphore;
use velo_hub::axum::{Router, body::to_bytes, extract::Request};
use velo_hub::client::HubClient;

use super::{Served, WAIT, list, member, spec};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_confirmed_retirement_answer_preserves_the_listed_phase() {
    let (before, after) = retirement_answer(false).await;
    assert_eq!(before, InstancePhase::Retired);
    assert_eq!(after, before);
}

/// An instance whose retirement announcement goes unanswered drops it,
/// and the hub withdraws `retired`; it still holds `ready`, which the
/// instance withdraws only once `retired` is answered. The list shows what
/// the hub holds: the instance is `ready` again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_retirement_answer_lists_the_instance_ready_again() {
    let (before, after) = retirement_answer(true).await;
    assert_eq!(before, InstancePhase::Retired);
    assert_eq!(
        after,
        InstancePhase::Ready,
        "the list does not show the instance as the hub holds it"
    );
}

async fn retirement_answer(cancel: bool) -> (InstancePhase, InstancePhase) {
    let served = Served::new().await;
    let (node, initial) = member(&served).await;
    initial
        .unregister()
        .await
        .expect("the setup registration ends");
    let retired_name = segment_service("a", SegmentPhase::Retired);
    let proxy = Answer::new(served.hub.control_addr(), retired_name.clone()).await;
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(served.hub.public_addr().port())
        .control_port(proxy.port)
        .build()
        .expect("a client through the proxy");
    client
        .register_peer(
            node.peer_info(),
            BTreeMap::from([(
                "gdl".to_string(),
                serde_json::to_value(spec().registration("a").expect("segment a"))
                    .expect("a registration"),
            )]),
        )
        .await
        .expect("the observed registration begins");
    let ready = client
        .register_service(&segment_service("a", SegmentPhase::Ready))
        .await
        .expect("the hub takes ready");
    let admission = Arc::clone(served.pool.admission.get().expect("admission attached"));
    let incarnation = admission.members()[0].0;
    let services = client.services();
    let announcing = tokio::spawn({
        let client = client.clone();
        async move { client.register_service(&retired_name).await }
    });
    proxy.observed(&proxy.applied).await;
    let before = list(&admission, &services, &served.pool.feed)
        .await
        .expect("the directory answers");
    assert_eq!(before.instances.len(), 1);
    assert_eq!(before.instances[0].phase, InstancePhase::Retired);
    // HubSession::announce's timeout drops this same register_service future.
    let retired = if cancel {
        announcing.abort();
        assert!(
            announcing
                .await
                .expect_err("the announcement is cancelled")
                .is_cancelled()
        );
        proxy.observed(&proxy.withdrawn).await;
        None
    } else {
        proxy.release.add_permits(1);
        Some(
            tokio::time::timeout(WAIT, announcing)
                .await
                .expect("the answer arrives")
                .expect("the task runs")
                .expect("the hub confirms retirement"),
        )
    };
    let after = list(&admission, &services, &served.pool.feed)
        .await
        .expect("the directory answers");
    assert_eq!(after.instances.len(), 1);
    assert_eq!(after.instances[0].peer.instance_id, node.instance_id());
    assert_eq!(
        admission.members()[0].0,
        incarnation,
        "the same registration stays admitted"
    );
    client
        .unregister()
        .await
        .expect("the observed registration ends");
    drop((ready, retired));
    proxy.release.add_permits(1);
    proxy.task.abort();
    let _ = proxy.task.await;
    served.stop().await;
    (before.instances[0].phase, after.instances[0].phase)
}

struct Answer {
    port: u16,
    applied: Arc<Semaphore>,
    withdrawn: Arc<Semaphore>,
    release: Arc<Semaphore>,
    task: tokio::task::JoinHandle<()>,
}

impl Answer {
    async fn new(target: SocketAddr, held: String) -> Self {
        let applied = Arc::new(Semaphore::new(0));
        let withdrawn = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a proxy port");
        let port = listener.local_addr().expect("a bound proxy port").port();
        let router = Router::new().fallback({
            let applied = Arc::clone(&applied);
            let withdrawn = Arc::clone(&withdrawn);
            let release = Arc::clone(&release);
            let http = reqwest::Client::new();
            move |request: Request| {
                let applied = Arc::clone(&applied);
                let withdrawn = Arc::clone(&withdrawn);
                let release = Arc::clone(&release);
                let http = http.clone();
                let held = held.clone();
                async move {
                    let (parts, body) = request.into_parts();
                    let body = to_bytes(body, 32 * 1024).await.expect("a bounded request");
                    let is_held = parts
                        .uri
                        .path()
                        .starts_with(&format!("/v1/services/{held}/instances/"));
                    let response = http
                        .request(
                            parts.method.clone(),
                            format!("http://{target}{}", parts.uri),
                        )
                        .headers(parts.headers)
                        .body(body)
                        .send()
                        .await
                        .expect("the hub answers the proxy");
                    let status = response.status();
                    let body = response.bytes().await.expect("the response body arrives");
                    assert!(
                        status.is_success(),
                        "the hub accepts the forwarded operation"
                    );
                    if is_held && parts.method == reqwest::Method::PUT {
                        applied.add_permits(1);
                        tokio::time::timeout(WAIT, release.acquire())
                            .await
                            .expect("the answer is released")
                            .expect("the gate stays open")
                            .forget();
                    } else if is_held && parts.method == reqwest::Method::DELETE {
                        withdrawn.add_permits(1);
                    }
                    (status, body)
                }
            }
        });
        let task = tokio::spawn(async move {
            velo_hub::axum::serve(listener, router)
                .await
                .expect("the proxy serves");
        });
        Self {
            port,
            applied,
            withdrawn,
            release,
            task,
        }
    }

    async fn observed(&self, signal: &Semaphore) {
        tokio::time::timeout(WAIT, signal.acquire())
            .await
            .expect("the operation reaches the hub")
            .expect("the signal stays open")
            .forget();
    }
}
