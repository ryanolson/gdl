use std::collections::BTreeMap;
use std::sync::Arc;

use gdl_model::{SegmentPhase, segment_service};
use gdl_watch::InstancePhase;
use tokio::sync::Semaphore;
use velo_hub::axum::{Json, Router, extract::Path, routing::get};
use velo_hub::client::{HubClient, HubServices};
use velo_hub::velo::Velo;
use velo_hub::velo::discovery::{ServiceDiscovery, ServiceRegistrationGuard};
use velo_hub::velo::transports::tcp::TcpTransportBuilder;

use super::super::instances::list;
use super::{Served, WAIT, spec};

mod order;

struct Directory {
    services: HubServices,
    entered: Arc<Semaphore>,
    gate: Arc<Semaphore>,
    task: tokio::task::JoinHandle<()>,
}

impl Directory {
    async fn new(services: HubServices, held: Vec<String>) -> Self {
        let entered = Arc::new(Semaphore::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a proxy port");
        let address = listener.local_addr().expect("a bound proxy port");
        let router = Router::new().route(
            "/v1/services/{name}",
            get({
                let entered = Arc::clone(&entered);
                let gate = Arc::clone(&gate);
                move |Path(name): Path<String>| {
                    let entered = Arc::clone(&entered);
                    let gate = Arc::clone(&gate);
                    let services = services.clone();
                    let held = held.contains(&name);
                    async move {
                        if held {
                            entered.add_permits(1);
                            tokio::time::timeout(WAIT, gate.acquire())
                                .await
                                .expect("the read is released")
                                .expect("the gate stays open")
                                .forget();
                        }
                        let ids = services
                            .get_instances(&name)
                            .await
                            .expect("the hub answers");
                        if !held {
                            entered.add_permits(1);
                        }
                        Json(velo_hub::protocol::ServiceInstancesResponse { instances: ids })
                    }
                }
            }),
        );
        let task = tokio::spawn(async move {
            velo_hub::axum::serve(listener, router)
                .await
                .expect("the proxy serves");
        });
        let services = HubClient::builder()
            .host("127.0.0.1")
            .public_port(address.port())
            .build()
            .expect("a proxy client")
            .services();
        Self {
            services,
            entered,
            gate,
            task,
        }
    }

    /// Wait until `reads` reads have reached their gates or finished. A
    /// segment's phases are read one after another, so a held read holds
    /// the ones after it.
    async fn entered(&self, reads: u32) {
        tokio::time::timeout(WAIT, self.entered.acquire_many(reads))
            .await
            .expect("the reads reach their gates or finish")
            .expect("the signal stays open")
            .forget();
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn member(served: &Served) -> (Velo, HubClient) {
    let tcp = TcpTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().expect("an address"))
        .build()
        .expect("a TCP transport");
    let node = Velo::builder()
        .add_transport(Arc::new(tcp))
        .mux_only()
        .build()
        .await
        .expect("a velo node");
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(served.hub.public_addr().port())
        .control_port(served.hub.control_addr().port())
        .build()
        .expect("a hub client");
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
        .expect("the hub admits the instance");
    (node, client)
}

async fn retirement_between_reads(withdraw_ready: bool) -> InstancePhase {
    let served = Served::new().await;
    let (node, client) = member(&served).await;
    let ready_name = segment_service("a", SegmentPhase::Ready);
    let mut ready = client
        .register_service(&ready_name)
        .await
        .expect("the hub takes ready");
    let admission = Arc::clone(served.pool.admission.get().expect("admission attached"));
    let services = client.services();
    let before = list(&admission, &services, &served.pool.feed)
        .await
        .expect("the directory answers");
    assert_eq!(before.instances.len(), 1);
    assert_eq!(before.instances[0].phase, InstancePhase::Ready);

    // The ready read, the first of the segment's, is held until the
    // instance has retired; the reads after it run only once it answers.
    let directory = Directory::new(services.clone(), vec![ready_name]).await;
    let listing = tokio::spawn({
        let admission = Arc::clone(&admission);
        let feed = Arc::clone(&served.pool.feed);
        let services = directory.services.clone();
        async move { list(&admission, &services, &feed).await }
    });
    directory.entered(1).await;
    let retired = client
        .register_service(&segment_service("a", SegmentPhase::Retired))
        .await
        .expect("the hub takes retirement");
    if withdraw_ready {
        ready.unregister().await.expect("the hub withdraws ready");
    }
    assert!(admission.member(node.instance_id()).is_some());
    directory.gate.add_permits(4);
    let during = tokio::time::timeout(WAIT, listing)
        .await
        .expect("the listing finishes")
        .expect("the listing task runs")
        .expect("the directory answers");
    let after = list(&admission, &services, &served.pool.feed)
        .await
        .expect("the directory answers");
    assert_eq!(after.instances.len(), 1);
    assert_eq!(after.instances[0].phase, InstancePhase::Retired);
    assert_eq!(during.instances.len(), 1, "the member stayed throughout");
    directory.stop().await;
    drop((ready, retired));
    served.stop().await;
    during.instances[0].phase
}

/// The control: with `ready` kept, the reads after the held one see the
/// retirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retaining_ready_across_the_reads_shows_the_retirement() {
    assert_eq!(
        retirement_between_reads(false).await,
        InstancePhase::Retired
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reused_id_does_not_preserve_the_departed_registration() {
    let served = Served::new().await;
    let (node, client) = member(&served).await;
    let retired = client
        .register_service(&segment_service("a", SegmentPhase::Retired))
        .await
        .expect("the hub takes retirement");
    let admission = Arc::clone(served.pool.admission.get().expect("admission attached"));
    let services = client.services();
    let before = list(&admission, &services, &served.pool.feed)
        .await
        .expect("the directory answers");
    assert_eq!(before.instances.len(), 1);
    assert_eq!(before.instances[0].segment, "a");
    assert_eq!(before.instances[0].phase, InstancePhase::Retired);

    let held = [
        SegmentPhase::Ready,
        SegmentPhase::Detached,
        SegmentPhase::Retired,
        SegmentPhase::Drained,
    ]
    .map(|phase| segment_service("a", phase))
    .into();
    let directory = Directory::new(services.clone(), held).await;
    let listing = tokio::spawn({
        let admission = Arc::clone(&admission);
        let feed = Arc::clone(&served.pool.feed);
        let services = directory.services.clone();
        async move { list(&admission, &services, &feed).await }
    });
    directory.entered(1).await;
    client.unregister().await.expect("the instance leaves");
    assert!(admission.member(node.instance_id()).is_none());
    drop(retired);
    client
        .register_peer(
            node.peer_info(),
            BTreeMap::from([(
                "gdl".to_string(),
                serde_json::to_value(spec().registration("f").expect("segment f"))
                    .expect("a registration"),
            )]),
        )
        .await
        .expect("the hub admits the same id for another segment");
    assert_eq!(
        admission.member(node.instance_id()).expect("a member").0,
        "f"
    );
    directory.gate.add_permits(4);
    let during = tokio::time::timeout(WAIT, listing)
        .await
        .expect("the listing finishes")
        .expect("the listing task runs")
        .expect("the directory answers");
    let after = list(&admission, &services, &served.pool.feed)
        .await
        .expect("the directory answers");
    assert_eq!(after.instances.len(), 1);
    assert_eq!(after.instances[0].segment, "f");
    assert_eq!(after.instances[0].phase, InstancePhase::Joining);
    directory.stop().await;
    served.stop().await;
    assert!(
        during.instances.is_empty(),
        "the departed registration survives under the replacement's id: {:?}",
        during.instances
    );
}
