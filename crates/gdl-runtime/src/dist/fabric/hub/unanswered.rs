use std::time::Duration;

use gdl_model::{SegmentPhase, segment_service};
use velo::discovery::ServiceDiscovery;

use super::HubSession;
use super::drains::spec;

#[allow(dead_code)]
#[path = "../../../../tests/common/proxy.rs"]
mod proxy;

const WAIT: Duration = Duration::from_secs(5);

async fn session(hub: &velo_hub::Hub, control: u16) -> (HubSession, velo::Velo) {
    let node = crate::dist::bind_node("127.0.0.1:0".parse().expect("address"))
        .await
        .expect("node");
    let session = HubSession::register(
        "127.0.0.1",
        hub.public_addr().port(),
        control,
        &node,
        spec().registration("sink").expect("registration"),
        Duration::from_secs(1),
    )
    .await
    .expect("registered");
    (session, node)
}

/// A follower can already be waiting to announce an avoidance promise
/// when leave queues behind an unanswered retirement.
#[tokio::test]
#[ignore = "F2: a sync queued before leave withdraws retired when the kept task is aborted"]
async fn a_waiting_acknowledgement_keeps_retired_until_leave() {
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    let hub = velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("hub");
    let control = proxy::Proxy::new(hub.control_addr()).await;
    let (mut session, node) = session(&hub, control.port).await;
    let deadline = tokio::time::Instant::now() + WAIT;
    session.loaded = Some(
        session
            .announce(SegmentPhase::Loaded, deadline)
            .await
            .expect("loaded"),
    );
    session.ready = Some(
        session
            .announce(SegmentPhase::Ready, deadline)
            .await
            .expect("ready"),
    );
    control.withhold_answers(b"PUT /v1/services/gdl-retired-");
    let (retired, ()) = tokio::join!(
        session.retire(tokio::time::Instant::now() + Duration::from_millis(200)),
        async {
            tokio::time::timeout(WAIT, control.withholding())
                .await
                .expect("the hub stored retired");
        }
    );
    assert!(retired.is_err());
    assert_eq!(session.unanswered.len(), 1);

    let client = session.client.clone();
    let name = super::avoids(velo::InstanceId::new_v4());
    let mut acknowledging = std::pin::pin!(client.register_service(&name));
    assert!(futures::poll!(acknowledging.as_mut()).is_pending());
    control.hold(b"DELETE /v1/instances/");
    let mut leaving = std::pin::pin!(session.leave());
    assert!(futures::poll!(leaving.as_mut()).is_pending());

    let services = client.services();
    let id = node.instance_id();
    let (acknowledged, (), (ready, loaded, retired)) =
        tokio::join!(acknowledging, leaving, async {
            tokio::time::timeout(WAIT, control.holding())
                .await
                .expect("leave reached the proxy");
            let mut phases = Vec::new();
            for phase in [
                SegmentPhase::Ready,
                SegmentPhase::Loaded,
                SegmentPhase::Retired,
            ] {
                let listed = services
                    .get_instances(&segment_service("sink", phase))
                    .await
                    .expect("the hub answers")
                    .contains(&id);
                phases.push(listed);
            }
            control.release();
            (phases[0], phases[1], phases[2])
        });
    acknowledged.expect("the follower announced its promise");
    control.answer();
    hub.shutdown().await.expect("the hub stopped");
    assert!(
        !(ready || loaded) || retired,
        "a sync queued ahead of leave withdrew retired: ready={ready}, loaded={loaded}, retired={retired}"
    );
}

/// The client's 60 s request timeout must not withdraw a retirement.
/// This test waits for that timeout and spends one more second retrying.
#[tokio::test]
#[ignore = "F3: the client's request timeout drops and withdraws an applied retirement"]
async fn a_client_timeout_cannot_withdraw_an_applied_retirement() {
    use futures::StreamExt;
    use velo::discovery::ServiceEvent;

    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    let hub = velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("hub");
    let control = proxy::Proxy::new(hub.control_addr()).await;
    let (mut session, node) = session(&hub, control.port).await;
    let deadline = tokio::time::Instant::now() + WAIT;
    session.loaded = Some(
        session
            .announce(SegmentPhase::Loaded, deadline)
            .await
            .expect("loaded"),
    );
    session.ready = Some(
        session
            .announce(SegmentPhase::Ready, deadline)
            .await
            .expect("ready"),
    );
    let services = session.client.services();
    let id = node.instance_id();
    let name = segment_service("sink", SegmentPhase::Retired);
    let mut retirements = services.watch_instances(&name).await.expect("retirements");
    assert!(matches!(
        retirements.next().await,
        Some(ServiceEvent::Initial(ids)) if ids.is_empty()
    ));
    control.withhold_answers(b"PUT /v1/services/gdl-retired-");
    let (finished, finish) = tokio::sync::oneshot::channel();
    let (_result, (ready, loaded, retired)) = tokio::join!(
        async {
            let result = session
                .retire(
                    tokio::time::Instant::now()
                        + velo_hub::client::DEFAULT_REQUEST_TIMEOUT
                        + Duration::from_secs(1),
                )
                .await;
            let _ = finished.send(());
            result
        },
        async {
            tokio::time::timeout(WAIT, control.withholding())
                .await
                .expect("the hub stored retired");
            assert!(
                services
                    .get_instances(&name)
                    .await
                    .expect("retired")
                    .contains(&id)
            );
            // Refuse retries, so a replacement cannot hide the withdrawal.
            control.refuse(b"PUT /v1/services/gdl-retired-");
            tokio::select! {
                () = async {
                    loop {
                        match retirements.next().await.expect("the watch stays open") {
                            ServiceEvent::Removed(removed) if removed == id => break,
                            _ => {}
                        }
                    }
                } => {},
                _ = finish => {},
            }
            let mut phases = Vec::new();
            for phase in [
                SegmentPhase::Ready,
                SegmentPhase::Loaded,
                SegmentPhase::Retired,
            ] {
                phases.push(
                    services
                        .get_instances(&segment_service("sink", phase))
                        .await
                        .expect("the hub answers")
                        .contains(&id),
                );
            }
            (phases[0], phases[1], phases[2])
        }
    );
    session.leave().await;
    control.answer();
    hub.shutdown().await.expect("the hub stopped");
    assert!(
        !(ready || loaded) || retired,
        "the client's timeout withdrew a stored retirement: ready={ready}, loaded={loaded}, retired={retired}"
    );
}
