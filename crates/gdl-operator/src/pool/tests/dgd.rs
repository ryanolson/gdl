//! A pool whose segment runs in a DynamoGraphDeployment (DGD): the pool
//! owns the DGD, dynamo's controllers run its pods, and the stop waits
//! for the segment's pods but not for dynamo's workers.

use super::*;

/// Pool `p` with segment `engine` in component `Frontend` of its DGD,
/// beside dynamo's `Worker`, with a parent when `parent`.
fn dgd_pool(parent: bool) -> GdlPool {
    let mut pool = pool(parent);
    pool.spec.pool.segments[0].component = Some("Frontend".into());
    pool.spec.pool.dgd = Some(
        serde_json::json!({
            "apiVersion": "nvidia.com/v1beta1",
            "kind": "DynamoGraphDeployment",
            "metadata": {"name": "p"},
            "spec": {"components": [
                {"name": "Frontend", "type": "frontend", "scalingAdapter": {}},
                {"name": "Worker", "type": "decode", "replicas": 3},
            ]},
        })
        .to_string(),
    );
    pool
}

/// A fake that holds the DGD pool, in a cluster that serves DGDs when
/// `dgds`.
fn fake(parent: bool, dgds: bool) -> Fake {
    let mut fake = Fake::with(dgd_pool(parent));
    fake.dgds = dgds;
    fake
}

/// The DGD pool made and running: its gdl objects applied and their pods
/// up, and the DGD's two frontends and three workers up.
async fn running_dgd(parent: bool) -> Fake {
    let fake = fake(parent, true);
    fake.reconcile(0).await.expect("the pool is made");
    fake.settle();
    let mut state = fake.state();
    state.dgd_pods.insert("Frontend".into(), 2);
    state.dgd_pods.insert("Worker".into(), 3);
    state.log.clear();
    state.status_writes = 0;
    drop(state);
    fake
}

/// The pool owns its DGD: it makes it beside its hub and its other
/// segment, and makes no Deployment or scaler for the segment in it.
#[tokio::test]
async fn the_pool_makes_its_dgd() {
    let fake = fake(false, true);
    fake.reconcile(0).await.expect("a reconcile");
    let log = fake.log();
    assert!(
        log.contains(&"create DynamoGraphDeployment/p".to_string()),
        "{log:?}"
    );
    assert!(log.contains(&"create Deployment/p".to_string()), "{log:?}");
    assert!(
        log.contains(&"create Deployment/p-front".to_string()),
        "{log:?}"
    );
    assert!(!log.iter().any(|e| e.contains("p-engine")), "{log:?}");
    assert_eq!(
        condition(&fake, "Dgd"),
        Some(("True".into(), "Rendered".into()))
    );
}

/// In a cluster that does not serve DGDs, a pool with one applies
/// nothing and says why: it is never half made and called ready.
#[tokio::test]
async fn a_pool_with_a_dgd_in_a_cluster_without_dgds_applies_nothing() {
    let fake = fake(false, false);
    fake.reconcile(0).await.expect("a reconcile");
    let log = fake.log();
    assert!(
        !log.iter()
            .any(|e| e.starts_with("create") || e.starts_with("apply")),
        "{log:?}"
    );
    assert_eq!(
        condition(&fake, "Dgd"),
        Some(("False".into(), "NoCrd".into()))
    );
    assert_eq!(
        condition(&fake, "Applied"),
        Some(("False".into(), "NoDgdCrd".into()))
    );
    assert_eq!(fake.status().phase, Some(crate::pool::Phase::Pending));
    assert!(fake.state().events.contains(&"NoCrd".to_string()));
}

/// The stop drains the pool, then waits for the segment's pods in the DGD
/// before it stops the hub; it then lets the pool go while dynamo's
/// workers still run, for garbage collection to delete with the DGD.
#[tokio::test]
async fn the_hub_waits_for_the_dgd_segment_and_not_for_the_workers() {
    let fake = running_dgd(true).await;
    fake.delete(false);
    // The fake hub withdraws the record at once, so one reconcile drains
    // the pool and goes on to its segments.
    fake.reconcile(1_000).await.expect("a reconcile");
    assert!(
        fake.log().contains(&"drain".to_string()),
        "{:?}",
        fake.log()
    );
    assert_eq!(step(&fake), Some(StopStep::Segments));
    fake.reconcile(2_000).await.expect("a reconcile");

    // The gdl segment's pods are gone; the DGD's frontends still run.
    fake.settle();
    fake.reconcile(3_000).await.expect("a reconcile");
    fake.reconcile(4_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    // The segments step itself waits: the stop never stores the hub's
    // step while a segment pod runs in the DGD.
    let log = fake.log();
    assert!(
        !log.iter()
            .any(|e| e.starts_with("write Deployment/p ") || e.starts_with("status Hub")),
        "{log:?}"
    );

    fake.state().dgd_pods.insert("Frontend".into(), 0);
    fake.reconcile(5_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Hub));
    fake.settle();
    fake.reconcile(6_000).await.expect("a reconcile");
    assert!(released(&fake), "{:?}", fake.log());
    assert_eq!(fake.state().dgd_pods["Worker"], 3);
    let log = fake.log();
    assert!(!log.iter().any(|e| e.starts_with("VIOLATION")), "{log:?}");
}

/// A frontend of the DGD that runs again once the stop is at its hub
/// takes the stop back to its segments, before the hub goes.
#[tokio::test]
async fn a_dgd_segment_pod_that_runs_again_takes_the_stop_back() {
    let fake = running_dgd(false).await;
    fake.state().dgd_pods.insert("Frontend".into(), 0);
    fake.delete(false);
    fake.reconcile(1_000).await.expect("a reconcile");
    fake.settle();
    fake.reconcile(2_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Hub));
    let hub_stopped = |fake: &Fake| {
        fake.log()
            .iter()
            .any(|e| e.starts_with("write Deployment/p "))
    };
    // The hub step wrote its count at once; its pod still runs.
    assert!(hub_stopped(&fake));
    fake.state().log.clear();
    fake.state().dgd_pods.insert("Frontend".into(), 1);
    fake.reconcile(3_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    assert!(!released(&fake));
}

/// A stop that resumes at its release, with a segment's pod running in
/// the DGD again, starts its segments over rather than let the pool go.
#[tokio::test]
async fn a_release_waits_for_a_dgd_segment_pod() {
    let fake = running_dgd(false).await;
    fake.state().dgd_pods.insert("Frontend".into(), 0);
    fake.delete(false);
    fake.reconcile(1_000).await.expect("a reconcile");
    fake.settle();
    fake.reconcile(2_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Hub));
    fake.settle();
    // The operator ends right after it stores the release.
    let writes = fake.state().status_writes;
    fake.state().crash_after = Some(writes + 1);
    assert!(fake.reconcile(3_000).await.is_err(), "the crash");
    assert_eq!(step(&fake), Some(StopStep::Release));
    {
        let mut state = fake.state();
        state.crashed = false;
        state.crash_after = None;
        state.dgd_pods.insert("Frontend".into(), 1);
    }
    fake.reconcile(4_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    assert!(!released(&fake), "{:?}", fake.log());
}

/// A held generation cannot hide the segment pods that still use the hub.
#[tokio::test]
async fn a_held_generation_cannot_hide_a_running_dgd_segment() {
    let fake = running_dgd(false).await;
    let dgd_uid = fake.uid(Kind::DynamoGraphDeployment, "p");
    {
        let mut state = fake.state();
        let pool = state.pool.as_mut().expect("a pool");
        pool.metadata.generation = Some(2);
        pool.spec.pool.hub.image = "registry.example/hub:next".into();
        pool.spec.pool.segments[0].component = None;
        pool.spec.pool.dgd = None;
        state.version += 1;
    }
    fake.reconcile(1_000).await.expect("the generation is held");
    assert_eq!(fake.status().held, Some(2));
    assert_eq!(fake.uid(Kind::DynamoGraphDeployment, "p"), dgd_uid);
    assert!(
        !fake.log().iter().any(|entry| {
            entry.starts_with("apply ")
                || entry.starts_with("create ")
                || entry.starts_with("delete ")
        }),
        "{:?}",
        fake.log()
    );

    fake.delete(false);
    fake.reconcile(2_000).await.expect("stop the gdl segment");
    fake.settle();
    fake.reconcile(3_000)
        .await
        .expect("wait for the DGD segment");
    fake.settle();
    fake.reconcile(4_000).await.expect("keep waiting");

    assert_eq!(fake.state().dgd_pods["Frontend"], 2);
    let log = fake.log();
    assert!(
        !released(&fake)
            && !log
                .iter()
                .any(|entry| entry.starts_with("write Deployment/p ")),
        "the live DGD segment still needs the hub: {log:?}"
    );
    assert_eq!(step(&fake), Some(StopStep::Segments));
}
