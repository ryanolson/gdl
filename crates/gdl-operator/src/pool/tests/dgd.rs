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
    // The frontends were made from the template that runs the segment.
    state.marked.insert("Frontend".into());
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

/// A DGD rollout can leave pods from the previous segment component using the hub.
#[tokio::test]
async fn a_component_move_keeps_running_segment_pods_in_the_stop() {
    let fake = running_dgd(false).await;
    {
        let mut state = fake.state();
        let pool = state.pool.as_mut().expect("a pool");
        pool.metadata.generation = Some(2);
        pool.spec.pool.segments[0].component = Some("Worker".into());
        let mut dgd: Value = serde_json::from_str(pool.spec.pool.dgd.as_deref().expect("the DGD"))
            .expect("DGD JSON");
        dgd["spec"]["components"][0]["podTemplate"] = serde_json::json!({
            "spec": {"containers": [{"name": "main", "image": "registry.example/dynamo:tag"}]},
        });
        dgd["spec"]["components"][1]["podTemplate"] = serde_json::json!({
            "spec": {"containers": [{"name": "main", "image": "registry.example/engine:tag"}]},
        });
        pool.spec.pool.dgd = Some(dgd.to_string());
        state.version += 1;
    }
    fake.reconcile(1_000)
        .await
        .expect("apply the component move");
    assert_eq!(fake.status().held, None);
    assert!(
        fake.log()
            .contains(&"apply DynamoGraphDeployment/p".to_string())
    );
    assert_eq!(
        fake.state().objects[&(Kind::DynamoGraphDeployment, "p".into())]
            .live
            .components,
        ["Worker"],
        "the DGD now marks only the destination component"
    );
    // Dynamo has not retired the previous template's Frontend pods yet.
    // Scaling the destination to zero does not end those source pods.
    fake.state().dgd_pods.insert("Worker".into(), 0);
    fake.state().log.clear();
    fake.delete(false);
    fake.reconcile(2_000).await.expect("stop the gdl segment");
    fake.settle();
    fake.reconcile(3_000)
        .await
        .expect("wait for the source pods");
    fake.settle();
    fake.reconcile(4_000).await.expect("keep waiting");

    assert_eq!(fake.state().dgd_pods["Frontend"], 2);
    let log = fake.log();
    assert!(
        !released(&fake)
            && !log
                .iter()
                .any(|entry| entry.starts_with("write Deployment/p ")),
        "the previous template's segment pods still need the hub: {log:?}"
    );
    assert_eq!(step(&fake), Some(StopStep::Segments));
}

/// A malformed template must set BadSpec instead of panicking the controller.
#[tokio::test]
async fn a_malformed_dgd_template_is_refused_without_panicking() {
    let mut pool = dgd_pool(false);
    let mut dgd: Value =
        serde_json::from_str(pool.spec.pool.dgd.as_deref().expect("the DGD")).expect("DGD JSON");
    dgd["spec"]["components"][0]["podTemplate"] = serde_json::json!([]);
    pool.spec.pool.dgd = Some(dgd.to_string());
    let mut fake = Fake::with(pool);
    fake.dgds = true;

    fake.reconcile(0)
        .await
        .expect("refuse the malformed template");

    assert!(
        fake.status()
            .conditions
            .iter()
            .any(|condition| { condition.type_ == "Applied" && condition.reason == "BadSpec" })
    );
    let log = fake.log();
    assert!(
        !log.iter()
            .any(|entry| entry.starts_with("create ") || entry.starts_with("apply ")),
        "a malformed DGD must apply nothing: {log:?}"
    );
}

/// A segment's pod that does not carry `gdl.dev/config`, as when dynamo
/// does not copy a template's annotations to its pods, is still the
/// segment's by its component in the DGD as applied: the hub waits for it.
#[tokio::test]
async fn a_segment_pod_found_by_its_component_alone_holds_the_hub() {
    let fake = running_dgd(false).await;
    fake.state().marked.clear();
    fake.delete(false);
    fake.reconcile(1_000).await.expect("a reconcile");
    fake.settle();
    fake.reconcile(2_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    assert!(!released(&fake));
    let log = fake.log();
    assert!(
        !log.iter().any(|e| e.starts_with("write Deployment/p ")),
        "{log:?}"
    );
}

/// Background deletion can remove a pruned DGD before its segment pods finish draining.
#[tokio::test]
async fn a_pruned_dgd_keeps_its_running_segment_pods_in_the_stop() {
    let fake = running_dgd(false).await;
    {
        let mut state = fake.state();
        let pool = state.pool.as_mut().expect("a pool");
        pool.metadata.generation = Some(2);
        pool.spec.pool.segments[0].component = None;
        pool.spec.pool.dgd = None;
        state.version += 1;
    }
    fake.reconcile(1_000)
        .await
        .expect("move the segment to a Deployment");
    assert_eq!(fake.status().held, None);
    assert!(
        !fake
            .state()
            .objects
            .contains_key(&(Kind::DynamoGraphDeployment, "p".into())),
        "the DGD is pruned in the background"
    );
    assert!(
        fake.log()
            .iter()
            .any(|entry| entry.starts_with("delete DynamoGraphDeployment/p "))
    );
    fake.settle();
    fake.state().log.clear();
    fake.delete(false);
    fake.reconcile(2_000).await.expect("stop the Deployments");
    fake.settle();
    fake.reconcile(3_000)
        .await
        .expect("wait for the retiring DGD segment");
    fake.settle();
    fake.reconcile(4_000).await.expect("keep waiting");

    assert_eq!(fake.state().dgd_pods["Frontend"], 2);
    let log = fake.log();
    assert!(
        !released(&fake)
            && !log
                .iter()
                .any(|entry| entry.starts_with("write Deployment/p ")),
        "the deleted DGD's segment pods still need the hub: {log:?}"
    );
    assert_eq!(step(&fake), Some(StopStep::Segments));
}
