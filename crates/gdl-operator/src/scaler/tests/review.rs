use super::*;

async fn cancel_after_label_write(lose_status: bool) -> Fake {
    let fake = Fake::with(&[9, 0, 8]);
    let mut wanted = spec(Some(2));
    wanted.service = Some(SERVICE.into());
    once(&fake, &wanted, 0).await;
    assert!(!fake.pod("pod-2").routable);
    assert_eq!(status(&fake).retiring[0].uid, "uid-2");
    if lose_status {
        // A crash after patching the label leaves the previous status stored.
        fake.state().status = None;
    }
    {
        let mut state = fake.state();
        state.pods[0].load = 0;
        state.pods[1].load = 20;
    }
    once(&fake, &wanted, 1_000).await;
    wanted.replicas = Some(3);
    once(&fake, &wanted, 2_000).await;
    assert!(status(&fake).retiring.is_empty());
    assert_eq!(fake.state().replicas, 3);
    fake
}

#[tokio::test]
async fn cancellation_restores_a_victim_whose_status_was_stored() {
    let fake = cancel_after_label_write(false).await;
    assert!(fake.state().pods.iter().all(|pod| pod.routable));
}

#[tokio::test]
#[ignore = "F1: a lost status write leaves a live frontend permanently unroutable"]
async fn cancellation_restores_a_victim_after_a_lost_status_write() {
    let fake = cancel_after_label_write(true).await;
    assert!(
        fake.pod("pod-2").routable,
        "the scale-down was cancelled, but the first victim never rejoins its Service"
    );
}

#[tokio::test]
#[ignore = "F2: after a victim disappears, a stale Deployment count unroutes the only ready frontend"]
async fn a_victim_that_dies_does_not_cost_the_last_ready_frontend() {
    let fake = Fake::with(&[5, 0]);
    let mut wanted = spec(Some(1));
    wanted.service = Some(SERVICE.into());
    once(&fake, &wanted, 0).await;
    assert_eq!(status(&fake).retiring[0].uid, "uid-2");
    {
        // Pod deletion is visible before the Deployment controller updates status.
        let mut state = fake.state();
        state.pods.retain(|pod| pod.view.uid != "uid-2");
        state.listed.remove("uid-2");
        assert_eq!(state.replicas, 2);
        assert!(!state.rolling);
    }
    once(&fake, &wanted, 1_000).await;
    assert_eq!(fake.state().pods.len(), 1);
    assert!(fake.pod("pod-1").view.ready);
    assert!(
        fake.pod("pod-1").routable,
        "the remaining ready frontend already satisfies the desired count"
    );
}

struct PausedProbe<'a> {
    fake: &'a Fake,
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl Probe for PausedProbe<'_> {
    async fn instances(&self, pool: &str) -> Result<Instances, String> {
        self.fake.instances(pool).await
    }

    async fn metrics(&self, ip: &str, port: u16) -> Result<String, String> {
        if ip == "10.0.0.1" {
            self.arrived.notify_one();
            self.resume.notified().await;
        }
        self.fake.metrics(ip, port).await
    }
}

#[tokio::test]
#[ignore = "F3: a second operator's stale reconcile overwrites a newer HPA scale"]
async fn concurrent_operators_preserve_a_newer_scale() {
    let fake = Fake::with(&[5, 5, 5, 0]);
    let probe = PausedProbe {
        fake: &fake,
        arrived: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    };
    let victims = Victims::default();
    let cx = Context {
        cluster: &fake,
        probe: &probe,
        victims: &victims,
    };
    let stale = GdlScaler::new("global-front", spec(Some(3)));
    tokio::time::timeout(Duration::from_secs(3), async {
        let (result, ()) = tokio::join!(reconcile(&cx, &stale, 0), async {
            probe.arrived.notified().await;
            // The HPA changes the scaler while this operator awaits a scrape.
            once(&fake, &spec(Some(5)), 1_000).await;
            fake.settle(false);
            assert_eq!(fake.state().replicas, 5);
            assert_eq!(fake.state().pods.len(), 5);
            probe.resume.notify_one();
        });
        result.expect("both operators reconcile successfully");
    })
    .await
    .expect("the interleaving completes");
    assert_eq!(
        fake.state().replicas,
        5,
        "the paused operator must not undo the scale that the other operator applied"
    );
}

#[tokio::test]
#[ignore = "F5: a restart after scaling restores routing before the ReplicaSet deletes the victim"]
async fn a_committed_removal_keeps_the_victim_out_of_its_service() {
    let fake = Fake::with(&[9, 0, 9]);
    let mut wanted = spec(Some(2));
    wanted.service = Some(SERVICE.into());
    wanted.routing_grace = Some("0s".into());
    once(&fake, &wanted, 0).await;
    fake.unlist();
    once(&fake, &wanted, 1_000).await;
    let persisted = status(&fake);
    assert_eq!(persisted.retiring[0].phase, RetiringPhase::Unrouting);
    assert!(persisted.retiring[0].unrouted.is_some());
    once(&fake, &wanted, 2_000).await;
    assert_eq!(fake.state().replicas, 2);
    assert_eq!(fake.pod("pod-2").cost, Some(VICTIM_COST));
    // The operator restarts before storing Deleting; the ReplicaSet still lags.
    fake.state().status = Some(persisted);
    once(&fake, &wanted, 3_000).await;
    assert!(!fake.pod("pod-2").view.terminating);
    fake.settle(false);
    assert!(fake.pod("pod-2").view.terminating);
    assert!(
        !fake.pod("pod-2").routable,
        "the committed victim is deleted after the restarted operator put it back in its Service"
    );
}
