//! Two operators at once on one scaler and its Deployment: what a
//! StatefulSet of one replica rules out, unless a person force-deletes the
//! operator's pod or removes its node while the pod still runs. The
//! per-object fences alone must keep them from both committing a victim,
//! both lowering the count, undoing a newer scale, or undoing each other's
//! write to a pod.

use super::*;

/// A fake whose pod reads both finish before either reconcile writes.
fn racing(loads: &[u64]) -> Fake {
    Fake {
        barrier: Some(tokio::sync::Barrier::new(2)),
        ..Fake::with(loads)
    }
}

/// The scaler as `fake` holds it, with `spec`: one operator's read.
fn snapshot(fake: &Fake, spec: &GdlScalerSpec) -> GdlScaler {
    let mut scaler = GdlScaler::new("global-front", spec.clone());
    scaler.metadata.generation = Some(1);
    scaler.status = fake.state().status.clone();
    scaler.metadata.resource_version = Some(fake.state().scaler_version.to_string());
    scaler
}

/// Reconcile `first` and `second`, each from its own read, at once.
async fn both(fake: &Fake, first: &GdlScaler, second: &GdlScaler) {
    let victims = Victims::default();
    let cx = Context {
        cluster: fake,
        probe: fake,
        victims: &victims,
    };
    let (a, b) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(reconcile(&cx, first, 0), reconcile(&cx, second, 0))
    })
    .await
    .unwrap_or_else(|_| panic!("the reconciles hang"));
    a.expect("the first reconciles");
    b.expect("the second reconciles");
}

fn costed(fake: &Fake) -> Vec<String> {
    fake.state()
        .pods
        .iter()
        .filter(|p| p.cost.is_some())
        .map(|p| p.view.name.clone())
        .collect()
}

/// From one read, both decide to remove a pod: one commits its victim and
/// lowers the count once; the other's writes are refused.
#[tokio::test]
async fn two_operators_from_one_read_commit_one_victim() {
    for service in [false, true] {
        let fake = racing(&[0, 0, 0]);
        let mut spec = spec(Some(2));
        if service {
            spec.service = Some(SERVICE.into());
        }
        let read = snapshot(&fake, &spec);
        both(&fake, &read, &read).await;
        let status = status(&fake);
        let events = fake.state().events.clone();
        let replicas = fake.state().replicas;
        let unrouted = fake.state().pods.iter().filter(|p| !p.routable).count();
        let costed = costed(&fake).len();
        assert_eq!(status.retiring.len(), 1, "{service}: {events:?}");
        if service {
            assert_eq!((unrouted, replicas, costed), (1, 3, 0), "{events:?}");
        } else {
            assert_eq!((unrouted, replicas, costed), (0, 2, 1), "{events:?}");
        }
    }
}

/// An operator whose read predates another's removal of a frontend from
/// its Service does not put that pod back: its status is stale.
#[tokio::test]
async fn a_stale_operator_leaves_a_victim_out_of_its_service() {
    let fake = Fake::with(&[9, 0, 9]);
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    let stale = snapshot(&fake, &spec);
    once(&fake, &spec, 0).await;
    assert!(!fake.pod("pod-2").routable);
    fake.take_writes();
    let victims = Victims::default();
    let cx = Context {
        cluster: &fake,
        probe: &fake,
        victims: &victims,
    };
    reconcile(&cx, &stale, 1_000).await.expect("reconciles");
    assert!(fake.take_writes().is_empty());
    assert!(!fake.pod("pod-2").routable);
    assert_eq!(status(&fake).retiring[0].uid, "uid-2");
}

/// One operator takes a victim back for a newer scale-up while another,
/// from an older read with the older count, writes its commit again: the
/// victim serves again with no cost, and the count stays. In either order.
#[tokio::test]
async fn a_stale_commit_does_not_undo_a_newer_scale_up() {
    for fresh_first in [true, false] {
        let fake = racing(&[9, 0, 9]);
        let mut older = spec(Some(2));
        older.service = Some(SERVICE.into());
        older.routing_grace = Some("0s".into());
        // A victim stored as committing whose count was not written yet.
        {
            let solo = Fake::with(&[9, 0, 9]);
            once(&solo, &older, 0).await;
            solo.unlist();
            once(&solo, &older, 1_000).await;
            let mut stored = status(&solo);
            stored.retiring[0].phase = RetiringPhase::Committing;
            let mut state = fake.state();
            state.status = Some(stored);
            state.pods[1].routable = false;
        }
        let mut newer = older.clone();
        newer.replicas = Some(3);
        let stale = snapshot(&fake, &older);
        // The HPA's write of the newer count moves the scaler's version.
        fake.state().scaler_version += 1;
        let fresh = snapshot(&fake, &newer);
        match fresh_first {
            true => both(&fake, &fresh, &stale).await,
            false => both(&fake, &stale, &fresh).await,
        }
        let pod = fake.pod("pod-2");
        let events = fake.state().events.clone();
        let replicas = fake.state().replicas;
        assert!(pod.routable, "{fresh_first}: {events:?}");
        assert_eq!(pod.cost, None, "{fresh_first}: {events:?}");
        assert_eq!(replicas, 3, "{fresh_first}: {events:?}");
        assert!(status(&fake).retiring.is_empty());
    }
}

/// Two operators from one read whose status writes change nothing both
/// put an orphaned pod back in its Service: the pod's version lets one
/// write land, and refuses the other, which read it before.
#[tokio::test]
async fn a_stale_pod_write_is_refused_by_the_pods_version() {
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    // The status both would write unchanged: only the pod's version can
    // tell their writes apart.
    let settled = Fake::with(&[0, 0]);
    once(&settled, &spec, 0).await;
    let fake = racing(&[0, 0]);
    {
        let mut state = fake.state();
        state.status = settled.state().status.clone();
        state.pods[1].routable = false;
    }
    let read = snapshot(&fake, &spec);
    both(&fake, &read, &read).await;
    let events = fake.state().events.clone();
    let writes: Vec<&String> = events.iter().filter(|e| e.contains("routable")).collect();
    assert_eq!(writes, ["pod-2 routable true"], "{events:?}");
    assert!(fake.pod("pod-2").routable);
}

struct PausedWrite<'a> {
    fake: &'a Fake,
    write: &'static str,
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl<'a> PausedWrite<'a> {
    fn new(fake: &'a Fake, write: &'static str) -> Self {
        Self {
            fake,
            write,
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        }
    }

    async fn before(&self, write: &str) {
        if self.write == write {
            self.arrived.notify_one();
            tokio::time::timeout(Duration::from_secs(3), self.resume.notified())
                .await
                .expect("the other operator releases the write");
        }
    }
}

impl Cluster for PausedWrite<'_> {
    async fn target(&self, name: &str) -> Result<Option<Target>, String> {
        self.fake.target(name).await
    }

    async fn pods(&self, selector: &str) -> Result<Vec<PodView>, String> {
        self.fake.pods(selector).await
    }

    async fn endpoints(&self, service: &str) -> Result<BTreeSet<String>, String> {
        self.fake.endpoints(service).await
    }

    async fn scale(&self, name: &str, replicas: i32, fence: &Fence) -> Result<String, Refused> {
        self.before("scale").await;
        self.fake.scale(name, replicas, fence).await
    }

    async fn routable(
        &self,
        pod: &PodView,
        version: &str,
        routable: bool,
    ) -> Result<String, Refused> {
        if routable {
            self.before("restore").await;
        }
        self.fake.routable(pod, version, routable).await
    }

    async fn cost(
        &self,
        pod: &PodView,
        version: &str,
        cost: Option<i32>,
    ) -> Result<String, Refused> {
        if cost.is_some() {
            self.before("cost").await;
        }
        self.fake.cost(pod, version, cost).await
    }

    async fn status(
        &self,
        name: &str,
        status: &GdlScalerStatus,
        version: Option<&str>,
    ) -> Result<Option<String>, Refused> {
        self.fake.status(name, status, version).await
    }
}

async fn count_across_cancellation(desired: i32) -> Fake {
    let fake = Fake::with(&[9, 0, 9]);
    let paused = PausedWrite::new(&fake, "scale");
    let victims = Victims::default();
    let cx = Context {
        cluster: &paused,
        probe: &fake,
        victims: &victims,
    };
    let read = snapshot(&fake, &spec(Some(2)));
    tokio::time::timeout(Duration::from_secs(3), async {
        let (result, ()) = tokio::join!(reconcile(&cx, &read, 0), async {
            paused.arrived.notified().await;
            assert_eq!(fake.state().replicas, 3);
            assert_eq!(status(&fake).retiring[0].phase, RetiringPhase::Committing);
            assert_eq!(fake.pod("pod-2").cost, Some(VICTIM_COST));
            let deployment_version = fake.state().version;
            // The HPA changes the scaler after the cost write, before the count write.
            fake.state().scaler_version += 1;
            once(&fake, &spec(Some(desired)), 1_000).await;
            assert!(status(&fake).retiring.is_empty());
            assert_eq!(fake.pod("pod-2").cost, None);
            assert_eq!(fake.state().replicas, desired);
            if desired == 3 {
                assert_eq!(fake.state().version, deployment_version);
            } else {
                assert_ne!(fake.state().version, deployment_version);
            }
            paused.resume.notify_one();
        });
        result.expect("the paused operator reconciles");
    })
    .await
    .expect("the interleaving completes");
    fake
}

#[tokio::test]
async fn a_count_write_is_refused_when_the_other_operator_changes_the_deployment() {
    let fake = count_across_cancellation(4).await;
    assert_eq!(fake.state().replicas, 4);
}

#[tokio::test]
#[ignore = "F1: a count write lands after the other operator cancels its victim for a newer scale"]
async fn a_count_write_after_cancellation_preserves_the_newer_scale() {
    let fake = count_across_cancellation(3).await;
    let state = fake.state();
    assert_eq!(
        state.replicas, 3,
        "a cancelled removal must not lower the newer desired count: {:?}",
        state.events
    );
}

async fn cancel_across_sweep(sweep: bool) -> Fake {
    let fake = Fake::with(&[9, 0, 9]);
    let mut down = spec(Some(2));
    down.service = Some(SERVICE.into());
    down.routing_grace = Some("0s".into());
    once(&fake, &down, 0).await;
    fake.unlist();
    once(&fake, &down, 1_000).await;
    let victims = Victims::default();

    // Stop after the cost write, as a restart before the count write can.
    {
        let paused = PausedWrite::new(&fake, "scale");
        let cx = Context {
            cluster: &paused,
            probe: &fake,
            victims: &victims,
        };
        let read = snapshot(&fake, &down);
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                result = reconcile(&cx, &read, 2_000) => panic!("the count write did not pause: {result:?}"),
                () = paused.arrived.notified() => {},
            }
        })
        .await
        .expect("the cost write completes");
    }
    assert_eq!(fake.state().replicas, 3);
    assert_eq!(status(&fake).retiring[0].phase, RetiringPhase::Committing);
    assert_eq!(fake.pod("pod-2").cost, Some(VICTIM_COST));

    let mut up = down;
    up.replicas = Some(3);
    fake.state().scaler_version += 1;
    let paused = PausedWrite::new(&fake, "restore");
    let cx = Context {
        cluster: &paused,
        probe: &fake,
        victims: &victims,
    };
    let read = snapshot(&fake, &up);
    tokio::time::timeout(Duration::from_secs(3), async {
        let (result, ()) = tokio::join!(reconcile(&cx, &read, 3_000), async {
            paused.arrived.notified().await;
            assert!(status(&fake).retiring.is_empty());
            assert!(!fake.pod("pod-2").routable);
            if sweep {
                once(&fake, &up, 3_001).await;
                assert!(fake.pod("pod-2").routable);
            }
            paused.resume.notify_one();
        });
        result.expect("the restoring operator reconciles");
    })
    .await
    .expect("the restore interleaving completes");
    assert!(status(&fake).retiring.is_empty());
    assert!(fake.pod("pod-2").routable);
    fake
}

async fn next_removal(fake: &Fake) -> (String, String) {
    let mut down = spec(Some(2));
    down.service = Some(SERVICE.into());
    down.routing_grace = Some("0s".into());
    {
        let mut state = fake.state();
        state.scaler_version += 1;
        state.pods[0].load = 0;
        state.pods[1].load = 99;
        state.pods[2].load = 99;
    }
    once(fake, &down, 4_000).await;
    fake.unlist();
    once(fake, &down, 5_000).await;
    once(fake, &down, 6_000).await;
    let chosen = status(fake).retiring[0].uid.clone();
    assert_eq!(chosen, "uid-1");
    assert_eq!(fake.state().replicas, 2);
    fake.settle(false);
    let deleted = fake
        .state()
        .pods
        .iter()
        .find(|p| p.view.terminating)
        .expect("the ReplicaSet deletes a pod")
        .view
        .uid
        .clone();
    (chosen, deleted)
}

#[tokio::test]
async fn cancellation_without_a_concurrent_sweep_clears_the_cost() {
    let fake = cancel_across_sweep(false).await;
    assert_eq!(fake.pod("pod-2").cost, None);
    let (chosen, deleted) = next_removal(&fake).await;
    assert_eq!(deleted, chosen);
}

#[tokio::test]
#[ignore = "F2: a concurrent sweep interrupts cost restoration after the victim leaves status"]
async fn a_sweep_during_cancellation_preserves_the_next_victim() {
    let fake = cancel_across_sweep(true).await;
    let retained_cost = fake.pod("pod-2").cost;
    let (chosen, deleted) = next_removal(&fake).await;
    assert_eq!(
        deleted,
        chosen,
        "the cancelled victim retains cost {retained_cost:?}: {:?}",
        fake.state().events
    );
}

async fn cost_across_cancellation(initial: Option<i32>) -> Fake {
    let fake = Fake::with(&[9, 0, 9]);
    fake.state().pods[1].cost = initial;
    let paused = PausedWrite::new(&fake, "cost");
    let victims = Victims::default();
    let cx = Context {
        cluster: &paused,
        probe: &fake,
        victims: &victims,
    };
    let read = snapshot(&fake, &spec(Some(2)));
    tokio::time::timeout(Duration::from_secs(3), async {
        let (result, ()) = tokio::join!(reconcile(&cx, &read, 0), async {
            paused.arrived.notified().await;
            assert_eq!(status(&fake).retiring[0].phase, RetiringPhase::Committing);
            assert_eq!(fake.pod("pod-2").cost, initial);
            fake.state().scaler_version += 1;
            once(&fake, &spec(Some(4)), 1_000).await;
            assert_eq!(fake.state().replicas, 4);
            assert!(status(&fake).retiring.is_empty());
            assert_eq!(fake.pod("pod-2").cost, None);
            paused.resume.notify_one();
        });
        result.expect("the stale operator reconciles");
    })
    .await
    .expect("the cost interleaving completes");
    fake
}

#[tokio::test]
async fn a_changed_cost_refuses_the_other_operators_stale_write() {
    let fake = cost_across_cancellation(Some(37)).await;
    assert_eq!(fake.state().replicas, 4);
    assert_eq!(fake.pod("pod-2").cost, None);
}

#[tokio::test]
#[ignore = "F3: an unchanged cost restore preserves the pod version and admits a delayed victim cost"]
async fn an_unchanged_cost_restore_refuses_the_other_operators_stale_write() {
    let fake = cost_across_cancellation(None).await;
    assert_eq!(fake.state().replicas, 4);
    assert!(status(&fake).retiring.is_empty());
    assert_eq!(
        fake.pod("pod-2").cost,
        None,
        "the delayed write must not undo a completed cost restore: {:?}",
        fake.state().events
    );
}

#[tokio::test]
async fn an_unchanged_pod_write_preserves_its_version_and_a_stale_one_is_refused() {
    let fake = Fake::with(&[0]);
    let pod = fake.pod("pod-1").view;
    let version = fake
        .routable(&pod, &pod.resource_version, true)
        .await
        .expect("current write succeeds");
    assert_eq!(version, pod.resource_version);
    let version = fake
        .cost(&pod, &version, None)
        .await
        .expect("current write succeeds");
    assert_eq!(version, pod.resource_version);
    let changed = fake
        .routable(&pod, &version, false)
        .await
        .expect("current write succeeds");
    assert_ne!(changed, version);
    assert!(matches!(
        fake.routable(&pod, &version, false).await,
        Err(Refused::Stale(_))
    ));
}

#[tokio::test]
async fn an_unchanged_status_preserves_its_version_and_a_stale_one_is_refused() {
    let fake = Fake::with(&[0]);
    let desired = GdlScalerStatus::default();
    let initial = fake.state().scaler_version.to_string();
    let version = fake
        .status("global-front", &desired, Some(&initial))
        .await
        .expect("current write succeeds");
    let repeated = fake
        .status("global-front", &desired, version.as_deref())
        .await
        .expect("current write succeeds");
    assert_eq!(repeated, version);
    fake.state().scaler_version += 1;
    assert!(matches!(
        fake.status("global-front", &desired, version.as_deref())
            .await,
        Err(Refused::Stale(_))
    ));
}
