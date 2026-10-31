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
