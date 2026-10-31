use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

struct Probes<'a> {
    fake: &'a Fake,
    calls: AtomicUsize,
    intermittent_errors: bool,
}

impl Hubs for Probes<'_> {
    async fn health(&self, pool: &str) -> Result<Health, String> {
        Hubs::health(self.fake, pool).await
    }

    async fn withdrawn(&self, pool: &str) -> Result<bool, String> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        // Model probe latency so the paused clock can bound an in-flight reconcile.
        tokio::time::sleep(Duration::from_secs(1)).await;
        if self.intermittent_errors && call.is_multiple_of(2) {
            Err("the withdrawal probe's connection was reset".into())
        } else {
            Hubs::withdrawn(self.fake, pool).await
        }
    }

    async fn drain(&self, pool: &str) -> Result<Drain, String> {
        Hubs::drain(self.fake, pool).await
    }

    async fn presence(&self, pool: &str, presence: gdl_k8s::Presence) -> Result<(), String> {
        Hubs::presence(self.fake, pool, presence).await
    }
}

async fn recheck(intermittent_errors: bool) -> (bool, usize, Fake) {
    let fake = running(true).await;
    fake.delete(false);
    fake.reconcile(1_000)
        .await
        .expect("drain and suspend segments");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    assert!(fake.status().withdrawn);
    fake.settle();
    assert_eq!(fake.state().pods["engine"], 0);
    assert_eq!(fake.state().pods["front"], 0);
    let hubs = Probes {
        fake: &fake,
        calls: AtomicUsize::new(0),
        intermittent_errors,
    };
    let cx = Context {
        pools: &fake,
        hubs: &hubs,
        namespace: "ns",
    };
    let result = tokio::time::timeout(
        Duration::from_secs(300),
        reconcile(&cx, &fake.pool().expect("pool"), 2_000),
    )
    .await;
    let returned = match result {
        Ok(result) => {
            result.expect("reconcile");
            true
        }
        Err(_) => false,
    };
    let calls = hubs.calls.load(Ordering::SeqCst);
    (returned, calls, fake)
}

#[tokio::test(start_paused = true)]
async fn a_successful_withdrawal_recheck_reaches_the_hub_stop() {
    let (returned, calls, fake) = recheck(false).await;
    assert!(returned);
    assert_eq!(calls, 1);
    assert_eq!(
        fake.state().objects[&(Kind::Deployment, "p".into())]
            .live
            .replicas,
        Some(0)
    );
    assert!(!released(&fake), "the hub pod still runs");
}

#[tokio::test(start_paused = true)]
async fn intermittent_withdrawal_probe_errors_yield_instead_of_restarting_drain_forever() {
    let (returned, calls, fake) = recheck(true).await;
    assert!(!released(&fake));
    assert_eq!(
        fake.state().objects[&(Kind::Deployment, "p".into())]
            .live
            .replicas,
        Some(1)
    );
    assert!(
        returned,
        "one reconcile did not return in 300 simulated seconds, making {calls} withdrawal probes while cycling through drain and hub"
    );
}

/// A hub whose withdrawal probe keeps failing at the hub step keeps the
/// step: it never sends the stop back to drain, and once the drain's bound
/// from the stop's start has run out, the finalizer stays, with the
/// `Withdrawn` condition and one event.
#[tokio::test]
async fn a_hub_whose_probe_keeps_failing_fails_closed_at_the_bound() {
    struct Failing<'a>(&'a Fake);

    impl Hubs for Failing<'_> {
        async fn health(&self, pool: &str) -> Result<Health, String> {
            Hubs::health(self.0, pool).await
        }

        async fn withdrawn(&self, _: &str) -> Result<bool, String> {
            Err("the withdrawal probe's connection was reset".into())
        }

        async fn drain(&self, pool: &str) -> Result<Drain, String> {
            Hubs::drain(self.0, pool).await
        }

        async fn presence(&self, pool: &str, presence: gdl_k8s::Presence) -> Result<(), String> {
            Hubs::presence(self.0, pool, presence).await
        }
    }

    let fake = running(true).await;
    fake.delete(false);
    fake.reconcile(0).await.expect("drain and suspend segments");
    fake.settle();
    fake.state().events.clear();
    fake.state().log.clear();
    let hubs = Failing(&fake);
    let cx = Context {
        pools: &fake,
        hubs: &hubs,
        namespace: "ns",
    };
    // The bound: the segments' grace, 10 s and 30 s, and 60 s.
    for now in [1_000, 50_000, 101_000, 102_000] {
        reconcile(&cx, &fake.pool().expect("pool"), now)
            .await
            .expect("a reconcile");
        assert_eq!(step(&fake), Some(StopStep::Hub), "at {now}");
    }
    assert_eq!(
        condition(&fake, "Withdrawn"),
        Some(("False".into(), "WithdrawalUnproven".into()))
    );
    assert_eq!(fake.state().events, ["WithdrawalUnproven"]);
    assert_eq!(
        fake.state().objects[&(Kind::Deployment, "p".into())]
            .live
            .replicas,
        Some(1)
    );
    assert!(!released(&fake));
    assert!(fake.log().iter().all(|e| !e.starts_with("status Drain")));
}
