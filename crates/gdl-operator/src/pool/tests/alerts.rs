use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

struct Errors<'a> {
    fake: &'a Fake,
    calls: AtomicUsize,
}

impl Hubs for Errors<'_> {
    async fn health(&self, pool: &str) -> Result<Health, String> {
        Hubs::health(self.fake, pool).await
    }

    async fn withdrawn(&self, _: &str) -> Result<bool, String> {
        let status = match self.calls.fetch_add(1, Ordering::SeqCst) % 2 {
            0 => "503 Service Unavailable",
            _ => "500 Internal Server Error",
        };
        Err(format!("GET the hub's withdrawal route: {status}"))
    }

    async fn drain(&self, pool: &str) -> Result<Drain, String> {
        Hubs::drain(self.fake, pool).await
    }

    async fn presence(&self, pool: &str, presence: gdl_k8s::Presence) -> Result<(), String> {
        Hubs::presence(self.fake, pool, presence).await
    }
}

#[tokio::test]
#[ignore = "F15: changing probe errors repeat the withdrawal warning event"]
async fn changing_probe_errors_emit_one_withdrawal_warning() {
    let fake = running(true).await;
    fake.delete(false);
    fake.reconcile(0).await.expect("drain and suspend segments");
    fake.settle();
    fake.state().events.clear();
    fake.state().log.clear();
    let since = fake.status().stop.expect("a stop").since;
    let hubs = Errors {
        fake: &fake,
        calls: AtomicUsize::new(0),
    };
    let cx = Context {
        pools: &fake,
        hubs: &hubs,
        namespace: "ns",
    };
    for now in [1_000, 50_000] {
        assert_eq!(
            reconcile(&cx, &fake.pool().expect("pool"), now).await,
            Ok(Duration::from_secs(1))
        );
        assert_eq!(step(&fake), Some(StopStep::Hub));
    }
    assert!(fake.state().events.is_empty());
    for now in [101_000, 102_000, 103_000] {
        assert_eq!(
            reconcile(&cx, &fake.pool().expect("pool"), now).await,
            Ok(Duration::from_secs(5))
        );
        assert_eq!(step(&fake), Some(StopStep::Hub));
        assert_eq!(fake.status().stop.expect("a stop").since, since);
        assert_eq!(
            condition(&fake, "Withdrawn"),
            Some(("False".into(), "WithdrawalUnproven".into()))
        );
    }
    assert_eq!(
        fake.state().objects[&(Kind::Deployment, "p".into())]
            .live
            .replicas,
        Some(1)
    );
    assert!(!released(&fake));
    assert!(fake.log().iter().all(|e| !e.starts_with("status Drain")));
    assert_eq!(fake.state().events, ["WithdrawalUnproven"]);
}
