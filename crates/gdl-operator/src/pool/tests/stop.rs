//! A pool's stop: its order, its restart points, and the objects it acts
//! on.

use super::*;

/// Index of the first log entry that starts with `prefix`.
fn first(log: &[String], prefix: &str) -> Option<usize> {
    log.iter().position(|entry| entry.starts_with(prefix))
}

/// A top pool's segments stop first, while its hub runs; its hub only once
/// no segment pod runs; and its finalizer goes only once no pod runs.
#[tokio::test]
async fn a_top_pool_stops_its_segments_then_its_hub_then_lets_go() {
    let fake = running(false).await;
    fake.delete(false);
    fake.reconcile(1_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    let log = fake.log();
    assert!(
        first(&log, "drain").is_none(),
        "a top pool is not drained: {log:?}"
    );
    assert!(first(&log, "write GdlScaler/p-engine").is_some(), "{log:?}");
    assert!(first(&log, "write GdlScaler/p-front").is_some(), "{log:?}");
    assert!(first(&log, "write Deployment/p ").is_none(), "{log:?}");
    // The segments' pods still run: the hub stays.
    fake.reconcile(2_000).await.expect("a reconcile");
    assert!(first(&fake.log(), "write Deployment/p ").is_none());

    fake.settle();
    fake.reconcile(3_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Hub));
    assert!(first(&fake.log(), "write Deployment/p ").is_some());
    assert!(!released(&fake), "the hub's pod still runs");

    fake.settle();
    fake.reconcile(4_000).await.expect("a reconcile");
    assert!(released(&fake), "{:?}", fake.log());
}

/// A pool with a parent is drained first, and its segments are stopped
/// only once its hub says its record is withdrawn.
#[tokio::test]
async fn a_pool_with_a_parent_drains_and_withdraws_first() {
    let fake = running(true).await;
    fake.state().withdraws = false;
    fake.delete(false);
    fake.reconcile(1_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Drain));
    let log = fake.log();
    let drain = first(&log, "drain").expect("a drain");
    assert!(
        first(&log, "status Drain").expect("the step") < drain,
        "{log:?}"
    );
    assert!(first(&log, "write ").is_none(), "{log:?}");

    fake.state().withdraws = true;
    fake.reconcile(2_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    assert!(fake.status().withdrawn);
    assert_eq!(
        condition(&fake, "Withdrawn"),
        Some(("True".into(), "Withdrawn".into()))
    );
    fake.settle();
    fake.reconcile(3_000).await.expect("a reconcile");
    fake.settle();
    fake.reconcile(4_000).await.expect("a reconcile");
    assert!(released(&fake), "{:?}", fake.log());
}

/// A withdrawal the hub has not said within the bound keeps the
/// finalizer, with a condition and one event: the parent would still hold
/// the pool's record. The stop goes on once the hub says it.
#[tokio::test]
async fn a_withdrawal_not_said_in_time_keeps_the_finalizer() {
    let fake = running(true).await;
    fake.state().withdraws = false;
    fake.delete(false);
    fake.reconcile(0).await.expect("a reconcile");
    // The bound: the segments' grace, 10 s and 30 s, and 60 s.
    fake.reconcile(99_000).await.expect("a reconcile");
    assert_eq!(condition(&fake, "Withdrawn"), None);
    fake.reconcile(101_000).await.expect("a reconcile");
    fake.reconcile(102_000).await.expect("a reconcile");
    assert_eq!(
        condition(&fake, "Withdrawn"),
        Some(("False".into(), "WithdrawalUnproven".into()))
    );
    assert_eq!(fake.state().events, ["WithdrawalUnproven"]);
    assert_eq!(step(&fake), Some(StopStep::Drain));
    assert!(first(&fake.log(), "write ").is_none());
    assert!(!released(&fake));

    fake.state().withdraws = true;
    fake.reconcile(103_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Segments));
}

/// Run a stop to its end, with the operator's process ending after
/// status write `crash`, and starting again: every write it makes to an
/// object names a uid that the status stored last, and the fake saw no
/// hub scaled while a segment pod ran, and no finalizer removed while a
/// pod ran.
async fn stop_with_a_crash(parent: bool, crash: usize) -> usize {
    let fake = running(parent).await;
    fake.state().crash_after = Some(crash);
    fake.delete(false);
    let mut now = 0;
    let mut crashed = false;
    for _ in 0..40 {
        now += 1_000;
        if fake.reconcile(now).await.is_err() {
            let mut state = fake.state();
            assert!(state.crashed, "a reconcile failed without a crash");
            state.crashed = false;
            state.crash_after = None;
            crashed = true;
        }
        // Each reconcile reads what the last one stored, as after a restart.
        if released(&fake) {
            break;
        }
        // Pods exit only between reconciles, so a step that waits is seen
        // waiting.
        if now % 2_000 == 0 {
            fake.settle();
        }
    }
    assert!(released(&fake), "crash {crash}: {:?}", fake.log());
    assert!(
        crashed || crash == usize::MAX,
        "crash {crash} never came: {:?}",
        fake.log()
    );
    let log = fake.log();
    assert!(
        log.iter().all(|e| !e.starts_with("VIOLATION")),
        "crash {crash}: {log:?}"
    );
    let mut stored: Vec<String> = Vec::new();
    for entry in &log {
        if let Some(rest) = entry.strip_prefix("status ") {
            stored = rest
                .split(['[', ']', ' '])
                .map(str::to_string)
                .filter(|s| s.starts_with("uid-"))
                .collect();
        } else if entry.starts_with("write ") {
            let uid = entry.split(' ').nth(2).expect("a uid");
            assert!(
                stored.iter().any(|s| s == uid),
                "crash {crash}: `{entry}` acts on an object the stored step does not name: {log:?}"
            );
        }
    }
    log.iter().filter(|e| e.starts_with("status ")).count()
}

/// A crash after each status write of a stop resumes it: the step stored
/// is taken, in order, and nothing is acted on that it does not name.
#[tokio::test]
async fn a_crash_after_each_status_write_resumes_the_stop() {
    for parent in [false, true] {
        let writes = stop_with_a_crash(parent, usize::MAX).await;
        assert!(writes >= 3, "{writes}");
        for crash in 1..=writes {
            stop_with_a_crash(parent, crash).await;
        }
    }
}

/// A scaler made again under the same name, after the step that names
/// its old uid was stored, is never acted on by that step: the new uid is
/// stored first.
#[tokio::test]
async fn a_scaler_made_again_is_written_down_before_it_is_suspended() {
    let fake = running(false).await;
    fake.state().crash_after = Some(1);
    fake.delete(false);
    assert!(fake.reconcile(1_000).await.is_err(), "the crash");
    {
        let mut state = fake.state();
        state.crashed = false;
        state.crash_after = None;
    }
    let old = fake.uid(Kind::GdlScaler, "p-engine");
    let stored = fake.status().stop.expect("a step").objects;
    assert!(stored.iter().any(|o| o.uid == old));
    let new = fake.recreate(Kind::GdlScaler, "p-engine");
    fake.state().log.clear();
    fake.reconcile(2_000).await.expect("a reconcile");
    let log = fake.log();
    let named = first(&log, "status Segments").expect("the step stored again");
    assert!(log[named].contains(&new), "{log:?}");
    let suspended = first(&log, "write GdlScaler/p-engine").expect("the suspend");
    assert!(named < suspended, "{log:?}");
    assert!(log[suspended].contains(&new), "{log:?}");
    assert!(log.iter().all(|e| !e.contains(&old)), "{log:?}");
}

/// The hub's Deployment made again under the same name, after the hub
/// step was stored, is written down before it is scaled.
#[tokio::test]
async fn a_hub_made_again_is_written_down_before_it_is_scaled() {
    let fake = running(false).await;
    fake.delete(false);
    fake.reconcile(1_000).await.expect("a reconcile");
    fake.settle();
    let writes = fake.state().status_writes;
    fake.state().crash_after = Some(writes + 1);
    assert!(fake.reconcile(2_000).await.is_err(), "the crash");
    assert_eq!(step(&fake), Some(StopStep::Hub));
    {
        let mut state = fake.state();
        state.crashed = false;
        state.crash_after = None;
        state.log.clear();
    }
    let new = fake.recreate(Kind::Deployment, "p");
    fake.reconcile(3_000).await.expect("a reconcile");
    let log = fake.log();
    let named = first(&log, "status Hub").expect("the step stored again");
    assert!(log[named].contains(&new), "{log:?}");
    let scaled = first(&log, "write Deployment/p ").expect("the scale");
    assert!(named < scaled && log[scaled].contains(&new), "{log:?}");
}

/// A pool whose hub's Deployment is gone cannot drain: it says so, and
/// stops as a top pool does.
#[tokio::test]
async fn a_pool_whose_hub_is_gone_stops_as_a_top_pool() {
    let fake = running(true).await;
    fake.state()
        .objects
        .remove(&(Kind::Deployment, "p".to_string()));
    fake.delete(false);
    fake.reconcile(1_000).await.expect("a reconcile");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    assert_eq!(
        condition(&fake, "HubGone"),
        Some(("True".into(), "NoHubDeployment".into()))
    );
    assert!(first(&fake.log(), "drain").is_none());
    assert_eq!(fake.state().events, ["NoHubDeployment"]);
}

/// A pool deleted with foreground propagation is named, with an event,
/// and its finalizer still goes once no pod of it runs, leaving
/// Kubernetes' own.
#[tokio::test]
async fn a_foreground_delete_is_named_and_still_lets_go() {
    let fake = running(false).await;
    fake.delete(true);
    for now in 1..=6 {
        fake.reconcile(now * 1_000).await.expect("a reconcile");
        fake.settle();
    }
    assert_eq!(
        condition(&fake, "ForegroundDelete"),
        Some(("True".into(), "ForegroundPropagation".into()))
    );
    assert_eq!(fake.state().events, ["ForegroundPropagation"]);
    let finalizers = fake.pool().and_then(|p| p.metadata.finalizers);
    assert_eq!(finalizers, Some(vec!["foregroundDeletion".to_string()]));
}

/// A drain the hub refuses keeps the finalizer, with a condition: the
/// pool's record would stay at its parent.
#[tokio::test]
async fn a_refused_drain_keeps_the_finalizer() {
    let fake = running(true).await;
    fake.state().refuses_drain = Some("a pool whose class no hop enters cannot be drained".into());
    fake.delete(false);
    fake.reconcile(1_000).await.expect("a reconcile");
    assert_eq!(
        condition(&fake, "Withdrawn"),
        Some(("False".into(), "DrainRefused".into()))
    );
    assert_eq!(step(&fake), Some(StopStep::Drain));
    assert!(!released(&fake));
}

#[tokio::test]
async fn a_resumed_hub_step_stops_recreated_segments_before_scaling_the_hub() {
    let fake = running(false).await;
    fake.delete(false);
    fake.reconcile(1_000).await.expect("suspend segments");
    fake.settle();
    let writes = fake.state().status_writes;
    fake.state().crash_after = Some(writes + 1);
    assert!(
        fake.reconcile(2_000).await.is_err(),
        "crash after storing the hub step"
    );
    assert_eq!(step(&fake), Some(StopStep::Hub));
    {
        let mut state = fake.state();
        state.crashed = false;
        state.crash_after = None;
        state.log.clear();
    }
    fake.recreate(Kind::Deployment, "p-engine");
    fake.recreate(Kind::GdlScaler, "p-engine");
    fake.settle();
    assert_eq!(fake.state().pods.get("engine"), Some(&2));

    fake.reconcile(3_000)
        .await
        .expect("resume persisted hub step");
    let replicas = fake.state().objects[&(Kind::Deployment, "p".into())]
        .live
        .replicas;
    assert_eq!(
        replicas,
        Some(1),
        "the recreated engine has running pods, so the stop must revisit segments first: {:?}",
        fake.log(),
    );
}

#[tokio::test]
#[ignore = "F8: a hub restarted after withdrawal is stopped without withdrawing its new record"]
async fn a_hub_restart_during_the_segments_step_requires_fresh_withdrawal() {
    let fake = running(true).await;
    fake.delete(false);
    fake.reconcile(1_000)
        .await
        .expect("drain and suspend segments");
    assert_eq!(step(&fake), Some(StopStep::Segments));
    assert!(fake.status().withdrawn);
    fake.settle();

    // A container restart keeps the Deployment uid but starts a new
    // publisher, which writes a record again and has not withdrawn it.
    let hub_uid = fake.uid(Kind::Deployment, "p");
    {
        let mut state = fake.state();
        state.drained = false;
        state.withdraws = false;
        state.log.clear();
    }
    assert!(!Hubs::withdrawn(&fake, "p").await.expect("new hub"));
    assert_eq!(fake.uid(Kind::Deployment, "p"), hub_uid);
    fake.reconcile(2_000)
        .await
        .expect("resume with a new publisher");
    fake.settle();
    fake.reconcile(3_000).await.expect("resume the stop again");

    assert!(
        !released(&fake),
        "the current hub's record is not withdrawn, so the finalizer must stay: {:?}",
        fake.log(),
    );
    assert_eq!(
        fake.state().objects[&(Kind::Deployment, "p".into())]
            .live
            .replicas,
        Some(1),
        "keep the current hub until it proves its new record withdrawn",
    );
}
