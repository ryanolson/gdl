//! A pool's objects: its finalizer first, their owner, the pool-restart
//! guard, and what it deletes.

use super::*;

/// A new pool gets its finalizer before any object, and each object is
/// the pool's; each segment's Deployment and scaler get `replicas` once,
/// at the quorum, under a manager of their own.
#[tokio::test]
async fn a_new_pool_gets_its_finalizer_before_any_object() {
    let fake = Fake::with(pool(false));
    fake.reconcile(0).await.expect("a reconcile");
    let log = fake.log();
    assert!(log[0].starts_with("finalizers gdl.dev/stop"), "{log:?}");
    let objects = fake.state().objects.clone();
    assert_eq!(objects.len(), 8, "{objects:?}");
    for object in objects.values() {
        assert_eq!(object.live.controller.as_deref(), Some(UID), "{object:?}");
    }
    for (kind, name, manager) in [
        (Kind::Deployment, "p-engine", "gdl-scaler"),
        (Kind::GdlScaler, "p-engine", "gdl-bootstrap"),
        (Kind::Deployment, "p-front", "gdl-scaler"),
        (Kind::GdlScaler, "p-front", "gdl-bootstrap"),
    ] {
        let uid = fake.uid(kind, name);
        let write = format!(
            "write {}/{name} {uid} {{\"replicas\":2}} as {manager}",
            kind.name()
        );
        assert!(log.contains(&write), "{write}: {log:?}");
    }
    // An absent presence is serving: the hub is not asked to say it.
    assert!(log.iter().all(|e| !e.starts_with("presence")), "{log:?}");
    // Once written, it is never written again.
    fake.state().log.clear();
    fake.reconcile(1_000).await.expect("a reconcile");
    assert!(
        fake.log().iter().all(|e| !e.contains("replicas")),
        "{:?}",
        fake.log()
    );
}

/// A change that replaces the hub is held whole without
/// `allowPoolRestart`, with `PoolRestartPending` and an event; a change to
/// a segment's settings is not.
#[tokio::test]
async fn a_hub_change_without_allow_pool_restart_is_held() {
    let fake = running(false).await;
    {
        let mut state = fake.state();
        let pool = state.pool.as_mut().expect("a pool");
        pool.spec.pool.deployment = "{\"graph\": 2}".into();
        pool.metadata.generation = Some(2);
    }
    fake.reconcile(1_000).await.expect("a reconcile");
    let log = fake.log();
    assert!(
        log.iter()
            .all(|e| !e.starts_with("apply ") && !e.starts_with("create ")),
        "{log:?}"
    );
    assert_eq!(fake.status().held, Some(2));
    assert_eq!(fake.status().observed_generation, Some(2));
    assert_eq!(
        condition(&fake, "PoolRestartPending"),
        Some(("True".into(), "HubChange".into()))
    );
    assert_eq!(fake.state().events, ["HubChange"]);
    let message = fake
        .status()
        .conditions
        .iter()
        .find(|c| c.type_ == "PoolRestartPending")
        .map(|c| c.message.clone())
        .expect("the condition");
    assert!(
        message.contains("the pool's deployment changes"),
        "{message}"
    );
    assert!(!message.contains("image"), "{message}");

    fake.state()
        .pool
        .as_mut()
        .expect("a pool")
        .spec
        .pool
        .allow_pool_restart = true;
    fake.reconcile(2_000).await.expect("a reconcile");
    assert!(fake.log().iter().any(|e| e == "apply Deployment/p"));
    assert_eq!(fake.status().held, None);
    assert_eq!(
        condition(&fake, "PoolRestartPending").map(|c| c.0),
        Some("False".into())
    );

    let fake = running(false).await;
    fake.state()
        .pool
        .as_mut()
        .expect("a pool")
        .spec
        .pool
        .segments[0]
        .settings = "[system]\nport = 9090\n[runtime]\ndrain_ms = 20000\n".into();
    fake.reconcile(1_000).await.expect("a reconcile");
    assert_eq!(fake.status().held, None);
    assert!(fake.log().iter().any(|e| e == "apply Deployment/p-engine"));
}

/// An object of a pool's name that the pool did not make is never
/// changed, and nothing of the generation is applied.
#[tokio::test]
async fn an_object_the_pool_did_not_make_is_never_changed() {
    let fake = Fake::with(pool(false));
    let planted = Live {
        kind: Kind::ConfigMap,
        name: "p".into(),
        uid: "theirs".into(),
        version: "1".into(),
        controller: None,
        template: None,
        replica_owners: Vec::new(),
        suspended: false,
        replicas: None,
        image: None,
        config: None,
        deleting: false,
    };
    fake.state().objects.insert(
        (Kind::ConfigMap, "p".into()),
        Obj {
            live: planted,
            labelled: false,
        },
    );
    fake.reconcile(0).await.expect("a reconcile");
    assert_eq!(
        condition(&fake, "NotOwned"),
        Some(("True".into(), "NotThePools".into()))
    );
    let log = fake.log();
    assert!(
        log.iter()
            .all(|e| !e.starts_with("apply ") && !e.starts_with("create ")),
        "{log:?}"
    );
    assert_eq!(fake.state().events, ["NotThePools"]);
    // The refusal speaks of this generation, so a reader of the status
    // need not wait for it.
    assert_eq!(fake.status().observed_generation, Some(1));
}

/// An object that carries the pool's labels and that the pool did not
/// make, such as a plain deploy's object of another cluster's pool of the
/// same name, is neither changed nor pruned.
#[tokio::test]
async fn a_labelled_object_the_pool_did_not_make_is_left_alone() {
    let fake = Fake::with(pool(false));
    let planted = Live {
        kind: Kind::Deployment,
        name: "p-old".into(),
        uid: "theirs".into(),
        version: "1".into(),
        controller: None,
        template: None,
        replica_owners: Vec::new(),
        suspended: false,
        replicas: Some(1),
        image: None,
        config: None,
        deleting: false,
    };
    fake.state().objects.insert(
        (Kind::Deployment, "p-old".into()),
        Obj {
            live: planted,
            labelled: true,
        },
    );
    fake.reconcile(0).await.expect("a reconcile");
    fake.reconcile(1_000).await.expect("a reconcile");
    let log = fake.log();
    assert!(log.iter().all(|e| !e.contains("p-old")), "{log:?}");
    assert!(
        fake.state()
            .objects
            .contains_key(&(Kind::Deployment, "p-old".into()))
    );
    // And a stop leaves it too.
    fake.settle();
    fake.state().pods.remove("old");
    fake.delete(false);
    for now in 2..=8 {
        fake.reconcile(now * 1_000).await.expect("a reconcile");
        fake.settle();
        fake.state().pods.remove("old");
    }
    assert!(released(&fake), "{:?}", fake.log());
    assert!(
        fake.log()
            .iter()
            .all(|e| !e.contains("p-old") && !e.contains("theirs"))
    );
}

/// A segment the pool no longer renders, as after a rename, is deleted by
/// its uid: its Deployment and its scaler.
#[tokio::test]
async fn a_renamed_segment_is_deleted_by_its_uid() {
    let fake = running(false).await;
    let deployment = fake.uid(Kind::Deployment, "p-engine");
    let scaler = fake.uid(Kind::GdlScaler, "p-engine");
    fake.state()
        .pool
        .as_mut()
        .expect("a pool")
        .spec
        .pool
        .segments[0]
        .name = "model".into();
    fake.reconcile(1_000).await.expect("a reconcile");
    let log = fake.log();
    assert!(
        log.contains(&format!("delete Deployment/p-engine {deployment}")),
        "{log:?}"
    );
    assert!(
        log.contains(&format!("delete GdlScaler/p-engine {scaler}")),
        "{log:?}"
    );
    assert!(
        log.iter().any(|e| e == "create Deployment/p-model"),
        "{log:?}"
    );
}

/// A reconcile whose read of the pool is out of date writes nothing but
/// the status it is refused.
#[tokio::test]
async fn a_stale_read_of_the_pool_writes_nothing() {
    let fake = running(false).await;
    let stale = fake.pool().expect("a pool");
    fake.state().version += 1;
    let cx = Context {
        pools: &fake,
        hubs: &fake,
        namespace: "ns",
    };
    reconcile(&cx, &stale, 1_000).await.expect("a reconcile");
    assert!(fake.log().is_empty(), "{:?}", fake.log());
}

/// A pool scaled to 0 says so at its hub first, and only then are its
/// scalers suspended; serving again lets them go.
#[tokio::test]
async fn a_pool_scaled_to_zero_says_so_before_its_scalers_hold() {
    let fake = running(false).await;
    fake.state()
        .pool
        .as_mut()
        .expect("a pool")
        .spec
        .pool
        .presence = gdl_k8s::Presence::ScaledToZero;
    fake.reconcile(1_000).await.expect("a reconcile");
    let log = fake.log();
    let said = first(&log, "presence ScaledToZero").expect("the hub is told");
    let held = first(&log, "write GdlScaler/p-engine").expect("the scaler is suspended");
    assert!(said < held, "{log:?}");
    assert!(log[held].contains("\"suspend\":\"zero\""), "{log:?}");

    fake.state().log.clear();
    fake.state()
        .pool
        .as_mut()
        .expect("a pool")
        .spec
        .pool
        .presence = gdl_k8s::Presence::Serving;
    fake.reconcile(2_000).await.expect("a reconcile");
    let log = fake.log();
    let said = first(&log, "presence Serving").expect("the hub is told");
    let freed = first(&log, "write GdlScaler/p-engine").expect("the scaler is let go");
    assert!(said < freed && log[freed].contains("{}"), "{log:?}");
}

fn first(log: &[String], prefix: &str) -> Option<usize> {
    log.iter().position(|entry| entry.starts_with(prefix))
}

/// A new hub image is named as the reason a generation is held, and a
/// pool's first create is never held.
#[tokio::test]
async fn a_held_generation_names_the_hub_image() {
    let fresh = Fake::with(pool(false));
    fresh.reconcile(0).await.expect("a reconcile");
    assert_eq!(fresh.status().held, None, "a first create is not a restart");
    let fake = running(false).await;
    {
        let mut state = fake.state();
        let pool = state.pool.as_mut().expect("a pool");
        pool.spec.pool.hub.image = "registry.example/hub@sha256:2".into();
        pool.metadata.generation = Some(2);
    }
    fake.reconcile(1_000).await.expect("a reconcile");
    let message = fake
        .status()
        .conditions
        .iter()
        .find(|c| c.type_ == "PoolRestartPending")
        .map(|c| c.message.clone())
        .expect("the condition");
    assert!(
        message.contains(
            "its image changes from `registry.example/hub:tag` to `registry.example/hub@sha256:2`"
        ),
        "{message}"
    );
}

#[tokio::test]
#[ignore = "F4: a restarted hub publishes serving while cached status prevents restoring scaledToZero"]
async fn a_restarted_hub_receives_the_pools_scaled_to_zero_presence() {
    let fake = running(true).await;
    fake.state().pool.as_mut().expect("pool").spec.pool.presence = gdl_k8s::Presence::ScaledToZero;
    fake.reconcile(1_000).await.expect("scale to zero");
    assert_eq!(fake.state().hub_presence, gdl_k8s::Presence::ScaledToZero);
    assert_eq!(
        fake.status().presence,
        Some(gdl_k8s::Presence::ScaledToZero)
    );
    fake.settle();

    // A new PoolReadiness starts its publisher with Serving.
    fake.state().hub_presence = gdl_k8s::Presence::Serving;
    fake.state().log.clear();
    fake.reconcile(2_000)
        .await
        .expect("reconcile after hub restart");
    assert!(
        fake.state()
            .objects
            .values()
            .filter(|object| object.live.kind == Kind::GdlScaler)
            .all(|object| object.live.suspended),
        "the segments remain suspended"
    );
    let presence = fake.state().hub_presence;
    assert_eq!(
        presence,
        gdl_k8s::Presence::ScaledToZero,
        "the restarted hub's directory record must agree with the suspended segments: {:?}",
        fake.log(),
    );
}
