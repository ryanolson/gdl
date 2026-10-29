//! A global pool's health with `requires`: its own quorum, and enough
//! followed model pools whose link says they are ready. Every hub runs in
//! process on OS-assigned ports, sharing one directory whose hub names a
//! test resolves once each hub serves; every wait is for something
//! observed.

mod common;

use std::sync::Arc;

use common::{
    Instance, POOL, Records, Resolving, get, global_default, global_requiring, health, hub_of_spec,
    meet_the_quorum, model, serve_pool, timing,
};
use gdl_hub::{PoolHealth, PoolReadiness, RequiredHealth};
use gdl_model::{
    DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, PoolSettings, Role, SegmentPhase,
    TypeInfo,
};
use gdl_pools::PoolState;
use velo_hub::Hub;

fn met(health: &PoolHealth) -> u32 {
    health.requires["model_x"].met
}

/// The health endpoint's status and body.
async fn status(hub: &velo_hub::Hub) -> (u16, serde_json::Value) {
    let (status, body) = get(hub, "/v1/ext/gdl-pool/health").await;
    (status, serde_json::from_str(&body).expect("JSON"))
}

/// A global pool that requires one ready model pool answers 503 until it
/// has its own quorum and follows a model pool whose link says `ready`,
/// 200 then, and 503 again while that pool is only `degraded` and once
/// its link drops. The body names each required class with the count
/// required and the count met. A pool that requires nothing answers as
/// before, with no `requires` in its body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_is_healthy_with_its_quorum_and_its_required_pools() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (global_hub, global_pool) = serve_pool(global_requiring(1), &directory, timing()).await;
    let (code, body) = status(&global_hub).await;
    assert_eq!(code, 503);
    assert_eq!(
        body["requires"],
        serde_json::json!({ "model_x": { "required": 1, "met": 0 } })
    );

    let mut own = Instance::join(&global_hub, "a").await;
    own.announce(SegmentPhase::Ready).await;
    health(&global_pool, "the global's own quorum", |h| {
        h.state == PoolState::Ready
    })
    .await;
    assert_eq!(status(&global_hub).await.0, 503, "no model pool yet");

    let (mut instances, _) = meet_the_quorum(&model_hub).await;
    records.until(PoolState::Ready).await;
    let healthy = health(&global_pool, "a ready model pool", |h| met(h) == 1).await;
    assert_eq!(
        healthy.requires["model_x"],
        RequiredHealth {
            required: 1,
            met: 1
        }
    );
    let (code, body) = status(&global_hub).await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(body["state"], "ready");

    // `d` below its quorum: the model pool is degraded, still followed,
    // and no longer counts.
    instances[2].withdraw(SegmentPhase::Ready).await;
    health(&model_pool, "the model pool degraded", |h| {
        h.state == PoolState::Degraded
    })
    .await;
    health(&global_pool, "a degraded model pool", |h| met(h) == 0).await;
    assert_eq!(status(&global_hub).await.0, 503);
    instances[2].announce(SegmentPhase::Ready).await;
    health(&global_pool, "the model pool ready again", |h| met(h) == 1).await;
    assert_eq!(status(&global_hub).await.0, 200);

    let (code, body) = status(&model_hub).await;
    assert_eq!(code, 200, "{body}");
    assert!(body.get("requires").is_none(), "{body}");

    model_hub.shutdown().await.expect("the hub stops");
    health(&global_pool, "the link dropped", |h| met(h) == 0).await;
    assert_eq!(status(&global_hub).await.0, 503);
    global_hub.shutdown().await.expect("the hub stops");
}

/// Two roots in `global`'s segment `a`: `S => F` crosses into `model_x`,
/// and `T => G` into `model_y`, so the global sends to both.
fn two_classes() -> DeploymentSpec {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let hop = |outer: &str, inner: &str| HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![TypeInfo::new("T", true, Default::default())],
        backward_types: vec![TypeInfo::new("T", true, Default::default())],
    };
    let graph = Manifest {
        name: "two-classes".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("T", Role::Source),
            node("F", Role::Sink),
            node("G", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("T", "G")],
    };
    let placement = Placement::new()
        .segment("a", ["S", "T"])
        .segment("f", ["F"])
        .segment("g", ["G"])
        .class("global", ["a"])
        .class("model_x", ["f"])
        .class("model_y", ["g"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// The pool `name` of `class`; a model pool is a child of
/// `global-us-east-1`.
fn pool(name: &str, class: &str, hub: &str, requires: &str) -> PoolSettings {
    pool_of("global-us-east-1", name, class, hub, requires)
}

/// The pool `name` of `class`; a model pool is a child of `parent`.
fn pool_of(parent: &str, name: &str, class: &str, hub: &str, requires: &str) -> PoolSettings {
    let home = if class == "global" {
        String::new()
    } else {
        format!("home = '{parent}'\n")
    };
    toml::from_str(&format!(
        "name = '{name}'\nclass = '{class}'\nregion = 'us-east-1'\n{home}hub = '{hub}'\n\
         directory = 'memory'\n{requires}"
    ))
    .expect("valid test fixture")
}

/// A hub of `two_classes()` for `pool`, resolved in `directory`, with
/// one ready instance of `segment`: its quorum.
async fn serve_ready(
    pool: PoolSettings,
    directory: &Resolving,
    segment: &'static str,
) -> (Hub, Arc<PoolReadiness<Resolving>>, Instance) {
    let name = pool.hub.clone().expect("a hub name");
    let (hub, readiness) = hub_of_spec(two_classes(), pool, directory.clone(), timing()).await;
    directory.resolve(&name, &hub);
    let mut instance = Instance::join_spec(&hub, &two_classes(), segment).await;
    instance.announce(SegmentPhase::Ready).await;
    health(&readiness, "the pool's own quorum", |h| {
        h.state == PoolState::Ready
    })
    .await;
    (hub, readiness, instance)
}

/// A pool is named within its class, as the directory keys it: a global
/// that sends to two classes follows a pool of each that share a name,
/// and each counts toward its own class's requirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pools_of_two_classes_may_share_a_name() {
    let directory = Resolving::new();
    let (x_hub, _x, _x_instance) = serve_ready(
        pool("m-us-east-1", "model_x", "x-hub:1337", ""),
        &directory,
        "f",
    )
    .await;
    let (y_hub, _y, _y_instance) = serve_ready(
        pool("m-us-east-1", "model_y", "y-hub:1337", ""),
        &directory,
        "g",
    )
    .await;
    let (global_hub, global_pool, _own) = serve_ready(
        pool(
            "global-us-east-1",
            "global",
            "global-hub:1337",
            "[requires]\nmodel_x = 1\nmodel_y = 1\n",
        ),
        &directory,
        "a",
    )
    .await;
    let healthy = health(&global_pool, "a ready pool of each class", |h| {
        h.requires.values().all(|r| r.met == 1)
    })
    .await;
    assert!(healthy.healthy(), "{healthy:?}");
    assert_eq!(status(&global_hub).await.0, 200);
    for hub in [global_hub, x_hub, y_hub] {
        hub.shutdown().await.expect("the hub stops");
    }
}

/// Unsaid, a global's `requires` is one ready child of each class its
/// class sends to: with none followed, its health answers 503 and names
/// the class it lacks, and 200 once a ready child is followed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_requires_a_ready_child_by_default() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    let mut own = Instance::join(&global_hub, "a").await;
    own.announce(SegmentPhase::Ready).await;
    health(&global_pool, "the global's own quorum", |h| {
        h.state == PoolState::Ready
    })
    .await;
    let (code, body) = status(&global_hub).await;
    assert_eq!(code, 503, "{body}");
    assert_eq!(
        body["requires"],
        serde_json::json!({ "model_x": { "required": 1, "met": 0 } })
    );
    assert_eq!(body["missing"], serde_json::json!(["model_x"]), "{body}");

    let (model_hub, _model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    records.until(PoolState::Ready).await;
    let healthy = health(&global_pool, "a ready child", |h| met(h) == 1).await;
    assert!(healthy.missing.is_empty(), "{healthy:?}");
    let (code, body) = status(&global_hub).await;
    assert_eq!(code, 200, "{body}");
    assert!(body.get("missing").is_none(), "{body}");
    for hub in [global_hub, model_hub] {
        hub.shutdown().await.expect("the hub stops");
    }
}

/// An explicit `requires` replaces the default: a global that sends to
/// `model_x` and `model_y` and requires only `model_x` is healthy with a
/// ready child of `model_x` alone, while one that leaves `requires`
/// unsaid lacks `model_y`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_explicit_requires_overrides_the_default() {
    let directory = Resolving::new();
    let (unsaid_x_hub, _unsaid_x, _unsaid_x_instance) = serve_ready(
        pool_of("global-us-east-1", "m-1", "model_x", "x1-hub:1337", ""),
        &directory,
        "f",
    )
    .await;
    let (named_x_hub, _named_x, _named_x_instance) = serve_ready(
        pool_of("global-us-east-2", "m-2", "model_x", "x2-hub:1337", ""),
        &directory,
        "f",
    )
    .await;
    let (unsaid_hub, unsaid, _unsaid_own) = serve_ready(
        pool("global-us-east-1", "global", "global-1-hub:1337", ""),
        &directory,
        "a",
    )
    .await;
    let (named_hub, named, _named_own) = serve_ready(
        pool(
            "global-us-east-2",
            "global",
            "global-2-hub:1337",
            "[requires]\nmodel_x = 1\n",
        ),
        &directory,
        "a",
    )
    .await;

    let lacking = health(&unsaid, "its ready child of model_x", |h| met(h) == 1).await;
    assert_eq!(lacking.missing, ["model_y"], "{lacking:?}");
    let (code, body) = status(&unsaid_hub).await;
    assert_eq!(code, 503, "{body}");
    assert_eq!(body["missing"], serde_json::json!(["model_y"]), "{body}");

    let healthy = health(&named, "its ready child of model_x", |h| met(h) == 1).await;
    assert_eq!(
        healthy.requires.keys().collect::<Vec<_>>(),
        ["model_x"],
        "{healthy:?}"
    );
    let (code, body) = status(&named_hub).await;
    assert_eq!(code, 200, "{body}");
    for hub in [unsaid_hub, named_hub, unsaid_x_hub, named_x_hub] {
        hub.shutdown().await.expect("the hub stops");
    }
}

/// A body without `missing`, as a hub of a build before it serves, is
/// judged by its counts: a class met by fewer pools than required leaves
/// the pool unhealthy.
#[test]
fn a_body_without_missing_is_judged_by_its_counts() {
    let body = serde_json::json!({
        "state": "ready",
        "segments": { "a": { "usable": 1, "quorum": 1 } },
        "requires": { "model_x": { "required": 1, "met": 0 } },
    });
    let health: PoolHealth = serde_json::from_value(body).expect("a body");
    assert!(health.missing.is_empty());
    assert!(!health.healthy(), "{health:?}");
    let met = serde_json::json!({
        "state": "ready",
        "segments": { "a": { "usable": 1, "quorum": 1 } },
        "requires": { "model_x": { "required": 1, "met": 1 } },
    });
    let health: PoolHealth = serde_json::from_value(met).expect("a body");
    assert!(health.healthy(), "{health:?}");
}
