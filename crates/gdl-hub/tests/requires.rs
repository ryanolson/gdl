//! A global cell's health with `requires`: its own quorum, and enough
//! followed model cells whose link says they are ready. Every hub runs in
//! process on OS-assigned ports, sharing one directory whose hub names a
//! test resolves once each hub serves; every wait is for something
//! observed.

mod common;

use std::sync::Arc;

use common::{
    CELL, Instance, Records, Resolving, get, global_requiring, health, hub_of_spec,
    meet_the_quorum, model, serve_cell, timing,
};
use gdl_cells::CellState;
use gdl_hub::{CellHealth, CellReadiness, RequiredHealth};
use gdl_model::{
    CellSettings, DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, SegmentPhase,
    TypeInfo,
};
use velo_hub::Hub;

fn met(health: &CellHealth) -> u32 {
    health.requires["model_x"].met
}

/// The health endpoint's status and body.
async fn status(hub: &velo_hub::Hub) -> (u16, serde_json::Value) {
    let (status, body) = get(hub, "/v1/ext/gdl-cell/health").await;
    (status, serde_json::from_str(&body).expect("JSON"))
}

/// A global cell that requires one ready model cell answers 503 until it
/// has its own quorum and follows a model cell whose link says `ready`,
/// 200 then, and 503 again while that cell is only `degraded` and once
/// its link drops. The body names each required class with the count
/// required and the count met. A cell that requires nothing answers as
/// before, with no `requires` in its body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_is_healthy_with_its_quorum_and_its_required_cells() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, model_cell) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (global_hub, global_cell) = serve_cell(global_requiring(1), &directory, timing()).await;
    let (code, body) = status(&global_hub).await;
    assert_eq!(code, 503);
    assert_eq!(
        body["requires"],
        serde_json::json!({ "model_x": { "required": 1, "met": 0 } })
    );

    let mut own = Instance::join(&global_hub, "a").await;
    own.announce(SegmentPhase::Ready).await;
    health(&global_cell, "the global's own quorum", |h| {
        h.state == CellState::Ready
    })
    .await;
    assert_eq!(status(&global_hub).await.0, 503, "no model cell yet");

    let (mut instances, _) = meet_the_quorum(&model_hub).await;
    records.until(CellState::Ready).await;
    let healthy = health(&global_cell, "a ready model cell", |h| met(h) == 1).await;
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

    // `d` below its quorum: the model cell is degraded, still followed,
    // and no longer counts.
    instances[2].withdraw(SegmentPhase::Ready).await;
    health(&model_cell, "the model cell degraded", |h| {
        h.state == CellState::Degraded
    })
    .await;
    health(&global_cell, "a degraded model cell", |h| met(h) == 0).await;
    assert_eq!(status(&global_hub).await.0, 503);
    instances[2].announce(SegmentPhase::Ready).await;
    health(&global_cell, "the model cell ready again", |h| met(h) == 1).await;
    assert_eq!(status(&global_hub).await.0, 200);

    let (code, body) = status(&model_hub).await;
    assert_eq!(code, 200, "{body}");
    assert!(body.get("requires").is_none(), "{body}");

    model_hub.shutdown().await.expect("the hub stops");
    health(&global_cell, "the link dropped", |h| met(h) == 0).await;
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

/// The cell `name` of `class`; a model cell is a child of
/// `global-us-east-1`.
fn cell(name: &str, class: &str, hub: &str, requires: &str) -> CellSettings {
    let home = if class == "global" {
        String::new()
    } else {
        "home = 'global-us-east-1'\n".to_string()
    };
    toml::from_str(&format!(
        "name = '{name}'\nclass = '{class}'\nregion = 'us-east-1'\n{home}hub = '{hub}'\n\
         directory = 'memory'\n{requires}"
    ))
    .expect("valid test fixture")
}

/// A hub of `two_classes()` for `cell`, resolved in `directory`, with
/// one ready instance of `segment`: its quorum.
async fn serve_ready(
    cell: CellSettings,
    directory: &Resolving,
    segment: &'static str,
) -> (Hub, Arc<CellReadiness<Resolving>>, Instance) {
    let name = cell.hub.clone().expect("a hub name");
    let (hub, readiness) = hub_of_spec(two_classes(), cell, directory.clone(), timing()).await;
    directory.resolve(&name, &hub);
    let mut instance = Instance::join_spec(&hub, &two_classes(), segment).await;
    instance.announce(SegmentPhase::Ready).await;
    health(&readiness, "the cell's own quorum", |h| {
        h.state == CellState::Ready
    })
    .await;
    (hub, readiness, instance)
}

/// A cell is named within its class, as the directory keys it: a global
/// that sends to two classes follows a cell of each that share a name,
/// and each counts toward its own class's requirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cells_of_two_classes_may_share_a_name() {
    let directory = Resolving::new();
    let (x_hub, _x, _x_instance) = serve_ready(
        cell("m-us-east-1", "model_x", "x-hub:1337", ""),
        &directory,
        "f",
    )
    .await;
    let (y_hub, _y, _y_instance) = serve_ready(
        cell("m-us-east-1", "model_y", "y-hub:1337", ""),
        &directory,
        "g",
    )
    .await;
    let (global_hub, global_cell, _own) = serve_ready(
        cell(
            "global-us-east-1",
            "global",
            "global-hub:1337",
            "[requires]\nmodel_x = 1\nmodel_y = 1\n",
        ),
        &directory,
        "a",
    )
    .await;
    let healthy = health(&global_cell, "a ready cell of each class", |h| {
        h.requires.values().all(|r| r.met == 1)
    })
    .await;
    assert!(healthy.healthy(), "{healthy:?}");
    assert_eq!(status(&global_hub).await.0, 200);
    for hub in [global_hub, x_hub, y_hub] {
        hub.shutdown().await.expect("the hub stops");
    }
}
