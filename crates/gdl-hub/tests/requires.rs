//! A global cell's health with `requires`: its own quorum, and enough
//! followed model cells whose link says they are ready. Every hub runs in
//! process on OS-assigned ports, sharing one directory whose hub names a
//! test resolves once each hub serves; every wait is for something
//! observed.

mod common;

use common::{
    CELL, Instance, Records, Resolving, get, global_requiring, health, meet_the_quorum, model,
    serve_cell, timing,
};
use gdl_cells::CellState;
use gdl_hub::{CellHealth, RequiredHealth};
use gdl_model::SegmentPhase;

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
