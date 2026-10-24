//! A parent hub whose follow link a full cell watch refuses follows the
//! cell again, as it follows again after any link that does not begin, and
//! follows it once a place frees. Every hub runs in process on
//! OS-assigned ports, sharing one directory whose hub names a test
//! resolves once each hub serves; every wait is for something observed.

mod common;

use std::time::Duration;

use common::{
    CELL, GLOBAL, Records, Resolving, WAIT, global_default, health, meet_the_quorum, model,
    serve_cell, timing,
};
use futures::StreamExt;
use gdl_cells::CellState;
use gdl_hub::{CellReadiness, CellTiming, WatchCaps};
use gdl_watch::CellWatcher;

/// Wait until `cell` has refused `n` watches of itself.
async fn refused(cell: &CellReadiness<Resolving>, n: u64) {
    let refusing = async {
        while cell.refused_watches().cell < n {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(WAIT, refusing)
        .await
        .unwrap_or_else(|_| panic!("{n} watches of the cell were not refused in time"));
}

/// The model cell's hub serves one watch of the cell, which the test
/// holds: the global's link is refused, and asked for again after the
/// refollow delay, and refused again. Once the test lets go, the global
/// follows the cell, its link counts among the cell's followers, and its
/// health meets the cell it requires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_follow_link_follows_the_cell_once_there_is_room() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let capped = CellTiming {
        watch_caps: WatchCaps {
            cell: 1,
            ..WatchCaps::default()
        },
        ..timing()
    };
    let (model_hub, model_cell) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        capped,
    )
    .await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    records.until(CellState::Ready).await;

    let watcher = CellWatcher::new().expect("a client");
    let address = format!("127.0.0.1:{}", model_hub.public_addr().port());
    let mut held = tokio::time::timeout(WAIT, watcher.watch(&address))
        .await
        .expect("the hub answers in time")
        .expect("the hub serves the cell's one watch");
    tokio::time::timeout(WAIT, held.next())
        .await
        .expect("the snapshot begins in time")
        .expect("the watch is served");

    let (global_hub, global_cell) = serve_cell(global_default(), &directory, timing()).await;
    // Refused twice: the link was asked for again after the first.
    refused(&model_cell, 2).await;
    assert!(model_cell.followers().borrow().is_empty());
    assert_eq!(
        global_cell.health().borrow().requires["model_x"].met,
        0,
        "a refused link met the cell"
    );

    drop(held);
    health(&global_cell, "the model cell followed and ready", |h| {
        h.requires["model_x"].met == 1
    })
    .await;
    let followers = model_cell.followers().borrow().clone();
    assert!(
        followers.contains(&("global".to_string(), GLOBAL.to_string())),
        "{followers:?}"
    );
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}
