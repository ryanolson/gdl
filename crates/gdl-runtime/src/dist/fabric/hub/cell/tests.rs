//! An instance's watch of its cell for its drain, read by the watch
//! client within a short bound set for the test, from a server that sends
//! a snapshot's first events and then only keep-alives: nothing the
//! partial snapshot says is learned, and the cell is watched again.

use std::time::Duration;

use gdl_cells::CellState;
use gdl_watch::{CellWatchEvent, CellWatcher, WatchLimits};
use tokio::sync::watch;
use velo::InstanceId;

use super::{Learned, REWATCH, run};
use crate::dist::cells::served::Server;

const WITHIN: Duration = Duration::from_millis(300);

fn cell(state: CellState) -> CellWatchEvent {
    CellWatchEvent::Cell {
        name: "m".into(),
        class: "model_x".into(),
        epoch: 1,
        state,
        load: None,
    }
}

/// A snapshot that says the cell drains, that the instance's retirement
/// is acknowledged, and that no cell follows the cell, then only
/// keep-alives, teaches the instance none of it: the watch ends at the
/// bound and the cell is watched again. A complete snapshot then teaches
/// it all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_a_snapshot_that_never_completes_says_is_learned() {
    let mut server = Server::new().await;
    let watcher = CellWatcher::with_limits(WatchLimits {
        synced_within: WITHIN,
        ..WatchLimits::default()
    })
    .expect("a client");
    let me = InstanceId::new_v4();
    let (learns, mut learned) = watch::channel(Learned::default());
    // Before the watch is asked for, so that its bound has run by then.
    let began = tokio::time::Instant::now();
    let task = tokio::spawn(run(watcher, server.address.clone(), Some(me), learns));
    let snapshot = [
        cell(CellState::Draining),
        CellWatchEvent::Retired {
            segment: "f".into(),
            instance: me,
            acknowledged: true,
        },
        CellWatchEvent::State {
            state: CellState::Draining,
            unfollowed: true,
        },
    ];

    let mut first = server.next().await;
    first.answer().await;
    for event in &snapshot {
        first.send(event).await;
    }
    first.keep_alive();
    let mut second = server.next().await;
    assert!(
        second.at - began >= WITHIN + REWATCH,
        "watched again after {:?}",
        second.at - began
    );
    let now = *learned.borrow();
    assert!(
        now.drain.is_none() && !now.acknowledged && !now.unfollowed,
        "learned from a partial snapshot: {now:?}"
    );

    second.answer().await;
    for event in &snapshot {
        second.send(event).await;
    }
    second.send(&CellWatchEvent::Synced).await;
    let now = *tokio::time::timeout(
        Duration::from_secs(5),
        learned.wait_for(|learned| learned.unfollowed),
    )
    .await
    .expect("learned in time")
    .expect("the watch says so before it ends");
    assert!(now.drain.is_some() && now.acknowledged, "{now:?}");
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the watch ends once the cell is unfollowed")
        .expect("the watch does not panic");
}
