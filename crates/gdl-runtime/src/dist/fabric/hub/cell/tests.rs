//! An instance's watch of its cell for its drain, read by the watch
//! client within a short bound set for the test, from a server that sends
//! a snapshot's first events and then only keep-alives: nothing the
//! partial snapshot says is learned, and the cell is watched again.

use std::time::Duration;

use gdl_cells::CellState;
use gdl_watch::{CellWatchEvent, CellWatcher, WatchLimits};
use tokio::sync::watch;
use velo::InstanceId;

use super::{Learned, run};
use crate::dist::cells::served::Server;
use crate::dist::cells::{REWATCH, REWATCH_CAP};

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
        second.at - began >= WITHIN + REWATCH / 2,
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

/// An entry instance learned that its cell drains, then retired, and its
/// hub then closes every watch, so that the instance waits between them at
/// its cap: its drain's bound and its retirement's bound still pass on
/// time, since neither waits on the watch. On the real clock, since the
/// watch client's own timers would run ahead on a paused one while it
/// waits on the socket. The failed watches establish the cap before the
/// drain starts, so its bound fits inside even the shortest capped wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_bounds_pass_on_time_while_the_drain_watch_waits_at_its_cap() {
    const BOUND: Duration = Duration::from_millis(100);
    const LATE: Duration = Duration::from_millis(200);
    let mut server = Server::new().await;
    let me = InstanceId::new_v4();
    let (learns, learned) = watch::channel(Learned::default());
    let watcher = CellWatcher::new().expect("a client");
    let mut drain = super::CellDrain {
        learned,
        role: super::Role::Entry { bound: BOUND },
        entries: vec!["f".into()],
        foreign: false,
        retired: None,
        watch: tokio::spawn(run(watcher, server.address.clone(), Some(me), learns)),
    };
    let mut step = REWATCH;
    while step < REWATCH_CAP {
        drop(server.next().await);
        step = (step * 2).min(REWATCH_CAP);
    }
    let mut first = server.next().await;
    first.answer().await;
    first.send(&cell(CellState::Draining)).await;
    first.send(&CellWatchEvent::Synced).await;
    let mut heard = drain.learned.clone();
    let drained = tokio::time::timeout(
        Duration::from_secs(5),
        heard.wait_for(|learned| learned.drain.is_some()),
    )
    .await
    .expect("the drain is learned in time")
    .expect("the watch lives")
    .drain
    .expect("a drain");
    drain.retire();
    let retired = drain.retired.expect("retired");
    drop(first);

    let declining = drain
        .declining()
        .expect("an entry instance of a draining cell");
    tokio::time::timeout(BOUND + LATE, declining)
        .await
        .expect("the drain's bound passed in time");
    let at = tokio::time::Instant::now();
    assert!(
        at >= drained + BOUND,
        "declined {:?} early",
        drained + BOUND - at
    );
    let parent = drain.parent().expect("a retired entry instance");
    tokio::time::sleep_until(retired + BOUND).await;
    assert!(
        parent.avoids(),
        "the retirement's bound passed and was not seen"
    );
    let again = server.next().await;
    assert!(
        again.at - retired >= REWATCH_CAP / 2,
        "the watch did not wait at its cap: {:?}",
        again.at - retired
    );
    // Use the deadlines because this task can resume after the retry.
    let due = drained.max(retired) + BOUND;
    assert!(due < again.at, "the bounds did not precede the next watch");
}
