//! A cell's hub tells a retired entry instance, on the cell's watch, once
//! every cell that followed the cell when the instance retired avoids it:
//! each follower's hub acknowledges the retirement once its choosers have,
//! or lets go of the cell. A watch lost without a release is never taken
//! for either; a cell that starts to follow after the retirement is not
//! waited for, since it is shown the instance retired. Here a test stands
//! in for the follower's hub. Every hub runs in process on OS-assigned
//! ports; every wait is for something observed.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::{GLOBAL, Instance, WAIT, meet_the_quorum, settings, timing};
use futures::{FutureExt, Stream, StreamExt};
use gdl_cells::MemoryDirectory;
use gdl_hub::{CellReadiness, CellTiming, CellWatch};
use gdl_model::{CellSettings, SegmentPhase};
use gdl_watch::{Avoided, CellWatchEvent, CellWatcher, Follower};
use velo_hub::Hub;
use velo_hub::velo::InstanceId;

async fn next<T>(watch: &mut (impl Stream<Item = T> + Unpin)) -> T {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("an event in time")
        .expect("the watch goes on")
}

/// The events the watch has now.
fn pending<T>(watch: &mut (impl Stream<Item = T> + Unpin)) -> Vec<T> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        events.push(event);
    }
    events
}

/// The next event about an instance: the cell's state and load, which
/// change as the quorum holds and the summary period passes, are passed
/// over.
async fn next_change(watch: &mut CellWatch) -> CellWatchEvent {
    loop {
        match next(watch).await {
            CellWatchEvent::Load { .. } | CellWatchEvent::State { .. } => {}
            event => return event,
        }
    }
}

/// A watch of the model cell past its snapshot.
async fn watching(cell: &CellReadiness<MemoryDirectory>) -> CellWatch {
    let mut watch = cell.watch().await.expect("the watch begins");
    while next(&mut watch).await != CellWatchEvent::Synced {}
    watch
}

/// The snapshot of a new watch of the model cell.
async fn snapshot(cell: &CellReadiness<MemoryDirectory>) -> Vec<CellWatchEvent> {
    let mut watch = cell.watch().await.expect("the watch begins");
    let mut events = Vec::new();
    loop {
        let event = next(&mut watch).await;
        if event == CellWatchEvent::Synced {
            return events;
        }
        events.push(event);
    }
}

/// The model cell, with the shortest summary period, which paces when a
/// watch closed without a release is marked lost.
fn summarized() -> CellSettings {
    CellSettings {
        summary: Duration::from_secs(1),
        ..settings()
    }
}

/// The model cell's hub, whose follower watches are lost `lease` after
/// they close without a release.
async fn model(lease: Duration) -> (Hub, std::sync::Arc<CellReadiness<MemoryDirectory>>) {
    common::hub_of(
        summarized(),
        MemoryDirectory::new(),
        CellTiming { lease, ..timing() },
    )
    .await
}

/// The hub's address, as a follower reaches it.
fn address(hub: &Hub) -> String {
    format!("127.0.0.1:{}", hub.public_addr().port())
}

/// The global cell's watch of the model cell in the follow `generation`
/// of the run `run`.
fn follower(run: u64, generation: u64) -> Follower {
    Follower {
        class: "global".into(),
        cell: GLOBAL.into(),
        run,
        generation,
    }
}

/// Open the follower's watch of the cell at `hub`, read past its snapshot,
/// and wait until the cell counts it: the stream, which holds the watch
/// open until it is dropped.
async fn follow(
    watcher: &CellWatcher,
    hub: &Hub,
    cell: &CellReadiness<MemoryDirectory>,
    follower: &Follower,
) -> impl Stream + Unpin + use<> {
    let mut events = watcher
        .follow(&address(hub), follower)
        .await
        .expect("the follower's watch begins");
    loop {
        let event = next(&mut events).await;
        if event.event() == Some(CellWatchEvent::Synced) {
            break;
        }
    }
    let mut followers = cell.followers();
    let counted = BTreeSet::from([("global".to_string(), GLOBAL.to_string())]);
    tokio::time::timeout(WAIT, followers.wait_for(|now| *now == counted))
        .await
        .expect("the cell counts its follower in time")
        .expect("the hub lives");
    events
}

/// The follower tells the cell's hub that its cell avoids `instance`.
async fn avoid(watcher: &CellWatcher, hub: &Hub, follower: &Follower, instance: InstanceId) {
    let avoided = Avoided {
        follower: follower.clone(),
        instance,
    };
    watcher
        .avoided(&address(hub), &avoided)
        .await
        .expect("the hub takes it");
}

/// Release the follower's watch.
async fn release(watcher: &CellWatcher, hub: &Hub, follower: &Follower) {
    watcher
        .release(&address(hub), follower)
        .await
        .expect("the hub takes the release");
}

fn retired(instance: InstanceId, acknowledged: bool) -> CellWatchEvent {
    CellWatchEvent::Retired {
        segment: "f".into(),
        instance,
        acknowledged,
    }
}

/// A ready entry instance of the model cell.
async fn entry(hub: &Hub) -> Instance {
    let mut instance = Instance::join(hub, "f").await;
    instance.announce(SegmentPhase::Ready).await;
    instance
}

/// Wait until the watch says that `instance` is ready.
async fn shown_ready(watch: &mut CellWatch, instance: InstanceId) {
    loop {
        if let CellWatchEvent::Ready { peer, .. } = next_change(watch).await
            && peer.instance_id() == instance
        {
            return;
        }
    }
}

/// With no cell following the model cell, an entry instance's retirement
/// is acknowledged in the event that announces it, and so in every
/// snapshot after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retirement_no_cell_follows_is_acknowledged_at_once() {
    let (hub, cell) = model(timing().lease).await;
    let (mut instances, _) = meet_the_quorum(&hub).await;
    let mut watch = watching(&cell).await;
    let entry = &mut instances[0];
    entry.announce(SegmentPhase::Retired).await;
    assert_eq!(next_change(&mut watch).await, retired(entry.id(), true));
    assert!(snapshot(&cell).await.contains(&retired(entry.id(), true)));
    hub.shutdown().await.expect("the hub stops");
}

/// A retirement waits for the follower's acknowledgement under the watch
/// it counted: one under another run, or of another instance, changes
/// nothing; the follower's own says so on the watch at once, and in every
/// snapshot after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retirement_is_acknowledged_once_its_follower_avoids_it() {
    let (hub, cell) = model(timing().lease).await;
    let (instances, _) = meet_the_quorum(&hub).await;
    let watcher = CellWatcher::new().expect("a watcher");
    let parent = follower(7, 1);
    let _following = follow(&watcher, &hub, &cell, &parent).await;
    let mut watch = watching(&cell).await;
    let mut retiring = entry(&hub).await;
    shown_ready(&mut watch, retiring.id()).await;
    retiring.announce(SegmentPhase::Retired).await;
    assert_eq!(next_change(&mut watch).await, retired(retiring.id(), false));

    avoid(&watcher, &hub, &follower(8, 1), retiring.id()).await;
    avoid(&watcher, &hub, &follower(7, 2), retiring.id()).await;
    avoid(&watcher, &hub, &parent, instances[0].id()).await;
    assert!(
        pending(&mut watch).iter().all(|event| matches!(
            event,
            CellWatchEvent::Load { .. } | CellWatchEvent::State { .. }
        )),
        "acknowledged by a watch it does not wait for"
    );
    avoid(&watcher, &hub, &parent, retiring.id()).await;
    assert_eq!(next_change(&mut watch).await, retired(retiring.id(), true));
    assert!(
        snapshot(&cell)
            .await
            .contains(&retired(retiring.id(), true))
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A follower that lets go of the cell makes no new offer to it: its
/// release acknowledges every retirement that waited for its watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_released_follower_counts_as_acknowledged() {
    let (hub, cell) = model(timing().lease).await;
    let (_instances, _) = meet_the_quorum(&hub).await;
    let watcher = CellWatcher::new().expect("a watcher");
    let parent = follower(7, 1);
    let following = follow(&watcher, &hub, &cell, &parent).await;
    let mut watch = watching(&cell).await;
    let mut retiring = entry(&hub).await;
    shown_ready(&mut watch, retiring.id()).await;
    retiring.announce(SegmentPhase::Retired).await;
    assert_eq!(next_change(&mut watch).await, retired(retiring.id(), false));
    drop(following);
    release(&watcher, &hub, &parent).await;
    assert_eq!(next_change(&mut watch).await, retired(retiring.id(), true));
    hub.shutdown().await.expect("the hub stops");
}

/// A watch closed without a release is lost once the lease has passed,
/// and is never taken as an acknowledgement: a retirement announced while
/// it is lost waits for it, and one announced while it was open goes on
/// waiting for it when it is lost. Only its late release acknowledges
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_watch_is_never_an_acknowledgement() {
    let lease = Duration::from_secs(1);
    let (hub, cell) = model(lease).await;
    let (_instances, _) = meet_the_quorum(&hub).await;
    let watcher = CellWatcher::new().expect("a watcher");
    let parent = follower(7, 1);
    let following = follow(&watcher, &hub, &cell, &parent).await;
    let mut watch = watching(&cell).await;
    let mut before = entry(&hub).await;
    shown_ready(&mut watch, before.id()).await;
    let mut after = entry(&hub).await;
    shown_ready(&mut watch, after.id()).await;
    before.announce(SegmentPhase::Retired).await;
    assert_eq!(next_change(&mut watch).await, retired(before.id(), false));

    drop(following);
    // Lost once the lease has passed since it closed, looked at once a
    // summary period: it no longer shows as a follower.
    let mut followers = cell.followers();
    tokio::time::timeout(WAIT, followers.wait_for(BTreeSet::is_empty))
        .await
        .expect("the watch lost in time")
        .expect("the hub lives");
    after.announce(SegmentPhase::Retired).await;
    assert_eq!(
        next_change(&mut watch).await,
        retired(after.id(), false),
        "a lost watch is waited for"
    );
    release(&watcher, &hub, &parent).await;
    let mut acknowledged = vec![next_change(&mut watch).await, next_change(&mut watch).await];
    acknowledged.sort_by_key(|event| match event {
        CellWatchEvent::Retired { instance, .. } => *instance == after.id(),
        _ => true,
    });
    assert_eq!(
        acknowledged,
        [retired(before.id(), true), retired(after.id(), true)],
        "nothing acknowledged them before the release"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A follower that reads the cell again opens a new watch under the same
/// follow and closes the one it replaces: it is still the follower the
/// retirement waits for, and its acknowledgement under that follow counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watch_replaced_under_the_same_follow_is_still_waited_for() {
    let (hub, cell) = model(timing().lease).await;
    let (_instances, _) = meet_the_quorum(&hub).await;
    let watcher = CellWatcher::new().expect("a watcher");
    let parent = follower(7, 1);
    let first = follow(&watcher, &hub, &cell, &parent).await;
    let mut watch = watching(&cell).await;
    let mut retiring = entry(&hub).await;
    shown_ready(&mut watch, retiring.id()).await;
    retiring.announce(SegmentPhase::Retired).await;
    assert_eq!(next_change(&mut watch).await, retired(retiring.id(), false));
    let _second = follow(&watcher, &hub, &cell, &parent).await;
    drop(first);
    let mut later = entry(&hub).await;
    shown_ready(&mut watch, later.id()).await;
    later.announce(SegmentPhase::Retired).await;
    assert_eq!(
        next_change(&mut watch).await,
        retired(later.id(), false),
        "the replacement is counted"
    );
    avoid(&watcher, &hub, &parent, retiring.id()).await;
    assert_eq!(next_change(&mut watch).await, retired(retiring.id(), true));
    avoid(&watcher, &hub, &parent, later.id()).await;
    assert_eq!(next_change(&mut watch).await, retired(later.id(), true));
    hub.shutdown().await.expect("the hub stops");
}

/// A cell that starts to follow after an entry instance retired is shown
/// it retired in its snapshot, so it never offers to it: the retirement
/// does not wait for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_that_starts_after_the_retirement_is_not_waited_for() {
    let (hub, cell) = model(timing().lease).await;
    let (_instances, _) = meet_the_quorum(&hub).await;
    let watcher = CellWatcher::new().expect("a watcher");
    let parent = follower(7, 1);
    let _first = follow(&watcher, &hub, &cell, &parent).await;
    let mut watch = watching(&cell).await;
    let mut retiring = entry(&hub).await;
    shown_ready(&mut watch, retiring.id()).await;
    retiring.announce(SegmentPhase::Retired).await;
    assert_eq!(next_change(&mut watch).await, retired(retiring.id(), false));

    let late = follower(7, 2);
    let mut later = watcher
        .follow(&address(&hub), &late)
        .await
        .expect("the follower's watch begins");
    let mut shown = Vec::new();
    loop {
        match next(&mut later).await.event() {
            Some(CellWatchEvent::Synced) => break,
            Some(event) => shown.push(event),
            None => panic!("an event of a kind this build does not know"),
        }
    }
    assert!(
        shown.contains(&retired(retiring.id(), false)),
        "shown retired: {shown:?}"
    );
    avoid(&watcher, &hub, &parent, retiring.id()).await;
    assert_eq!(next_change(&mut watch).await, retired(retiring.id(), true));
    hub.shutdown().await.expect("the hub stops");
}
