//! A cell's hub tells a retired entry instance, on the cell's watch, once
//! every cell that followed the cell when the instance retired avoids it:
//! each follower's hub acknowledges the retirement once its choosers have,
//! or lets go of the cell. A watch lost without a release is never taken
//! for either; a cell that starts to follow after the retirement is not
//! waited for, since it is shown the instance retired. In the first tests
//! a test stands in for the follower's hub; in the last, a global cell's
//! hub follows the cell, and tests stand in for its choosers. Every hub
//! runs in process on OS-assigned ports; every wait is for something
//! observed.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::{
    CELL, GLOBAL, Instance, Resolving, WAIT, global_default, meet_the_quorum, serve_cell, settings,
    timing,
};
use futures::{FutureExt, Stream, StreamExt};
use gdl_cells::MemoryDirectory;
use gdl_hub::{CellReadiness, CellTiming, CellWatch};
use gdl_model::{CellSettings, SegmentPhase};
use gdl_watch::{Avoided, Avoids, CellWatchEvent, CellWatcher, FollowEvent, Follower};
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
async fn watching<D: gdl_cells::CellDirectory>(cell: &CellReadiness<D>) -> CellWatch {
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
        if event.expect("the watch does not fail").event() == Some(CellWatchEvent::Synced) {
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
        match next(&mut later)
            .await
            .expect("the watch does not fail")
            .event()
        {
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

/// A model cell followed by a global cell's hub: the model cell's hub and
/// extension, its entry instance `retiring`, ready and exposed by the
/// global's relay, and the global's hub and extension, with the follow's
/// generation.
struct Followed {
    model: (Hub, std::sync::Arc<CellReadiness<Resolving>>),
    retiring: Instance,
    _others: Vec<Instance>,
    global: (Hub, std::sync::Arc<CellReadiness<Resolving>>),
    generation: u64,
}

/// [`Followed`], with `choosers` choosers of the global cell, each of
/// which has said it runs a follower before the model cell is followed.
async fn followed(choosers: usize) -> (Followed, Vec<Instance>) {
    let directory = Resolving::new();
    let global_settings = global_default();
    let name = global_settings.hub.clone().expect("a hub name");
    let (global_hub, global_cell) =
        common::hub_of(global_settings, directory.clone(), timing()).await;
    directory.resolve(&name, &global_hub);
    let mut joined = Vec::new();
    for _ in 0..choosers {
        joined.push(Instance::chooser(&global_hub).await);
    }
    let mut relay = global_cell.follows();
    let (model_hub, model_cell) = serve_cell(
        common::model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (others, _) = meet_the_quorum(&model_hub).await;
    let retiring = entry(&model_hub).await;
    let mut generation = None;
    loop {
        match next(&mut relay).await {
            FollowEvent::Follow { generation: g, .. } => generation = Some(g),
            FollowEvent::Exposed { peer, .. } if peer.instance_id() == retiring.id() => break,
            _ => {}
        }
    }
    let followed = Followed {
        model: (model_hub, model_cell),
        retiring,
        _others: others,
        global: (global_hub, global_cell),
        generation: generation.expect("a follow before its instances"),
    };
    (followed, joined)
}

impl Followed {
    /// What a chooser says once its tables hold `retiring` retired.
    fn avoids(&self) -> Avoids {
        Avoids {
            class: "model_x".into(),
            cell: CELL.into(),
            generation: self.generation,
            instance: self.retiring.id(),
        }
    }

    /// Retire `retiring`, and wait until the global's hub waits for its
    /// choosers to avoid it, or, with `waits` false, does not: the model
    /// cell's watch from before the retirement, past it.
    async fn retire(&mut self, waits: bool) -> CellWatch {
        let mut watch = watching(&self.model.1).await;
        self.retiring.announce(SegmentPhase::Retired).await;
        assert_eq!(
            next_change(&mut watch).await,
            retired(self.retiring.id(), false),
            "the global's follow is waited for"
        );
        if waits {
            let waited = self.retirement();
            let mut retirements = self.global.1.retirements();
            tokio::time::timeout(WAIT, retirements.wait_for(|now| now.contains(&waited)))
                .await
                .expect("the global's hub waits for its choosers in time")
                .expect("the hub lives");
        }
        watch
    }

    fn retirement(&self) -> (String, String, u64, InstanceId) {
        (
            "model_x".into(),
            CELL.into(),
            self.generation,
            self.retiring.id(),
        )
    }

    /// Whether the global's hub still waits for its choosers to avoid
    /// `retiring`.
    fn waiting(&self) -> bool {
        self.global
            .1
            .retirements()
            .borrow()
            .contains(&self.retirement())
    }

    async fn shutdown(self) {
        self.global.0.shutdown().await.expect("the hub stops");
        self.model.0.shutdown().await.expect("the hub stops");
    }
}

/// The global's hub waits for each of its choosers to avoid the retired
/// instance, and refuses an acknowledgement from a member that is not a
/// chooser; once both have, it tells the model cell's hub, which says so
/// on the cell's watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_acknowledges_once_every_chooser_avoids_the_retiree() {
    let (mut cell, choosers) = followed(2).await;
    let mut watch = cell.retire(true).await;
    let undeclared = Instance::join(&cell.global.0, "a").await;
    undeclared
        .avoid(&cell.avoids())
        .await
        .expect_err("not a chooser");
    choosers[0].avoid(&cell.avoids()).await.expect("taken");
    assert!(cell.waiting(), "one of two choosers");
    choosers[0]
        .avoid(&cell.avoids())
        .await
        .expect("taken twice");
    assert!(cell.waiting(), "the same chooser twice");
    choosers[1].avoid(&cell.avoids()).await.expect("taken");
    assert!(!cell.waiting());
    assert_eq!(
        next_change(&mut watch).await,
        retired(cell.retiring.id(), true)
    );
    cell.shutdown().await;
}

/// A chooser that leaves the global's hub counts as one that avoids the
/// retired instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chooser_that_leaves_counts_as_having_avoided_the_retiree() {
    let (mut cell, mut choosers) = followed(2).await;
    let mut watch = cell.retire(true).await;
    choosers[0].avoid(&cell.avoids()).await.expect("taken");
    assert!(cell.waiting());
    choosers.pop().expect("a chooser").leave().await;
    assert_eq!(
        next_change(&mut watch).await,
        retired(cell.retiring.id(), true)
    );
    assert!(!cell.waiting());
    cell.shutdown().await;
}

/// A chooser that says it runs a follower after the retirement was applied
/// is shown the instance retired by the relay's snapshot, and is not
/// waited for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chooser_that_joins_after_the_retirement_is_not_waited_for() {
    let (mut cell, choosers) = followed(1).await;
    let mut watch = cell.retire(true).await;
    let _late = Instance::chooser(&cell.global.0).await;
    choosers[0].avoid(&cell.avoids()).await.expect("taken");
    assert!(!cell.waiting());
    assert_eq!(
        next_change(&mut watch).await,
        retired(cell.retiring.id(), true)
    );
    cell.shutdown().await;
}

/// A global cell without a chooser avoids the retired instance at once,
/// and tells the model cell's hub so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_without_a_chooser_acknowledges_at_once() {
    let (mut cell, _) = followed(0).await;
    let mut watch = cell.retire(false).await;
    assert_eq!(
        next_change(&mut watch).await,
        retired(cell.retiring.id(), true)
    );
    assert!(!cell.waiting());
    cell.shutdown().await;
}

/// A retiree that leaves its cell's hub while a chooser has yet to avoid
/// it is waited for no more: once the relay sends its `left`, the global's
/// hub holds nothing of its retirement, and a recovered chooser, which is
/// never shown it again, need not settle it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retiree_that_leaves_is_waited_for_no_more() {
    let (mut cell, _choosers) = followed(1).await;
    let _watch = cell.retire(true).await;
    let mut relay = cell.global.1.follows();
    while !matches!(next(&mut relay).await, FollowEvent::Synced { .. }) {}
    let id = cell.retiring.id();
    let Followed {
        retiring, global, ..
    } = cell;
    retiring.leave().await;
    loop {
        if let FollowEvent::Left { instance, .. } = next(&mut relay).await
            && instance == id
        {
            break;
        }
    }
    assert!(
        !global
            .1
            .retirements()
            .borrow()
            .iter()
            .any(|(_, _, _, of)| *of == id),
        "the retirement of an instance that left is still waited for"
    );
}
