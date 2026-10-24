//! A cell's watch, as a hub that follows the cell reads it: a snapshot of
//! the cell and its entry instances, then each change. In the deployment
//! of these tests, `f` is the model class's entry segment (the hop from
//! `S` crosses into the class at `F`) and `d` is not. Every hub runs in
//! process on OS-assigned ports, and every wait is for something observed.

mod common;

use std::collections::{HashMap, HashSet};

use common::{
    CELL, Gated, Instance, Records, WAIT, cell_hub, config, health, hub_of, meet_the_quorum,
    settings, timing, usable,
};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use gdl_cells::{CellState, MemoryDirectory};
use gdl_hub::{CellTiming, GraphAdmission};
use gdl_model::{CellSettings, SegmentPhase};
use gdl_watch::{CellWatchEvent, CellWatcher};
use velo_hub::Hub;
use velo_hub::velo::{InstanceId, PeerInfo};

/// The address of `hub`'s public listener, as a cell's record names it.
fn address(hub: &Hub) -> String {
    format!("127.0.0.1:{}", hub.public_addr().port())
}

/// The cell's watch on `hub`, through the client.
async fn watching(hub: &Hub) -> BoxStream<'static, CellWatchEvent> {
    let watcher = CellWatcher::new().expect("a client");
    tokio::time::timeout(WAIT, watcher.watch(&address(hub)))
        .await
        .expect("the hub answers in time")
        .expect("the hub serves the watch")
        .map(|item| item.event().expect("an event of a kind this build knows"))
        .boxed()
}

async fn next(watch: &mut (impl Stream<Item = CellWatchEvent> + Unpin)) -> CellWatchEvent {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("an event in time")
        .expect("the watch goes on")
}

/// Wait for the watch to end.
async fn ends(watch: &mut (impl Stream<Item = CellWatchEvent> + Unpin)) {
    let event = tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("the watch ends in time");
    assert_eq!(event, None, "the watch ended");
}

/// The snapshot: every event up to and including the marker.
async fn snapshot(watch: &mut (impl Stream<Item = CellWatchEvent> + Unpin)) -> Vec<CellWatchEvent> {
    let mut events = Vec::new();
    loop {
        let event = next(watch).await;
        let synced = event == CellWatchEvent::Synced;
        events.push(event);
        if synced {
            return events;
        }
    }
}

/// The `PeerInfo` of `id`, as the hub's members name it.
async fn peer(hub: &Hub, id: InstanceId) -> PeerInfo {
    let (status, body) = common::get(hub, "/v1/ext/gdl/members").await;
    assert_eq!(status, 200, "{body}");
    let members: Vec<serde_json::Value> = serde_json::from_str(&body).expect("JSON");
    members
        .into_iter()
        .map(|member| serde_json::from_value::<PeerInfo>(member["peer"].clone()).expect("a peer"))
        .find(|peer| peer.instance_id() == id)
        .expect("a member")
}

fn ready(peer: PeerInfo) -> CellWatchEvent {
    CellWatchEvent::Ready {
        segment: "f".into(),
        peer,
    }
}

fn state(state: CellState) -> CellWatchEvent {
    CellWatchEvent::State { state }
}

/// The model cell's first event, in `state`, under `epoch`.
fn cell(epoch: u64, state: CellState) -> CellWatchEvent {
    CellWatchEvent::Cell {
        name: CELL.into(),
        class: "model_x".into(),
        epoch,
        state,
        load: None,
    }
}

/// A cell as its follower holds it, from a snapshot and the events since.
#[derive(Debug, Default, PartialEq, Eq)]
struct View {
    epoch: u64,
    state: Option<CellState>,
    retired: HashSet<InstanceId>,
    ready: HashMap<InstanceId, PeerInfo>,
}

impl View {
    fn of(events: &[CellWatchEvent]) -> Self {
        let mut view = Self::default();
        for event in events {
            view.apply(event);
        }
        view
    }

    fn apply(&mut self, event: &CellWatchEvent) {
        match event {
            CellWatchEvent::Cell { epoch, state, .. } => {
                self.epoch = *epoch;
                self.state = Some(*state);
            }
            CellWatchEvent::State { state } => self.state = Some(*state),
            CellWatchEvent::Ready { peer, .. } => {
                assert!(
                    !self.retired.contains(&peer.instance_id()),
                    "a retired instance sent as ready"
                );
                let known = self.ready.insert(peer.instance_id(), peer.clone());
                assert!(known.is_none(), "an instance sent as ready twice");
            }
            CellWatchEvent::Retired { instance, .. } => {
                self.ready.remove(instance);
                assert!(self.retired.insert(*instance), "retired twice");
            }
            CellWatchEvent::Left { instance, .. } => {
                let known = self.ready.remove(instance).is_some() | self.retired.remove(instance);
                assert!(known, "an instance that left was never sent");
            }
            CellWatchEvent::Synced | CellWatchEvent::Load { .. } | CellWatchEvent::Unfollowed => {}
        }
    }
}

/// The snapshot carries the cell (its name, class, epoch, and state), then
/// its retired entry instances, then its ready ones with their `PeerInfo`
/// as the hub's members name them, then the marker. An instance of a
/// segment no other class reaches is not in it, nor an entry instance that
/// never announced it is ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_snapshot_holds_the_cell_and_its_entry_instances() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, cell_readiness) = cell_hub(directory.clone(), timing()).await;
    let published = records.next().await;

    let mut serving = Instance::join(&hub, "f").await;
    serving.announce(SegmentPhase::Ready).await;
    health(&cell_readiness, "f at 1", |h| usable(h, "f") == 1).await;
    let mut retiring = Instance::join(&hub, "f").await;
    retiring.announce(SegmentPhase::Ready).await;
    health(&cell_readiness, "f at 2", |h| usable(h, "f") == 2).await;
    retiring.announce(SegmentPhase::Retired).await;
    health(&cell_readiness, "f back at 1", |h| usable(h, "f") == 1).await;
    let _loading = Instance::join(&hub, "f").await;
    let mut inside = Instance::join(&hub, "d").await;
    inside.announce(SegmentPhase::Ready).await;
    health(&cell_readiness, "d at 1", |h| {
        usable(h, "d") == 1 && h.state == CellState::Degraded
    })
    .await;

    let mut watch = watching(&hub).await;
    assert_eq!(
        snapshot(&mut watch).await,
        [
            cell(published.epoch, CellState::Degraded),
            CellWatchEvent::Retired {
                segment: "f".into(),
                instance: retiring.id(),
            },
            ready(peer(&hub, serving.id()).await),
            CellWatchEvent::Synced,
        ]
    );
    drop(watch);
    hub.shutdown().await.expect("the hub stops");
}

/// After the snapshot, an entry instance that becomes ready, one that
/// retires, and one that leaves each arrive as their event, in order, as
/// does each change of state; an instance of the segment no other class
/// reaches never appears. A watch opened afterwards holds what the first
/// one has seen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_change_arrives_in_order() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, cell_readiness) = cell_hub(directory.clone(), timing()).await;
    records.next().await;
    let mut retiring = Instance::join(&hub, "f").await;
    retiring.announce(SegmentPhase::Ready).await;
    let mut leaving = Instance::join(&hub, "f").await;
    leaving.announce(SegmentPhase::Ready).await;
    health(&cell_readiness, "f at 2", |h| usable(h, "f") == 2).await;

    let mut watch = watching(&hub).await;
    let mut view = View::of(&snapshot(&mut watch).await);
    assert_eq!(view.state, Some(CellState::Unavailable));
    assert_eq!(
        view.ready.keys().copied().collect::<HashSet<_>>(),
        HashSet::from([retiring.id(), leaving.id()])
    );

    let mut joining = Instance::join(&hub, "f").await;
    joining.announce(SegmentPhase::Ready).await;
    let joined = next(&mut watch).await;
    assert_eq!(joined, ready(peer(&hub, joining.id()).await));
    view.apply(&joined);

    let mut inside = Instance::join(&hub, "d").await;
    inside.announce(SegmentPhase::Ready).await;
    let degraded = next(&mut watch).await;
    assert_eq!(degraded, state(CellState::Degraded), "and no event for d");
    view.apply(&degraded);

    retiring.announce(SegmentPhase::Retired).await;
    let retired = next(&mut watch).await;
    assert_eq!(
        retired,
        CellWatchEvent::Retired {
            segment: "f".into(),
            instance: retiring.id(),
        }
    );
    view.apply(&retired);

    let gone = leaving.id();
    leaving.leave().await;
    let left = next(&mut watch).await;
    assert_eq!(
        left,
        CellWatchEvent::Left {
            segment: "f".into(),
            instance: gone,
        }
    );
    view.apply(&left);

    let mut second = Instance::join(&hub, "d").await;
    second.announce(SegmentPhase::Ready).await;
    let turned = next(&mut watch).await;
    assert_eq!(turned, state(CellState::Ready), "and no event for d");
    view.apply(&turned);

    let mut again = watching(&hub).await;
    assert_eq!(View::of(&snapshot(&mut again).await), view);
    hub.shutdown().await.expect("the hub stops");
}

/// A cell below its quorum is watched with its entry instances as they
/// are; as it becomes degraded and then ready, each change of state
/// arrives, and no instance is sent again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_below_its_quorum_is_watched_with_its_instances() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, cell_readiness) = cell_hub(directory.clone(), timing()).await;
    let published = records.next().await;
    let mut entry = Instance::join(&hub, "f").await;
    entry.announce(SegmentPhase::Ready).await;
    health(&cell_readiness, "f at 1", |h| usable(h, "f") == 1).await;

    let mut watch = watching(&hub).await;
    let entered = ready(peer(&hub, entry.id()).await);
    assert_eq!(
        snapshot(&mut watch).await,
        [
            cell(published.epoch, CellState::Unavailable),
            entered.clone(),
            CellWatchEvent::Synced,
        ]
    );
    let mut inside = Vec::new();
    for _ in 0..2 {
        let mut instance = Instance::join(&hub, "d").await;
        instance.announce(SegmentPhase::Ready).await;
        inside.push(instance);
    }
    assert_eq!(next(&mut watch).await, state(CellState::Degraded));
    assert_eq!(next(&mut watch).await, state(CellState::Ready));

    let mut again = watching(&hub).await;
    assert_eq!(
        snapshot(&mut again).await,
        [
            cell(published.epoch, CellState::Ready),
            entered,
            CellWatchEvent::Synced,
        ]
    );
    // The next event on both is the next change: nothing came between.
    let mut later = Instance::join(&hub, "f").await;
    later.announce(SegmentPhase::Ready).await;
    let joined = ready(peer(&hub, later.id()).await);
    assert_eq!(next(&mut watch).await, joined);
    assert_eq!(next(&mut again).await, joined);
    hub.shutdown().await.expect("the hub stops");
}

const GLOBAL: &str = "global-01-us-east-1";

/// The global cell: its class runs `a`, which no hop enters.
fn global() -> CellSettings {
    toml::from_str(&format!(
        "name = '{GLOBAL}'\nclass = 'global'\nregion = 'us-east-1'\nhub = 'global-01-hub:1337'\n\
         directory = 'memory'\n"
    ))
    .expect("valid test fixture")
}

/// A class that no hop enters has no entry segment: its watch carries the
/// cell's state alone, even with the instances of its class and of other
/// classes coming and going.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_class_no_hop_enters_is_watched_for_its_state_alone() {
    let directory = MemoryDirectory::new();
    let mut records = Records::of(&directory, "global", GLOBAL);
    let (hub, _cell) = hub_of(global(), directory.clone(), timing()).await;
    let published = records.next().await;
    let mut watch = watching(&hub).await;
    assert_eq!(
        snapshot(&mut watch).await,
        [
            CellWatchEvent::Cell {
                name: GLOBAL.into(),
                class: "global".into(),
                epoch: published.epoch,
                state: CellState::Unavailable,
                load: None,
            },
            CellWatchEvent::Synced,
        ]
    );
    // The hub admits a segment of another class; it is not the cell's.
    let mut other = Instance::join(&hub, "f").await;
    other.announce(SegmentPhase::Ready).await;
    let mut instance = Instance::join(&hub, "a").await;
    instance.announce(SegmentPhase::Ready).await;
    assert_eq!(next(&mut watch).await, state(CellState::Degraded));
    assert_eq!(next(&mut watch).await, state(CellState::Ready));
    instance.announce(SegmentPhase::Retired).await;
    assert_eq!(next(&mut watch).await, state(CellState::Unavailable));
    hub.shutdown().await.expect("the hub stops");
}

/// A watcher that does not read while more events than its buffer arrive
/// has its watch ended, past its snapshot; meanwhile another watcher, the
/// health endpoint, and the published record keep up. A watch over a
/// socket would have the kernel's buffers absorb the backlog, so the
/// watcher that does not read is the extension's own, in process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watcher_that_falls_behind_its_buffer_is_ended() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let timing = CellTiming {
        watch_buffer: 8,
        ..timing()
    };
    let (hub, cell_readiness) = cell_hub(directory.clone(), timing).await;
    let published = records.next().await;
    let mut behind = cell_readiness.watch().await.expect("the hub runs");
    let mut watch = watching(&hub).await;
    let mut view = View::of(&snapshot(&mut watch).await);

    // Three events, then nine more: more than the buffer.
    let (_quorum, _) = meet_the_quorum(&hub).await;
    let mut more = Vec::new();
    for _ in 0..9 {
        let mut instance = Instance::join(&hub, "f").await;
        instance.announce(SegmentPhase::Ready).await;
        more.push(instance);
    }
    while view.ready.len() < 10 || view.state != Some(CellState::Ready) {
        view.apply(&next(&mut watch).await);
    }
    health(&cell_readiness, "ready with ten f", |h| {
        h.state == CellState::Ready && usable(h, "f") == 10
    })
    .await;
    records.until(CellState::Ready).await;

    let behind_events = tokio::time::timeout(WAIT, behind.by_ref().collect::<Vec<_>>())
        .await
        .expect("the watch that fell behind ends");
    assert_eq!(
        behind_events,
        [
            cell(published.epoch, CellState::Unavailable),
            CellWatchEvent::Synced
        ]
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A second hub process for the same cell ends nothing of the first's
/// watches by itself, and a watcher of the second sees its later epoch;
/// when the first hub stops, its watches end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_that_stops_ends_its_watches_and_its_successor_has_a_later_epoch() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (first, first_cell) = cell_hub(directory.clone(), timing()).await;
    let before = records.next().await;
    let mut watch = watching(&first).await;
    assert_eq!(
        snapshot(&mut watch).await,
        [
            cell(before.epoch, CellState::Unavailable),
            CellWatchEvent::Synced
        ]
    );

    let (second, _second_cell) = cell_hub(directory.clone(), timing()).await;
    let after = loop {
        let record = records.next().await;
        if record.epoch != before.epoch {
            break record;
        }
    };
    assert!(after.epoch > before.epoch);
    let mut successor = watching(&second).await;
    assert_eq!(
        snapshot(&mut successor).await,
        [
            cell(after.epoch, CellState::Unavailable),
            CellWatchEvent::Synced
        ]
    );

    // The first hub's watch goes on.
    let mut entry = Instance::join(&first, "f").await;
    entry.announce(SegmentPhase::Ready).await;
    assert_eq!(
        next(&mut watch).await,
        ready(peer(&first, entry.id()).await)
    );
    health(&first_cell, "f at 1", |h| usable(h, "f") == 1).await;

    first.shutdown().await.expect("the hub stops");
    ends(&mut watch).await;
    second.shutdown().await.expect("the hub stops");
    ends(&mut successor).await;
}

/// Dropping a watch releases what the hub holds for it, in process and
/// over a connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_watch_releases_it() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, cell_readiness) = cell_hub(directory.clone(), timing()).await;
    records.next().await;
    let mut watchers = cell_readiness.watchers();
    let mut count = async |n: usize| {
        tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching == n))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {n} watchers"))
            .expect("the extension lives");
    };
    count(0).await;
    let inside = cell_readiness.watch().await.expect("the hub runs");
    count(1).await;
    let mut outside = watching(&hub).await;
    snapshot(&mut outside).await;
    count(2).await;
    drop(inside);
    count(1).await;
    drop(outside);
    count(0).await;
    hub.shutdown().await.expect("the hub stops");
}

/// A watch that arrives before the hub has stored its cell's record waits
/// for it, in process and over a connection, since until then the epoch it
/// would send is not settled; then it carries the record's epoch. The hub
/// counts a waiting watch among its watchers, which is how the test knows
/// the hub holds the request while the write is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watch_waits_for_the_cells_first_record() {
    let gate = Gated::new();
    let mut records = Records::new(&gate.inner);
    gate.close();
    let (hub, cell_readiness) = hub_of(settings(), gate.clone(), timing()).await;
    gate.holding().await;
    let mut watchers = cell_readiness.watchers();
    let mut count = async |n: usize, what: &str| {
        tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching == n))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
            .expect("the extension lives");
    };
    let mut inside = Box::pin(cell_readiness.watch());
    assert!(
        futures::poll!(inside.as_mut()).is_pending(),
        "no epoch while the write is held"
    );
    count(1, "the watch in process to wait").await;
    let address = address(&hub);
    let outside = tokio::spawn(async move {
        let watcher = CellWatcher::new().expect("a client");
        watcher.watch(&address).await.map(|events| {
            events.map(|item| item.event().expect("an event of a kind this build knows"))
        })
    });
    count(2, "the hub to hold the request").await;
    gate.open();
    let published = records.next().await;
    let expected = [
        cell(published.epoch, CellState::Unavailable),
        CellWatchEvent::Synced,
    ];
    let mut inside = tokio::time::timeout(WAIT, inside)
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    assert_eq!(snapshot(&mut inside).await, expected);
    let mut outside = tokio::time::timeout(WAIT, outside)
        .await
        .expect("the hub answers in time")
        .expect("the task runs")
        .expect("the hub serves the watch");
    assert_eq!(snapshot(&mut outside).await, expected);
    hub.shutdown().await.expect("the hub stops");
}

/// A hub without a cell serves no watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_without_a_cell_serves_no_watch() {
    let hub = Hub::builder(config())
        .extension(GraphAdmission::new(common::spec()))
        .serve()
        .await
        .expect("the hub serves");
    let watcher = CellWatcher::new().expect("a client");
    let Err(error) = watcher.watch(&address(&hub)).await else {
        panic!("watched a hub without a cell");
    };
    assert!(error.to_string().contains("404"), "{error}");
    hub.shutdown().await.expect("the hub stops");
}
