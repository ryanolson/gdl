//! A cell's hub knows which cells follow it, from the watches their hubs
//! hold open, each of which names its follower; a draining cell tells its
//! watchers once no cell follows it any more, which is when no chooser of
//! another cell can still hold an offer to it. A global that starts to
//! follow a cell that is already draining never offers it, and lets go at
//! once. Every hub runs in process on OS-assigned ports, sharing one
//! directory; every wait is for something observed.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::{
    CELL, GLOBAL, Instance, Records, Resolving, WAIT, global_requiring, health, meet_the_quorum,
    model, serve_cell, timing,
};
use futures::{FutureExt, Stream, StreamExt};
use gdl_cells::{CellDirectory, CellRecord, CellState, DirectoryError, MemoryWatch, ResumeToken};
use gdl_hub::{CellReadiness, CellWatch};
use gdl_watch::{CellWatchEvent, FollowEvent, Unoffered};
use tokio::sync::watch;

async fn next<T>(watch: &mut (impl Stream<Item = T> + Unpin)) -> T {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("an event in time")
        .expect("the watch goes on")
}

/// Read up to and including the first event `last` takes, and return what
/// was read.
async fn through<T>(
    watch: &mut (impl Stream<Item = T> + Unpin),
    last: impl Fn(&T) -> bool,
) -> Vec<T> {
    let mut events = Vec::new();
    loop {
        let event = next(watch).await;
        let done = last(&event);
        events.push(event);
        if done {
            return events;
        }
    }
}

/// The events the watch has now.
fn pending<T>(watch: &mut (impl Stream<Item = T> + Unpin)) -> Vec<T> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        events.push(event);
    }
    events
}

/// A watch of the model cell past its snapshot.
async fn watching<D: CellDirectory>(cell: &CellReadiness<D>) -> CellWatch {
    let mut watch = cell.watch().await.expect("the watch begins");
    through(&mut watch, |event| *event == CellWatchEvent::Synced).await;
    watch
}

fn global() -> (String, String) {
    ("global".to_string(), GLOBAL.to_string())
}

/// Wait until the model cell's followers are `expected`.
async fn followers<D: CellDirectory>(
    cell: &CellReadiness<D>,
    expected: BTreeSet<(String, String)>,
) {
    let mut followers = cell.followers();
    let waited = tokio::time::timeout(WAIT, followers.wait_for(|now| *now == expected))
        .await
        .is_ok();
    assert!(
        waited,
        "timed out waiting for the followers {expected:?}: {:?}",
        *followers.borrow()
    );
}

/// The follow `generation` of the model cell acknowledged at `revision`.
fn unoffered(generation: u64, revision: u64) -> Unoffered {
    Unoffered {
        class: "model_x".into(),
        cell: CELL.into(),
        generation,
        revision,
    }
}

/// A draining cell that a global follows tells its watchers nothing until
/// the global lets go of it, once its chooser acknowledged that it holds no
/// offer to it: then the cell has no follower, and says so once, and a
/// watch begun later says so after the cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_cell_says_so_once_its_last_follower_lets_go() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, model_cell) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    records.until(CellState::Ready).await;
    // The global sees the directory only once its chooser is a member, so
    // that the follow waits for the chooser's acknowledgement.
    let unseen = Unseen {
        inner: directory.clone(),
        open: Arc::new(watch::Sender::new(false)),
    };
    let global_settings = global_requiring(0);
    let name = global_settings.hub.clone().expect("a hub name");
    let (global_hub, global_cell) = common::hub_of(global_settings, unseen.clone(), timing()).await;
    directory.resolve(&name, &global_hub);
    let chooser = Instance::join(&global_hub, "a").await;
    let mut relay = global_cell.follows();
    unseen.open.send_replace(true);
    let installed = through(&mut relay, |event| {
        matches!(event, FollowEvent::Follow { .. })
    })
    .await;
    let Some(FollowEvent::Follow { generation, .. }) = installed.last() else {
        unreachable!("a follow");
    };
    let generation = *generation;
    followers(&model_cell, [global()].into()).await;
    let mut watch = watching(&model_cell).await;

    model_cell.drain().await.expect("the cell drains");
    through(&mut relay, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    let drained = through(&mut watch, |event| {
        *event
            == CellWatchEvent::State {
                state: CellState::Draining,
            }
    })
    .await;
    assert!(!drained.contains(&CellWatchEvent::Unfollowed));
    assert!(
        !pending(&mut watch).contains(&CellWatchEvent::Unfollowed),
        "unfollowed while a global follows the cell"
    );

    chooser
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    followers(&model_cell, BTreeSet::new()).await;
    through(&mut watch, |event| *event == CellWatchEvent::Unfollowed).await;
    let mut later = model_cell.watch().await.expect("the watch begins");
    let snapshot = through(&mut later, |event| *event == CellWatchEvent::Synced).await;
    assert!(
        matches!(
            snapshot[..2],
            [
                CellWatchEvent::Cell {
                    state: CellState::Draining,
                    ..
                },
                CellWatchEvent::Unfollowed
            ]
        ),
        "{snapshot:?}"
    );
    assert_eq!(
        snapshot
            .iter()
            .filter(|event| **event == CellWatchEvent::Unfollowed)
            .count(),
        1
    );
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A cell that no cell follows says so as soon as it drains.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_no_one_follows_says_so_as_it_drains() {
    let directory = Resolving::new();
    let (model_hub, model_cell) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    // Members, so that the drain does not end, and its watches with it,
    // as it begins.
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    let mut watch = watching(&model_cell).await;
    model_cell.drain().await.expect("the cell drains");
    let drained = through(&mut watch, |event| *event == CellWatchEvent::Unfollowed).await;
    assert!(
        drained.contains(&CellWatchEvent::State {
            state: CellState::Draining
        }),
        "{drained:?}"
    );
    model_hub.shutdown().await.expect("the hub stops");
}

/// A directory whose writes a test can hold: the record a hub publishes
/// while the gate is shut is stored only once it opens, so that readers
/// see the cell as it was.
#[derive(Clone)]
struct Lagging {
    inner: Resolving,
    open: Arc<watch::Sender<bool>>,
}

impl CellDirectory for Lagging {
    type Watch = MemoryWatch;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        let mut open = self.open.subscribe();
        let _ = open.wait_for(|open| *open).await;
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: gdl_cells::Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.inner.watch(class, from)
    }
}

/// A directory whose watches a test can hold: a watch begins only once
/// the gate opens, so that a hub learns of the cells in it only then.
#[derive(Clone)]
struct Unseen {
    inner: Resolving,
    open: Arc<watch::Sender<bool>>,
}

impl CellDirectory for Unseen {
    type Watch = futures::stream::BoxStream<'static, Result<gdl_cells::CellEvent, DirectoryError>>;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: gdl_cells::Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch {
        let (inner, class) = (self.inner.clone(), class.to_string());
        let mut open = self.open.subscribe();
        futures::stream::once(async move {
            let _ = open.wait_for(|open| *open).await;
            inner.watch(&class, from)
        })
        .flatten()
        .boxed()
    }
}

/// A global that starts to follow a cell that is already draining, whose
/// record still says it is ready, installs the follow not offered, never
/// exposes an instance of it, and lets go of it as soon as its chooser
/// acknowledges; the cell sees its follower come and go. The cell said
/// that no cell follows it as it began to drain, since none did, and does
/// not say so again. The global does not follow the cell again while its
/// record is the one it followed it from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_follow_of_a_draining_cell_is_never_offered_and_let_go_at_once() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let lagging = Lagging {
        inner: directory.clone(),
        open: Arc::new(watch::Sender::new(true)),
    };
    let (model_hub, model_cell) = common::hub_of(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        lagging.clone(),
        timing(),
    )
    .await;
    directory.resolve("model-x-01-hub:1337", &model_hub);
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    records.until(CellState::Ready).await;
    lagging.open.send_replace(false);
    let draining = Arc::clone(&model_cell);
    let drain = tokio::spawn(async move { draining.drain().await });
    health(&model_cell, "the cell draining", |h| {
        h.state == CellState::Draining
    })
    .await;
    // No cell followed it as it began to drain, so it said so then.
    let mut watch = model_cell.watch().await.expect("the watch begins");
    let snapshot = through(&mut watch, |event| *event == CellWatchEvent::Synced).await;
    assert_eq!(
        snapshot.get(1),
        Some(&CellWatchEvent::Unfollowed),
        "{snapshot:?}"
    );

    // The global sees the directory only once its chooser is a member, so
    // that the follow waits for the chooser's acknowledgement.
    let unseen = Unseen {
        inner: directory.clone(),
        open: Arc::new(watch::Sender::new(false)),
    };
    let global_settings = global_requiring(0);
    let name = global_settings.hub.clone().expect("a hub name");
    let (global_hub, global_cell) = common::hub_of(global_settings, unseen.clone(), timing()).await;
    directory.resolve(&name, &global_hub);
    let chooser = Instance::join(&global_hub, "a").await;
    let mut relay = global_cell.follows();
    unseen.open.send_replace(true);
    let installed = through(&mut relay, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    let generation = match &installed[installed.len() - 2..] {
        [
            FollowEvent::Follow {
                generation,
                state: CellState::Draining,
                ..
            },
            FollowEvent::Offering {
                revision: 1,
                offered: false,
                ..
            },
        ] => *generation,
        other => panic!("not installed not offered: {other:?}"),
    };
    followers(&model_cell, [global()].into()).await;
    chooser
        .acknowledge(&unoffered(generation, 1))
        .await
        .expect("the hub takes the acknowledgement");
    let closed = through(&mut relay, |event| {
        matches!(event, FollowEvent::Dropped { .. })
    })
    .await;
    assert!(
        !closed.iter().any(|event| matches!(
            event,
            FollowEvent::Exposed { .. } | FollowEvent::Offering { offered: true, .. }
        )),
        "offered: {closed:?}"
    );
    followers(&model_cell, BTreeSet::new()).await;
    assert!(
        !pending(&mut watch).contains(&CellWatchEvent::Unfollowed),
        "said twice"
    );
    let mut following = global_cell.following();
    let key = ("model_x".to_string(), CELL.to_string());
    tokio::time::timeout(WAIT, following.wait_for(|cells| !cells.contains_key(&key)))
        .await
        .expect("let go of in time")
        .expect("the extension runs");
    // Followed again, it would be within the global's wait before a
    // follow, 250 ms; it is given several times that.
    let again = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        following.wait_for(|cells| cells.contains_key(&key)),
    );
    assert!(
        again.await.is_err(),
        "followed again from the record it was followed from"
    );

    lagging.open.send_replace(true);
    records.until(CellState::Draining).await;
    drain
        .await
        .expect("the drain's task")
        .expect("the cell drains");
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}
