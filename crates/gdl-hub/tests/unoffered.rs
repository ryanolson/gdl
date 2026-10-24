//! A global cell's hub marks a followed cell not offered, with a new
//! revision, when its link says it takes no work or the link is lost, and
//! closes the follow only once every chooser of the cell's class among its
//! members has acknowledged that revision while it is still the follow's
//! current one. In the deployment of these tests, `global` runs `a`, whose
//! instances are the choosers of `model_x`; a test's choosers are members
//! of the global's hub that acknowledge as the test says. Every hub runs
//! in process on OS-assigned ports, sharing one directory; every wait is
//! for something observed.

mod common;

use common::{
    CELL, Instance, Records, Resolving, WAIT, global_requiring, health, meet_the_quorum, model,
    serve_cell, timing,
};
use futures::{FutureExt, Stream, StreamExt};
use gdl_cells::CellState;
use gdl_hub::{CellReadiness, FollowsWatch};
use gdl_model::SegmentPhase;
use gdl_watch::{FollowEvent, Unoffered};
use velo_hub::Hub;

async fn next(watch: &mut (impl Stream<Item = FollowEvent> + Unpin)) -> FollowEvent {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("an event in time")
        .expect("the watch goes on")
}

/// The events the watch has now. The hub applies an acknowledgement and
/// sends what it changes before it answers, so once one is answered, what
/// it closed is here.
fn pending(watch: &mut (impl Stream<Item = FollowEvent> + Unpin)) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        events.push(event);
    }
    events
}

/// Read the relay up to and including the first event `last` takes, and
/// return what was read.
async fn through(
    watch: &mut (impl Stream<Item = FollowEvent> + Unpin),
    last: impl Fn(&FollowEvent) -> bool,
) -> Vec<FollowEvent> {
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

fn offering(generation: u64, revision: u64, offered: bool) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        cell: CELL.into(),
        generation,
        revision,
        offered,
    }
}

fn dropped(generation: u64) -> FollowEvent {
    FollowEvent::Dropped {
        class: "model_x".into(),
        cell: CELL.into(),
        generation,
    }
}

fn unoffered(generation: u64, revision: u64) -> Unoffered {
    Unoffered {
        class: "model_x".into(),
        cell: CELL.into(),
        generation,
        revision,
    }
}

/// Whether `events` change the membership of the follow's instances: a
/// retirement, or an instance that left.
fn membership(events: &[FollowEvent]) -> Vec<&FollowEvent> {
    events
        .iter()
        .filter(|event| {
            matches!(
                event,
                FollowEvent::Retired { .. } | FollowEvent::Left { .. }
            )
        })
        .collect()
}

/// A model cell at its quorum, followed by a global cell: the hubs, the
/// model cell's instances (`f`, then two of `d`), the global's extension,
/// a watch of its relay past the follow's install, and the follow's
/// generation.
struct Followed {
    model_hub: Hub,
    model_cell: std::sync::Arc<CellReadiness<Resolving>>,
    instances: Vec<Instance>,
    global_hub: Hub,
    global_cell: std::sync::Arc<CellReadiness<Resolving>>,
    watch: FollowsWatch,
    generation: u64,
}

async fn followed() -> Followed {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, model_cell) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    records.until(CellState::Ready).await;
    let (global_hub, global_cell) = serve_cell(global_requiring(0), &directory, timing()).await;
    let mut watch = global_cell.follows();
    let entry = instances[0].id();
    let installed = through(
        &mut watch,
        |event| matches!(event, FollowEvent::Exposed { peer, .. } if peer.instance_id() == entry),
    )
    .await;
    let generation = installed
        .iter()
        .find_map(|event| match event {
            FollowEvent::Follow { generation, .. } => Some(*generation),
            _ => None,
        })
        .expect("the follow was installed");
    Followed {
        model_hub,
        model_cell,
        instances,
        global_hub,
        global_cell,
        watch,
        generation,
    }
}

impl Followed {
    /// Whether the global's hub still follows the model cell in the
    /// follow's generation.
    fn follows(&self) -> bool {
        self.global_cell
            .following()
            .borrow()
            .get(&("model_x".to_string(), CELL.to_string()))
            == Some(&self.generation)
    }

    /// Take the model cell's `d` instances out of `ready`, and wait until
    /// the relay says the follow is not offered: the cell is `unavailable`.
    async fn unavailable(&mut self) -> Vec<FollowEvent> {
        for instance in &mut self.instances[1..] {
            instance.withdraw(SegmentPhase::Ready).await;
        }
        through(&mut self.watch, |event| {
            matches!(event, FollowEvent::Offering { offered: false, .. })
        })
        .await
    }

    /// Put the model cell's `d` instances back in `ready`, and wait until
    /// the relay says the follow is offered and the cell `ready`.
    async fn ready(&mut self) -> Vec<FollowEvent> {
        for instance in &mut self.instances[1..] {
            instance.announce(SegmentPhase::Ready).await;
        }
        health(&self.model_cell, "the model cell ready", |h| {
            h.state == CellState::Ready
        })
        .await;
        let generation = self.generation;
        through(&mut self.watch, move |event| {
            *event
                == FollowEvent::State {
                    class: "model_x".into(),
                    cell: CELL.into(),
                    generation,
                    state: CellState::Ready,
                }
        })
        .await
    }

    async fn stop(self) {
        self.global_hub.shutdown().await.expect("the hub stops");
        self.model_hub.shutdown().await.expect("the hub stops");
    }
}

/// A followed cell that drains is marked not offered with the follow's
/// next revision, and its follow closes once every chooser has
/// acknowledged that revision: one that joined after the mark too, since
/// it is a member of the hub when the follow would close. Then the global
/// no longer follows the cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_cell_closes_once_every_chooser_acknowledged() {
    let mut cells = followed().await;
    let generation = cells.generation;
    let first = Instance::chooser(&cells.global_hub).await;

    cells.model_cell.drain().await.expect("the cell drains");
    let marked = through(&mut cells.watch, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    let second = Instance::chooser(&cells.global_hub).await;

    first
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(pending(&mut cells.watch), [], "closed before the second");
    assert!(cells.follows());
    second
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(next(&mut cells.watch).await, dropped(generation));
    let mut following = cells.global_cell.following();
    tokio::time::timeout(
        WAIT,
        following.wait_for(|cells| !cells.contains_key(&("model_x".to_string(), CELL.to_string()))),
    )
    .await
    .expect("the global let go of the cell in time")
    .expect("the extension runs");
    cells.stop().await;
}

/// A cell marked not offered and then usable again is offered again at
/// the next revision, and an acknowledgement of the earlier revision then
/// closes nothing: the follow, and what the relay says of the cell's
/// instances, go on as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_offered_again_is_not_closed_by_an_earlier_acknowledgement() {
    let mut cells = followed().await;
    let generation = cells.generation;
    let chooser = Instance::chooser(&cells.global_hub).await;

    let marked = cells.unavailable().await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    let again = cells.ready().await;
    assert!(
        again.contains(&offering(generation, 3, true)),
        "not offered again: {again:?}"
    );
    chooser
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(pending(&mut cells.watch), [], "the follow changed");
    assert!(cells.follows(), "the follow closed");
    assert_eq!(
        membership(&[marked, again].concat()),
        Vec::<&FollowEvent>::new(),
        "the cell's instances changed"
    );
    cells.stop().await;
}

/// Not offered at `r`, offered at `r + 1`, not offered at `r + 2`: a
/// chooser that acknowledged `r` and has not acknowledged since has not
/// acknowledged `r + 2`, so the follow does not close until it does, even
/// once every other chooser has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_acknowledgement_answers_only_the_revision_it_names() {
    let mut cells = followed().await;
    let generation = cells.generation;
    let (late, prompt) = (
        Instance::chooser(&cells.global_hub).await,
        Instance::chooser(&cells.global_hub).await,
    );

    let marked = cells.unavailable().await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    late.acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    cells.ready().await;
    let marked = cells.unavailable().await;
    assert_eq!(marked.last(), Some(&offering(generation, 4, false)));
    prompt
        .acknowledge(&unoffered(generation, 4))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(
        pending(&mut cells.watch),
        [],
        "closed on an acknowledgement of an earlier revision"
    );
    assert!(cells.follows());
    late.acknowledge(&unoffered(generation, 4))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(next(&mut cells.watch).await, dropped(generation));
    cells.stop().await;
}

/// A chooser that leaves counts as one that acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chooser_that_leaves_counts_as_acknowledged() {
    let mut cells = followed().await;
    let generation = cells.generation;
    let (leaving, staying) = (
        Instance::chooser(&cells.global_hub).await,
        Instance::chooser(&cells.global_hub).await,
    );
    cells.unavailable().await;
    staying
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(pending(&mut cells.watch), []);
    leaving.leave().await;
    assert_eq!(next(&mut cells.watch).await, dropped(generation));
    cells.stop().await;
}

/// A follow whose link is lost is marked not offered at once, and is kept
/// beside the follows that come after it until every chooser has
/// acknowledged it: the global follows the cell again meanwhile, since its
/// record still says it takes work, and the earlier follow closes on its
/// own acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_link_is_not_offered_at_once_and_closes_once_acknowledged() {
    let cells = followed().await;
    let generation = cells.generation;
    let chooser = Instance::chooser(&cells.global_hub).await;
    let mut following = cells.global_cell.following();
    let key = ("model_x".to_string(), CELL.to_string());

    let Followed {
        model_hub,
        global_hub,
        mut watch,
        ..
    } = cells;
    model_hub.shutdown().await.expect("the hub stops");
    let marked = through(&mut watch, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    tokio::time::timeout(
        WAIT,
        following.wait_for(|cells| cells.get(&key).is_some_and(|g| *g > generation)),
    )
    .await
    .expect("followed again in time")
    .expect("the extension runs");
    assert_eq!(pending(&mut watch), [], "the lost follow closed");

    chooser
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(next(&mut watch).await, dropped(generation));
    // A watch begun now holds nothing of the lost follow.
    let mut again = global_hub_relay(&global_hub);
    let snapshot = through(&mut again, |event| *event == FollowEvent::Synced).await;
    assert_eq!(snapshot, [FollowEvent::Synced]);
    global_hub.shutdown().await.expect("the hub stops");
}

/// A watch of the relay through the hub's client.
fn global_hub_relay(hub: &Hub) -> impl Stream<Item = FollowEvent> + Unpin {
    let watcher = gdl_watch::CellWatcher::new().expect("a client");
    let address = format!("127.0.0.1:{}", hub.public_addr().port());
    futures::stream::once(async move {
        tokio::time::timeout(WAIT, watcher.follows(&address))
            .await
            .expect("the hub answers in time")
            .expect("the hub serves the relay")
    })
    .flatten()
    .boxed()
}

/// A follow closed because its cell took no work is followed again from
/// the first later write of the cell's record that makes it a home cell,
/// also one written while the follow's link waits out its delay before a
/// follow: the cell recovers within the global's 2 s delay, and its hub
/// writes nothing more for the length of the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_that_recovers_as_its_follow_closes_is_followed_again() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, _model_cell) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (mut instances, _) = meet_the_quorum(&model_hub).await;
    records.until(CellState::Ready).await;
    let slow = gdl_hub::CellTiming {
        refollow: std::time::Duration::from_secs(2),
        ..timing()
    };
    let (global_hub, global_cell) = serve_cell(global_requiring(0), &directory, slow).await;
    let key = ("model_x".to_string(), CELL.to_string());
    let mut following = global_cell.following();
    let first = *tokio::time::timeout(WAIT, following.wait_for(|cells| cells.contains_key(&key)))
        .await
        .expect("followed in time")
        .expect("the extension runs")
        .get(&key)
        .expect("followed");
    let mut watch = global_cell.follows();
    through(&mut watch, |event| *event == FollowEvent::Synced).await;

    for instance in &mut instances[1..] {
        instance.withdraw(SegmentPhase::Ready).await;
    }
    // With no chooser to wait for, the follow closes as it is marked.
    through(&mut watch, |event| *event == dropped(first)).await;
    for instance in &mut instances[1..] {
        instance.announce(SegmentPhase::Ready).await;
    }
    records.until(CellState::Ready).await;
    tokio::time::timeout(
        WAIT,
        following.wait_for(|cells| cells.get(&key).is_some_and(|g| *g > first)),
    )
    .await
    .expect("followed again in time")
    .expect("the extension runs");
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A member of a chooser's segment counts as a chooser that must
/// acknowledge only once it says it runs a follower, which an instance
/// without `[cell]` never does: such a member does not keep a follow from
/// closing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_that_runs_no_follower_holds_no_follow() {
    let mut cells = followed().await;
    let generation = cells.generation;
    let _member = Instance::join(&cells.global_hub, "a").await;
    cells.model_cell.drain().await.expect("the cell drains");
    let marked = through(&mut cells.watch, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    assert_eq!(next(&mut cells.watch).await, dropped(generation));
    cells.stop().await;
}
