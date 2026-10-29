//! A global pool's hub marks a followed pool not offered, with a new
//! revision, when its link says it takes no work or the link is lost, and
//! closes the follow only once every chooser of the pool's class among its
//! members has acknowledged that revision while it is still the follow's
//! current one. In the deployment of these tests, `global` runs `a`, whose
//! instances are the choosers of `model_x`; a test's choosers are members
//! of the global's hub that acknowledge as the test says. Every hub runs
//! in process on OS-assigned ports, sharing one directory; every wait is
//! for something observed.

mod common;

use common::{
    Instance, POOL, Records, Resolving, WAIT, global_default, health, meet_the_quorum, model,
    serve_pool, timing,
};
use std::time::{Duration, Instant};

use futures::{FutureExt, Stream, StreamExt};
use gdl_hub::{FollowsWatch, PoolReadiness};
use gdl_model::SegmentPhase;
use gdl_pools::PoolState;
use gdl_watch::{FollowEvent, Unoffered};
use velo_hub::Hub;

async fn next(watch: &mut (impl Stream<Item = FollowEvent> + Unpin)) -> FollowEvent {
    loop {
        let event = tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("an event in time")
            .expect("the watch goes on");
        if let Some(event) = common::plain_follow(event) {
            return event;
        }
    }
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
        pool: POOL.into(),
        generation,
        revision,
        offered,
    }
}

fn dropped(generation: u64) -> FollowEvent {
    FollowEvent::Dropped {
        class: "model_x".into(),
        pool: POOL.into(),
        generation,
    }
}

fn unoffered(generation: u64, revision: u64) -> Unoffered {
    Unoffered {
        class: "model_x".into(),
        pool: POOL.into(),
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

/// A model pool at its quorum, followed by a global pool: the hubs, the
/// model pool's instances (`f`, then two of `d`), the global's extension,
/// a watch of its relay past the follow's install, and the follow's
/// generation.
struct Followed {
    model_hub: Hub,
    model_pool: std::sync::Arc<PoolReadiness<Resolving>>,
    instances: Vec<Instance>,
    global_hub: Hub,
    global_pool: std::sync::Arc<PoolReadiness<Resolving>>,
    watch: FollowsWatch,
    generation: u64,
}

async fn followed() -> Followed {
    followed_paced(timing()).await
}

/// [`followed`], the global's hub paced by `global`.
async fn followed_paced(global: gdl_hub::PoolTiming) -> Followed {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    records.until(PoolState::Ready).await;
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, global).await;
    let mut watch = global_pool.follows();
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
        model_pool,
        instances,
        global_hub,
        global_pool,
        watch,
        generation,
    }
}

impl Followed {
    /// Whether the global's hub still follows the model pool in the
    /// follow's generation.
    fn follows(&self) -> bool {
        self.global_pool
            .following()
            .borrow()
            .get(&("model_x".to_string(), POOL.to_string()))
            == Some(&self.generation)
    }

    /// Take the model pool's `d` instances out of `ready`, and wait until
    /// the relay says the follow is not offered: the pool is `unavailable`.
    async fn unavailable(&mut self) -> Vec<FollowEvent> {
        for instance in &mut self.instances[1..] {
            instance.withdraw(SegmentPhase::Ready).await;
        }
        through(&mut self.watch, |event| {
            matches!(event, FollowEvent::Offering { offered: false, .. })
        })
        .await
    }

    /// Put the model pool's `d` instances back in `ready`, and wait until
    /// the relay says the follow is offered and the pool `ready`.
    async fn ready(&mut self) -> Vec<FollowEvent> {
        for instance in &mut self.instances[1..] {
            instance.announce(SegmentPhase::Ready).await;
        }
        health(&self.model_pool, "the model pool ready", |h| {
            h.state == PoolState::Ready
        })
        .await;
        let generation = self.generation;
        through(&mut self.watch, move |event| {
            *event
                == FollowEvent::State {
                    class: "model_x".into(),
                    pool: POOL.into(),
                    generation,
                    state: PoolState::Ready,
                }
        })
        .await
    }

    async fn stop(self) {
        self.global_hub.shutdown().await.expect("the hub stops");
        self.model_hub.shutdown().await.expect("the hub stops");
    }
}

/// A followed pool that drains is marked not offered with the follow's
/// next revision, and its follow closes once every chooser has
/// acknowledged that revision: one that joined after the mark too, since
/// it is a member of the hub when the follow would close. Then the global
/// no longer follows the pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_pool_closes_once_every_chooser_acknowledged() {
    let mut pools = followed().await;
    let generation = pools.generation;
    let first = Instance::chooser(&pools.global_hub).await;

    pools.model_pool.drain().await.expect("the pool drains");
    let marked = through(&mut pools.watch, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    let second = Instance::chooser(&pools.global_hub).await;

    first
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(pending(&mut pools.watch), [], "closed before the second");
    assert!(pools.follows());
    second
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(next(&mut pools.watch).await, dropped(generation));
    let mut following = pools.global_pool.following();
    tokio::time::timeout(
        WAIT,
        following.wait_for(|pools| !pools.contains_key(&("model_x".to_string(), POOL.to_string()))),
    )
    .await
    .expect("the global let go of the pool in time")
    .expect("the extension runs");
    pools.stop().await;
}

/// A pool marked not offered and then usable again is offered again at
/// the next revision, and an acknowledgement of the earlier revision then
/// closes nothing: the follow, and what the relay says of the pool's
/// instances, go on as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_offered_again_is_not_closed_by_an_earlier_acknowledgement() {
    let mut pools = followed().await;
    let generation = pools.generation;
    let chooser = Instance::chooser(&pools.global_hub).await;

    let marked = pools.unavailable().await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    let again = pools.ready().await;
    assert!(
        again.contains(&offering(generation, 3, true)),
        "not offered again: {again:?}"
    );
    chooser
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(pending(&mut pools.watch), [], "the follow changed");
    assert!(pools.follows(), "the follow closed");
    assert_eq!(
        membership(&[marked, again].concat()),
        Vec::<&FollowEvent>::new(),
        "the pool's instances changed"
    );
    pools.stop().await;
}

/// Not offered at `r`, offered at `r + 1`, not offered at `r + 2`: a
/// chooser that acknowledged `r` and has not acknowledged since has not
/// acknowledged `r + 2`, so the follow does not close until it does, even
/// once every other chooser has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_acknowledgement_answers_only_the_revision_it_names() {
    let mut pools = followed().await;
    let generation = pools.generation;
    let (late, prompt) = (
        Instance::chooser(&pools.global_hub).await,
        Instance::chooser(&pools.global_hub).await,
    );

    let marked = pools.unavailable().await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    late.acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    pools.ready().await;
    let marked = pools.unavailable().await;
    assert_eq!(marked.last(), Some(&offering(generation, 4, false)));
    prompt
        .acknowledge(&unoffered(generation, 4))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(
        pending(&mut pools.watch),
        [],
        "closed on an acknowledgement of an earlier revision"
    );
    assert!(pools.follows());
    late.acknowledge(&unoffered(generation, 4))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(next(&mut pools.watch).await, dropped(generation));
    pools.stop().await;
}

/// A chooser that leaves counts as one that acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chooser_that_leaves_counts_as_acknowledged() {
    let mut pools = followed().await;
    let generation = pools.generation;
    let (leaving, staying) = (
        Instance::chooser(&pools.global_hub).await,
        Instance::chooser(&pools.global_hub).await,
    );
    pools.unavailable().await;
    staying
        .acknowledge(&unoffered(generation, 2))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(pending(&mut pools.watch), []);
    leaving.leave().await;
    assert_eq!(next(&mut pools.watch).await, dropped(generation));
    pools.stop().await;
}

/// A follow whose link is lost is marked not offered at once, and is kept
/// beside the follows that come after it until every chooser has
/// acknowledged it: the global follows the pool again meanwhile, since its
/// record still says it takes work, and the earlier follow closes on its
/// own acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_link_is_not_offered_at_once_and_closes_once_acknowledged() {
    let pools = followed().await;
    let generation = pools.generation;
    let chooser = Instance::chooser(&pools.global_hub).await;
    let mut following = pools.global_pool.following();
    let key = ("model_x".to_string(), POOL.to_string());

    let Followed {
        model_hub,
        global_hub,
        mut watch,
        ..
    } = pools;
    model_hub.shutdown().await.expect("the hub stops");
    let marked = through(&mut watch, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    tokio::time::timeout(
        WAIT,
        following.wait_for(|pools| pools.get(&key).is_some_and(|g| *g > generation)),
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
    let snapshot = through(&mut again, |event| {
        matches!(event, FollowEvent::Synced { .. })
    })
    .await;
    assert_eq!(snapshot, [FollowEvent::Synced { listed: false }]);
    global_hub.shutdown().await.expect("the hub stops");
}

/// A watch of the relay through the hub's client.
fn global_hub_relay(hub: &Hub) -> impl Stream<Item = FollowEvent> + Unpin {
    let watcher = gdl_watch::PoolWatcher::new().expect("a client");
    let address = format!("127.0.0.1:{}", hub.public_addr().port());
    futures::stream::once(async move {
        tokio::time::timeout(WAIT, watcher.follows(&address, None))
            .await
            .expect("the hub answers in time")
            .expect("the hub serves the relay")
    })
    .flatten()
    // A relay of this build sends no kind this build does not know.
    .map(|item| {
        item.expect("the relay's watch does not fail")
            .event()
            .expect("an event of a known kind")
    })
    .boxed()
}

/// A follow closed because its pool took no work is followed again from
/// the first later write of the pool's record that makes it a child that
/// takes work, also one written while the follow's link waits out its
/// delay before a follow: the pool recovers within the global's 2 s delay, and its hub
/// writes nothing more for the length of the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_that_recovers_as_its_follow_closes_is_followed_again() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, _model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (mut instances, _) = meet_the_quorum(&model_hub).await;
    records.until(PoolState::Ready).await;
    let slow = gdl_hub::PoolTiming {
        refollow: std::time::Duration::from_secs(2),
        ..timing()
    };
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, slow).await;
    let key = ("model_x".to_string(), POOL.to_string());
    let mut following = global_pool.following();
    let first = *tokio::time::timeout(WAIT, following.wait_for(|pools| pools.contains_key(&key)))
        .await
        .expect("followed in time")
        .expect("the extension runs")
        .get(&key)
        .expect("followed");
    let mut watch = global_pool.follows();
    through(&mut watch, |event| {
        matches!(event, FollowEvent::Synced { .. })
    })
    .await;

    for instance in &mut instances[1..] {
        instance.withdraw(SegmentPhase::Ready).await;
    }
    // With no chooser to wait for, the follow closes as it is marked.
    through(&mut watch, |event| *event == dropped(first)).await;
    for instance in &mut instances[1..] {
        instance.announce(SegmentPhase::Ready).await;
    }
    records.until(PoolState::Ready).await;
    tokio::time::timeout(
        WAIT,
        following.wait_for(|pools| pools.get(&key).is_some_and(|g| *g > first)),
    )
    .await
    .expect("followed again in time")
    .expect("the extension runs");
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A member of a chooser's segment counts as a chooser that must
/// acknowledge only once it says it runs a follower, which an instance
/// without `[pool]` never does: such a member does not keep a follow from
/// closing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_that_runs_no_follower_holds_no_follow() {
    let mut pools = followed().await;
    let generation = pools.generation;
    let _member = Instance::join(&pools.global_hub, "a").await;
    pools.model_pool.drain().await.expect("the pool drains");
    let marked = through(&mut pools.watch, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    assert_eq!(marked.last(), Some(&offering(generation, 2, false)));
    assert_eq!(next(&mut pools.watch).await, dropped(generation));
    pools.stop().await;
}

/// How long the global's hub waits for a chooser with no watch of its
/// relay open, in the tests of that bound.
const LOST: Duration = Duration::from_millis(500);

/// The global's pace in the tests of the bound: [`LOST`], with the relay's
/// watches over HTTP capped at `relay`.
fn lost(relay: usize) -> gdl_hub::PoolTiming {
    gdl_hub::PoolTiming {
        lost_chooser: LOST,
        watch_caps: gdl_model::WatchCaps {
            relay,
            ..gdl_model::WatchCaps::default()
        },
        ..timing()
    }
}

/// A watch of the relay at `hub` over HTTP that names `chooser`, as a
/// chooser's follower opens it, or why the hub refused it.
async fn named(hub: &Hub, chooser: &Instance) -> Result<impl Stream + use<>, String> {
    let watcher = gdl_watch::PoolWatcher::new().expect("a client");
    let address = format!("127.0.0.1:{}", hub.public_addr().port());
    let me = gdl_watch::RelayWatcher {
        instance: chooser.id(),
    };
    tokio::time::timeout(WAIT, watcher.follows(&address, Some(&me)))
        .await
        .expect("the hub answers in time")
        .map_err(|error| error.to_string())
}

/// Drop `watch`, and wait until the global's hub has seen it close: its
/// relay's watches, the in-process one included, are down to `left`.
async fn close(pools: &Followed, watch: impl Stream, left: usize) {
    drop(watch);
    let mut open = pools.global_pool.follows_watchers();
    tokio::time::timeout(WAIT, open.wait_for(|open| *open == left))
        .await
        .expect("the hub saw the watch close in time")
        .expect("the extension runs");
}

/// Drain the model pool, and return the revision that marks its follow
/// not offered.
async fn drain(pools: &mut Followed) -> u64 {
    pools.model_pool.drain().await.expect("the pool drains");
    let marked = through(&mut pools.watch, |event| {
        matches!(event, FollowEvent::Offering { offered: false, .. })
    })
    .await;
    match marked.last() {
        Some(FollowEvent::Offering { revision, .. }) => *revision,
        _ => unreachable!("an offering"),
    }
}

/// A chooser whose watch of the relay is refused, the relay's cap met by
/// another chooser's, cannot learn that a draining pool is not offered: it
/// is waited for until the bound has passed since it said that it runs a
/// follower, and no longer. The follow then closes on the acknowledgement
/// of the chooser that watches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chooser_refused_a_watch_is_not_waited_for_past_the_bound() {
    let mut pools = followed_paced(lost(1)).await;
    let generation = pools.generation;
    let watching = Instance::chooser(&pools.global_hub).await;
    let _held = named(&pools.global_hub, &watching)
        .await
        .expect("the first watch takes the relay's place");
    let before = Instant::now();
    let refused = Instance::chooser(&pools.global_hub).await;
    let refusal = named(&pools.global_hub, &refused)
        .await
        .err()
        .expect("the relay's cap is met");
    assert!(refusal.contains("503"), "{refusal}");

    let revision = drain(&mut pools).await;
    watching
        .acknowledge(&unoffered(generation, revision))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(
        tokio::time::timeout(LOST + WAIT, next(&mut pools.watch))
            .await
            .expect("closed once the bound passed"),
        dropped(generation)
    );
    assert!(
        before.elapsed() >= LOST,
        "closed {:?} after the refused chooser declared, within the bound",
        before.elapsed()
    );
    assert!(refused.registered().await, "the refused chooser left");
    pools.stop().await;
}

/// A chooser whose watch of the relay closes and opens again within the
/// bound is waited for as before: past the bound, the follow of a
/// draining pool stays open until it acknowledges too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chooser_that_watches_again_within_the_bound_is_waited_for() {
    let mut pools = followed_paced(lost(16)).await;
    let generation = pools.generation;
    let (first, second) = (
        Instance::chooser(&pools.global_hub).await,
        Instance::chooser(&pools.global_hub).await,
    );
    let _first = named(&pools.global_hub, &first).await.expect("a watch");
    let gone = named(&pools.global_hub, &second).await.expect("a watch");
    let open = *pools.global_pool.follows_watchers().borrow();

    let revision = drain(&mut pools).await;
    close(&pools, gone, open - 1).await;
    let _back = named(&pools.global_hub, &second)
        .await
        .expect("watched again");
    first
        .acknowledge(&unoffered(generation, revision))
        .await
        .expect("the hub takes the acknowledgement");
    tokio::time::sleep(LOST * 2).await;
    assert_eq!(
        pending(&mut pools.watch),
        [],
        "closed without the chooser that came back"
    );
    second
        .acknowledge(&unoffered(generation, revision))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(next(&mut pools.watch).await, dropped(generation));
    pools.stop().await;
}

/// A chooser that watches the relay and has not acknowledged, as one that
/// holds a long response from the pool, keeps the follow of a draining
/// pool open past the bound, however long: the bound is for choosers that
/// cannot answer. Once it acknowledges, the follow closes. Its watch then
/// closing changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watching_chooser_keeps_the_follow_open_past_the_bound() {
    let mut pools = followed_paced(lost(16)).await;
    let generation = pools.generation;
    let (done, holding) = (
        Instance::chooser(&pools.global_hub).await,
        Instance::chooser(&pools.global_hub).await,
    );
    let _done = named(&pools.global_hub, &done).await.expect("a watch");
    let _holding = named(&pools.global_hub, &holding).await.expect("a watch");

    let revision = drain(&mut pools).await;
    done.acknowledge(&unoffered(generation, revision))
        .await
        .expect("the hub takes the acknowledgement");
    tokio::time::sleep(LOST * 3).await;
    assert_eq!(
        pending(&mut pools.watch),
        [],
        "closed on a chooser that watches and holds an offer"
    );
    assert!(pools.follows());
    holding
        .acknowledge(&unoffered(generation, revision))
        .await
        .expect("the hub takes the acknowledgement");
    assert_eq!(next(&mut pools.watch).await, dropped(generation));
    pools.stop().await;
}
