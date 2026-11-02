//! A shared pool names its parents' class in place of one parent pool
//! (`parents = "global"`), so the hub of every pool of that class follows
//! it. Three global pools in three regions follow one shared model pool
//! through one directory that yields every record. The model pool's hub
//! counts each parent's watch on its own, so a drain waits until every
//! parent lets go, a retirement until every parent's choosers avoid the
//! instance, and a parent that stops changes nothing for the others. Every
//! hub runs in process on OS-assigned ports; every wait is for something
//! observed.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use common::{Instance, Resolving, WAIT, meet_the_quorum, serve_pool, timing};
use futures::{FutureExt, Stream, StreamExt};
use gdl_hub::{FollowsWatch, PoolReadiness, PoolTiming, PoolWatch};
use gdl_model::{PoolSettings, SegmentPhase};
use gdl_pools::PoolState;
use gdl_watch::{Avoided, Avoids, FollowEvent, Follower, PoolWatchEvent, PoolWatcher, Unoffered};
use velo_hub::Hub;
use velo_hub::velo::InstanceId;

/// The regions of the three global pools.
const REGIONS: [&str; 3] = ["us-east-1", "eu-west-1", "ap-south-1"];

/// The shared model pool, in one of the regions.
const SHARED: &str = "model-x-shared-eu-west-1";

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

/// Read up to and including the first event `last` takes.
async fn through<T>(watch: &mut (impl Stream<Item = T> + Unpin), last: impl Fn(&T) -> bool) {
    while !last(&next(watch).await) {}
}

/// The global pool of `region`.
fn global(region: &str) -> PoolSettings {
    toml::from_str(&format!(
        "name = 'global-{region}'\nclass = 'global'\nregion = '{region}'\n\
         hub = 'global-{region}-hub:1337'\ndirectory = 'memory'\n"
    ))
    .expect("valid test fixture")
}

/// The shared model pool: every global pool is its parent. `d` needs two
/// usable instances, `f` one.
fn shared() -> PoolSettings {
    toml::from_str(&format!(
        "name = '{SHARED}'\nclass = 'model_x'\nregion = 'eu-west-1'\nparents = 'global'\n\
         hub = 'model-x-shared-hub:1337'\ndirectory = 'memory'\n[ready]\nd = 2\n"
    ))
    .expect("valid test fixture")
}

/// What a draining pool's watch says once no pool follows it.
fn unfollowed() -> PoolWatchEvent {
    PoolWatchEvent::State {
        state: PoolState::Draining,
        unfollowed: true,
        retire: None,
    }
}

fn retired(instance: InstanceId, acknowledged: bool) -> PoolWatchEvent {
    PoolWatchEvent::Retired {
        segment: "f".into(),
        instance,
        acknowledged,
    }
}

/// A watch of the model pool past its snapshot.
async fn watching(pool: &PoolReadiness<Resolving>) -> PoolWatch {
    let mut watch = pool.watch().await.expect("the watch begins");
    through(&mut watch, |event| *event == PoolWatchEvent::Synced).await;
    watch
}

/// Wait until the model pool's followers are the global pools of
/// `regions`.
async fn followed_by(pool: &PoolReadiness<Resolving>, regions: &[&str]) {
    let expected: BTreeSet<(String, String)> = regions
        .iter()
        .map(|region| ("global".to_string(), format!("global-{region}")))
        .collect();
    let mut followers = pool.followers();
    let waited = tokio::time::timeout(WAIT, followers.wait_for(|now| *now == expected))
        .await
        .is_ok();
    assert!(
        waited,
        "timed out waiting for the followers {expected:?}: {:?}",
        *followers.borrow()
    );
}

/// One global pool following the shared pool: its hub, its extension, its
/// one chooser, its relay, and the generation of its follow.
struct Global {
    region: &'static str,
    hub: Hub,
    pool: Arc<PoolReadiness<Resolving>>,
    chooser: Instance,
    relay: FollowsWatch,
    generation: u64,
}

/// Three global pools, each with a chooser, and the shared model pool they
/// follow, with its quorum met and an entry instance, `entry`, that every
/// global's relay exposes.
struct Fleet {
    globals: Vec<Global>,
    model: (Hub, Arc<PoolReadiness<Resolving>>),
    entry: Instance,
    _others: Vec<Instance>,
}

/// [`Fleet`], the shared pool's hub run at `model_timing`. Each global's
/// chooser joins before the shared pool starts, so that each follow waits
/// for its chooser.
async fn fleet(model: PoolSettings, model_timing: PoolTiming) -> Fleet {
    let directory = Resolving::new();
    let mut started = Vec::new();
    for region in REGIONS {
        let (hub, pool) = serve_pool(global(region), &directory, timing()).await;
        let chooser = Instance::chooser(&hub).await;
        let relay = pool.follows();
        started.push((region, hub, pool, chooser, relay));
    }
    let (model_hub, model_pool) = serve_pool(model, &directory, model_timing).await;
    let (others, _) = meet_the_quorum(&model_hub).await;
    let mut entry = Instance::join(&model_hub, "f").await;
    entry.announce(SegmentPhase::Ready).await;
    let mut globals = Vec::new();
    for (region, hub, pool, chooser, mut relay) in started {
        let mut generation = None;
        loop {
            match next(&mut relay).await {
                FollowEvent::Follow {
                    pool: followed,
                    generation: g,
                    region: in_region,
                    ..
                } if followed == SHARED => {
                    assert_eq!(in_region, "eu-west-1");
                    generation = Some(g);
                }
                FollowEvent::Exposed { pool: of, peer, .. }
                    if of == SHARED && peer.instance_id() == entry.id() =>
                {
                    break;
                }
                _ => {}
            }
        }
        globals.push(Global {
            region,
            hub,
            pool,
            chooser,
            relay,
            generation: generation.expect("a follow before its instances"),
        });
    }
    Fleet {
        globals,
        model: (model_hub, model_pool),
        entry,
        _others: others,
    }
}

impl Fleet {
    async fn shutdown(self) {
        for global in self.globals {
            global.hub.shutdown().await.expect("the hub stops");
        }
        self.model.0.shutdown().await.expect("the hub stops");
    }
}

impl Global {
    /// Read the relay until it says the shared pool is not offered: the
    /// revision its chooser acknowledges.
    async fn unoffered(&mut self) -> Unoffered {
        loop {
            if let FollowEvent::Offering {
                pool,
                generation,
                revision,
                offered: false,
                ..
            } = next(&mut self.relay).await
                && pool == SHARED
            {
                return Unoffered {
                    class: "model_x".into(),
                    pool: SHARED.into(),
                    generation,
                    revision,
                };
            }
        }
    }

    /// What the chooser says once its tables hold `instance` retired.
    fn avoids(&self, instance: InstanceId) -> Avoids {
        Avoids {
            class: "model_x".into(),
            pool: SHARED.into(),
            generation: self.generation,
            instance,
        }
    }

    /// Whether this global's hub still waits for its chooser to avoid
    /// `instance`.
    fn waits_for(&self, instance: InstanceId) -> bool {
        self.pool.retirements().borrow().contains(&(
            "model_x".into(),
            SHARED.into(),
            self.generation,
            instance,
        ))
    }
}

/// The hub of each global pool, in three regions, follows the one model
/// pool that names their class as its parents, and offers it; the model
/// pool counts all three as its followers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_global_pools_in_three_regions_follow_one_shared_pool() {
    let fleet = fleet(shared(), timing()).await;
    followed_by(&fleet.model.1, &REGIONS).await;
    for global in &fleet.globals {
        let following = global.pool.following().borrow().clone();
        assert_eq!(
            following.get(&("model_x".to_string(), SHARED.to_string())),
            Some(&global.generation),
            "global-{} follows the shared pool",
            global.region
        );
    }
    fleet.shutdown().await;
}

/// A drain of the shared pool says that no pool follows it only once every
/// parent has let go: each global releases the pool once its chooser
/// acknowledges that it holds no offer to it, and the pool still has a
/// follower after the first and the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_of_a_shared_pool_waits_until_every_parent_lets_go() {
    let mut fleet = fleet(shared(), timing()).await;
    followed_by(&fleet.model.1, &REGIONS).await;
    let mut watch = watching(&fleet.model.1).await;
    fleet.model.1.drain().await.expect("the pool drains");
    through(&mut watch, |event| {
        *event
            == PoolWatchEvent::State {
                state: PoolState::Draining,
                unfollowed: false,
                retire: None,
            }
    })
    .await;
    let mut unoffered = Vec::new();
    for global in &mut fleet.globals {
        unoffered.push(global.unoffered().await);
    }
    for (index, global) in fleet.globals.iter().enumerate() {
        global
            .chooser
            .acknowledge(&unoffered[index])
            .await
            .expect("the hub takes the acknowledgement");
        followed_by(&fleet.model.1, &REGIONS[index + 1..]).await;
        if index + 1 < REGIONS.len() {
            assert!(
                !pending(&mut watch).contains(&unfollowed()),
                "unfollowed while {} parents still follow the pool",
                REGIONS.len() - index - 1
            );
        }
    }
    through(&mut watch, |event| *event == unfollowed()).await;
    fleet.shutdown().await;
}

/// A retired entry instance of the shared pool is acknowledged only once
/// the choosers of every parent avoid it: each global's hub waits for its
/// own chooser, then tells the model pool's hub for its whole pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retirement_waits_until_every_parents_choosers_avoid_it() {
    let mut fleet = fleet(shared(), timing()).await;
    followed_by(&fleet.model.1, &REGIONS).await;
    let mut watch = watching(&fleet.model.1).await;
    let id = fleet.entry.id();
    fleet.entry.announce(SegmentPhase::Retired).await;
    loop {
        match next(&mut watch).await {
            PoolWatchEvent::Load { .. } | PoolWatchEvent::State { .. } => {}
            event => {
                assert_eq!(event, retired(id, false), "every parent is waited for");
                break;
            }
        }
    }
    for global in &fleet.globals {
        let mut retirements = global.pool.retirements();
        let waited = (
            "model_x".to_string(),
            SHARED.to_string(),
            global.generation,
            id,
        );
        tokio::time::timeout(WAIT, retirements.wait_for(|now| now.contains(&waited)))
            .await
            .expect("each global waits for its chooser in time")
            .expect("the hub lives");
    }
    for (index, global) in fleet.globals.iter().enumerate() {
        global
            .chooser
            .avoid(&global.avoids(id))
            .await
            .expect("taken");
        assert!(!global.waits_for(id), "global-{} avoids it", global.region);
        if index + 1 < REGIONS.len() {
            assert!(fleet.globals[index + 1].waits_for(id));
            assert!(
                !pending(&mut watch).contains(&retired(id, true)),
                "acknowledged while a parent's chooser has yet to avoid it"
            );
        }
    }
    loop {
        if let PoolWatchEvent::Retired {
            instance,
            acknowledged,
            ..
        } = next(&mut watch).await
            && instance == id
        {
            assert!(acknowledged);
            break;
        }
    }
    fleet.shutdown().await;
}

/// The model pool's hub settles a retirement per parent: with three
/// parents' watches open, two that say their pool avoids the instance
/// leave it waiting, and the third acknowledges it. A test stands in for
/// each parent's hub, so each step is taken before the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retirement_counts_each_parent_on_its_own() {
    let directory = Resolving::new();
    let (hub, pool) = serve_pool(shared(), &directory, timing()).await;
    let (_others, _) = meet_the_quorum(&hub).await;
    let mut entry = Instance::join(&hub, "f").await;
    entry.announce(SegmentPhase::Ready).await;
    let address = common::address(&hub);
    let watcher = PoolWatcher::new().expect("a client");
    let parents: Vec<Follower> = REGIONS
        .iter()
        .map(|region| Follower {
            class: "global".into(),
            pool: format!("global-{region}"),
            run: 7,
            generation: 1,
        })
        .collect();
    let mut watches = Vec::new();
    for parent in &parents {
        let mut events = watcher.follow(&address, parent).await.expect("a watch");
        loop {
            let event = next(&mut events).await.expect("the watch does not fail");
            if event.event() == Some(PoolWatchEvent::Synced) {
                break;
            }
        }
        watches.push(events);
    }
    followed_by(&pool, &REGIONS).await;
    let mut watch = watching(&pool).await;
    entry.announce(SegmentPhase::Retired).await;
    through(&mut watch, |event| *event == retired(entry.id(), false)).await;
    for (index, parent) in parents.iter().enumerate() {
        let avoided = Avoided {
            follower: parent.clone(),
            instance: entry.id(),
        };
        watcher
            .avoided(&address, &avoided)
            .await
            .expect("the hub takes it");
        if index + 1 < parents.len() {
            assert!(
                !pending(&mut watch).contains(&retired(entry.id(), true)),
                "acknowledged after {} of {} parents",
                index + 1,
                parents.len()
            );
        }
    }
    through(&mut watch, |event| *event == retired(entry.id(), true)).await;
    hub.shutdown().await.expect("the hub stops");
}

/// A parent whose hub stops changes nothing for the others: they keep the
/// shared pool followed and offered, and the pool stays ready. The
/// stopped parent's watch closed without a release, so the pool counts it
/// until its lease passes, then holds it as lost: a later drain never says
/// that no pool follows it on the other parents' releases alone, so the
/// pool's entry instances wait out their own drain bound. Runs about 3 s: the lost watch is marked at the
/// pool's 1 s summary once its 2 s lease has passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_that_stops_changes_nothing_for_the_others() {
    let quick = PoolSettings {
        summary: Duration::from_secs(1),
        ..shared()
    };
    let lease = PoolTiming {
        lease: Duration::from_secs(2),
        ..timing()
    };
    let mut fleet = fleet(quick, lease).await;
    followed_by(&fleet.model.1, &REGIONS).await;
    let mut watchers = fleet.model.1.watchers();
    let before = *watchers.borrow();
    let stopped = fleet.globals.remove(1);
    stopped.hub.shutdown().await.expect("the hub stops");
    tokio::time::timeout(WAIT, watchers.wait_for(|now| *now < before))
        .await
        .expect("the stopped parent's watch closes in time")
        .expect("the hub lives");
    let others = [REGIONS[0], REGIONS[2]];
    // Its lease passes: it no longer shows as a follower.
    followed_by(&fleet.model.1, &others).await;
    assert_eq!(fleet.model.1.health().borrow().state, PoolState::Ready);
    for global in &mut fleet.globals {
        let shared = ("model_x".to_string(), SHARED.to_string());
        assert_eq!(
            global.pool.following().borrow().get(&shared),
            Some(&global.generation)
        );
        for event in pending(&mut global.relay) {
            assert!(
                !matches!(
                    &event,
                    FollowEvent::Offering { pool, offered: false, .. }
                        | FollowEvent::Dropped { pool, .. }
                        if pool == SHARED
                ) && !matches!(
                    &event,
                    FollowEvent::State { pool, state, .. }
                        if pool == SHARED && *state != PoolState::Ready
                ),
                "global-{} saw {event:?}",
                global.region
            );
        }
    }

    let mut watch = watching(&fleet.model.1).await;
    fleet.model.1.drain().await.expect("the pool drains");
    let mut unoffered = Vec::new();
    for global in &mut fleet.globals {
        unoffered.push(global.unoffered().await);
    }
    for (global, unoffered) in fleet.globals.iter().zip(&unoffered) {
        global
            .chooser
            .acknowledge(unoffered)
            .await
            .expect("the hub takes the acknowledgement");
    }
    followed_by(&fleet.model.1, &[]).await;
    assert!(
        !pending(&mut watch).contains(&unfollowed()),
        "the stopped parent's lost watch was taken as a release"
    );
    drop(stopped);
    fleet.shutdown().await;
}
