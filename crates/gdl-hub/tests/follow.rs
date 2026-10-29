//! A global pool's hub follows its children, the model pools that name it
//! as their parent, over their pool watches, and relays what it follows to its own instances. In the
//! deployment of these tests, `global` runs `a` and sends to `model_x`,
//! whose entry segment is `f`. Every hub runs in process on OS-assigned
//! ports, sharing one directory whose hub names a test resolves once each
//! hub serves; every wait is for something observed.

mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use common::{
    GLOBAL, Instance, POOL, Records, Resolving, WAIT, global_default, health, meet_the_quorum,
    model, serve_pool, timing, usable,
};
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use gdl_hub::{PoolReadiness, PoolTiming};
use gdl_model::SegmentPhase;
use gdl_pools::{PoolDirectory, PoolRecord, PoolState, Withdrawal};
use gdl_watch::{FollowEvent, PoolWatcher};
use velo_hub::Hub;
use velo_hub::velo::{InstanceId, PeerInfo};

/// The relay on `hub`, through the client.
async fn follows(hub: &Hub) -> BoxStream<'static, FollowEvent> {
    let watcher = PoolWatcher::new().expect("a client");
    let address = format!("127.0.0.1:{}", hub.public_addr().port());
    tokio::time::timeout(WAIT, watcher.follows(&address, None))
        .await
        .expect("the hub answers in time")
        .expect("the hub serves the relay")
        .map(|item| {
            item.expect("the watch does not fail")
                .event()
                .expect("an event of a kind this build knows")
        })
        .boxed()
}

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

/// The snapshot: every event up to and including the marker.
async fn snapshot(watch: &mut (impl Stream<Item = FollowEvent> + Unpin)) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    loop {
        let event = next(watch).await;
        let synced = matches!(event, FollowEvent::Synced { .. });
        events.push(event);
        if synced {
            return events;
        }
    }
}

/// Apply events to `relay` until it satisfies `done`.
async fn until(
    watch: &mut (impl Stream<Item = FollowEvent> + Unpin),
    relay: &mut Relay,
    what: &str,
    mut done: impl FnMut(&Relay) -> bool,
) {
    let waited = tokio::time::timeout(WAIT, async {
        while !done(relay) {
            let event = watch.next().await.expect("the watch goes on");
            relay.apply(&event);
        }
    })
    .await;
    assert!(waited.is_ok(), "timed out waiting for {what}: {relay:?}");
}

/// Wait until the pools the hub follows satisfy `done`, and return them.
/// The pools are `model_x`'s, by name: the only class the global sends to.
async fn following(
    pool: &PoolReadiness<Resolving>,
    what: &str,
    mut done: impl FnMut(&BTreeMap<String, u64>) -> bool,
) -> BTreeMap<String, u64> {
    let model_x = |pools: &BTreeMap<(String, String), u64>| -> BTreeMap<String, u64> {
        pools
            .iter()
            .filter(|((class, _), _)| class == "model_x")
            .map(|((_, pool), generation)| (pool.clone(), *generation))
            .collect()
    };
    let mut following = pool.following();
    let waited = tokio::time::timeout(WAIT, async {
        following
            .wait_for(|pools| done(&model_x(pools)))
            .await
            .map(|pools| model_x(&pools))
    })
    .await;
    match waited {
        Ok(pools) => pools.expect("the extension runs"),
        Err(_) => panic!("timed out waiting for {what}: {:?}", *following.borrow()),
    }
}

/// The follow `generation` of [`POOL`], marked not offered at its first
/// change.
fn unoffered(generation: u64) -> FollowEvent {
    FollowEvent::Offering {
        class: "model_x".into(),
        pool: POOL.into(),
        generation,
        revision: 2,
        offered: false,
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

/// A record a test writes into the directory itself: a child of the
/// global, of its region, deployment, and class, ready, at `hub`.
fn written(like: &PoolRecord, name: &str, hub: &str) -> PoolRecord {
    PoolRecord {
        name: name.into(),
        state: PoolState::Ready,
        hub: hub.into(),
        ..like.clone()
    }
}

/// A pool no hub serves: the address of a port that refuses.
fn nowhere() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let address = listener.local_addr().expect("an address");
    drop(listener);
    address.to_string()
}

/// What a consumer of the relay holds, by the rules [`FollowEvent`]
/// states, asserting each of them as it applies an event.
#[derive(Debug, Default, PartialEq, Eq)]
struct Relay {
    follows: BTreeMap<String, Followed>,
    /// The pools the relay said were withdrawn, with the generation each
    /// withdrawal named.
    withdrawn: BTreeMap<String, u64>,
}

#[derive(Debug, PartialEq, Eq)]
struct Followed {
    generation: u64,
    state: PoolState,
    retired: HashSet<InstanceId>,
    exposed: HashMap<InstanceId, PeerInfo>,
    /// Instances a state hid, which a consumer keeps until they leave or
    /// are exposed again.
    hidden: HashSet<InstanceId>,
}

fn exposes(state: PoolState) -> bool {
    matches!(state, PoolState::Ready | PoolState::Degraded)
}

impl Relay {
    fn of(events: &[FollowEvent]) -> Self {
        let mut relay = Self::default();
        for event in events {
            relay.apply(event);
        }
        relay
    }

    /// The follow `pool` of `generation`: a consumer never sees an event
    /// of a generation it does not hold.
    fn current(&mut self, pool: &str, generation: u64) -> &mut Followed {
        let followed = self
            .follows
            .get_mut(pool)
            .unwrap_or_else(|| panic!("an event of {pool}, which is not followed"));
        assert_eq!(
            followed.generation, generation,
            "an event of {pool}'s other generation"
        );
        followed
    }

    fn apply(&mut self, event: &FollowEvent) {
        match event {
            FollowEvent::Follow {
                pool,
                generation,
                state,
                ..
            } => {
                let installed = self.follows.insert(
                    pool.clone(),
                    Followed {
                        generation: *generation,
                        state: *state,
                        retired: HashSet::new(),
                        exposed: HashMap::new(),
                        hidden: HashSet::new(),
                    },
                );
                assert!(installed.is_none(), "{pool} installed twice");
            }
            FollowEvent::Synced { .. } | FollowEvent::Listed { .. } => {}
            FollowEvent::Load {
                pool, generation, ..
            }
            | FollowEvent::Offering {
                pool, generation, ..
            } => {
                self.current(pool, *generation);
            }
            FollowEvent::State {
                pool,
                generation,
                state,
                ..
            } => {
                let followed = self.current(pool, *generation);
                followed.state = *state;
                if !exposes(*state) {
                    let hidden: Vec<InstanceId> =
                        followed.exposed.drain().map(|(id, _)| id).collect();
                    followed.hidden.extend(hidden);
                }
            }
            FollowEvent::Exposed {
                pool,
                generation,
                peer,
                ..
            } => {
                let followed = self.current(pool, *generation);
                assert!(
                    exposes(followed.state),
                    "exposed while {:?}",
                    followed.state
                );
                let id = peer.instance_id();
                assert!(
                    !followed.retired.contains(&id),
                    "a retired instance exposed"
                );
                followed.hidden.remove(&id);
                let known = followed.exposed.insert(id, peer.clone());
                assert!(known.is_none(), "an instance exposed twice");
            }
            FollowEvent::Retired {
                pool,
                generation,
                instance,
                ..
            } => {
                let followed = self.current(pool, *generation);
                followed.exposed.remove(instance);
                followed.hidden.remove(instance);
                assert!(followed.retired.insert(*instance), "retired twice");
            }
            FollowEvent::Left {
                pool,
                generation,
                instance,
                ..
            } => {
                let followed = self.current(pool, *generation);
                let known = followed.exposed.remove(instance).is_some()
                    | followed.hidden.remove(instance)
                    | followed.retired.remove(instance);
                assert!(known, "an instance that left was never sent");
            }
            FollowEvent::Dropped {
                pool, generation, ..
            } => {
                self.current(pool, *generation);
                self.follows.remove(pool);
            }
            // It names the pool's latest follow, which may have been
            // dropped already.
            FollowEvent::Withdrawn {
                pool, generation, ..
            } => {
                if let Some(followed) = self.follows.get(pool) {
                    assert_eq!(followed.generation, *generation, "not the latest follow");
                }
                self.withdrawn.insert(pool.clone(), *generation);
            }
        }
    }

    fn exposed(&self, pool: &str) -> HashSet<InstanceId> {
        self.follows
            .get(pool)
            .map(|followed| followed.exposed.keys().copied().collect())
            .unwrap_or_default()
    }
}

/// A global follows its children, the model pools whose record names it as
/// their parent (`home`) with its contract and placement hashes, once
/// their record says they take work (`ready` or `degraded`), whatever
/// their region. It does not follow a pool whose parent is another global,
/// one with no parent, one with another contract or placement hash, one
/// that is `unavailable`, or one scaled to 0, whose record never lapses
/// though its hub may be gone. The follower decides on each record in the
/// order the directory gives them, so once it follows a pool whose record
/// came last, it has decided on every record before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_follows_its_children_that_take_work() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let published = records.next().await;
    assert_eq!(published.state, PoolState::Unavailable);
    assert_eq!(published.home.as_deref(), Some(GLOBAL));
    let elsewhere = PoolRecord {
        region: "us-west-2".into(),
        ..written(&published, "model-x-01-us-west-2", &nowhere())
    };
    let cousin = PoolRecord {
        home: Some("global-02-us-east-1".into()),
        ..written(&published, "model-x-03-us-east-1", &nowhere())
    };
    let orphan = PoolRecord {
        home: None,
        ..written(&published, "model-x-04-us-east-1", &nowhere())
    };
    let foreign = PoolRecord {
        contract: published.contract ^ 1,
        ..written(&published, "model-x-02-us-east-1", &nowhere())
    };
    let replaced = PoolRecord {
        placement: published.placement ^ 1,
        ..written(&published, "model-x-05-us-east-1", &nowhere())
    };
    let scaled = PoolRecord {
        presence: gdl_pools::Presence::ScaledToZero,
        ..written(&published, "model-x-06-us-east-1", &nowhere())
    };
    for record in [elsewhere, cousin, orphan, foreign, replaced, scaled] {
        directory.inner.publish(record).await.expect("stored");
    }
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;

    let first = written(&published, "model-x-08-us-east-1", &nowhere());
    directory.inner.publish(first).await.expect("stored");
    let pools = following(&global_pool, "the first written pool", |pools| {
        pools.contains_key("model-x-08-us-east-1")
    })
    .await;
    assert_eq!(
        pools.keys().collect::<Vec<_>>(),
        ["model-x-01-us-west-2", "model-x-08-us-east-1"],
        "another region's child, and not another global's, an orphan, another deployment's, an unavailable pool, or one scaled to 0"
    );

    let (_instances, _) = meet_the_quorum(&model_hub).await;
    let pools = following(&global_pool, "the model pool", |pools| {
        pools.contains_key(POOL)
    })
    .await;
    assert_eq!(
        pools.keys().collect::<Vec<_>>(),
        [POOL, "model-x-01-us-west-2", "model-x-08-us-east-1"]
    );
    let mut watchers = model_pool.watchers();
    tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching == 1))
        .await
        .expect("the global watches the model pool")
        .expect("the extension lives");
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A scale-to-zero record must also stop a follow opened while serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_followed_pool_scaled_to_zero_is_no_longer_offered() {
    let directory = Resolving::new();
    let (model_hub, model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    let mut records = Records::new(&directory.inner);
    let (ready, _) = records.until(PoolState::Ready).await;
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    following(&global_pool, "the serving child", |pools| {
        pools.contains_key(POOL)
    })
    .await;
    let mut watchers = model_pool.watchers();
    tokio::time::timeout(WAIT, watchers.wait_for(|count| *count == 1))
        .await
        .expect("the parent opens the child's watch")
        .expect("the child runs");
    let mut events = global_pool.follows();
    until(
        &mut events,
        &mut Relay::default(),
        "the child's entry",
        |relay| {
            relay
                .follows
                .get(POOL)
                .is_some_and(|pool| !pool.exposed.is_empty())
        },
    )
    .await;

    model_pool
        .set_presence(gdl_pools::Presence::ScaledToZero)
        .await
        .expect("the scale-to-zero record is stored");
    let marker = "model-x-09-us-east-1";
    directory
        .inner
        .publish(written(&ready, marker, &nowhere()))
        .await
        .expect("the marker follows the scale-to-zero record");
    // The directory orders records, so seeing this follow proves the
    // parent processed the scale-to-zero record first.
    let pools = following(&global_pool, "the marker", |pools| {
        pools.contains_key(marker)
    })
    .await;
    let events = snapshot(&mut global_pool.follows()).await;
    let offered = events.iter().any(|event| {
        matches!(event, FollowEvent::Follow { pool, state, .. } if pool == POOL && exposes(*state))
    }) && !events.iter().any(|event| {
        matches!(event, FollowEvent::Offering { pool, offered: false, .. } if pool == POOL)
    });
    global_hub.shutdown().await.expect("the parent stops");
    model_hub.shutdown().await.expect("the child stops");
    assert!(
        !offered,
        "the parent still offers the pool after its scale-to-zero record: follows={pools:?}, events={events:?}"
    );
}

/// Through the relay, a followed pool appears with its state before any
/// of its instances, and its retired instances before its ready ones.
/// While the pool says it is `unavailable`, none of its instances is
/// exposed, not even one that becomes ready meanwhile; when it takes work
/// again, they are, after the state that allows them. The follow
/// outlives the pool's record leaving the follow set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follow_is_its_state_first_then_retired_then_ready() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let mut serving = Instance::join(&model_hub, "f").await;
    serving.announce(SegmentPhase::Ready).await;
    let mut retiring = Instance::join(&model_hub, "f").await;
    retiring.announce(SegmentPhase::Ready).await;
    health(&model_pool, "f at 2", |h| usable(h, "f") == 2).await;
    retiring.announce(SegmentPhase::Retired).await;
    health(&model_pool, "f back at 1", |h| usable(h, "f") == 1).await;
    let mut inside = Vec::new();
    for _ in 0..2 {
        let mut instance = Instance::join(&model_hub, "d").await;
        instance.announce(SegmentPhase::Ready).await;
        inside.push(instance);
    }
    records.until(PoolState::Ready).await;

    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    // A chooser that never acknowledges that the pool is not offered, so
    // that the follow lives through the states that take no work.
    let _chooser = Instance::chooser(&global_hub).await;
    let mut inner = global_pool.follows();
    let mut relay = Relay::of(&snapshot(&mut inner).await);
    until(
        &mut inner,
        &mut relay,
        "the follow's ready instance",
        |relay| relay.exposed(POOL).contains(&serving.id()),
    )
    .await;
    let generation = relay.follows[POOL].generation;
    drop(inner);
    let mut watch = follows(&global_hub).await;
    let serving_peer = peer(&model_hub, serving.id()).await;
    let first = snapshot(&mut watch).await;
    assert_eq!(
        first,
        [
            FollowEvent::Follow {
                listing: None,
                class: "model_x".into(),
                pool: POOL.into(),
                generation,
                region: "us-east-1".into(),
                state: PoolState::Ready,
                load: None,
                load_age_ms: None,
            },
            FollowEvent::Retired {
                class: "model_x".into(),
                pool: POOL.into(),
                generation,
                segment: "f".into(),
                instance: retiring.id(),
            },
            FollowEvent::Exposed {
                class: "model_x".into(),
                pool: POOL.into(),
                generation,
                segment: "f".into(),
                peer: serving_peer.clone(),
            },
            FollowEvent::Synced { listed: false },
        ]
    );
    let mut relay = Relay::of(&first);

    let state = |state| FollowEvent::State {
        class: "model_x".into(),
        pool: POOL.into(),
        generation,
        state,
    };
    for instance in &mut inside {
        instance.withdraw(SegmentPhase::Ready).await;
    }
    for expected in [PoolState::Degraded, PoolState::Unavailable] {
        let event = next(&mut watch).await;
        assert_eq!(event, state(expected));
        relay.apply(&event);
    }
    assert_eq!(next(&mut watch).await, unoffered(generation));
    records.until(PoolState::Unavailable).await;

    let mut joining = Instance::join(&model_hub, "f").await;
    joining.announce(SegmentPhase::Ready).await;
    health(&model_pool, "f at 2", |h| usable(h, "f") == 2).await;
    let mut back = Instance::join(&model_hub, "d").await;
    back.announce(SegmentPhase::Ready).await;
    let event = next(&mut watch).await;
    assert_eq!(
        event,
        state(PoolState::Degraded),
        "nothing exposed before it"
    );
    relay.apply(&event);
    let mut exposed = HashMap::new();
    for _ in 0..2 {
        let event = next(&mut watch).await;
        relay.apply(&event);
        let FollowEvent::Exposed { peer, .. } = event else {
            panic!("{event:?}");
        };
        exposed.insert(peer.instance_id(), peer);
    }
    assert_eq!(
        exposed,
        HashMap::from([
            (serving.id(), serving_peer),
            (joining.id(), peer(&model_hub, joining.id()).await),
        ])
    );

    // A watch opened now holds what the first one has seen.
    let mut again = follows(&global_hub).await;
    assert_eq!(Relay::of(&snapshot(&mut again).await), relay);
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// Liveness is the link: stopping a followed pool's hub drops the follow
/// at once, though its record is fresh in the directory. The global waits
/// longer than the test before it would follow the pool again, so the
/// drop is the link's and not a new follow's. A new hub process for the
/// pool, with a later epoch, is followed as a new generation once it
/// takes work, without that wait, and nothing of the old generation
/// arrives after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_whose_hub_stops_is_dropped_and_its_successor_followed_anew() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (first_hub, _first_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (first_instances, _) = meet_the_quorum(&first_hub).await;
    let (before, _) = records.until(PoolState::Ready).await;
    let (global_hub, global_pool) = serve_pool(
        global_default(),
        &directory,
        PoolTiming {
            refollow: Duration::from_secs(600),
            ..timing()
        },
    )
    .await;
    let mut watch = follows(&global_hub).await;
    let mut relay = Relay::of(&snapshot(&mut watch).await);
    until(
        &mut watch,
        &mut relay,
        "the follow's ready instance",
        |relay| relay.exposed(POOL).contains(&first_instances[0].id()),
    )
    .await;
    let first = relay.follows[POOL].generation;

    first_hub.shutdown().await.expect("the hub stops");
    // Not offered at once, and, with no chooser to wait for, closed.
    let event = next(&mut watch).await;
    assert_eq!(event, unoffered(first));
    relay.apply(&event);
    let event = next(&mut watch).await;
    assert_eq!(
        event,
        FollowEvent::Dropped {
            class: "model_x".into(),
            pool: POOL.into(),
            generation: first,
        }
    );
    relay.apply(&event);

    let (second_hub, _second_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-b:1337"),
        &directory,
        timing(),
    )
    .await;
    let (second_instances, _) = meet_the_quorum(&second_hub).await;
    let (after, _) = records.until(PoolState::Ready).await;
    assert!(after.epoch > before.epoch);
    until(&mut watch, &mut relay, "the new follow, ready", |relay| {
        relay.follows.get(POOL).is_some_and(|followed| {
            followed.state == PoolState::Ready
                && followed.exposed.contains_key(&second_instances[0].id())
        })
    })
    .await;
    let second = relay.follows[POOL].generation;
    assert!(second > first, "{second} after {first}");
    assert!(
        relay.withdrawn.is_empty(),
        "a stopped hub is not a withdrawal"
    );
    let pools = following(&global_pool, "the new generation", |pools| {
        pools.get(POOL) == Some(&second)
    })
    .await;
    assert_eq!(pools.len(), 1);
    global_hub.shutdown().await.expect("the hub stops");
    second_hub.shutdown().await.expect("the hub stops");
}

/// A hub at the address a record names that serves another pool, or the
/// pool in another epoch than the record's, is not followed for it: the
/// follow ends at the pool's first event, and the global follows it again
/// after a while, as a new generation, which is how the test knows the
/// first was refused. A newer epoch in the directory ends the follow of
/// the hub it replaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_serving_another_pool_or_epoch_is_not_installed() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, _model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let (record, _) = records.until(PoolState::Ready).await;
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    let mut watch = global_pool.follows();
    let mut relay = Relay::of(&snapshot(&mut watch).await);
    until(&mut watch, &mut relay, "the follow", |relay| {
        relay.exposed(POOL).contains(&instances[0].id())
    })
    .await;
    let generation = relay.follows[POOL].generation;

    let other = written(&record, "model-x-09-us-east-1", &record.hub);
    directory.inner.publish(other).await.expect("stored");
    following(&global_pool, "a second follow of the other pool", |pools| {
        pools.get("model-x-09-us-east-1") >= Some(&2)
    })
    .await;
    let mut again = follows(&global_hub).await;
    let held = Relay::of(&snapshot(&mut again).await);
    assert_eq!(
        held.follows.keys().collect::<Vec<_>>(),
        [POOL],
        "the other pool is not installed"
    );

    let later = PoolRecord {
        epoch: record.epoch + 1,
        seq: 1,
        ..record.clone()
    };
    directory.inner.publish(later).await.expect("stored");
    assert_eq!(next(&mut watch).await, unoffered(generation));
    let event = next(&mut watch).await;
    assert_eq!(
        event,
        FollowEvent::Dropped {
            class: "model_x".into(),
            pool: POOL.into(),
            generation,
        }
    );
    following(
        &global_pool,
        "a second follow in the later epoch",
        |pools| pools.get(POOL) >= Some(&(generation + 2)),
    )
    .await;
    let mut last = follows(&global_hub).await;
    assert_eq!(
        snapshot(&mut last).await,
        [FollowEvent::Synced { listed: false }]
    );
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// Write a record of a pool no hub serves, and wait until the global
/// follows it: it has then decided on every record before it.
async fn written_and_followed(
    directory: &Resolving,
    global: &PoolReadiness<Resolving>,
    like: &PoolRecord,
) -> BTreeMap<String, u64> {
    let marker = written(like, "model-x-09-us-east-1", &nowhere());
    directory.inner.publish(marker).await.expect("stored");
    following(global, "the written pool", |pools| {
        pools.contains_key("model-x-09-us-east-1")
    })
    .await
}

/// The events the watch has now.
fn pending(watch: &mut (impl Stream<Item = FollowEvent> + Unpin)) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        events.push(event);
    }
    events
}

/// A followed pool whose record is withdrawn while its link is live stays
/// followed: the link, not the directory, says whether the pool is there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawn_record_does_not_end_a_live_follow() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, _model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let (record, _) = records.until(PoolState::Ready).await;
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    let mut watch = global_pool.follows();
    let mut relay = Relay::of(&snapshot(&mut watch).await);
    until(&mut watch, &mut relay, "the follow", |relay| {
        relay.exposed(POOL).contains(&instances[0].id())
    })
    .await;
    let generation = relay.follows[POOL].generation;

    directory
        .inner
        .withdraw(Withdrawal {
            class: record.class.clone(),
            pool: POOL.into(),
            epoch: record.epoch,
            seq: record.seq + 1,
        })
        .await
        .expect("withdrawn");
    let pools = written_and_followed(&directory, &global_pool, &record).await;
    assert_eq!(pools.get(POOL), Some(&generation));
    for event in pending(&mut watch) {
        relay.apply(&event);
    }
    assert_eq!(relay.follows[POOL].generation, generation);
    assert!(relay.exposed(POOL).contains(&instances[0].id()));
    assert_eq!(relay.withdrawn.get(POOL), Some(&generation));
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A followed pool that drains stays followed, exposing nothing and not
/// offered, while a chooser of the global has yet to acknowledge it, and
/// while any of its instances is a member of its hub. Once the last
/// leaves, its hub withdraws its record and ends its watch, and the global
/// does not follow the pool again; it drops the follow once the chooser
/// acknowledges.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_pool_is_dropped_once_its_hub_withdraws_it() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let (record, _) = records.until(PoolState::Ready).await;
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    let chooser = Instance::chooser(&global_hub).await;
    let mut watch = global_pool.follows();
    let mut relay = Relay::of(&snapshot(&mut watch).await);
    until(&mut watch, &mut relay, "the follow", |relay| {
        relay.exposed(POOL).contains(&instances[0].id())
    })
    .await;
    let generation = relay.follows[POOL].generation;

    model_pool.drain().await.expect("the pool drains");
    until(&mut watch, &mut relay, "the pool draining", |relay| {
        relay.follows[POOL].state == PoolState::Draining
    })
    .await;
    assert!(relay.exposed(POOL).is_empty());
    assert_eq!(next(&mut watch).await, unoffered(generation));
    for instance in instances {
        instance.leave().await;
    }
    following(
        &global_pool,
        "the drained pool no longer followed",
        |pools| !pools.contains_key(POOL),
    )
    .await;
    until(&mut watch, &mut relay, "the withdrawal", |relay| {
        relay.withdrawn.contains_key(POOL)
    })
    .await;
    assert_eq!(relay.withdrawn[POOL], generation);
    assert!(relay.follows.contains_key(POOL), "dropped unacknowledged");
    chooser
        .acknowledge(&gdl_watch::Unoffered {
            class: "model_x".into(),
            pool: POOL.into(),
            generation,
            revision: 2,
        })
        .await
        .expect("the hub takes the acknowledgement");
    until(&mut watch, &mut relay, "the follow dropped", |relay| {
        !relay.follows.contains_key(POOL)
    })
    .await;
    // Once the directory has shown it a later write, the global has
    // decided on every record before it, and not followed the pool again.
    let pools = written_and_followed(&directory, &global_pool, &record).await;
    assert!(!pools.contains_key(POOL), "followed again: {pools:?}");
    for event in pending(&mut watch) {
        relay.apply(&event);
    }
    assert!(
        relay
            .follows
            .get(POOL)
            .is_none_or(|followed| followed.generation == generation),
        "followed again"
    );
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A drained pool whose follow every chooser acknowledged is dropped
/// before its last instance leaves; the withdrawal of its record that
/// follows is relayed after the drop, naming the follow that was dropped,
/// so that a chooser that held the pool learns it was drained on purpose.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawal_after_the_drop_is_relayed() {
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
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    let chooser = Instance::chooser(&global_hub).await;
    let mut watch = global_pool.follows();
    let mut relay = Relay::of(&snapshot(&mut watch).await);
    until(&mut watch, &mut relay, "the follow", |relay| {
        relay.exposed(POOL).contains(&instances[0].id())
    })
    .await;
    let generation = relay.follows[POOL].generation;

    model_pool.drain().await.expect("the pool drains");
    until(&mut watch, &mut relay, "the pool draining", |relay| {
        relay.follows[POOL].state == PoolState::Draining
    })
    .await;
    assert_eq!(next(&mut watch).await, unoffered(generation));
    chooser
        .acknowledge(&gdl_watch::Unoffered {
            class: "model_x".into(),
            pool: POOL.into(),
            generation,
            revision: 2,
        })
        .await
        .expect("the hub takes the acknowledgement");
    until(&mut watch, &mut relay, "the follow dropped", |relay| {
        !relay.follows.contains_key(POOL)
    })
    .await;
    assert!(
        relay.withdrawn.is_empty(),
        "withdrawn before the instances left"
    );
    for instance in instances {
        instance.leave().await;
    }
    let event = next(&mut watch).await;
    assert_eq!(
        event,
        FollowEvent::Withdrawn {
            class: "model_x".into(),
            pool: POOL.into(),
            generation,
        }
    );
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A followed pool whose record expires on the global's clock while its
/// link is live stays followed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_record_does_not_end_a_live_follow() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let lease = Duration::from_millis(500);
    let (model_hub, _model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        PoolTiming { lease, ..timing() },
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let (record, _) = records.until(PoolState::Ready).await;
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    let mut watch = global_pool.follows();
    let mut relay = Relay::of(&snapshot(&mut watch).await);
    until(&mut watch, &mut relay, "the follow", |relay| {
        relay.exposed(POOL).contains(&instances[0].id())
    })
    .await;
    let generation = relay.follows[POOL].generation;

    // The global saw the record before it followed the pool, and the
    // pool's heartbeat is longer than the test: past this instant the
    // record is expired on the global's clock. Expiry is no event, so the
    // test waits for the instant itself.
    tokio::time::sleep_until((Instant::now() + lease + Duration::from_millis(100)).into()).await;
    let pools = written_and_followed(&directory, &global_pool, &record).await;
    assert_eq!(pools.get(POOL), Some(&generation));
    for event in pending(&mut watch) {
        relay.apply(&event);
    }
    assert!(relay.withdrawn.is_empty(), "an expiry is not a withdrawal");
    assert_eq!(relay.follows[POOL].generation, generation);
    assert!(relay.exposed(POOL).contains(&instances[0].id()));
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// A watcher of the relay that does not read while more events than its
/// buffer arrive has its watch ended past its snapshot, while another
/// keeps up. Over a socket the kernel's buffers would absorb the backlog,
/// so the watcher that does not read is the extension's own, in process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_relay_watcher_that_falls_behind_is_ended() {
    let directory = Resolving::new();
    let (model_hub, _model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (global_hub, global_pool) = serve_pool(
        global_default(),
        &directory,
        PoolTiming {
            watch_buffer: 4,
            ..timing()
        },
    )
    .await;
    // Opened once the hub's snapshot says its listings are whole: one
    // opened before waits for them, and so takes its snapshot only when
    // first read.
    let mut watch = follows(&global_hub).await;
    let mut relay = Relay::of(&snapshot(&mut watch).await);
    let mut behind = global_pool.follows();

    let (_quorum, _) = meet_the_quorum(&model_hub).await;
    let mut more = Vec::new();
    for _ in 0..6 {
        let mut instance = Instance::join(&model_hub, "f").await;
        instance.announce(SegmentPhase::Ready).await;
        more.push(instance);
    }
    until(&mut watch, &mut relay, "seven exposed instances", |relay| {
        relay.exposed(POOL).len() == 7
    })
    .await;
    let behind_events = tokio::time::timeout(
        WAIT,
        behind
            .by_ref()
            .filter_map(|event| std::future::ready(common::plain_follow(event)))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("the watch that fell behind ends");
    assert_eq!(behind_events, [FollowEvent::Synced { listed: false }]);
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// Dropping a watch of the relay releases what the hub holds for it, in
/// process and over a connection; stopping the hub ends every watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_relay_watch_releases_it() {
    let directory = Resolving::new();
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    let mut watchers = global_pool.follows_watchers();
    let mut count = async |n: usize| {
        tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching == n))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {n} watchers"))
            .expect("the extension lives");
    };
    count(0).await;
    let inside = global_pool.follows();
    count(1).await;
    let mut outside = follows(&global_hub).await;
    assert_eq!(
        snapshot(&mut outside).await,
        [FollowEvent::Synced { listed: false }]
    );
    count(2).await;
    drop(inside);
    count(1).await;
    drop(outside);
    count(0).await;

    let mut last = global_pool.follows();
    assert_eq!(
        snapshot(&mut last).await,
        [FollowEvent::Synced { listed: false }]
    );
    global_hub.shutdown().await.expect("the hub stops");
    let ended = tokio::time::timeout(WAIT, last.next())
        .await
        .expect("the watch ends in time");
    assert_eq!(ended, None);
}

/// A watch of the relay taken once the hub has stopped ends at once,
/// without the snapshot: a stopped hub's follows are not routable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_hub_relays_nothing() {
    let directory = Resolving::new();
    let (model_hub, _model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    let mut watch = global_pool.follows();
    let mut relay = Relay::of(&snapshot(&mut watch).await);
    until(&mut watch, &mut relay, "the follow", |relay| {
        relay.exposed(POOL).contains(&instances[0].id())
    })
    .await;
    global_hub.shutdown().await.expect("the hub stops");
    let after = tokio::time::timeout(WAIT, global_pool.follows().collect::<Vec<_>>())
        .await
        .expect("the watch ends in time");
    assert_eq!(after, []);
    model_hub.shutdown().await.expect("the hub stops");
}

/// The relay's wire form is JSON tagged by `event`, as the pool watch's
/// is, so that an instance of another build reads it.
#[test]
fn a_relay_event_is_json_tagged_by_its_kind() {
    let instance = InstanceId::new_v4();
    assert_eq!(
        serde_json::to_value(FollowEvent::Left {
            class: "model_x".into(),
            pool: POOL.into(),
            generation: 3,
            segment: "f".into(),
            instance,
        })
        .expect("an event serializes"),
        serde_json::json!({
            "event": "left", "class": "model_x", "pool": POOL, "generation": 3, "segment": "f",
            "instance": instance,
        }),
    );
    assert_eq!(
        serde_json::to_value(FollowEvent::Dropped {
            class: "model_x".into(),
            pool: GLOBAL.into(),
            generation: 1,
        })
        .expect("an event serializes"),
        serde_json::json!({
            "event": "dropped", "class": "model_x", "pool": GLOBAL, "generation": 1,
        }),
    );
}
