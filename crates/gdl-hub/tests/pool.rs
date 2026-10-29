//! A pool's hub: the `gdl-pool` extension counts the usable instances of
//! each segment of the pool's class through its own hub, judges the pool's
//! state, serves it on the pool health endpoint, and publishes the pool's
//! record. Every hub here runs in process on OS-assigned ports, and every
//! wait is for something observed: a status, a record, a response.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::{
    GLOBAL, Gated, Instance, POOL, Records, config, get, global_default, health, hub_of,
    meet_the_quorum, millis_now, pool_hub, settings, spec, timing, usable,
};
use gdl_hub::{GraphAdmission, PoolHealth, PoolReadiness, PoolTiming, SegmentHealth};
use gdl_model::{PoolSettings, SegmentPhase};
use gdl_pools::{MemoryDirectory, PoolDirectory, PoolRecord, PoolState};
use velo_hub::Hub;

/// The usable instances of each segment, as the extension counts them,
/// once it has counted `d` at `count`.
async fn counted(pool: &PoolReadiness<MemoryDirectory>, count: u32) -> PoolHealth {
    health(pool, &format!("d at {count}"), |h| usable(h, "d") == count).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_ready_instances_that_have_not_retired_are_counted() {
    let (hub, pool) = pool_hub(MemoryDirectory::new(), timing()).await;

    // Ready counts.
    let mut first = Instance::join(&hub, "d").await;
    first.announce(SegmentPhase::Loaded).await;
    first.announce(SegmentPhase::Serving).await;
    first.announce(SegmentPhase::Ready).await;
    counted(&pool, 1).await;
    // Ready and retired does not.
    first.announce(SegmentPhase::Retired).await;
    counted(&pool, 0).await;

    // Serving alone does not: with a second instance that only serves
    // and a third that is ready, the count drops to zero when the third
    // leaves the hub, as an instance that leaves stops counting.
    let mut serving = Instance::join(&hub, "d").await;
    serving.announce(SegmentPhase::Loaded).await;
    serving.announce(SegmentPhase::Serving).await;
    let mut third = Instance::join(&hub, "d").await;
    third.announce(SegmentPhase::Ready).await;
    let health = counted(&pool, 1).await;
    assert_eq!(usable(&health, "f"), 0, "f has no instance");
    third.leave().await;
    counted(&pool, 0).await;

    // The retired one withdrawing `ready` changes nothing; the serving one
    // turning ready counts, and stops counting when it leaves.
    first.withdraw(SegmentPhase::Ready).await;
    serving.announce(SegmentPhase::Ready).await;
    counted(&pool, 1).await;
    serving.leave().await;
    counted(&pool, 0).await;
    hub.shutdown().await.expect("the hub stops");
}

/// A ready instance that leaves `retired` counts again: the count follows
/// the retired service both ways, through the hub.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_that_leaves_retired_counts_again() {
    let (hub, pool) = pool_hub(MemoryDirectory::new(), timing()).await;
    let mut instance = Instance::join(&hub, "d").await;
    instance.announce(SegmentPhase::Ready).await;
    counted(&pool, 1).await;
    instance.announce(SegmentPhase::Retired).await;
    counted(&pool, 0).await;
    instance.withdraw(SegmentPhase::Retired).await;
    counted(&pool, 1).await;
    hub.shutdown().await.expect("the hub stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pool_publishes_its_record_and_each_change_of_state() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let before = millis_now();
    let (hub, pool) = pool_hub(directory.clone(), timing()).await;

    // Published at once, before any instance: unavailable.
    let first = records.next().await;
    let layout = gdl_model::layout(&spec().graph, &spec().placement).expect("valid test fixture");
    assert_eq!(
        first,
        PoolRecord {
            dropped: Vec::new(),
            presence: gdl_pools::Presence::Serving,
            properties: Vec::new(),
            // With no record before it, the list counts from the epoch,
            // in the high bits.
            properties_version: first.epoch << 20,
            revoked: Vec::new(),
            name: POOL.into(),
            class: "model_x".into(),
            region: "us-east-1".into(),
            home: Some(GLOBAL.into()),
            contract: layout.contract_hash(),
            placement: layout.placement_hash(),
            state: PoolState::Unavailable,
            epoch: first.epoch,
            seq: first.seq,
            hub: "model-x-01-hub:1337".into(),
            lease: Duration::from_secs(30),
            load: None,
        }
    );
    assert!(first.epoch >= before, "the epoch is the start's time");

    // Ready only once the quorum has held for the hold, degraded meanwhile.
    let (mut instances, began) = meet_the_quorum(&hub).await;
    let (held, _) = records.until(PoolState::Degraded).await;
    let (ready, at) = records.until(PoolState::Ready).await;
    assert!(
        at.duration_since(began) >= timing().hold,
        "ready {:?} after the quorum was met",
        at.duration_since(began)
    );
    assert_eq!(ready.epoch, first.epoch, "one epoch for the hub's life");
    assert_eq!(
        ready.home.as_deref(),
        Some(GLOBAL),
        "every write names the parent"
    );
    assert!(ready.seq > held.seq && held.seq > first.seq);
    assert_eq!(
        health(&pool, "ready", |h| h.state == PoolState::Ready)
            .await
            .segments,
        BTreeMap::from([
            (
                "d".into(),
                SegmentHealth {
                    usable: 2,
                    quorum: 2
                }
            ),
            (
                "f".into(),
                SegmentHealth {
                    usable: 1,
                    quorum: 1
                }
            ),
        ])
    );

    // A drop is published at once: no heartbeat falls in this test.
    let last = instances.pop().expect("a d instance");
    last.leave().await;
    assert_eq!(records.next().await.state, PoolState::Degraded);
    let f = instances.remove(0);
    f.leave().await;
    assert_eq!(records.next().await.state, PoolState::Unavailable);
    hub.shutdown().await.expect("the hub stops");
}

/// A pool of a class that nothing sends to names no parent, even when its
/// settings give one: no chooser of another class offers it work, so it
/// has none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_of_a_class_nothing_sends_to_names_no_parent() {
    let directory = MemoryDirectory::new();
    let mut records = Records::of(&directory, "global", GLOBAL);
    let settings = PoolSettings {
        home: Some("global-02-us-east-1".into()),
        ..global_default()
    };
    let (hub, _pool) = hub_of(settings, directory.clone(), timing()).await;
    let record = records.next().await;
    assert_eq!(record.class, "global");
    assert_eq!(record.home, None, "{record:?}");
    hub.shutdown().await.expect("the hub stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_record_is_republished_on_the_heartbeat() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let timing = PoolTiming {
        heartbeat: Duration::from_millis(100),
        ..timing()
    };
    let (hub, _pool) = pool_hub(directory.clone(), timing).await;
    let first = records.next().await;
    for _ in 0..3 {
        let beat = records.next().await;
        assert_eq!(beat.state, PoolState::Unavailable, "no change of state");
        assert_eq!(beat.epoch, first.epoch);
        assert!(beat.seq > first.seq);
    }
    hub.shutdown().await.expect("the hub stops");
}

/// A hub that starts again for the same pool takes a later epoch: later
/// than its predecessor's, and later than any the directory holds, even
/// one ahead of this host's clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_that_restarts_publishes_with_a_later_epoch() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, _pool) = pool_hub(directory.clone(), timing()).await;
    let before = records.next().await;
    hub.shutdown().await.expect("the hub stops");
    let (hub, _pool) = pool_hub(directory.clone(), timing()).await;
    let after = records.next().await;
    assert!(after.epoch > before.epoch, "{after:?} after {before:?}");
    hub.shutdown().await.expect("the hub stops");

    // A predecessor whose clock ran an hour ahead.
    let ahead = PoolRecord {
        epoch: millis_now() + 3_600_000,
        seq: 1,
        ..after.clone()
    };
    directory.publish(ahead.clone()).await.expect("stored");
    assert_eq!(records.next().await, ahead);
    let (hub, _pool) = pool_hub(directory.clone(), timing()).await;
    let next = records.next().await;
    assert!(next.epoch > ahead.epoch, "{next:?} after {ahead:?}");
    hub.shutdown().await.expect("the hub stops");
}

/// A write the directory refuses as not newer is reported, and the next
/// write moves past what the directory holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_write_is_passed_by_the_next() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let timing = PoolTiming {
        heartbeat: Duration::from_millis(100),
        ..timing()
    };
    let (hub, _pool) = pool_hub(directory.clone(), timing).await;
    let first = records.next().await;
    let ahead = PoolRecord {
        seq: first.seq + 100,
        ..first.clone()
    };
    directory.publish(ahead.clone()).await.expect("stored");
    loop {
        let record = records.next().await;
        if record.seq > ahead.seq {
            assert_eq!(record.epoch, first.epoch);
            break;
        }
    }
    hub.shutdown().await.expect("the hub stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pool_health_endpoint_answers_ready_only_once_the_pool_is() {
    let (hub, pool) = pool_hub(MemoryDirectory::new(), timing()).await;
    let (status, body) = get(&hub, "/v1/ext/gdl-pool/health").await;
    assert_eq!(status, 503, "{body}");
    let body: serde_json::Value = serde_json::from_str(&body).expect("JSON");
    assert_eq!(
        body,
        serde_json::json!({
            "state": "unavailable",
            "segments": {
                "d": { "usable": 0, "quorum": 2 },
                "f": { "usable": 0, "quorum": 1 },
            },
        })
    );
    let (_instances, _) = meet_the_quorum(&hub).await;
    health(&pool, "ready", |h| h.state == PoolState::Ready).await;
    let (status, body) = get(&hub, "/v1/ext/gdl-pool/health").await;
    assert_eq!(status, 200, "{body}");
    let body: PoolHealth = serde_json::from_str(&body).expect("JSON");
    assert_eq!(body.state, PoolState::Ready);
    assert_eq!(usable(&body, "d"), 2);
    hub.shutdown().await.expect("the hub stops");
}

/// A hub given no pool settings runs as before: admission alone, and no
/// pool health endpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_without_a_pool_has_no_pool_endpoint() {
    let hub = Hub::builder(config())
        .extension(GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("the hub serves");
    let (status, _) = get(&hub, "/v1/ext/gdl-pool/health").await;
    assert_eq!(status, 404);
    let instance = Instance::join(&hub, "d").await;
    let (status, members) = get(&hub, "/v1/ext/gdl/members").await;
    assert_eq!(status, 200);
    assert!(members.contains("\"segment\":\"d\""), "{members}");
    instance.leave().await;
    hub.shutdown().await.expect("the hub stops");
}

/// Settings that cannot publish a well-formed record of this deployment's
/// pool are refused when the extension is made, not on every heartbeat.
#[test]
fn settings_a_pools_hub_cannot_publish_are_refused() {
    type Change = fn(&mut PoolSettings);
    let cases: [(&str, Change, PoolTiming); 9] = [
        ("[pool] hub", |pool| pool.hub = None, timing()),
        // Settings made in code, not read: the address is checked here too.
        (
            "[pool] hub: \"http://hub:1337\"",
            |pool| pool.hub = Some("http://hub:1337".into()),
            timing(),
        ),
        (
            "the placement has no class `model_y`",
            |pool| pool.class = "model_y".into(),
            timing(),
        ),
        (
            "[pool] ready.a: not a segment of class `model_x`",
            |pool| {
                pool.ready.insert("a".into(), 1);
            },
            timing(),
        ),
        (
            "pool name `Model_X`",
            |pool| pool.name = "Model_X".into(),
            timing(),
        ),
        (
            "the lease",
            |_| {},
            PoolTiming {
                lease: Duration::ZERO,
                ..timing()
            },
        ),
        (
            "heartbeat",
            |_| {},
            PoolTiming {
                heartbeat: Duration::ZERO,
                ..timing()
            },
        ),
        (
            "watch buffer",
            |_| {},
            PoolTiming {
                watch_buffer: 0,
                ..timing()
            },
        ),
        (
            "refollow",
            |_| {},
            PoolTiming {
                refollow: Duration::ZERO,
                ..timing()
            },
        ),
    ];
    for (expected, change, timing) in cases {
        let mut pool = settings();
        change(&mut pool);
        let Err(error) = PoolReadiness::new(&spec(), pool, MemoryDirectory::new(), timing) else {
            panic!("accepted settings without {expected}");
        };
        assert!(error.to_string().contains(expected), "{error}");
    }
}

/// The pool's judgment does not wait for the directory: while a write of
/// `ready` is held, an instance that leaves makes the pool degraded on the
/// health endpoint at once. Once the directory takes writes again, it
/// gets the pool's latest state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_write_does_not_hold_the_judgment() {
    let gate = Gated::new();
    let mut records = Records::new(&gate.inner);
    // A hold long enough to close the gate in before it ends.
    let timing = PoolTiming {
        hold: Duration::from_secs(1),
        ..timing()
    };
    let (hub, pool) = pool_hub(gate.clone(), timing).await;
    records.next().await;
    let (mut instances, _) = meet_the_quorum(&hub).await;
    records.until(PoolState::Degraded).await;
    gate.close();
    health(&pool, "ready", |h| h.state == PoolState::Ready).await;
    gate.holding().await;

    let last = instances.pop().expect("a d instance");
    last.leave().await;
    let judged = health(&pool, "the drop", |h| usable(h, "d") == 1).await;
    assert_eq!(judged.state, PoolState::Degraded);
    let (status, body) = get(&hub, "/v1/ext/gdl-pool/health").await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(gate.holds(), 1, "the write of `ready` is still held");

    gate.open();
    records.until(PoolState::Degraded).await;
    hub.shutdown().await.expect("the hub stops");
}
