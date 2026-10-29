//! A pool's drain at its hub: `POST /v1/ext/gdl-pool/drain` on the control
//! listener makes the pool `draining` whatever its quorum, on its health
//! endpoint, its record, and its watch at once; once the last of its
//! instances has left the hub, the hub withdraws the pool's record and ends
//! its watches. Every hub runs in process on OS-assigned ports, and every
//! wait is for something observed.

mod common;

use common::{
    GLOBAL, Gated, Instance, POOL, Records, WAIT, get, global_default, health, hub_of,
    meet_the_quorum, pool_hub, timing, usable,
};
use futures::{FutureExt, StreamExt};
use gdl_hub::{PoolHealth, PoolWatch};
use gdl_pools::{MemoryDirectory, MemoryWatch, PoolDirectory, PoolEvent, PoolState};
use gdl_watch::PoolWatchEvent;
use velo_hub::Hub;

/// Ask the hub to drain its pool: the status and the body.
async fn drain(hub: &Hub) -> (u16, String) {
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-pool/drain",
        hub.control_addr().port()
    );
    let response = tokio::time::timeout(WAIT, reqwest::Client::new().post(&url).send())
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    let status = response.status().as_u16();
    (status, response.text().await.expect("a body"))
}

/// A ready pool that is drained is `draining` on its health endpoint
/// (503), its watch, and its record by the time the drain is answered;
/// a second drain changes nothing, and a quorum that changes afterwards
/// leaves it `draining`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_pool_is_draining_at_once_whatever_its_quorum() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, pool) = pool_hub(directory.clone(), timing()).await;
    let (mut instances, _) = meet_the_quorum(&hub).await;
    records.until(PoolState::Ready).await;
    let mut watch = pool.watch().await.expect("the watch begins");
    loop {
        let event = tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("the snapshot in time")
            .expect("the watch goes on");
        if event == PoolWatchEvent::Synced {
            break;
        }
    }

    let (status, body) = drain(&hub).await;
    assert_eq!(status, 202, "{body}");
    let answered: PoolHealth = serde_json::from_str(&body).expect("the pool's health");
    assert_eq!(answered.state, PoolState::Draining);
    let (status, body) = get(&hub, "/v1/ext/gdl-pool/health").await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(
        watch.next().now_or_never(),
        Some(Some(PoolWatchEvent::State {
            state: PoolState::Draining,
            unfollowed: false,
        })),
        "the watch did not say draining by the time the drain was answered"
    );
    // No pool follows this one, so it says so with the drain.
    assert_eq!(
        watch.next().now_or_never(),
        Some(Some(PoolWatchEvent::State {
            state: PoolState::Draining,
            unfollowed: true,
        }))
    );
    let (record, _) = records.until(PoolState::Draining).await;

    let (status, body) = drain(&hub).await;
    assert_eq!(status, 202, "a second drain: {body}");
    let instance = instances.pop().expect("an instance of d");
    instance.leave().await;
    let counted = health(&pool, "d at 1", |health| usable(health, "d") == 1).await;
    assert_eq!(
        counted.state,
        PoolState::Draining,
        "the quorum ended the drain"
    );
    assert_eq!(
        watch.next().now_or_never(),
        None,
        "a second drain or a quorum lost changed the state"
    );
    assert!(
        records.next().now_or_never().is_none(),
        "the record was written again after {record:?}"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A pool whose class no hop enters, a global pool, cannot be drained as
/// a pool yet: the drain is refused, and the pool's state is left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_whose_class_no_hop_enters_is_not_drained() {
    let directory = MemoryDirectory::new();
    let mut records = Records::of(&directory, "global", GLOBAL);
    let (hub, pool) = hub_of(global_default(), directory.clone(), timing()).await;
    let mut instance = Instance::join(&hub, "a").await;
    instance.announce(gdl_model::SegmentPhase::Ready).await;
    records.until(PoolState::Ready).await;

    let (status, body) = drain(&hub).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(pool.health().borrow().state, PoolState::Ready);
    hub.shutdown().await.expect("the hub stops");
}

/// The next event of the model pool `POOL` in `watch`, a directory watch
/// of its class: a record, or its withdrawal.
async fn next_of_pool(watch: &mut MemoryWatch) -> PoolEvent {
    let next = async {
        loop {
            match watch.next().await {
                Some(Ok(
                    event @ (PoolEvent::Current(_)
                    | PoolEvent::Put { .. }
                    | PoolEvent::Withdrawn { .. }),
                )) if named(&event) == POOL => return event,
                Some(Ok(_)) => {}
                other => panic!("the directory's watch ended: {other:?}"),
            }
        }
    };
    tokio::time::timeout(WAIT, next)
        .await
        .expect("timed out waiting for the pool in the directory")
}

fn named(event: &PoolEvent) -> &str {
    match event {
        PoolEvent::Current(record) | PoolEvent::Put { record, .. } => &record.name,
        PoolEvent::Withdrawn { withdrawal, .. } => &withdrawal.pool,
        _ => "",
    }
}

/// The watch's next event, within [`WAIT`]: `None` once it has ended.
async fn after(watch: &mut PoolWatch) -> Option<PoolWatchEvent> {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("the watch moves on in time")
}

/// A draining pool's record stays while any of its instances is a member
/// of the hub, and is withdrawn, after the last write of it, once the last
/// one leaves; its watches end then, and a new one is refused. The health
/// endpoint still says the pool is draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_pool_is_withdrawn_once_its_last_instance_leaves() {
    let directory = MemoryDirectory::new();
    let mut directory_watch = directory.watch("model_x", None);
    let (hub, pool) = pool_hub(directory.clone(), timing()).await;
    let (mut instances, _) = meet_the_quorum(&hub).await;
    let mut watch = pool.watch().await.expect("the watch begins");
    while after(&mut watch).await != Some(PoolWatchEvent::Synced) {}

    pool.drain().await.expect("the pool drains");
    let draining = loop {
        match next_of_pool(&mut directory_watch).await {
            PoolEvent::Current(record) | PoolEvent::Put { record, .. }
                if record.state == PoolState::Draining =>
            {
                break record;
            }
            PoolEvent::Withdrawn { .. } => panic!("withdrawn while its instances are members"),
            _ => {}
        }
    };
    let last = instances.pop().expect("an instance");
    for instance in instances {
        instance.leave().await;
    }
    assert!(
        directory_watch.next().now_or_never().is_none(),
        "the record changed while an instance is a member"
    );

    last.leave().await;
    match next_of_pool(&mut directory_watch).await {
        PoolEvent::Withdrawn { withdrawal, .. } => {
            assert_eq!(withdrawal.epoch, draining.epoch);
            assert!(
                withdrawal.seq > draining.seq,
                "the withdrawal is not after the last write: {withdrawal:?} {draining:?}"
            );
        }
        other => panic!("not withdrawn: {other:?}"),
    }
    loop {
        match after(&mut watch).await {
            None => break,
            Some(PoolWatchEvent::Left { .. } | PoolWatchEvent::Load { .. }) => {}
            Some(other) => panic!("the watch went on with {other:?}"),
        }
    }
    assert!(
        pool.watch().await.is_err(),
        "a watch of a withdrawn pool began"
    );
    let (status, body) = get(&hub, "/v1/ext/gdl-pool/health").await;
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("draining"), "{body}");
    hub.shutdown().await.expect("the hub stops");
}

/// Once a drained pool's record is withdrawn, its hub refuses an instance
/// that asks to join: the pool's watch, from which an instance learns of
/// the drain, is gone, and nothing publishes the pool for it to serve.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_pool_refuses_an_instance_once_it_is_withdrawn() {
    let directory = MemoryDirectory::new();
    let mut directory_watch = directory.watch("model_x", None);
    let (hub, pool) = pool_hub(directory.clone(), timing()).await;
    pool.drain().await.expect("the pool drains");
    while !matches!(
        next_of_pool(&mut directory_watch).await,
        PoolEvent::Withdrawn { .. }
    ) {}

    let joined = Instance::try_join(&hub, &common::spec(), "f").await;
    assert!(
        joined.is_err(),
        "a drained, withdrawn pool admitted an instance"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A pool drained with no instance at its hub is withdrawn at once, after
/// its record has said it is draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_pool_with_no_instance_is_withdrawn_at_once() {
    let directory = MemoryDirectory::new();
    let mut directory_watch = directory.watch("model_x", None);
    let (hub, pool) = pool_hub(directory.clone(), timing()).await;
    let mut records = Records::new(&directory);
    records.until(PoolState::Unavailable).await;

    pool.drain().await.expect("the pool drains");
    let mut seen = Vec::new();
    loop {
        let event = next_of_pool(&mut directory_watch).await;
        let withdrawn = matches!(event, PoolEvent::Withdrawn { .. });
        seen.push(match event {
            PoolEvent::Current(record) | PoolEvent::Put { record, .. } => Some(record.state),
            _ => None,
        });
        if withdrawn {
            break;
        }
    }
    assert_eq!(
        seen.iter().rev().nth(1),
        Some(&Some(PoolState::Draining)),
        "not draining before it was withdrawn: {seen:?}"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// The record the directory holds for the model pool now, if any.
async fn held(directory: &MemoryDirectory) -> Option<gdl_pools::PoolRecord> {
    let mut watch = directory.watch("model_x", None);
    let mut record = None;
    let read = async {
        while let Some(Ok(event)) = watch.next().await {
            match event {
                PoolEvent::Current(current) if current.name == POOL => record = Some(current),
                PoolEvent::Synced { .. } => return,
                _ => {}
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the directory's snapshot in time");
    record
}

/// A drain is answered only once the directory has stored the record
/// that says the pool is draining: while the directory holds the write,
/// the drain waits for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_is_answered_once_the_draining_record_is_stored() {
    let directory = Gated::new();
    let mut records = Records::new(&directory.inner);
    let (hub, pool) = pool_hub(directory.clone(), timing()).await;
    let (_instances, _) = meet_the_quorum(&hub).await;
    records.until(PoolState::Ready).await;

    directory.close();
    let inner = directory.inner.clone();
    let draining = tokio::spawn(async move {
        pool.drain().await.expect("the pool drains");
        held(&inner).await.map(|record| record.state)
    });
    directory.holding().await;
    directory.open();
    let stored = tokio::time::timeout(WAIT, draining)
        .await
        .expect("the drain answered in time")
        .expect("the drain's task");
    assert_eq!(
        stored,
        Some(PoolState::Draining),
        "the drain was answered before its record was stored"
    );
    hub.shutdown().await.expect("the hub stops");
}
