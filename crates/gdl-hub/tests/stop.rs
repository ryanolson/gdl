//! A pool's hub that is stopped by draining ([`gdl_hub::PoolStop::run`],
//! what the hub binary runs on its first signal under
//! `[pool] drain_on_stop`): a pool that a hop enters drains, as
//! `POST /v1/ext/gdl-pool/drain` drains it, and the stop ends once its
//! record is withdrawn; a pool whose class no hop enters, a global pool,
//! cannot drain, so the stop ends once its instances have left, whatever
//! phase they announce, and its record is left as it is. Either stop ends
//! at its wait if that comes first, and says which. Every hub runs in
//! process on OS-assigned ports, and every wait is for something
//! observed.

mod common;

use std::time::Duration;

use common::{GLOBAL, Instance, POOL, Records, WAIT, global_default, hub_of, meet_the_quorum};
use gdl_hub::{GraphAdmission, Stopped};
use gdl_model::SegmentPhase;
use gdl_pools::{LastWrite, MemoryDirectory, PoolDirectory, PoolState};
use tokio::task::JoinHandle;

/// Start the stop of `hub`'s pool, bounded by `wait`, on a task of its
/// own, as the hub binary finds it: through the hub's admission.
fn stopping(hub: &velo_hub::Hub, wait: Duration) -> JoinHandle<Stopped> {
    let stop = hub
        .extension::<GraphAdmission>()
        .and_then(|admission| admission.pool_stop())
        .expect("a pool's hub has a stop");
    tokio::spawn(async move { stop.run(wait).await })
}

/// How a stop started by [`stopping`] ended, within [`WAIT`].
async fn stopped(stop: JoinHandle<Stopped>) -> Stopped {
    tokio::time::timeout(WAIT, stop)
        .await
        .expect("the stop ends in time")
        .expect("the stop's task")
}

/// The last write the directory holds of `class`'s pool `pool`.
async fn last_write(directory: &MemoryDirectory, class: &str, pool: &str) -> Option<LastWrite> {
    directory
        .last_write(class, pool)
        .await
        .expect("the directory answers")
}

/// The stop drains a pool that a hop enters, as the drain request does,
/// and ends only once the pool's record is withdrawn: not while an
/// instance is still a member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_pool_hub_drains_its_pool_and_ends_once_its_record_is_withdrawn() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, pool) = hub_of(common::settings(), directory.clone(), common::timing()).await;
    let (instances, _) = meet_the_quorum(&hub).await;
    records.until(PoolState::Ready).await;

    let stop = stopping(&hub, WAIT * 4);
    records.until(PoolState::Draining).await;
    assert_eq!(pool.health().borrow().state, PoolState::Draining);
    assert!(!stop.is_finished(), "the stop ended with members left");
    for instance in instances {
        instance.leave().await;
    }
    assert_eq!(stopped(stop).await, Stopped::Withdrawn);
    assert!(pool.health().borrow().withdrawn);
    assert!(
        matches!(
            last_write(&directory, "model_x", POOL).await,
            Some(LastWrite::Withdrawn(_))
        ),
        "the record is not withdrawn"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A member that does not leave keeps the drain from ending: the stop ends
/// at its wait, says so, and names how many members are left; the
/// record is still `draining`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_pool_hub_whose_pool_does_not_drain_ends_at_its_wait() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, pool) = hub_of(common::settings(), directory.clone(), common::timing()).await;
    let (_instances, _) = meet_the_quorum(&hub).await;
    records.until(PoolState::Ready).await;

    let wait = Duration::from_millis(300);
    let ended = stopped(stopping(&hub, wait)).await;
    assert_eq!(ended, Stopped::Waited { wait, members: 3 });
    assert!(!pool.health().borrow().withdrawn);
    assert!(
        matches!(
            last_write(&directory, "model_x", POOL).await,
            Some(LastWrite::Record(record)) if record.state == PoolState::Draining
        ),
        "the record is not draining"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A global pool cannot drain, so its stop waits until its instances have
/// left: an instance that has announced that it drained is still a member,
/// and is waited for, since a source's instance announces that while it
/// still holds admitted work. The pool is not drained, and its record is
/// not withdrawn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_global_hub_ends_once_its_instances_have_left() {
    let directory = MemoryDirectory::new();
    let mut records = Records::of(&directory, "global", GLOBAL);
    let (hub, pool) = hub_of(global_default(), directory.clone(), common::timing()).await;
    let mut instance = Instance::join(&hub, "a").await;
    instance.announce(SegmentPhase::Ready).await;
    let (ready, _) = records.until(PoolState::Ready).await;

    let stop = stopping(&hub, WAIT * 4);
    instance.withdraw(SegmentPhase::Ready).await;
    instance.announce(SegmentPhase::Retired).await;
    instance.announce(SegmentPhase::Drained).await;
    // The hub answers the instance as a member after its announcements.
    assert!(instance.registered().await, "the instance is not a member");
    assert!(!stop.is_finished(), "the stop ended with a member left");
    assert_ne!(pool.health().borrow().state, PoolState::Draining);
    instance.leave().await;
    assert_eq!(stopped(stop).await, Stopped::Left);
    match last_write(&directory, "global", GLOBAL).await {
        Some(LastWrite::Record(record)) => {
            assert_eq!(record.epoch, ready.epoch);
            assert_ne!(record.state, PoolState::Draining);
        }
        other => panic!("the global pool's record changed to {other:?}"),
    }
    hub.shutdown().await.expect("the hub stops");
}

/// A global hub's stop with an instance that never leaves, as a joiner
/// started without a deadline, ends at its wait, naming the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_global_hub_with_a_member_that_stays_ends_at_its_wait() {
    let directory = MemoryDirectory::new();
    let mut records = Records::of(&directory, "global", GLOBAL);
    let (hub, _pool) = hub_of(global_default(), directory.clone(), common::timing()).await;
    let mut instance = Instance::join(&hub, "a").await;
    instance.announce(SegmentPhase::Ready).await;
    records.until(PoolState::Ready).await;

    let wait = Duration::from_millis(300);
    assert_eq!(
        stopped(stopping(&hub, wait)).await,
        Stopped::Waited { wait, members: 1 }
    );
    assert!(instance.registered().await);
    hub.shutdown().await.expect("the hub stops");
}

/// A global hub with no instance stops at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_global_hub_with_no_instance_ends_at_once() {
    let directory = MemoryDirectory::new();
    let (hub, _pool) = hub_of(global_default(), directory.clone(), common::timing()).await;
    assert_eq!(stopped(stopping(&hub, WAIT * 4)).await, Stopped::Left);
    hub.shutdown().await.expect("the hub stops");
}

/// The drain request and the stop are one drain: a drain asked for after
/// the stop is answered as a second drain, and a stop after a drain
/// waits for the same withdrawal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_and_the_drain_request_are_one_drain() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, pool) = hub_of(common::settings(), directory.clone(), common::timing()).await;
    let (instances, _) = meet_the_quorum(&hub).await;
    records.until(PoolState::Ready).await;

    let drained = pool.drain().await.expect("the pool drains");
    assert_eq!(drained.state, PoolState::Draining);
    let stop = stopping(&hub, WAIT * 4);
    let again = pool.drain().await.expect("a second drain");
    assert_eq!(again.state, PoolState::Draining);
    assert!(!stop.is_finished(), "the stop ended with members left");
    for instance in instances {
        instance.leave().await;
    }
    assert_eq!(stopped(stop).await, Stopped::Withdrawn);
    hub.shutdown().await.expect("the hub stops");
}

/// A program that cannot name the pool's directory type finds the pool's
/// stop through the hub's admission, the one extension every gdl hub
/// runs; a hub without a pool has none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pools_stop_is_found_through_the_hubs_admission() {
    let directory = MemoryDirectory::new();
    let (hub, _pool) = hub_of(global_default(), directory.clone(), common::timing()).await;
    let stop = hub
        .extension::<GraphAdmission>()
        .expect("admission")
        .pool_stop()
        .expect("a pool's hub has a stop");
    assert_eq!(
        tokio::time::timeout(WAIT, stop.run(WAIT)).await,
        Ok(Stopped::Left)
    );
    hub.shutdown().await.expect("the hub stops");

    let plain = velo_hub::Hub::builder(common::config())
        .extension(GraphAdmission::new(common::spec()))
        .serve()
        .await
        .expect("a hub without a pool");
    assert!(
        plain
            .extension::<GraphAdmission>()
            .expect("admission")
            .pool_stop()
            .is_none()
    );
    plain.shutdown().await.expect("the hub stops");
}
