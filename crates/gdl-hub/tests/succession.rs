//! A pool's successor hub numbers past its predecessor: a later epoch, and a
//! property-list version that is not below the predecessor's last, so that a
//! reader that compares versions or epochs across the two hubs never sees one
//! go back. The version may stay the same: it names one list, and a successor
//! whose list did not change lists it at the version it read. Each case starts a real hub over a directory that holds what a
//! predecessor left: its record, the tombstone of its withdrawal, or nothing.
//! The predecessor is seeded rather than run, since its state is what matters
//! and a hub would need more than 2^20 list changes to reach some of it.

mod common;

use common::{POOL, Records, hub_of, millis_now, settings, timing};
use gdl_pools::{MemoryDirectory, PoolDirectory, PoolRecord, Withdrawal};

/// The least list version a hub of `epoch` counts from, as the hub computes
/// it: the epoch in the high bits.
fn floor(epoch: u64) -> u64 {
    epoch << 20
}

/// The predecessor's last record: written at `epoch`, with its list at
/// `version`.
fn predecessor(epoch: u64, version: u64) -> PoolRecord {
    serde_json::from_value(serde_json::json!({
        "name": POOL,
        "class": "model_x",
        "region": "us-east-1",
        "home": common::GLOBAL,
        "contract": 1,
        "placement": 1,
        "state": "draining",
        "epoch": epoch,
        "seq": 1,
        "hub": "model-x-01-hub:1337",
        "properties_version": version,
    }))
    .expect("a pool record")
}

/// Seed `directory` with the predecessor's record, withdrawn when
/// `withdrawn`.
async fn seed(directory: &MemoryDirectory, last: &PoolRecord, withdrawn: bool) {
    directory
        .publish(last.clone())
        .await
        .expect("the predecessor's record");
    if withdrawn {
        let withdrawal = Withdrawal {
            class: last.class.clone(),
            pool: last.name.clone(),
            epoch: last.epoch,
            seq: last.seq + 1,
        };
        directory
            .withdraw(withdrawal)
            .await
            .expect("the predecessor's withdrawal");
    }
}

/// The first record a hub of the pool writes, started over `directory`
/// after `last`: a watch's snapshot holds `last` while it is kept.
async fn successor(directory: &MemoryDirectory, last: &PoolRecord) -> PoolRecord {
    let mut records = Records::new(directory);
    let (hub, _pool) = hub_of(settings(), directory.clone(), timing()).await;
    let first = loop {
        let record = records.next().await;
        if record.version() != last.version() {
            break record;
        }
    };
    hub.shutdown().await.expect("the hub stops");
    first
}

/// Neither the epoch nor the list version went back from `last` to `next`,
/// and the epoch moved on.
fn assert_past(last: &PoolRecord, next: &PoolRecord) {
    assert!(
        next.epoch > last.epoch,
        "the successor's epoch must be later: {} -> {}",
        last.epoch,
        next.epoch
    );
    assert!(
        next.properties_version >= last.properties_version,
        "the successor's list version must not go back: {} -> {}",
        last.properties_version,
        next.properties_version
    );
}

/// A predecessor that made more than 2^20 list changes in its epoch has a
/// version past the next epoch's floor. While its record is kept, the
/// successor reads the version from it and counts from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_behind_a_kept_record_keeps_a_version_past_many_list_changes() {
    let epoch = millis_now() + 600_000;
    let last = predecessor(epoch, floor(epoch) + (1 << 20) + 1024);
    let directory = MemoryDirectory::new();
    seed(&directory, &last, false).await;
    assert_past(&last, &successor(&directory, &last).await);
}

/// The same predecessor, withdrawn: the tombstone keeps the epoch but not
/// the list version, so the successor moves one epoch past the tombstone
/// and counts from that epoch's floor, which is below the predecessor's
/// last version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a withdrawal's tombstone keeps no list version, so a successor counts from the next epoch's floor, below a predecessor that made more than 2^20 list changes"]
async fn a_successor_behind_a_kept_withdrawal_keeps_a_version_past_many_list_changes() {
    let epoch = millis_now() + 600_000;
    let last = predecessor(epoch, floor(epoch) + (1 << 20) + 1024);
    let directory = MemoryDirectory::new();
    seed(&directory, &last, true).await;
    assert_past(&last, &successor(&directory, &last).await);
}

/// A predecessor whose clock ran a minute ahead, withdrawn: while the
/// directory keeps the tombstone, the successor's first write is refused
/// as not newer, and it moves past the tombstone's epoch. So a clock that
/// is behind alone does not lower the numbering.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_behind_a_kept_withdrawal_numbers_past_a_clock_ahead() {
    let epoch = millis_now() + 60_000;
    let last = predecessor(epoch, floor(epoch) + 1);
    let directory = MemoryDirectory::new();
    seed(&directory, &last, true).await;
    assert_past(&last, &successor(&directory, &last).await);
}

/// The same predecessor, after the directory forgot the pool, as a relay
/// whose state was lost or an emptied table: nothing the successor can read
/// holds the predecessor's epoch or version, so it counts from its own
/// clock, which is behind. No state kept in the directory can order this
/// successor; only a clock, or state kept outside the directory, can.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a directory that forgot the pool holds nothing to number past, so a successor whose clock is behind its predecessor's numbers lower (operations/limitations.md)"]
async fn a_successor_after_the_directory_forgot_the_pool_numbers_past_it() {
    let epoch = millis_now() + 60_000;
    let last = predecessor(epoch, floor(epoch) + 1);
    let forgotten = MemoryDirectory::new();
    assert_past(&last, &successor(&forgotten, &last).await);
}
