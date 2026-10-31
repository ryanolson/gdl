//! A pool's successor hub numbers past its predecessor: a later epoch, and a
//! property-list version above the predecessor's last, so that a
//! reader that compares versions or epochs across the two hubs never sees one
//! go back. Each case starts a real hub over a directory that holds what a
//! predecessor left: its record, the tombstone of its withdrawal, or nothing.
//! Most cases seed the predecessor rather than run it, since its state is
//! what matters and a hub would need more than 2^20 list changes to reach
//! some of it.

mod common;

use common::{POOL, Records, hub_of, millis_now, settings, timing};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gdl_pools::{
    DirectoryError, LastWrite, MemoryDirectory, MemoryWatch, PoolDirectory, PoolRecord,
    ResumeToken, Withdrawal,
};

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

/// How the predecessor left its pool.
#[derive(Clone, Copy)]
enum Left {
    /// Its record stays.
    Listed,
    /// It withdrew, and its tombstone keeps its list version.
    Withdrawn,
    /// It withdrew, as a hub of an earlier build does: its tombstone keeps
    /// no list version.
    WithdrawnByAnEarlierBuild,
}

/// Seed `directory` with the predecessor's record, and its withdrawal as
/// it `left`.
async fn seed(directory: &MemoryDirectory, last: &PoolRecord, left: Left) {
    directory
        .publish(last.clone())
        .await
        .expect("the predecessor's record");
    let properties_version = match left {
        Left::Listed => return,
        Left::Withdrawn => last.properties_version,
        Left::WithdrawnByAnEarlierBuild => 0,
    };
    {
        let withdrawal = Withdrawal {
            class: last.class.clone(),
            pool: last.name.clone(),
            epoch: last.epoch,
            seq: last.seq + 1,
            properties_version,
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
    successor_over(directory, directory.clone(), last).await
}

/// [`successor`], with the hub writing through `through`, a directory over
/// `directory`.
async fn successor_over<D: PoolDirectory>(
    directory: &MemoryDirectory,
    through: D,
    last: &PoolRecord,
) -> PoolRecord {
    let mut records = Records::new(directory);
    let (hub, _pool) = hub_of(settings(), through, timing()).await;
    let first = loop {
        let record = records.next().await;
        if record.version() != last.version() {
            break record;
        }
    };
    hub.shutdown().await.expect("the hub stops");
    first
}

/// Both the epoch and the list version moved on from `last` to `next`.
fn assert_past(last: &PoolRecord, next: &PoolRecord) {
    assert!(
        next.epoch > last.epoch,
        "the successor's epoch must be later: {} -> {}",
        last.epoch,
        next.epoch
    );
    assert!(
        next.properties_version > last.properties_version,
        "the successor's list version must be higher: {} -> {}",
        last.properties_version,
        next.properties_version
    );
}

/// A predecessor that made more than 2^20 list changes in its epoch has a
/// version past the next epoch's floor. While its record is kept, the
/// successor reads the version from it and counts past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_behind_a_kept_record_numbers_past_many_list_changes() {
    let epoch = millis_now() + 600_000;
    let last = predecessor(epoch, floor(epoch) + (1 << 20) + 1024);
    let directory = MemoryDirectory::new();
    seed(&directory, &last, Left::Listed).await;
    assert_past(&last, &successor(&directory, &last).await);
}

/// The same predecessor, withdrawn: the tombstone keeps the list version
/// with the epoch, and the successor reads it, so it counts past it rather
/// than from the next epoch's floor, which is below.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_behind_a_kept_withdrawal_numbers_past_many_list_changes() {
    let epoch = millis_now() + 600_000;
    let last = predecessor(epoch, floor(epoch) + (1 << 20) + 1024);
    let directory = MemoryDirectory::new();
    seed(&directory, &last, Left::Withdrawn).await;
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
    seed(&directory, &last, Left::Withdrawn).await;
    assert_past(&last, &successor(&directory, &last).await);
}

/// The same predecessor, after the directory forgot the pool, as a relay
/// whose state was lost or an emptied table: nothing the successor can read
/// holds the predecessor's epoch or version, so it counts from its own
/// clock, which is behind. No state kept in the directory can order this
/// successor; only a clock, or state kept outside the directory, can.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "cannot pass: a directory that forgot the pool holds nothing to number past, which no order kept in the directory can fix, so a successor whose clock is behind numbers lower (operations/limitations.md)"]
async fn a_successor_after_the_directory_forgot_the_pool_numbers_past_it() {
    let epoch = millis_now() + 60_000;
    let last = predecessor(epoch, floor(epoch) + 1);
    let forgotten = MemoryDirectory::new();
    assert_past(&last, &successor(&forgotten, &last).await);
}

/// A predecessor of an earlier build, withdrawn: its tombstone keeps no list
/// version, so the successor numbers as it did before the tombstone kept
/// one, never worse: one epoch past the tombstone, from that epoch's floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_behind_a_withdrawal_of_an_earlier_build_numbers_as_before() {
    let epoch = millis_now() + 600_000;
    let last = predecessor(epoch, floor(epoch) + 1);
    let directory = MemoryDirectory::new();
    seed(&directory, &last, Left::WithdrawnByAnEarlierBuild).await;
    let next = successor(&directory, &last).await;
    assert_past(&last, &next);
    assert_eq!(next.epoch, epoch + 1, "one past the tombstone");
    assert_eq!(
        next.properties_version,
        floor(epoch + 1),
        "from the epoch's floor"
    );
}

/// A directory whose first read of a pool's last write finds nothing, as a
/// read that raced the tombstone's write: the hub learns of the tombstone
/// only when its first write is refused as not newer.
#[derive(Clone)]
struct Stale {
    inner: MemoryDirectory,
    read: Arc<AtomicBool>,
}

impl PoolDirectory for Stale {
    type Watch = MemoryWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.inner.publish(record).await
    }

    async fn publish_after(
        &self,
        record: PoolRecord,
        last: Option<gdl_pools::Version>,
    ) -> Result<(), DirectoryError> {
        self.inner.publish_after(record, last).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    async fn last_write(
        &self,
        class: &str,
        pool: &str,
    ) -> Result<Option<LastWrite>, DirectoryError> {
        if self.read.swap(true, Ordering::SeqCst) {
            self.inner.last_write(class, pool).await
        } else {
            Ok(None)
        }
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.inner.watch(class, from)
    }
}

/// A successor whose first read missed the tombstone is refused as not
/// newer, reads the pool's last write again, and numbers past its list
/// version as well as its epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_refused_by_a_withdrawal_it_did_not_read_numbers_past_it() {
    let epoch = millis_now() + 600_000;
    let last = predecessor(epoch, floor(epoch) + (1 << 20) + 1024);
    let directory = MemoryDirectory::new();
    seed(&directory, &last, Left::Withdrawn).await;
    let stale = Stale {
        inner: directory.clone(),
        read: Arc::default(),
    };
    assert_past(&last, &successor_over(&directory, stale, &last).await);
}

/// A hub that drains and withdraws leaves a tombstone with its list's last
/// version, and the hub after it numbers past it, though nothing in the
/// list changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hubs_own_withdrawal_keeps_its_list_version() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, pool) = hub_of(settings(), directory.clone(), timing()).await;
    let first = records.next().await;
    pool.drain().await.expect("a drain");
    let mut health = pool.health();
    tokio::time::timeout(common::WAIT, health.wait_for(|health| health.withdrawn))
        .await
        .expect("withdrawn in time")
        .expect("the hub runs");
    hub.shutdown().await.expect("the hub stops");
    let Some(LastWrite::Withdrawn(withdrawal)) = directory.last("model_x", POOL) else {
        panic!("the pool's tombstone");
    };
    assert_eq!(withdrawal.epoch, first.epoch);
    assert!(withdrawal.properties_version >= first.properties_version);
    let last = PoolRecord {
        properties_version: withdrawal.properties_version,
        ..first
    };
    assert_past(&last, &successor(&directory, &last).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_with_a_failed_reread_stays_past_the_retained_tombstone() {
    use std::sync::atomic::AtomicUsize;

    #[derive(Clone)]
    struct FailingReread {
        inner: MemoryDirectory,
        reads: Arc<AtomicUsize>,
    }

    impl PoolDirectory for FailingReread {
        type Watch = MemoryWatch;

        async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
            self.inner.publish(record).await
        }

        async fn publish_after(
            &self,
            record: PoolRecord,
            last: Option<gdl_pools::Version>,
        ) -> Result<(), DirectoryError> {
            self.inner.publish_after(record, last).await
        }

        async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
            self.inner.withdraw(withdrawal).await
        }

        async fn last_write(
            &self,
            class: &str,
            pool: &str,
        ) -> Result<Option<LastWrite>, DirectoryError> {
            match self.reads.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(None),
                1 => Err(DirectoryError::Unavailable("the reread failed".into())),
                _ => self.inner.last_write(class, pool).await,
            }
        }

        fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
            self.inner.watch(class, from)
        }
    }

    let epoch = millis_now() + 600_000;
    let last = predecessor(epoch, floor(epoch) + (1 << 20) + 1024);
    let directory = MemoryDirectory::new();
    seed(&directory, &last, Left::Withdrawn).await;
    assert!(matches!(
        directory.last("model_x", POOL),
        Some(LastWrite::Withdrawn(_))
    ));
    let through = FailingReread {
        inner: directory.clone(),
        reads: Arc::default(),
    };
    assert_past(&last, &successor_over(&directory, through, &last).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_stays_past_a_predecessor_that_writes_during_its_start() {
    #[derive(Clone)]
    struct Racing {
        inner: MemoryDirectory,
        latest: PoolRecord,
        raced: Arc<AtomicBool>,
    }

    impl PoolDirectory for Racing {
        type Watch = MemoryWatch;

        async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
            if !self.raced.swap(true, Ordering::SeqCst) {
                self.inner.publish(self.latest.clone()).await?;
            }
            self.inner.publish(record).await
        }

        // The predecessor's write lands before the hub's first write of
        // either kind.
        async fn publish_after(
            &self,
            record: PoolRecord,
            last: Option<gdl_pools::Version>,
        ) -> Result<(), DirectoryError> {
            if !self.raced.swap(true, Ordering::SeqCst) {
                self.inner.publish(self.latest.clone()).await?;
            }
            self.inner.publish_after(record, last).await
        }

        async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
            self.inner.withdraw(withdrawal).await
        }

        async fn last_write(
            &self,
            class: &str,
            pool: &str,
        ) -> Result<Option<LastWrite>, DirectoryError> {
            self.inner.last_write(class, pool).await
        }

        fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
            self.inner.watch(class, from)
        }
    }

    let epoch = millis_now() + 600_000;
    let last = predecessor(epoch, floor(epoch + 1) - 1);
    let latest = PoolRecord {
        seq: last.seq + 1,
        properties_version: last.properties_version + 2,
        ..last.clone()
    };
    let directory = MemoryDirectory::new();
    seed(&directory, &last, Left::Listed).await;
    let through = Racing {
        inner: directory.clone(),
        latest: latest.clone(),
        raced: Arc::default(),
    };
    let mut records = Records::new(&directory);
    let (hub, _pool) = hub_of(settings(), through, timing()).await;
    let next = loop {
        let record = records.next().await;
        if record.epoch > epoch {
            break record;
        }
    };
    hub.shutdown().await.expect("the hub stops");
    assert_past(&latest, &next);
}
