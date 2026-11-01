//! The number of a pool's load summaries. A pool's hub numbers each
//! summary one past the last, and a hub that starts again numbers from 1
//! under a later epoch, so the pair (epoch, sequence) a reader sees never
//! goes back: across a restart of the pool's hub, and when the hub raises
//! its epoch before its first write is stored. Every hub runs in process on
//! OS-assigned ports; every wait is bounded and for something observed.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use common::{Instance, POOL, Resolving, WAIT, hub_of, meet_the_quorum, model, serve_pool, timing};
use futures::StreamExt;
use gdl_hub::PoolWatch;
use gdl_model::PoolSettings;
use gdl_pools::{
    DirectoryError, LastWrite, PoolDirectory, PoolRecord, ResumeToken, Version, Withdrawal,
};
use gdl_watch::{LoadReport, PoolWatchEvent};

/// The model pool, with the shortest summary period, 1 s: each test here
/// waits for a few summaries, so runs a few seconds.
fn pool() -> PoolSettings {
    PoolSettings {
        summary: Duration::from_secs(1),
        ..model(POOL, "us-east-1", "model-x-01-hub:1337")
    }
}

/// A watch of the pool: its epoch, from the snapshot's first event, and
/// the watch.
async fn watch<D: PoolDirectory>(pool: &gdl_hub::PoolReadiness<D>) -> (u64, PoolWatch) {
    let mut watch = tokio::time::timeout(WAIT, pool.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    let first = tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("an event in time")
        .expect("the watch goes on");
    let PoolWatchEvent::Pool { epoch, .. } = first else {
        panic!("a watch begins with the pool: {first:?}");
    };
    (epoch, watch)
}

/// The numbers of the next `count` summaries `watch` sends.
async fn numbers(watch: &mut PoolWatch, count: usize) -> Vec<u64> {
    tokio::time::timeout(WAIT, async {
        let mut numbers = Vec::new();
        while numbers.len() < count {
            if let PoolWatchEvent::Load { load: Some(load) } =
                watch.next().await.expect("the watch goes on")
            {
                numbers.push(load.sequence);
            }
        }
        numbers
    })
    .await
    .expect("the summaries in time")
}

/// Report from `instance` until `numbers` ends: the hub makes a summary
/// only while it counts a report.
async fn reporting<T>(instance: &Instance, numbers: impl Future<Output = T>) -> T {
    let report = async {
        loop {
            instance
                .report(LoadReport {
                    admitted: 1,
                    limit: 8,
                    ..Default::default()
                })
                .await
                .expect("the hub takes it");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    };
    tokio::select! {
        () = report => unreachable!("reports go on"),
        numbers = numbers => numbers,
    }
}

/// A hub numbers its summaries one past the last; its successor, on the
/// same directory, numbers from 1 again under a later epoch, so the pair
/// it sends is past every pair its predecessor sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_hub_numbers_past_its_predecessor() {
    let directory = Resolving::new();
    let (first_hub, first) = serve_pool(pool(), &directory, timing()).await;
    let (instances, _) = meet_the_quorum(&first_hub).await;
    let (first_epoch, mut first_watch) = watch(&first).await;
    let before = reporting(&instances[0], numbers(&mut first_watch, 3)).await;
    assert!(
        before.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "one past the last: {before:?}"
    );
    drop(instances);
    first_hub.shutdown().await.expect("the hub stops");

    let (second_hub, second) = serve_pool(pool(), &directory, timing()).await;
    let (instances, _) = meet_the_quorum(&second_hub).await;
    let (second_epoch, mut second_watch) = watch(&second).await;
    let after = reporting(&instances[0], numbers(&mut second_watch, 2)).await;
    assert!(second_epoch > first_epoch);
    let last = (first_epoch, *before.last().expect("numbers"));
    for number in after {
        assert!(
            (second_epoch, number) > last,
            "({second_epoch}, {number}) goes back from {last:?}"
        );
    }
    second_hub.shutdown().await.expect("the hub stops");
}

/// A directory that refuses the hub's first write as not newer, as if
/// another write had landed after the hub read the pool's last write: the
/// hub raises its epoch past the one held and writes again.
#[derive(Clone)]
struct Overtaken {
    inner: Resolving,
    overtaken: Arc<AtomicBool>,
    /// The epoch of the write refused.
    refused: Arc<AtomicU64>,
}

impl PoolDirectory for Overtaken {
    type Watch = <Resolving as PoolDirectory>::Watch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.inner.publish(record).await
    }

    async fn publish_after(
        &self,
        record: PoolRecord,
        last: Option<Version>,
    ) -> Result<(), DirectoryError> {
        if !self.overtaken.swap(true, Ordering::SeqCst) {
            self.refused.store(record.epoch, Ordering::SeqCst);
            return Err(DirectoryError::NotNewer {
                class: record.class.clone(),
                pool: record.name.clone(),
                held: Version {
                    epoch: record.epoch + 5_000,
                    seq: 0,
                },
                offered: record.version(),
            });
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

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch {
        self.inner.watch(class, from)
    }
}

/// A hub whose first write is refused raises its epoch while it runs. Its
/// watches begin only once a write is stored, under the raised epoch, and
/// the summaries keep their numbers, which the raise does not reset: the
/// pair a reader sees grows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_whose_first_write_is_refused_still_numbers_forward() {
    let directory = Overtaken {
        inner: Resolving::new(),
        overtaken: Arc::new(AtomicBool::new(false)),
        refused: Arc::new(AtomicU64::new(0)),
    };
    let (hub, readiness) = hub_of(pool(), directory.clone(), timing()).await;
    let name = pool().hub.expect("named");
    directory.inner.resolve(&name, &hub);
    let (instances, _) = meet_the_quorum(&hub).await;
    let (epoch, mut watch) = watch(&readiness).await;
    assert!(
        directory.overtaken.load(Ordering::SeqCst),
        "the write was refused"
    );
    assert!(
        epoch > directory.refused.load(Ordering::SeqCst),
        "the watch's epoch is the raised one"
    );
    let seen = reporting(&instances[0], numbers(&mut watch, 3)).await;
    assert!(
        seen[0] > 0 && seen.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "one past the last: {seen:?}"
    );
    hub.shutdown().await.expect("the hub stops");
}
