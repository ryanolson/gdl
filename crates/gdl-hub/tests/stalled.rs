mod common;

use std::sync::Arc;

use common::{POOL, WAIT, get, health, meet_the_quorum, millis_now, pool_hub, timing};
use gdl_pools::{
    DirectoryError, LastWrite, MemoryDirectory, MemoryWatch, PoolDirectory, PoolRecord, PoolState,
    ResumeToken, Version, Withdrawal,
};
use tokio::sync::watch;

#[derive(Clone)]
struct Unreadable {
    inner: MemoryDirectory,
    reads: Arc<watch::Sender<usize>>,
}

impl PoolDirectory for Unreadable {
    type Watch = MemoryWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.inner.publish(record).await
    }

    async fn publish_after(
        &self,
        record: PoolRecord,
        last: Option<Version>,
    ) -> Result<(), DirectoryError> {
        self.inner.publish_after(record, last).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    async fn last_write(
        &self,
        _class: &str,
        _pool: &str,
    ) -> Result<Option<LastWrite>, DirectoryError> {
        let mut read = 0;
        self.reads.send_modify(|reads| {
            *reads += 1;
            read = *reads;
        });
        if read == 1 {
            Ok(None)
        } else {
            Err(DirectoryError::Unavailable(
                "the retained pool item does not decode".into(),
            ))
        }
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.inner.watch(class, from)
    }
}

/// A refused first write followed by permanently failed reads leaves the
/// publisher waiting. Quorum alone must not report that hub as healthy.
/// The pool watch takes its five-second startup bound before refusing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "F7: health answers 200 while the first write is refused and its rereads fail indefinitely"]
async fn health_exposes_a_publisher_stuck_reading_its_last_write() {
    let inner = MemoryDirectory::new();
    let last = Withdrawal {
        class: "model_x".into(),
        pool: POOL.into(),
        epoch: millis_now() + 600_000,
        seq: 2,
        properties_version: 1,
    };
    inner.withdraw(last.clone()).await.expect("the tombstone");
    let reads = Arc::new(watch::Sender::new(0));
    let mut observed = reads.subscribe();
    let (hub, pool) = pool_hub(
        Unreadable {
            inner: inner.clone(),
            reads,
        },
        timing(),
    )
    .await;
    let (_instances, _) = meet_the_quorum(&hub).await;
    health(&pool, "the own quorum is ready", |health| {
        health.state == PoolState::Ready
    })
    .await;
    tokio::time::timeout(WAIT, observed.wait_for(|reads| *reads >= 3))
        .await
        .expect("the refused write retries its failed read")
        .expect("the publisher runs");
    assert_eq!(
        inner.last("model_x", POOL),
        Some(LastWrite::Withdrawn(last)),
        "no successor record has been stored"
    );
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-pool/watch",
        hub.public_addr().port()
    );
    let watching = tokio::time::timeout(std::time::Duration::from_secs(8), reqwest::get(url))
        .await
        .expect("the pool watch answers within its startup bound")
        .expect("the hub answers");
    assert_eq!(
        watching.status(),
        503,
        "the hub cannot serve its pool watch"
    );
    let (status, body) = get(&hub, "/v1/ext/gdl-pool/health").await;
    hub.shutdown().await.expect("the failed-read loop cancels");
    assert_eq!(
        status, 503,
        "a hub that cannot settle its epoch or serve its pool watch must expose the stalled publisher: {body}"
    );
}
