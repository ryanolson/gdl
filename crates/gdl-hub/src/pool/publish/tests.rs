use super::*;
use gdl_pools::{MemoryDirectory, MemoryWatch, ResumeToken};
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct Delayed {
    inner: MemoryDirectory,
    committed: watch::Sender<Option<Withdrawal>>,
    reply: Arc<Notify>,
}

impl PoolDirectory for Delayed {
    type Watch = MemoryWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal.clone()).await?;
        self.committed.send_replace(Some(withdrawal));
        self.reply.notified().await;
        Ok(())
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.inner.watch(class, from)
    }
}

async fn withdrawn_after_reply(republish: bool) -> bool {
    let directory = Delayed {
        inner: MemoryDirectory::new(),
        committed: watch::Sender::new(None),
        reply: Arc::new(Notify::new()),
    };
    let record: PoolRecord = serde_json::from_value(serde_json::json!({
        "name": "model-a",
        "class": "model",
        "region": "us",
        "home": "global",
        "contract": 1,
        "placement": 1,
        "state": "draining",
        "epoch": 0,
        "seq": 0,
        "hub": "127.0.0.1:1337"
    }))
    .expect("a pool record");
    let feed = Arc::new(Feed::new(
        record.name.clone(),
        record.class.clone(),
        vec![("engine".into(), true)],
        Duration::from_secs(1),
        16,
        Duration::from_secs(30),
        Vec::new(),
    ));
    let health = watch::Sender::new(PoolHealth::of(
        &crate::Quorum::new([("engine", 1)], Duration::from_secs(1)),
        PoolState::Draining,
        &Default::default(),
    ));
    let (states, state) = watch::channel(PoolState::Draining);
    let (presence, asked) = watch::channel(Asked {
        presence: Presence::Serving,
        request: 0,
    });
    let drain = Arc::new(Drain::new(["engine".into()]));
    drain.start();
    let publisher = Publisher {
        directory: Arc::new(directory.clone()),
        record: record.clone(),
        heartbeat: Duration::from_secs(10),
        cadence: None,
        feed: Arc::clone(&feed),
        drain,
        stored: watch::Sender::new(None),
        parent: None,
        health: health.clone(),
    };
    let sources = Sources {
        states: state,
        presence: asked,
        loads: watch::Sender::new(None).subscribe(),
        properties: feed.properties(),
    };
    let mut committed = directory.committed.subscribe();
    let publishing = tokio::spawn(publisher.run(sources, CancellationToken::new()));
    let withdrawal = tokio::time::timeout(WAIT, committed.wait_for(Option::is_some))
        .await
        .expect("the withdrawal is stored in time")
        .expect("the directory stays open")
        .clone()
        .expect("the stored withdrawal");
    assert!(
        directory
            .inner
            .record(&record.class, &record.name)
            .is_none()
    );
    assert!(!health.borrow().withdrawn, "the store has not answered yet");

    if republish {
        let successor = PoolRecord {
            epoch: withdrawal.epoch + 1,
            seq: 1,
            state: PoolState::Ready,
            ..record.clone()
        };
        directory
            .inner
            .publish(successor)
            .await
            .expect("a later hub publishes");
    }
    directory.reply.notify_one();
    tokio::time::timeout(WAIT, publishing)
        .await
        .expect("the publisher ends in time")
        .expect("the publisher task");
    assert_eq!(
        directory
            .inner
            .record(&record.class, &record.name)
            .is_some(),
        republish
    );
    drop((states, presence));
    health.borrow().withdrawn
}

#[tokio::test]
async fn a_stored_withdrawal_with_no_later_writer_is_proof() {
    assert!(withdrawn_after_reply(false).await);
}

#[tokio::test]
async fn a_withdrawal_replaced_before_its_reply_is_not_proof() {
    assert!(
        !withdrawn_after_reply(true).await,
        "the hub says withdrawn while the directory holds the later hub's record"
    );
}

#[tokio::test]
#[ignore = "F7: correcting clocks and restarting advances the current record but can stay below the lost predecessor's future epoch"]
async fn a_restart_with_synchronized_clocks_overtakes_a_lost_future_epoch() {
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock follows the epoch")
            .as_millis(),
    )
    .expect("the time fits");
    let predecessor_epoch = now + 3_600_000;
    let directory = MemoryDirectory::new();
    let record: PoolRecord = serde_json::from_value(serde_json::json!({
        "name": "model-a",
        "class": "model",
        "region": "us",
        "home": "global",
        "contract": 1,
        "placement": 1,
        "state": "ready",
        "epoch": 0,
        "seq": 0,
        "hub": "127.0.0.1:1337"
    }))
    .expect("a pool record");

    // The predecessor's clock was an hour ahead. Its record was lost,
    // and both subsequent starts use the same corrected system clock.
    let (first, held) = Writer::start(&directory, None, &record, WAIT).await;
    assert!(held.expect("the directory answers").is_none());
    assert!(first.epoch < predecessor_epoch, "the lost record matters");
    directory
        .publish(PoolRecord {
            epoch: first.epoch,
            seq: 1,
            ..record.clone()
        })
        .await
        .expect("the current hub stores its record");

    let (restarted, held) = Writer::start(&directory, None, &record, WAIT).await;
    assert_eq!(
        held.expect("the directory answers")
            .expect("the current record is present")
            .epoch,
        first.epoch
    );
    assert!(
        restarted.epoch > first.epoch,
        "the restart advances its record"
    );
    assert!(
        restarted.epoch > predecessor_epoch,
        "clock correction and a restart must overtake the lost epoch: restart={}, predecessor={predecessor_epoch}",
        restarted.epoch
    );
}
