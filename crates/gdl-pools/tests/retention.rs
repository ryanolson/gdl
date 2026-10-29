//! A restart must keep the withdrawal's version, not only the absence of its record.

use std::{
    collections::BTreeMap,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

use futures::{FutureExt, StreamExt};
use gdl_pools::{
    DirectoryError, MemoryDirectory, MemoryLimits, MemoryWatch, PoolDirectory, PoolEvent,
    PoolRecord, PoolState, ResumeToken, Withdrawal,
    conformance::{self, Backend},
};

const LIMITS: MemoryLimits = MemoryLimits {
    buffer: 8,
    history: 5,
};

#[derive(Clone)]
struct Stored {
    live: MemoryDirectory,
    saved: Arc<Mutex<BTreeMap<(String, String), Saved>>>,
    save_withdrawal_version: bool,
}

impl PoolDirectory for Stored {
    type Watch = MemoryWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.live.publish(record.clone()).await?;
        self.saved.lock().expect("the store").insert(
            (record.class.clone(), record.name.clone()),
            Saved::Record(Box::new(record)),
        );
        Ok(())
    }

    async fn withdraw(&self, mut withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.live.withdraw(withdrawal.clone()).await?;
        let key = (withdrawal.class.clone(), withdrawal.pool.clone());
        let mut saved = self.saved.lock().expect("the store");
        // The faulty store persists removal but leaves the row's version unchanged.
        if !self.save_withdrawal_version
            && let Some(previous) = saved.get(&key)
        {
            let version = match previous {
                Saved::Record(record) => record.version(),
                Saved::Withdrawn(withdrawal) => withdrawal.version(),
            };
            withdrawal.epoch = version.epoch;
            withdrawal.seq = version.seq;
        }
        saved.insert(key, Saved::Withdrawn(withdrawal));
        Ok(())
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.live.watch(class, from)
    }
}

#[derive(Clone)]
enum Saved {
    Record(Box<PoolRecord>),
    Withdrawn(Withdrawal),
}

struct Store {
    save_withdrawal_version: bool,
}

impl Backend for Store {
    type Directory = Stored;

    async fn fresh<const N: usize>(&self) -> [Stored; N] {
        let directory = Stored {
            live: MemoryDirectory::with_limits(LIMITS),
            saved: Arc::default(),
            save_withdrawal_version: self.save_withdrawal_version,
        };
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        LIMITS.history
    }

    fn watchers(&self, directory: &Stored, class: &str) -> usize {
        directory.live.watchers(class)
    }

    async fn restart(&self, directory: Stored) -> Stored {
        let saved = Arc::clone(&directory.saved);
        let entries: Vec<_> = saved.lock().expect("the store").values().cloned().collect();
        drop(directory);
        let live = MemoryDirectory::with_limits(LIMITS);
        for entry in entries {
            match entry {
                Saved::Record(record) => live.publish(*record).await.expect("restore the record"),
                Saved::Withdrawn(withdrawal) => live
                    .withdraw(withdrawal)
                    .await
                    .expect("restore the withdrawal"),
            }
        }
        Stored {
            live,
            saved,
            save_withdrawal_version: self.save_withdrawal_version,
        }
    }
}

fn record(seq: u64) -> PoolRecord {
    PoolRecord {
        presence: gdl_pools::Presence::Serving,
        models: Vec::new(),
        models_version: 0,
        revoked: Vec::new(),
        name: "m2".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: None,
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch: 1,
        seq,
        hub: "m2:1337".into(),
        lease: PoolRecord::DEFAULT_LEASE,
        load: None,
    }
}

#[tokio::test(start_paused = true)]
async fn a_store_that_keeps_the_withdrawal_version_conforms() {
    conformance::run(&Store {
        save_withdrawal_version: true,
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_store_that_loses_the_withdrawal_version_on_restart_fails() {
    let backend = Store {
        save_withdrawal_version: false,
    };
    let [directory] = backend.fresh().await;
    directory.publish(record(1)).await.expect("the record");
    let withdrawal = Withdrawal {
        class: "model_x".into(),
        pool: "m2".into(),
        epoch: 1,
        seq: 5,
    };
    directory
        .withdraw(withdrawal)
        .await
        .expect("the withdrawal");
    let restarted = backend.restart(directory).await;
    assert!(matches!(
        restarted.publish(record(1)).await,
        Err(DirectoryError::NotNewer { .. })
    ));
    restarted
        .publish(record(4))
        .await
        .expect("the regressed tombstone allows a stale record to return");
    let mut watch = restarted.watch("model_x", None);
    assert_eq!(watch.next().await, Some(Ok(PoolEvent::Current(record(4)))));
    drop((watch, restarted));

    let outcome = AssertUnwindSafe(conformance::run(&backend))
        .catch_unwind()
        .await;
    assert!(
        outcome.is_err(),
        "the suite accepted a backend whose tombstone regressed across restart"
    );
}
