//! A snapshot that the backend reads only after its watch is polled.

use futures::{FutureExt, StreamExt};
use gdl_model::PropertyKey;
use gdl_pools::{
    DirectoryError, LastWrite, PoolDirectory, PoolEvent, PoolProperty, PoolRecord, PoolState,
    Presence, PropertyState, ResumeToken, Withdrawal,
    conformance::{Backend, Rule},
};

use super::{Fault, Faulty, LIMITS};
use crate::{Events, fails_only_on, failure};

/// [`Fault::SnapshotList`], whose watch reads its snapshot only after
/// `polls` polls that read nothing, and yields nothing on the poll that
/// reads it.
struct Deferred {
    polls: usize,
}

struct DeferredDirectory {
    inner: Faulty,
    polls: usize,
}

impl PoolDirectory for DeferredDirectory {
    type Watch = Events;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.inner.publish(record).await
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

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Events {
        let (directory, polls) = (self.inner.clone(), self.polls);
        let class = class.to_owned();
        Box::pin(
            futures::stream::once(async move {
                for _ in 0..polls {
                    tokio::task::yield_now().await;
                }
                let events = directory.watch(&class, from);
                tokio::task::yield_now().await;
                events
            })
            .flatten(),
        )
    }
}

impl Backend for Deferred {
    type Directory = DeferredDirectory;

    async fn fresh<const N: usize>(&self) -> [DeferredDirectory; N] {
        Fault::SnapshotList
            .fresh()
            .await
            .map(|inner| DeferredDirectory {
                inner,
                polls: self.polls,
            })
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        LIMITS.history
    }

    fn watchers(&self, directory: &DeferredDirectory, class: &str) -> usize {
        directory.inner.inner.watchers(class)
    }

    async fn restart(&self, directory: DeferredDirectory) -> DeferredDirectory {
        directory
    }
}

fn record(seq: u64) -> PoolRecord {
    let key = |kind| {
        PropertyKey::new(
            "test",
            serde_json::json!({ "name": format!("{kind}-{seq}") }),
        )
        .expect("a key")
    };
    PoolRecord {
        dropped: Vec::new(),
        presence: Presence::Serving,
        properties: vec![PoolProperty {
            property: key("property"),
            state: PropertyState::Ready,
            since: seq,
        }],
        properties_version: seq,
        revoked: vec![key("revoked")],
        name: "m1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        parent: None,
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch: 1,
        seq,
        hub: "m1:1337".into(),
        lease: PoolRecord::DEFAULT_LEASE,
        load: None,
    }
}

/// A backend whose watch reads its snapshot only after some polls, and
/// joins the snapshot's records to the latest list, tears a record whose
/// list changed after that read, and fails the case of [`Rule::Whole`],
/// on that rule alone, however many polls it waits.
#[tokio::test(start_paused = true)]
async fn a_lazy_snapshot_joined_to_the_latest_list_fails() {
    for polls in [0, 1, 4] {
        let backend = Deferred { polls };
        let [writer, reader] = backend.fresh().await;
        let first = record(1);
        let second = record(2);
        writer
            .publish(first.clone())
            .await
            .expect("the first write");
        let mut snapshot = reader.watch("model_x", None);
        for _ in 0..=polls {
            assert!(snapshot.next().now_or_never().is_none());
        }
        assert_eq!(backend.watchers(&reader, "model_x"), 1);
        writer
            .publish(second.clone())
            .await
            .expect("the second write");
        let Some(Ok(PoolEvent::Current(torn))) = snapshot.next().await else {
            panic!("the snapshot holds a record");
        };
        assert_eq!(torn.version(), first.version());
        assert_eq!(torn.properties, second.properties);
        assert_eq!(torn.revoked, second.revoked);
        assert_ne!(torn, first);
        assert_ne!(torn, second);
        drop((writer, reader, snapshot));

        let why = failure(&backend).await;
        assert!(
            why.ends_with("failed: a_record_arrives_whole"),
            "{polls} polls: {why}"
        );
        fails_only_on(&backend, Rule::Whole).await;
    }
}
