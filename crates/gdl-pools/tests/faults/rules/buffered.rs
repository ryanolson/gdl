//! A snapshot that the backend reads when its watch is made, and joins to
//! the lists all at once before it yields any record.

use futures::StreamExt;
use gdl_model::PropertyKey;
use gdl_pools::{
    DirectoryError, LastWrite, PoolDirectory, PoolEvent, PoolProperty, PoolRecord, PoolState,
    Presence, PropertyState, ResumeToken, Withdrawal,
    conformance::{Backend, Rule},
};

use super::{Fault, Faulty, LIMITS};
use crate::{Events, fails_only_on, failure};

/// [`Fault::SnapshotList`], whose watch takes its whole snapshot on its
/// first poll, joining each record to the latest list then, and yields
/// it after.
struct Buffered;

struct BufferedDirectory(Faulty);

impl PoolDirectory for BufferedDirectory {
    type Watch = Events;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.0.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.0.withdraw(withdrawal).await
    }

    async fn last_write(
        &self,
        class: &str,
        pool: &str,
    ) -> Result<Option<LastWrite>, DirectoryError> {
        self.0.last_write(class, pool).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Events {
        let mut events = self.0.watch(class, from);
        Box::pin(
            futures::stream::once(async move {
                let mut snapshot = Vec::new();
                while let Some(event) = events.next().await {
                    let end = !matches!(event, Ok(PoolEvent::Current(_)));
                    snapshot.push(event);
                    if end {
                        break;
                    }
                }
                futures::stream::iter(snapshot).chain(events)
            })
            .flatten(),
        )
    }
}

impl Backend for Buffered {
    type Directory = BufferedDirectory;

    async fn fresh<const N: usize>(&self) -> [BufferedDirectory; N] {
        Fault::SnapshotList.fresh().await.map(BufferedDirectory)
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        LIMITS.history
    }

    fn watchers(&self, directory: &BufferedDirectory, class: &str) -> usize {
        directory.0.inner.watchers(class)
    }

    async fn restart(&self, directory: BufferedDirectory) -> BufferedDirectory {
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

/// A backend that reads a snapshot's records when the watch is made and
/// joins them all to the latest list on the first poll tears a record
/// whose list changed in between, before any record arrives, and fails
/// the case of [`Rule::Whole`], on that rule alone.
#[tokio::test(start_paused = true)]
async fn a_buffered_snapshot_joined_to_the_latest_list_fails() {
    let [writer, reader] = Buffered.fresh().await;
    let first = record(1);
    let second = record(2);
    writer
        .publish(first.clone())
        .await
        .expect("the first write");
    let mut snapshot = reader.watch("model_x", None);
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

    let why = failure(&Buffered).await;
    assert!(why.ends_with("failed: a_record_arrives_whole"), "{why}");
    fails_only_on(&Buffered, Rule::Whole).await;
}
