//! A replay from a backend that serves a token across one change.

use futures::StreamExt;
use gdl_model::PropertyKey;
use gdl_pools::{
    DirectoryError, LastWrite, MemoryDirectory, MemoryLimits, PoolDirectory, PoolEvent,
    PoolProperty, PoolRecord, PoolState, Presence, PropertyState, ResumeToken, Withdrawal,
    conformance::{Backend, Rule},
};

use super::{Fault, Faulty, LIMITS};
use crate::{Events, fails_only_on, failure};

/// [`Fault::LatestList`] on its replays only, with a history of one
/// change.
struct OneChange;

struct ReplayDirectory(Faulty);

impl PoolDirectory for ReplayDirectory {
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
        if from.is_some() {
            self.0.watch(class, from)
        } else {
            Box::pin(self.0.inner.watch(class, from))
        }
    }
}

impl Backend for OneChange {
    type Directory = ReplayDirectory;

    async fn fresh<const N: usize>(&self) -> [ReplayDirectory; N] {
        let [mut directory] = Fault::LatestList.fresh().await;
        directory.inner = MemoryDirectory::with_limits(MemoryLimits {
            history: 1,
            ..LIMITS
        });
        std::array::from_fn(|_| ReplayDirectory(directory.clone()))
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        1
    }

    fn watchers(&self, directory: &ReplayDirectory, class: &str) -> usize {
        directory.0.inner.watchers(class)
    }

    async fn restart(&self, directory: ReplayDirectory) -> ReplayDirectory {
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

/// A backend that serves a token across one change only, and joins the
/// changes a resumed watch replays to the latest list, tears a replayed
/// record whose list changed after the watch resumed, and fails the case
/// of [`Rule::Whole`], on that rule alone.
#[tokio::test(start_paused = true)]
async fn a_one_change_replay_joined_to_the_latest_list_fails() {
    let [writer, reader] = OneChange.fresh().await;
    let mut live = reader.watch("model_x", None);
    let Some(Ok(PoolEvent::Synced { token, .. })) = live.next().await else {
        panic!("the empty snapshot's token");
    };
    let first = record(1);
    let second = record(2);
    writer
        .publish(first.clone())
        .await
        .expect("the first write");
    let mut resumed = reader.watch("model_x", Some(token));
    assert!(matches!(
        resumed.next().await,
        Some(Ok(PoolEvent::Synced {
            snapshot: false,
            ..
        }))
    ));
    writer
        .publish(second.clone())
        .await
        .expect("the second write");
    let Some(Ok(PoolEvent::Put { record: torn, .. })) = resumed.next().await else {
        panic!("the first write is replayed");
    };
    assert_eq!(torn.version(), first.version());
    assert_eq!(torn.properties, second.properties);
    assert_eq!(torn.revoked, second.revoked);
    assert_ne!(torn, first);
    assert_ne!(torn, second);
    drop((writer, reader, live, resumed));

    let why = failure(&OneChange).await;
    assert!(why.ends_with("failed: a_record_arrives_whole"), "{why}");
    fails_only_on(&OneChange, Rule::Whole).await;
}
