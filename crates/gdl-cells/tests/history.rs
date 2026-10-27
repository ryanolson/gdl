//! A directory may keep a short replay history and resnapshot older tokens.

use futures::StreamExt;
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellState, MemoryDirectory, MemoryLimits,
    conformance::{self, Backend, Rule},
};

struct ShortHistory;

impl Backend for ShortHistory {
    type Directory = MemoryDirectory;

    async fn fresh<const N: usize>(&self) -> [MemoryDirectory; N] {
        let directory = MemoryDirectory::with_limits(MemoryLimits {
            buffer: 8,
            history: 1,
        });
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        8
    }

    fn history(&self) -> usize {
        1
    }

    fn watchers(&self, directory: &MemoryDirectory, class: &str) -> usize {
        directory.watchers(class)
    }

    async fn restart(&self, _: MemoryDirectory) -> MemoryDirectory {
        self.fresh::<1>().await.into_iter().next().expect("one")
    }

    fn keeps(&self, rule: Rule) -> bool {
        rule != Rule::Kept
    }
}

fn record(seq: u64) -> CellRecord {
    CellRecord {
        presence: gdl_cells::Presence::Serving,
        models: Vec::new(),
        models_version: 0,
        revoked: Vec::new(),
        name: "model-a".into(),
        class: "model".into(),
        region: "us-east-1".into(),
        home: None,
        contract: 1,
        placement: 2,
        state: CellState::Ready,
        epoch: 1,
        seq,
        hub: "model-a:1337".into(),
        lease: CellRecord::DEFAULT_LEASE,
        load: None,
    }
}

/// A backend that keeps one change serves a token one change ago, says
/// `Resnapshot` for an older one, and passes the suite.
#[tokio::test(start_paused = true)]
async fn a_backend_with_one_replay_event_conforms() {
    let [directory] = ShortHistory.fresh().await;
    let mut live = directory.watch("model", None);
    let Some(Ok(CellEvent::Synced { token, .. })) = live.next().await else {
        panic!("the empty snapshot ends");
    };
    directory.publish(record(1)).await.expect("a record");
    let Some(Ok(CellEvent::Put { token: recent, .. })) = live.next().await else {
        panic!("the first change has a token");
    };
    directory.publish(record(2)).await.expect("a newer record");
    let mut replay = directory.watch("model", Some(recent.clone()));
    assert_eq!(
        replay.next().await,
        Some(Ok(CellEvent::Synced {
            token: recent,
            snapshot: false,
        }))
    );
    assert!(matches!(
        replay.next().await,
        Some(Ok(CellEvent::Put { record: replayed, .. })) if replayed == record(2)
    ));
    let mut stale = directory.watch("model", Some(token));
    assert_eq!(stale.next().await, Some(Ok(CellEvent::Resnapshot)));
    assert_eq!(stale.next().await, None);
    drop((live, replay, stale, directory));

    conformance::run(&ShortHistory).await;
}
