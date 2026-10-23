use std::time::Duration;

use futures::stream;
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellState, DirectoryError, ResumeToken, Withdrawal,
    conformance::Backend,
};

use crate::{Events, failure};

/// A backend whose snapshot never ends, though each of its records comes
/// well within a wait's bound.
#[derive(Clone)]
struct Endless;

impl CellDirectory for Endless {
    type Watch = Events;

    async fn publish(&self, _: CellRecord) -> Result<(), DirectoryError> {
        Ok(())
    }

    async fn withdraw(&self, _: Withdrawal) -> Result<(), DirectoryError> {
        Ok(())
    }

    fn watch(&self, class: &str, _: Option<ResumeToken>) -> Events {
        let record = CellRecord {
            name: "m1".into(),
            class: class.into(),
            region: "us-east-1".into(),
            contract: 1,
            placement: 2,
            state: CellState::Ready,
            epoch: 1,
            seq: 1,
            hub: "m1:1337".into(),
            lease: CellRecord::DEFAULT_LEASE,
            load: None,
        };
        Box::pin(stream::unfold(record, |record| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Some((Ok(CellEvent::Current(record.clone())), record))
        }))
    }
}

impl Backend for Endless {
    type Directory = Endless;

    async fn fresh<const N: usize>(&self) -> [Endless; N] {
        std::array::from_fn(|_| Endless)
    }

    fn buffer(&self) -> usize {
        1
    }

    fn history(&self) -> usize {
        1
    }

    fn watchers(&self, _: &Endless, _: &str) -> usize {
        0
    }
}

/// Every case fails in bounded time, rather than one case running forever
/// and the rest never running.
#[tokio::test(start_paused = true)]
async fn a_snapshot_that_never_ends_fails_every_case_in_bounded_time() {
    let why = failure(&Endless).await;
    for case in [
        "malformed_writes_are_refused",
        "writes_are_ordered_by_epoch_and_seq",
        "a_watch_sees_its_class_snapshot_then_changes",
        "a_watch_resumes_after_a_token",
        "a_lagging_watcher_is_cut_off",
        "a_dropped_watch_is_released",
    ] {
        assert!(why.contains(case), "{why}");
    }
}
