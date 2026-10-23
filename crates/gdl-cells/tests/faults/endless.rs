use std::{sync::mpsc, thread, time::Duration};

use futures::stream;
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellState, DirectoryError, ResumeToken, Withdrawal,
    conformance::Backend,
};

use crate::{Events, failure};

/// A backend whose snapshot never ends, though each of its records comes
/// well within a wait's bound: after 10 ms, or at once when `ready`.
#[derive(Clone, Copy)]
struct Endless {
    ready: bool,
}

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
        let event = Ok(CellEvent::Current(record));
        if self.ready {
            return Box::pin(stream::repeat(event));
        }
        Box::pin(stream::unfold(event, |event| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Some((event.clone(), event))
        }))
    }
}

impl Backend for Endless {
    type Directory = Endless;

    async fn fresh<const N: usize>(&self) -> [Endless; N] {
        [*self; N]
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

const CASES: [&str; 6] = [
    "malformed_writes_are_refused",
    "writes_are_ordered_by_epoch_and_seq",
    "a_watch_sees_its_class_snapshot_then_changes",
    "a_watch_resumes_after_a_token",
    "a_lagging_watcher_is_cut_off",
    "a_dropped_watch_is_released",
];

/// Every case fails in bounded time, rather than one case running forever
/// and the rest never running.
#[tokio::test(start_paused = true)]
async fn a_snapshot_that_never_ends_fails_every_case_in_bounded_time() {
    let why = failure(&Endless { ready: false }).await;
    for case in CASES {
        assert!(why.contains(case), "{why}");
    }
}

/// A stream that is always ready never returns control to its poller on
/// its own, so a loop that drains it must yield for its bound to fire.
///
/// A suite that never yields cannot be stopped from its own task, so it
/// runs on a thread of its own, and the test waits for its answer for a
/// few seconds of real time. Its runtime's clock is paused and moved on by
/// a second task, which runs only when the suite yields: the bounds pass
/// without being waited out.
#[test]
fn an_always_ready_snapshot_that_never_ends_fails_every_case_in_bounded_time() {
    let (answer, answered) = mpsc::channel();
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("a runtime");
        let why = runtime.block_on(async {
            tokio::spawn(async {
                loop {
                    tokio::time::advance(Duration::from_millis(100)).await;
                    tokio::task::yield_now().await;
                }
            });
            failure(&Endless { ready: true }).await
        });
        let _ = answer.send(why);
    });
    let why = answered
        .recv_timeout(Duration::from_secs(10))
        .expect("the suite neither finished nor yielded");
    for case in CASES {
        assert!(why.contains(case), "{why}");
    }
}
