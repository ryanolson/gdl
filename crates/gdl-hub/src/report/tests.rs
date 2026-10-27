use std::task::Poll;
use std::time::Duration;

use gdl_cells::{CellRecord, CellState, MemoryLimits, RelayDirectory, Version, Withdrawal};
use gdl_watch::Report;

use super::Reports;

const WAIT: Duration = Duration::from_secs(5);

/// A withdrawal that arrives while its cell's first report is still
/// being stored is checked after that report's write, so it removes the
/// record, and a reopened directory holds the withdrawal.
#[test]
fn a_withdrawal_overtaking_a_report_waiting_for_storage_stays_withdrawn() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        for persistent in [false, true] {
            let state = tempfile::Builder::new()
                .prefix("gdl-report-order-")
                .tempdir()
                .expect("a state directory");
            let children = if persistent {
                RelayDirectory::open(state.path(), MemoryLimits::default()).expect("opens")
            } else {
                RelayDirectory::new()
            };
            let parent = CellRecord {
                name: "global-us".into(),
                class: "global".into(),
                region: "us-east-1".into(),
                home: None,
                contract: 1,
                placement: 2,
                state: CellState::Ready,
                epoch: 1,
                seq: 1,
                hub: "global-us:1337".into(),
                lease: Duration::from_secs(30),
                load: None,
                presence: Default::default(),
                models: Vec::new(),
                models_version: 0,
            };
            let record = CellRecord {
                name: "model-x-01".into(),
                class: "model_x".into(),
                home: Some(parent.name.clone()),
                hub: "model-x-01:1337".into(),
                ..parent.clone()
            };
            let withdrawal = Withdrawal {
                class: record.class.clone(),
                cell: record.name.clone(),
                epoch: 1,
                seq: 2,
            };
            let reports = std::sync::Arc::new(Reports::new(
                children.clone(),
                &parent,
                Duration::from_secs(1),
            ));

            // Occupy the blocking pool so the verified report cannot reach storage yet.
            let (release, released) = std::sync::mpsc::channel();
            let (started, began) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).expect("the test waits for the blocker");
                let _ = released.recv_timeout(WAIT);
            });
            tokio::time::timeout(WAIT, began)
                .await
                .expect("the blocker starts in time")
                .expect("the blocker starts");
            let mut publishing =
                Box::pin(std::sync::Arc::clone(&reports).take(Report::Record(record.clone())));
            let published = match futures::poll!(publishing.as_mut()) {
                Poll::Ready(answer) => Some(answer),
                Poll::Pending => None,
            };
            let mut withdrawing = Box::pin(
                std::sync::Arc::clone(&reports).take(Report::Withdrawal(withdrawal.clone())),
            );
            let answer = match futures::poll!(withdrawing.as_mut()) {
                Poll::Ready(answer) => Some(answer),
                Poll::Pending => None,
            };
            // The withdrawal waits for the report that came before it.
            assert!(
                published.is_some() || answer.is_none(),
                "persistent={persistent}: the withdrawal was answered before the earlier report"
            );
            release.send(()).expect("release storage");
            tokio::time::timeout(WAIT, async {
                blocker.await.expect("the blocker ends");
                match published {
                    Some(answer) => answer,
                    None => publishing.await,
                }
                .expect("the report is taken");
                match answer {
                    Some(answer) => answer,
                    None => withdrawing.await,
                }
                .expect("the withdrawal is taken");
            })
            .await
            .expect("both reports finish in time");

            let reopened = if persistent {
                RelayDirectory::open(state.path(), MemoryLimits::default()).expect("reopens")
            } else {
                children
            };
            assert_eq!(
                reopened.held(&record.class, &record.name),
                Some(Version { epoch: 1, seq: 2 }),
                "persistent={persistent}: an acknowledged withdrawal left the earlier report stored"
            );
            assert!(!reopened.holds_record(&record.class, &record.name));
        }
    });
}
