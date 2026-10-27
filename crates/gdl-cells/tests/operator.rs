//! An operator's commands for a cell whose hub is gone: a withdrawal, and
//! a record that says the cell was scaled to 0. Each is written at an
//! epoch past the cell's last record, so that no late write of the old
//! epoch undoes it, and a reader shows the cell as the command says. The
//! commands use only the trait, so they hold for `memory` and `dir` alike.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use gdl_cells::{
    CellCommand, CellDirectory, CellEvent, CellRecord, CellState, CellView, CommandWrite,
    DirDirectory, DirectoryError, MemoryDirectory, ModelEntry, ModelState, Presence, Version,
};
use tempfile::TempDir;

const CLASS: &str = "model_x";
const CELL: &str = "model-x-02-us-east-1";

/// The last record the cell's hub wrote before it went: `ready`, with two
/// models.
fn last(epoch: u64, seq: u64) -> CellRecord {
    CellRecord {
        name: CELL.into(),
        class: CLASS.into(),
        region: "us-east-1".into(),
        home: Some("global-01-us-east-1".into()),
        contract: 1,
        placement: 2,
        state: CellState::Ready,
        epoch,
        seq,
        hub: "model-x-02-hub:1337".into(),
        lease: Duration::from_millis(300),
        load: None,
        presence: Presence::Serving,
        models: vec![
            ModelEntry {
                model: "acme-legal".into(),
                routing_key: None,
                state: ModelState::Ready,
                since: 7,
            },
            ModelEntry {
                model: "llama-70b".into(),
                routing_key: None,
                state: ModelState::Ready,
                since: 7,
            },
        ],
        models_version: 7,
        revoked: Vec::new(),
    }
}

fn millis(at: SystemTime) -> u64 {
    u64::try_from(
        at.duration_since(UNIX_EPOCH)
            .expect("after 1970")
            .as_millis(),
    )
    .expect("milliseconds fit")
}

/// A fresh `dir` directory, held as long as the case runs.
fn dir() -> (DirDirectory, TempDir) {
    let temp = tempfile::Builder::new()
        .prefix("gdl-operator-")
        .tempdir()
        .expect("a temporary directory");
    (DirDirectory::new(temp.path()), temp)
}

/// The class's records as a reader sees them, from a new watch's snapshot,
/// at `now`.
async fn view(directory: &impl CellDirectory, now: Instant) -> CellView {
    let mut watch = std::pin::pin!(directory.watch(CLASS, None));
    let mut view = CellView::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .expect("the watch answers in time")
            .expect("the watch goes on")
            .expect("the watch has no error");
        let synced = matches!(event, CellEvent::Synced { .. });
        view.apply(event, now);
        if synced {
            return view;
        }
    }
}

/// The write carries an epoch past the last record's, also when the
/// operator's clock is behind the hub's, and keeps the rest of the record
/// as it was: its models and their version stay, so that a hub that
/// starts again keeps their history.
#[test]
fn a_command_writes_at_an_epoch_past_the_last_record() {
    let now = SystemTime::now();
    let ahead = last(millis(now) + 60_000, 40);
    let CommandWrite::Record(scaled) = CellCommand::ScaleToZero.write(&ahead, now) else {
        panic!("a scale to 0 is a record");
    };
    assert_eq!(
        scaled.version(),
        Version {
            epoch: ahead.epoch + 1,
            seq: 1
        }
    );
    assert_eq!(scaled.presence, Presence::ScaledToZero);
    assert_eq!(
        CellRecord {
            presence: Presence::Serving,
            epoch: ahead.epoch,
            seq: ahead.seq,
            ..*scaled
        },
        ahead,
        "nothing but the presence and the version changes"
    );

    let behind = last(millis(now) - 60_000, 40);
    let CommandWrite::Withdrawal(withdrawal) = CellCommand::Withdraw.write(&behind, now) else {
        panic!("a withdrawal");
    };
    assert_eq!(
        withdrawal.version(),
        Version {
            epoch: millis(now),
            seq: 1
        }
    );
    assert_eq!(
        (withdrawal.class.as_str(), withdrawal.cell.as_str()),
        (CLASS, CELL)
    );
}

/// Withdrawn by the operator, the cell's models leave every reader's list,
/// and a late record from the hub's old epoch, a heartbeat that was on its
/// way, is refused and never brings it back.
async fn a_withdrawal_holds_against_a_late_record(directory: impl CellDirectory) {
    let held = last(millis(SystemTime::now()), 40);
    directory
        .publish(held.clone())
        .await
        .expect("the hub's last record");

    let version = CellCommand::Withdraw
        .give(&directory, CLASS, CELL)
        .await
        .expect("the directory takes the withdrawal")
        .expect("the cell has a record to withdraw");
    assert!(
        version > held.version(),
        "{version} after {}",
        held.version()
    );
    let now = Instant::now();
    let seen = view(&directory, now).await;
    assert!(seen.get(CELL, now).is_none(), "the record is gone");
    assert!(
        seen.records(now).all(|record| record
            .models
            .iter()
            .all(|entry| entry.model != "acme-legal")),
        "no record lists the cell's models"
    );

    let late = CellRecord {
        seq: held.seq + 100,
        ..held
    };
    let refused = directory.publish(late).await;
    assert!(
        matches!(refused, Err(DirectoryError::NotNewer { held, .. }) if held == version),
        "a late record of the old epoch meets the withdrawal's tombstone: {refused:?}"
    );
    assert!(
        view(&directory, Instant::now())
            .await
            .get(CELL, Instant::now())
            .is_none()
    );
}

#[tokio::test]
async fn memory_a_withdrawal_holds_against_a_late_record() {
    a_withdrawal_holds_against_a_late_record(MemoryDirectory::new()).await;
}

#[tokio::test]
async fn dir_a_withdrawal_holds_against_a_late_record() {
    let (directory, _held) = dir();
    a_withdrawal_holds_against_a_late_record(directory).await;
}

/// Scaled to 0 by the operator, the cell reads as scaled to 0 once its
/// lease has long passed, not as missing; a late `serving` record of the
/// old epoch is refused.
async fn a_scale_to_zero_holds_against_a_late_record(directory: impl CellDirectory) {
    let held = last(millis(SystemTime::now()), 40);
    directory
        .publish(held.clone())
        .await
        .expect("the hub's last record");

    let version = CellCommand::ScaleToZero
        .give(&directory, CLASS, CELL)
        .await
        .expect("the directory takes the record")
        .expect("the cell has a record to scale to 0");
    assert!(version > held.version());

    let late = CellRecord {
        seq: held.seq + 100,
        ..held.clone()
    };
    let refused = directory.publish(late).await;
    assert!(
        matches!(refused, Err(DirectoryError::NotNewer { .. })),
        "a late record of the old epoch: {refused:?}"
    );

    let now = Instant::now();
    let seen = view(&directory, now).await;
    let later = now + held.lease * 100;
    let record = seen
        .get(CELL, later)
        .expect("a cell scaled to 0 never lapses");
    assert_eq!(record.presence, Presence::ScaledToZero);
    assert_eq!(record.version(), version);
    assert_eq!(record.models, held.models, "its models are kept");
}

#[tokio::test]
async fn memory_a_scale_to_zero_holds_against_a_late_record() {
    a_scale_to_zero_holds_against_a_late_record(MemoryDirectory::new()).await;
}

#[tokio::test]
async fn dir_a_scale_to_zero_holds_against_a_late_record() {
    let (directory, _held) = dir();
    a_scale_to_zero_holds_against_a_late_record(directory).await;
}

/// A cell the directory holds no record of has nothing to command: no
/// write is made, so no tombstone keeps a later cell of the name out.
#[tokio::test]
async fn a_command_for_a_cell_with_no_record_writes_nothing() {
    let directory = MemoryDirectory::new();
    for command in [CellCommand::Withdraw, CellCommand::ScaleToZero] {
        let given = command.give(&directory, CLASS, CELL).await;
        assert_eq!(given, Ok(None), "{command:?}");
    }
    let held = last(1, 1);
    directory
        .publish(held)
        .await
        .expect("a later cell of the name publishes from any epoch");
}

/// A hub that starts again after the operator's write takes an epoch past
/// it, as after its own: a cell scaled to 0 serves again, and a withdrawn
/// cell comes back, from the hub's first write.
#[tokio::test]
async fn a_hub_that_starts_again_writes_past_the_command() {
    let directory = MemoryDirectory::new();
    let held = last(millis(SystemTime::now()), 40);
    directory
        .publish(held.clone())
        .await
        .expect("the hub's last record");
    let version = CellCommand::ScaleToZero
        .give(&directory, CLASS, CELL)
        .await
        .expect("the directory takes the record")
        .expect("a record to scale");
    let restarted = CellRecord {
        epoch: version.epoch + 1,
        seq: 1,
        ..held
    };
    directory
        .publish(restarted)
        .await
        .expect("the newer epoch is taken");
}
