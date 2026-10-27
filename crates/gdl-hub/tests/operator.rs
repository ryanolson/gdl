//! An operator's commands for a child cell whose hub is gone, given on its
//! parent's hub, which reads the child's records: withdraw the child, or
//! say that it was scaled to 0. Each is written at an epoch past the
//! child's last record, so that a late record of the old epoch cannot undo
//! it. Under `memory` (as `dir`: a directory the hubs share) the parent
//! writes through the directory; under `relay` it writes into the records
//! its children report to it.
//!
//! Every hub runs in this process on OS-assigned ports; every wait is
//! bounded and for something observed.

mod common;

use std::time::{Duration, Instant};

use common::{GLOBAL, Records, WAIT, address, config, global_default, hub_of, spec, timing};
use futures::StreamExt;
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellView, DirectoryError, MemoryDirectory, Presence,
};
use gdl_hub::{CellReadiness, CellTiming, GraphAdmission, HubDirectory};
use gdl_model::{CellSettings, ModelKey};
use gdl_watch::{CellWatcher, Report};
use velo_hub::Hub;

const CHILD: &str = "model-x-02-us-east-1";

/// The child cell, which declares llama-70b, reporting to `parent_hub`
/// under `relay`, or publishing to the shared directory without one.
fn child(parent_hub: Option<&str>) -> CellSettings {
    let directory = parent_hub.map_or("directory = 'memory'\n".to_string(), |parent| {
        format!("parent_hub = '{parent}'\n")
    });
    let settings: CellSettings = toml::from_str(&format!(
        "name = '{CHILD}'\nclass = 'model_x'\nregion = 'us-east-1'\nhome = '{GLOBAL}'\n\
         hub = 'model-x-02-hub:1337'\n{directory}[ready]\nd = 2\n"
    ))
    .expect("valid test fixture");
    CellSettings {
        models: vec![ModelKey::new("llama-70b")],
        ..settings
    }
}

/// Give the command at `path` on `hub`'s control listener: the status.
async fn command(hub: &Hub, path: &str) -> u16 {
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-cell{path}",
        hub.control_addr().port()
    );
    let response = tokio::time::timeout(WAIT, reqwest::Client::new().post(&url).send())
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    response.status().as_u16()
}

fn withdraw(cell: &str) -> String {
    gdl_watch::child_withdraw_path("model_x", cell)
}

fn scale_to_zero(cell: &str) -> String {
    gdl_watch::child_scale_to_zero_path("model_x", cell)
}

/// The class's records as a reader sees them, from a new watch's snapshot.
async fn view(directory: &impl CellDirectory, now: Instant) -> CellView {
    let mut watch = std::pin::pin!(directory.watch("model_x", None));
    let mut view = CellView::new();
    loop {
        let event = tokio::time::timeout(WAIT, watch.next())
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

/// A child's hub that published its record and stopped: its last record.
async fn gone_child(directory: &MemoryDirectory) -> CellRecord {
    let mut records = Records::of(directory, "model_x", CHILD);
    let (hub, _child) = hub_of(child(None), directory.clone(), timing()).await;
    let last = records.next().await;
    hub.shutdown().await.expect("the child's hub stops");
    last
}

/// The parent withdraws its child whose hub is gone: the child's record
/// and its models leave the directory, and a late record of the child's
/// old epoch is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_withdraws_a_child_whose_hub_is_gone() {
    let directory = MemoryDirectory::new();
    let (parent, _readiness) = hub_of(global_default(), directory.clone(), timing()).await;
    let last = gone_child(&directory).await;
    assert_eq!(last.models.len(), 1, "{last:?}");

    assert_eq!(command(&parent, &withdraw(CHILD)).await, 204);
    let now = Instant::now();
    let seen = view(&directory, now).await;
    assert!(seen.get(CHILD, now).is_none(), "withdrawn");
    assert!(
        seen.records(now).all(|record| record.models.is_empty()),
        "no record lists llama-70b"
    );
    let late = CellRecord {
        seq: last.seq + 100,
        ..last
    };
    let refused = directory.publish(late).await;
    assert!(
        matches!(refused, Err(DirectoryError::NotNewer { .. })),
        "{refused:?}"
    );
    assert_eq!(
        command(&parent, &withdraw(CHILD)).await,
        404,
        "nothing left to withdraw"
    );
    parent.shutdown().await.expect("the parent's hub stops");
}

/// The parent says that its child whose hub is gone was scaled to 0: a
/// reader shows the child scaled to 0 long after its lease, not missing,
/// with its models; a late `serving` record of the old epoch is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_says_a_child_whose_hub_is_gone_was_scaled_to_zero() {
    let directory = MemoryDirectory::new();
    let (parent, _readiness) = hub_of(global_default(), directory.clone(), timing()).await;
    let last = gone_child(&directory).await;

    assert_eq!(command(&parent, &scale_to_zero(CHILD)).await, 204);
    let late = CellRecord {
        seq: last.seq + 100,
        ..last.clone()
    };
    let refused = directory.publish(late).await;
    assert!(
        matches!(refused, Err(DirectoryError::NotNewer { .. })),
        "{refused:?}"
    );
    let now = Instant::now();
    let seen = view(&directory, now).await;
    let scaled = seen
        .get(CHILD, now + last.lease * 10)
        .expect("a cell scaled to 0 never lapses");
    assert_eq!(scaled.presence, Presence::ScaledToZero);
    assert!(scaled.version() > last.version());
    assert_eq!(scaled.models, last.models);
    parent.shutdown().await.expect("the parent's hub stops");
}

/// The parent commands only its own children: a cell it holds no record
/// of, and a cell whose parent is another cell, are not found, and
/// nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_commands_only_its_own_children() {
    let directory = MemoryDirectory::new();
    let (parent, _readiness) = hub_of(global_default(), directory.clone(), timing()).await;
    let last = gone_child(&directory).await;
    let other = CellRecord {
        name: "model-x-09-us-east-1".into(),
        home: Some("global-02-us-west-2".into()),
        ..last
    };
    directory
        .publish(other.clone())
        .await
        .expect("another parent's child");

    for path in [
        withdraw("model-x-07-us-east-1"),
        withdraw(&other.name),
        scale_to_zero(&other.name),
        gdl_watch::child_withdraw_path("model_y", CHILD),
    ] {
        assert_eq!(command(&parent, &path).await, 404, "{path}");
    }
    let now = Instant::now();
    let held = view(&directory, now).await;
    assert_eq!(held.get(&other.name, now), Some(&other), "untouched");
    assert_eq!(
        command(&parent, &withdraw(CHILD)).await,
        204,
        "its own child is found"
    );
    parent.shutdown().await.expect("the parent's hub stops");
}

/// A cell's hub under the `relay` backend, its own name resolved once it
/// serves.
struct RelayCell {
    hub: Hub,
    directory: HubDirectory,
    /// Where a parent keeps its children's records.
    _state: Option<tempfile::TempDir>,
}

/// The hub of `cell`; a parent, which sends to `model_x`, keeps its
/// children's records in a temporary state directory.
async fn relay_cell(cell: CellSettings, timing: CellTiming) -> RelayCell {
    let gdl_model::DirectoryBackend::Relay { parent_hub, .. } = cell.directory.clone() else {
        panic!("a relay cell: {:?}", cell.directory);
    };
    let state = (cell.class == "global").then(|| {
        tempfile::Builder::new()
            .prefix("gdl-operator-")
            .tempdir()
            .expect("a state directory")
    });
    let state_dir = state.as_ref().map(|state| state.path().to_owned());
    let cell = CellSettings {
        directory: gdl_model::DirectoryBackend::Relay {
            parent_hub: parent_hub.clone(),
            state_dir: state_dir.clone(),
        },
        ..cell
    };
    let directory = HubDirectory::new(parent_hub, state_dir).expect("a relay directory");
    let named = common::Named::new(directory.clone());
    let name = cell.hub.clone().expect("a hub name");
    let readiness = std::sync::Arc::new(
        CellReadiness::new(&spec(), cell, named.clone(), timing)
            .expect("valid test fixture")
            .with_reports(&directory)
            .expect("a lease its reports cover"),
    );
    let hub = Hub::builder(config())
        .extension(GraphAdmission::new(spec()))
        .extension(readiness)
        .serve()
        .await
        .expect("the hub serves");
    named.resolve(&name, &hub);
    RelayCell {
        hub,
        directory,
        _state: state,
    }
}

/// The global cell under `relay`: it reports nowhere.
fn relay_global() -> CellSettings {
    toml::from_str(&format!(
        "name = '{GLOBAL}'\nclass = 'global'\nregion = 'us-east-1'\nhub = 'global-hub:1337'\n"
    ))
    .expect("valid test fixture")
}

/// Under `relay`, a child whose hub is gone: the parent's hub, and the
/// child's last record it holds.
async fn gone_relay_child() -> (RelayCell, CellRecord) {
    let parent = relay_cell(relay_global(), timing()).await;
    let watcher = CellWatcher::new().expect("a client");
    let at = address(&parent.hub);
    let child = relay_cell(child(Some(&at)), timing()).await;
    let reported = tokio::time::timeout(WAIT, async {
        loop {
            if let Ok(Some(record)) = watcher.held(&at, "model_x", CHILD).await {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the child reports in time");
    child.hub.shutdown().await.expect("the child's hub stops");
    let last = watcher
        .held(&at, "model_x", CHILD)
        .await
        .expect("the parent answers")
        .expect("the parent holds the child's record");
    assert!(last.version() >= reported.version());
    (parent, last)
}

/// Under `relay`, the parent withdraws a child whose hub is gone: the
/// parent no longer holds the record, and a late report of the child's old
/// epoch is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_relay_a_parent_withdraws_a_gone_child() {
    let (parent, last) = gone_relay_child().await;
    let at = address(&parent.hub);
    let watcher = CellWatcher::new().expect("a client");

    assert_eq!(command(&parent.hub, &withdraw(CHILD)).await, 204);
    assert_eq!(watcher.held(&at, "model_x", CHILD).await, Ok(None));
    let late = Report::Record(CellRecord {
        seq: last.seq + 100,
        ..last
    });
    assert!(
        watcher.report(&at, &late).await.is_err(),
        "a late report of the old epoch is refused"
    );
    assert_eq!(watcher.held(&at, "model_x", CHILD).await, Ok(None));
    parent.hub.shutdown().await.expect("the parent's hub stops");
}

/// Under `relay`, the parent says that a child whose hub is gone was
/// scaled to 0: a reader of the parent's records shows the child scaled to
/// 0 long after its lease; a late report of the old epoch is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_relay_a_parent_scales_a_gone_child_to_zero() {
    let (parent, last) = gone_relay_child().await;
    let at = address(&parent.hub);
    let watcher = CellWatcher::new().expect("a client");

    assert_eq!(command(&parent.hub, &scale_to_zero(CHILD)).await, 204);
    let late = Report::Record(CellRecord {
        seq: last.seq + 100,
        ..last.clone()
    });
    assert!(watcher.report(&at, &late).await.is_err());
    let now = Instant::now();
    let seen = view(&parent.directory, now).await;
    let scaled = seen
        .get(CHILD, now + last.lease * 10)
        .expect("a cell scaled to 0 never lapses");
    assert_eq!(scaled.presence, Presence::ScaledToZero);
    assert!(scaled.version() > last.version());
    parent.hub.shutdown().await.expect("the parent's hub stops");
}

/// A child the parent followed, whose hub is gone, and which the operator
/// then withdraws on the parent: the parent's relay says the child
/// withdrew, as for a drained cell, so a node that reads the followed cells
/// holds it withdrawn, and a gate answers 404 for a model no other set
/// serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_followed_child_withdrawn_by_the_operator_reads_as_withdrawn() {
    let directory = common::Resolving::new();
    let (parent, readiness) = common::serve_cell(global_default(), &directory, timing()).await;
    let mut relay = readiness.follows();
    let (child, _readiness) = common::serve_cell(
        common::model(CHILD, "us-east-1", "model-x-02-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (instances, _) = common::meet_the_quorum(&child).await;
    let mut following = readiness.following();
    tokio::time::timeout(
        WAIT,
        following.wait_for(|followed| followed.contains_key(&("model_x".into(), CHILD.into()))),
    )
    .await
    .expect("the parent follows its child in time")
    .expect("the parent runs");
    drop(instances);
    child.shutdown().await.expect("the child's hub stops");

    assert_eq!(command(&parent, &withdraw(CHILD)).await, 204);
    let withdrawn = tokio::time::timeout(WAIT, async {
        while let Some(event) = relay.next().await {
            if matches!(&event, gdl_watch::FollowEvent::Withdrawn { cell, .. } if cell == CHILD) {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(withdrawn, Ok(true), "the relay says the child withdrew");
    parent.shutdown().await.expect("the parent's hub stops");
}

/// The documented exception to the fence: a hub that still answers, none
/// of whose writes was answered yet, cannot tell an operator's command from
/// a record that was there before it started, so it moves past the command
/// as a restarted hub must ([`a_restarted_hub_recovers_a_withdrawal_after_an_unstored_first_write`]).
/// The commands are for a hub that is gone; its writes of the old epoch
/// are still refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_none_of_whose_writes_was_answered_moves_past_a_command() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::{Notify, mpsc};

    #[derive(Clone)]
    struct LostAnswer {
        records: MemoryDirectory,
        first: Arc<AtomicBool>,
        arrived: Arc<Notify>,
        release: Arc<Notify>,
        attempts: mpsc::UnboundedSender<(gdl_cells::Version, bool)>,
    }

    impl CellDirectory for LostAnswer {
        type Watch = gdl_cells::MemoryWatch;

        async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
            let version = record.version();
            let result = self.records.publish(record).await;
            if self.first.swap(false, Ordering::SeqCst) {
                result.expect("the first publish reaches the directory");
                self.arrived.notify_one();
                tokio::time::timeout(WAIT, self.release.notified())
                    .await
                    .expect("the test releases the lost acknowledgement");
                return Err(DirectoryError::Unavailable(
                    "the write was stored but its acknowledgement was lost".into(),
                ));
            }
            self.attempts
                .send((version, result.is_ok()))
                .expect("the test receives the publish result");
            result
        }

        async fn withdraw(&self, withdrawal: gdl_cells::Withdrawal) -> Result<(), DirectoryError> {
            self.records.withdraw(withdrawal).await
        }

        fn watch(&self, class: &str, from: Option<gdl_cells::ResumeToken>) -> Self::Watch {
            self.records.watch(class, from)
        }
    }

    let mut undone = Vec::new();
    for operator in [
        gdl_cells::CellCommand::Withdraw,
        gdl_cells::CellCommand::ScaleToZero,
    ] {
        let records = MemoryDirectory::new();
        let (attempts, mut attempted) = mpsc::unbounded_channel();
        let directory = LostAnswer {
            records: records.clone(),
            first: Arc::new(AtomicBool::new(true)),
            arrived: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            attempts,
        };
        let (parent, parent_cell) = hub_of(global_default(), records.clone(), timing()).await;
        let (hub, readiness) = hub_of(child(None), directory.clone(), timing()).await;
        tokio::time::timeout(WAIT, directory.arrived.notified())
            .await
            .expect("the child's first record is stored, with no answer yet");
        let command = parent_cell
            .command_child("model_x", CHILD, operator)
            .await
            .expect("the operator's command is stored")
            .expect("the parent found its child");

        directory.release.notify_one();
        let retrying = tokio::spawn(async move { readiness.set_presence(Presence::Serving).await });
        let overwrite = tokio::time::timeout(Duration::from_millis(500), async {
            while let Some((offered, accepted)) = attempted.recv().await {
                if accepted {
                    return offered;
                }
                assert!(offered < command, "the original epoch is fenced");
            }
            panic!("the directory stays alive while the hub runs");
        })
        .await;
        if let Ok(version) = overwrite {
            undone.push((operator, command, version));
        }
        retrying.abort();
        hub.shutdown().await.expect("the child's hub stops");
        parent.shutdown().await.expect("the parent's hub stops");
    }
    assert_eq!(
        undone.len(),
        2,
        "the hub moves past each command with a later epoch: {undone:?}"
    );
}

async fn recover_withdrawn_cell(disconnected: bool) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::SystemTime;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct Disconnected {
        records: MemoryDirectory,
        first: Arc<AtomicBool>,
        attempts: mpsc::UnboundedSender<(gdl_cells::Version, bool)>,
    }

    impl CellDirectory for Disconnected {
        type Watch = gdl_cells::MemoryWatch;

        async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
            let version = record.version();
            let result = if self.first.swap(false, Ordering::SeqCst) {
                Err(DirectoryError::Unavailable(
                    "connection refused before the write reached the directory".into(),
                ))
            } else {
                self.records.publish(record).await
            };
            self.attempts
                .send((version, result.is_ok()))
                .expect("the test receives the publish result");
            result
        }

        async fn withdraw(&self, withdrawal: gdl_cells::Withdrawal) -> Result<(), DirectoryError> {
            self.records.withdraw(withdrawal).await
        }

        fn watch(&self, class: &str, from: Option<gdl_cells::ResumeToken>) -> Self::Watch {
            self.records.watch(class, from)
        }
    }

    let records = MemoryDirectory::new();
    let last = gone_child(&records).await;
    let withdrawal = gdl_cells::CellCommand::Withdraw
        .write(&last, SystemTime::now() + Duration::from_secs(3600));
    let command = withdrawal.version();
    withdrawal
        .to(&records)
        .await
        .expect("the operator withdraws");
    assert!(records.record("model_x", CHILD).is_none());

    let (attempts, mut attempted) = mpsc::unbounded_channel();
    let directory = Disconnected {
        records: records.clone(),
        first: Arc::new(AtomicBool::new(disconnected)),
        attempts,
    };
    let timing = CellTiming {
        heartbeat: Duration::from_millis(20),
        ..timing()
    };
    let (hub, _readiness) = hub_of(child(None), directory, timing).await;
    let writes = tokio::time::timeout(WAIT, async {
        let mut writes = Vec::new();
        for _ in 0..4 {
            let write = attempted.recv().await.expect("the publisher keeps running");
            writes.push(write);
            if write.1 {
                break;
            }
        }
        writes
    })
    .await
    .expect("the publisher retries within the bound");
    hub.shutdown().await.expect("the restarted hub stops");
    assert!(
        writes
            .iter()
            .any(|(version, stored)| *stored && *version > command),
        "a restarted hub must recover past the withdrawal {command}; disconnected={disconnected}, writes={writes:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_hub_recovers_a_withdrawal_when_the_directory_answers() {
    recover_withdrawn_cell(false).await;
}

/// A withdrawn cell can restart behind the operator's clock, even when
/// its first write fails before reaching the directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_hub_recovers_a_withdrawal_after_an_unstored_first_write() {
    recover_withdrawn_cell(true).await;
}
