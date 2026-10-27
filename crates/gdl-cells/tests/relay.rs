//! The `relay` backend's directory, the records a parent cell's hub holds
//! for its children, passes the directory conformance suite, and keeps
//! every rule but [`Rule::Kept`]. A child's lease runs on the parent's
//! clock, so a record that has not been renewed for its lease is left out
//! of a new watch's snapshot, as a reader that had watched all along would
//! have dropped it; and the records live in the hub's memory, so a
//! restart loses them. As a store it keeps [`Rule::EveryRecord`]; the
//! relay backend as a whole does not, since a parent's hub takes only its
//! own children's reports (`crates/gdl-hub/tests/relay.rs`).

use std::time::Duration;

use futures::StreamExt;
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellState, DirectoryError, MemoryLimits, Presence,
    RelayDirectory, Version, Withdrawal,
    conformance::{self, Backend, Rule},
};

struct Relay(MemoryLimits);

impl Backend for Relay {
    type Directory = RelayDirectory;

    async fn fresh<const N: usize>(&self) -> [RelayDirectory; N] {
        let directory = RelayDirectory::with_limits(self.0);
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        self.0.buffer
    }

    fn history(&self) -> usize {
        self.0.history
    }

    fn watchers(&self, directory: &RelayDirectory, class: &str) -> usize {
        directory.watchers(class)
    }

    // The parent's hub holds its children's records in memory.
    async fn restart(&self, _: RelayDirectory) -> RelayDirectory {
        RelayDirectory::with_limits(self.0)
    }

    fn keeps(&self, rule: Rule) -> bool {
        rule != Rule::Kept
    }
}

#[tokio::test]
async fn the_relay_directory_conforms_with_small_bounds() {
    conformance::run(&Relay(MemoryLimits {
        buffer: 8,
        history: 5,
    }))
    .await;
}

#[tokio::test]
async fn the_relay_directory_conforms_with_its_defaults() {
    conformance::run(&Relay(MemoryLimits::default())).await;
}

const WAIT: Duration = Duration::from_secs(5);

fn record(cell: &str, seq: u64, lease: Duration) -> CellRecord {
    CellRecord {
        name: cell.into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some("global-us-east-1".into()),
        contract: 1,
        placement: 2,
        state: CellState::Ready,
        epoch: 7,
        seq,
        hub: format!("{cell}:1337"),
        lease,
        load: None,
        presence: Presence::Serving,
        models: Vec::new(),
        models_version: 0,
    }
}

/// The cells of a new watch's snapshot, each with its version.
async fn snapshot(directory: &RelayDirectory) -> Vec<(String, Version)> {
    let mut watch = std::pin::pin!(directory.watch("model_x", None));
    let mut cells = Vec::new();
    let read = async {
        loop {
            match watch.next().await {
                Some(Ok(CellEvent::Current(record))) => {
                    cells.push((record.name.clone(), record.version()))
                }
                Some(Ok(CellEvent::Synced { .. })) => return,
                other => panic!("the snapshot: {other:?}"),
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the snapshot in time");
    cells
}

/// A record whose lease has passed on the directory's clock since its
/// last newer write is left out of a new watch's snapshot, and nothing is
/// sent for it: silence is not a withdrawal. Its version stays held, so
/// an older write is still refused, and a newer one brings it back.
#[tokio::test(start_paused = true)]
async fn a_record_not_renewed_for_its_lease_leaves_the_snapshot() {
    let directory = RelayDirectory::new();
    let lease = Duration::from_millis(300);
    let mut live = Box::pin(directory.watch("model_x", None));
    while !matches!(live.next().await, Some(Ok(CellEvent::Synced { .. }))) {}

    directory
        .publish(record("m1", 1, lease))
        .await
        .expect("a record");
    directory
        .publish(record("m2", 1, Duration::from_secs(30)))
        .await
        .expect("a record");
    let v = |seq| Version { epoch: 7, seq };
    assert_eq!(
        snapshot(&directory).await,
        [("m1".into(), v(1)), ("m2".into(), v(1))]
    );
    assert_eq!(directory.held("model_x", "m1"), Some(v(1)));
    assert!(directory.holds_record("model_x", "m1"));

    // A copy of the same version renews nothing.
    tokio::time::advance(Duration::from_millis(200)).await;
    assert!(matches!(
        directory.publish(record("m1", 1, lease)).await,
        Err(DirectoryError::NotNewer { .. })
    ));
    tokio::time::advance(Duration::from_millis(101)).await;
    assert_eq!(snapshot(&directory).await, [("m2".into(), v(1))]);
    // Held, and refused at its version, all the same.
    assert_eq!(directory.held("model_x", "m1"), Some(v(1)));
    assert!(matches!(
        directory.publish(record("m1", 1, lease)).await,
        Err(DirectoryError::NotNewer { .. })
    ));

    directory
        .publish(record("m1", 2, lease))
        .await
        .expect("a newer write");
    assert_eq!(
        snapshot(&directory).await,
        [("m1".into(), v(2)), ("m2".into(), v(1))]
    );

    // A withdrawal leaves no record behind.
    directory
        .withdraw(Withdrawal {
            class: "model_x".into(),
            cell: "m1".into(),
            epoch: 7,
            seq: 3,
        })
        .await
        .expect("a withdrawal");
    assert_eq!(directory.held("model_x", "m1"), Some(v(3)));
    assert!(!directory.holds_record("model_x", "m1"));

    // The live watch saw every write, and no event of the expiry.
    let mut seen = Vec::new();
    for _ in 0..4 {
        match live.next().await {
            Some(Ok(CellEvent::Put { record, .. })) => seen.push(("put", record.name, record.seq)),
            Some(Ok(CellEvent::Withdrawn { withdrawal, .. })) => {
                seen.push(("withdrawn", withdrawal.cell, withdrawal.seq))
            }
            other => panic!("the live watch: {other:?}"),
        }
    }
    assert_eq!(
        seen,
        [
            ("put", "m1".into(), 1),
            ("put", "m2".into(), 1),
            ("put", "m1".into(), 2),
            ("withdrawn", "m1".into(), 3),
        ]
    );
}

/// Resuming after a lease has expired must not make the same record
/// usable when a fresh snapshot already leaves it out.
#[tokio::test(start_paused = true)]

async fn a_resumed_watch_does_not_revive_an_expired_record() {
    use gdl_cells::CellView;

    let directory = RelayDirectory::new();
    let token = {
        let mut watch = Box::pin(directory.watch("model_x", None));
        match watch.next().await {
            Some(Ok(CellEvent::Synced { token, .. })) => token,
            other => panic!("the empty directory: {other:?}"),
        }
    };
    let lease = Duration::from_millis(300);
    directory
        .publish(record("m1", 1, lease))
        .await
        .expect("a record");
    assert_eq!(snapshot(&directory).await.len(), 1);
    tokio::time::advance(lease + Duration::from_millis(1)).await;
    assert!(
        snapshot(&directory).await.is_empty(),
        "expired on the parent's clock"
    );

    let mut resumed = Box::pin(directory.watch("model_x", Some(token)));
    let mut view = CellView::new();
    let read = tokio::time::timeout(Duration::from_millis(1), async {
        while let Some(Ok(event)) = resumed.next().await {
            view.apply(event, std::time::Instant::now());
        }
    });
    let _ = read.await;
    let offered: Vec<_> = view
        .records(std::time::Instant::now())
        .map(|record| record.name.as_str())
        .collect();
    assert!(
        offered.is_empty(),
        "resuming revived records whose parent-clock lease expired: {offered:?}"
    );
}

/// A child scaled to 0 on purpose sends no heartbeat, so its record stays
/// in a new watch's snapshot past its lease: its parent tells it from a
/// child that is missing.
#[tokio::test(start_paused = true)]
async fn a_record_scaled_to_zero_stays_past_its_lease() {
    let directory = RelayDirectory::new();
    let lease = Duration::from_millis(300);
    let scaled = CellRecord {
        presence: Presence::ScaledToZero,
        ..record("m1", 1, lease)
    };
    directory.publish(scaled).await.expect("a record");
    directory
        .publish(record("m2", 1, lease))
        .await
        .expect("a record");
    tokio::time::advance(lease * 10).await;
    let v = |seq| Version { epoch: 7, seq };
    assert_eq!(snapshot(&directory).await, [("m1".into(), v(1))]);
}
