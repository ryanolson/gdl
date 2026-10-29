//! The `relay` backend's directory, the records a parent pool's hub holds
//! for its children, passes the directory conformance suite, and keeps
//! [`Rule::Kept`] when it is given a state directory: it writes each
//! record and withdrawal there before it takes it, and reads them back
//! when it opens. A record whose lease has passed stays in a new watch's
//! snapshot, since expiry is the reader's. As a store it keeps
//! [`Rule::EveryRecord`]; the relay backend as a whole does not, since a
//! parent's hub takes only its own children's reports
//! (`crates/gdl-hub/tests/relay.rs`).

use std::{sync::Mutex, time::Duration};

use futures::StreamExt;
use gdl_pools::{
    DirectoryError, MemoryLimits, PoolDirectory, PoolEvent, PoolRecord, PoolState, PoolView,
    Presence, RelayDirectory, Version, Withdrawal,
    conformance::{self, Backend},
};
use tempfile::TempDir;

/// Fresh directories, each keeping its state in a temporary directory
/// held until the case that made it is over.
struct Relay {
    limits: MemoryLimits,
    held: Mutex<Vec<TempDir>>,
}

impl Relay {
    fn new(limits: MemoryLimits) -> Self {
        Self {
            limits,
            held: Mutex::default(),
        }
    }
}

impl Backend for Relay {
    type Directory = RelayDirectory;

    async fn fresh<const N: usize>(&self) -> [RelayDirectory; N] {
        let temp = temp();
        let directory = RelayDirectory::open(temp.path(), self.limits).expect("a state directory");
        self.held.lock().expect("the held directories").push(temp);
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        self.limits.buffer
    }

    fn history(&self) -> usize {
        self.limits.history
    }

    fn watchers(&self, directory: &RelayDirectory, class: &str) -> usize {
        directory.watchers(class)
    }

    // A new hub process over the same state directory.
    async fn restart(&self, directory: RelayDirectory) -> RelayDirectory {
        let root = directory.root().expect("a stored directory").to_owned();
        drop(directory);
        RelayDirectory::open(&root, self.limits).expect("the state directory again")
    }
}

#[tokio::test]
async fn the_relay_directory_conforms_with_small_bounds() {
    conformance::run(&Relay::new(MemoryLimits {
        buffer: 8,
        history: 5,
    }))
    .await;
}

#[tokio::test]
async fn the_relay_directory_conforms_with_its_defaults() {
    conformance::run(&Relay::new(MemoryLimits::default())).await;
}

const WAIT: Duration = Duration::from_secs(5);

fn temp() -> TempDir {
    tempfile::Builder::new()
        .prefix("gdl-pools-relay-")
        .tempdir()
        .expect("a temporary directory")
}

fn record(pool: &str, seq: u64, lease: Duration) -> PoolRecord {
    PoolRecord {
        name: pool.into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some("global-us-east-1".into()),
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch: 7,
        seq,
        hub: format!("{pool}:1337"),
        lease,
        load: None,
        presence: Presence::Serving,
        properties: Vec::new(),
        properties_version: 0,
        revoked: Vec::new(),
    }
}

fn withdrawal(pool: &str, seq: u64) -> Withdrawal {
    Withdrawal {
        class: "model_x".into(),
        pool: pool.into(),
        epoch: 7,
        seq,
    }
}

/// The pools of a new watch's snapshot, each with its version.
async fn snapshot(directory: &RelayDirectory) -> Vec<(String, Version)> {
    let mut watch = std::pin::pin!(directory.watch("model_x", None));
    let mut pools = Vec::new();
    let read = async {
        loop {
            match watch.next().await {
                Some(Ok(PoolEvent::Current(record))) => {
                    pools.push((record.name.clone(), record.version()))
                }
                Some(Ok(PoolEvent::Synced { .. })) => return,
                other => panic!("the snapshot: {other:?}"),
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the snapshot in time");
    pools
}

/// A record that has not been renewed for its lease stays in a new
/// watch's snapshot: silence is not a withdrawal, and the lease is the
/// reader's to apply. Its version stays held, so an older write is still
/// refused, and only a withdrawal removes it.
#[tokio::test(start_paused = true)]
async fn a_record_not_renewed_for_its_lease_stays_until_withdrawn() {
    let directory = RelayDirectory::new();
    let lease = Duration::from_millis(300);
    directory
        .publish(record("m1", 1, lease))
        .await
        .expect("a record");
    let v = |seq| Version { epoch: 7, seq };

    tokio::time::advance(lease * 10).await;
    assert_eq!(snapshot(&directory).await, [("m1".into(), v(1))]);
    assert_eq!(directory.held("model_x", "m1"), Some(v(1)));
    assert!(directory.holds_record("model_x", "m1"));
    assert!(matches!(
        directory.publish(record("m1", 1, lease)).await,
        Err(DirectoryError::NotNewer { .. })
    ));

    directory
        .withdraw(withdrawal("m1", 2))
        .await
        .expect("a withdrawal");
    assert!(snapshot(&directory).await.is_empty());
    assert_eq!(directory.held("model_x", "m1"), Some(v(2)));
    assert!(!directory.holds_record("model_x", "m1"));
}

/// A reader that has let a record's lease pass does not take it as fresh
/// when a later watch of the same directory yields it again, at the same
/// version: the reader keeps its view across the watches, as a hub's
/// follow does, and only a newer write renews the record.
#[tokio::test]
async fn a_reader_does_not_renew_an_expired_record_on_a_new_watch() {
    let directory = RelayDirectory::new();
    let lease = Duration::from_millis(300);
    directory
        .publish(record("m1", 1, lease))
        .await
        .expect("a record");

    let start = std::time::Instant::now();
    let mut view = PoolView::new();
    let read = async |view: &mut PoolView, at: std::time::Instant| {
        let mut watch = std::pin::pin!(directory.watch("model_x", None));
        loop {
            let event = tokio::time::timeout(WAIT, watch.next())
                .await
                .expect("an event in time")
                .expect("the watch goes on")
                .expect("an event");
            let synced = matches!(event, PoolEvent::Synced { .. });
            view.apply(event, at);
            if synced {
                return;
            }
        }
    };
    read(&mut view, start).await;
    assert!(view.get("m1", start).is_some(), "fresh when first seen");

    // The watch ended, and the reader watches again once the lease has
    // passed on its clock.
    let later = start + lease * 2;
    view.apply(PoolEvent::Resnapshot, later);
    read(&mut view, later).await;
    assert!(
        view.get("m1", later).is_none(),
        "the same version, seen again, renewed the record"
    );

    directory
        .publish(record("m1", 2, lease))
        .await
        .expect("a newer write");
    read(&mut view, later).await;
    assert!(view.get("m1", later).is_some(), "a newer write renews it");
}

/// What a stored directory holds outlives it: a record, every field of
/// it, and a withdrawal at its own version, read back by the next
/// directory opened over the same state.
#[tokio::test]
async fn a_stored_directory_reads_back_what_it_held() {
    let temp = temp();
    let directory = RelayDirectory::open(temp.path(), MemoryLimits::default()).expect("opens");
    let m1 = PoolRecord {
        state: PoolState::Degraded,
        ..record("m1", 3, Duration::from_millis(300))
    };
    directory.publish(m1.clone()).await.expect("a record");
    directory
        .publish(record("m2", 1, PoolRecord::DEFAULT_LEASE))
        .await
        .expect("a record");
    directory
        .withdraw(withdrawal("m2", 2))
        .await
        .expect("a withdrawal");
    drop(directory);

    let again = RelayDirectory::open(temp.path(), MemoryLimits::default()).expect("reopens");
    let mut watch = std::pin::pin!(again.watch("model_x", None));
    assert_eq!(
        tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("in time"),
        Some(Ok(PoolEvent::Current(m1)))
    );
    assert!(matches!(
        tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("in time"),
        Some(Ok(PoolEvent::Synced { snapshot: true, .. }))
    ));
    assert_eq!(
        again.held("model_x", "m2"),
        Some(Version { epoch: 7, seq: 2 })
    );
    assert!(!again.holds_record("model_x", "m2"));
    assert!(matches!(
        again
            .publish(record("m2", 2, PoolRecord::DEFAULT_LEASE))
            .await,
        Err(DirectoryError::NotNewer { .. })
    ));
}

/// A write that cannot be stored is refused as unavailable, and reaches
/// no watcher, and holds no version: the same write is taken once the
/// state directory can be written again.
#[tokio::test]
async fn a_write_that_cannot_be_stored_is_refused_and_holds_nothing() {
    let temp = temp();
    let directory = RelayDirectory::open(temp.path(), MemoryLimits::default()).expect("opens");
    // A file where the class's directory would be.
    let blocker = temp.path().join("model_x");
    std::fs::write(&blocker, b"not a directory").expect("a file");

    let refused = directory
        .publish(record("m1", 1, PoolRecord::DEFAULT_LEASE))
        .await;
    assert!(
        matches!(refused, Err(DirectoryError::Unavailable(_))),
        "{refused:?}"
    );
    assert_eq!(directory.held("model_x", "m1"), None);
    assert!(snapshot(&directory).await.is_empty());

    std::fs::remove_file(&blocker).expect("the file goes");
    directory
        .publish(record("m1", 1, PoolRecord::DEFAULT_LEASE))
        .await
        .expect("the same write, once it can be stored");
    assert_eq!(
        snapshot(&directory).await,
        [("m1".into(), Version { epoch: 7, seq: 1 })]
    );
}

/// A state directory that cannot be read is refused when it opens, rather
/// than opened empty: a parent's hub that started without its children's
/// records would read a silent child as gone.
#[test]
fn a_state_directory_that_cannot_be_read_is_refused() {
    let temp = temp();
    let file = temp.path().join("state");
    std::fs::write(&file, b"not a directory").expect("a file");
    let refused = RelayDirectory::open(&file, MemoryLimits::default());
    assert!(
        matches!(refused, Err(DirectoryError::Unavailable(_))),
        "{refused:?}"
    );
}

/// A child scaled to 0 on purpose sends no heartbeat, and its record stays
/// in a new watch's snapshot past its lease, as a missing child's does:
/// the reader tells them apart by their presence.
#[tokio::test(start_paused = true)]
async fn a_record_scaled_to_zero_stays_past_its_lease() {
    let directory = RelayDirectory::new();
    let lease = Duration::from_millis(300);
    let scaled = PoolRecord {
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
    assert_eq!(
        snapshot(&directory).await,
        [("m1".into(), v(1)), ("m2".into(), v(1))]
    );
}

/// A resumed reader must see writes before a restart, or be told to resnapshot.
#[tokio::test]
async fn a_reopened_directory_does_not_silently_skip_a_write_on_resume() {
    let temp = temp();
    let directory = RelayDirectory::open(temp.path(), MemoryLimits::default()).expect("opens");
    let first = record("m1", 1, PoolRecord::DEFAULT_LEASE);
    directory.publish(first.clone()).await.expect("a record");
    let mut watch = directory.watch("model_x", None);
    assert_eq!(
        watch.next().await,
        Some(Ok(PoolEvent::Current(first.clone())))
    );
    let Some(Ok(PoolEvent::Synced { token, .. })) = watch.next().await else {
        panic!("the snapshot's token");
    };
    drop(watch);
    let mut view = PoolView::new();
    view.apply(PoolEvent::Current(first), std::time::Instant::now());
    directory
        .publish(record("m1", 2, PoolRecord::DEFAULT_LEASE))
        .await
        .expect("a write while the reader is disconnected");
    drop(directory);

    let again = RelayDirectory::open(temp.path(), MemoryLimits::default()).expect("reopens");
    let mut resumed = again.watch("model_x", Some(token));
    // This later write bounds the replay without a sleep or a pending read.
    again
        .publish(record("m2", 1, PoolRecord::DEFAULT_LEASE))
        .await
        .expect("a later write");
    loop {
        let event = tokio::time::timeout(WAIT, resumed.next())
            .await
            .expect("an event in time")
            .expect("the watch continues")
            .expect("an event");
        if matches!(event, PoolEvent::Resnapshot) {
            assert_eq!(
                snapshot(&again).await,
                [
                    ("m1".into(), Version { epoch: 7, seq: 2 }),
                    ("m2".into(), Version { epoch: 7, seq: 1 }),
                ]
            );
            return;
        }
        let later = matches!(&event, PoolEvent::Put { record, .. } if record.name == "m2");
        view.apply(event, std::time::Instant::now());
        if later {
            break;
        }
    }
    assert_eq!(
        view.get("m1", std::time::Instant::now())
            .map(PoolRecord::version),
        Some(Version { epoch: 7, seq: 2 }),
        "the resumed reader missed a stored write before the restart"
    );
}
