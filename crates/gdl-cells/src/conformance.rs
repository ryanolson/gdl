//! The directory conformance suite: the behavior every [`CellDirectory`]
//! backend owes its readers, run by [`run`] against a [`Backend`] that
//! makes fresh directories.
//!
//! A backend crate enables the `conformance` feature in its
//! dev-dependencies and calls [`run`] from a test. Every wait in a case
//! is bounded, so a broken backend fails rather than hangs, and every case
//! runs, so one failure does not hide another.

use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, time::Duration};

use futures::{FutureExt, Stream, StreamExt};

use crate::{
    CellDirectory, CellEvent, CellRecord, CellState, DirectoryError, ResumeToken, Version,
    Withdrawal,
};

/// A directory backend under test.
pub trait Backend: Sync {
    /// The backend's directory handle.
    type Directory: CellDirectory;

    /// `N` handles to one new, empty directory, connected as the hubs of
    /// `N` cells would be: a write through one is seen through every other.
    fn fresh<const N: usize>(&self) -> impl Future<Output = [Self::Directory; N]> + Send;

    /// How many changes a watcher may fall behind by: one more, and it is
    /// cut off with [`CellEvent::Resnapshot`].
    fn buffer(&self) -> usize;

    /// How many of a class's latest changes a resume token is served
    /// across: a token followed by more changes than this yields
    /// [`CellEvent::Resnapshot`].
    fn history(&self) -> usize;

    /// How many watches of `class` the backend holds for `directory`.
    fn watchers(&self, directory: &Self::Directory, class: &str) -> usize;
}

/// Run every case against `backend`, and panic naming each case that
/// failed. Call it in a tokio runtime with its timer enabled, as
/// `#[tokio::test]` builds.
pub async fn run<B: Backend>(backend: &B) {
    let mut failed = Vec::new();
    macro_rules! case {
        ($name:ident) => {
            eprintln!("directory conformance: {}", stringify!($name));
            if AssertUnwindSafe($name(backend))
                .catch_unwind()
                .await
                .is_err()
            {
                failed.push(stringify!($name));
            }
        };
    }
    case!(malformed_writes_are_refused);
    case!(writes_are_ordered_by_epoch_and_seq);
    case!(a_watch_sees_its_class_snapshot_then_changes);
    case!(a_watch_resumes_after_a_token);
    case!(a_lagging_watcher_is_cut_off);
    case!(a_dropped_watch_is_released);
    assert!(
        failed.is_empty(),
        "directory conformance failed: {}",
        failed.join(", ")
    );
}

const WAIT: Duration = Duration::from_secs(5);
const MODEL: &str = "model_x";
const GLOBAL: &str = "global";

type Watch<D> = Pin<Box<<D as CellDirectory>::Watch>>;

/// What the helpers read from: a watch, pinned.
trait Events: Stream<Item = Result<CellEvent, DirectoryError>> + Unpin {}

impl<S: Stream<Item = Result<CellEvent, DirectoryError>> + Unpin> Events for S {}

fn record(class: &str, cell: &str, epoch: u64, seq: u64) -> CellRecord {
    CellRecord {
        name: cell.into(),
        class: class.into(),
        region: "us-east-1".into(),
        contract: 1,
        placement: 2,
        state: CellState::Ready,
        epoch,
        seq,
        hub: format!("{cell}:1337"),
        lease: CellRecord::DEFAULT_LEASE,
        load: None,
    }
}

fn withdrawal(class: &str, cell: &str, epoch: u64, seq: u64) -> Withdrawal {
    Withdrawal {
        class: class.into(),
        cell: cell.into(),
        epoch,
        seq,
    }
}

async fn within<F: Future>(what: &str, future: F) -> F::Output {
    tokio::time::timeout(WAIT, future)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {WAIT:?}"))
}

async fn publish<D: CellDirectory>(
    directory: &D,
    record: CellRecord,
) -> Result<(), DirectoryError> {
    within("publish", directory.publish(record)).await
}

async fn withdraw<D: CellDirectory>(directory: &D, w: Withdrawal) -> Result<(), DirectoryError> {
    within("withdraw", directory.withdraw(w)).await
}

fn watch<D: CellDirectory>(directory: &D, class: &str, from: Option<ResumeToken>) -> Watch<D> {
    Box::pin(directory.watch(class, from))
}

async fn next(watch: &mut impl Events, what: &str) -> CellEvent {
    match within(what, watch.next()).await {
        Some(Ok(event)) => event,
        Some(Err(error)) => panic!("{what}: the watch failed: {error}"),
        None => panic!("{what}: the watch ended"),
    }
}

async fn ends(watch: &mut impl Events, what: &str) {
    if let Some(event) = within(what, watch.next()).await {
        panic!("{what}: the watch went on with {event:?}");
    }
}

/// The records of a snapshot, by cell name, and its `Synced` token. The
/// whole snapshot is bounded, not only each of its events, so a snapshot
/// that never ends fails the case.
async fn snapshot(watch: &mut impl Events) -> (Vec<(String, Version)>, ResumeToken) {
    within("the whole snapshot", async {
        let mut records = Vec::new();
        loop {
            match next(watch, "the snapshot").await {
                CellEvent::Current(record) => records.push((record.name.clone(), record.version())),
                CellEvent::Synced {
                    token,
                    snapshot: true,
                } => {
                    records.sort();
                    return (records, token);
                }
                other => panic!("the snapshot: {other:?}"),
            }
        }
    })
    .await
}

/// What the directory holds for `class`, read through a new watch's
/// snapshot: each cell's name and version.
async fn held<D: CellDirectory>(directory: &D, class: &str) -> Vec<(String, Version)> {
    snapshot(&mut watch(directory, class, None)).await.0
}

fn at(cell: &str, epoch: u64, seq: u64) -> (String, Version) {
    (cell.to_owned(), Version { epoch, seq })
}

/// The next event is a put of `cell` at `version`; its token.
async fn put(watch: &mut impl Events, cell: &str, epoch: u64, seq: u64) -> ResumeToken {
    match next(watch, &format!("the put of {cell}")).await {
        CellEvent::Put { record, token }
            if record.name == cell && record.version() == (Version { epoch, seq }) =>
        {
            token
        }
        other => panic!("expected the put of {cell} at ({epoch}, {seq}), got {other:?}"),
    }
}

/// The next event is the withdrawal of `cell` at `version`; its token.
async fn withdrawn(watch: &mut impl Events, cell: &str, epoch: u64, seq: u64) -> ResumeToken {
    match next(watch, &format!("the withdrawal of {cell}")).await {
        CellEvent::Withdrawn { withdrawal, token }
            if withdrawal.cell == cell && withdrawal.version() == (Version { epoch, seq }) =>
        {
            token
        }
        other => panic!("expected the withdrawal of {cell} at ({epoch}, {seq}), got {other:?}"),
    }
}

fn not_newer(result: Result<(), DirectoryError>, held: (u64, u64), offered: (u64, u64)) {
    let v = |(epoch, seq)| Version { epoch, seq };
    match result {
        Err(DirectoryError::NotNewer {
            held: h,
            offered: o,
            ..
        }) if h == v(held) && o == v(offered) => {}
        other => panic!("expected {offered:?} refused as not newer than {held:?}, got {other:?}"),
    }
}

async fn malformed_writes_are_refused<B: Backend>(backend: &B) {
    let [writer, reader] = within("a fresh directory", backend.fresh()).await;
    let mut watch = watch(&reader, MODEL, None);
    snapshot(&mut watch).await;
    let bad = [
        CellRecord {
            name: "M1".into(),
            ..record(MODEL, "m1", 1, 1)
        },
        CellRecord {
            region: "-east".into(),
            ..record(MODEL, "m1", 1, 1)
        },
        CellRecord {
            class: "model-x".into(),
            ..record(MODEL, "m1", 1, 1)
        },
        CellRecord {
            lease: Duration::ZERO,
            ..record(MODEL, "m1", 1, 1)
        },
        CellRecord {
            lease: Duration::from_micros(1_500),
            ..record(MODEL, "m1", 1, 1)
        },
    ];
    for record in bad {
        let result = publish(&writer, record.clone()).await;
        assert!(
            matches!(result, Err(DirectoryError::Malformed(_))),
            "{record:?}: {result:?}"
        );
    }
    for w in [
        withdrawal(MODEL, "M1", 1, 1),
        withdrawal("Model", "m1", 1, 1),
    ] {
        let result = withdraw(&writer, w.clone()).await;
        assert!(
            matches!(result, Err(DirectoryError::Malformed(_))),
            "{w:?}: {result:?}"
        );
    }
    // Nothing reached the watcher: the next event is a good write's.
    publish(&writer, record(MODEL, "m1", 1, 1))
        .await
        .expect("a good record");
    put(&mut watch, "m1", 1, 1).await;
}

async fn writes_are_ordered_by_epoch_and_seq<B: Backend>(backend: &B) {
    let [writer, reader] = within("a fresh directory", backend.fresh()).await;
    let mut watch = watch(&reader, MODEL, None);
    snapshot(&mut watch).await;

    publish(&writer, record(MODEL, "m1", 1, 1))
        .await
        .expect("a first record");
    put(&mut watch, "m1", 1, 1).await;
    publish(&writer, record(MODEL, "m1", 1, 2))
        .await
        .expect("a higher seq");
    put(&mut watch, "m1", 1, 2).await;
    // Not newer: refused, stored nowhere, and seen by no watcher.
    not_newer(
        publish(&writer, record(MODEL, "m1", 1, 2)).await,
        (1, 2),
        (1, 2),
    );
    not_newer(
        publish(&writer, record(MODEL, "m1", 1, 1)).await,
        (1, 2),
        (1, 1),
    );
    // Read before any later write could cover what a refusal stored.
    assert_eq!(held(&reader, MODEL).await, [at("m1", 1, 2)]);
    // A new epoch wins over any seq of an older one.
    publish(&writer, record(MODEL, "m1", 2, 0))
        .await
        .expect("a new epoch");
    put(&mut watch, "m1", 2, 0).await;

    // A withdrawal is a write: newer than the record, or refused.
    not_newer(
        withdraw(&writer, withdrawal(MODEL, "m1", 2, 0)).await,
        (2, 0),
        (2, 0),
    );
    assert_eq!(held(&reader, MODEL).await, [at("m1", 2, 0)]);
    withdraw(&writer, withdrawal(MODEL, "m1", 2, 1))
        .await
        .expect("a withdrawal");
    withdrawn(&mut watch, "m1", 2, 1).await;
    // Its tombstone stops a late publish; a newer epoch publishes again.
    not_newer(
        publish(&writer, record(MODEL, "m1", 2, 1)).await,
        (2, 1),
        (2, 1),
    );
    not_newer(
        publish(&writer, record(MODEL, "m1", 1, 9)).await,
        (2, 1),
        (1, 9),
    );
    assert_eq!(held(&reader, MODEL).await, []);
    publish(&writer, record(MODEL, "m1", 3, 0))
        .await
        .expect("a newer epoch");
    put(&mut watch, "m1", 3, 0).await;

    // Withdrawing a cell never held leaves a tombstone all the same.
    withdraw(&writer, withdrawal(MODEL, "m9", 1, 5))
        .await
        .expect("a withdrawal of nothing");
    withdrawn(&mut watch, "m9", 1, 5).await;
    not_newer(
        publish(&writer, record(MODEL, "m9", 1, 5)).await,
        (1, 5),
        (1, 5),
    );
    assert_eq!(held(&reader, MODEL).await, [at("m1", 3, 0)]);
    publish(&writer, record(MODEL, "m9", 1, 6))
        .await
        .expect("newer than the tombstone");
    put(&mut watch, "m9", 1, 6).await;

    // The directory holds each cell's newest write, through any handle.
    assert_eq!(held(&writer, MODEL).await, [at("m1", 3, 0), at("m9", 1, 6)]);
}

async fn a_watch_sees_its_class_snapshot_then_changes<B: Backend>(backend: &B) {
    let [writer, reader, other] = within("a fresh directory", backend.fresh()).await;
    let mut empty = watch(&reader, MODEL, None);
    assert!(
        snapshot(&mut empty).await.0.is_empty(),
        "a new directory holds nothing"
    );
    drop(empty);

    for cell in ["m2", "m1"] {
        publish(&writer, record(MODEL, cell, 1, 1))
            .await
            .expect("a model cell");
    }
    publish(&writer, record(GLOBAL, "g1", 1, 1))
        .await
        .expect("a global cell");
    let mut first = watch(&reader, MODEL, None);
    let mut second = watch(&other, MODEL, None);
    for watch in [&mut first, &mut second] {
        let (records, _) = snapshot(watch).await;
        let v = Version { epoch: 1, seq: 1 };
        assert_eq!(records, [("m1".to_owned(), v), ("m2".to_owned(), v)]);
    }

    // Each watcher sees every change of its class, in order, and nothing
    // of another class.
    publish(&writer, record(GLOBAL, "g2", 1, 1))
        .await
        .expect("a global cell");
    publish(&writer, record(MODEL, "m3", 1, 1))
        .await
        .expect("a model cell");
    withdraw(&writer, withdrawal(MODEL, "m1", 1, 2))
        .await
        .expect("a withdrawal");
    withdraw(&writer, withdrawal(GLOBAL, "g1", 1, 2))
        .await
        .expect("a withdrawal");
    publish(&writer, record(MODEL, "m2", 1, 2))
        .await
        .expect("a heartbeat");
    for watch in [&mut first, &mut second] {
        put(watch, "m3", 1, 1).await;
        withdrawn(watch, "m1", 1, 2).await;
        put(watch, "m2", 1, 2).await;
    }

    // A watch of the other class sees only its own.
    let mut global = watch(&reader, GLOBAL, None);
    let (records, _) = snapshot(&mut global).await;
    assert_eq!(records, [("g2".to_owned(), Version { epoch: 1, seq: 1 })]);
}

async fn a_watch_resumes_after_a_token<B: Backend>(backend: &B) {
    let [writer, reader] = within("a fresh directory", backend.fresh()).await;
    // The other class has as many changes as this case makes before it
    // tries a token of this class there, so a backend that numbers each
    // class's changes alike would find the position, and only the token's
    // class refuses it.
    for seq in 1..=3 {
        publish(&writer, record(GLOBAL, "g1", 1, seq))
            .await
            .expect("a global cell");
    }
    let mut live = watch(&reader, MODEL, None);
    let (_, start) = snapshot(&mut live).await;
    publish(&writer, record(MODEL, "m1", 1, 1))
        .await
        .expect("m1");
    let after_m1 = put(&mut live, "m1", 1, 1).await;
    publish(&writer, record(MODEL, "m2", 1, 1))
        .await
        .expect("m2");
    put(&mut live, "m2", 1, 1).await;
    withdraw(&writer, withdrawal(MODEL, "m1", 1, 2))
        .await
        .expect("m1 withdrawn");
    let last = withdrawn(&mut live, "m1", 1, 2).await;
    drop(live);

    // From a change's token: exactly the changes after it, then live ones.
    let mut resumed = watch(&reader, MODEL, Some(after_m1.clone()));
    match next(&mut resumed, "the resumed watch's start").await {
        CellEvent::Synced {
            token,
            snapshot: false,
        } => assert_eq!(token, after_m1),
        other => panic!("a resumed watch starts with `Synced`, not {other:?}"),
    }
    put(&mut resumed, "m2", 1, 1).await;
    withdrawn(&mut resumed, "m1", 1, 2).await;
    publish(&writer, record(MODEL, "m3", 1, 1))
        .await
        .expect("m3");
    put(&mut resumed, "m3", 1, 1).await;
    drop(resumed);

    // From the snapshot's token: every change since.
    let mut resumed = watch(&writer, MODEL, Some(start));
    assert!(matches!(
        next(&mut resumed, "the start").await,
        CellEvent::Synced {
            snapshot: false,
            ..
        }
    ));
    put(&mut resumed, "m1", 1, 1).await;
    put(&mut resumed, "m2", 1, 1).await;
    withdrawn(&mut resumed, "m1", 1, 2).await;
    put(&mut resumed, "m3", 1, 1).await;
    drop(resumed);

    // Another class's token is not served, though that class has changes
    // up to its position.
    let mut elsewhere = watch(&reader, GLOBAL, Some(last));
    assert_eq!(
        next(&mut elsewhere, "another class's token").await,
        CellEvent::Resnapshot
    );
    ends(&mut elsewhere, "another class's token").await;

    // Nor is a token more changes ago than the backend keeps.
    for seq in 0..=backend.history() as u64 {
        publish(&writer, record(MODEL, "m3", 2, seq))
            .await
            .expect("a heartbeat");
    }
    let mut stale = watch(&reader, MODEL, Some(after_m1));
    assert_eq!(
        next(&mut stale, "a stale token").await,
        CellEvent::Resnapshot
    );
    ends(&mut stale, "a stale token").await;
}

async fn a_lagging_watcher_is_cut_off<B: Backend>(backend: &B) {
    let [writer, reader] = within("a fresh directory", backend.fresh()).await;
    let mut lagging = watch(&reader, MODEL, None);
    snapshot(&mut lagging).await;
    let mut reading = watch(&writer, MODEL, None);
    snapshot(&mut reading).await;

    // While one watcher reads nothing, every publish completes and the
    // other watcher sees every change.
    let changes = backend.buffer() as u64 + 1;
    for seq in 1..=changes {
        publish(&writer, record(MODEL, "m1", 1, seq))
            .await
            .expect("a heartbeat");
        put(&mut reading, "m1", 1, seq).await;
    }

    // The lagging watcher sees an unbroken prefix of the changes, then
    // `Resnapshot`, then the end.
    let mut expected = 1;
    loop {
        match next(&mut lagging, "the lagging watcher").await {
            CellEvent::Put { record, .. } => {
                // No more changes were made than this, so the loop ends.
                assert!(expected <= changes, "a change never made: {record:?}");
                assert_eq!(
                    record.version(),
                    Version {
                        epoch: 1,
                        seq: expected
                    },
                    "a gap"
                );
                expected += 1;
            }
            CellEvent::Resnapshot => break,
            other => panic!("the lagging watcher: {other:?}"),
        }
    }
    assert!(
        expected <= changes,
        "every change fit: the watcher did not lag"
    );
    ends(&mut lagging, "the lagging watcher").await;
}

async fn a_dropped_watch_is_released<B: Backend>(backend: &B) {
    let [directory] = within("a fresh directory", backend.fresh()).await;
    assert_eq!(backend.watchers(&directory, MODEL), 0);
    let mut first = watch(&directory, MODEL, None);
    let mut second = watch(&directory, MODEL, None);
    let mut global = watch(&directory, GLOBAL, None);
    for watch in [&mut first, &mut second, &mut global] {
        snapshot(watch).await;
    }
    assert_eq!(backend.watchers(&directory, MODEL), 2);
    drop(first);
    assert_eq!(backend.watchers(&directory, MODEL), 1);
    drop(second);
    assert_eq!(backend.watchers(&directory, MODEL), 0);
    assert_eq!(backend.watchers(&directory, GLOBAL), 1);
    drop(global);
    assert_eq!(backend.watchers(&directory, GLOBAL), 0);

    // A watch that ended at once holds nothing.
    let mut stale = watch(&directory, MODEL, Some(ResumeToken::new(MODEL, u64::MAX)));
    assert_eq!(
        next(&mut stale, "a token from the future").await,
        CellEvent::Resnapshot
    );
    assert_eq!(backend.watchers(&directory, MODEL), 0);
}
