//! The directory conformance suite: the behavior every [`CellDirectory`]
//! backend owes its readers, run by [`run`] against a [`Backend`] that
//! makes fresh directories.
//!
//! A backend crate enables the `conformance` feature in its
//! dev-dependencies and calls [`run`] from a test. Every wait in a case
//! is bounded, so a broken backend fails rather than hangs, and every case
//! runs, so one failure does not hide another.
//!
//! Each of the trait's four rules ([`Rule`]) has its own cases. A backend
//! that does not keep a rule yet says so ([`Backend::keeps`]), and then
//! the rule's cases must fail on the rule itself: so what a backend says
//! of itself stays true, and a backend that comes to keep the rule fails
//! the suite until it says so.

use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, time::Duration};

use futures::{FutureExt, Stream, StreamExt};

use crate::{
    CellDirectory, CellEvent, CellRecord, CellState, DirectoryError, LoadSummary, ResumeToken,
    Version, Withdrawal,
};

/// The smallest [`Backend::buffer`] the suite takes.
pub const MIN_BUFFER: usize = 3;

/// A rule of the directory contract ([`CellDirectory`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rule {
    /// A published record stays until its cell withdraws it, across the
    /// backend's own restart. Nothing deletes it by time.
    Kept,
    /// A watch of a class yields every record of the class, whatever its
    /// region, parent, or hashes.
    EveryRecord,
    /// A watch gives a snapshot, then the changes after it (several
    /// writes of one cell may fold into the latest), and resumes from a
    /// token or says [`CellEvent::Resnapshot`]. Every backend keeps it:
    /// one that cannot resume says `Resnapshot`.
    ///
    /// The suite takes a fold of several publishes of one cell, but not
    /// of a publish into the withdrawal after it on a resumed watch: the
    /// resume case expects each of those changes.
    Changes,
    /// A record arrives whole: a watch yields each record as it was
    /// published, never with a part of another version.
    Whole,
}

/// A directory backend under test.
pub trait Backend: Sync {
    /// The backend's directory handle.
    type Directory: CellDirectory;

    /// `N` handles to one new, empty directory, connected as the hubs of
    /// `N` cells would be: a write through one is seen through every other.
    fn fresh<const N: usize>(&self) -> impl Future<Output = [Self::Directory; N]> + Send;

    /// How many changes a watcher may fall behind by: one more, and it is
    /// cut off with [`CellEvent::Resnapshot`]. The suite makes up to
    /// [`MIN_BUFFER`] changes before a watcher reads them, so it takes no
    /// smaller buffer.
    fn buffer(&self) -> usize;

    /// How many of a class's latest changes a resume token is served
    /// across: a token followed by more changes than this yields
    /// [`CellEvent::Resnapshot`]. 0 for a backend that never resumes, and
    /// says `Resnapshot` for every token.
    fn history(&self) -> usize;

    /// How many watches of `class` the backend holds for `directory`.
    fn watchers(&self, directory: &Self::Directory, class: &str) -> usize;

    /// The backend restarted: `directory`, the last of its handles to
    /// what the case made, is dropped as its process ends, and the
    /// handle returned reads what the backend keeps across the restart.
    fn restart(&self, directory: Self::Directory) -> impl Future<Output = Self::Directory> + Send;

    /// Whether the backend keeps `rule`. One that does not must fail the
    /// rule's cases on the rule itself. Every backend keeps
    /// [`Rule::Changes`].
    fn keeps(&self, rule: Rule) -> bool {
        let _ = rule;
        true
    }
}

/// Run every case against `backend`, and panic naming each case that
/// failed. Call it in a tokio runtime with its timer enabled, as
/// `#[tokio::test]` builds.
pub async fn run<B: Backend>(backend: &B) {
    assert!(
        backend.buffer() >= MIN_BUFFER,
        "the suite makes up to {MIN_BUFFER} changes before a watcher reads them, so it takes no buffer under {MIN_BUFFER}, not {}",
        backend.buffer()
    );
    assert!(
        backend.keeps(Rule::Changes),
        "every backend keeps {:?}: one that cannot resume says `Resnapshot`",
        Rule::Changes
    );
    let mut failed = Vec::new();
    macro_rules! case {
        ($name:ident) => {
            case!($name, None)
        };
        ($name:ident, $rule:expr) => {
            let rule: Option<Rule> = $rule;
            let kept = rule.is_none_or(|rule| backend.keeps(rule));
            let name = stringify!($name);
            eprintln!(
                "directory conformance: {name}{}",
                if kept {
                    ""
                } else {
                    " (a rule the backend does not keep: must fail on it)"
                }
            );
            let outcome = AssertUnwindSafe($name(backend)).catch_unwind().await;
            match (rule, kept, outcome) {
                (_, true, Ok(())) => {}
                (_, true, Err(_)) => failed.push(name.to_owned()),
                (Some(rule), false, Ok(())) => failed.push(format!(
                    "{name} (passed, though the backend says it does not keep {rule:?})"
                )),
                (Some(rule), false, Err(panic)) => {
                    let why = message(&*panic);
                    if !why.starts_with(&marker(rule)) {
                        failed.push(format!("{name} (failed, but not on {rule:?}: {why})"));
                    }
                }
                (None, false, _) => unreachable!("a case without a rule is always kept"),
            }
        };
    }
    case!(malformed_writes_are_refused);
    case!(writes_are_ordered_by_epoch_and_seq);
    case!(
        a_watch_sees_its_class_snapshot_then_changes,
        Some(Rule::Changes)
    );
    case!(a_watch_resumes_after_a_token, Some(Rule::Changes));
    case!(a_lagging_watcher_is_cut_off, Some(Rule::Changes));
    case!(a_dropped_watch_is_released);
    case!(a_record_stays_until_withdrawn, Some(Rule::Kept));
    case!(
        a_watch_sees_every_record_of_its_class,
        Some(Rule::EveryRecord)
    );
    case!(a_record_arrives_whole, Some(Rule::Whole));
    assert!(
        failed.is_empty(),
        "directory conformance failed: {}",
        failed.join(", ")
    );
}

/// What a panic of a case says.
fn message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|why| (*why).to_owned()))
        .unwrap_or_default()
}

/// How a panic that breaks `rule` begins, which [`broke`] writes.
fn marker(rule: Rule) -> String {
    format!("rule {rule:?} broken: ")
}

/// Fail the case for breaking `rule`, saying `why`: unlike any other
/// failure of the case, this is how a backend that does not keep the rule
/// is expected to fail it.
fn broke(rule: Rule, why: impl std::fmt::Display) -> ! {
    panic!("{}{why}", marker(rule))
}

const WAIT: Duration = Duration::from_secs(5);
const MODEL: &str = "model_x";
const GLOBAL: &str = "global";

type Watch<D> = Pin<Box<<D as CellDirectory>::Watch>>;

/// What the helpers read from: a watch, pinned.
trait Events: Stream<Item = Result<CellEvent, DirectoryError>> + Unpin {}

impl<S: Stream<Item = Result<CellEvent, DirectoryError>> + Unpin> Events for S {}

/// A record with a model, so that a backend is held to keeping a record
/// whole, its models with it.
fn record(class: &str, cell: &str, epoch: u64, seq: u64) -> CellRecord {
    CellRecord {
        presence: crate::Presence::Serving,
        models: vec![crate::ModelEntry {
            model: format!("{cell}-model"),
            routing_key: Some("a".into()),
            state: crate::ModelState::Ready,
            since: seq,
        }],
        models_version: seq,
        name: cell.into(),
        class: class.into(),
        region: "us-east-1".into(),
        home: None,
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

/// The next event is `Resnapshot`, and then the watch ends.
async fn resnapshot(watch: &mut impl Events, what: &str) {
    assert_eq!(next(watch, what).await, CellEvent::Resnapshot, "{what}");
    ends(watch, what).await;
}

/// The records of a snapshot, by cell name, and its `Synced` token.
async fn snapshot(watch: &mut impl Events) -> (Vec<(String, Version)>, ResumeToken) {
    let (records, token) = snapshot_records(watch).await;
    let records = records
        .into_iter()
        .map(|record| (record.name.clone(), record.version()))
        .collect();
    (records, token)
}

/// The whole records of a snapshot, sorted by cell name, and its `Synced`
/// token. The whole snapshot is bounded, not only each of its events, so
/// a snapshot that never ends fails the case.
async fn snapshot_records(watch: &mut impl Events) -> (Vec<CellRecord>, ResumeToken) {
    within("the whole snapshot", async {
        let mut records = Vec::new();
        loop {
            // A watch whose every event is ready at once would keep this
            // loop in one poll, where the bound never gets to fire: spend
            // the task's budget, so that the loop yields once it is spent.
            tokio::task::consume_budget().await;
            match next(watch, "the snapshot").await {
                CellEvent::Current(record) => records.push(record),
                CellEvent::Synced {
                    token,
                    snapshot: true,
                } => {
                    records.sort_by(|a, b| a.name.cmp(&b.name));
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

    // A token is served while no more changes follow it than the backend
    // keeps; a backend that keeps none never resumes. Any other token
    // yields `Resnapshot`, and the watch ends.
    let served = |changes: usize| backend.history() > 0 && changes <= backend.history();

    // From a change's token, which two changes follow: exactly the changes
    // after it, then live ones.
    let mut resumed = watch(&reader, MODEL, Some(after_m1.clone()));
    let replayed = served(2);
    if replayed {
        match next(&mut resumed, "the resumed watch's start").await {
            CellEvent::Synced {
                token,
                snapshot: false,
            } => assert_eq!(token, after_m1),
            other => panic!("a resumed watch starts with `Synced`, not {other:?}"),
        }
        put(&mut resumed, "m2", 1, 1).await;
        withdrawn(&mut resumed, "m1", 1, 2).await;
    } else {
        resnapshot(&mut resumed, "a token two changes ago").await;
    }
    publish(&writer, record(MODEL, "m3", 1, 1))
        .await
        .expect("m3");
    if replayed {
        put(&mut resumed, "m3", 1, 1).await;
    }
    drop(resumed);

    // From the snapshot's token, which four changes follow: every change
    // since.
    let mut resumed = watch(&writer, MODEL, Some(start));
    if served(4) {
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
    } else {
        resnapshot(&mut resumed, "a token four changes ago").await;
    }
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

/// Rule [`Rule::Kept`]: a record whose lease has passed is still held,
/// and a restart of the backend keeps every record and every withdrawal.
async fn a_record_stays_until_withdrawn<B: Backend>(backend: &B) {
    let [writer, reader] = within("a fresh directory", backend.fresh()).await;
    let lease = Duration::from_millis(20);
    // Fields away from their defaults, so that a backend that keeps only
    // some of them fails.
    let m1 = CellRecord {
        home: Some("global-us".into()),
        contract: 7,
        placement: 8,
        state: CellState::Degraded,
        hub: "m1-kept:4242".into(),
        lease,
        load: Some(LoadSummary {
            admitted: 3,
            limit: 16,
            instances: 2,
            refusals: 1,
            period_ms: 10_000,
        }),
        ..record(MODEL, "m1", 1, 1)
    };
    publish(&writer, m1.clone())
        .await
        .expect("a record with a short lease");
    publish(&writer, record(MODEL, "m2", 1, 1))
        .await
        .expect("a record");
    withdraw(&writer, withdrawal(MODEL, "m2", 1, 2))
        .await
        .expect("its withdrawal");

    // The rule is about time passing, so the case lets it pass: ten
    // leases, on whichever clock the backend keeps, and four of the `dir`
    // backend's polls.
    tokio::time::sleep(lease * 10).await;
    let (now, _) = snapshot_records(&mut watch(&reader, MODEL, None)).await;
    if now != [m1.clone()] {
        broke(
            Rule::Kept,
            format_args!("once m1's lease passed, a new watch holds {now:?}, not m1"),
        );
    }

    drop(writer);
    let restarted = within("a restart", backend.restart(reader)).await;
    // Every field of the record is kept, not only its version.
    let (now, _) = snapshot_records(&mut watch(&restarted, MODEL, None)).await;
    if now != [m1] {
        broke(
            Rule::Kept,
            format_args!("after a restart, the directory holds {now:?}, not m1"),
        );
    }
    // A withdrawal is kept too, at its own version, or a late write of a
    // withdrawn cell would bring it back.
    match publish(&restarted, record(MODEL, "m2", 1, 2)).await {
        Err(DirectoryError::NotNewer {
            held: Version { epoch: 1, seq: 2 },
            ..
        }) => {}
        Err(DirectoryError::NotNewer { held, .. }) => broke(
            Rule::Kept,
            format_args!(
                "after a restart, the directory holds m2 at {held:?}, not its withdrawal's (1, 2)"
            ),
        ),
        Ok(()) => broke(
            Rule::Kept,
            "after a restart, a write of m2 at its withdrawal's version was taken",
        ),
        Err(error) => panic!("after a restart, a write of m2: {error}"),
    }
}

/// Rule [`Rule::EveryRecord`]: every watch of a class, through any
/// handle, sees every record of the class, in every region, of every
/// parent, and of any hashes: which cells to follow is the reader's
/// choice.
async fn a_watch_sees_every_record_of_its_class<B: Backend>(backend: &B) {
    let directories: [B::Directory; 3] = within("a fresh directory", backend.fresh()).await;
    let records = [
        record(MODEL, "m1", 1, 1),
        CellRecord {
            region: "eu-west-1".into(),
            home: Some("global-eu".into()),
            ..record(MODEL, "m2", 1, 1)
        },
        CellRecord {
            home: Some("global-us".into()),
            contract: 9,
            placement: 8,
            ..record(MODEL, "m3", 1, 1)
        },
    ];
    for (directory, record) in directories.iter().zip(records.clone()) {
        publish(directory, record).await.expect("a model cell");
    }
    let mut watches = Vec::new();
    for (handle, directory) in directories.iter().enumerate() {
        let mut watch = watch(directory, MODEL, None);
        let (held, _) = snapshot_records(&mut watch).await;
        if held != records {
            let names: Vec<_> = held.iter().map(|record| record.name.as_str()).collect();
            broke(
                Rule::EveryRecord,
                format_args!(
                    "a watch through handle {handle} holds {names:?}, not m1, m2, and m3 as published: {held:?}"
                ),
            );
        }
        watches.push(watch);
    }

    let m4 = CellRecord {
        region: "ap-south-1".into(),
        home: Some("global-ap".into()),
        ..record(MODEL, "m4", 1, 1)
    };
    publish(&directories[2], m4.clone())
        .await
        .expect("a cell in a third region");
    for (handle, watch) in watches.iter_mut().enumerate() {
        // Only a put that never comes, or another change in its place,
        // breaks the rule; a watch that fails or ends is a failure of its
        // own.
        match tokio::time::timeout(WAIT, watch.next()).await {
            Ok(Some(Ok(CellEvent::Put { record, .. }))) if record == m4 => {}
            Err(_) => broke(
                Rule::EveryRecord,
                format_args!("the watch through handle {handle} never got the put of m4"),
            ),
            Ok(Some(Ok(event))) => broke(
                Rule::EveryRecord,
                format_args!("the watch through handle {handle} got {event:?}, not the put of m4"),
            ),
            Ok(Some(Err(error))) => panic!("the watch through handle {handle} failed: {error}"),
            Ok(None) => panic!("the watch through handle {handle} ended"),
        }
    }
}

/// A record with every field set, at `seq`: each field but the name,
/// class, and epoch differs between two seqs, so that a part of one
/// version with the rest of another equals neither.
fn whole(seq: u64) -> CellRecord {
    CellRecord {
        presence: crate::Presence::Serving,
        models: Vec::new(),
        models_version: 0,
        name: "m1".into(),
        class: MODEL.into(),
        region: format!("region-{seq}"),
        home: Some(format!("global-{seq}")),
        contract: 100 + seq,
        placement: 200 + seq,
        state: if seq.is_multiple_of(2) {
            CellState::Ready
        } else {
            CellState::Degraded
        },
        epoch: 1,
        seq,
        hub: format!("m1-{seq}:1337"),
        lease: Duration::from_secs(30 + seq),
        load: Some(LoadSummary {
            admitted: 10 * seq,
            limit: 64 + seq,
            instances: 2 + seq as u32,
            refusals: seq,
            period_ms: 10_000 + seq,
        }),
    }
}

/// The next changes of `watch` are the puts of `first` and then `second`,
/// two versions of one cell, each whole; or only the put of `second`,
/// which rule 3 lets a backend fold `first` into.
async fn whole_changes(
    watch: &mut impl Events,
    first: &CellRecord,
    second: &CellRecord,
    what: &str,
) {
    let mut expected = [first, second].into_iter();
    while let Some(published) = expected.next() {
        match next(watch, what).await {
            CellEvent::Put { record, .. } if record == *published => {}
            CellEvent::Put { record, .. }
                if published == first && record.version() == second.version() =>
            {
                if record != *second {
                    broke(
                        Rule::Whole,
                        format_args!("published {second:?}, and {what} is {record:?}"),
                    );
                }
                expected.next();
            }
            CellEvent::Put { record, .. } => broke(
                Rule::Whole,
                format_args!("published {published:?}, and {what} is {record:?}"),
            ),
            other => panic!("{what}: {other:?}"),
        }
    }
}

/// Rule [`Rule::Whole`]: a record arrives as it was published, every
/// field of it, in a snapshot, as a change, and as each change a resumed
/// watch replays. A backend that stores a part of a record apart (a
/// second table, say) installs the record only with its matching part.
async fn a_record_arrives_whole<B: Backend>(backend: &B) {
    let [writer, reader] = within("a fresh directory", backend.fresh()).await;
    let mut live = watch(&reader, MODEL, None);
    let (_, start) = snapshot(&mut live).await;

    let first = whole(1);
    publish(&writer, first.clone()).await.expect("a record");
    let (held, _) = snapshot_records(&mut watch(&reader, MODEL, None)).await;
    if held != [first.clone()] {
        broke(
            Rule::Whole,
            format_args!("published {first:?}, and a snapshot holds {held:?}"),
        );
    }
    // Both writes are made before the live watch reads either, so that a
    // backend that joins a queued change to a part written since serves
    // the first with the second's part.
    let second = whole(2);
    publish(&writer, second.clone())
        .await
        .expect("a newer record");
    whole_changes(&mut live, &first, &second, "a watch's change").await;
    // A resumed watch replays both versions, each whole.
    let mut resumed = watch(&reader, MODEL, Some(start));
    match next(&mut resumed, "the resumed watch's start").await {
        CellEvent::Synced {
            snapshot: false, ..
        } => whole_changes(&mut resumed, &first, &second, "a resumed watch's change").await,
        // A backend whose history does not cover both says so: there is
        // no replay.
        CellEvent::Resnapshot => {}
        other => panic!("the resumed watch's start: {other:?}"),
    }
    let (held, _) = snapshot_records(&mut watch(&reader, MODEL, None)).await;
    if held != [second.clone()] {
        broke(
            Rule::Whole,
            format_args!("published {second:?}, and a snapshot holds {held:?}"),
        );
    }
}
