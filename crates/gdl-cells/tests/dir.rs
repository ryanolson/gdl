//! The `dir` backend passes the directory conformance suite, and holds
//! what files shared between processes add: two handles over one
//! directory, each standing for a process, see each other's writes; a
//! withdrawal's tombstone stops a stale write from the other; a reader
//! never sees a partial record; a file that does not parse is skipped;
//! and a resume token is served only by the process that watched it.

use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::StreamExt;
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellState, DirDirectory, DirLimits, DirectoryError,
    ResumeToken, Version, Withdrawal,
    conformance::{self, Backend},
};
use tempfile::TempDir;

/// Fresh directories in temporary directories, each held until the case
/// that made it is over.
struct Dir {
    limits: DirLimits,
    held: Mutex<Vec<TempDir>>,
}

impl Backend for Dir {
    type Directory = DirDirectory;

    // The handles are clones of one directory, as the hubs of one process
    // would hold it: resume tokens are served across them, and the
    // separate processes of the tests below are separate directories.
    async fn fresh<const N: usize>(&self) -> [DirDirectory; N] {
        let temp = temp();
        let directory = DirDirectory::with_limits(temp.path(), self.limits);
        self.held.lock().expect("the held directories").push(temp);
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        self.limits.buffer
    }

    fn history(&self) -> usize {
        self.limits.history
    }

    fn watchers(&self, directory: &DirDirectory, class: &str) -> usize {
        directory.watchers(class)
    }
}

// Small bounds: every change the suite waits for is found by a poll, so
// the lagging case's `buffer + 1` changes cost that many polls.
#[tokio::test]
async fn the_dir_directory_conforms() {
    conformance::run(&Dir {
        limits: DirLimits {
            buffer: 8,
            history: 5,
        },
        held: Mutex::default(),
    })
    .await;
}

const MODEL: &str = "model_x";
const WAIT: Duration = Duration::from_secs(5);

type Watch = Pin<Box<<DirDirectory as CellDirectory>::Watch>>;

fn temp() -> TempDir {
    tempfile::Builder::new()
        .prefix("gdl-cells-dir-")
        .tempdir()
        .expect("a temporary directory")
}

fn record(cell: &str, epoch: u64, seq: u64) -> CellRecord {
    CellRecord {
        name: cell.into(),
        class: MODEL.into(),
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

fn withdrawal(cell: &str, epoch: u64, seq: u64) -> Withdrawal {
    Withdrawal {
        class: MODEL.into(),
        cell: cell.into(),
        epoch,
        seq,
    }
}

fn watch(directory: &DirDirectory, from: Option<ResumeToken>) -> Watch {
    Box::pin(directory.watch(MODEL, from))
}

async fn next(watch: &mut Watch) -> Option<Result<CellEvent, DirectoryError>> {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .unwrap_or_else(|_| panic!("nothing within {WAIT:?}"))
}

async fn event(watch: &mut Watch) -> CellEvent {
    match next(watch).await {
        Some(Ok(event)) => event,
        other => panic!("expected an event, got {other:?}"),
    }
}

/// A snapshot's cells by name and version, and its token.
async fn snapshot(watch: &mut Watch) -> (Vec<(String, Version)>, ResumeToken) {
    let mut cells = Vec::new();
    loop {
        match event(watch).await {
            CellEvent::Current(record) => cells.push((record.name.clone(), record.version())),
            CellEvent::Synced {
                token,
                snapshot: true,
            } => return (cells, token),
            other => panic!("the snapshot: {other:?}"),
        }
    }
}

fn at(cell: &str, epoch: u64, seq: u64) -> (String, Version) {
    (cell.into(), Version { epoch, seq })
}

/// The next event is a put of `cell` at `(epoch, seq)`; its token.
async fn put(watch: &mut Watch, cell: &str, epoch: u64, seq: u64) -> ResumeToken {
    match event(watch).await {
        CellEvent::Put { record, token }
            if (record.name.as_str(), record.version()) == (cell, Version { epoch, seq }) =>
        {
            token
        }
        other => panic!("expected the put of {cell} at ({epoch}, {seq}), got {other:?}"),
    }
}

async fn resnapshot(watch: &mut Watch) {
    assert_eq!(event(watch).await, CellEvent::Resnapshot);
    assert!(next(watch).await.is_none(), "the watch went on");
}

#[tokio::test]
async fn two_processes_see_each_others_writes_and_a_withdrawal_stands() {
    let temp = temp();
    let (a, b) = (
        DirDirectory::new(temp.path()),
        DirDirectory::new(temp.path()),
    );
    let mut seen_by_a = watch(&a, None);
    let mut seen_by_b = watch(&b, None);
    assert!(snapshot(&mut seen_by_a).await.0.is_empty());
    assert!(snapshot(&mut seen_by_b).await.0.is_empty());

    a.publish(record("m1", 1, 1)).await.expect("m1 from a");
    for watch in [&mut seen_by_a, &mut seen_by_b] {
        put(watch, "m1", 1, 1).await;
    }
    b.publish(record("m2", 1, 1)).await.expect("m2 from b");
    for watch in [&mut seen_by_a, &mut seen_by_b] {
        put(watch, "m2", 1, 1).await;
    }
    a.withdraw(withdrawal("m1", 1, 2))
        .await
        .expect("m1 withdrawn by a");
    for watch in [&mut seen_by_a, &mut seen_by_b] {
        match event(watch).await {
            CellEvent::Withdrawn { withdrawal: w, .. } => assert_eq!(w, withdrawal("m1", 1, 2)),
            other => panic!("expected m1's withdrawal, got {other:?}"),
        }
    }

    // The other process's stale write meets the tombstone in the file.
    for seq in [1, 2] {
        match b.publish(record("m1", 1, seq)).await {
            Err(DirectoryError::NotNewer { held, offered, .. }) => {
                assert_eq!(
                    (held, offered),
                    (Version { epoch: 1, seq: 2 }, Version { epoch: 1, seq })
                );
            }
            other => panic!("a stale write of m1 at seq {seq}: {other:?}"),
        }
    }
    // It reached no watcher: the next change each sees is m2's heartbeat.
    b.publish(record("m2", 1, 2)).await.expect("m2's heartbeat");
    for watch in [&mut seen_by_a, &mut seen_by_b] {
        put(watch, "m2", 1, 2).await;
    }
    // And a third process, started now, reads the same from the files.
    let c = DirDirectory::new(temp.path());
    assert_eq!(snapshot(&mut watch(&c, None)).await.0, [at("m2", 1, 2)]);
}

#[tokio::test]
async fn a_token_is_served_only_by_the_process_that_watched() {
    let temp = temp();
    let (a, b) = (
        DirDirectory::new(temp.path()),
        DirDirectory::new(temp.path()),
    );
    let mut seen_by_a = watch(&a, None);
    let mut seen_by_b = watch(&b, None);
    snapshot(&mut seen_by_a).await;
    snapshot(&mut seen_by_b).await;
    a.publish(record("m1", 1, 1)).await.expect("m1");
    let token = put(&mut seen_by_a, "m1", 1, 1).await;
    put(&mut seen_by_b, "m1", 1, 1).await;

    // Another process, though it watches the same class.
    resnapshot(&mut watch(&b, Some(token.clone()))).await;
    // The same process after a restart.
    drop(seen_by_a);
    drop(a);
    let restarted = DirDirectory::new(temp.path());
    resnapshot(&mut watch(&restarted, Some(token))).await;
    assert_eq!(
        snapshot(&mut watch(&restarted, None)).await.0,
        [at("m1", 1, 1)]
    );
}

#[tokio::test]
async fn a_token_outlives_an_unwatched_spell_only_if_nothing_changed() {
    let temp = temp();
    let (a, b) = (
        DirDirectory::new(temp.path()),
        DirDirectory::new(temp.path()),
    );
    let mut seen = watch(&a, None);
    let (_, token) = snapshot(&mut seen).await;
    drop(seen);

    // Nothing changed while `a` watched nothing: the token resumes.
    let mut resumed = watch(&a, Some(token.clone()));
    assert_eq!(
        event(&mut resumed).await,
        CellEvent::Synced {
            token,
            snapshot: false
        }
    );
    b.publish(record("m1", 1, 1)).await.expect("m1");
    let token = put(&mut resumed, "m1", 1, 1).await;
    drop(resumed);

    // A change `a` was not watching for: it cannot say what it missed.
    b.publish(record("m1", 1, 2)).await.expect("m1's heartbeat");
    resnapshot(&mut watch(&a, Some(token))).await;
    assert_eq!(snapshot(&mut watch(&a, None)).await.0, [at("m1", 1, 2)]);
}

/// Each publish replaces the file while a thread reads it as fast as it
/// can: every read is a whole record. The record is large, so that a
/// write in place would be caught part-way.
#[tokio::test]
async fn a_reader_never_sees_a_partial_record() {
    const WRITES: u64 = 300;
    let temp = temp();
    let (a, b) = (
        DirDirectory::new(temp.path()),
        DirDirectory::new(temp.path()),
    );
    let hub = format!("{}:1337", "h".repeat(64 * 1024));
    let mut seen = watch(&b, None);
    snapshot(&mut seen).await;

    let file = temp.path().join(MODEL).join("m1.json");
    let stop = Arc::new(AtomicBool::new(false));
    let (reads, partial) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let reader = std::thread::spawn({
        let (stop, reads, partial) = (stop.clone(), reads.clone(), partial.clone());
        move || {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(bytes) = std::fs::read(&file) {
                    reads.fetch_add(1, Ordering::Relaxed);
                    if serde_json::from_slice::<serde_json::Value>(&bytes).is_err() {
                        partial.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    });
    for seq in 1..=WRITES {
        let record = CellRecord {
            hub: hub.clone(),
            ..record("m1", 1, seq)
        };
        a.publish(record).await.expect("a heartbeat");
    }
    stop.store(true, Ordering::Relaxed);
    reader.join().expect("the reader");
    assert!(reads.load(Ordering::Relaxed) > 0, "the reader read nothing");
    assert_eq!(partial.load(Ordering::Relaxed), 0, "partial reads");

    // The watch, polling, sees some of the heartbeats, each whole, in
    // order, up to the last.
    let mut last = 0;
    while last < WRITES {
        match event(&mut seen).await {
            CellEvent::Put { record, .. } => {
                assert_eq!(record.hub, hub);
                assert!(record.seq > last, "{} after {last}", record.seq);
                last = record.seq;
            }
            other => panic!("the heartbeats: {other:?}"),
        }
    }
}

/// Two writes of one length in one tick of the file system's clock leave a
/// file's modification time and length as they were: a scan reads a file
/// changed so recently all the same. The time is set ahead, so that the
/// file is recent however long the first scan takes to read it.
#[tokio::test]
async fn a_rewrite_that_keeps_the_files_stamp_is_seen() {
    let temp = temp();
    let (a, b) = (
        DirDirectory::new(temp.path()),
        DirDirectory::new(temp.path()),
    );
    let file = temp.path().join(MODEL).join("m1.json");
    let ahead = std::time::SystemTime::now() + Duration::from_secs(30);
    let touch = || {
        std::fs::File::options()
            .write(true)
            .open(&file)
            .and_then(|written| written.set_modified(ahead))
            .expect("m1's time set ahead");
        std::fs::metadata(&file).expect("m1's file")
    };
    a.publish(record("m1", 1, 1)).await.expect("m1");
    let stamp = touch();
    let mut seen = watch(&b, None);
    assert_eq!(snapshot(&mut seen).await.0, [at("m1", 1, 1)]);

    a.publish(record("m1", 1, 2)).await.expect("m1's heartbeat");
    let now = touch();
    assert_eq!(
        (now.modified().ok(), now.len()),
        (stamp.modified().ok(), stamp.len()),
        "the stamp is as it was"
    );
    put(&mut seen, "m1", 1, 2).await;
}

/// A token is served across the latest `history` changes and no more,
/// though the class was watched throughout.
#[tokio::test]
async fn a_token_is_served_across_its_history_and_no_further() {
    let temp = temp();
    let limits = DirLimits {
        buffer: 16,
        history: 3,
    };
    let a = DirDirectory::with_limits(temp.path(), limits);
    let mut seen = watch(&a, None);
    snapshot(&mut seen).await;
    let mut tokens = Vec::new();
    for seq in 1..=5 {
        a.publish(record("m1", 1, seq)).await.expect("a heartbeat");
        tokens.push(put(&mut seen, "m1", 1, seq).await);
    }
    // After the first change, four more: one more than is kept.
    resnapshot(&mut watch(&a, Some(tokens[0].clone()))).await;
    // After the second, the three kept.
    let mut resumed = watch(&a, Some(tokens[1].clone()));
    assert!(matches!(
        event(&mut resumed).await,
        CellEvent::Synced {
            snapshot: false,
            ..
        }
    ));
    for seq in 3..=5 {
        put(&mut resumed, "m1", 1, seq).await;
    }
}

/// A watch the directory can no longer read ends with the error, or with
/// `Resnapshot` when its buffer is full, but never ends without a word.
#[tokio::test]
async fn a_full_watch_is_told_when_its_class_cannot_be_read() {
    let temp = temp();
    let limits = DirLimits {
        buffer: 1,
        history: 8,
    };
    let (a, b) = (
        DirDirectory::with_limits(temp.path(), limits),
        DirDirectory::new(temp.path()),
    );
    let (mut unread, mut reading) = (watch(&a, None), watch(&a, None));
    snapshot(&mut unread).await;
    snapshot(&mut reading).await;
    b.publish(record("m1", 1, 1)).await.expect("m1");
    // The change fills the unread watch's buffer.
    put(&mut reading, "m1", 1, 1).await;
    let class = temp.path().join(MODEL);
    std::fs::remove_dir_all(&class).expect("the class directory");
    std::fs::write(&class, "not a directory").expect("a file in its place");
    assert!(
        matches!(
            next(&mut reading).await,
            Some(Err(DirectoryError::Unavailable(_)))
        ),
        "the reading watch is told"
    );
    let mut last = None;
    while let Some(item) = next(&mut unread).await {
        last = Some(item);
    }
    assert!(
        matches!(
            last,
            Some(Err(DirectoryError::Unavailable(_)) | Ok(CellEvent::Resnapshot))
        ),
        "the unread watch ended with {last:?}"
    );
}

#[tokio::test]
async fn a_file_that_does_not_parse_is_skipped() {
    let temp = temp();
    let class = temp.path().join(MODEL);
    std::fs::create_dir_all(&class).expect("the class directory");
    let write = |name: &str, text: &str| std::fs::write(class.join(name), text).expect("a file");
    let json =
        |record: &CellRecord| serde_json::json!({ "order": 1, "record": record }).to_string();
    write("m2.json", "{\"order\": 1, \"rec");
    // Whole, but not the record of the cell or class the file is named for.
    write("m3.json", &json(&record("m4", 1, 1)));
    write(
        "m5.json",
        &json(&CellRecord {
            class: "global".into(),
            ..record("m5", 1, 1)
        }),
    );
    write(
        "m6.json",
        &json(&CellRecord {
            lease: Duration::ZERO,
            ..record("m6", 1, 1)
        }),
    );
    // Not a cell's file at all.
    write(".m7.json.tmp", &json(&record("m7", 1, 1)));
    write("notes.txt", "a note");
    write("M8.json", &json(&record("m8", 1, 1)));

    let (a, b) = (
        DirDirectory::new(temp.path()),
        DirDirectory::new(temp.path()),
    );
    a.publish(record("m1", 1, 1)).await.expect("m1");
    let mut seen = watch(&b, None);
    assert_eq!(snapshot(&mut seen).await.0, [at("m1", 1, 1)]);

    // A file that goes bad while watched is skipped too, and the watch
    // goes on.
    write("m3.json", "garbage");
    a.publish(record("m9", 1, 1)).await.expect("m9");
    put(&mut seen, "m9", 1, 1).await;
    // A cell's own hub replaces its unreadable file.
    a.publish(record("m2", 1, 1))
        .await
        .expect("m2 over its bad file");
    put(&mut seen, "m2", 1, 1).await;
}

#[tokio::test]
async fn a_directory_it_cannot_use_is_an_error() {
    let temp = temp();
    let file = temp.path().join("cells");
    std::fs::write(&file, "not a directory").expect("a file");
    let directory = DirDirectory::new(&file);
    assert!(
        matches!(
            directory.publish(record("m1", 1, 1)).await,
            Err(DirectoryError::Unavailable(_))
        ),
        "a write under a file"
    );
    let mut seen = watch(&directory, None);
    assert!(
        matches!(
            next(&mut seen).await,
            Some(Err(DirectoryError::Unavailable(_)))
        ),
        "a watch under a file"
    );
    assert!(next(&mut seen).await.is_none(), "an error ends the watch");
    assert_eq!(directory.watchers(MODEL), 0);

    // A class that is not a class name names no directory.
    let mut seen = Box::pin(DirDirectory::new(temp.path()).watch("../x", None));
    assert!(
        matches!(
            next(&mut seen).await,
            Some(Err(DirectoryError::Malformed(_)))
        ),
        "a watch of a path"
    );
    assert!(next(&mut seen).await.is_none());
}
