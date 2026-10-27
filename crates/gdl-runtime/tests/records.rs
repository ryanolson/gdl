//! A node's handle on its cell hub's records (`gdl::CellRecords`): taken
//! while the nodes are built, bound when the instance registers with its
//! cell's hub, and shared by every instance of the cell through the hub.
//!
//! The hub and the instances run in this process on OS-assigned ports;
//! every wait is bounded.

mod common;

use common::WAIT;
use common::cells::{Resolving, SEGMENTS, cell_hub_of, global_cell, global_hub, settings, spec};
use gdl::{CellRecords, Expected, Record, RecordsUnavailable, Written};
use gdl_runtime::{Launch, Process};
use tokio::sync::mpsc;
use velo_hub::Hub;

/// An instance of `a` in the global cell at `hub`, launched, with the
/// handle on the table `fingerprints` that its nodes took while they were
/// built, and the answers to the handle's writes.
struct Instance {
    records: CellRecords,
    answers: mpsc::UnboundedReceiver<(Vec<u8>, Option<u64>, Option<Record>)>,
    launch: Launch,
    _process: Process,
}

async fn instance(hub: &Hub) -> Instance {
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &global_cell(),
    );
    assert!(
        config.runtime.records.is_under_hub(),
        "a `[hub]` and a `[cell]`"
    );
    let records = config
        .runtime
        .records
        .scope(async { CellRecords::current("fingerprints") })
        .await
        .expect("a table under a cell's hub");
    let (sent, answers) = mpsc::unbounded_channel();
    records.on_answer(move |key, written| {
        let answer = match written {
            Written::Applied { version } => (Some(version), None),
            Written::Refused { current } => (None, current.cloned()),
            Written::Unanswered => (None, None),
        };
        let _ = sent.send((key.to_vec(), answer.0, answer.1));
    });
    let process = Process::start(&config).await.expect("the process starts");
    let launch = tokio::time::timeout(
        WAIT,
        config.launch(&process, "a", SEGMENTS, &spec(), async { Ok(()) }),
    )
    .await
    .expect("the launch in time")
    .expect("the instance launches");
    assert!(config.runtime.records.is_bound(), "bound at the launch");
    Instance {
        records,
        answers,
        launch,
        _process: process,
    }
}

async fn answer(instance: &mut Instance) -> (Vec<u8>, Option<u64>, Option<Record>) {
    tokio::time::timeout(WAIT, instance.answers.recv())
        .await
        .expect("an answer in time")
        .expect("the answers go on")
}

/// Two instances of one segment under one cell's hub share its records: a
/// write from one is read by the other, and a write from the other that
/// expects no record is refused with the first one's record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_from_one_instance_is_read_by_another() {
    let directory = Resolving::new();
    let (hub, _) = cell_hub_of(&spec(), global_hub(), &directory).await;
    let (mut one, mut two) = tokio::join!(instance(&hub), instance(&hub));
    assert_ne!(
        one.launch.node().instance_id(),
        two.launch.node().instance_id(),
        "two instances, each with its own link"
    );
    assert!(
        one.records
            .set(b"session-7", b"model-x-02", Expected::Absent)
    );
    let (key, applied, _) = answer(&mut one).await;
    assert_eq!(key, b"session-7");
    let version = applied.expect("applied");
    let written = Record {
        value: b"model-x-02".to_vec(),
        version,
    };
    let read = tokio::time::timeout(WAIT, two.records.get(b"session-7"))
        .await
        .expect("an answer in time")
        .expect("the hub answers");
    assert_eq!(read, Some(written.clone()));
    assert!(two.records.eviction_age().is_some());
    assert!(
        two.records
            .set(b"session-7", b"model-x-01", Expected::Absent)
    );
    assert_eq!(
        answer(&mut two).await,
        (b"session-7".to_vec(), None, Some(written))
    );
}

/// Without a cell's hub there is no table: an instance with a hub but no
/// `[cell]`, whose hub has no record service, and outside a build.
#[tokio::test]
async fn without_a_cell_hub_the_handle_is_not_bound() {
    let config = settings(1, 2, 5_000, "");
    assert!(!config.runtime.records.is_under_hub());
    let taken = config
        .runtime
        .records
        .scope(async { CellRecords::current("fingerprints") })
        .await;
    assert_eq!(taken.expect_err("no table"), RecordsUnavailable::Unbound);
    assert_eq!(
        CellRecords::current("fingerprints").expect_err("no table"),
        RecordsUnavailable::Unbound
    );
}

/// A timed-out request can still arrive at the hub. It must not replace
/// a later write to the same key: the later write goes out only once the
/// earlier request has ended. End to end against the real hub; the
/// ordering itself is proved without a race in `gdl`'s
/// `a_timed_out_batch_holds_back_the_next`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_batch_cannot_overwrite_a_later_batch() {
    let directory = Resolving::new();
    let (hub, _) = cell_hub_of(&spec(), global_hub(), &directory).await;
    let control = common::proxy::Proxy::new(hub.control_addr()).await;
    let config = settings(
        hub.public_addr().port(),
        control.port,
        5_000,
        &global_cell(),
    );
    let records = config
        .runtime
        .records
        .table("fingerprints")
        .expect("a table");
    let (sent, mut answers) = mpsc::unbounded_channel();
    records.on_answer(move |_, written| {
        let applied = match written {
            Written::Applied { version } => Some(version),
            Written::Unanswered => None,
            other => panic!("unexpected write answer: {other:?}"),
        };
        let _ = sent.send(applied);
    });
    let process = Process::start(&config).await.expect("the process starts");
    let _launch = tokio::time::timeout(
        WAIT,
        config.launch(&process, "a", SEGMENTS, &spec(), async { Ok(()) }),
    )
    .await
    .expect("the launch in time")
    .expect("the instance launches");

    // The value "first" is base64 on the wire. Hold only its request,
    // leaving registration, heartbeats, and the second write running.
    control.hold(b"\"value\":\"Zmlyc3Q=\"");
    assert!(records.set(b"session-7", b"first", Expected::Replace));
    tokio::time::timeout(WAIT, control.holding())
        .await
        .expect("the first request is held");
    assert_eq!(
        tokio::time::timeout(WAIT, answers.recv())
            .await
            .expect("the first write times out")
            .expect("the callback runs"),
        None
    );
    assert!(records.set(b"session-7", b"second", Expected::Replace));
    control.release();
    tokio::time::timeout(WAIT, control.answered_after_hold())
        .await
        .expect("the hub processes the delayed request");
    let version = tokio::time::timeout(WAIT, answers.recv())
        .await
        .expect("the second write is answered")
        .expect("the callback runs")
        .expect("the second write applies");
    let found = tokio::time::timeout(WAIT, records.get(b"session-7"))
        .await
        .expect("the read in time")
        .expect("the hub answers");
    assert_eq!(
        found,
        Some(Record {
            value: b"second".to_vec(),
            version,
        }),
        "an earlier timed-out batch must not overwrite the acknowledged later write"
    );
}
