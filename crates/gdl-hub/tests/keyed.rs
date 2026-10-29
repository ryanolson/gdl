//! A pool hub's keyed record service: the hub's own instances read and
//! write its table by request and answer, over the control listener, with
//! their own credentials. Every hub runs in process on OS-assigned ports;
//! every wait is for something observed.

mod common;

use std::time::Duration;

use common::{Instance, WAIT, config, spec};
use gdl_hub::{GraphAdmission, KeyedRecords, hub_builder};
use gdl_model::{DirectoryBackend, PoolSettings};
use gdl_watch::{Expected, KeyedAnswer, KeyedOp, KeyedRecord, KeyedReply};
use velo_hub::Hub;

/// A hub of the deployment with the keyed record service beside
/// admission, of `capacity` records.
async fn keyed_hub(capacity: usize) -> Hub {
    Hub::builder(config())
        .extension(GraphAdmission::new(spec()))
        .extension(KeyedRecords::new(capacity).expect("a capacity"))
        .serve()
        .await
        .expect("the hub serves")
}

fn get(key: &[u8]) -> KeyedOp {
    KeyedOp::Get { key: key.to_vec() }
}

fn set(key: &[u8], value: &[u8], expected: Expected) -> KeyedOp {
    KeyedOp::Set {
        key: key.to_vec(),
        value: value.to_vec(),
        expected,
    }
}

fn check(key: &[u8], version: u64) -> KeyedOp {
    KeyedOp::Check {
        key: key.to_vec(),
        version,
    }
}

fn record(value: &[u8], version: u64) -> KeyedRecord {
    KeyedRecord {
        value: value.to_vec(),
        version,
    }
}

/// `ops` asked as `instance`, within the wait.
async fn ask(instance: &Instance, ops: Vec<KeyedOp>) -> KeyedReply {
    tokio::time::timeout(WAIT, instance.keyed(ops))
        .await
        .expect("the hub answers in time")
        .expect("the hub takes the ask")
}

/// The answer to the one operation `op`.
async fn one(instance: &Instance, op: KeyedOp) -> KeyedAnswer {
    let mut reply = ask(instance, vec![op]).await;
    assert_eq!(reply.answers.len(), 1, "{reply:?}");
    reply.answers.remove(0)
}

fn applied(answer: KeyedAnswer) -> u64 {
    match answer {
        KeyedAnswer::Applied { version } => version,
        other => panic!("not applied: {other:?}"),
    }
}

/// One instance's write is what another instance reads; a key nobody
/// wrote is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_get_after_a_set_from_another_instance_returns_the_record() {
    let hub = keyed_hub(16).await;
    let (writer, reader) = (
        Instance::join(&hub, "a").await,
        Instance::join(&hub, "f").await,
    );
    let version = applied(one(&writer, set(b"session", b"model-x-02", Expected::Absent)).await);
    assert_eq!(
        one(&reader, get(b"session")).await,
        KeyedAnswer::Found {
            record: record(b"model-x-02", version),
        }
    );
    assert_eq!(one(&reader, get(b"other")).await, KeyedAnswer::Missing);
    hub.shutdown().await.expect("the hub stops");
}

/// Two instances that both write a key expecting no record: the first
/// write stays, and the second is refused with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn of_two_absent_sets_the_first_stays_and_the_second_gets_it_back() {
    let hub = keyed_hub(16).await;
    let (first, second) = (
        Instance::join(&hub, "a").await,
        Instance::join(&hub, "a").await,
    );
    let version = applied(one(&first, set(b"session", b"model-x-01", Expected::Absent)).await);
    assert_eq!(
        one(&second, set(b"session", b"model-x-02", Expected::Absent)).await,
        KeyedAnswer::Refused {
            current: Some(record(b"model-x-01", version)),
        }
    );
    assert_eq!(
        one(&second, get(b"session")).await,
        KeyedAnswer::Found {
            record: record(b"model-x-01", version),
        }
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A write at a version that is no longer the record's is refused with
/// the record as it is; a replace always applies; and every write that
/// applies has a newer version than the one before it, whoever wrote it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_version_is_refused_and_a_replace_always_applies() {
    let hub = keyed_hub(16).await;
    let (a, b) = (
        Instance::join(&hub, "a").await,
        Instance::join(&hub, "a").await,
    );
    let one_ = applied(one(&a, set(b"s", b"model-x-01", Expected::Absent)).await);
    let two = applied(one(&b, set(b"s", b"model-x-02", Expected::Version(one_))).await);
    assert!(two > one_, "{one_} then {two}");
    // a still holds the first version.
    assert_eq!(
        one(&a, set(b"s", b"model-x-03", Expected::Version(one_))).await,
        KeyedAnswer::Refused {
            current: Some(record(b"model-x-02", two)),
        }
    );
    let three = applied(one(&a, set(b"s", b"model-x-03", Expected::Replace)).await);
    let four = applied(one(&b, set(b"t", b"model-x-01", Expected::Replace)).await);
    let five = applied(one(&a, set(b"s", b"model-x-01", Expected::Replace)).await);
    assert!(
        two < three && three < four && four < five,
        "{two} {three} {four} {five}"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A check answers unchanged while the record is at the version asked
/// about, the newer record once another instance wrote it, and gone once
/// the table evicted it. The hub that `hub_builder` builds for a pool
/// serves the table at the capacity its settings give: one record here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_check_answers_unchanged_then_the_newer_record_then_gone() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-keyed-")
        .tempdir()
        .expect("a directory");
    let mut pool: PoolSettings = common::settings();
    pool.directory = DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    };
    pool.timing.keyed_capacity = 1;
    let hub = hub_builder(config(), spec(), Some(pool))
        .expect("a pool's hub")
        .serve()
        .await
        .expect("the hub serves");
    let (holder, writer) = (
        Instance::join(&hub, "a").await,
        Instance::join(&hub, "f").await,
    );
    let held = applied(one(&holder, set(b"s", b"model-x-01", Expected::Absent)).await);
    assert_eq!(
        one(&holder, check(b"s", held)).await,
        KeyedAnswer::Unchanged
    );
    let moved = applied(one(&writer, set(b"s", b"model-x-02", Expected::Version(held))).await);
    assert_eq!(
        one(&holder, check(b"s", held)).await,
        KeyedAnswer::Newer {
            record: record(b"model-x-02", moved),
        }
    );
    // One record's capacity: another key evicts it.
    applied(one(&writer, set(b"t", b"model-x-01", Expected::Absent)).await);
    assert_eq!(one(&holder, check(b"s", moved)).await, KeyedAnswer::Gone);
    hub.shutdown().await.expect("the hub stops");
}

/// At its capacity the table evicts its least recently used record. The
/// eviction age in each reply is the age of that record: none while the
/// table is empty, rising while nobody touches it, and falling once it is
/// touched or evicted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_least_recently_used_record_is_evicted_and_the_age_follows_it() {
    // The age a record must reach before the test touches it: longer than
    // a busy host takes for two requests, so that the age after the touch
    // is surely the younger record's.
    const AGED: Duration = Duration::from_millis(250);
    let hub = keyed_hub(2).await;
    let instance = Instance::join(&hub, "a").await;
    let age = |reply: &KeyedReply| reply.eviction_age_ms.map(Duration::from_millis);
    // An ask of no operations reads the age and touches nothing.
    assert_eq!(age(&ask(&instance, vec![]).await), None);
    applied(one(&instance, set(b"a", b"1", Expected::Absent)).await);
    let risen = tokio::time::timeout(WAIT, async {
        loop {
            let seen = age(&ask(&instance, vec![]).await).expect("a record");
            if seen >= AGED {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the age rises in time");
    // b is written now, then a is touched: b is the oldest, and young.
    applied(one(&instance, set(b"b", b"2", Expected::Absent)).await);
    let touched = ask(&instance, vec![get(b"a")]).await;
    let fallen = age(&touched).expect("records");
    assert!(fallen < risen, "the age went from {risen:?} to {fallen:?}");
    // c evicts b, the least recently used; a and c stay.
    applied(one(&instance, set(b"c", b"3", Expected::Absent)).await);
    let reply = ask(&instance, vec![get(b"b"), get(b"a"), get(b"c")]).await;
    assert!(
        matches!(
            reply.answers.as_slice(),
            [
                KeyedAnswer::Missing,
                KeyedAnswer::Found { .. },
                KeyedAnswer::Found { .. }
            ]
        ),
        "{reply:?}"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// One request carries many writes, and each gets its own answer, in
/// order: those that apply each at a version newer than the one before,
/// and one that is refused with the record an earlier write in the same
/// request made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_of_sets_gets_one_answer_for_each() {
    let hub = keyed_hub(1024).await;
    let instance = Instance::join(&hub, "a").await;
    let mut ops: Vec<KeyedOp> = (0u32..100)
        .map(|n| set(&n.to_be_bytes(), &n.to_le_bytes(), Expected::Absent))
        .collect();
    ops.push(set(&7u32.to_be_bytes(), b"again", Expected::Absent));
    ops.push(get(&42u32.to_be_bytes()));
    let reply = ask(&instance, ops).await;
    assert_eq!(reply.answers.len(), 102, "{reply:?}");
    let versions: Vec<u64> = reply.answers[..100]
        .iter()
        .map(|answer| applied(answer.clone()))
        .collect();
    assert!(versions.windows(2).all(|w| w[0] < w[1]), "{versions:?}");
    assert_eq!(
        reply.answers[100],
        KeyedAnswer::Refused {
            current: Some(record(&7u32.to_le_bytes(), versions[7])),
        }
    );
    assert_eq!(
        reply.answers[101],
        KeyedAnswer::Found {
            record: record(&42u32.to_le_bytes(), versions[42]),
        }
    );
    hub.shutdown().await.expect("the hub stops");
}

/// Only an instance registered with the hub reads its table: a request
/// without the credential of the instance its path names is refused, and
/// a hub without a pool serves no table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_hubs_own_instances_reach_its_table() {
    let hub = keyed_hub(16).await;
    let instance = Instance::join(&hub, "a").await;
    let body = serde_json::json!({ "ops": [{ "op": "get", "key": "AA==" }] });
    let url = |hub: &Hub, instance| {
        format!(
            "http://127.0.0.1:{}/v1/ext/{}{}",
            hub.control_addr().port(),
            gdl_watch::KEYED_EXTENSION,
            gdl_watch::keyed_path(instance)
        )
    };
    let http = reqwest::Client::new();
    let status = |url: String| {
        let request = http.post(url).json(&body).send();
        async move {
            tokio::time::timeout(WAIT, request)
                .await
                .expect("the hub answers in time")
                .expect("the hub answers")
                .status()
        }
    };
    let refused = status(url(&hub, instance.id())).await;
    assert!(refused.is_client_error(), "{refused}");
    // The credential is good: the same ask with it is answered.
    assert_eq!(
        ask(&instance, vec![get(b"\0")]).await.answers,
        [KeyedAnswer::Missing]
    );
    let plain = hub_builder(config(), spec(), None)
        .expect("no pool")
        .serve()
        .await
        .expect("the hub serves");
    assert_eq!(status(url(&plain, instance.id())).await.as_u16(), 404);
    plain.shutdown().await.expect("the hub stops");
    hub.shutdown().await.expect("the hub stops");
}
