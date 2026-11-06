//! The etcd pool directory against a real etcd, outside the default suite:
//! `scripts/etcd.sh` fetches etcd and runs this target with `GDL_ETCD`
//! naming the binary. Each case starts its own etcd on OS-assigned ports,
//! behind a proxy it can cut or point at another store.
//!
//! The conformance suite runs first; its `restart` restarts the etcd
//! process over the same data directory, so rule 1 is held across the
//! store's own restart. The cases after it are the backend's own: the
//! layout, the writes' compares, the store's identity, and the watch task's
//! snapshots and reconnects.

mod assembly;
mod deadline;
mod endpoint;
mod joining;
mod lifecycle;
mod member;
mod missing;
mod refusals;
mod replay;
mod shutdown;
mod store;

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use etcd_client::GetOptions;
use futures::StreamExt;
use gdl_pools::{
    DirectoryError, LastWrite, PoolDirectory, PoolEvent, PoolProperty, PoolRecord, PoolState,
    Presence, PropertyState, ResumeToken, Version, Withdrawal,
    conformance::{self, Backend},
};
use gdl_pools_etcd::{
    EtcdDirectory, EtcdLimits, STORE_ID,
    testing::{Hook, Point},
};
use store::Store;
use tokio::sync::Mutex as AsyncMutex;

const WAIT: Duration = Duration::from_secs(10);
const MODEL: &str = "model_x";

const LIMITS: EtcdLimits = EtcdLimits {
    buffer: 16,
    history: 64,
    request_bytes: 1_572_864,
    response_bytes: 32 << 20,
    timeout: Duration::from_secs(5),
};

async fn open(url: &str, prefix: &str, limits: EtcdLimits) -> EtcdDirectory {
    EtcdDirectory::with_limits(vec![url.to_owned()], prefix.to_owned(), limits)
        .await
        .expect("a directory")
}

/// The suite's store: one etcd for every case, each case under a prefix of
/// its own, so that each starts from an empty directory.
struct Local {
    store: AsyncMutex<Store>,
    url: String,
    cases: AtomicU64,
}

impl Backend for Local {
    type Directory = EtcdDirectory;

    // Clones of one handle, as the hubs of one process would hold it:
    // resume tokens are served across them.
    async fn fresh<const N: usize>(&self) -> [EtcdDirectory; N] {
        let case = self.cases.fetch_add(1, Ordering::Relaxed);
        let directory = open(&self.url, &format!("/gdl-test/{case}/pools"), LIMITS).await;
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        LIMITS.history
    }

    fn watchers(&self, directory: &EtcdDirectory, class: &str) -> usize {
        directory.watchers(class)
    }

    // The etcd process restarts over the same data directory, so the case
    // holds rule 1 across the store's own restart, not only a hub's.
    async fn restart(&self, directory: EtcdDirectory) -> EtcdDirectory {
        let prefix = directory.prefix().to_owned();
        drop(directory);
        self.store.lock().await.restart().await;
        open(&self.url, &prefix, LIMITS).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_etcd_directory_conforms() {
    let store = Store::start(&[]).await;
    let url = store.proxy.url();
    conformance::run(&Local {
        store: AsyncMutex::new(store),
        url,
        cases: AtomicU64::new(0),
    })
    .await;
}

type Watch = Pin<Box<<EtcdDirectory as PoolDirectory>::Watch>>;

fn key(name: &str) -> gdl_model::PropertyKey {
    gdl_model::PropertyKey::new("test", serde_json::json!({ "name": name })).expect("a key")
}

/// A record of `pool` whose one property names its version, padded to
/// `pad` bytes, so that a record joined to another version's list fails an
/// equality.
fn record(pool: &str, epoch: u64, seq: u64, pad: usize) -> PoolRecord {
    PoolRecord {
        dropped: Vec::new(),
        presence: Presence::Serving,
        properties: vec![PoolProperty {
            property: key(&format!("{pool}-{epoch}-{seq}-{}", "x".repeat(pad))),
            state: PropertyState::Ready,
            since: seq,
        }],
        properties_version: (epoch << 32) + seq,
        revoked: vec![key(&format!("revoked-{seq}"))],
        name: pool.into(),
        class: MODEL.into(),
        region: "us-east-1".into(),
        parent: Some(gdl_model::Parent::Pool("global-us".into())),
        contract: 1,
        placement: 2,
        state: PoolState::Ready,
        epoch,
        seq,
        hub: format!("{pool}:4000"),
        lease: PoolRecord::DEFAULT_LEASE,
        load: None,
    }
}

/// `record` at a later seq with the same list: a heartbeat.
fn heartbeat(of: &PoolRecord, seq: u64) -> PoolRecord {
    PoolRecord { seq, ..of.clone() }
}

fn withdrawal(pool: &str, epoch: u64, seq: u64) -> Withdrawal {
    Withdrawal {
        class: MODEL.into(),
        pool: pool.into(),
        epoch,
        seq,
        properties_version: (epoch << 32) + seq,
    }
}

fn watch(directory: &EtcdDirectory, class: &str, from: Option<ResumeToken>) -> Watch {
    Box::pin(directory.watch(class, from))
}

async fn next(watch: &mut Watch, what: &str) -> PoolEvent {
    match tokio::time::timeout(WAIT, watch.next()).await {
        Ok(Some(Ok(event))) => event,
        Ok(Some(Err(error))) => panic!("{what}: the watch failed: {error}"),
        Ok(None) => panic!("{what}: the watch ended"),
        Err(_) => panic!("{what}: nothing within {WAIT:?}"),
    }
}

/// A snapshot's records, sorted by name, and its token.
async fn snapshot(watch: &mut Watch) -> (Vec<PoolRecord>, ResumeToken) {
    let mut records = Vec::new();
    loop {
        match next(watch, "the snapshot").await {
            PoolEvent::Current(record) => records.push(record),
            PoolEvent::Synced {
                token,
                snapshot: true,
            } => {
                records.sort_by(|a, b| a.name.cmp(&b.name));
                return (records, token);
            }
            other => panic!("the snapshot: {other:?}"),
        }
    }
}

/// The watch says `Resnapshot`, after any changes before it, and ends.
async fn resnapshots(watch: &mut Watch, what: &str) {
    loop {
        match next(watch, what).await {
            PoolEvent::Resnapshot => break,
            PoolEvent::Put { .. } | PoolEvent::Withdrawn { .. } => {}
            other => panic!("{what}: {other:?}"),
        }
    }
    let end = tokio::time::timeout(WAIT, watch.next())
        .await
        .unwrap_or_else(|_| panic!("{what}: the watch did not end"));
    assert!(end.is_none(), "{what}: went on with {end:?}");
}

async fn put_of(watch: &mut Watch, what: &str) -> PoolRecord {
    match next(watch, what).await {
        PoolEvent::Put { record, .. } => record,
        other => panic!("{what}: {other:?}"),
    }
}

/// The revision a store is at.
async fn revision(client: &mut etcd_client::Client) -> i64 {
    client
        .get(STORE_ID, None)
        .await
        .expect("a read")
        .header()
        .expect("a header")
        .revision()
}

/// Write keys outside every directory until the store is at `target`.
async fn fill_to(client: &mut etcd_client::Client, target: i64) {
    while revision(client).await < target {
        client.put("/filler", "x", None).await.expect("a filler");
    }
}

/// Every key under `prefix`, with its mod revision and lease.
async fn keys(client: &mut etcd_client::Client, prefix: &str) -> Vec<(String, i64, i64)> {
    client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await
        .expect("a read")
        .kvs()
        .iter()
        .map(|kv| {
            (
                String::from_utf8_lossy(kv.key()).into_owned(),
                kv.mod_revision(),
                kv.lease(),
            )
        })
        .collect()
}

/// Read through `directory` until its connection reaches the store the
/// proxy points at: the first call after a switch can meet the closed
/// connection to the old one. A read touches nothing a write compares.
async fn reach(directory: &EtcdDirectory) {
    for _ in 0..3 {
        match directory.last_write(MODEL, "unwritten").await {
            Ok(_) => return,
            Err(DirectoryError::Unavailable(why)) if why.contains("transport") => {}
            Err(other) => panic!("a read: {other}"),
        }
    }
    panic!("the directory never reached the store");
}

/// A hook that runs `act` the first time the task reaches `at`, and the
/// count of its runs.
fn once<F, Fut>(at: Point, act: F) -> (Hook, Arc<AtomicUsize>)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let fired = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&fired);
    let act = Arc::new(act);
    let hook: Hook = Arc::new(move |point| {
        let act = Arc::clone(&act);
        let first = point == at
            && counted
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
        Box::pin(async move {
            if first {
                act().await;
            }
        })
    });
    (hook, fired)
}

/// No key the directory writes has a lease, so nothing it holds expires
/// by time: the suite's case waits only 200 ms, which a lease of seconds
/// outlives.
#[tokio::test]
async fn no_key_has_a_lease() {
    let store = Store::start(&[]).await;
    let directory = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    let m1 = record("m1", 1, 1, 0);
    directory.publish(m1.clone()).await.expect("m1");
    directory
        .publish(heartbeat(&m1, 2))
        .await
        .expect("a heartbeat");
    directory
        .withdraw(withdrawal("m2", 1, 1))
        .await
        .expect("a withdrawal");
    let mut raw = store.raw().await;
    let written = keys(&mut raw, "/gdl/").await;
    let names: Vec<&str> = written.iter().map(|(key, ..)| key.as_str()).collect();
    assert_eq!(
        names,
        [
            "/gdl/pools/model_x/m1",
            "/gdl/pools/model_x/m1.list",
            "/gdl/pools/model_x/m2",
            STORE_ID
        ],
        "the layout"
    );
    for (key, _, lease) in &written {
        assert_eq!(*lease, 0, "{key} has a lease");
    }
}

/// A heartbeat, whose list has not changed, writes the record key alone;
/// a write whose list changed writes both keys in one revision.
#[tokio::test]
async fn a_heartbeat_writes_the_record_key_only() {
    let store = Store::start(&[]).await;
    let directory = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    let mut raw = store.raw().await;
    let m1 = record("m1", 1, 1, 0);
    directory.publish(m1.clone()).await.expect("m1");
    let first = keys(&mut raw, "/gdl/pools/").await;
    let [(_, record_at, _), (_, list_at, _)] = first[..] else {
        panic!("{first:?}");
    };
    assert_eq!(record_at, list_at, "one transaction wrote both keys");
    for seq in 2..=4 {
        directory
            .publish(heartbeat(&m1, seq))
            .await
            .expect("a heartbeat");
        let now = keys(&mut raw, "/gdl/pools/").await;
        assert_eq!(now[1].1, list_at, "a heartbeat rewrote the list");
        assert!(now[0].1 > record_at, "a heartbeat did not write the record");
    }
    // A handle that never wrote the pool, such as a restarted hub, reads
    // the list before its first write, and leaves it too when it is equal.
    let restarted = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    restarted
        .publish(PoolRecord {
            epoch: 2,
            seq: 0,
            ..m1.clone()
        })
        .await
        .expect("a new epoch");
    let now = keys(&mut raw, "/gdl/pools/").await;
    assert_eq!(now[1].1, list_at, "an equal list was written again");
    // A changed list is written with its record.
    restarted
        .publish(record("m1", 2, 1, 0))
        .await
        .expect("a new list");
    let now = keys(&mut raw, "/gdl/pools/").await;
    assert_eq!(now[0].1, now[1].1, "the new list and its record");
    let read = directory.last_write(MODEL, "m1").await.expect("a read");
    assert_eq!(
        read,
        Some(LastWrite::Record(Box::new(record("m1", 2, 1, 0))))
    );
}

/// A record arrives with its own list, never another version's: each
/// version's list differs, in a live watch, a snapshot, a resumed watch,
/// and the changes a reconnect catches up on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_never_arrives_with_another_versions_list() {
    let store = Store::start(&[]).await;
    let writer = open(&store.etcd().client_url(), "/gdl/pools", LIMITS).await;
    // The reader takes no response larger than one request, so the changes
    // it catches up on below arrive only in etcd's fragments.
    let reader = open(
        &store.proxy.url(),
        "/gdl/pools",
        EtcdLimits {
            response_bytes: LIMITS.request_bytes,
            ..LIMITS
        },
    )
    .await;
    let mut live = watch(&reader, MODEL, None);
    let (_, start) = snapshot(&mut live).await;
    let mut published = Vec::new();
    for seq in 1..=12 {
        // A new list on odd seqs, a heartbeat of the last one on even.
        let record = if seq % 2 == 1 {
            record("m1", 1, seq, 0)
        } else {
            heartbeat(published.last().expect("a list"), seq)
        };
        writer.publish(record.clone()).await.expect("a write");
        published.push(record);
    }
    for expected in &published {
        assert_eq!(&put_of(&mut live, "a live change").await, expected);
    }
    let mut resumed = watch(&reader, MODEL, Some(start));
    assert!(matches!(
        next(&mut resumed, "a resume").await,
        PoolEvent::Synced {
            snapshot: false,
            ..
        }
    ));
    for expected in &published {
        assert_eq!(&put_of(&mut resumed, "a replayed change").await, expected);
    }
    drop(resumed);
    let (held, _) = snapshot(&mut watch(&reader, MODEL, None)).await;
    assert_eq!(held, [published.last().expect("a write").clone()]);

    // While the reader is cut off, more versions are written, some with
    // lists near the size bound, so that the changes it catches up on are
    // more than one response holds.
    store.proxy.cut();
    let mut caught = Vec::new();
    for seq in 13..=22 {
        let record = if seq % 2 == 1 {
            record("m1", 1, seq, 1_200_000)
        } else {
            heartbeat(caught.last().expect("a list"), seq)
        };
        writer.publish(record.clone()).await.expect("a write");
        caught.push(record);
    }
    store.proxy.switch(store.etcd().client);
    for expected in &caught {
        let got = put_of(&mut live, "a change caught up on").await;
        assert!(
            &got == expected,
            "caught up on {} at seq {}, joined to the wrong list",
            got.name,
            got.seq
        );
    }
}

/// A class's records live under `<prefix>/<class>/`, so a class whose name
/// another class's name begins with reads none of that class's records.
#[tokio::test]
async fn a_class_reads_none_of_a_class_its_name_prefixes() {
    let store = Store::start(&[]).await;
    let directory = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    let short = PoolRecord {
        class: "m".into(),
        ..record("a1", 1, 1, 0)
    };
    let long = PoolRecord {
        class: "m_x".into(),
        ..record("b1", 1, 1, 0)
    };
    directory.publish(short.clone()).await.expect("m");
    directory.publish(long.clone()).await.expect("m_x");
    let (held, _) = snapshot(&mut watch(&directory, "m", None)).await;
    assert_eq!(held, [short]);
    let (held, _) = snapshot(&mut watch(&directory, "m_x", None)).await;
    assert_eq!(held, [long]);
}

/// A write that cannot fit one etcd request is refused as malformed
/// before it is sent, and stores nothing; the largest write the backend
/// takes lands, so the bound is never above etcd's own.
#[tokio::test]
async fn a_write_too_large_for_one_request_is_malformed() {
    let store = Store::start(&[]).await;
    let directory = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    let mut seq = 0;
    let mut try_pad = |pad: usize| {
        seq += 1;
        let record = record("m1", 1, seq, pad);
        let directory = directory.clone();
        async move { directory.publish(record).await }
    };
    // The largest padding that lands, by bisection between one that lands
    // and one that is refused.
    let (mut fits, mut over) = (1_000_000, 1_600_000);
    try_pad(fits).await.expect("a record well under the bound");
    assert!(matches!(
        try_pad(over).await,
        Err(DirectoryError::Malformed(_))
    ));
    while over - fits > 1 {
        let mid = (fits + over) / 2;
        match try_pad(mid).await {
            Ok(()) => fits = mid,
            Err(DirectoryError::Malformed(_)) => over = mid,
            Err(other) => panic!("a write of {mid} bytes: {other}"),
        }
    }
    let held = directory.last_write(MODEL, "m1").await.expect("a read");
    let held = held.expect("a record").version();
    // One byte more is refused, and stores nothing.
    let result = try_pad(over).await;
    assert!(
        matches!(&result, Err(DirectoryError::Malformed(why)) if why.contains("request")),
        "{result:?}"
    );
    let after = directory.last_write(MODEL, "m1").await.expect("a read");
    assert_eq!(
        after.expect("a record").version(),
        held,
        "a refused write landed"
    );
    // A withdrawal is small, and lands.
    directory
        .withdraw(withdrawal("m1", 2, 0))
        .await
        .expect("a withdrawal");
}

/// Two writes that read the same last write never both land; nor do two
/// publishes of one version; two increasing versions both land, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_writes_never_both_land_on_one_read() {
    let store = Store::start(&[]).await;
    let url = store.proxy.url();
    let a = open(&url, "/gdl/pools", LIMITS).await;
    let b = open(&url, "/gdl/pools", LIMITS).await;
    for round in 1..=20u64 {
        let pool = format!("r{round}");
        a.publish(record(&pool, 1, 1, 0))
            .await
            .expect("the first write");
        let read = Some(Version { epoch: 1, seq: 1 });
        // Each hub read (1, 1), and numbered past it in its own epoch.
        let (x, y) = tokio::join!(
            a.publish_after(record(&pool, 2, 1, 0), read),
            b.publish_after(record(&pool, 3, 1, 0), read),
        );
        assert!(
            x.is_ok() != y.is_ok(),
            "round {round}: two writes after one read: {x:?}, {y:?}"
        );
        for refused in [x, y].into_iter().filter_map(Result::err) {
            assert!(
                matches!(refused, DirectoryError::NotNewer { .. }),
                "{refused}"
            );
        }
        // Two publishes of one version, with different lists.
        let (x, y) = tokio::join!(
            a.publish(record(&pool, 9, 1, 1)),
            b.publish(record(&pool, 9, 1, 2)),
        );
        assert!(
            x.is_ok() != y.is_ok(),
            "round {round}: one version twice: {x:?}, {y:?}"
        );
        let held = a
            .last_write(MODEL, &pool)
            .await
            .expect("a read")
            .expect("a write");
        let won = if x.is_ok() { 1 } else { 2 };
        assert_eq!(held, LastWrite::Record(Box::new(record(&pool, 9, 1, won))));
    }
    // Two increasing versions, from two handles, both land in order.
    let mut seen = watch(&b, MODEL, None);
    snapshot(&mut seen).await;
    a.publish(record("up", 1, 1, 0)).await.expect("v1");
    b.publish(record("up", 1, 2, 0)).await.expect("v2");
    assert_eq!(put_of(&mut seen, "v1").await, record("up", 1, 1, 0));
    assert_eq!(put_of(&mut seen, "v2").await, record("up", 1, 2, 0));
}

/// A class's snapshot larger than one response arrives whole, in pages:
/// with lists near the size bound, and with lists past what the reader's
/// pages are sized for, whose pages it halves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_larger_than_one_response_arrives_whole() {
    // etcd takes requests up to 3 MiB here, so that a writer with a larger
    // bound than the reader's writes lists the reader's pages are not
    // sized for.
    let store = Store::start(&["--max-request-bytes=3145728"]).await;
    let url = store.proxy.url();
    let small = EtcdLimits {
        response_bytes: 4 << 20,
        ..LIMITS
    };
    let writer = open(&url, "/gdl/pools", small).await;
    let mut published = Vec::new();
    for n in 0..8 {
        let record = record(&format!("near-{n}"), 1, 1, 1_400_000 + n);
        writer.publish(record.clone()).await.expect("a large list");
        published.push(record);
    }
    let reader = open(&url, "/gdl/pools", small).await;
    let (held, _) = snapshot(&mut watch(&reader, MODEL, None)).await;
    assert_eq!(held.len(), published.len());
    assert!(held == published, "a snapshot in pages held another record");

    let big = EtcdLimits {
        request_bytes: 3 << 20,
        ..small
    };
    let wide = open(&url, "/gdl/pools", big).await;
    for n in 0..3 {
        let record = record(&format!("past-{n}"), 1, 1, 2_400_000 + n);
        wide.publish(record.clone())
            .await
            .expect("a list past the reader's bound");
        published.push(record);
    }
    published.sort_by(|a, b| a.name.cmp(&b.name));
    let reader = open(&url, "/gdl/pools", small).await;
    let (held, _) = snapshot(&mut watch(&reader, MODEL, None)).await;
    assert_eq!(held.len(), published.len());
    assert!(
        held == published,
        "a snapshot with halved pages held another record"
    );
}

/// A compaction that passes a snapshot's revision while its pages are read
/// restarts the snapshot, and readers see only the complete one: never the
/// pool withdrawn between the two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_during_a_snapshot_gives_one_complete_snapshot() {
    let store = Store::start(&[]).await;
    let url = store.proxy.url();
    // Pages of two keys: a record and its list.
    let paged = EtcdLimits {
        request_bytes: 16 << 10,
        response_bytes: 40 << 10,
        ..LIMITS
    };
    let writer = open(&url, "/gdl/pools", paged).await;
    for pool in ["a1", "b1", "c1", "d1"] {
        writer
            .publish(record(pool, 1, 1, 0))
            .await
            .expect("a record");
    }
    let reader = open(&url, "/gdl/pools", paged).await;
    let between = writer.clone();
    let raw = Arc::new(AsyncMutex::new(store.raw().await));
    let (hook, fired) = once(Point::BetweenPages, move || {
        let between = between.clone();
        let raw = Arc::clone(&raw);
        async move {
            // `a1` was on the first page.
            between
                .withdraw(withdrawal("a1", 1, 2))
                .await
                .expect("a withdrawal between pages");
            let mut raw = raw.lock().await;
            let now = revision(&mut raw).await;
            raw.compact(now, None).await.expect("a compaction");
        }
    });
    reader.set_hook(hook);
    let mut watch = watch(&reader, MODEL, None);
    let (held, _) = snapshot(&mut watch).await;
    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "the hook ran between pages"
    );
    let names: Vec<&str> = held.iter().map(|record| record.name.as_str()).collect();
    assert_eq!(names, ["b1", "c1", "d1"]);
    // Nothing of the first attempt follows.
    let after = tokio::time::timeout(Duration::from_millis(500), watch.next()).await;
    assert!(after.is_err(), "after the snapshot: {after:?}");
}

/// A watch that reconnects from a revision compaction has passed gives its
/// readers `Resnapshot`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconnect_after_compaction_resnapshots() {
    let store = Store::start(&[]).await;
    let writer = open(&store.etcd().client_url(), "/gdl/pools", LIMITS).await;
    let reader = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    writer.publish(record("m1", 1, 1, 0)).await.expect("m1");
    let mut live = watch(&reader, MODEL, None);
    snapshot(&mut live).await;
    store.proxy.cut();
    // Two writes, so that compaction passes the revision after the watch's:
    // etcd keeps the revision it compacts at.
    for seq in 2..=3 {
        writer
            .publish(heartbeat(&record("m1", 1, 1, 0), seq))
            .await
            .expect("a heartbeat");
    }
    let mut raw = store.raw().await;
    let now = revision(&mut raw).await;
    raw.compact(now, None).await.expect("a compaction");
    store.proxy.switch(store.etcd().client);
    resnapshots(&mut live, "a reconnect past compaction").await;
    let (held, _) = snapshot(&mut watch(&reader, MODEL, None)).await;
    assert_eq!(held, [heartbeat(&record("m1", 1, 1, 0), 3)]);
}

/// Push the reader's watch of `MODEL` to a revision past 30, so that a new
/// store is below it.
async fn watched_past(writer: &EtcdDirectory, reader: &EtcdDirectory) -> Watch {
    let mut live = watch(reader, MODEL, None);
    snapshot(&mut live).await;
    let m1 = record("m1", 1, 1, 0);
    writer.publish(m1.clone()).await.expect("m1");
    put_of(&mut live, "m1").await;
    for seq in 2..=30 {
        writer
            .publish(heartbeat(&m1, seq))
            .await
            .expect("a heartbeat");
        put_of(&mut live, "a heartbeat").await;
    }
    live
}

/// A reconnect to a new, empty store, whose revision is below the
/// watch's, gives every reader `Resnapshot`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconnect_to_a_new_empty_store_resnapshots() {
    let mut store = Store::start(&[]).await;
    let url = store.proxy.url();
    let writer = open(&url, "/gdl/pools", LIMITS).await;
    let reader = open(&url, "/gdl/pools", LIMITS).await;
    let mut live = watched_past(&writer, &reader).await;
    let mut other = watch(&reader, MODEL, None);
    snapshot(&mut other).await;
    let empty = store.another(&[]).await;
    store.replace(empty);
    resnapshots(&mut live, "the first reader").await;
    resnapshots(&mut other, "the second reader").await;
    let (held, _) = snapshot(&mut watch(&reader, MODEL, None)).await;
    assert_eq!(held, [], "the new store holds nothing");
}

/// A reconnect to a new, empty store whose revision has passed the old
/// one's gives `Resnapshot`: the revision alone does not show the store
/// was replaced, and its identity does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconnect_to_a_new_store_past_the_old_revision_resnapshots() {
    let mut store = Store::start(&[]).await;
    let url = store.proxy.url();
    let writer = open(&url, "/gdl/pools", LIMITS).await;
    let reader = open(&url, "/gdl/pools", LIMITS).await;
    let mut live = watched_past(&writer, &reader).await;
    store.proxy.cut();
    let empty = store.another(&[]).await;
    let mut raw = empty.raw().await;
    fill_to(&mut raw, 200).await;
    store.replace(empty);
    resnapshots(&mut live, "a store past the old revision").await;
}

/// The store is replaced after the task's last look at the old one and
/// before its watch is created on the new one, which has an identity of
/// its own and a revision past the old: the identity read on the created
/// watch's connection shows the replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_replaced_before_the_watch_is_created_resnapshots() {
    let mut store = Store::start(&[]).await;
    let url = store.proxy.url();
    let writer = open(&url, "/gdl/pools", LIMITS).await;
    let reader = open(&url, "/gdl/pools", LIMITS).await;
    let mut live = watched_past(&writer, &reader).await;
    let replacement = store.another(&[]).await;
    // The new store has its own identity, made by a handle's first write.
    let elsewhere = open(&replacement.client_url(), "/gdl/pools", LIMITS).await;
    elsewhere.publish(record("z1", 1, 1, 0)).await.expect("z1");
    let mut raw = replacement.raw().await;
    fill_to(&mut raw, 200).await;
    let target = replacement.client;
    let proxy_state = store.proxy.switcher();
    let (hook, fired) = once(Point::BeforeWatch, move || {
        let proxy_state = proxy_state.clone();
        async move { proxy_state.switch(target) }
    });
    reader.set_hook(hook);
    // The old store is still there: the reconnect could check it, and only
    // then is the store replaced.
    store.proxy.reset();
    resnapshots(&mut live, "a store replaced before the watch").await;
    assert_eq!(fired.load(Ordering::SeqCst), 1);
    store.etcd = Some(replacement);
    let (held, _) = snapshot(&mut watch(&reader, MODEL, None)).await;
    assert_eq!(held, [record("z1", 1, 1, 0)]);
}

/// A write that read a pool on one store, and is sent to a store that
/// replaced it, is refused, though the new store's tombstone of the pool
/// has the revision of the record the writer read: only the store's
/// identity in the compare tells the two apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_write_to_a_replacement_store_is_refused() {
    let mut store = Store::start(&[]).await;
    // The record lands past the revisions the replacement needs for its
    // identity and another pool's write.
    fill_to(&mut store.raw().await, 10).await;
    let stale = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    let m1 = record("m1", 1, 1, 0);
    stale.publish(m1.clone()).await.expect("m1");
    let mut raw = store.raw().await;
    let read_at = keys(&mut raw, "/gdl/pools/").await[0].1;
    assert!(read_at > 4, "{read_at}");
    let replacement = store.another(&[]).await;
    let other = open(&replacement.client_url(), "/gdl/pools", LIMITS).await;
    // The replacement's identity first, then its tombstone of m1 at the
    // revision `stale` read m1 at.
    other
        .withdraw(withdrawal("unrelated", 1, 1))
        .await
        .expect("the identity");
    let mut raw = replacement.raw().await;
    fill_to(&mut raw, read_at - 1).await;
    other
        .withdraw(withdrawal("m1", 2, 0))
        .await
        .expect("a newer withdrawal");
    let tombstone_at = keys(&mut raw, "/gdl/pools/model_x/m1").await[0].1;
    assert_eq!(
        tombstone_at, read_at,
        "the tombstone has the record's revision"
    );
    store.replace(replacement);
    reach(&stale).await;
    let result = stale.publish(heartbeat(&m1, 2)).await;
    assert!(
        matches!(
            result,
            Err(DirectoryError::NotNewer {
                held: Version { epoch: 2, seq: 0 },
                ..
            })
        ),
        "{result:?}"
    );
    assert_eq!(
        other.last_write(MODEL, "m1").await.expect("a read"),
        Some(LastWrite::Withdrawn(withdrawal("m1", 2, 0)))
    );
}

/// A handle whose store was replaced by an empty one makes the identity
/// key again, and its next heartbeat lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handle_makes_the_identity_again_on_an_empty_store() {
    let mut store = Store::start(&[]).await;
    let directory = open(&store.proxy.url(), "/gdl/pools", LIMITS).await;
    let m1 = record("m1", 1, 1, 0);
    directory.publish(m1.clone()).await.expect("m1");
    let empty = store.another(&[]).await;
    store.replace(empty);
    reach(&directory).await;
    directory
        .publish(heartbeat(&m1, 2))
        .await
        .expect("a heartbeat on the empty store");
    let mut raw = store.raw().await;
    let ids = raw.get(STORE_ID, None).await.expect("a read").kvs().len();
    assert_eq!(ids, 1, "the identity was made again");
    assert_eq!(
        directory.last_write(MODEL, "m1").await.expect("a read"),
        Some(LastWrite::Record(Box::new(heartbeat(&m1, 2))))
    );
    // And again for a pool the handle never wrote on that store.
    directory.publish(record("m2", 1, 1, 0)).await.expect("m2");
}
