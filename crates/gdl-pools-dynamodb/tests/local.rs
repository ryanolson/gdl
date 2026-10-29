//! The DynamoDB directory against DynamoDB Local: it passes the directory
//! conformance suite and keeps every rule of the contract, and holds what
//! two tables in a replicated store add. Two handles over one pair of
//! tables, each standing for a hub's process, see each other's writes and
//! refuse each other's tokens; a record whose list has not arrived is not
//! installed, and a new watch's snapshot waits for it, within a bound; a
//! list its pool has moved past is marked to expire, and the one a record
//! names is not.
//!
//! Outside the default suite: the target needs the `dynamodb-local`
//! feature and a DynamoDB Local at `GDL_DYNAMODB_LOCAL`. Run it with
//! `scripts/dynamodb-local.sh`, which starts one in Docker and stops it
//! after. Every case makes tables of its own, so cases share the server.

use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use aws_sdk_dynamodb::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    types::AttributeValue,
};
use futures::StreamExt;
use gdl_pools::{
    DirectoryError, ModelEntry, ModelState, PoolDirectory, PoolEvent, PoolRecord, PoolState,
    Presence, ResumeToken, Version, Withdrawal,
    conformance::{self, Backend},
};
use gdl_pools_dynamodb::{DynamoDirectory, DynamoLimits, Tables};

// In a directory of its own, so that cargo does not take it for a test
// target.
#[path = "local/interleave.rs"]
mod interleave;

const MODEL: &str = "model_x";
const WAIT: Duration = Duration::from_secs(5);

/// Small bounds and a short period: every change the suite waits for is
/// found by a query, so the lagging case's `buffer + 1` changes cost that
/// many periods.
const LIMITS: DynamoLimits = DynamoLimits {
    buffer: 8,
    history: 5,
    period: Duration::from_millis(20),
    first_sync: Duration::from_secs(5),
};

/// A client of the DynamoDB Local at `GDL_DYNAMODB_LOCAL`, with dummy
/// credentials, which DynamoDB Local takes and never checks.
fn client() -> Client {
    let endpoint = std::env::var("GDL_DYNAMODB_LOCAL").unwrap_or_else(|_| {
        panic!(
            "GDL_DYNAMODB_LOCAL names no DynamoDB Local: run scripts/dynamodb-local.sh, which starts one"
        )
    });
    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("local", "local", None, None, "local"))
        .http_client(aws_smithy_http_client::Builder::new().build_http())
        .build();
    Client::from_conf(config)
}

/// `client`'s settings with an HTTP pool of their own, for a client whose
/// calls a case pauses: a pause blocks its thread's runtime, and would
/// stall a pooled connection that another client reused.
fn own_pool(client: &Client) -> aws_sdk_dynamodb::config::Builder {
    client
        .config()
        .to_builder()
        .http_client(aws_smithy_http_client::Builder::new().build_http())
}

/// Two new tables, named apart from every other case's and run's.
async fn tables(client: &Client) -> Tables {
    static TABLES: AtomicUsize = AtomicUsize::new(0);
    let n = TABLES.fetch_add(1, Ordering::Relaxed);
    let run = std::process::id();
    let tables = Tables {
        records: format!("gdl-pools-{run}-{n}"),
        lists: format!("gdl-lists-{run}-{n}"),
    };
    DynamoDirectory::create_tables(client, &tables)
        .await
        .expect("the tables");
    tables
}

struct Local {
    client: Client,
}

impl Backend for Local {
    type Directory = DynamoDirectory;

    // The handles are clones of one directory, as the hubs of one process
    // would hold it: resume tokens are served across them. The separate
    // processes of the tests below are separate handles.
    async fn fresh<const N: usize>(&self) -> [DynamoDirectory; N] {
        let tables = tables(&self.client).await;
        let directory = DynamoDirectory::with_limits(self.client.clone(), tables, LIMITS);
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        LIMITS.history
    }

    fn watchers(&self, directory: &DynamoDirectory, class: &str) -> usize {
        directory.watchers(class)
    }

    // A new process over the same tables.
    async fn restart(&self, directory: DynamoDirectory) -> DynamoDirectory {
        DynamoDirectory::with_limits(self.client.clone(), directory.tables().clone(), LIMITS)
    }
}

#[tokio::test]
async fn the_dynamodb_directory_conforms() {
    conformance::run(&Local { client: client() }).await;
}

type Watch = Pin<Box<<DynamoDirectory as PoolDirectory>::Watch>>;

fn record(pool: &str, epoch: u64, seq: u64, models: &[&str]) -> PoolRecord {
    PoolRecord {
        presence: Presence::Serving,
        models: models
            .iter()
            .map(|model| ModelEntry {
                model: (*model).into(),
                routing_key: None,
                state: ModelState::Ready,
                since: epoch << 32,
            })
            .collect(),
        models_version: (epoch << 32) + models.len() as u64,
        revoked: Vec::new(),
        name: pool.into(),
        class: MODEL.into(),
        region: "us-east-1".into(),
        home: Some("global-us".into()),
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

fn watch(directory: &DynamoDirectory, from: Option<ResumeToken>) -> Watch {
    Box::pin(directory.watch(MODEL, from))
}

async fn next(watch: &mut Watch) -> Option<Result<PoolEvent, DirectoryError>> {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .unwrap_or_else(|_| panic!("nothing within {WAIT:?}"))
}

async fn event(watch: &mut Watch) -> PoolEvent {
    match next(watch).await {
        Some(Ok(event)) => event,
        other => panic!("expected an event, got {other:?}"),
    }
}

/// A snapshot's whole records, and its token.
async fn snapshot(watch: &mut Watch) -> (Vec<PoolRecord>, ResumeToken) {
    let mut records = Vec::new();
    loop {
        match event(watch).await {
            PoolEvent::Current(record) => records.push(record),
            PoolEvent::Synced {
                token,
                snapshot: true,
            } => return (records, token),
            other => panic!("the snapshot: {other:?}"),
        }
    }
}

/// The next event is the put of `published`, whole; its token.
async fn put(watch: &mut Watch, published: &PoolRecord) -> ResumeToken {
    match event(watch).await {
        PoolEvent::Put { record, token } if record == *published => token,
        other => panic!("expected the put of {published:?}, got {other:?}"),
    }
}

/// The lists table's items of `pool`, by sort key.
async fn lists(client: &Client, tables: &Tables, pool: &str) -> Vec<(String, Option<u64>)> {
    let page = client
        .query()
        .table_name(&tables.lists)
        .key_condition_expression("#owner = :owner")
        .expression_attribute_names("#owner", "owner")
        .expression_attribute_values(":owner", AttributeValue::S(format!("{MODEL}/{pool}")))
        .consistent_read(true)
        .send()
        .await
        .expect("the lists");
    page.items()
        .iter()
        .map(|item| {
            let key = match &item["list"] {
                AttributeValue::S(key) => key.clone(),
                other => panic!("a list key: {other:?}"),
            };
            let expires = item.get("expires_at").map(|at| match at {
                AttributeValue::N(at) => at.parse().expect("a time"),
                other => panic!("an expiry: {other:?}"),
            });
            (key, expires)
        })
        .collect()
}

/// The key of the list that `pool`'s stored record names, and that list's
/// expiry. Fails when the record names no list or its list is missing: a
/// stored record must name a list that is there.
async fn named(client: &Client, tables: &Tables, pool: &str) -> (String, Option<u64>) {
    named_list(client, tables, pool)
        .await
        .unwrap_or_else(|broken| panic!("{broken}"))
}

/// [`named`], or why the stored record names no list that is there.
async fn named_list(
    client: &Client,
    tables: &Tables,
    pool: &str,
) -> Result<(String, Option<u64>), String> {
    let stored = client
        .get_item()
        .table_name(&tables.records)
        .key("class", AttributeValue::S(MODEL.into()))
        .key("pool", AttributeValue::S(pool.into()))
        .consistent_read(true)
        .send()
        .await
        .expect("the stored record");
    let key = match stored.item().and_then(|item| item.get("list")) {
        Some(AttributeValue::S(key)) => key.clone(),
        other => {
            return Err(format!(
                "the stored record of {pool} names no list: {other:?}"
            ));
        }
    };
    let lists = lists(client, tables, pool).await;
    match lists.iter().find(|(list, _)| *list == key) {
        Some(&(_, expires)) => Ok((key, expires)),
        None => Err(format!(
            "the stored record of {pool} names {key}, which is missing: {lists:?}"
        )),
    }
}

/// Whether no list of `lists` is set to expire. With [`named`], or a
/// snapshot that joins the record to its list, the claim of a case whose
/// pool's lists all stay: the list the record names is there and live.
fn none_expire(lists: &[(String, Option<u64>)]) -> bool {
    lists.iter().all(|(_, expiry)| expiry.is_none())
}

type Item = std::collections::HashMap<String, AttributeValue>;

/// The items a publish of `record` writes, made in tables of their own: its
/// record and its list, to put in a case's tables in either order, as
/// replication may deliver them.
async fn items(client: &Client, record: PoolRecord) -> (Item, Item) {
    let tables = tables(client).await;
    DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS)
        .publish(record)
        .await
        .expect("a staged record");
    let only = |table: &str| {
        let table = table.to_owned();
        async move {
            let scan = client
                .scan()
                .table_name(table)
                .send()
                .await
                .expect("a scan");
            match scan.items() {
                [item] => item.clone(),
                items => panic!("one item: {items:?}"),
            }
        }
    };
    (only(&tables.records).await, only(&tables.lists).await)
}

async fn put_item(client: &Client, table: &str, item: Item) {
    client
        .put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .expect("an item put");
}

/// Nothing comes on `watch` for ten periods: a wait for something that
/// must not happen, bounded so that the case fails rather than hangs.
async fn quiet(watch: &mut Watch, what: &str) {
    if let Ok(event) = tokio::time::timeout(LIMITS.period * 10, watch.next()).await {
        panic!("{what}: {event:?}");
    }
}

#[tokio::test]
async fn two_processes_see_each_others_writes_and_refuse_each_others_tokens() {
    let client = client();
    let tables = tables(&client).await;
    let a = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let b = DynamoDirectory::with_limits(client.clone(), tables, LIMITS);
    let mut seen_by_a = watch(&a, None);
    let mut seen_by_b = watch(&b, None);
    assert!(snapshot(&mut seen_by_a).await.0.is_empty());
    assert!(snapshot(&mut seen_by_b).await.0.is_empty());

    let m1 = record("model-x-01", 1, 1, &["qwen-32b"]);
    a.publish(m1.clone()).await.expect("m1 from a");
    let token = put(&mut seen_by_a, &m1).await;
    put(&mut seen_by_b, &m1).await;
    let m2 = record("model-x-02", 1, 1, &["qwen-32b", "llama-70b"]);
    b.publish(m2.clone()).await.expect("m2 from b");
    put(&mut seen_by_a, &m2).await;
    put(&mut seen_by_b, &m2).await;

    // The table arbitrates between processes: a stale write from either is
    // refused at the version the other wrote.
    a.withdraw(Withdrawal {
        class: MODEL.into(),
        pool: "model-x-01".into(),
        epoch: 1,
        seq: 2,
    })
    .await
    .expect("m1 withdrawn by a");
    match b.publish(record("model-x-01", 1, 2, &["qwen-32b"])).await {
        Err(DirectoryError::NotNewer { held, offered, .. }) => {
            assert_eq!(held, Version { epoch: 1, seq: 2 });
            assert_eq!(offered, Version { epoch: 1, seq: 2 });
        }
        other => panic!("a stale write from b: {other:?}"),
    }

    // A token is served only by the handle whose watch made it.
    let mut elsewhere = watch(&b, Some(token.clone()));
    assert_eq!(event(&mut elsewhere).await, PoolEvent::Resnapshot);
    assert!(next(&mut elsewhere).await.is_none(), "the watch went on");
    let mut resumed = watch(&a, Some(token));
    assert!(matches!(
        event(&mut resumed).await,
        PoolEvent::Synced {
            snapshot: false,
            ..
        }
    ));
}

/// Replication does not order the two tables, so a region can hold a
/// record before its list. The record is not installed until the list
/// arrives: a live watch keeps the pool's earlier record, a new watch's
/// snapshot waits, and both see the record whole once the list is there.
#[tokio::test]
async fn a_record_waits_for_its_list() {
    let client = client();
    let tables = tables(&client).await;
    let writer = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let reader = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let mut live = watch(&reader, None);
    snapshot(&mut live).await;

    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    writer
        .publish(first.clone())
        .await
        .expect("the first record");
    put(&mut live, &first).await;

    // Its models change, and this region gets the record before the list.
    let second = record("model-x-01", 1, 2, &["qwen-32b", "llama-70b"]);
    let (record_item, list_item) = items(&client, second.clone()).await;
    put_item(&client, &tables.records, record_item).await;

    quiet(&mut live, "a record without its list reached a live watch").await;
    let mut fresh = watch(&reader, None);
    quiet(&mut fresh, "a snapshot completed without a record's list").await;

    put_item(&client, &tables.lists, list_item).await;
    put(&mut live, &second).await;
    let (records, _) = snapshot(&mut fresh).await;
    assert_eq!(records, [second]);
}

/// A snapshot whose list never arrives is not completed: the watch ends
/// with the pools it waited for, and the reader watches again after its
/// backoff.
#[tokio::test]
async fn a_snapshot_that_waits_too_long_for_a_list_ends_its_watch() {
    let client = client();
    let tables = tables(&client).await;
    let first_sync = Duration::from_millis(300);
    let reader = DynamoDirectory::with_limits(
        client.clone(),
        tables.clone(),
        DynamoLimits {
            first_sync,
            ..LIMITS
        },
    );
    let (record_item, _) = items(&client, record("model-x-01", 1, 1, &["qwen-32b"])).await;
    put_item(&client, &tables.records, record_item).await;

    let started = tokio::time::Instant::now();
    let mut watch = watch(&reader, None);
    match next(&mut watch).await {
        Some(Err(DirectoryError::Unavailable(problem))) => {
            assert!(problem.contains("model-x-01"), "{problem}");
        }
        other => panic!("expected the watch to end unavailable, got {other:?}"),
    }
    assert!(started.elapsed() >= first_sync, "ended before its bound");
    assert!(next(&mut watch).await.is_none(), "the watch went on");
    assert_eq!(reader.watchers(MODEL), 0);
}

/// Only a list its pool has moved past is marked to expire, and only the
/// lists table has TTL. A heartbeat does not write its list again.
#[tokio::test]
async fn a_superseded_list_expires_and_the_named_one_stays() {
    let client = client();
    let tables = tables(&client).await;
    let directory = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    directory
        .publish(record("model-x-01", 1, 1, &["qwen-32b"]))
        .await
        .expect("a record");
    directory
        .publish(record("model-x-01", 1, 2, &["qwen-32b"]))
        .await
        .expect("a heartbeat");
    let lists_now = lists(&client, &tables, "model-x-01").await;
    assert!(
        none_expire(&lists_now),
        "one list, not expiring: {lists_now:?}"
    );

    let latest = record("model-x-01", 1, 3, &["qwen-32b", "llama-70b"]);
    directory.publish(latest.clone()).await.expect("new models");
    let lists_now = lists(&client, &tables, "model-x-01").await;
    let expiring = lists_now.iter().filter(|list| list.1.is_some()).count();
    assert_eq!(
        (lists_now.len(), expiring),
        (2, 1),
        "the earlier list expires, the named one stays: {lists_now:?}"
    );
    // The list that stays is the one the record names.
    let (records, _) = snapshot(&mut watch(&directory, None)).await;
    assert_eq!(records, [latest]);

    // A withdrawal moves its pool past every list.
    directory
        .withdraw(Withdrawal {
            class: MODEL.into(),
            pool: "model-x-01".into(),
            epoch: 1,
            seq: 4,
        })
        .await
        .expect("withdrawn");
    // A withdrawal expires nothing: the pool keeps its last list.
    let lists_now = lists(&client, &tables, "model-x-01").await;
    assert_eq!(
        lists_now.iter().filter(|list| list.1.is_none()).count(),
        1,
        "{lists_now:?}"
    );

    for (table, ttl) in [(&tables.records, false), (&tables.lists, true)] {
        let described = client
            .describe_time_to_live()
            .table_name(table)
            .send()
            .await
            .expect("the table's TTL");
        let status = described
            .time_to_live_description()
            .and_then(|ttl| ttl.time_to_live_status())
            .cloned();
        let on = matches!(
            status,
            Some(aws_sdk_dynamodb::types::TimeToLiveStatus::Enabled)
                | Some(aws_sdk_dynamodb::types::TimeToLiveStatus::Enabling)
        );
        assert_eq!(on, ttl, "{table}: {status:?}");
    }
}

#[tokio::test]
async fn a_cached_list_is_kept_when_published_after_another_handles_withdrawal() {
    let client = client();
    let tables = tables(&client).await;
    let writer = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let operator = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    writer
        .publish(first.clone())
        .await
        .expect("the first record");

    operator
        .withdraw(Withdrawal {
            class: MODEL.into(),
            pool: first.name.clone(),
            epoch: first.epoch,
            seq: 2,
        })
        .await
        .expect("a withdrawal through another handle in the same region");
    assert!(
        none_expire(&lists(&client, &tables, &first.name).await),
        "a withdrawal expires no list"
    );

    let latest = PoolRecord { seq: 3, ..first };
    writer
        .publish(latest.clone())
        .await
        .expect("a newer publish with the same list");
    let (records, _) = snapshot(&mut watch(&writer, None)).await;
    assert_eq!(
        records,
        std::slice::from_ref(&latest),
        "the live record names the list"
    );

    let lists_now = lists(&client, &tables, &latest.name).await;
    assert!(
        none_expire(&lists_now),
        "a live record's list must not retain its withdrawal's TTL: {lists_now:?}"
    );
}

#[tokio::test]
async fn a_delayed_withdrawal_does_not_expire_a_republished_list() {
    use std::sync::{Mutex, mpsc};

    use aws_sdk_dynamodb::{
        config::{ConfigBag, Intercept, interceptors::BeforeSerializationInterceptorContextRef},
        operation::{transact_write_items::TransactWriteItemsInput, update_item::UpdateItemInput},
    };
    use tokio::sync::oneshot;

    // Pause the cleanup after its tombstone is stored, so that a newer
    // publish finishes before the earlier withdrawal sets the list's TTL.
    #[derive(Debug)]
    struct PauseExpiry {
        began: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl Intercept for PauseExpiry {
        fn name(&self) -> &'static str {
            "PauseExpiry"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let input = context.input();
            let cleanup = input.downcast_ref::<UpdateItemInput>().is_some()
                || input.downcast_ref::<TransactWriteItemsInput>().is_some();
            if cleanup && let Some(began) = self.began.lock().expect("the signal lock").take() {
                began.send(()).expect("the test waits for cleanup");
                self.release
                    .lock()
                    .expect("the release lock")
                    .recv_timeout(WAIT)
                    .expect("the test releases cleanup");
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let writer = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    writer
        .publish(first.clone())
        .await
        .expect("the first record");

    let (began, paused) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let config = own_pool(&client)
        .interceptor(PauseExpiry {
            began: Mutex::new(Some(began)),
            release: Mutex::new(released),
        })
        .build();
    let operator = DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
    let withdrawing = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the operator's runtime")
            .block_on(async move {
                tokio::time::timeout(WAIT, async move {
                    operator
                        .withdraw(Withdrawal {
                            class: MODEL.into(),
                            pool: "model-x-01".into(),
                            epoch: 1,
                            seq: 2,
                        })
                        .await
                })
                .await
            })
    });
    // A withdrawal has no list to expire, so it completes without a write
    // to the lists table: the cleanup this case paused never begins.
    tokio::task::spawn_blocking(move || withdrawing.join().expect("the operator thread"))
        .await
        .expect("join the operator")
        .expect("the withdrawal stays within its bound")
        .expect("the withdrawal completes");
    assert!(
        paused.await.is_err(),
        "the withdrawal wrote to the lists table"
    );
    drop(release);

    let latest = PoolRecord { seq: 3, ..first };
    writer
        .publish(latest.clone())
        .await
        .expect("a newer record");
    assert!(
        none_expire(&lists(&client, &tables, &latest.name).await),
        "the newer publish leaves its list without TTL"
    );
    let latest = PoolRecord { seq: 4, ..latest };
    writer
        .publish(latest.clone())
        .await
        .expect("a later heartbeat");
    let (records, _) = snapshot(&mut watch(&writer, None)).await;
    assert_eq!(
        records,
        std::slice::from_ref(&latest),
        "the newer record remains live"
    );
    let lists_now = lists(&client, &tables, &latest.name).await;
    assert!(
        none_expire(&lists_now),
        "delayed cleanup must not expire the live record's list: {lists_now:?}"
    );
}

#[tokio::test]
async fn a_withdrawal_between_a_list_and_its_record_does_not_expire_the_list() {
    use std::sync::{Mutex, mpsc};

    use aws_sdk_dynamodb::{
        config::{ConfigBag, Intercept, interceptors::BeforeSerializationInterceptorContextRef},
        operation::put_item::PutItemInput,
    };
    use tokio::sync::oneshot;

    // Stop after the list is written, before the record can protect it.
    #[derive(Debug)]
    struct PauseRecord {
        began: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl Intercept for PauseRecord {
        fn name(&self) -> &'static str {
            "PauseRecord"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let record = context
                .input()
                .downcast_ref::<PutItemInput>()
                .is_some_and(|put| put.condition_expression().is_some());
            if record && let Some(began) = self.began.lock().expect("the signal lock").take() {
                began.send(()).expect("the test waits for the record");
                self.release
                    .lock()
                    .expect("the release lock")
                    .recv_timeout(WAIT)
                    .expect("the test releases the record");
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let operator = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    operator
        .publish(first.clone())
        .await
        .expect("the first record");

    let (began, paused) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let config = own_pool(&client)
        .interceptor(PauseRecord {
            began: Mutex::new(Some(began)),
            release: Mutex::new(released),
        })
        .build();
    let writer = DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
    let published = PoolRecord { seq: 3, ..first };
    let latest = PoolRecord {
        seq: 4,
        ..published.clone()
    };
    let publishing = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the publisher's runtime")
            .block_on(async move {
                tokio::time::timeout(WAIT, async {
                    writer.publish(published.clone()).await?;
                    writer
                        .publish(PoolRecord {
                            seq: 4,
                            ..published
                        })
                        .await
                })
                .await
            })
    });
    tokio::time::timeout(WAIT, paused)
        .await
        .expect("the record write begins within the bound")
        .expect("the publisher has written its list");

    operator
        .withdraw(Withdrawal {
            class: MODEL.into(),
            pool: latest.name.clone(),
            epoch: 1,
            seq: 2,
        })
        .await
        .expect("the withdrawal completes before the newer record is stored");
    assert!(
        none_expire(&lists(&client, &tables, &latest.name).await),
        "a withdrawal expires no list, the prepared one included"
    );
    release.send(()).expect("release the record");
    tokio::task::spawn_blocking(move || publishing.join().expect("the publisher thread"))
        .await
        .expect("join the publisher")
        .expect("the publish stays within its bound")
        .expect("the newer record is stored");

    let reader = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let (records, _) = snapshot(&mut watch(&reader, None)).await;
    assert_eq!(
        records,
        std::slice::from_ref(&latest),
        "the list belongs to a live record"
    );
    let lists_now = lists(&client, &tables, &latest.name).await;
    assert!(
        none_expire(&lists_now),
        "a successful publish must clear cleanup between its list and record writes: {lists_now:?}"
    );
}

/// A pool scaled to 0 after a withdrawal in its region sends no later
/// write, so its list must not be left expiring: a withdrawal expires none.
#[tokio::test]
async fn a_pool_scaled_to_zero_after_a_withdrawal_keeps_its_list() {
    use std::sync::Arc;

    use aws_sdk_dynamodb::{
        config::{ConfigBag, Intercept, interceptors::BeforeSerializationInterceptorContextRef},
        operation::transact_write_items::TransactWriteItemsInput,
    };

    #[derive(Debug)]
    struct FailKeep(Arc<AtomicUsize>);

    impl Intercept for FailKeep {
        fn name(&self) -> &'static str {
            "FailKeep"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let keep = context
                .input()
                .downcast_ref::<TransactWriteItemsInput>()
                .is_some_and(|input| {
                    input
                        .transact_items()
                        .iter()
                        .any(|item| item.put().is_some())
                });
            if keep {
                self.0.fetch_add(1, Ordering::Relaxed);
                return Err(std::io::Error::other("injected keep transaction failure").into());
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let failed = Arc::new(AtomicUsize::new(0));
    let config = own_pool(&client)
        .interceptor(FailKeep(Arc::clone(&failed)))
        .build();
    let writer = DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
    let operator = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    writer
        .publish(first.clone())
        .await
        .expect("the first record");
    operator
        .withdraw(Withdrawal {
            class: MODEL.into(),
            pool: first.name.clone(),
            epoch: first.epoch,
            seq: 2,
        })
        .await
        .expect("the same-region withdrawal");
    assert!(none_expire(&lists(&client, &tables, &first.name).await));

    // A scaled-to-zero pool sends no heartbeat that could repair the list.
    let latest = PoolRecord {
        seq: 3,
        presence: Presence::ScaledToZero,
        ..first
    };
    let acknowledged = writer.publish(latest.clone()).await;
    assert_eq!(
        failed.load(Ordering::Relaxed),
        0,
        "no write checks or repairs a list, so nothing can fail there"
    );
    let stranded = lists(&client, &tables, &latest.name).await;
    assert_eq!(
        snapshot(&mut watch(&operator, None)).await.0,
        std::slice::from_ref(&latest)
    );

    // The next publish repairs the cache miss, but it is not owed by the pool.
    writer
        .publish(PoolRecord { seq: 4, ..latest })
        .await
        .expect("a later publish");
    assert!(none_expire(&lists(&client, &tables, "model-x-01").await));
    assert!(
        acknowledged.is_err() || none_expire(&stranded),
        "a successful publish must not leave its live list expiring: result={acknowledged:?}, lists={stranded:?}"
    );
}

#[tokio::test]
async fn a_missing_lists_first_sync_bound_survives_a_stalled_retry() {
    use std::sync::Arc;

    use aws_sdk_dynamodb::config::{
        ConfigBag, Intercept, RuntimeComponents, interceptors::BeforeTransmitInterceptorContextMut,
    };
    use tokio::sync::Notify;

    #[derive(Debug)]
    struct StallRetry {
        queries: AtomicUsize,
        endpoint: String,
        began: Arc<Notify>,
    }

    impl Intercept for StallRetry {
        fn name(&self) -> &'static str {
            "StallRetry"
        }

        fn modify_before_transmit(
            &self,
            context: &mut BeforeTransmitInterceptorContextMut<'_>,
            _runtime: &RuntimeComponents,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if context.request().headers().get("x-amz-target") == Some("DynamoDB_20120810.Query")
                && self.queries.fetch_add(1, Ordering::Relaxed) > 0
            {
                context.request_mut().set_uri(self.endpoint.as_str())?;
                self.began.notify_one();
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let (item, _) = items(&client, record("model-x-01", 1, 1, &["qwen-32b"])).await;
    put_item(&client, &tables.records, item).await;
    // The listener accepts TCP handshakes but sends no HTTP response.
    let stalled = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
    stalled.set_nonblocking(true).expect("a bounded accept");
    let began = Arc::new(Notify::new());
    let config = own_pool(&client)
        .interceptor(StallRetry {
            queries: AtomicUsize::new(0),
            endpoint: format!(
                "http://{}",
                stalled.local_addr().expect("the listener's port")
            ),
            began: Arc::clone(&began),
        })
        .build();
    let reader = DynamoDirectory::with_limits(
        Client::from_conf(config),
        tables,
        DynamoLimits {
            first_sync: Duration::from_millis(300),
            ..LIMITS
        },
    );
    let mut watch = watch(&reader, None);
    let waiting = watch.next();
    tokio::pin!(waiting);
    tokio::select! {
        result = &mut waiting => panic!("the watch ended before the retry stalled: {result:?}"),
        result = tokio::time::timeout(WAIT, began.notified()) => result.expect("the second query begins"),
    }
    // Two seconds gives the 300 ms deadline room on a busy host.
    let result = tokio::time::timeout(Duration::from_secs(2), &mut waiting).await;
    let _connection = stalled
        .accept()
        .expect("the retry connected to the listener");
    assert!(
        matches!(result, Ok(Some(Err(DirectoryError::Unavailable(_))))),
        "first_sync must end a watch waiting for a known missing list even while a retry stalls: {result:?}"
    );
}

#[tokio::test]
async fn a_stalled_write_does_not_hold_a_watch_past_first_sync() {
    use aws_sdk_dynamodb::config::{
        ConfigBag, Intercept, RuntimeComponents, interceptors::BeforeTransmitInterceptorContextMut,
    };

    #[derive(Debug)]
    struct StallWrite(String);

    impl Intercept for StallWrite {
        fn name(&self) -> &'static str {
            "StallWrite"
        }

        fn modify_before_transmit(
            &self,
            context: &mut BeforeTransmitInterceptorContextMut<'_>,
            _runtime: &RuntimeComponents,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if context.request().headers().get("x-amz-target") == Some("DynamoDB_20120810.PutItem")
            {
                context.request_mut().set_uri(self.0.as_str())?;
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let stalled = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback listener");
    let config = own_pool(&client)
        .interceptor(StallWrite(format!(
            "http://{}",
            stalled.local_addr().expect("the listener's port")
        )))
        .build();
    let directory = DynamoDirectory::with_limits(
        Client::from_conf(config),
        tables,
        DynamoLimits {
            first_sync: Duration::from_millis(300),
            ..LIMITS
        },
    );
    let mut live = watch(&directory, None);
    assert!(snapshot(&mut live).await.0.is_empty());

    let writer = directory.clone();
    let publishing =
        tokio::spawn(async move { writer.publish(record("model-x-01", 1, 1, &[])).await });
    // The writer owns the class lock before its HTTP request starts.
    let (_connection, _) = tokio::time::timeout(WAIT, stalled.accept())
        .await
        .expect("the writer connects within its bound")
        .expect("the writer's connection");
    let mut fresh = watch(&directory, None);
    // Two seconds gives the 300 ms deadline room on a busy host.
    let result = tokio::time::timeout(Duration::from_secs(2), fresh.next()).await;
    assert!(!publishing.is_finished(), "the writer is still stalled");
    publishing.abort();
    let _ = publishing.await;
    assert!(
        matches!(result, Ok(Some(Err(DirectoryError::Unavailable(_))))),
        "first_sync must include waiting for the class lock held by a stalled write: {result:?}"
    );
}

/// A record put whose answer is lost after a withdrawal leaves no list
/// expiring for the next publish to find.
#[tokio::test]
async fn a_lost_record_response_leaves_its_list_unexpired() {
    use std::sync::{Arc, atomic::AtomicBool};

    use aws_sdk_dynamodb::{
        config::{
            ConfigBag, Intercept, RuntimeComponents,
            interceptors::{
                BeforeDeserializationInterceptorContextRef,
                BeforeSerializationInterceptorContextRef,
            },
            retry::RetryConfig,
        },
        operation::put_item::PutItemInput,
    };

    /// Loses the answer of the next records-table put that succeeds, once
    /// armed.
    #[derive(Debug)]
    struct LoseResponse {
        armed: Arc<AtomicBool>,
        record: AtomicBool,
    }

    impl Intercept for LoseResponse {
        fn name(&self) -> &'static str {
            "LoseResponse"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let record = context
                .input()
                .downcast_ref::<PutItemInput>()
                .is_some_and(|put| put.item().is_some_and(|item| item.contains_key("pool")));
            self.record.store(record, Ordering::Relaxed);
            Ok(())
        }

        fn read_after_transmit(
            &self,
            context: &BeforeDeserializationInterceptorContextRef<'_>,
            _runtime: &RuntimeComponents,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if context.response().status().is_success()
                && self.record.load(Ordering::Relaxed)
                && self.armed.swap(false, Ordering::Relaxed)
            {
                return Err(std::io::Error::other("injected loss of a successful response").into());
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let lose = Arc::new(AtomicBool::new(false));
    let config = own_pool(&client)
        .retry_config(RetryConfig::disabled())
        .interceptor(LoseResponse {
            armed: Arc::clone(&lose),
            record: AtomicBool::new(false),
        })
        .build();
    let writer = DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
    let operator = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    writer
        .publish(first.clone())
        .await
        .expect("the first record");
    operator
        .withdraw(Withdrawal {
            class: MODEL.into(),
            pool: first.name.clone(),
            epoch: first.epoch,
            seq: 2,
        })
        .await
        .expect("the same-region withdrawal");
    assert!(none_expire(&lists(&client, &tables, &first.name).await));

    // The cached list's put is refused, since the pool's item is the
    // withdrawal: the write puts a list of its own, then its record, whose
    // answer is lost.
    lose.store(true, Ordering::Relaxed);
    let stored = PoolRecord { seq: 3, ..first };
    let uncertain = writer.publish(stored.clone()).await;
    assert!(!lose.load(Ordering::Relaxed), "the response was lost");
    assert!(
        matches!(uncertain, Err(DirectoryError::Unavailable(_))),
        "the write's result is unknown to the writer: {uncertain:?}"
    );
    assert_eq!(
        snapshot(&mut watch(&operator, None)).await.0,
        std::slice::from_ref(&stored),
        "DynamoDB stored the record before its response was lost"
    );

    let latest = PoolRecord {
        seq: 4,
        presence: Presence::ScaledToZero,
        ..stored
    };
    writer
        .publish(latest.clone())
        .await
        .expect("the next publish is acknowledged");
    assert_eq!(
        snapshot(&mut watch(&operator, None)).await.0,
        std::slice::from_ref(&latest)
    );
    let now = lists(&client, &tables, &latest.name).await;
    assert!(
        none_expire(&now),
        "a successful publish after a lost record response must keep its live list from expiring: {now:?}"
    );
}

/// Two publishes of one pool through clones of a handle run one at a
/// time, and neither leaves the list expiring after a withdrawal between
/// them.
#[tokio::test]
async fn overlapping_publishes_of_a_pool_leave_its_list_unexpired() {
    use aws_sdk_dynamodb::{
        config::{ConfigBag, Intercept, interceptors::BeforeSerializationInterceptorContextRef},
        operation::{put_item::PutItemInput, transact_write_items::TransactWriteItemsInput},
    };
    use std::sync::{Arc, Mutex, atomic::AtomicBool, mpsc};
    use tokio::sync::oneshot;

    #[derive(Debug)]
    struct Pause {
        began: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl Pause {
        fn wait(&self) {
            if let Some(began) = self.began.lock().expect("the signal lock").take() {
                began.send(()).expect("the test waits for the record");
                self.release
                    .lock()
                    .expect("the release lock")
                    .recv_timeout(WAIT)
                    .expect("the test releases the record");
            }
        }
    }
    #[derive(Debug)]
    struct Overlap {
        first: Pause,
        second: Pause,
        prepared: Mutex<Option<oneshot::Sender<()>>>,
        armed: Arc<AtomicBool>,
        failed: Arc<AtomicUsize>,
    }
    impl Intercept for Overlap {
        fn name(&self) -> &'static str {
            "Overlap"
        }
        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if let Some(put) = context.input().downcast_ref::<PutItemInput>() {
                if put.condition_expression().is_some() {
                    match put.item().and_then(|item| item.get("seq")) {
                        Some(AttributeValue::N(seq)) if seq == "2" => self.first.wait(),
                        Some(AttributeValue::N(seq)) if seq == "4" => self.second.wait(),
                        _ => {}
                    }
                } else if self.armed.load(Ordering::Relaxed)
                    && let Some(prepared) = self.prepared.lock().expect("the signal lock").take()
                {
                    prepared
                        .send(())
                        .expect("the second publish removed the cache entry");
                }
            }
            if context
                .input()
                .downcast_ref::<TransactWriteItemsInput>()
                .is_some_and(|input| {
                    input
                        .transact_items()
                        .iter()
                        .any(|item| item.put().is_some())
                })
            {
                self.failed.fetch_add(1, Ordering::Relaxed);
                return Err(std::io::Error::other("injected keep transaction failure").into());
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let (first_began, first_paused) = oneshot::channel();
    let (first_release, first_released) = mpsc::channel();
    let (second_began, second_paused) = oneshot::channel();
    let (second_release, second_released) = mpsc::channel();
    let (prepared, preparing) = oneshot::channel();
    let armed = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicUsize::new(0));
    let config = own_pool(&client)
        .interceptor(Overlap {
            first: Pause {
                began: Mutex::new(Some(first_began)),
                release: Mutex::new(first_released),
            },
            second: Pause {
                began: Mutex::new(Some(second_began)),
                release: Mutex::new(second_released),
            },
            prepared: Mutex::new(Some(prepared)),
            armed: armed.clone(),
            failed: failed.clone(),
        })
        .build();
    let writer = DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
    let operator = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    writer
        .publish(first.clone())
        .await
        .expect("the first record");

    let start = |directory: DynamoDirectory, published: PoolRecord| {
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the publisher runtime")
                .block_on(
                    async move { tokio::time::timeout(WAIT, directory.publish(published)).await },
                )
        })
    };
    let one = start(
        writer.clone(),
        PoolRecord {
            seq: 2,
            ..first.clone()
        },
    );
    tokio::time::timeout(WAIT, first_paused)
        .await
        .expect("the first record starts")
        .expect("the first record is paused");
    armed.store(true, Ordering::Relaxed);
    let two = start(
        writer.clone(),
        PoolRecord {
            seq: 4,
            ..first.clone()
        },
    );
    // A handle writes a class one write at a time, so the second publish
    // does not reach its list while the first is under way: the two never
    // hold the cache at once. Ten periods bound a wait for what must not
    // happen.
    let mut preparing = preparing;
    assert!(
        tokio::time::timeout(LIMITS.period * 10, &mut preparing)
            .await
            .is_err(),
        "the second publish began while the first was under way"
    );
    first_release.send(()).expect("release the first record");
    tokio::task::spawn_blocking(move || one.join().expect("the first thread"))
        .await
        .expect("join the first publisher")
        .expect("the first write finishes")
        .expect("the first write succeeds");
    tokio::time::timeout(WAIT, second_paused)
        .await
        .expect("the second record starts")
        .expect("the second record is paused");
    operator
        .withdraw(Withdrawal {
            class: MODEL.into(),
            pool: first.name.clone(),
            epoch: 1,
            seq: 3,
        })
        .await
        .expect("the withdrawal between the second list and record");
    assert!(none_expire(&lists(&client, &tables, &first.name).await));
    second_release.send(()).expect("release the second record");
    let result = tokio::task::spawn_blocking(move || two.join().expect("the second thread"))
        .await
        .expect("join the second publisher")
        .expect("the second write finishes");
    assert!(
        result.is_ok(),
        "no write checks or repairs a list: {result:?}"
    );
    assert_eq!(failed.load(Ordering::Relaxed), 0);

    let latest = PoolRecord {
        seq: 5,
        presence: Presence::ScaledToZero,
        ..first
    };
    writer
        .publish(latest.clone())
        .await
        .expect("the next publish succeeds");
    assert_eq!(snapshot(&mut watch(&operator, None)).await.0, [latest]);
    let now = lists(&client, &tables, "model-x-01").await;
    assert!(
        none_expire(&now),
        "a successful publish after a failed keep must clear its list's TTL, including a cache entry restored by an overlapping publish: {now:?}"
    );
}

/// A list expires only when its pool's record is stored at a higher
/// `models_version`, which only the pool's hub raises: a heartbeat, an
/// operator's command at a new epoch that copies the record, a record at
/// a lower version, and a withdrawal expire nothing.
#[tokio::test]
async fn a_list_expires_only_past_its_models_version() {
    let client = client();
    let tables = tables(&client).await;
    let directory = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let expiring = || async {
        lists(&client, &tables, "model-x-01")
            .await
            .into_iter()
            .filter(|list| list.1.is_some())
            .count()
    };
    let first = record("model-x-01", 1, 1, &["qwen-32b", "llama-70b"]);
    directory.publish(first.clone()).await.expect("a record");
    directory
        .publish(PoolRecord {
            seq: 2,
            ..first.clone()
        })
        .await
        .expect("a heartbeat");
    assert_eq!(expiring().await, 0, "a heartbeat");
    // A command copies the record at a new epoch, models and all.
    let command = PoolRecord {
        epoch: 7,
        seq: 1,
        presence: Presence::ScaledToZero,
        ..first.clone()
    };
    directory.publish(command.clone()).await.expect("a command");
    assert_eq!(expiring().await, 0, "a command at the same models_version");
    // A newer write whose models_version is lower names its own list, and
    // moves the pool past none.
    let lower = PoolRecord {
        epoch: 8,
        seq: 1,
        models_version: first.models_version - 1,
        ..record("model-x-01", 8, 1, &["qwen-32b"])
    };
    directory
        .publish(lower)
        .await
        .expect("a lower models_version");
    assert_eq!(expiring().await, 0, "a record at a lower models_version");
    let higher = record("model-x-01", 9, 1, &["qwen-32b"]);
    assert!(higher.models_version > first.models_version);
    directory
        .publish(higher)
        .await
        .expect("a higher models_version");
    assert_eq!(expiring().await, 1, "the list the record replaced expires");
    directory
        .withdraw(Withdrawal {
            class: MODEL.into(),
            pool: "model-x-01".into(),
            epoch: 9,
            seq: 2,
        })
        .await
        .expect("withdrawn");
    assert_eq!(expiring().await, 1, "a withdrawal");
}

#[tokio::test]
async fn a_same_region_command_keeps_its_list_after_delayed_expiry() {
    use std::sync::{Mutex, mpsc};

    use aws_sdk_dynamodb::{
        config::{ConfigBag, Intercept, interceptors::BeforeSerializationInterceptorContextRef},
        operation::update_item::UpdateItemInput,
    };
    use gdl_pools::PoolCommand;
    use tokio::sync::oneshot;

    #[derive(Debug)]
    struct PauseExpiry {
        began: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl Intercept for PauseExpiry {
        fn name(&self) -> &'static str {
            "PauseExpiry"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if context.input().downcast_ref::<UpdateItemInput>().is_some()
                && let Some(began) = self.began.lock().expect("the signal lock").take()
            {
                began.send(()).expect("the test waits for expiry");
                self.release
                    .lock()
                    .expect("the release lock")
                    .recv_timeout(WAIT)
                    .expect("the test releases expiry");
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let operator = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    operator
        .publish(first.clone())
        .await
        .expect("the first record");
    let last = gdl_pools::last_record(&operator, MODEL, &first.name)
        .await
        .expect("the command's snapshot")
        .expect("the pool");
    let command = PoolCommand::ScaleToZero.write(&last, std::time::SystemTime::now());
    let gdl_pools::CommandWrite::Record(command_record) = command.clone() else {
        panic!("a scale-to-zero record");
    };

    let (began, paused) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let config = own_pool(&client)
        .interceptor(PauseExpiry {
            began: Mutex::new(Some(began)),
            release: Mutex::new(released),
        })
        .build();
    let writer = DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
    let publishing = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the publisher runtime")
            .block_on(async move {
                tokio::time::timeout(
                    WAIT,
                    writer.publish(record("model-x-01", 1, 2, &["qwen-32b", "llama-70b"])),
                )
                .await
            })
    });
    tokio::time::timeout(WAIT, paused)
        .await
        .expect("expiry begins within the bound")
        .expect("the higher model version is stored");

    // A fresh command handle rewrites the list, clearing any earlier TTL.
    let command_handle = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    command
        .to(&command_handle)
        .await
        .expect("the same-region command");
    assert!(
        lists(&client, &tables, &last.name)
            .await
            .iter()
            .all(|(_, expiry)| expiry.is_none()),
        "the command has put its list without TTL"
    );
    release.send(()).expect("release expiry");
    tokio::task::spawn_blocking(move || publishing.join().expect("the publisher thread"))
        .await
        .expect("join the publisher")
        .expect("the publish finishes within the bound")
        .expect("the publish succeeds");

    let reader = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    assert_eq!(
        snapshot(&mut watch(&reader, None)).await.0,
        [*command_record]
    );
    let (list, expiry) = named(&client, &tables, &last.name).await;
    assert_eq!(
        expiry,
        None,
        "a delayed expiry must not mark the list named by the live same-region command: {list} in {:?}",
        lists(&client, &tables, &last.name).await
    );
}

#[tokio::test]
async fn a_new_snapshot_keeps_first_sync_after_the_live_pollers_backoff() {
    use aws_sdk_dynamodb::config::{
        ConfigBag, Intercept, RuntimeComponents, interceptors::BeforeTransmitInterceptorContextMut,
    };
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct Queries {
        count: AtomicUsize,
        began: mpsc::UnboundedSender<usize>,
    }

    impl Intercept for Queries {
        fn name(&self) -> &'static str {
            "Queries"
        }

        fn modify_before_transmit(
            &self,
            context: &mut BeforeTransmitInterceptorContextMut<'_>,
            _runtime: &RuntimeComponents,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if context.request().headers().get("x-amz-target") == Some("DynamoDB_20120810.Query") {
                let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
                let _ = self.began.send(count);
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let writer = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    writer
        .publish(first.clone())
        .await
        .expect("the first record");
    let (began, mut queries) = mpsc::unbounded_channel();
    let config = own_pool(&client)
        .interceptor(Queries {
            count: AtomicUsize::new(0),
            began,
        })
        .build();
    let reader = DynamoDirectory::with_limits(
        Client::from_conf(config),
        tables.clone(),
        DynamoLimits {
            first_sync: Duration::from_millis(200),
            period: Duration::from_secs(60),
            ..LIMITS
        },
    );
    let mut live = watch(&reader, None);
    let (_, token) = snapshot(&mut live).await;

    let second = record("model-x-01", 1, 2, &["qwen-32b", "llama-70b"]);
    let (record_item, _) = items(&client, second).await;
    put_item(&client, &tables.records, record_item).await;
    // A resume kicks the live poller but need not wait for the missing list.
    let mut resumed = watch(&reader, Some(token));
    assert!(matches!(
        event(&mut resumed).await,
        PoolEvent::Synced {
            snapshot: false,
            ..
        }
    ));
    drop(resumed);

    // Observe seven scans so the next retry exceeds a new watch's bound.
    // This takes a few seconds because the live poller backs off normally.
    tokio::time::timeout(WAIT, async {
        while queries.recv().await.expect("the live poller") < 7 {}
    })
    .await
    .expect("the missing-list retries advance");

    let mut fresh = watch(&reader, None);
    let result = tokio::time::timeout(Duration::from_secs(1), fresh.next()).await;
    assert!(
        matches!(result, Ok(Some(Err(DirectoryError::Unavailable(_))))),
        "a new snapshot's 200 ms first_sync must not inherit the live poller's seconds-long missing-list backoff: {result:?}"
    );
}

#[tokio::test]
async fn a_cached_command_handle_keeps_the_list_it_names_again() {
    use gdl_pools::{CommandWrite, PoolCommand};

    let client = client();
    let tables = tables(&client).await;
    let hub = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let operator = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    hub.publish(first.clone()).await.expect("the first record");

    // A command handle can serve more than one request for this pool.
    PoolCommand::ScaleToZero
        .write(&first, std::time::SystemTime::now())
        .to(&operator)
        .await
        .expect("the first command");
    let last = gdl_pools::last_record(&operator, MODEL, &first.name)
        .await
        .expect("the command's snapshot")
        .expect("the pool");
    let command = PoolCommand::ScaleToZero.write(&last, std::time::SystemTime::now());
    let CommandWrite::Record(command_record) = command.clone() else {
        panic!("a scale-to-zero record");
    };

    hub.publish(PoolRecord {
        epoch: last.epoch,
        seq: last.seq + 1,
        ..record("model-x-01", 1, 2, &["qwen-32b", "llama-70b"])
    })
    .await
    .expect("the hub advances the model list after the command reads it");
    assert_eq!(
        lists(&client, &tables, &first.name)
            .await
            .iter()
            .filter(|(_, expiry)| expiry.is_some())
            .count(),
        1,
        "the earlier list is marked to expire"
    );

    command.to(&operator).await.expect("the newer command");
    let reader = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    assert_eq!(
        snapshot(&mut watch(&reader, None)).await.0,
        [*command_record],
        "the command's record names the earlier list"
    );
    let (list, expiry) = named(&client, &tables, &first.name).await;
    assert_eq!(
        expiry,
        None,
        "a successful same-region command must keep its named list without TTL, including through a cached handle: {list} in {:?}",
        lists(&client, &tables, &first.name).await
    );
}

#[tokio::test]
async fn a_fresh_command_keeps_its_list_when_another_put_overtakes_it() {
    use std::sync::{Mutex, mpsc};

    use aws_sdk_dynamodb::{
        config::{ConfigBag, Intercept, interceptors::BeforeSerializationInterceptorContextRef},
        operation::put_item::PutItemInput,
    };
    use gdl_pools::{CommandWrite, PoolCommand};
    use tokio::sync::oneshot;

    #[derive(Debug)]
    struct PauseRecord {
        began: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl Intercept for PauseRecord {
        fn name(&self) -> &'static str {
            "PauseRecord"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let record = context
                .input()
                .downcast_ref::<PutItemInput>()
                .is_some_and(|put| put.condition_expression().is_some());
            if record && let Some(began) = self.began.lock().expect("the signal lock").take() {
                began.send(()).expect("the test waits for the record");
                self.release
                    .lock()
                    .expect("the release lock")
                    .recv_timeout(WAIT)
                    .expect("the test releases the record");
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS)
        .publish(first.clone())
        .await
        .expect("the first record");
    let command = PoolCommand::ScaleToZero.write(&first, std::time::SystemTime::now());
    let CommandWrite::Record(command_record) = command.clone() else {
        panic!("a scale-to-zero record");
    };
    let (began, paused) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let config = own_pool(&client)
        .interceptor(PauseRecord {
            began: Mutex::new(Some(began)),
            release: Mutex::new(released),
        })
        .build();
    let operator = DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
    let publishing = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the command runtime")
            .block_on(async move { tokio::time::timeout(WAIT, command.to(&operator)).await })
    });
    tokio::time::timeout(WAIT, paused)
        .await
        .expect("the command begins within the bound")
        .expect("the command has put its list");

    // A new hub handle puts the same list, then moves past it.
    let hub = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    hub.publish(PoolRecord {
        epoch: 2,
        seq: 1,
        ..first.clone()
    })
    .await
    .expect("a new hub handle puts the list again");
    hub.publish(PoolRecord {
        epoch: 2,
        seq: 2,
        ..record("model-x-01", 1, 2, &["qwen-32b", "llama-70b"])
    })
    .await
    .expect("the hub stores a higher models_version");
    assert_eq!(
        lists(&client, &tables, &first.name)
            .await
            .iter()
            .filter(|(_, expiry)| expiry.is_some())
            .count(),
        1,
        "the put that overtook the command is marked to expire"
    );
    release.send(()).expect("release the command record");
    tokio::task::spawn_blocking(move || publishing.join().expect("the command thread"))
        .await
        .expect("join the command")
        .expect("the command finishes within the bound")
        .expect("the command is stored");

    let reader = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    assert_eq!(
        snapshot(&mut watch(&reader, None)).await.0,
        [*command_record],
        "the live command names the expiring list"
    );
    let (list, expiry) = named(&client, &tables, &first.name).await;
    assert_eq!(
        expiry,
        None,
        "even a fresh same-region command must keep its named list when another put overtakes its list put: {list} in {:?}",
        lists(&client, &tables, &first.name).await
    );
}

#[tokio::test]
async fn a_snapshot_keeps_its_deadline_during_a_late_stalled_scan() {
    use std::sync::{Arc, atomic::AtomicBool};

    use aws_sdk_dynamodb::config::{
        ConfigBag, Intercept, RuntimeComponents, interceptors::BeforeTransmitInterceptorContextMut,
    };
    use tokio::{sync::Notify, time::Instant};

    #[derive(Debug)]
    struct StallLateScan {
        armed: Arc<AtomicBool>,
        endpoint: String,
        began: Arc<Notify>,
        // Each query that is not stalled.
        queried: Arc<Notify>,
    }

    impl Intercept for StallLateScan {
        fn name(&self) -> &'static str {
            "StallLateScan"
        }

        fn modify_before_transmit(
            &self,
            context: &mut BeforeTransmitInterceptorContextMut<'_>,
            _runtime: &RuntimeComponents,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if context.request().headers().get("x-amz-target") == Some("DynamoDB_20120810.Query") {
                if self.armed.load(Ordering::Relaxed) {
                    context.request_mut().set_uri(self.endpoint.as_str())?;
                    self.began.notify_one();
                } else {
                    self.queried.notify_one();
                }
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let (item, _) = items(&client, record("model-x-01", 1, 1, &["qwen-32b"])).await;
    put_item(&client, &tables.records, item).await;
    let stalled = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback listener");
    let began = Arc::new(Notify::new());
    let armed = Arc::new(AtomicBool::new(false));
    let queried = Arc::new(Notify::new());
    let config = own_pool(&client)
        .interceptor(StallLateScan {
            armed: Arc::clone(&armed),
            endpoint: format!(
                "http://{}",
                stalled.local_addr().expect("the listener's port")
            ),
            began: Arc::clone(&began),
            queried: Arc::clone(&queried),
        })
        .build();
    let first_sync = Duration::from_secs(1);
    let reader = DynamoDirectory::with_limits(
        Client::from_conf(config),
        tables,
        DynamoLimits {
            first_sync,
            period: Duration::from_secs(60),
            ..LIMITS
        },
    );
    let mut fresh = watch(&reader, None);
    let started = Instant::now();
    let waiting = fresh.next();
    tokio::pin!(waiting);
    // The first watch waits for its list while the poller backs off. At
    // 600 ms, queries stall, and a second watch starts a scan at once: a
    // scan that begins late in the first watch's wait.
    // Observed first: the watch's own scan ran, found the list missing,
    // and left the snapshot waiting.
    tokio::select! {
        result = &mut waiting => panic!("the watch ended before its first scan: {result:?}"),
        result = tokio::time::timeout(WAIT, queried.notified()) => result.expect("the watch's first scan"),
    }
    tokio::select! {
        result = &mut waiting => panic!("the watch ended before the late query stalled: {result:?}"),
        () = tokio::time::sleep_until(started + Duration::from_millis(600)) => {}
    }
    armed.store(true, Ordering::Relaxed);
    let mut second = watch(&reader, None);
    let kicking = second.next();
    tokio::pin!(kicking);
    tokio::select! {
        result = &mut waiting => panic!("the watch ended before the late query stalled: {result:?}"),
        result = &mut kicking => panic!("the second watch ended before the late query stalled: {result:?}"),
        result = tokio::time::timeout(WAIT, began.notified()) => result.expect("a late scan starts"),
    }
    let late = started.elapsed();
    assert!(
        late >= Duration::from_millis(600) && late < first_sync,
        "the stalled scan began at {late:?}, not late in the snapshot's wait"
    );
    let (_connection, _) = tokio::time::timeout(WAIT, stalled.accept())
        .await
        .expect("the late scan connects")
        .expect("the scan's connection");
    // The 400 ms margin exceeds local query latency; a reset adds at least 600 ms.
    let result = tokio::time::timeout_at(
        started + first_sync + Duration::from_millis(400),
        &mut waiting,
    )
    .await;
    assert!(
        matches!(result, Ok(Some(Err(DirectoryError::Unavailable(_))))),
        "a snapshot's 1 s first_sync must include a late scan instead of granting it another 1 s: elapsed={:?}, result={result:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_delayed_second_list_put_cannot_restore_an_expiry_token() {
    use std::sync::{Mutex, mpsc};

    use aws_sdk_dynamodb::{
        config::{
            ConfigBag, Intercept, RuntimeComponents,
            interceptors::{
                BeforeSerializationInterceptorContextRef, FinalizerInterceptorContextRef,
            },
        },
        operation::{put_item::PutItemInput, update_item::UpdateItemInput},
    };
    use gdl_pools::{CommandWrite, PoolCommand};
    use tokio::sync::oneshot;

    // The first writer pauses after its record is stored, before its
    // publish returns: the last point at which a late call of it could
    // land. A write puts no list after its record, so nothing it does
    // there can touch the list a later command names.
    #[derive(Debug)]
    enum At {
        AfterRecord,
        Expiry,
    }

    #[derive(Debug)]
    struct Pause {
        at: At,
        // Whether the call under way is a records-table put: the input is
        // gone by the time the call ends.
        record: std::sync::atomic::AtomicBool,
        began: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl Pause {
        fn hold(&self) {
            if let Some(began) = self.began.lock().expect("the signal lock").take() {
                began.send(()).expect("the test waits for the write");
                self.release
                    .lock()
                    .expect("the release lock")
                    .recv_timeout(WAIT)
                    .expect("the test releases the write");
            }
        }
    }

    impl Intercept for Pause {
        fn name(&self) -> &'static str {
            "Pause"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let input = context.input();
            self.record.store(
                input
                    .downcast_ref::<PutItemInput>()
                    .is_some_and(|put| put.item().is_some_and(|item| item.contains_key("pool"))),
                Ordering::Relaxed,
            );
            if matches!(self.at, At::Expiry) && input.downcast_ref::<UpdateItemInput>().is_some() {
                self.hold();
            }
            Ok(())
        }

        fn read_after_execution(
            &self,
            _context: &FinalizerInterceptorContextRef<'_>,
            _runtime: &RuntimeComponents,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if matches!(self.at, At::AfterRecord) && self.record.load(Ordering::Relaxed) {
                self.hold();
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let pausing = |at| {
        let (began, paused) = oneshot::channel();
        let (release, released) = mpsc::channel();
        let config = own_pool(&client)
            .interceptor(Pause {
                at,
                record: std::sync::atomic::AtomicBool::new(false),
                began: Mutex::new(Some(began)),
                release: Mutex::new(released),
            })
            .build();
        (
            DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS),
            paused,
            release,
        )
    };
    let start = |directory: DynamoDirectory, published: PoolRecord| {
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the publisher runtime")
                .block_on(
                    async move { tokio::time::timeout(WAIT, directory.publish(published)).await },
                )
        })
    };

    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    let (initial, second_list_paused, second_list_release) = pausing(At::AfterRecord);
    let initial_write = start(initial, first.clone());
    tokio::time::timeout(WAIT, second_list_paused)
        .await
        .expect("the first writer reaches its pause within the bound")
        .expect("the first record is stored");

    let operator = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let last = gdl_pools::last_record(&operator, MODEL, &first.name)
        .await
        .expect("the command's snapshot")
        .expect("the pool");
    let command = PoolCommand::ScaleToZero.write(&last, std::time::SystemTime::now());
    let CommandWrite::Record(command_record) = command.clone() else {
        panic!("a scale-to-zero record");
    };

    let (hub, expiry_paused, expiry_release) = pausing(At::Expiry);
    let higher_write = start(
        hub,
        PoolRecord {
            epoch: 2,
            seq: 1,
            ..record("model-x-01", 1, 2, &["qwen-32b", "llama-70b"])
        },
    );
    tokio::time::timeout(WAIT, expiry_paused)
        .await
        .expect("expiry begins within the bound")
        .expect("the higher model version is stored");

    command.to(&operator).await.expect("the command is stored");
    assert!(
        lists(&client, &tables, &first.name)
            .await
            .iter()
            .all(|(_, expiry)| expiry.is_none()),
        "the command has put its list without TTL"
    );

    // The first writer finishes after the command.
    second_list_release
        .send(())
        .expect("release the second put");
    tokio::task::spawn_blocking(move || initial_write.join().expect("the initial thread"))
        .await
        .expect("join the initial publisher")
        .expect("the initial publish finishes within the bound")
        .expect("the initial publish succeeds");

    // The expiry names the list of the record it replaced, put before the
    // command put its own.
    expiry_release.send(()).expect("release expiry");
    tokio::task::spawn_blocking(move || higher_write.join().expect("the hub thread"))
        .await
        .expect("join the hub publisher")
        .expect("the hub publish finishes within the bound")
        .expect("the hub publish succeeds");

    let reader = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    assert_eq!(
        snapshot(&mut watch(&reader, None)).await.0,
        [*command_record],
        "the command remains the pool's live record"
    );
    let (list, expiry) = named(&client, &tables, &first.name).await;
    assert_eq!(
        expiry,
        None,
        "a delayed second list put must not restore a token that lets delayed expiry delete the successful command's live list: {list} in {:?}",
        lists(&client, &tables, &first.name).await
    );
}

#[tokio::test]
async fn a_command_keeps_an_overtaking_put_after_its_delayed_expiry() {
    use std::sync::{Mutex, mpsc};

    use aws_sdk_dynamodb::{
        config::{ConfigBag, Intercept, interceptors::BeforeSerializationInterceptorContextRef},
        operation::{put_item::PutItemInput, update_item::UpdateItemInput},
    };
    use gdl_pools::{CommandWrite, PoolCommand};
    use tokio::sync::oneshot;

    #[derive(Debug)]
    enum At {
        Record,
        Expiry,
    }

    #[derive(Debug)]
    struct Pause {
        at: At,
        began: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl Intercept for Pause {
        fn name(&self) -> &'static str {
            "Pause"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let pause = match self.at {
                At::Record => context
                    .input()
                    .downcast_ref::<PutItemInput>()
                    .is_some_and(|put| put.item().is_some_and(|item| item.contains_key("pool"))),
                At::Expiry => context.input().downcast_ref::<UpdateItemInput>().is_some(),
            };
            if pause && let Some(began) = self.began.lock().expect("the signal lock").take() {
                began.send(()).expect("the test waits for the write");
                self.release
                    .lock()
                    .expect("the release lock")
                    .recv_timeout(WAIT)
                    .expect("the test releases the write");
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let pausing = |at| {
        let (began, paused) = oneshot::channel();
        let (release, released) = mpsc::channel();
        let config = own_pool(&client)
            .interceptor(Pause {
                at,
                began: Mutex::new(Some(began)),
                release: Mutex::new(released),
            })
            .build();
        (
            DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS),
            paused,
            release,
        )
    };
    let start = |directory: DynamoDirectory, published: PoolRecord| {
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the publisher runtime")
                .block_on(
                    async move { tokio::time::timeout(WAIT, directory.publish(published)).await },
                )
        })
    };

    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS)
        .publish(first.clone())
        .await
        .expect("the first record");
    let command = PoolCommand::ScaleToZero.write(&first, std::time::SystemTime::now());
    let CommandWrite::Record(command_record) = command else {
        panic!("a scale-to-zero record");
    };
    let (operator, record_paused, record_release) = pausing(At::Record);
    let commanding = start(operator, (*command_record).clone());
    tokio::time::timeout(WAIT, record_paused)
        .await
        .expect("the command begins within the bound")
        .expect("the command has put its list");

    let (hub, expiry_paused, expiry_release) = pausing(At::Expiry);
    hub.publish(PoolRecord {
        epoch: 2,
        seq: 1,
        ..first.clone()
    })
    .await
    .expect("a restarted hub puts the same list with its own token");
    let advancing = start(
        hub,
        PoolRecord {
            epoch: 2,
            seq: 2,
            ..record("model-x-01", 1, 2, &["qwen-32b", "llama-70b"])
        },
    );
    tokio::time::timeout(WAIT, expiry_paused)
        .await
        .expect("expiry begins within the bound")
        .expect("the higher model version is stored");
    assert!(
        lists(&client, &tables, &first.name)
            .await
            .iter()
            .all(|(_, expiry)| expiry.is_none()),
        "expiry has not landed yet"
    );

    record_release.send(()).expect("release the command record");
    tokio::task::spawn_blocking(move || commanding.join().expect("the command thread"))
        .await
        .expect("join the command")
        .expect("the command finishes within the bound")
        .expect("the command succeeds");
    let reader = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    assert_eq!(
        snapshot(&mut watch(&reader, None)).await.0,
        [*command_record],
        "the successful command names the earlier list"
    );

    expiry_release.send(()).expect("release the hub's expiry");
    tokio::task::spawn_blocking(move || advancing.join().expect("the hub thread"))
        .await
        .expect("join the hub")
        .expect("the hub finishes within the bound")
        .expect("the hub write succeeds");
    let (list, expiry) = named(&client, &tables, &first.name).await;
    assert_eq!(
        expiry,
        None,
        "a command must keep its live list {list} when another put overtakes its first put and that put's expiry lands after its second put: {:?}",
        lists(&client, &tables, &first.name).await
    );
}

#[tokio::test]
async fn a_command_reinstalls_its_list_after_ttl_removes_an_overtaking_put() {
    use std::sync::{Mutex, mpsc};

    use aws_sdk_dynamodb::{
        config::{ConfigBag, Intercept, interceptors::BeforeSerializationInterceptorContextRef},
        operation::put_item::PutItemInput,
    };
    use gdl_pools::{CommandWrite, PoolCommand};
    use tokio::sync::oneshot;

    #[derive(Debug)]
    struct PauseRecord {
        began: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl Intercept for PauseRecord {
        fn name(&self) -> &'static str {
            "PauseRecord"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _config: &mut ConfigBag,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let record = context
                .input()
                .downcast_ref::<PutItemInput>()
                .is_some_and(|put| put.item().is_some_and(|item| item.contains_key("pool")));
            if record && let Some(began) = self.began.lock().expect("the signal lock").take() {
                began.send(()).expect("the test waits for the record");
                self.release
                    .lock()
                    .expect("the release lock")
                    .recv_timeout(WAIT)
                    .expect("the test releases the record");
            }
            Ok(())
        }
    }

    let client = client();
    let tables = tables(&client).await;
    let first = record("model-x-01", 1, 1, &["qwen-32b"]);
    DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS)
        .publish(first.clone())
        .await
        .expect("the first record");
    let command = PoolCommand::ScaleToZero.write(&first, std::time::SystemTime::now());
    let CommandWrite::Record(command_record) = command.clone() else {
        panic!("a scale-to-zero record");
    };
    let (began, paused) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let config = own_pool(&client)
        .interceptor(PauseRecord {
            began: Mutex::new(Some(began)),
            release: Mutex::new(released),
        })
        .build();
    let operator = DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
    let publishing = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the command runtime")
            .block_on(async move { tokio::time::timeout(WAIT, command.to(&operator)).await })
    });
    tokio::time::timeout(WAIT, paused)
        .await
        .expect("the command begins within the bound")
        .expect("the command has put its list");

    let hub = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    hub.publish(PoolRecord {
        epoch: 2,
        seq: 1,
        ..first.clone()
    })
    .await
    .expect("a restarted hub puts the same list with its own token");
    hub.publish(PoolRecord {
        epoch: 2,
        seq: 2,
        ..record("model-x-01", 1, 2, &["qwen-32b", "llama-70b"])
    })
    .await
    .expect("the hub stores a higher models_version and expires its prior list");
    let expiring = lists(&client, &tables, &first.name)
        .await
        .into_iter()
        .filter(|(_, expiry)| expiry.is_some())
        .collect::<Vec<_>>();
    assert_eq!(expiring.len(), 1, "only the superseded list can expire");
    let (list, expiry) = &expiring[0];

    // Model TTL deletion after the superseded list's one-hour retention.
    client
        .delete_item()
        .table_name(&tables.lists)
        .key(
            "owner",
            AttributeValue::S(format!("{MODEL}/{}", first.name)),
        )
        .key("list", AttributeValue::S(list.clone()))
        .condition_expression("#expires = :expires")
        .expression_attribute_names("#expires", "expires_at")
        .expression_attribute_values(
            ":expires",
            AttributeValue::N(expiry.expect("TTL").to_string()),
        )
        .send()
        .await
        .expect("TTL removes only the list marked to expire");
    assert_eq!(
        named(&client, &tables, &first.name).await.1,
        None,
        "the higher model version's list stays"
    );

    release.send(()).expect("release the command record");
    tokio::task::spawn_blocking(move || publishing.join().expect("the command thread"))
        .await
        .expect("join the command")
        .expect("the command finishes within the bound")
        .expect("the command succeeds");
    let stored = client
        .get_item()
        .table_name(&tables.records)
        .key("class", AttributeValue::S(MODEL.into()))
        .key("pool", AttributeValue::S(first.name.clone()))
        .consistent_read(true)
        .send()
        .await
        .expect("the stored command");
    let stored = stored.item().expect("the command's record exists");
    assert_eq!(
        stored["epoch"],
        AttributeValue::N(command_record.epoch.to_string())
    );
    let (named, expiry) = named(&client, &tables, &first.name).await;
    assert_eq!(
        expiry, None,
        "a successful command must name a list that is there and not set to expire, when TTL removed the put that overtook its first put ({list} was removed): {named}"
    );
}
