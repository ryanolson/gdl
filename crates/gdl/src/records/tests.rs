use std::collections::HashMap;
use std::sync::Mutex;

use futures::FutureExt as _;
use tokio::sync::{mpsc, oneshot};

use super::*;

/// A link to a table in memory that answers as the hub's does, and keeps
/// how many operations each request carried. A request applies when its
/// future runs, as a request reaches the hub only once it arrives.
#[derive(Default)]
struct Memory {
    table: Arc<Mutex<Stored>>,
    asks: Mutex<Vec<usize>>,
    /// Refuse every request, as a hub that cannot be reached.
    down: AtomicBool,
    /// Hold the next request until the sender is used or dropped.
    hold: Mutex<Option<oneshot::Receiver<()>>>,
    /// How long each request takes to arrive.
    delay: Mutex<Duration>,
}

/// The records by key, and the last version given.
type Stored = (HashMap<Vec<u8>, Record>, u64);

/// One operation, owned, as it travels.
enum Owned {
    Get(Vec<u8>),
    Check(Vec<u8>, u64),
    Set(Vec<u8>, Vec<u8>, Expected),
}

impl Memory {
    fn asks(&self) -> Vec<usize> {
        self.asks.lock().expect("lock").clone()
    }

    /// Hold the next request on its way until the returned sender is used.
    fn hold_next(&self) -> oneshot::Sender<()> {
        let (release, held) = oneshot::channel();
        *self.hold.lock().expect("lock") = Some(held);
        release
    }
}

fn apply(table: &mut Stored, op: Owned) -> RecordAnswer {
    let (table, version) = table;
    match op {
        Owned::Get(key) => match table.get(&key) {
            Some(record) => RecordAnswer::Found(record.clone()),
            None => RecordAnswer::Missing,
        },
        Owned::Check(key, held) => match table.get(&key) {
            Some(record) if record.version == held => RecordAnswer::Unchanged,
            Some(record) => RecordAnswer::Newer(record.clone()),
            None => RecordAnswer::Gone,
        },
        Owned::Set(key, value, expected) => {
            let current = table.get(&key);
            let matches = match expected {
                Expected::Replace => true,
                Expected::Absent => current.is_none(),
                Expected::Version(v) => current.is_some_and(|r| r.version == v),
            };
            if matches {
                *version += 1;
                table.insert(
                    key,
                    Record {
                        value,
                        version: *version,
                    },
                );
                RecordAnswer::Applied(*version)
            } else {
                RecordAnswer::Refused(current.cloned())
            }
        }
    }
}

impl RecordLink for Memory {
    fn ask(
        &self,
        ops: &mut dyn Iterator<Item = RecordOp<'_>>,
    ) -> BoxFuture<'static, Result<RecordReply>> {
        let ops: Vec<Owned> = ops
            .map(|op| match op {
                RecordOp::Get { key } => Owned::Get(key.to_vec()),
                RecordOp::Check { key, version } => Owned::Check(key.to_vec(), version),
                RecordOp::Set {
                    key,
                    value,
                    expected,
                } => Owned::Set(key.to_vec(), value.to_vec(), expected),
            })
            .collect();
        self.asks.lock().expect("lock").push(ops.len());
        let down = self.down.load(Ordering::Relaxed);
        let held = self.hold.lock().expect("lock").take();
        let delay = *self.delay.lock().expect("lock");
        let table = Arc::clone(&self.table);
        async move {
            if let Some(held) = held {
                let _ = held.await;
            }
            tokio::time::sleep(delay).await;
            if down {
                return Err(Error::new(ErrorKind::Transport, "down"));
            }
            let mut table = table.lock().expect("lock");
            Ok(RecordReply {
                answers: ops.into_iter().map(|op| apply(&mut table, op)).collect(),
                eviction_age: Some(Duration::from_secs(42)),
            })
        }
        .boxed()
    }
}

/// A table named `name` of a service linked to `link`, whose answers
/// arrive on the receiver as (key, answer).
fn linked(link: &Arc<Memory>, name: &str) -> (CellRecords, Answers) {
    let records = HubRecords::linked(Arc::clone(link) as Arc<dyn RecordLink>);
    answering(&records, name)
}

type Answers = mpsc::UnboundedReceiver<(Vec<u8>, Written<'static>, Option<Record>)>;

/// The table `name` of `records`, whose answers arrive on the receiver,
/// a refusal's record beside it.
fn answering(records: &HubRecords, name: &str) -> (CellRecords, Answers) {
    let table = records.table(name).expect("a table");
    let (sent, answers) = mpsc::unbounded_channel();
    assert!(table.on_answer(move |key, written| {
        let (owned, current) = match written {
            Written::Refused { current } => (Written::Refused { current: None }, current.cloned()),
            Written::Applied { version } => (Written::Applied { version }, None),
            Written::Unanswered => (Written::Unanswered, None),
        };
        let _ = sent.send((key.to_vec(), owned, current));
    }));
    (table, answers)
}

async fn answer(answers: &mut Answers) -> (Vec<u8>, Written<'static>, Option<Record>) {
    tokio::time::timeout(Duration::from_secs(5), answers.recv())
        .await
        .expect("an answer in time")
        .expect("the answers go on")
}

/// The count of `table`'s writes that ended with `outcome`, scraped. Read
/// from the text form, since the parsed form's accessors change with
/// prometheus's features.
fn scraped(registry: &prometheus::Registry, table: &str, outcome: &str) -> Option<f64> {
    let text = prometheus::TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encoded");
    let (table, outcome) = (
        format!("table=\"{table}\""),
        format!("outcome=\"{outcome}\""),
    );
    text.lines()
        .filter(|line| line.starts_with("cell_record_writes_total{"))
        .find(|line| line.contains(&table) && line.contains(&outcome))
        .and_then(|line| line.rsplit(' ').next()?.parse().ok())
}

/// Without a cell's hub there is no table to take: outside a build, in a
/// build of an instance without a hub, and from a service made with
/// `new`. Under a hub, a build takes one, before the hub is linked.
#[tokio::test]
async fn current_says_when_there_is_no_cell_hub() {
    assert_eq!(
        CellRecords::current("fingerprints").expect_err("no table"),
        RecordsUnavailable::Unbound
    );
    let alone = HubRecords::new();
    let taken = alone
        .scope(async { CellRecords::current("fingerprints") })
        .await;
    assert_eq!(taken.expect_err("no table"), RecordsUnavailable::Unbound);
    let error: Error = RecordsUnavailable::Unbound.into();
    assert_eq!(error.kind(), ErrorKind::Placement);
    let hub = HubRecords::under_hub();
    let taken = hub
        .scope(async { CellRecords::current("fingerprints") })
        .await
        .expect("a table under a hub");
    assert_eq!(taken.table(), "fingerprints");
    assert!(!hub.is_bound());
}

/// A table's name is its keys' prefix of one length byte: an empty name,
/// or one longer than 255 bytes, is refused.
#[tokio::test]
async fn a_table_name_is_one_to_255_bytes() {
    let hub = HubRecords::under_hub();
    assert!(matches!(
        hub.table(""),
        Err(RecordsUnavailable::Name { .. })
    ));
    assert!(matches!(
        hub.table(&"x".repeat(256)),
        Err(RecordsUnavailable::Name { .. })
    ));
    assert!(hub.table(&"x".repeat(255)).is_ok());
}

/// A write is answered through the callback; a second write that expects
/// no record is refused with the record the first wrote, and a get and a
/// check see it.
#[tokio::test]
async fn a_refused_write_carries_the_current_record() {
    let link = Arc::new(Memory::default());
    let (records, mut answers) = linked(&link, "fingerprints");
    assert!(records.set(b"s1", b"model-x-01", Expected::Absent));
    assert_eq!(
        answer(&mut answers).await,
        (b"s1".to_vec(), Written::Applied { version: 1 }, None)
    );
    assert!(records.set(b"s1", b"model-x-02", Expected::Absent));
    let first = Record {
        value: b"model-x-01".to_vec(),
        version: 1,
    };
    assert_eq!(
        answer(&mut answers).await,
        (
            b"s1".to_vec(),
            Written::Refused { current: None },
            Some(first.clone())
        )
    );
    assert_eq!(records.get(b"s1").await.expect("answered"), Some(first));
    assert_eq!(records.get(b"s2").await.expect("answered"), None);
    assert_eq!(
        records.check(b"s1", 1).await.expect("answered"),
        Checked::Unchanged
    );
    assert_eq!(
        records.check(b"s2", 1).await.expect("answered"),
        Checked::Gone
    );
    assert_eq!(records.eviction_age(), Some(Duration::from_secs(42)));
    assert!(records.set(b"s1", b"model-x-02", Expected::Version(1)));
    assert_eq!(
        answer(&mut answers).await.1,
        Written::Applied { version: 2 }
    );
    assert_eq!(
        records.check(b"s1", 1).await.expect("answered"),
        Checked::Newer(Record {
            value: b"model-x-02".to_vec(),
            version: 2
        })
    );
}

/// Writes wait up to 5 ms for others and go out in one request.
#[tokio::test(start_paused = true)]
async fn writes_go_out_together_after_5_ms() {
    let link = Arc::new(Memory::default());
    let (records, mut answers) = linked(&link, "t");
    for key in [b"a", b"b", b"c"] {
        assert!(records.set(key, b"v", Expected::Replace));
    }
    tokio::time::sleep(Duration::from_millis(4)).await;
    assert_eq!(link.asks(), Vec::<usize>::new(), "not before 5 ms");
    for _ in 0..3 {
        answer(&mut answers).await;
    }
    assert_eq!(link.asks(), [3], "one request for the three");
}

/// A batch's worth of writes goes out at once, without waiting the 5 ms.
#[tokio::test(start_paused = true)]
async fn a_batch_worth_goes_out_at_once() {
    let link = Arc::new(Memory::default());
    let (records, mut answers) = linked(&link, "t");
    let value = [7u8; 1024];
    let mut sent = 0;
    while sent * (value.len() + 32) < outbox::BATCH_BYTES {
        let key = (sent as u32).to_be_bytes();
        assert!(records.set(&key, &value, Expected::Replace));
        sent += 1;
    }
    let start = tokio::time::Instant::now();
    for _ in 0..sent {
        answer(&mut answers).await;
    }
    assert_eq!(start.elapsed(), Duration::ZERO, "no wait for a full batch");
    assert_eq!(link.asks().iter().sum::<usize>(), sent);
}

/// Before the hub is linked, writes wait in the queue; once it is full, a
/// write is dropped and counted. Once linked, the queued writes go out.
#[tokio::test]
async fn a_full_queue_drops_and_counts() {
    let records = HubRecords::under_hub();
    let registry = prometheus::Registry::new();
    records.register(&registry).expect("registered");
    let (table, mut answers) = answering(&records, "t");
    let value = [0u8; 200];
    let mut queued = 0;
    let mut dropped = 0;
    for n in 0u32..10_000 {
        if table.set(&n.to_be_bytes(), &value, Expected::Replace) {
            queued += 1;
        } else {
            dropped += 1;
        }
    }
    assert!(
        queued > 0 && dropped > 0,
        "{queued} queued, {dropped} dropped"
    );
    assert_eq!(scraped(&registry, "t", "dropped"), Some(dropped as f64));
    let link = Arc::new(Memory::default());
    assert!(records.bind(Arc::clone(&link) as Arc<dyn RecordLink>));
    for _ in 0..queued {
        assert!(matches!(
            answer(&mut answers).await.1,
            Written::Applied { .. }
        ));
    }
    assert_eq!(scraped(&registry, "t", "applied"), Some(queued as f64));
    assert_eq!(link.asks().iter().sum::<usize>(), queued);
}

/// Two tables keep their keys apart, though the hub has one table.
#[tokio::test]
async fn tables_keep_their_keys_apart() {
    let link = Arc::new(Memory::default());
    let records = HubRecords::linked(Arc::clone(&link) as Arc<dyn RecordLink>);
    let (one, mut one_answers) = answering(&records, "one");
    let (two, mut two_answers) = answering(&records, "two");
    assert!(one.set(b"k", b"1", Expected::Absent));
    assert!(two.set(b"k", b"2", Expected::Absent));
    assert!(matches!(
        answer(&mut one_answers).await.1,
        Written::Applied { .. }
    ));
    assert!(matches!(
        answer(&mut two_answers).await.1,
        Written::Applied { .. }
    ));
    assert_eq!(
        one.get(b"k")
            .await
            .expect("answered")
            .expect("a record")
            .value,
        b"1"
    );
    assert_eq!(
        two.get(b"k")
            .await
            .expect("answered")
            .expect("a record")
            .value,
        b"2"
    );
    // A name that is a prefix of another's, with a key that makes up the
    // rest, is still another key.
    let (o, _) = answering(&records, "o");
    assert_eq!(o.get(b"nek").await.expect("answered"), None);
}

/// A hub that cannot be asked answers each write `unanswered`, and a get
/// before the hub is linked fails rather than waits.
#[tokio::test]
async fn a_write_with_no_answer_says_so() {
    let link = Arc::new(Memory::default());
    link.down.store(true, Ordering::Relaxed);
    let (records, mut answers) = linked(&link, "t");
    assert!(records.set(b"k", b"v", Expected::Replace));
    assert_eq!(answer(&mut answers).await.1, Written::Unanswered);
    let unlinked = HubRecords::under_hub().table("t").expect("a table");
    let error = unlinked.get(b"k").await.expect_err("no hub to ask");
    assert_eq!(error.kind(), ErrorKind::Transport);
}

/// A get goes to the hub at once: it waits for no queued write, and for
/// no batch of writes on its way.
#[tokio::test(start_paused = true)]
async fn a_get_bypasses_queued_writes_and_a_batch_in_flight() {
    let link = Arc::new(Memory::default());
    let (records, mut answers) = linked(&link, "t");
    assert!(records.set(b"queued", b"value", Expected::Replace));
    assert_eq!(records.get(b"queued").await.expect("an answer"), None);
    assert_eq!(link.asks(), [1], "only the get has reached the link");

    let release = link.hold_next();
    tokio::time::sleep(Duration::from_millis(6)).await;
    assert_eq!(link.asks(), [1, 1], "the first write is in flight");
    assert!(records.set(b"next", b"value", Expected::Replace));
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(5), records.get(b"queued"))
            .await
            .expect("the read bypasses the pending batch")
            .expect("an answer"),
        None
    );
    assert_eq!(link.asks(), [1, 1, 1], "the second write stays queued");
    assert!(answers.try_recv().is_err(), "neither write is answered");
    release.send(()).expect("the first write is still held");
    assert_eq!(answer(&mut answers).await.0, b"queued");
    assert_eq!(answer(&mut answers).await.0, b"next");
}

/// Writes to one key reach the hub in the order they were queued.
#[tokio::test]
async fn writes_to_one_key_keep_their_order() {
    let link = Arc::new(Memory::default());
    let (records, mut answers) = linked(&link, "t");
    for n in 0u8..50 {
        assert!(records.set(b"k", &[n], Expected::Replace));
    }
    for n in 1..=50 {
        assert_eq!(
            answer(&mut answers).await.1,
            Written::Applied { version: n }
        );
    }
    assert_eq!(
        records
            .get(b"k")
            .await
            .expect("answered")
            .expect("a record")
            .value,
        [49]
    );
}

/// Dropping the last handle sends what is queued, then ends the task.
#[tokio::test(start_paused = true)]
async fn a_dropped_handle_sends_what_is_queued() {
    let link = Arc::new(Memory::default());
    let (records, mut answers) = linked(&link, "t");
    assert!(records.set(b"k", b"v", Expected::Replace));
    drop(records);
    assert!(matches!(
        answer(&mut answers).await.1,
        Written::Applied { .. }
    ));
    assert!(answers.recv().await.is_none(), "the task ended");
}

/// A batch whose answer did not come in time may still reach the hub, so
/// the next batch waits until its request ends: a later write to the key
/// is applied after it, not overwritten by it.
#[tokio::test(start_paused = true)]
async fn a_timed_out_batch_holds_back_the_next() {
    let link = Arc::new(Memory::default());
    let release = link.hold_next();
    let (records, mut answers) = linked(&link, "t");
    assert!(records.set(b"k", b"first", Expected::Replace));
    assert_eq!(answer(&mut answers).await.1, Written::Unanswered);
    assert!(records.set(b"k", b"second", Expected::Replace));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(link.asks(), [1], "the second waits for the first to end");
    release.send(()).expect("held");
    assert_eq!(
        answer(&mut answers).await.1,
        Written::Applied { version: 2 }
    );
    assert_eq!(
        records.get(b"k").await.expect("answered"),
        Some(Record {
            value: b"second".to_vec(),
            version: 2
        })
    );
}

/// One request at a time: while the hub is slow to answer, a full batch
/// waits for the earlier answer, and the writes made meanwhile queue up
/// to the bound rather than drop. It goes out as soon as the answer comes.
#[tokio::test(start_paused = true)]
async fn a_slow_answer_holds_the_next_batch_in_the_queue() {
    let link = Arc::new(Memory::default());
    *link.delay.lock().expect("lock") = Duration::from_millis(100);
    let (records, mut answers) = linked(&link, "t");
    assert!(records.set(b"first", b"v", Expected::Replace));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(link.asks(), [1], "the first batch is on its way");
    let value = [7u8; 1024];
    for n in 0u32..64 {
        assert!(
            records.set(&n.to_be_bytes(), &value, Expected::Replace),
            "queued, not dropped"
        );
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(link.asks(), [1], "one request at a time");
    let start = tokio::time::Instant::now();
    answer(&mut answers).await;
    let answered = start.elapsed();
    for _ in 0..64 {
        answer(&mut answers).await;
    }
    assert_eq!(link.asks(), [1, 64]);
    assert_eq!(
        start.elapsed() - answered,
        Duration::from_millis(100),
        "sent as soon as the first answer came"
    );
}

/// A full batch must not end the following batch's wait for more writes.
#[tokio::test(start_paused = true)]
async fn a_partial_batch_after_a_full_one_waits_for_its_own_deadline() {
    let link = Arc::new(Memory::default());
    let (records, mut answers) = linked(&link, "t");
    let value = [7u8; 1024];
    for n in 0u32..64 {
        assert!(records.set(&n.to_be_bytes(), &value, Expected::Replace));
    }
    for _ in 0..64 {
        answer(&mut answers).await;
    }
    assert_eq!(link.asks(), [64], "the full batch was answered");

    assert!(records.set(b"next", b"v", Expected::Replace));
    tokio::time::sleep(Duration::from_millis(4)).await;
    assert_eq!(
        link.asks(),
        [64],
        "the new partial batch must collect writes for its own 5 ms"
    );
    assert!(matches!(
        answer(&mut answers).await.1,
        Written::Applied { .. }
    ));
    assert_eq!(link.asks(), [64, 1]);
}

/// A batch whose wait expired during a request goes out when it ends.
#[tokio::test(start_paused = true)]
async fn a_partial_batch_does_not_restart_its_deadline_after_an_answer() {
    let link = Arc::new(Memory::default());
    let release = link.hold_next();
    let (records, mut answers) = linked(&link, "t");
    assert!(records.set(b"k", b"first", Expected::Replace));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(link.asks(), [1], "the first request is on its way");

    assert!(records.set(b"k", b"second", Expected::Replace));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(link.asks(), [1], "one request at a time");
    let released = tokio::time::Instant::now();
    release.send(()).expect("the first request is held");
    assert_eq!(
        answer(&mut answers).await.1,
        Written::Applied { version: 1 }
    );
    assert_eq!(
        answer(&mut answers).await.1,
        Written::Applied { version: 2 }
    );
    assert_eq!(
        released.elapsed(),
        Duration::ZERO,
        "the second batch already waited more than 5 ms in the queue"
    );
}
