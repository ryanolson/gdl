//! A handle on a named table of keyed records in the segment's own pool's
//! hub: the hub's record service, read by request and answer, and written
//! in batches. A node or a pool policy takes it once, in
//! [`Build::build`](crate::Build::build), and reads it per request. The
//! runtime binds it to the hub; this crate never calls the hub itself.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{Notify, watch};

use crate::{Error, ErrorKind, Result};
use outbox::Batch;

mod outbox;
mod scrape;

tokio::task_local! {
    /// The record service of the segment whose nodes are being built.
    static BUILDING: HubRecords;
}

/// A handle on one named table of keyed records in the segment's own
/// pool's hub. Cheap to clone; every clone is the same handle, with one
/// write queue and one answer callback.
///
/// Take it once, in [`Build::build`](crate::Build::build), with
/// [`PoolRecords::current`]:
///
/// ```no_run
/// # async fn build() -> gdl::Result<()> {
/// use gdl::{PoolRecords, Checked, Expected};
///
/// let records = PoolRecords::current("fingerprints")?;
/// let found = records.get(b"session-7").await?; // record and version, or none
/// if let Some(record) = &found {
///     let checked = records.check(b"session-7", record.version).await?;
///     # let _ = checked;
/// }
/// records.set(b"session-7", b"model-x-02", Expected::Absent); // does not wait
/// # Ok(())
/// # }
/// ```
///
/// - [`PoolRecords::get`] and [`PoolRecords::check`] each ask the hub at
///   once and wait for its answer: one round trip. Bound the wait with a
///   timeout of your own.
/// - [`PoolRecords::set`] does not wait and does not allocate. It copies
///   the write into a bounded queue, which a task sends to the hub in one
///   request 5 ms after its first write, or as soon as 64 KiB are queued,
///   one request at a time. When the queue
///   is full, the write is dropped and counted
///   (`pool_record_writes_total{outcome="dropped"}`). Each write's answer
///   reaches the callback given to [`PoolRecords::on_answer`].
///
/// Every table of a pool shares the hub's one table: the name keeps their
/// keys apart, but they share its capacity and its least recently used
/// order.
///
/// # The eviction age
///
/// Each answer from the hub carries its table's eviction age, the age of
/// its least recently used record; [`PoolRecords::eviction_age`] gives
/// the last one seen. A reader that caches a record checks it again
/// before a deadline, so that a record in use is touched before the hub
/// evicts it. The age is a time until eviction only once the table is
/// full. Below its capacity (`[pool] keyed_capacity`, 1,000,000 records
/// by default) the hub evicts nothing, and the age is only the age of its
/// oldest record: short soon after the hub starts, or on a small table. So
/// set a cache deadline of half the age, but never less than a floor of
/// your own (10 s, say) or more than a ceiling (5 minutes, say). A
/// deadline that is too short costs only extra checks; one that is too
/// long can let the hub evict a record in use, and the next turn misses.
#[derive(Clone)]
pub struct PoolRecords {
    front: Arc<Front>,
}

impl PoolRecords {
    /// The handle on the table named `table` in the pool's hub of the
    /// segment whose nodes the runtime is building: call it in
    /// [`Build::build`](crate::Build::build). Each call gives a handle of
    /// its own, with its own queue and callback.
    ///
    /// [`RecordsUnavailable::Unbound`] when the segment instance runs
    /// without a pool's hub (in one process, or with a hub but no
    /// `[pool]`), or outside a build: use a store of your own then.
    /// [`RecordsUnavailable::Name`] when `table` is empty or longer than
    /// 255 bytes.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime, when the segment runs under a pool's hub:
    /// the handle starts the task that sends its writes.
    pub fn current(table: &str) -> std::result::Result<Self, RecordsUnavailable> {
        BUILDING
            .try_with(HubRecords::clone)
            .map_err(|_| RecordsUnavailable::Unbound)?
            .table(table)
    }

    /// The table's name.
    pub fn table(&self) -> &str {
        &self.front.table.name
    }

    /// Read the record of `key`: its value and version, or `None` when the
    /// hub holds none. Asks the hub at once and waits for its answer. An
    /// error when the hub cannot be asked (the instance has not reached
    /// it yet, or has left it) or does not answer.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        let table = &self.front.table;
        let key = table.wire_key(key);
        match table.ask_one(RecordOp::Get { key: &key }).await? {
            RecordAnswer::Found(record) => Ok(Some(record)),
            RecordAnswer::Missing => Ok(None),
            other => Err(unexpected("get", &other)),
        }
    }

    /// Ask whether the record of `key` is still at `version`: unchanged,
    /// at another version (with the record), or gone (the hub evicted it,
    /// or never held it). Asks the hub at once and waits, as
    /// [`PoolRecords::get`] does.
    pub async fn check(&self, key: &[u8], version: u64) -> Result<Checked> {
        let table = &self.front.table;
        let key = table.wire_key(key);
        match table
            .ask_one(RecordOp::Check { key: &key, version })
            .await?
        {
            RecordAnswer::Unchanged => Ok(Checked::Unchanged),
            RecordAnswer::Newer(record) => Ok(Checked::Newer(record)),
            RecordAnswer::Gone => Ok(Checked::Gone),
            other => Err(unexpected("check", &other)),
        }
    }

    /// Write `value` to `key` if the record matches `expected`, without
    /// waiting: the write is queued, and its answer reaches the callback
    /// given to [`PoolRecords::on_answer`]. Does not allocate.
    ///
    /// Whether the write was queued: `false` when the queue had no room
    /// for it, so it was dropped and counted. A write of more than 16 KiB
    /// (key, value, and the table's name) never fits. Writes to one key
    /// through one handle reach the hub in the order they were queued:
    /// the handle has one request on its way at a time, so while the hub
    /// is slow to answer, writes queue, then drop. Writes queued before
    /// the instance reaches its hub wait in the queue until it does.
    pub fn set(&self, key: &[u8], value: &[u8], expected: Expected) -> bool {
        self.front.table.queue(key, value, expected)
    }

    /// Give each answer to a write of this handle to `answer`, with the
    /// write's key: applied at a version, refused with the record the hub
    /// holds, or unanswered. It runs on the task that sends the writes, so
    /// keep it short. Only the first callback given is kept: whether this
    /// one was. Answers that arrive before a callback is given are counted,
    /// and not given to anyone.
    pub fn on_answer(&self, answer: impl Fn(&[u8], Written<'_>) + Send + Sync + 'static) -> bool {
        self.front.table.answer.set(Box::new(answer)).is_ok()
    }

    /// The table's eviction age in the last answer from the hub: the age
    /// of its least recently used record, or `None` when the table was
    /// empty or no answer has come yet. See [the eviction
    /// age](PoolRecords#the-eviction-age) for how to use it.
    pub fn eviction_age(&self) -> Option<Duration> {
        match self.front.table.eviction_age_ms.load(Ordering::Relaxed) {
            NO_AGE => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }
}

impl fmt::Debug for PoolRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolRecords")
            .field("table", &self.table())
            .finish_non_exhaustive()
    }
}

/// Why [`PoolRecords::current`] gave no handle.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordsUnavailable {
    /// The segment instance runs without a pool's hub, or the call was not
    /// made in a build.
    #[error("this segment instance runs without a pool's hub, so it has no record tables")]
    Unbound,
    /// The table's name cannot be used.
    #[error("record table name {name:?} {why}")]
    Name {
        /// The name given.
        name: String,
        /// What is wrong with it.
        why: &'static str,
    },
}

impl From<RecordsUnavailable> for Error {
    fn from(unavailable: RecordsUnavailable) -> Self {
        Error::new(ErrorKind::Placement, unavailable.to_string())
    }
}

/// A record of a pool hub's table: its value and its version. Each write
/// the hub applies gets a version greater than any it gave before, so of
/// two records of one key, the one with the greater version is newer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The value.
    pub value: Vec<u8>,
    /// The version of the write that stored it.
    pub version: u64,
}

/// What a record must be for a write to apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// Whatever it is: the write always applies.
    Replace,
    /// The hub holds no record of the key.
    Absent,
    /// The hub holds the record at this version.
    Version(u64),
}

/// The answer to [`PoolRecords::check`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checked {
    /// The record is still at the version asked about.
    Unchanged,
    /// The record is at another version: here it is.
    Newer(Record),
    /// The hub holds no record of the key: it evicted it, or never held
    /// it.
    Gone,
}

/// The answer to one write ([`PoolRecords::set`]), as the callback given
/// to [`PoolRecords::on_answer`] gets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Written<'a> {
    /// The write applied, at this version.
    Applied {
        /// The write's version.
        version: u64,
    },
    /// The write did not apply, because the record did not match what it
    /// expected: the record the hub holds, or none.
    Refused {
        /// The record the hub holds, if any.
        current: Option<&'a Record>,
    },
    /// No answer came: the hub could not be asked, or did not answer
    /// within 1 s. The write may or may not have applied.
    Unanswered,
}

/// The record service of a segment instance: what its nodes take
/// [`PoolRecords`] through. The runtime makes it for an instance that runs
/// under a pool's hub, and binds it to the hub once the instance has
/// registered there. Without a pool's hub it is not bound, and
/// [`PoolRecords::current`] gives [`RecordsUnavailable::Unbound`]. Cheap
/// to clone; every clone is the same service.
#[derive(Clone, Default)]
pub struct HubRecords {
    inner: Arc<Binding>,
}

#[derive(Default)]
struct Binding {
    /// Whether the instance runs under a pool's hub, decided before its
    /// nodes are built: whether a table can be taken.
    under_hub: bool,
    /// The link to the hub, once the instance has registered there.
    link: watch::Sender<Option<Arc<dyn RecordLink>>>,
    /// Each table's write counts, by its name, for the scrape: kept after
    /// the table is dropped, so that a counter never goes back.
    counts: Mutex<Vec<(Box<str>, Arc<Counts>)>>,
    /// Whether the counts are registered with a registry.
    registered: OnceLock<()>,
}

impl HubRecords {
    /// A record service that is not bound: no table can be taken from it.
    /// What an instance without a pool's hub has.
    pub fn new() -> Self {
        Self::default()
    }

    /// A record service that the runtime binds to the pool's hub at the
    /// launch: tables can be taken from it, and their writes wait for the
    /// link.
    #[doc(hidden)]
    pub fn under_hub() -> Self {
        Self {
            inner: Arc::new(Binding {
                under_hub: true,
                ..Binding::default()
            }),
        }
    }

    /// A record service bound to `link` already, for tests.
    #[doc(hidden)]
    pub fn linked(link: Arc<dyn RecordLink>) -> Self {
        let records = Self::under_hub();
        records.bind(link);
        records
    }

    /// Whether tables can be taken from it: the instance runs under a
    /// pool's hub.
    pub fn is_under_hub(&self) -> bool {
        self.inner.under_hub
    }

    /// Whether the runtime has bound it to the hub.
    pub fn is_bound(&self) -> bool {
        self.inner.link.borrow().is_some()
    }

    /// Bind it to `link`, unless it is bound already: whether this call
    /// bound it. Writes queued before start to go out.
    #[doc(hidden)]
    pub fn bind(&self, link: Arc<dyn RecordLink>) -> bool {
        self.inner.link.send_if_modified(|held| {
            if held.is_some() {
                return false;
            }
            *held = Some(link);
            true
        })
    }

    /// The handle on the table named `table`, as [`PoolRecords::current`]
    /// gives it inside a build.
    #[doc(hidden)]
    pub fn table(&self, table: &str) -> std::result::Result<PoolRecords, RecordsUnavailable> {
        if !self.inner.under_hub {
            return Err(RecordsUnavailable::Unbound);
        }
        let why = if table.is_empty() {
            Some("is empty")
        } else if table.len() > usize::from(u8::MAX) {
            Some("is longer than 255 bytes")
        } else {
            None
        };
        if let Some(why) = why {
            return Err(RecordsUnavailable::Name {
                name: table.to_string(),
                why,
            });
        }
        let counts = Arc::new(Counts::default());
        self.inner
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((table.into(), Arc::clone(&counts)));
        let table = Arc::new(Table::new(table, Arc::clone(&self.inner), counts));
        tokio::spawn(outbox::drain(
            Arc::clone(&table),
            self.inner.link.subscribe(),
        ));
        Ok(PoolRecords {
            front: Arc::new(Front { table }),
        })
    }

    /// Record each table's writes in `registry`, as
    /// `pool_record_writes_total`, once: later calls do nothing.
    #[doc(hidden)]
    pub fn register(&self, registry: &prometheus::Registry) -> prometheus::Result<()> {
        if self.inner.registered.set(()).is_err() {
            return Ok(());
        }
        registry.register(Box::new(scrape::Scrape::new(Arc::clone(&self.inner))?))
    }

    /// Run `build` with this service as the one [`PoolRecords::current`]
    /// takes tables from.
    #[doc(hidden)]
    pub async fn scope<F: Future>(&self, build: F) -> F::Output {
        BUILDING.scope(self.clone(), build).await
    }
}

impl fmt::Debug for HubRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HubRecords")
            .field("under_hub", &self.is_under_hub())
            .field("bound", &self.is_bound())
            .finish()
    }
}

/// How a [`HubRecords`] reaches its hub: the runtime's link, or a test's.
#[doc(hidden)]
pub trait RecordLink: Send + Sync + 'static {
    /// Send `ops` to the hub in one request, in order, and give its
    /// answers, one for each operation in order, and the table's eviction
    /// age. Reads `ops` before it returns. The future must end on its own,
    /// answered or not: a table sends its next batch of writes only once
    /// the last request has ended, so that two requests never race to the
    /// hub. The runtime's ends by velo-hub's request timeout
    /// (`DEFAULT_REQUEST_TIMEOUT`, 60 s).
    fn ask(
        &self,
        ops: &mut dyn Iterator<Item = RecordOp<'_>>,
    ) -> BoxFuture<'static, Result<RecordReply>>;
}

/// One operation a [`RecordLink`] sends, with its key as the hub stores
/// it: the table's name, then the key.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOp<'a> {
    /// Read the record of `key`.
    Get {
        /// The key.
        key: &'a [u8],
    },
    /// Write `value` to `key` if the record matches `expected`.
    Set {
        /// The key.
        key: &'a [u8],
        /// The value.
        value: &'a [u8],
        /// What the record must be.
        expected: Expected,
    },
    /// Ask whether the record of `key` is still at `version`.
    Check {
        /// The key.
        key: &'a [u8],
        /// The version the asker holds.
        version: u64,
    },
}

/// The hub's answer to one [`RecordOp`].
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordAnswer {
    /// A get found the record.
    Found(Record),
    /// A get found none.
    Missing,
    /// A set applied, at this version.
    Applied(u64),
    /// A set did not apply: the record as it is, if any.
    Refused(Option<Record>),
    /// A check found the record at its version.
    Unchanged,
    /// A check found the record at another version.
    Newer(Record),
    /// A check found no record.
    Gone,
}

/// The hub's answer to one [`RecordLink::ask`].
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordReply {
    /// One answer for each operation, in order.
    pub answers: Vec<RecordAnswer>,
    /// The table's eviction age, `None` while it is empty.
    pub eviction_age: Option<Duration>,
}

/// What every clone of one [`PoolRecords`] holds: dropping the last one
/// ends the task that sends its writes, once it has sent what is queued.
struct Front {
    table: Arc<Table>,
}

impl Drop for Front {
    fn drop(&mut self) {
        self.table.closed.store(true, Ordering::Release);
        self.table.pending.notify_one();
    }
}

/// `eviction_age_ms` before any answer, or after one with an empty table.
const NO_AGE: u64 = u64::MAX;

/// One table's state, shared by its handle and the task that sends its
/// writes.
struct Table {
    name: Box<str>,
    /// The table's name as the prefix of each key the hub stores: its
    /// length in one byte, then its bytes, so that no two tables' keys
    /// meet.
    prefix: Box<[u8]>,
    binding: Arc<Binding>,
    /// The queued writes, split by key so that writers of different keys
    /// rarely meet on one lock, and the writes of one key stay in order.
    shards: Box<[Mutex<Batch>]>,
    /// What the shards hold, in bytes of their budget.
    queued: AtomicUsize,
    /// When the table was made: what `first_write` counts from.
    born: tokio::time::Instant,
    /// When a write last landed in an empty queue, in nanoseconds since
    /// `born`: the batch is sent 5 ms after it.
    first_write: AtomicU64,
    /// Signalled when a write lands in an empty queue, and when the
    /// handle is dropped.
    pending: Notify,
    /// Signalled when the queue reaches a batch's worth.
    full: Notify,
    closed: AtomicBool,
    answer: OnceLock<Box<OnAnswer>>,
    eviction_age_ms: AtomicU64,
    counts: Arc<Counts>,
}

impl Table {
    fn new(name: &str, binding: Arc<Binding>, counts: Arc<Counts>) -> Self {
        let mut prefix = Vec::with_capacity(1 + name.len());
        prefix.push(u8::try_from(name.len()).unwrap_or(u8::MAX));
        prefix.extend_from_slice(name.as_bytes());
        Self {
            name: name.into(),
            prefix: prefix.into(),
            binding,
            shards: (0..outbox::SHARDS)
                .map(|_| Mutex::new(Batch::new()))
                .collect(),
            queued: AtomicUsize::new(0),
            born: tokio::time::Instant::now(),
            first_write: AtomicU64::new(0),
            pending: Notify::new(),
            full: Notify::new(),
            closed: AtomicBool::new(false),
            answer: OnceLock::new(),
            eviction_age_ms: AtomicU64::new(NO_AGE),
            counts,
        }
    }

    /// `key` as the hub stores it.
    fn wire_key(&self, key: &[u8]) -> Vec<u8> {
        let mut wire = Vec::with_capacity(self.prefix.len() + key.len());
        wire.extend_from_slice(&self.prefix);
        wire.extend_from_slice(key);
        wire
    }

    /// Queue one write: no allocation, no wait.
    fn queue(&self, key: &[u8], value: &[u8], expected: Expected) -> bool {
        let cost = outbox::cost(self.prefix.len() + key.len() + value.len());
        let shard = &self.shards[outbox::shard(key)];
        let before = {
            let mut batch = shard.lock().unwrap_or_else(PoisonError::into_inner);
            if !batch.push(&self.prefix, key, value, expected, cost) {
                drop(batch);
                self.counts.dropped.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            // Under the lock, so that the task that takes this batch
            // subtracts its cost only after it was added.
            self.queued.fetch_add(cost, Ordering::AcqRel)
        };
        if before == 0 {
            let since = self.born.elapsed().as_nanos();
            self.first_write
                .store(u64::try_from(since).unwrap_or(u64::MAX), Ordering::Release);
            self.pending.notify_one();
        }
        if before < outbox::BATCH_BYTES && before + cost >= outbox::BATCH_BYTES {
            self.full.notify_one();
        }
        true
    }

    /// Ask the hub one operation at once.
    async fn ask_one(&self, op: RecordOp<'_>) -> Result<RecordAnswer> {
        let link = self.binding.link.borrow().clone().ok_or_else(|| {
            Error::new(
                ErrorKind::Transport,
                "this segment instance has not reached its pool's hub yet",
            )
        })?;
        let reply = link.ask(&mut std::iter::once(op)).await?;
        self.saw(reply.eviction_age);
        reply.answers.into_iter().next().ok_or_else(|| {
            Error::new(
                ErrorKind::Transport,
                "the pool's hub gave no answer to a record operation",
            )
        })
    }

    /// Keep the eviction age of the latest answer.
    fn saw(&self, age: Option<Duration>) {
        let ms = age.map_or(NO_AGE, |age| {
            u64::try_from(age.as_millis()).unwrap_or(NO_AGE - 1)
        });
        self.eviction_age_ms.store(ms, Ordering::Relaxed);
    }

    /// Give the answer to the write of `key` to the callback, and count it.
    fn answered(&self, key: &[u8], written: Written<'_>) {
        let count = match written {
            Written::Applied { .. } => &self.counts.applied,
            Written::Refused { .. } => &self.counts.refused,
            Written::Unanswered => &self.counts.unanswered,
        };
        count.fetch_add(1, Ordering::Relaxed);
        if let Some(answer) = self.answer.get() {
            answer(key, written);
        }
    }
}

/// The callback a handle's write answers reach.
type OnAnswer = dyn Fn(&[u8], Written<'_>) + Send + Sync;

/// One table's writes, by how they ended.
#[derive(Default)]
struct Counts {
    applied: AtomicU64,
    refused: AtomicU64,
    dropped: AtomicU64,
    unanswered: AtomicU64,
}

fn unexpected(op: &str, answer: &RecordAnswer) -> Error {
    Error::new(
        ErrorKind::Transport,
        format!("the pool's hub answered a {op} with {answer:?}"),
    )
}

#[cfg(test)]
mod tests;
