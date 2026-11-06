use std::{
    collections::HashMap,
    future::Future,
    hash::{BuildHasher, RandomState},
    sync::{Arc, Mutex, MutexGuard, PoisonError, atomic::AtomicUsize},
    time::{Duration, SystemTime},
};

use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, GetResponse, KvClient, Txn, TxnOp, TxnOpResponse,
    TxnResponse, proto::PbTxnRequest,
};
use gdl_pools::{
    DirectoryError, LastWrite, PoolDirectory, PoolRecord, ResumeToken, Version, Withdrawal,
};
use prost::Message;

use crate::{
    value::{self, Split, Stored},
    watch::{self, EtcdWatch, State},
};

/// The key that names the store a directory's keys are in: a random value
/// that the first handle to find it absent makes. Every write compares it,
/// and every watch checks it when it is made, so that a store replaced by
/// an empty one, which reuses small revisions, is never taken for the one
/// a handle read.
pub const STORE_ID: &str = "/gdl/store-id";

/// What etcd adds to a transaction before it checks the request limit: the
/// raft request's header and the transaction's own tag and length, 17
/// bytes against etcd 3.5.21 without authentication. Kept well above that,
/// so that a write the backend sends is never refused by etcd as too
/// large.
const ETCD_OVERHEAD: usize = 64;

/// How many times a write reads the pool again after its compare failed,
/// before it says the directory is unavailable: each retry follows a write
/// of the pool by another writer, or a replaced store.
const ATTEMPTS: usize = 8;

/// A pool directory in etcd, for the hubs of one site: Slurm, on-prem, and
/// clouds without DynamoDB.
///
/// Each pool has two keys under the prefix: its record key holds its last
/// write without its property list (the record, or the tombstone of its
/// withdrawal), and its list key holds the list. A write is one
/// transaction that lands only if the store is still the one the handle
/// read ([`STORE_ID`]) and the record key has not changed since it was
/// read, or does not exist yet. The list key is written in the same
/// transaction only when the list changed, so a heartbeat writes the
/// record key alone. A withdrawal is a value, never a delete, and no key
/// has a lease: nothing the directory holds expires by time.
///
/// A watch of a class is served by one task per class per handle, while
/// the class has watches. It reads the class's snapshot at one revision,
/// in pages, then watches the store from the revision after it, and keeps
/// the latest changes for watches that resume. A watcher that falls behind
/// its buffer gets [`gdl_pools::PoolEvent::Resnapshot`], and so does every
/// watcher of the class when the store's watch cannot resume: its revision
/// was compacted, or the store was replaced. A resume token is a place in
/// what this handle and its clones watched.
///
/// The handle links no TLS and sends no credentials: the etcd members are
/// reached over plain `http://`.
#[derive(Debug, Clone)]
pub struct EtcdDirectory {
    pub(crate) shared: Arc<Shared>,
}

/// How an [`EtcdDirectory`] buffers, sizes its calls, and waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EtcdLimits {
    /// The changes a watcher may have unread before it is cut off with
    /// [`gdl_pools::PoolEvent::Resnapshot`].
    pub buffer: usize,
    /// The latest changes of each class kept for watches that resume.
    pub history: usize,
    /// etcd's request limit, `--max-request-bytes` (1.5 MiB by default):
    /// a write whose transaction does not fit is refused as
    /// [`DirectoryError::Malformed`] before it is sent, and the largest
    /// value a snapshot's page is sized for.
    pub request_bytes: usize,
    /// The largest response the client takes, and so the bytes a
    /// snapshot's page may hold. At least `request_bytes`, so that one
    /// key always fits a page.
    pub response_bytes: usize,
    /// How long one call to etcd may take, and how long a new watch waits
    /// for its snapshot, before it fails as
    /// [`DirectoryError::Unavailable`].
    pub timeout: Duration,
}

impl Default for EtcdLimits {
    fn default() -> Self {
        Self {
            buffer: 256,
            history: 256,
            request_bytes: 1_572_864,
            response_bytes: 32 << 20,
            timeout: Duration::from_secs(10),
        }
    }
}

/// A pool as this handle last read or wrote it: what its next write
/// compares, so that a heartbeat needs no read first.
#[derive(Debug, Clone)]
struct Held {
    /// The store's identity at the read; None when the store had none.
    id: Option<Vec<u8>>,
    /// The record key's revision: 0 when it does not exist.
    revision: i64,
    /// The version of the pool's last write.
    version: Option<Version>,
    /// The list key's revision and value.
    list: Option<(i64, Vec<u8>)>,
}

pub(crate) struct Shared {
    pub(crate) client: Client,
    pub(crate) endpoints: Vec<String>,
    pub(crate) prefix: String,
    pub(crate) limits: EtcdLimits,
    // The position before each class's first change through this handle.
    pub(crate) base: u64,
    // The member a class's watch task tries next, modulo the endpoints.
    pub(crate) endpoint: AtomicUsize,
    pub(crate) state: Mutex<State>,
    // The store identity this handle's writes compare.
    identity: tokio::sync::Mutex<Option<Vec<u8>>>,
    held: Mutex<HashMap<(String, String), Held>>,
    #[cfg(feature = "etcd-local")]
    pub(crate) hook: Mutex<Option<crate::testing::Hook>>,
}

// By hand: the client and a test's hook do not print.
impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("endpoints", &self.endpoints)
            .field("prefix", &self.prefix)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl EtcdDirectory {
    /// The directory under `prefix` in the etcd cluster whose members'
    /// client URLs are `endpoints`, with the default limits. Nothing is
    /// read or made until it is used.
    ///
    /// # Errors
    ///
    /// No endpoint, or one that is not a URL.
    pub async fn connect(endpoints: Vec<String>, prefix: String) -> Result<Self, DirectoryError> {
        Self::with_limits(endpoints, prefix, EtcdLimits::default()).await
    }

    /// The directory under `prefix` at `endpoints`, with `limits`. A
    /// buffer of 0 is taken as 1, and a response limit under the request
    /// limit as the request limit.
    ///
    /// # Errors
    ///
    /// No endpoint, or one that is not a URL.
    pub async fn with_limits(
        endpoints: Vec<String>,
        prefix: String,
        limits: EtcdLimits,
    ) -> Result<Self, DirectoryError> {
        let limits = EtcdLimits {
            buffer: limits.buffer.max(1),
            response_bytes: limits.response_bytes.max(limits.request_bytes),
            ..limits
        };
        let client = connect(&endpoints).await?;
        Ok(Self {
            shared: Arc::new(Shared {
                client,
                endpoints,
                prefix,
                limits,
                base: watch::base(),
                endpoint: AtomicUsize::new(0),
                state: Mutex::default(),
                identity: tokio::sync::Mutex::new(None),
                held: Mutex::default(),
                #[cfg(feature = "etcd-local")]
                hook: Mutex::default(),
            }),
        })
    }

    /// The prefix of the directory's keys.
    pub fn prefix(&self) -> &str {
        &self.shared.prefix
    }

    /// How many watches of `class` this handle and its clones have open.
    pub fn watchers(&self, class: &str) -> usize {
        self.shared.watchers(class)
    }
}

/// A client of `endpoints`, which reaches each member over plain HTTP/2
/// with keepalive, so that a member that stops answering ends its
/// connection. The interval is above etcd's default
/// `--grpc-keepalive-min-time`, 5 s, which a shorter one breaks. Every call
/// requires a leader: a member cut off from the rest of its cluster still
/// answers on its client port, but refuses the call and cancels its
/// watches, so the watch task moves to another member rather than wait on
/// one that no write reaches.
pub(crate) async fn connect(endpoints: &[String]) -> Result<Client, DirectoryError> {
    let options = ConnectOptions::new()
        .with_keep_alive(Duration::from_secs(10), Duration::from_secs(5))
        .with_connect_timeout(Duration::from_secs(5))
        .with_require_leader(true);
    Client::connect(endpoints, Some(options))
        .await
        .map_err(|error| {
            DirectoryError::Malformed(format!("the etcd endpoints {endpoints:?}: {error}"))
        })
}

impl Shared {
    /// The KV client, which takes responses up to the response limit.
    pub(crate) fn kv(&self) -> KvClient {
        kv(&self.client, &self.limits)
    }

    fn held(&self) -> MutexGuard<'_, HashMap<(String, String), Held>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The store identity this handle's writes compare: read, or made if
    /// the store has none, on first use.
    async fn identity(&self) -> Result<Vec<u8>, DirectoryError> {
        let mut identity = self.identity.lock().await;
        if let Some(id) = identity.as_ref() {
            return Ok(id.clone());
        }
        let id = identify(&self.client, &self.limits).await?;
        *identity = Some(id.clone());
        Ok(id)
    }

    /// Take `seen`, the identity a write found, for the store's: or, when
    /// the store has none, an empty store that replaced the one this
    /// handle read, make it again.
    async fn reidentify(&self, seen: Option<Vec<u8>>) -> Result<(), DirectoryError> {
        let id = match seen {
            Some(id) => id,
            None => identify(&self.client, &self.limits).await?,
        };
        tracing::warn!(
            prefix = self.prefix,
            "the etcd store is not the one this handle read: it was replaced"
        );
        *self.identity.lock().await = Some(id);
        Ok(())
    }

    /// Read `pool`'s record and list keys, and the store's identity, at
    /// one revision.
    async fn read(
        &self,
        class: &str,
        pool: &str,
    ) -> Result<(Held, Option<Stored>), DirectoryError> {
        let txn = Txn::new().and_then([
            TxnOp::get(STORE_ID, None),
            TxnOp::get(value::record_key(&self.prefix, class, pool), None),
            TxnOp::get(value::list_key(&self.prefix, class, pool), None),
        ]);
        let what = format!("reading pool `{pool}` of class `{class}`");
        let read = call(&what, self.limits.timeout, self.kv().txn(txn)).await?;
        held_of(&read, class, pool)
    }

    /// Write `pool`'s record key, and its list key when `list` is a list
    /// that differs from the one held, if the write at `version` is newer
    /// than the pool's last write and, given `after`, that is the write the
    /// caller read: one transaction that compares the store's identity and
    /// the record key's revision.
    async fn write(
        &self,
        class: &str,
        pool: &str,
        version: Version,
        write: Write<'_>,
        after: Option<Option<Version>>,
    ) -> Result<(), DirectoryError> {
        let owner = (class.to_owned(), pool.to_owned());
        let record_key = value::record_key(&self.prefix, class, pool);
        let list_key = value::list_key(&self.prefix, class, pool);
        // A hint, not a promise: the store compares what it names, so a
        // write may rest on it, but a refusal may not, since another writer
        // or a replaced store can have moved past it. Out while the write
        // runs, so that a write dropped part way leaves the next to read the
        // pool. The flag says whether the state was read from the store for
        // this write.
        let mut known = self.held().remove(&owner).map(|held| (held, false));
        for _ in 0..ATTEMPTS {
            let id = self.identity().await?;
            let (held, read) = match known.take() {
                Some((held, read)) if held.id.as_ref() == Some(&id) => (held, read),
                _ => (self.read(class, pool).await?.0, true),
            };
            if held.id.as_ref() != Some(&id) {
                self.reidentify(held.id).await?;
                continue;
            }
            if let Some(refused) = refused(class, pool, held.version, version, after) {
                if !read {
                    continue;
                }
                self.held().insert(owner, held);
                return Err(refused);
            }
            let (value, list) = match &write {
                Write::Record(Split { fields, list }) => {
                    let kept = held
                        .list
                        .as_ref()
                        .filter(|(_, held)| held == list)
                        .map(|(at, _)| *at);
                    (
                        value::encode_record(fields, kept),
                        kept.is_none().then_some(list),
                    )
                }
                Write::Withdrawal(withdrawal) => (value::encode_withdrawal(withdrawal), None),
            };
            let unchanged = if held.revision == 0 {
                Compare::create_revision(record_key.as_str(), CompareOp::Equal, 0)
            } else {
                Compare::mod_revision(record_key.as_str(), CompareOp::Equal, held.revision)
            };
            let mut puts = Vec::with_capacity(2);
            if let Some(list) = list {
                puts.push(TxnOp::put(list_key.as_str(), list.clone(), None));
            }
            puts.push(TxnOp::put(record_key.as_str(), value, None));
            let txn = Txn::new()
                .when([
                    Compare::value(STORE_ID, CompareOp::Equal, id.clone()),
                    unchanged,
                ])
                .and_then(puts)
                .or_else([
                    TxnOp::get(STORE_ID, None),
                    TxnOp::get(record_key.as_str(), None),
                    TxnOp::get(list_key.as_str(), None),
                ]);
            self.fits(class, pool, &txn)?;
            let what = format!("writing pool `{pool}` of class `{class}`");
            let done = call(&what, self.limits.timeout, self.kv().txn(txn)).await?;
            if done.succeeded() {
                let revision = header(&done)?;
                let list = match list {
                    Some(list) => Some((revision, list.clone())),
                    None => held.list,
                };
                self.held().insert(
                    owner,
                    Held {
                        id: Some(id),
                        revision,
                        version: Some(version),
                        list,
                    },
                );
                return Ok(());
            }
            // The store holds another write of the pool, or is another
            // store: what it holds is in the failure's reads.
            known = Some((held_of(&done, class, pool)?.0, true));
        }
        Err(DirectoryError::Unavailable(format!(
            "pool `{pool}` of class `{class}`: {ATTEMPTS} writes in a row found the pool changed since they read it"
        )))
    }

    /// Whether `txn` fits one etcd request, with room for what etcd adds.
    fn fits(&self, class: &str, pool: &str, txn: &Txn) -> Result<(), DirectoryError> {
        let bytes = PbTxnRequest::from(txn.clone()).encoded_len();
        let limit = self.limits.request_bytes;
        if bytes + ETCD_OVERHEAD > limit {
            return Err(DirectoryError::Malformed(format!(
                "pool `{pool}` of class `{class}`: the write is {bytes} bytes, and with the {ETCD_OVERHEAD} etcd adds it does not fit etcd's request limit of {limit} bytes; its property list is too large"
            )));
        }
        Ok(())
    }
}

/// What a write stores in a pool's record key.
enum Write<'a> {
    Record(&'a Split),
    Withdrawal(&'a Withdrawal),
}

/// The KV client of `client`, which takes responses up to the response
/// limit.
pub(crate) fn kv(client: &Client, limits: &EtcdLimits) -> KvClient {
    client
        .kv_client()
        .max_decoding_message_size(limits.response_bytes)
}

/// The store's identity, read through `client`, or made if it has none:
/// one transaction that puts a random value only where no key is.
pub(crate) async fn identify(
    client: &Client,
    limits: &EtcdLimits,
) -> Result<Vec<u8>, DirectoryError> {
    let made = random_id();
    let txn = Txn::new()
        .when([Compare::create_revision(STORE_ID, CompareOp::Equal, 0)])
        .and_then([TxnOp::put(STORE_ID, made.clone(), None)])
        .or_else([TxnOp::get(STORE_ID, None)]);
    let done = call(
        "making the store's identity",
        limits.timeout,
        kv(client, limits).txn(txn),
    )
    .await?;
    if done.succeeded() {
        return Ok(made);
    }
    match done.op_responses().first() {
        Some(TxnOpResponse::Get(got)) => match got.kvs().first() {
            Some(kv) => Ok(kv.value().to_vec()),
            // Made and then deleted between the compare and the read: the
            // next use reads it again.
            None => Err(DirectoryError::Unavailable(
                "the store's identity was deleted as it was read".into(),
            )),
        },
        _ => Err(unexpected("making the store's identity")),
    }
}

/// The store's identity, read through `client`: None when it has none.
pub(crate) async fn read_identity(
    client: &Client,
    limits: &EtcdLimits,
) -> Result<Option<Vec<u8>>, DirectoryError> {
    let got = call(
        "reading the store's identity",
        limits.timeout,
        kv(client, limits).get(STORE_ID, None),
    )
    .await?;
    Ok(got.kvs().first().map(|kv| kv.value().to_vec()))
}

/// 128 random bits, as hex: two hashes of the clock by hashers that the
/// standard library keys from the operating system's randomness.
fn random_id() -> Vec<u8> {
    let now = SystemTime::now();
    let a = RandomState::new().hash_one((now, std::process::id()));
    let b = RandomState::new().hash_one((now, a));
    format!("{a:016x}{b:016x}").into_bytes()
}

/// A pool's state, from the three reads of [`Shared::read`] or of a
/// failed write: the store's identity, the record key, and the list key.
fn held_of(
    read: &TxnResponse,
    class: &str,
    pool: &str,
) -> Result<(Held, Option<Stored>), DirectoryError> {
    let responses = read.op_responses();
    let [
        TxnOpResponse::Get(id),
        TxnOpResponse::Get(record),
        TxnOpResponse::Get(list),
    ] = &responses[..]
    else {
        return Err(unexpected(&format!(
            "reading pool `{pool}` of class `{class}`"
        )));
    };
    let first = |got: &GetResponse| got.kvs().first().cloned();
    let (stored, revision) = match first(record) {
        Some(kv) => (
            Some(value::decode(kv.value()).ok_or_else(|| {
                DirectoryError::Unavailable(format!(
                    "the record key of pool `{pool}` of class `{class}` holds a value this backend did not write"
                ))
            })?),
            kv.mod_revision(),
        ),
        None => (None, 0),
    };
    Ok((
        Held {
            id: first(id).map(|kv| kv.value().to_vec()),
            revision,
            version: stored.as_ref().map(Stored::version),
            list: first(list).map(|kv| (kv.mod_revision(), kv.value().to_vec())),
        },
        stored,
    ))
}

fn header(response: &TxnResponse) -> Result<i64, DirectoryError> {
    response
        .header()
        .map(|header| header.revision())
        .ok_or_else(|| unexpected("a write"))
}

fn unexpected(what: &str) -> DirectoryError {
    DirectoryError::Unavailable(format!("{what}: etcd answered in a shape it never does"))
}

/// Run `future`, a call to etcd, within `timeout`: a call that fails or
/// does not answer makes the directory unavailable.
pub(crate) async fn call<T>(
    what: &str,
    timeout: Duration,
    future: impl Future<Output = Result<T, etcd_client::Error>>,
) -> Result<T, DirectoryError> {
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(answer)) => Ok(answer),
        Ok(Err(error)) => Err(DirectoryError::Unavailable(format!("{what}: {error}"))),
        Err(_) => Err(DirectoryError::Unavailable(format!(
            "{what}: etcd did not answer within {timeout:?}"
        ))),
    }
}

/// Why a write at `offered` of `pool` may not land on the pool's last
/// write, at `held`: it is not newer, or, for a write made after reading
/// the last write at `after` (`Some`), the store holds another. None when
/// it may land.
fn refused(
    class: &str,
    pool: &str,
    held: Option<Version>,
    offered: Version,
    after: Option<Option<Version>>,
) -> Option<DirectoryError> {
    let held = held?;
    let moved = after.is_some_and(|after| after != Some(held));
    (offered <= held || moved).then(|| DirectoryError::NotNewer {
        class: class.to_owned(),
        pool: pool.to_owned(),
        held,
        offered,
    })
}

impl EtcdDirectory {
    /// [`PoolDirectory::publish`], or, given `after` (`Some`),
    /// [`PoolDirectory::publish_after`].
    async fn publish_with(
        &self,
        record: PoolRecord,
        after: Option<Option<Version>>,
    ) -> Result<(), DirectoryError> {
        record.check()?;
        let split = value::split(&record);
        self.shared
            .write(
                &record.class,
                &record.name,
                record.version(),
                Write::Record(&split),
                after,
            )
            .await
    }
}

impl PoolDirectory for EtcdDirectory {
    type Watch = EtcdWatch;

    // Boxed: a write holds several calls' futures, too large to move on a
    // caller's stack, and it runs once a heartbeat, not per request.
    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        Box::pin(self.publish_with(record, None)).await
    }

    async fn publish_after(
        &self,
        record: PoolRecord,
        last: Option<Version>,
    ) -> Result<(), DirectoryError> {
        Box::pin(self.publish_with(record, Some(last))).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        withdrawal.check()?;
        Box::pin(self.shared.write(
            &withdrawal.class,
            &withdrawal.pool,
            withdrawal.version(),
            Write::Withdrawal(&withdrawal),
            None,
        ))
        .await
    }

    /// One read of the pool's two keys at one revision, so the answer is
    /// one state of the pool. A record is joined only to the list it
    /// names; a list that is not that one, or a value that does not
    /// decode, is an error, never none.
    async fn last_write(
        &self,
        class: &str,
        pool: &str,
    ) -> Result<Option<LastWrite>, DirectoryError> {
        let (held, stored) = Box::pin(self.shared.read(class, pool)).await?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        let fields = match stored {
            Stored::Withdrawn(withdrawal) => return Ok(Some(LastWrite::Withdrawn(withdrawal))),
            Stored::Record { ref fields, .. } => fields,
        };
        let named = stored.list_at(held.revision);
        let joined = held
            .list
            .as_ref()
            .filter(|(at, _)| Some(*at) == named)
            .and_then(|(_, list)| value::join(fields, list));
        joined
            .map(|record| Some(LastWrite::Record(Box::new(record))))
            .ok_or_else(|| {
                DirectoryError::Unavailable(format!(
                    "pool `{pool}` of class `{class}`: its record names a property list its list key does not hold"
                ))
            })
    }

    /// The watch begins when it is first polled, which must be inside a
    /// tokio runtime.
    fn watch(&self, class: &str, from: Option<ResumeToken>) -> EtcdWatch {
        EtcdWatch::new(Arc::clone(&self.shared), class, from)
    }
}
