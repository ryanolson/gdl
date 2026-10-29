use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_dynamodb::{
    Client,
    client::Waiters,
    error::{DisplayErrorContext, SdkError},
    operation::put_item::PutItemError,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, ReturnValue,
        ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TimeToLiveSpecification,
    },
};
use gdl_pools::{DirectoryError, PoolDirectory, PoolRecord, Version, Withdrawal};

use crate::{
    item::{self, Item, ListKey},
    watch::{self, DynamoWatch, State},
};

/// A pool directory in two DynamoDB tables, for hubs in several regions:
/// global tables with multi-Region eventual consistency (MREC) replicate
/// each write to every region, and each hub reads its own region's copy.
///
/// The records table holds one item per pool: its record without the
/// model list, or the tombstone of its withdrawal. The lists table holds
/// each model list a record names, keyed by the record's `models_version`,
/// a digest of the list, and the token of the put that made it, so a scan
/// reads a few hundred bytes per pool and fetches a list only when a
/// pool's list changes. A write is a conditional put that holds only if it
/// is newer than the pool's item, so a late write is refused with
/// [`DirectoryError::NotNewer`]. A list is written before the record that
/// names it, and no put replaces another's list.
///
/// Neither table expires a live item: a record stays until its pool
/// withdraws it, and a reader applies the lease on its own clock
/// ([`gdl_pools::PoolView`]). The lists table's TTL removes only a list
/// that no record of its pool names again: an hour after a write replaced
/// the record that named it, at a higher `models_version`.
///
/// A watch is polled: while a class has watches through this handle, one
/// task queries the class's items every [`DynamoLimits::period`], and the
/// differences are the watch's changes. Each query is billed, so the
/// period is the cost. A record whose list has not arrived (replication
/// does not order the two tables) is not installed: the watch keeps the
/// pool's earlier version, and queries again sooner, backing off to the
/// period. A new watch's snapshot waits for every list it names, and a
/// watch whose snapshot cannot complete within [`DynamoLimits::first_sync`]
/// ends with [`DirectoryError::Unavailable`]. A failed query ends every
/// watch of its class the same way: the reader watches again after its
/// own backoff.
///
/// A resume token is a place in what this handle and its clones watched:
/// a token of another handle, or from before a spell in which the class
/// went unwatched and its items changed, yields
/// [`gdl_pools::PoolEvent::Resnapshot`].
#[derive(Debug, Clone)]
pub struct DynamoDirectory {
    shared: Arc<Shared>,
}

/// The two tables a [`DynamoDirectory`] uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tables {
    /// One item per pool, keyed by `class` (partition) and `pool` (sort).
    pub records: String,
    /// One item per model list, keyed by `owner` (partition) and `list`
    /// (sort), with TTL on `expires_at`.
    pub lists: String,
}

/// How a [`DynamoDirectory`] polls and buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DynamoLimits {
    /// The changes a watcher may have unread before it is cut off with
    /// [`gdl_pools::PoolEvent::Resnapshot`].
    pub buffer: usize,
    /// The latest changes of each class kept for watches that resume.
    pub history: usize,
    /// The wait between two queries of a watched class: each wait is drawn
    /// between half of it and all of it, so that hubs that started together
    /// do not query together.
    pub period: Duration,
    /// How long a new watch's snapshot waits for the lists its records
    /// name before the watch ends with [`DirectoryError::Unavailable`]. A
    /// scan that takes longer fails, and ends every watch of its class.
    pub first_sync: Duration,
}

impl Default for DynamoLimits {
    fn default() -> Self {
        Self {
            buffer: 256,
            history: 256,
            period: Duration::from_secs(1),
            first_sync: Duration::from_secs(30),
        }
    }
}

/// How long a list its pool has moved past stays before the lists table's
/// TTL may remove it: far past the replication lag, so that a reader whose
/// region still shows the earlier record can still join it.
const SUPERSEDED: Duration = Duration::from_secs(3_600);

/// How long [`DynamoDirectory::create_tables`] waits for a new table to be
/// active.
const TABLE_ACTIVE: Duration = Duration::from_secs(300);

/// The list a pool's last write through a handle named, and the write's
/// epoch.
#[derive(Debug, Clone, Copy)]
struct Written {
    key: ListKey,
    epoch: u64,
}

#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) client: Client,
    pub(crate) tables: Tables,
    pub(crate) limits: DynamoLimits,
    // The position before each class's first change through this handle.
    pub(crate) base: u64,
    pub(crate) state: Mutex<State>,
    // The list each pool's last write through this handle named, so that
    // a heartbeat does not write its list again.
    written: Mutex<HashMap<(String, String), Written>>,
}

impl DynamoDirectory {
    /// The directory in `tables`, through `client`, with the default
    /// limits. Nothing is read or made until it is used.
    pub fn new(client: Client, tables: Tables) -> Self {
        Self::with_limits(client, tables, DynamoLimits::default())
    }

    /// The directory in `tables`, through `client`, with `limits`. A
    /// buffer of 0 is taken as 1, and a period under 1 ms as 1 ms.
    pub fn with_limits(client: Client, tables: Tables, limits: DynamoLimits) -> Self {
        let limits = DynamoLimits {
            period: limits.period.max(Duration::from_millis(1)),
            ..limits
        };
        Self {
            shared: Arc::new(Shared {
                client,
                tables,
                limits,
                base: watch::base(),
                state: Mutex::default(),
                written: Mutex::default(),
            }),
        }
    }

    /// Make the two tables, in this client's region alone, with on-demand
    /// billing and TTL on the lists table. For DynamoDB Local and for
    /// trials: a deployment makes its global tables with their replicas in
    /// the tool that owns its infrastructure, with the same keys.
    ///
    /// # Errors
    ///
    /// [`DirectoryError::Unavailable`] when a table cannot be made, for
    /// example because it exists.
    pub async fn create_tables(client: &Client, tables: &Tables) -> Result<(), DirectoryError> {
        let key = |name: &str, kind| {
            KeySchemaElement::builder()
                .attribute_name(name)
                .key_type(kind)
                .build()
                .expect("a key has its name and type")
        };
        let text = |name: &str| {
            AttributeDefinition::builder()
                .attribute_name(name)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("an attribute has its name and type")
        };
        for (table, partition, sort) in [
            (&tables.records, item::CLASS, item::POOL),
            (&tables.lists, item::OWNER, item::LIST_KEY),
        ] {
            client
                .create_table()
                .table_name(table)
                .billing_mode(BillingMode::PayPerRequest)
                .key_schema(key(partition, KeyType::Hash))
                .key_schema(key(sort, KeyType::Range))
                .attribute_definitions(text(partition))
                .attribute_definitions(text(sort))
                .send()
                .await
                .map_err(|error| unavailable(&format!("making table {table}"), error))?;
        }
        // A new table takes TTL only once it is active.
        client
            .wait_until_table_exists()
            .table_name(&tables.lists)
            .wait(TABLE_ACTIVE)
            .await
            .map_err(|error| {
                DirectoryError::Unavailable(format!(
                    "waiting for table {} to be active: {}",
                    tables.lists,
                    DisplayErrorContext(error)
                ))
            })?;
        let ttl = TimeToLiveSpecification::builder()
            .attribute_name(item::EXPIRES_AT)
            .enabled(true)
            .build()
            .expect("TTL has its attribute and switch");
        client
            .update_time_to_live()
            .table_name(&tables.lists)
            .time_to_live_specification(ttl)
            .send()
            .await
            .map_err(|error| unavailable(&format!("turning on TTL of {}", tables.lists), error))?;
        Ok(())
    }

    /// The tables, as given.
    pub fn tables(&self) -> &Tables {
        &self.shared.tables
    }

    /// How many watches of `class` this handle and its clones have open.
    pub fn watchers(&self, class: &str) -> usize {
        self.shared.watchers(class)
    }

    /// Put `item`, a write of `pool` of `class` at `version`, if it is
    /// newer than the pool's item, and, given `kept`, only while the pool's
    /// item names that list: the list the item it replaced named, if any.
    async fn put(
        &self,
        class: &str,
        pool: &str,
        version: Version,
        item: Item,
        kept: Option<ListKey>,
    ) -> Result<Option<ListKey>, Refusal> {
        let (mut names, mut values) = item::newer(version);
        let condition = match kept {
            None => item::NEWER.to_owned(),
            Some(kept) => {
                names.insert("#names".into(), item::NAMES.into());
                values.insert(":names".into(), AttributeValue::S(kept.sort_key()));
                format!("({}) AND #names = :names", item::NEWER)
            }
        };
        let put = self
            .shared
            .client
            .put_item()
            .table_name(&self.shared.tables.records)
            .set_item(Some(item))
            .condition_expression(condition)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .return_values(ReturnValue::AllOld)
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await;
        match put {
            Ok(put) => Ok(put.attributes().and_then(item::names_list)),
            Err(SdkError::ServiceError(failed))
                if matches!(
                    failed.err(),
                    PutItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                let held = match failed.err() {
                    PutItemError::ConditionalCheckFailedException(refused) => refused.item(),
                    _ => None,
                };
                let held = match held.and_then(item::version) {
                    Some(held) if held >= version => held,
                    // Older, or no item at all: only the list it names
                    // refused a write that kept one.
                    _ if kept.is_some() => {
                        return Err(Refusal::Elsewhere(DirectoryError::Unavailable(format!(
                            "pool `{pool}` of class `{class}`: a write at {version} found the pool's item naming another list"
                        ))));
                    }
                    // The table refused the write, so it holds a newer one;
                    // a table that does not say which is not DynamoDB.
                    _ => {
                        return Err(Refusal::Failed(DirectoryError::Unavailable(format!(
                            "pool `{pool}` of class `{class}`: a write at {version} was refused without a newer item that refused it"
                        ))));
                    }
                };
                Err(Refusal::NotNewer {
                    error: DirectoryError::NotNewer {
                        class: class.to_owned(),
                        pool: pool.to_owned(),
                        held,
                        offered: version,
                    },
                })
            }
            Err(error) => Err(Refusal::Failed(unavailable(
                &format!("writing pool `{pool}` of class `{class}`"),
                error,
            ))),
        }
    }

    /// Mark `key`'s list of `pool` to expire: a write replaced the record
    /// that named it, at a higher `models_version`. Only the writer that put
    /// the list names it, and only while the pool's item names it already,
    /// so no record of the pool names it again
    /// (`docs/src/development/pools-design.md`). Best effort: a list left in
    /// place costs only its storage.
    async fn supersede(&self, class: &str, pool: &str, key: ListKey) {
        let expires = SystemTime::now()
            .checked_add(SUPERSEDED)
            .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
            .map_or(u64::MAX, |at| at.as_secs());
        let updated = self
            .shared
            .client
            .update_item()
            .table_name(&self.shared.tables.lists)
            .set_key(Some(item::list_key(class, pool, key)))
            // An update makes the item it names, and TTL may have removed
            // this one already.
            .condition_expression("attribute_exists(#list)")
            .update_expression("SET #expires = :expires")
            .expression_attribute_names("#list", item::LIST_KEY)
            .expression_attribute_names("#expires", item::EXPIRES_AT)
            .expression_attribute_values(":expires", item::number(expires))
            .send()
            .await;
        if let Err(error) = updated {
            tracing::debug!(class, pool, error = %DisplayErrorContext(error), "a superseded model list was not marked to expire");
        }
    }

    fn written(&self) -> MutexGuard<'_, HashMap<(String, String), Written>> {
        self.shared
            .written
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Put a model list.
    async fn put_list(&self, pool: &str, list: Item) -> Result<(), DirectoryError> {
        self.shared
            .client
            .put_item()
            .table_name(&self.shared.tables.lists)
            .set_item(Some(list))
            .send()
            .await
            .map(drop)
            .map_err(|error| {
                unavailable(&format!("writing the model list of pool `{pool}`"), error)
            })
    }

    /// Write a pool's item, as [`Self::put`] does, and then mark the list
    /// of the item it replaced to expire, if the new item is a record at a
    /// higher `models_version`. A withdrawal expires nothing.
    async fn write(
        &self,
        class: &str,
        pool: &str,
        version: Version,
        item: Item,
        models_version: Option<u64>,
        kept: Option<ListKey>,
    ) -> Result<(), Refusal> {
        let before = self.put(class, pool, version, item, kept).await?;
        if let Some(before) =
            before.filter(|before| models_version.is_some_and(|now| now > before.models_version()))
        {
            self.supersede(class, pool, before).await;
        }
        Ok(())
    }
}

/// Why a pool's write was not stored.
enum Refusal {
    /// The table holds a write at least as new.
    NotNewer {
        error: DirectoryError,
    },
    /// A write that kept its handle's list found the pool's item naming
    /// another list, or no item: it puts a list of its own.
    Elsewhere(DirectoryError),
    Failed(DirectoryError),
}

impl From<Refusal> for DirectoryError {
    fn from(refusal: Refusal) -> Self {
        match refusal {
            Refusal::NotNewer { error } | Refusal::Elsewhere(error) | Refusal::Failed(error) => {
                error
            }
        }
    }
}

impl From<DirectoryError> for Refusal {
    fn from(error: DirectoryError) -> Self {
        Self::Failed(error)
    }
}

impl PoolDirectory for DynamoDirectory {
    type Watch = DynamoWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        record.check()?;
        let mut encoded = item::record(&record, item::next_order())?;
        let (class, pool) = (record.class.as_str(), record.name.as_str());
        // One write of the class at a time through this handle, so that a
        // scan never finds one of its writes without the writes before it.
        let io = self.shared.io(class);
        let _io = io.lock().await;
        let owner = (class.to_owned(), pool.to_owned());
        // A hint, not a promise: the table checks that the pool's item
        // still names a kept list. Out while the write runs, so that a write
        // dropped part way leaves the next one to put its list.
        let cached = self.written().remove(&owner);
        let version = record.version();
        let models_version = Some(record.models_version);
        let Some((key, list)) = encoded.list.take() else {
            return Ok(self
                .write(class, pool, version, encoded.item, models_version, None)
                .await?);
        };
        // A write at the epoch of the handle's last write of the pool, with
        // the same list, names that list again, but only while the pool's
        // item still names it: once another item replaced it, a write of
        // the list makes an item of its own. Another epoch is another
        // writer, such as a command or a restarted hub, and always puts its
        // own.
        if let Some(cached) =
            cached.filter(|cached| cached.epoch == record.epoch && cached.key.same_list(&key))
        {
            let mut kept = encoded.item.clone();
            item::name(&mut kept, cached.key);
            match self
                .write(class, pool, version, kept, models_version, Some(cached.key))
                .await
            {
                Err(Refusal::Elsewhere(_)) => {}
                done => {
                    self.written().insert(owner, cached);
                    return Ok(done?);
                }
            }
        }
        self.put_list(pool, list).await?;
        self.write(class, pool, version, encoded.item, models_version, None)
            .await?;
        self.written().insert(
            owner,
            Written {
                key,
                epoch: record.epoch,
            },
        );
        Ok(())
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        withdrawal.check()?;
        let item = item::withdrawal(&withdrawal, item::next_order());
        let io = self.shared.io(&withdrawal.class);
        let _io = io.lock().await;
        Ok(self
            .write(
                &withdrawal.class,
                &withdrawal.pool,
                withdrawal.version(),
                item,
                None,
                None,
            )
            .await?)
    }

    /// The watch begins when it is first polled, which must be inside a
    /// tokio runtime.
    fn watch(&self, class: &str, from: Option<gdl_pools::ResumeToken>) -> DynamoWatch {
        DynamoWatch::new(Arc::clone(&self.shared), class, from)
    }
}

/// A call to DynamoDB that failed, as the directory reports it.
pub(crate) fn unavailable<E, R>(what: &str, error: SdkError<E, R>) -> DirectoryError
where
    E: std::error::Error + 'static,
    R: std::fmt::Debug,
{
    DirectoryError::Unavailable(format!("{what}: {}", DisplayErrorContext(error)))
}
