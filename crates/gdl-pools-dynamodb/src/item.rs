//! The items of the two tables, and how a pool's write and its property list
//! become them. Pure: nothing here calls DynamoDB.
//!
//! The records table holds one item per pool, keyed by `class` and
//! `pool`: the record without its property list, or a withdrawal's
//! tombstone. The lists table holds each property list a record names, keyed
//! by `owner` (`<class>/<pool>`) and `list`: the record's `properties_version`,
//! a digest of the list, and the token of the put that made it. Each put
//! makes an item of its own, so no put replaces another's and an expiry
//! names one put's item; a reader that checks the digest never joins a
//! record to another version's list.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use aws_sdk_dynamodb::types::AttributeValue;
use gdl_pools::{
    DirectoryError, PoolEvent, PoolProperty, PoolRecord, ResumeToken, Version, Withdrawal,
};
use serde::{Deserialize, Serialize};

/// An item of either table, as the SDK holds it.
pub(crate) type Item = HashMap<String, AttributeValue>;

/// The records table's partition key: the pool's class.
pub(crate) const CLASS: &str = "class";
/// The records table's sort key: the pool's name.
pub(crate) const POOL: &str = "pool";
const EPOCH: &str = "epoch";
const SEQ: &str = "seq";
const ORDER: &str = "order";
const KIND: &str = "kind";
const BODY: &str = "body";
const LIST: &str = "list";
/// The lists table's partition key: `<class>/<pool>`.
pub(crate) const OWNER: &str = "owner";
/// The lists table's sort key.
pub(crate) const LIST_KEY: &str = LIST;
/// The records table's attribute that holds the key of the record's list.
pub(crate) const NAMES: &str = LIST;
/// The lists table's TTL attribute: set only on a list its pool has moved
/// past, never on one a record names.
pub(crate) const EXPIRES_AT: &str = "expires_at";

const RECORD: &str = "record";
const WITHDRAWN: &str = "withdrawn";

/// DynamoDB's limit on one item, in bytes: the names and values of its
/// attributes.
const ITEM_LIMIT: usize = 400 * 1024;

/// The condition of every write of a pool's item: newer than what the
/// table holds, by epoch, then by `seq`. An item without an epoch holds
/// no pool's write, and is written over.
pub(crate) const NEWER: &str =
    "attribute_not_exists(#epoch) OR #epoch < :epoch OR (#epoch = :epoch AND #seq < :seq)";

/// The names and values [`NEWER`] reads, for a write at `version`.
pub(crate) fn newer(version: Version) -> (HashMap<String, String>, Item) {
    (
        HashMap::from([("#epoch".into(), EPOCH.into()), ("#seq".into(), SEQ.into())]),
        Item::from([
            (":epoch".into(), number(version.epoch)),
            (":seq".into(), number(version.seq)),
        ]),
    )
}

/// The key of a property list in the lists table: the record's
/// `properties_version`, a digest of the list's stored body, and the token of
/// the put that made the item, which no other put shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ListKey {
    properties_version: u64,
    digest: u64,
    token: u64,
}

impl ListKey {
    /// The `properties_version` of the record that names the list.
    pub(crate) fn properties_version(&self) -> u64 {
        self.properties_version
    }

    /// Whether `other` keys the same list, whatever put made it.
    pub(crate) fn same_list(&self, other: &Self) -> bool {
        self.properties_version == other.properties_version && self.digest == other.digest
    }

    /// The sort key: zero-padded, so that a class's lists sort by version,
    /// and of one length for every token.
    pub(crate) fn sort_key(&self) -> String {
        format!(
            "{:020}.{:016x}.{:016x}",
            self.properties_version, self.digest, self.token
        )
    }

    fn parse(key: &str) -> Option<Self> {
        let mut parts = key.split('.');
        let key = Self {
            properties_version: parts.next()?.parse().ok()?,
            digest: u64::from_str_radix(parts.next()?, 16).ok()?,
            token: u64::from_str_radix(parts.next()?, 16).ok()?,
        };
        parts.next().is_none().then_some(key)
    }
}

/// What the lists table holds of a record: the two fields of a record
/// that grow with its properties.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(from = "ListWire")]
pub(crate) struct List {
    #[serde(default)]
    pub(crate) properties: Vec<PoolProperty>,
    #[serde(default)]
    pub(crate) revoked: Vec<gdl_model::PropertyKey>,
    /// The kind of each property or revocation that did not check when
    /// the body was read, and that it left out. Not stored.
    #[serde(skip)]
    pub(crate) dropped: Vec<String>,
}

/// A list's body as it is read: each property and revocation read on its
/// own, so that one that does not check never costs the pool its list.
#[derive(Deserialize)]
struct ListWire {
    #[serde(default)]
    properties: Vec<serde_json::Value>,
    #[serde(default)]
    revoked: Vec<serde_json::Value>,
}

impl From<ListWire> for List {
    fn from(wire: ListWire) -> Self {
        let (properties, mut dropped) = gdl_model::read_each(wire.properties);
        let (revoked, unrevoked) = gdl_model::read_each(wire.revoked);
        dropped.extend(unrevoked);
        Self {
            properties,
            revoked,
            dropped,
        }
    }
}

/// A pool's write, as the two tables hold it.
#[derive(Debug, Clone)]
pub(crate) struct Encoded {
    /// The records table's item.
    pub(crate) item: Item,
    /// The record's list, with its key and the lists table's item, when
    /// the record has properties or revocations. The key has a new token, and
    /// [`Encoded::item`] names it.
    pub(crate) list: Option<(ListKey, Item)>,
}

/// The items of `record`, written at `order`, with a new token for its
/// list. A record whose list or whose own item would pass DynamoDB's item
/// limit is refused.
pub(crate) fn record(record: &PoolRecord, order: u64) -> Result<Encoded, DirectoryError> {
    let list = (!record.properties.is_empty() || !record.revoked.is_empty()).then(|| List {
        dropped: Vec::new(),
        properties: record.properties.clone(),
        revoked: record.revoked.clone(),
    });
    let list = match list {
        None => None,
        Some(list) => {
            let body = serde_json::to_string(&list).expect("a property list serializes");
            let key = ListKey {
                properties_version: record.properties_version,
                digest: digest(body.as_bytes()),
                token: new_token(),
            };
            let item = Item::from([
                (
                    OWNER.into(),
                    AttributeValue::S(owner(&record.class, &record.name)),
                ),
                (LIST_KEY.into(), AttributeValue::S(key.sort_key())),
                (BODY.into(), AttributeValue::S(body)),
            ]);
            fits(&item, &record.name, "property list")?;
            Some((key, item))
        }
    };
    let bare = PoolRecord {
        properties: Vec::new(),
        revoked: Vec::new(),
        ..record.clone()
    };
    let mut item = keyed(&record.class, &record.name, record.version(), order);
    item.insert(KIND.into(), AttributeValue::S(RECORD.into()));
    item.insert(
        BODY.into(),
        AttributeValue::S(serde_json::to_string(&bare).expect("a record serializes")),
    );
    if let Some((key, _)) = &list {
        name(&mut item, *key);
    }
    fits(&item, &record.name, "record")?;
    Ok(Encoded { item, list })
}

/// The tombstone item of `withdrawal`, written at `order`.
pub(crate) fn withdrawal(withdrawal: &Withdrawal, order: u64) -> Item {
    let mut item = keyed(
        &withdrawal.class,
        &withdrawal.pool,
        withdrawal.version(),
        order,
    );
    item.insert(KIND.into(), AttributeValue::S(WITHDRAWN.into()));
    item
}

/// The lists table's key of `key`'s list of `pool` of `class`.
pub(crate) fn list_key(class: &str, pool: &str, key: ListKey) -> Item {
    Item::from([
        (OWNER.into(), AttributeValue::S(owner(class, pool))),
        (LIST_KEY.into(), AttributeValue::S(key.sort_key())),
    ])
}

/// A pool's item as a scan reads it: the record without its list, and the
/// key of the list it names, or a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub(crate) pool: String,
    pub(crate) version: Version,
    pub(crate) order: u64,
    pub(crate) held: StoredHeld,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoredHeld {
    Record {
        record: Box<PoolRecord>,
        list: Option<ListKey>,
    },
    Withdrawn,
}

/// The pool's write `item` holds, if it is one of `class`: a record a
/// publish would take, of the pool the item is keyed by, at the item's
/// version, or a tombstone. None for anything else, which a scan passes
/// over.
pub(crate) fn decode(class: &str, item: &Item) -> Option<Stored> {
    if text(item, CLASS)? != class {
        return None;
    }
    let pool = text(item, POOL)?;
    let version = version(item)?;
    let order = count(item, ORDER)?;
    let held = match text(item, KIND)? {
        WITHDRAWN => StoredHeld::Withdrawn,
        RECORD => {
            let record: PoolRecord = serde_json::from_str(text(item, BODY)?).ok()?;
            let belongs = record.name == pool
                && record.class == class
                && record.version() == version
                && record.properties.is_empty()
                && record.revoked.is_empty()
                && record.check().is_ok();
            if !belongs {
                return None;
            }
            let list = match item.get(LIST) {
                None => None,
                Some(AttributeValue::S(key)) => Some(ListKey::parse(key)?),
                Some(_) => return None,
            };
            if list.is_some_and(|list| list.properties_version != record.properties_version) {
                return None;
            }
            StoredHeld::Record {
                record: Box::new(record),
                list,
            }
        }
        _ => return None,
    };
    Some(Stored {
        pool: pool.to_owned(),
        version,
        order,
        held,
    })
}

/// The list `item` holds for `key`, if its body is the one `key`'s digest
/// names. None for a body of another list, which a reader takes as a list
/// still on its way.
pub(crate) fn decode_list(key: ListKey, item: &Item) -> Option<List> {
    let body = text(item, BODY)?;
    if text(item, LIST_KEY)? != key.sort_key() || digest(body.as_bytes()) != key.digest {
        return None;
    }
    serde_json::from_str(body).ok()
}

/// The version of the pool's write a records-table item holds, if it
/// holds one.
pub(crate) fn version(item: &Item) -> Option<Version> {
    Some(Version {
        epoch: count(item, EPOCH)?,
        seq: count(item, SEQ)?,
    })
}

/// The key of the list a records-table item names, if it names one.
pub(crate) fn names_list(item: &Item) -> Option<ListKey> {
    match item.get(NAMES)? {
        AttributeValue::S(key) => ListKey::parse(key),
        _ => None,
    }
}

/// Make the records-table item `record` name the list `key`.
pub(crate) fn name(record: &mut Item, key: ListKey) {
    record.insert(NAMES.into(), AttributeValue::S(key.sort_key()));
}

/// A token for a new put of a list: random, so that two writers in any
/// processes do not share one.
fn new_token() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(next_order())
}

/// A pool's write, whole, as a scan installs it: a record with its list
/// joined, or a withdrawal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    pub(crate) version: Version,
    pub(crate) order: u64,
    pub(crate) list: Option<ListKey>,
    pub(crate) held: Held,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Held {
    Record(Box<PoolRecord>),
    Withdrawn(Withdrawal),
}

impl Entry {
    /// The entry of `stored`, a write of `class`, with `list` joined to a
    /// record that names one.
    pub(crate) fn new(class: &str, stored: Stored, list: Option<List>) -> Self {
        let (held, key) = match stored.held {
            StoredHeld::Withdrawn => (
                Held::Withdrawn(Withdrawal {
                    class: class.to_owned(),
                    pool: stored.pool,
                    epoch: stored.version.epoch,
                    seq: stored.version.seq,
                }),
                None,
            ),
            StoredHeld::Record {
                mut record,
                list: key,
            } => {
                if let Some(list) = list {
                    if !list.dropped.is_empty() {
                        tracing::debug!(
                            class,
                            pool = %record.name,
                            kinds = ?list.dropped,
                            "a pool's list holds properties that do not check: they are left out"
                        );
                    }
                    record.properties = list.properties;
                    record.revoked = list.revoked;
                    record.dropped.extend(list.dropped);
                }
                (Held::Record(record), key)
            }
        };
        Self {
            version: stored.version,
            order: stored.order,
            list: key,
            held,
        }
    }

    pub(crate) fn record(&self) -> Option<&PoolRecord> {
        match &self.held {
            Held::Record(record) => Some(record),
            Held::Withdrawn(_) => None,
        }
    }

    /// The change this entry makes, as a watch reports it at `token`.
    pub(crate) fn event(&self, token: ResumeToken) -> PoolEvent {
        match &self.held {
            Held::Record(record) => PoolEvent::Put {
                record: PoolRecord::clone(record),
                token,
            },
            Held::Withdrawn(withdrawal) => PoolEvent::Withdrawn {
                withdrawal: withdrawal.clone(),
                token,
            },
        }
    }
}

/// The order of a write made now: this host's wall clock in nanoseconds,
/// and always past this process's last write's, though the clock steps
/// back. A scan finds several changes at once and orders them by it, so
/// one process's writes are seen in the order it made them.
pub(crate) fn next_order() -> u64 {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        });
    let previous = LAST
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
            Some(now.max(last.saturating_add(1)))
        })
        .expect("the update always succeeds");
    now.max(previous.saturating_add(1))
}

/// A number attribute.
pub(crate) fn number(value: u64) -> AttributeValue {
    AttributeValue::N(value.to_string())
}

fn keyed(class: &str, pool: &str, version: Version, order: u64) -> Item {
    Item::from([
        (CLASS.into(), AttributeValue::S(class.into())),
        (POOL.into(), AttributeValue::S(pool.into())),
        (EPOCH.into(), number(version.epoch)),
        (SEQ.into(), number(version.seq)),
        (ORDER.into(), number(order)),
    ])
}

fn owner(class: &str, pool: &str) -> String {
    format!("{class}/{pool}")
}

fn text<'a>(item: &'a Item, name: &str) -> Option<&'a str> {
    match item.get(name)? {
        AttributeValue::S(text) => Some(text),
        _ => None,
    }
}

fn count(item: &Item, name: &str) -> Option<u64> {
    match item.get(name)? {
        AttributeValue::N(number) => number.parse().ok(),
        _ => None,
    }
}

/// Refuse `item` when it is past DynamoDB's item limit, which would fail
/// the write anyway, and fail it the same way on every attempt.
fn fits(item: &Item, pool: &str, what: &str) -> Result<(), DirectoryError> {
    let size: usize = item
        .iter()
        .map(|(name, value)| {
            name.len()
                + match value {
                    AttributeValue::S(text) | AttributeValue::N(text) => text.len(),
                    _ => 0,
                }
        })
        .sum();
    if size > ITEM_LIMIT {
        return Err(DirectoryError::Refused(format!(
            "pool `{pool}`: its {what} is {size} bytes in DynamoDB, over the item limit of {ITEM_LIMIT}"
        )));
    }
    Ok(())
}

/// FNV-1a over the list's stored body: the same in every process and
/// build, which is all a list's key needs.
fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests;
