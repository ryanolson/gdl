//! The items of the two tables, and how a cell's write and its model list
//! become them. Pure: nothing here calls DynamoDB.
//!
//! The records table holds one item per cell, keyed by `class` and
//! `cell`: the record without its model list, or a withdrawal's
//! tombstone. The lists table holds each model list a record names, keyed
//! by `owner` (`<class>/<cell>`) and `list`, the record's `models_version`
//! and a digest of the list. A list's key names its content, so two
//! writers of one cell never overwrite each other's list, and a reader
//! that checks the digest never joins a record to another version's list.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use aws_sdk_dynamodb::types::AttributeValue;
use gdl_cells::{
    CellEvent, CellRecord, DirectoryError, ModelEntry, ResumeToken, Version, Withdrawal,
};
use serde::{Deserialize, Serialize};

/// An item of either table, as the SDK holds it.
pub(crate) type Item = HashMap<String, AttributeValue>;

/// The records table's partition key: the cell's class.
pub(crate) const CLASS: &str = "class";
/// The records table's sort key: the cell's name.
pub(crate) const CELL: &str = "cell";
const EPOCH: &str = "epoch";
const SEQ: &str = "seq";
const ORDER: &str = "order";
const KIND: &str = "kind";
const BODY: &str = "body";
const LIST: &str = "list";
/// The lists table's partition key: `<class>/<cell>`.
pub(crate) const OWNER: &str = "owner";
/// The lists table's sort key.
pub(crate) const LIST_KEY: &str = LIST;
/// The token of one put of a list, in the list's item and in the record
/// written after it: an expiry holds only while the list is still that
/// put, so a later put of the same list, by any writer, keeps it.
pub(crate) const PUT: &str = "put";
/// The records table's attribute that holds [`PUT`] of the record's list.
pub(crate) const LIST_PUT: &str = "list_put";
/// The lists table's TTL attribute: set only on a list its cell has moved
/// past, never on one a record names.
pub(crate) const EXPIRES_AT: &str = "expires_at";

const RECORD: &str = "record";
const WITHDRAWN: &str = "withdrawn";

/// DynamoDB's limit on one item, in bytes: the names and values of its
/// attributes.
const ITEM_LIMIT: usize = 400 * 1024;

/// The condition of every write of a cell's item: newer than what the
/// table holds, by epoch, then by `seq`. An item without an epoch holds
/// no cell's write, and is written over.
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

/// The key of a model list in the lists table: the record's
/// `models_version` and a digest of the list's stored body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ListKey {
    models_version: u64,
    digest: u64,
}

impl ListKey {
    /// The `models_version` of the record that names the list.
    pub(crate) fn models_version(&self) -> u64 {
        self.models_version
    }

    /// The sort key: zero-padded, so that a class's lists sort by version.
    pub(crate) fn sort_key(&self) -> String {
        format!("{:020}.{:016x}", self.models_version, self.digest)
    }

    fn parse(key: &str) -> Option<Self> {
        let (version, digest) = key.split_once('.')?;
        Some(Self {
            models_version: version.parse().ok()?,
            digest: u64::from_str_radix(digest, 16).ok()?,
        })
    }
}

/// What the lists table holds of a record: the two fields of a record
/// that grow with its models.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct List {
    #[serde(default)]
    pub(crate) models: Vec<ModelEntry>,
    #[serde(default)]
    pub(crate) revoked: Vec<gdl_model::ModelKey>,
}

/// A cell's write, as the two tables hold it.
#[derive(Debug, Clone)]
pub(crate) struct Encoded {
    /// The records table's item.
    pub(crate) item: Item,
    /// The record's list, with its key and the lists table's item, when
    /// the record has models or revocations.
    pub(crate) list: Option<(ListKey, Item)>,
}

/// The items of `record`, written at `order`. A record whose list or
/// whose own item would pass DynamoDB's item limit is refused.
pub(crate) fn record(record: &CellRecord, order: u64) -> Result<Encoded, DirectoryError> {
    let list = (!record.models.is_empty() || !record.revoked.is_empty()).then(|| List {
        models: record.models.clone(),
        revoked: record.revoked.clone(),
    });
    let list = match list {
        None => None,
        Some(list) => {
            let body = serde_json::to_string(&list).expect("a model list serializes");
            let key = ListKey {
                models_version: record.models_version,
                digest: digest(body.as_bytes()),
            };
            let item = Item::from([
                (
                    OWNER.into(),
                    AttributeValue::S(owner(&record.class, &record.name)),
                ),
                (LIST_KEY.into(), AttributeValue::S(key.sort_key())),
                (BODY.into(), AttributeValue::S(body)),
            ]);
            fits(&item, &record.name, "model list")?;
            Some((key, item))
        }
    };
    let bare = CellRecord {
        models: Vec::new(),
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
        item.insert(LIST.into(), AttributeValue::S(key.sort_key()));
    }
    fits(&item, &record.name, "record")?;
    Ok(Encoded { item, list })
}

/// The tombstone item of `withdrawal`, written at `order`.
pub(crate) fn withdrawal(withdrawal: &Withdrawal, order: u64) -> Item {
    let mut item = keyed(
        &withdrawal.class,
        &withdrawal.cell,
        withdrawal.version(),
        order,
    );
    item.insert(KIND.into(), AttributeValue::S(WITHDRAWN.into()));
    item
}

/// The lists table's key of `key`'s list of `cell` of `class`.
pub(crate) fn list_key(class: &str, cell: &str, key: ListKey) -> Item {
    Item::from([
        (OWNER.into(), AttributeValue::S(owner(class, cell))),
        (LIST_KEY.into(), AttributeValue::S(key.sort_key())),
    ])
}

/// A cell's item as a scan reads it: the record without its list, and the
/// key of the list it names, or a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub(crate) cell: String,
    pub(crate) version: Version,
    pub(crate) order: u64,
    pub(crate) held: StoredHeld,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoredHeld {
    Record {
        record: Box<CellRecord>,
        list: Option<ListKey>,
    },
    Withdrawn,
}

/// The cell's write `item` holds, if it is one of `class`: a record a
/// publish would take, of the cell the item is keyed by, at the item's
/// version, or a tombstone. None for anything else, which a scan passes
/// over.
pub(crate) fn decode(class: &str, item: &Item) -> Option<Stored> {
    if text(item, CLASS)? != class {
        return None;
    }
    let cell = text(item, CELL)?;
    let version = version(item)?;
    let order = count(item, ORDER)?;
    let held = match text(item, KIND)? {
        WITHDRAWN => StoredHeld::Withdrawn,
        RECORD => {
            let record: CellRecord = serde_json::from_str(text(item, BODY)?).ok()?;
            let belongs = record.name == cell
                && record.class == class
                && record.version() == version
                && record.models.is_empty()
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
            if list.is_some_and(|list| list.models_version != record.models_version) {
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
        cell: cell.to_owned(),
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

/// The version of the cell's write a records-table item holds, if it
/// holds one.
pub(crate) fn version(item: &Item) -> Option<Version> {
    Some(Version {
        epoch: count(item, EPOCH)?,
        seq: count(item, SEQ)?,
    })
}

/// The key of the list a records-table item names, and the token of the
/// list's put that the item followed, if it names one.
pub(crate) fn names_list(item: &Item) -> Option<(ListKey, u64)> {
    let key = match item.get(LIST)? {
        AttributeValue::S(key) => ListKey::parse(key)?,
        _ => return None,
    };
    Some((key, count(item, LIST_PUT)?))
}

/// Stamp a write's list item, and the record item that names it, with the
/// token of the list's put.
pub(crate) fn stamp(list: &mut Item, record: &mut Item, put: u64) {
    list.insert(PUT.into(), number(put));
    record.insert(LIST_PUT.into(), number(put));
}

/// A token for a new put of a list: random, so that two writers in any
/// processes do not share one.
pub(crate) fn new_put() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(next_order())
}

/// A cell's write, whole, as a scan installs it: a record with its list
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
    Record(Box<CellRecord>),
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
                    cell: stored.cell,
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
                    record.models = list.models;
                    record.revoked = list.revoked;
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

    pub(crate) fn record(&self) -> Option<&CellRecord> {
        match &self.held {
            Held::Record(record) => Some(record),
            Held::Withdrawn(_) => None,
        }
    }

    /// The change this entry makes, as a watch reports it at `token`.
    pub(crate) fn event(&self, token: ResumeToken) -> CellEvent {
        match &self.held {
            Held::Record(record) => CellEvent::Put {
                record: CellRecord::clone(record),
                token,
            },
            Held::Withdrawn(withdrawal) => CellEvent::Withdrawn {
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

fn keyed(class: &str, cell: &str, version: Version, order: u64) -> Item {
    Item::from([
        (CLASS.into(), AttributeValue::S(class.into())),
        (CELL.into(), AttributeValue::S(cell.into())),
        (EPOCH.into(), number(version.epoch)),
        (SEQ.into(), number(version.seq)),
        (ORDER.into(), number(order)),
    ])
}

fn owner(class: &str, cell: &str) -> String {
    format!("{class}/{cell}")
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
fn fits(item: &Item, cell: &str, what: &str) -> Result<(), DirectoryError> {
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
            "cell `{cell}`: its {what} is {size} bytes in DynamoDB, over the item limit of {ITEM_LIMIT}"
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
