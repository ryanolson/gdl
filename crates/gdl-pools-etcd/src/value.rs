//! The directory's keys and their values.
//!
//! Per pool, a record key, `<prefix>/<class>/<pool>`, holds the pool's last
//! write without its property list: the record, or the tombstone of its
//! withdrawal. A list key, `<prefix>/<class>/<pool>.list`, holds the list
//! (`properties` and `revoked`). A pool's name is a DNS label, so it never
//! holds `.`, and no pool's record key is another's list key.
//!
//! A record names its list by the list key's revision: `null` when one
//! transaction wrote both keys, so the list is at the record's own
//! revision, or the revision of the list it kept. A reader joins a record
//! only to the list at that revision, never to the pool's latest list, so
//! a record never arrives with another version's list.

use gdl_pools::{PoolRecord, Version, Withdrawal};
use serde_json::{Map, Value};

/// The keys of the record fields that live in the list key.
const LIST_FIELDS: [&str; 2] = ["properties", "revoked"];

/// The suffix of a pool's list key.
pub(crate) const LIST: &str = ".list";

/// The prefix of every key of `class`: with its trailing `/`, so that a
/// class whose name another's begins with reads none of the other's keys.
pub(crate) fn class_prefix(prefix: &str, class: &str) -> String {
    format!("{prefix}/{class}/")
}

/// The record key of `pool` of `class`.
pub(crate) fn record_key(prefix: &str, class: &str, pool: &str) -> String {
    format!("{prefix}/{class}/{pool}")
}

/// The list key of `pool` of `class`.
pub(crate) fn list_key(prefix: &str, class: &str, pool: &str) -> String {
    format!("{prefix}/{class}/{pool}{LIST}")
}

/// The first key past every key that begins with `prefix`: etcd's range
/// end for a prefix.
pub(crate) fn range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

/// What a key under a class's prefix is.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Name<'a> {
    Record(&'a str),
    List(&'a str),
}

/// What the key `key` under `class_prefix` names; None for a key that is
/// neither a record key nor a list key.
pub(crate) fn name<'a>(class_prefix: &str, key: &'a [u8]) -> Option<Name<'a>> {
    let rest = std::str::from_utf8(key).ok()?.strip_prefix(class_prefix)?;
    match rest.strip_suffix(LIST) {
        Some(pool) if !pool.contains(['/', '.']) && !pool.is_empty() => Some(Name::List(pool)),
        Some(_) => None,
        None if !rest.contains(['/', '.']) && !rest.is_empty() => Some(Name::Record(rest)),
        None => None,
    }
}

/// What a record key holds, read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Stored {
    /// A record without its list, and the revision of its list key: None
    /// for the record's own revision.
    Record {
        fields: Map<String, Value>,
        version: Version,
        list: Option<i64>,
    },
    Withdrawn(Withdrawal),
}

impl Stored {
    pub(crate) fn version(&self) -> Version {
        match self {
            Self::Record { version, .. } => *version,
            Self::Withdrawn(withdrawal) => withdrawal.version(),
        }
    }

    /// The revision of the list a record stored at `revision` names.
    pub(crate) fn list_at(&self, revision: i64) -> Option<i64> {
        match self {
            Self::Record { list, .. } => Some(list.unwrap_or(revision)),
            Self::Withdrawn(_) => None,
        }
    }
}

/// A record, split into its record key's fields and its list key's value.
pub(crate) struct Split {
    pub(crate) fields: Map<String, Value>,
    pub(crate) list: Vec<u8>,
}

/// Split `record` into the fields its record key holds and its list.
pub(crate) fn split(record: &PoolRecord) -> Split {
    let Ok(Value::Object(mut fields)) = serde_json::to_value(record) else {
        unreachable!("a record serializes to an object");
    };
    let mut list = Map::new();
    for field in LIST_FIELDS {
        list.insert(
            field.to_owned(),
            fields.remove(field).unwrap_or(Value::Array(Vec::new())),
        );
    }
    let list = serde_json::to_vec(&Value::Object(list)).expect("a list serializes");
    Split { fields, list }
}

/// The record key's value for a record of `fields` whose list is at
/// `list`: None for the record's own revision.
pub(crate) fn encode_record(fields: &Map<String, Value>, list: Option<i64>) -> Vec<u8> {
    let value = serde_json::json!({ "record": fields, "list": list });
    serde_json::to_vec(&value).expect("a record serializes")
}

/// The record key's value for a withdrawal.
pub(crate) fn encode_withdrawal(withdrawal: &Withdrawal) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "withdrawn": withdrawal }))
        .expect("a withdrawal serializes")
}

/// A record key's value, read; None for one this backend did not write.
pub(crate) fn decode(value: &[u8]) -> Option<Stored> {
    let Value::Object(mut stored) = serde_json::from_slice(value).ok()? else {
        return None;
    };
    if let Some(withdrawn) = stored.remove("withdrawn") {
        return serde_json::from_value(withdrawn)
            .ok()
            .map(Stored::Withdrawn);
    }
    let Value::Object(fields) = stored.remove("record")? else {
        return None;
    };
    let number = |key: &str| fields.get(key).and_then(Value::as_u64);
    let version = Version {
        epoch: number("epoch")?,
        seq: number("seq")?,
    };
    let list = match stored.remove("list") {
        None | Some(Value::Null) => None,
        Some(Value::Number(at)) => Some(at.as_i64()?),
        Some(_) => return None,
    };
    Some(Stored::Record {
        fields,
        version,
        list,
    })
}

/// The record of `fields` with the list `list`, whole; None when either
/// does not read as this backend writes them.
pub(crate) fn join(fields: &Map<String, Value>, list: &[u8]) -> Option<PoolRecord> {
    let Value::Object(list) = serde_json::from_slice(list).ok()? else {
        return None;
    };
    let mut whole = fields.clone();
    for field in LIST_FIELDS {
        whole.insert(field.to_owned(), list.get(field)?.clone());
    }
    serde_json::from_value(Value::Object(whole)).ok()
}

#[cfg(test)]
mod tests;
