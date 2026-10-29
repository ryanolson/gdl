//! What a segment instance asks its own pool's hub of the hub's keyed
//! record table, and the hub's answer: request and answer only, with no
//! change messages and no holders. A node may read keyed data from its
//! own pool's hub this way; a router still reads only typed request data,
//! which a node computes from what it read.

use serde::{Deserialize, Serialize};
use velo::InstanceId;

/// The name the keyed record service mounts its routes under, at
/// `/v1/ext/{KEYED_EXTENSION}`, beside the pool's extension
/// ([`crate::EXTENSION`]).
pub const KEYED_EXTENSION: &str = "gdl-keyed";

/// The keyed record service's route, under [`KEYED_EXTENSION`], on the
/// hub's control listener: `POST` with a [`KeyedAsk`], carrying the
/// credential of the instance the path names, which must be registered
/// with this hub. Answered `200 OK` with a [`KeyedReply`] that has one
/// answer for each operation, in order. A refused write is an answer, not
/// an error.
pub const KEYED_ROUTE: &str = "/instances/{instance_id}/keyed";

/// [`KEYED_ROUTE`]'s path for `instance`.
pub fn keyed_path(instance: InstanceId) -> String {
    format!("/instances/{instance}/keyed")
}

/// One request to the keyed record table: its operations, applied in
/// order, each on its own. Many writes in one request cost one round
/// trip, and each gets its own answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyedAsk {
    /// The operations, in order.
    pub ops: Vec<KeyedOp>,
}

/// One operation on the keyed record table. Each moves the record it
/// names, if the table holds one, to the front of the table's
/// least-recently-used order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum KeyedOp {
    /// Read the record of `key`: [`KeyedAnswer::Found`] or
    /// [`KeyedAnswer::Missing`].
    Get {
        /// The key.
        #[serde(with = "text")]
        key: Vec<u8>,
    },
    /// Write `value` to `key` if the record matches `expected`:
    /// [`KeyedAnswer::Applied`] with the write's new version, or
    /// [`KeyedAnswer::Refused`] with the record as it is.
    Set {
        /// The key.
        #[serde(with = "text")]
        key: Vec<u8>,
        /// The value.
        #[serde(with = "text")]
        value: Vec<u8>,
        /// What the record must be for the write to apply.
        expected: Expected,
    },
    /// Ask whether the record of `key` is still at `version`:
    /// [`KeyedAnswer::Unchanged`], [`KeyedAnswer::Newer`] with the record,
    /// or [`KeyedAnswer::Gone`].
    Check {
        /// The key.
        #[serde(with = "text")]
        key: Vec<u8>,
        /// The version the asker holds.
        version: u64,
    },
}

/// What a record must be for a write to apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expected {
    /// Whatever it is: the write always applies.
    Replace,
    /// The table holds no record of the key.
    Absent,
    /// The table holds the record at this version.
    Version(u64),
}

/// A record of the keyed record table: its value and its version. Each
/// write the table applies gets a version greater than any it gave
/// before, so a reader that holds two records of one key keeps the one
/// with the greater version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyedRecord {
    /// The value.
    #[serde(with = "text")]
    pub value: Vec<u8>,
    /// The version of the write that stored it.
    pub version: u64,
}

/// The answer to one [`KeyedOp`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "answer")]
pub enum KeyedAnswer {
    /// A get found the record.
    Found {
        /// The record.
        record: KeyedRecord,
    },
    /// A get found no record.
    Missing,
    /// A set applied, at this version.
    Applied {
        /// The write's version.
        version: u64,
    },
    /// A set did not apply, because the record did not match: the record
    /// as it is, or none.
    Refused {
        /// The record the table holds, if any.
        current: Option<KeyedRecord>,
    },
    /// A check found the record at the version asked about.
    Unchanged,
    /// A check found the record at another version.
    Newer {
        /// The record.
        record: KeyedRecord,
    },
    /// A check found no record: the table evicted it, or never held it.
    Gone,
}

/// The hub's answer to a [`KeyedAsk`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyedReply {
    /// One answer for each operation, in order.
    pub answers: Vec<KeyedAnswer>,
    /// The age, in milliseconds, of the table's least recently used
    /// record once the operations applied: about how long a record that
    /// nobody reads or writes stays before the table evicts it. `None`
    /// while the table is empty. A reader that caches a record checks it
    /// again well within this, so that a record in use is touched before
    /// it would be evicted.
    pub eviction_age_ms: Option<u64>,
}

/// Bytes as standard base64 text: JSON has no bytes, and an array of
/// numbers costs about four characters a byte.
mod text {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        STANDARD
            .decode(text.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keys and values travel as base64 text, and come back as the bytes
    /// they were, whatever the bytes; each operation and answer is tagged.
    #[test]
    fn an_ask_and_a_reply_round_trip_as_tagged_json() {
        let ask = KeyedAsk {
            ops: vec![
                KeyedOp::Get { key: vec![0, 255] },
                KeyedOp::Set {
                    key: b"k".to_vec(),
                    value: (0..=255).collect(),
                    expected: Expected::Version(7),
                },
                KeyedOp::Set {
                    key: vec![],
                    value: vec![],
                    expected: Expected::Absent,
                },
                KeyedOp::Check {
                    key: b"k".to_vec(),
                    version: 3,
                },
            ],
        };
        let text = serde_json::to_string(&ask).expect("json");
        assert!(
            text.starts_with(r#"{"ops":[{"op":"get","key":"AP8="}"#),
            "{text}"
        );
        assert!(text.contains(r#""expected":{"version":7}"#), "{text}");
        assert!(text.contains(r#""expected":"absent""#), "{text}");
        assert_eq!(serde_json::from_str::<KeyedAsk>(&text).expect("json"), ask);
        let record = KeyedRecord {
            value: b"model-x-02".to_vec(),
            version: 9,
        };
        let reply = KeyedReply {
            answers: vec![
                KeyedAnswer::Found {
                    record: record.clone(),
                },
                KeyedAnswer::Missing,
                KeyedAnswer::Applied { version: 10 },
                KeyedAnswer::Refused { current: None },
                KeyedAnswer::Refused {
                    current: Some(record.clone()),
                },
                KeyedAnswer::Unchanged,
                KeyedAnswer::Newer { record },
                KeyedAnswer::Gone,
            ],
            eviction_age_ms: Some(12),
        };
        let text = serde_json::to_string(&reply).expect("json");
        assert!(text.contains(r#"{"answer":"missing"}"#), "{text}");
        assert_eq!(
            serde_json::from_str::<KeyedReply>(&text).expect("json"),
            reply
        );
    }

    /// A key that is not base64 is refused where it is read, not stored
    /// as something else.
    #[test]
    fn a_key_that_is_not_base64_is_refused() {
        let refused = serde_json::from_str::<KeyedOp>(r#"{"op":"get","key":"not base64!"}"#);
        assert!(refused.is_err(), "{refused:?}");
    }
}
