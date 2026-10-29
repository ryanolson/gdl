use gdl_model::PropertyKey;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::*;

/// The test property type: a name, then a partition and a version, both
/// optional, in that order, so that its derived `Ord` sorts by name, then
/// partition, then version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Test {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    partition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<String>,
}

const TEST: PropertyKind<Test> = PropertyKind::new("test");

fn test(name: &str, partition: Option<&str>, version: Option<&str>) -> Test {
    Test {
        name: name.into(),
        partition: partition.map(Into::into),
        version: version.map(Into::into),
    }
}

fn pool(name: &str, state: PropertyState) -> PropertyPool {
    PropertyPool::new(name, "us-east", state, PoolCondition::Live)
}

fn names(listed: &[(Test, PropertyPool)]) -> Vec<&str> {
    listed.iter().map(|(_, pool)| pool.pool()).collect()
}

/// The range of the keys that share a name and a partition: every
/// version, from the start of the range to its end.
fn named<'a>(
    index: &'a PropertyIndex<Test>,
    name: &str,
    partition: Option<&str>,
) -> &'a [(Test, PropertyPool)] {
    index.range_by(|key| (key.name.as_str(), key.partition.as_deref()).cmp(&(name, partition)))
}

/// A key is found by its value, with its pools by name; another
/// partition is another key, and a key no pool lists finds nothing. A
/// pool that lists one key twice keeps the first.
#[test]
fn a_key_is_found_by_its_value() {
    let index = PropertyIndex::new([
        (
            test("qwen", None, None),
            pool("model-x-02", PropertyState::Starting),
        ),
        (
            test("qwen", None, None),
            pool("model-x-01", PropertyState::Ready),
        ),
        (
            test("qwen", Some("p1"), None),
            pool("model-x-03", PropertyState::Ready),
        ),
        (
            test("llama", None, None),
            pool("model-x-01", PropertyState::Drained),
        ),
        (
            test("qwen", None, None),
            pool("model-x-01", PropertyState::Unavailable),
        ),
    ]);
    let qwen = index.get(&test("qwen", None, None));
    assert_eq!(names(qwen), ["model-x-01", "model-x-02"]);
    assert_eq!(qwen[0].1.state(), PropertyState::Ready, "the first kept");
    assert_eq!(
        names(index.get(&test("qwen", Some("p1"), None))),
        ["model-x-03"]
    );
    assert_eq!(index.get(&test("llama", None, None)).len(), 1);
    assert!(index.get(&test("mistral", None, None)).is_empty());
    assert!(index.get(&test("qwen", Some("p2"), None)).is_empty());
    let listed: Vec<&Test> = index.entries().iter().map(|(key, _)| key).collect();
    assert_eq!(
        listed,
        [
            &test("llama", None, None),
            &test("qwen", None, None),
            &test("qwen", None, None),
            &test("qwen", Some("p1"), None),
        ]
    );
}

/// A range by leading fields keeps every key in it, several of one pool
/// among them, each with its own state: the index merges no states.
#[test]
fn a_range_keeps_several_entries_of_one_pool_each_with_its_state() {
    let index = PropertyIndex::new([
        (
            test("qwen", None, Some("v1")),
            pool("model-x-01", PropertyState::Draining),
        ),
        (
            test("qwen", None, Some("v2")),
            pool("model-x-01", PropertyState::Ready),
        ),
        (
            test("qwen", None, Some("v3")),
            pool("model-x-01", PropertyState::Starting),
        ),
        (
            test("qwen", None, Some("v2")),
            pool("model-x-02", PropertyState::Unavailable),
        ),
        (
            test("llama", None, Some("v1")),
            pool("model-x-01", PropertyState::Ready),
        ),
        (
            test("qwen", Some("p"), Some("v1")),
            pool("model-x-03", PropertyState::Ready),
        ),
    ]);
    let range = named(&index, "qwen", None);
    let seen: Vec<(Option<&str>, &str, PropertyState)> = range
        .iter()
        .map(|(key, pool)| (key.version.as_deref(), pool.pool(), pool.state()))
        .collect();
    assert_eq!(
        seen,
        [
            (Some("v1"), "model-x-01", PropertyState::Draining),
            (Some("v2"), "model-x-01", PropertyState::Ready),
            (Some("v2"), "model-x-02", PropertyState::Unavailable),
            (Some("v3"), "model-x-01", PropertyState::Starting),
        ]
    );
    assert!(named(&index, "mistral", None).is_empty());
    assert_eq!(names(named(&index, "llama", None)), ["model-x-01"]);
}

/// A request that names no partition never reaches a partitioned key,
/// whatever its version: the partition comes before the version in the
/// type, so the range of `(name, None)` ends before any partition.
#[test]
fn a_request_with_no_partition_never_reaches_a_partitioned_key() {
    let index = PropertyIndex::new([
        (
            test("qwen", Some("a"), None),
            pool("model-x-01", PropertyState::Ready),
        ),
        (
            test("qwen", Some("a"), Some("v1")),
            pool("model-x-02", PropertyState::Ready),
        ),
        (
            test("qwen", Some("b"), Some("v0")),
            pool("model-x-03", PropertyState::Ready),
        ),
    ]);
    assert!(named(&index, "qwen", None).is_empty());
    assert_eq!(
        names(named(&index, "qwen", Some("a"))),
        ["model-x-01", "model-x-02"]
    );
    let unpartitioned = PropertyIndex::new([
        (
            test("qwen", None, Some("v9")),
            pool("model-x-04", PropertyState::Ready),
        ),
        (
            test("qwen", Some("a"), None),
            pool("model-x-01", PropertyState::Ready),
        ),
    ]);
    assert_eq!(names(named(&unpartitioned, "qwen", None)), ["model-x-04"]);
}

/// The chooser decodes each registered kind's keys into its type: a key
/// that does not decode is left out and counted, another kind's keys are
/// not read, and two encodings of one key are one key. A kind no pool
/// lists has an empty index, synced as the table is; a kind not
/// registered has none, nor does another type under a registered name.
#[test]
fn a_key_that_does_not_decode_is_left_out_and_counted() {
    let kinds = PropertyKinds::new();
    kinds.register(&TEST).expect("registered");
    const EMPTY: PropertyKind<Test> = PropertyKind::new("empty");
    kinds.register(&EMPTY).expect("registered");
    let listed = |kind: &str, key: serde_json::Value, name: &str| {
        (
            PropertyKey::new(kind, key),
            pool(name, PropertyState::Ready),
        )
    };
    let (_, now) = kinds.snapshot();
    let indexes = PropertyIndexes::decode(
        &now,
        true,
        &[
            listed("test", json!({"name": "qwen"}), "model-x-01"),
            listed(
                "test",
                json!({"name": "qwen", "partition": null}),
                "model-x-02",
            ),
            listed("test", json!({"name": 7}), "model-x-03"),
            listed("test", json!({"name": "qwen", "weight": 2}), "model-x-04"),
            listed("other", json!({"name": "qwen"}), "model-x-05"),
        ],
    );
    assert!(indexes.is_synced());
    let index = indexes.get(&TEST).expect("registered");
    assert!(index.is_synced());
    assert_eq!(
        names(index.get(&test("qwen", None, None))),
        ["model-x-01", "model-x-02"]
    );
    assert_eq!(
        index.undecoded(),
        2,
        "a name of another type, an unknown field"
    );
    let empty = indexes.get(&EMPTY).expect("registered");
    assert!(empty.is_empty() && empty.is_synced());
    assert!(indexes.get(&PropertyKind::<Test>::new("other")).is_none());
    assert!(indexes.get(&PropertyKind::<String>::new("test")).is_none());
    assert!(!PropertyIndexes::NONE.is_synced());
    assert!(PropertyIndexes::NONE.get(&TEST).is_none());
}

/// Two keys that decode to one value in one pool, as a type that ignores
/// a field reads them, are one entry: the first in the order the pool
/// lists them.
#[test]
fn two_keys_that_decode_alike_are_one_entry_the_first() {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    struct Lax {
        name: String,
    }
    const LAX: PropertyKind<Lax> = PropertyKind::new("test");
    let kinds = PropertyKinds::new();
    kinds.register(&LAX).expect("registered");
    let (_, now) = kinds.snapshot();
    let indexes = PropertyIndexes::decode(
        &now,
        true,
        &[
            (
                PropertyKey::new("test", json!({"name": "qwen"})),
                pool("model-x-01", PropertyState::Draining),
            ),
            (
                PropertyKey::new("test", json!({"name": "qwen", "weight": 2})),
                pool("model-x-01", PropertyState::Ready),
            ),
        ],
    );
    let index = indexes.get(&LAX).expect("registered");
    assert_eq!(index.entries().len(), 1);
    assert_eq!(index.entries()[0].1.state(), PropertyState::Draining);
}

/// One kind name has one type: the same type again is no change, another
/// type is refused, and so is a name that is not a settings key.
#[test]
fn a_kind_is_registered_with_one_type() {
    let kinds = PropertyKinds::new();
    kinds.register(&TEST).expect("registered");
    let (changes, _) = kinds.snapshot();
    kinds.register(&TEST).expect("again");
    assert_eq!(kinds.snapshot().0, changes, "no change");
    let refused = kinds
        .register(&PropertyKind::<String>::new("test"))
        .expect_err("another type");
    assert!(refused.to_string().contains("registered with"), "{refused}");
    assert!(kinds.register(&PropertyKind::<Test>::new("Test")).is_err());
}

/// The indexes of a table that has none list no kind and are not synced,
/// so a reader tells them from synced indexes that lack the key.
#[test]
fn no_index_is_not_synced() {
    assert!(!PropertyIndexes::NONE.is_synced());
    assert!(PropertyIndexes::default().get(&TEST).is_none());
    let synced = PropertyIndexes::new(true).with(&TEST, PropertyIndex::new([]));
    let index = synced.get(&TEST).expect("given");
    assert!(index.is_synced() && index.get(&test("qwen", None, None)).is_empty());
}

#[test]
#[ignore = "F2: canonicalization removes a required null field before typed decoding"]
fn a_required_null_field_decodes_from_its_own_kind() {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, crate::Message)]
    struct Nullable {
        name: String,
        marker: (),
    }
    const NULLABLE: PropertyKind<Nullable> = PropertyKind::new("nullable");
    let value = Nullable {
        name: "a".into(),
        marker: (),
    };
    let plain = serde_json::to_value(&value).expect("encoded without canonicalization");
    assert_eq!(
        Nullable::deserialize(&plain).expect("the required null field is valid"),
        value
    );
    let encoded = NULLABLE.encode(&value).expect("a valid object key");
    let kinds = PropertyKinds::new();
    kinds.register(&NULLABLE).expect("registered");
    let (_, now) = kinds.snapshot();
    let indexes = PropertyIndexes::decode(
        &now,
        true,
        &[(encoded, pool("model-x-01", PropertyState::Ready))],
    );
    let index = indexes.get(&NULLABLE).expect("registered");
    assert_eq!(index.undecoded(), 0, "the kind must decode its own key");
    assert_eq!(index.get(&value).len(), 1);
}

/// A range ends where its last key is, whatever its length: the end is
/// found by doubling steps, then a search of the last step. Checked
/// against a scan for each length up to past several steps, with keys
/// before and after it.
#[test]
fn a_range_of_any_length_ends_at_its_last_key() {
    for before in [0, 1, 5] {
        for length in 0..40 {
            for after in [0, 1, 7] {
                let pool = |n: usize| pool(&format!("p{n:03}"), PropertyState::Ready);
                let entries = (0..before)
                    .map(|n| (test("a", None, None), pool(n)))
                    .chain(
                        (0..length).map(|n| (test("b", None, Some(&format!("v{n:03}"))), pool(n))),
                    )
                    .chain((0..after).map(|n| (test("c", None, None), pool(n))));
                let index = PropertyIndex::new(entries);
                let found = named(&index, "b", None);
                assert_eq!(found.len(), length, "{before} {length} {after}");
                assert!(found.iter().all(|(key, _)| key.name == "b"));
            }
        }
    }
}
