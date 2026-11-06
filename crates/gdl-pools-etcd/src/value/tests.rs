use std::time::Duration;

use gdl_pools::{
    LoadSummary, PoolProperty, PoolRecord, PoolState, Presence, PropertyState, Withdrawal,
};

use super::*;

fn key(name: &str) -> gdl_model::PropertyKey {
    gdl_model::PropertyKey::new("test", serde_json::json!({ "name": name })).expect("a key")
}

fn whole() -> PoolRecord {
    PoolRecord {
        dropped: Vec::new(),
        presence: Presence::Serving,
        properties: vec![PoolProperty {
            property: key("shard-1"),
            state: PropertyState::Unavailable,
            since: 7,
        }],
        properties_version: 1_001,
        revoked: vec![key("revoked-1")],
        name: "m1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        parent: Some(gdl_model::Parent::Class("global".into())),
        contract: 3,
        placement: 4,
        state: PoolState::Degraded,
        epoch: 5,
        seq: 6,
        hub: "m1:1337".into(),
        lease: Duration::from_secs(31),
        load: Some(LoadSummary {
            admitted: 1,
            limit: 2,
            instances: 3,
            refusals: 4,
            period_ms: 10_000,
            ..Default::default()
        }),
    }
}

/// A record split into its two keys reads back whole, every field, from
/// the record key's value joined to the list key's.
#[test]
fn a_split_record_joins_back_whole() {
    let record = whole();
    let Split { fields, list } = split(&record);
    assert!(!fields.contains_key("properties") && !fields.contains_key("revoked"));
    for at in [None, Some(42)] {
        let stored = decode(&encode_record(&fields, at)).expect("a record");
        let Stored::Record {
            fields: read,
            version,
            list: named,
        } = &stored
        else {
            panic!("{stored:?}");
        };
        assert_eq!(*version, record.version());
        assert_eq!(*named, at);
        assert_eq!(stored.list_at(9), Some(at.unwrap_or(9)));
        assert_eq!(join(read, &list), Some(record.clone()));
    }
    // An empty list is a list too.
    let bare = PoolRecord {
        properties: Vec::new(),
        revoked: Vec::new(),
        ..whole()
    };
    let Split { fields, list } = split(&bare);
    assert_eq!(join(&fields, &list), Some(bare));
}

/// A tombstone reads back with its version and its list version.
#[test]
fn a_withdrawal_reads_back() {
    let withdrawal = Withdrawal {
        class: "model_x".into(),
        pool: "m1".into(),
        epoch: 2,
        seq: 3,
        properties_version: 1 << 21,
    };
    let stored = decode(&encode_withdrawal(&withdrawal)).expect("a tombstone");
    assert_eq!(stored.version(), withdrawal.version());
    assert_eq!(stored.list_at(9), None);
    assert_eq!(stored, Stored::Withdrawn(withdrawal));
}

/// A value this backend did not write reads as none, not as a record.
#[test]
fn a_foreign_value_is_not_a_record() {
    for foreign in [
        &b"x"[..],
        b"{}",
        b"[]",
        b"{\"record\":{}}",
        b"{\"record\":{\"epoch\":1,\"seq\":1},\"list\":\"a\"}",
    ] {
        assert_eq!(
            decode(foreign),
            None,
            "{}",
            String::from_utf8_lossy(foreign)
        );
    }
    let Split { fields, .. } = split(&whole());
    assert_eq!(
        join(&fields, b"{\"properties\":[]}"),
        None,
        "a list without revoked"
    );
}

/// A key under a class's prefix is a pool's record key or its list key;
/// pools whose names begin alike stay apart, as do classes.
#[test]
fn keys_name_their_pool() {
    let class = class_prefix("/gdl/pools", "m");
    assert_eq!(class, "/gdl/pools/m/");
    assert_eq!(record_key("/gdl/pools", "m", "m1"), "/gdl/pools/m/m1");
    assert_eq!(list_key("/gdl/pools", "m", "m1"), "/gdl/pools/m/m1.list");
    assert_eq!(name(&class, b"/gdl/pools/m/m1"), Some(Name::Record("m1")));
    assert_eq!(
        name(&class, b"/gdl/pools/m/m1-a"),
        Some(Name::Record("m1-a"))
    );
    assert_eq!(
        name(&class, b"/gdl/pools/m/m1.list"),
        Some(Name::List("m1"))
    );
    assert_eq!(
        name(&class, b"/gdl/pools/m/m1-a.list"),
        Some(Name::List("m1-a"))
    );
    for other in [
        &b"/gdl/pools/m_x/m1"[..],
        b"/gdl/pools/m/",
        b"/gdl/pools/m/.list",
        b"/gdl/pools/m/a/b",
        b"/gdl/pools/m/a.b",
    ] {
        assert_eq!(
            name(&class, other),
            None,
            "{}",
            String::from_utf8_lossy(other)
        );
    }
}

#[test]
fn a_prefix_range_ends_past_every_key_it_begins() {
    assert_eq!(range_end(b"/gdl/pools/m/"), b"/gdl/pools/m0".to_vec());
    assert_eq!(range_end(b"a\xff"), b"b".to_vec());
    assert_eq!(range_end(b"\xff"), vec![0]);
}
