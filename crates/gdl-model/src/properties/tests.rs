use serde_json::json;

use super::*;

fn read(text: &str) -> PropertyKey {
    serde_json::from_str(text).expect("a key")
}

/// Two builds that write a key's fields in another order, or write a
/// missing optional field as `null`, name one property: equal, ordered
/// equal, and hashed alike. Each key is written back as it was read.
#[test]
fn two_encodings_of_one_key_name_one_property() {
    use std::hash::{BuildHasher, RandomState};
    let a = read(r#"{"kind":"test","key":{"name":"a","partition":"p","version":null}}"#);
    let b = read(r#"{"kind":"test","key":{"version":null,"partition":"p","name":"a"}}"#);
    let c = read(r#"{"kind":"test","key":{"partition":"p","name":"a"}}"#);
    let hasher = RandomState::new();
    for other in [&b, &c] {
        assert_eq!(&a, other);
        assert_eq!(a.cmp(other), Ordering::Equal);
        assert_eq!(hasher.hash_one(&a), hasher.hash_one(other));
    }
    assert_eq!(a.key()["version"], Value::Null, "kept as read");
    assert_eq!(
        serde_json::to_value(&c).expect("json"),
        json!({"kind": "test", "key": {"partition": "p", "name": "a"}}),
        "written as read"
    );
    assert_eq!(
        serde_json::from_str::<PropertyKey>(&serde_json::to_string(&a).expect("json"))
            .expect("a key")
            .key(),
        a.key(),
        "the null field survives a round trip"
    );
    let made = PropertyKey::new(
        "test",
        json!({"version": null, "partition": "p", "name": "a"}),
    );
    assert_eq!(made, a, "made as read");
    let entry: PropertyEntry = serde_json::from_str(
        r#"{"kind":"test","key":{"partition":"p","name":"a","version":null},"state":"ready"}"#,
    )
    .expect("an entry");
    assert_eq!(entry.property(), &a);
}

/// A canonical object iterates its fields by name whatever order they
/// were written in, which the order of keys relies on. With
/// `serde_json/preserve_order` the map keeps the written order; the key
/// is rebuilt sorted, so this holds in both builds.
#[test]
fn a_canonical_object_iterates_its_fields_by_name() {
    let key = read(r#"{"kind":"test","key":{"z":1,"a":{"y":null,"c":2,"b":3}}}"#);
    let names: Vec<&str> = key
        .canonical
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(names, ["a", "z"]);
    let inner: Vec<&str> = key.canonical["a"]
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(inner, ["b", "c"], "null left out at any depth");
}

/// Keys differ in any field, a `null` in an array is its own, and keys
/// order by kind, then by key, totally: the order agrees with equality.
#[test]
fn keys_order_totally_and_agree_with_equality() {
    let keys = [
        PropertyKey::new("a", json!({"name": "x"})),
        PropertyKey::new("b", json!({"name": "x"})),
        PropertyKey::new("b", json!({"name": "x", "partition": "p"})),
        PropertyKey::new("b", json!({"name": "y"})),
        PropertyKey::new("b", json!({"n": 1})),
        PropertyKey::new("b", json!({"n": 1.0})),
        PropertyKey::new("b", json!({"n": 2})),
        PropertyKey::new("b", json!({"n": -0.0})),
        PropertyKey::new("b", json!({"list": [null, 1]})),
        PropertyKey::new("b", json!({"list": [1]})),
        PropertyKey::new("b", json!({"flag": true})),
    ];
    for (i, a) in keys.iter().enumerate() {
        for (j, b) in keys.iter().enumerate() {
            assert_eq!(a == b, i == j, "{a:?} {b:?}");
            assert_eq!(a.cmp(b) == Ordering::Equal, i == j, "{a:?} {b:?}");
            assert_eq!(a.cmp(b), b.cmp(a).reverse(), "{a:?} {b:?}");
        }
    }
    let mut sorted = keys.to_vec();
    sorted.sort();
    for pair in sorted.windows(2) {
        assert_eq!(pair[0].cmp(&pair[1]), Ordering::Less);
    }
    assert_eq!(
        PropertyKey::new("b", json!({"n": -0.0})),
        PropertyKey::new("b", json!({"n": 0.0})),
        "a negative zero is zero"
    );
}

/// A key is published only with a kind of a settings key's shape and an
/// object for a key.
#[test]
fn a_key_is_checked_for_its_kind_and_its_shape() {
    PropertyKey::new("test", json!({"name": "a"}))
        .check()
        .expect("a key");
    PropertyKey::new("test", json!({}))
        .check()
        .expect("an empty object is a key");
    for (bad, why) in [
        (
            PropertyKey::new("", json!({"name": "a"})),
            "property kind name",
        ),
        (
            PropertyKey::new("Test", json!({"name": "a"})),
            "property kind name",
        ),
        (
            PropertyKey::new("a__b", json!({"name": "a"})),
            "property kind name",
        ),
        (PropertyKey::new("test", json!("a")), "must be an object"),
        (PropertyKey::new("test", json!(null)), "must be an object"),
    ] {
        let refused = bad.check().expect_err("refused");
        assert!(refused.contains(why), "{bad:?}: {refused}");
    }
}

/// A key is encoded from its type, in the canonical form.
#[test]
fn a_key_is_encoded_from_its_type() {
    #[derive(Serialize)]
    struct Test {
        name: &'static str,
        partition: Option<&'static str>,
    }
    let key = PropertyKey::encode(
        "test",
        &Test {
            name: "a",
            partition: None,
        },
    )
    .expect("encoded");
    assert_eq!(key, PropertyKey::new("test", json!({"name": "a"})));
    assert_eq!(key.kind(), "test");
}

/// JSON reads a float back as a near value, not always the same one, so
/// a key that holds a float is refused, at any depth: it would not name
/// its property after a report or a stored record. A key of integers,
/// the largest and the smallest too, reads back as the same property.
#[test]
fn a_key_of_integers_keeps_its_identity_and_a_float_is_refused() {
    for number in [
        json!(1.5),
        json!(1.5801779765215413),
        json!(-0.0),
        json!(2.0),
    ] {
        for key in [
            json!({ "ratio": number }),
            json!({ "deep": { "list": [1, number] } }),
        ] {
            let refused = PropertyKey::new("test", key.clone())
                .check()
                .expect_err("a float");
            assert!(
                refused.contains("property kind `test`") && refused.contains("not an integer"),
                "{key}: {refused}"
            );
        }
    }
    for number in [json!(1), json!(0), json!(i64::MIN), json!(u64::MAX)] {
        let key = PropertyKey::new("test", json!({ "count": number, "list": [number] }));
        key.check().expect("integers");
        let wire = serde_json::to_string(&key).expect("encoded");
        let read: PropertyKey = serde_json::from_str(&wire).expect("read");
        assert_eq!(key, read, "a report or stored record keeps {wire}");
        assert_eq!(key.cmp(&read), Ordering::Equal);
        assert_eq!(serde_json::to_string(&read).expect("encoded"), wire);
    }
}
