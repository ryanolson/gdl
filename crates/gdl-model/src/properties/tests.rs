use serde_json::json;

use super::*;

fn read(text: &str) -> PropertyKey {
    serde_json::from_str(text).expect("a key")
}

/// Two builds that write a key's fields in another order name one
/// property: equal, ordered equal, and hashed alike. A field written as
/// `null` and a missing one are two values, so they name two properties.
/// Each key is written back as it was read.
#[test]
fn two_encodings_of_one_key_name_one_property() {
    use std::hash::{BuildHasher, RandomState};
    let a = read(r#"{"kind":"test","key":{"name":"a","partition":"p","version":null}}"#);
    let b = read(r#"{"kind":"test","key":{"version":null,"partition":"p","name":"a"}}"#);
    let c = read(r#"{"kind":"test","key":{"partition":"p","name":"a"}}"#);
    let hasher = RandomState::new();
    assert_eq!(a, b);
    assert_eq!(a.cmp(&b), Ordering::Equal);
    assert_eq!(hasher.hash_one(&a), hasher.hash_one(&b));
    assert_ne!(a, c, "a null field is not a missing one");
    assert_ne!(a.cmp(&c), Ordering::Equal);
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
    )
    .expect("a key");
    assert_eq!(made, a, "made as read");
    let entry: PropertyEntry = serde_json::from_str(
        r#"{"kind":"test","key":{"partition":"p","name":"a","version":null},"state":"ready"}"#,
    )
    .expect("an entry");
    assert_eq!(entry.property(), &a);
}

/// A canonical object iterates its fields by name whatever order they
/// were written in, which the order of keys relies on, and keeps every
/// `null`. With
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
    assert_eq!(inner, ["b", "c", "y"], "a null kept at any depth");
}

/// Keys differ in any field, a `null` in an array is its own, and keys
/// order by kind, then by key, totally: the order agrees with equality.
#[test]
fn keys_order_totally_and_agree_with_equality() {
    let keys = [
        ("a", json!({"name": "x"})),
        ("b", json!({"name": "x"})),
        ("b", json!({"name": "x", "partition": "p"})),
        ("b", json!({"name": "y"})),
        ("b", json!({"n": 1})),
        ("b", json!({"n": -1})),
        ("b", json!({"n": 2})),
        ("b", json!({"n": u64::MAX})),
        ("b", json!({"n": i64::MIN})),
        ("b", json!({"list": [null, 1]})),
        ("b", json!({"list": [1]})),
        ("b", json!({"flag": true})),
    ]
    .map(|(kind, key)| PropertyKey::new(kind, key).expect("a key"));
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
}

/// A key is made, or read, only with a kind of a settings key's shape
/// and an object for a key, so every key that exists can be published.
#[test]
fn a_key_is_checked_for_its_kind_and_its_shape() {
    PropertyKey::new("test", json!({"name": "a"})).expect("a key");
    PropertyKey::new("test", json!({})).expect("an empty object is a key");
    for (kind, key, why) in [
        ("", json!({"name": "a"}), "property kind name"),
        ("Test", json!({"name": "a"}), "property kind name"),
        ("a__b", json!({"name": "a"}), "property kind name"),
        ("test", json!("a"), "must be an object"),
        ("test", json!(null), "must be an object"),
    ] {
        let refused = PropertyKey::new(kind, key.clone()).expect_err("refused");
        assert!(refused.contains(why), "{kind} {key}: {refused}");
        let written = json!({ "kind": kind, "key": key }).to_string();
        let read = serde_json::from_str::<PropertyKey>(&written).expect_err("refused when read");
        assert!(read.to_string().contains(why), "{written}: {read}");
    }
}

/// A key is encoded from its type as the type writes it: a type that
/// skips a missing optional field and one that writes it as `null` name
/// two properties, so a property type must encode alike in every build.
#[test]
fn a_key_is_encoded_from_its_type() {
    #[derive(Serialize)]
    struct Skipping {
        name: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        partition: Option<&'static str>,
    }
    #[derive(Serialize)]
    struct Writing {
        name: &'static str,
        partition: Option<&'static str>,
    }
    let skipping = PropertyKey::encode(
        "test",
        &Skipping {
            name: "a",
            partition: None,
        },
    )
    .expect("encoded");
    assert_eq!(
        skipping,
        PropertyKey::new("test", json!({"name": "a"})).expect("a key")
    );
    assert_eq!(skipping.kind(), "test");
    let writing = PropertyKey::encode(
        "test",
        &Writing {
            name: "a",
            partition: None,
        },
    )
    .expect("encoded");
    assert_eq!(
        writing,
        PropertyKey::new("test", json!({"name": "a", "partition": null})).expect("a key")
    );
    assert_ne!(skipping, writing, "two encodings, two properties");
}

/// JSON reads a float back as a near value, not always the same one, so
/// a key that holds a float is refused, at any depth, when it is made and
/// when it is read: it would not name its property after a report or a
/// stored record. A key of integers, the largest and the smallest too,
/// reads back as the same property.
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
            let refused = PropertyKey::new("test", key.clone()).expect_err("a float");
            assert!(
                refused.contains("property kind `test`") && refused.contains("not an integer"),
                "{key}: {refused}"
            );
            let written = json!({ "kind": "test", "key": key }).to_string();
            let read = serde_json::from_str::<PropertyKey>(&written).expect_err("a float read");
            assert!(
                read.to_string().contains("not an integer"),
                "{written}: {read}"
            );
            let entry = json!({ "kind": "test", "key": key, "state": "ready" }).to_string();
            assert!(
                serde_json::from_str::<PropertyEntry>(&entry).is_err(),
                "{entry}"
            );
        }
    }
    for number in [json!(1), json!(0), json!(i64::MIN), json!(u64::MAX)] {
        let key = PropertyKey::new("test", json!({ "count": number, "list": [number] }))
            .expect("integers");
        let wire = serde_json::to_string(&key).expect("encoded");
        let read: PropertyKey = serde_json::from_str(&wire).expect("read");
        assert_eq!(key, read, "a report or stored record keeps {wire}");
        assert_eq!(key.cmp(&read), Ordering::Equal);
        assert_eq!(serde_json::to_string(&read).expect("encoded"), wire);
    }
}
