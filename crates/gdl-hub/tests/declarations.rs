//! A pool's declared properties built in code, not read from its
//! settings: they hold only keys that check, since a key that does not
//! cannot be made or read.

mod common;

use gdl_hub::PoolReadiness;
use gdl_model::{PoolSettings, PropertyKey};
use gdl_pools::MemoryDirectory;
use serde_json::json;

/// A pool built in code takes a key of integers. A key with a float, which
/// JSON does not read back exactly, cannot be made, nor read from JSON, so
/// no path declares it to a pool.
#[test]
fn a_programmatic_pool_refuses_a_float_property() {
    let declare = |key| {
        PoolReadiness::new(
            &common::spec(),
            PoolSettings {
                properties: vec![key],
                ..common::settings()
            },
            MemoryDirectory::new(),
            common::timing(),
        )
    };
    let key = PropertyKey::new("test", json!({"count": 1})).expect("a key");
    assert!(declare(key).is_ok());

    let float = json!({"ratio": 1.5801779765215413});
    let refused = PropertyKey::new("test", float.clone()).expect_err("a float");
    assert!(refused.contains("not an integer"), "{refused}");
    let written = json!({"kind": "test", "key": float}).to_string();
    let read = serde_json::from_str::<PropertyKey>(&written).expect_err("a float read");
    assert!(read.to_string().contains("not an integer"), "{read}");
}
