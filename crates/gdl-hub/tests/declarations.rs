mod common;

use gdl_hub::PoolReadiness;
use gdl_model::{PoolSettings, PropertyKey};
use gdl_pools::MemoryDirectory;
use serde_json::json;

#[test]
#[ignore = "F3: programmatic pool declarations bypass the property key's float check"]
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
    assert!(declare(PropertyKey::new("test", json!({"count": 1}))).is_ok());

    let key = PropertyKey::new("test", json!({"ratio": 1.5801779765215413}));
    assert!(
        key.check().is_err(),
        "the key is outside the supported domain"
    );
    let written = serde_json::to_string(&key).expect("a key serializes");
    let read: PropertyKey = serde_json::from_str(&written).expect("a key reads");
    assert_ne!(read, key, "this float changes identity across JSON");
    assert!(
        declare(key).is_err(),
        "PoolReadiness::new accepts an unsupported property through PoolSettings.properties"
    );
}
