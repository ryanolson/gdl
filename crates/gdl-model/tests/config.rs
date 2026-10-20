//! Graph configuration validation.
use gdl_model::{ConfigField, ConfigValue, resolve_config};
use std::collections::BTreeMap;

#[test]
fn graph_config_uses_declared_defaults_and_refuses_unmarked_fields_and_wrong_types() {
    let fields = vec![ConfigField {
        label: String::new(),
        subtext: String::new(),
        name: "enabled_chat".into(),
        description: "Expose chat".into(),
        default: ConfigValue::Bool(true),
    }];
    let defaults = resolve_config(&fields, &BTreeMap::new()).expect("defaults");
    assert_eq!(defaults["enabled_chat"], ConfigValue::Bool(true));
    let changed = BTreeMap::from([("enabled_chat".into(), ConfigValue::Bool(false))]);
    assert_eq!(
        resolve_config(&fields, &changed).expect("test result"),
        changed
    );
    assert!(
        resolve_config(
            &fields,
            &BTreeMap::from([("listen".into(), ConfigValue::Text("secret".into()))])
        )
        .is_err()
    );
    assert!(
        resolve_config(
            &fields,
            &BTreeMap::from([("enabled_chat".into(), ConfigValue::Text("false".into()))])
        )
        .is_err()
    );
}
