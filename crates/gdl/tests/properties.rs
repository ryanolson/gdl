//! Identity checks for message keys with null-valued map entries.

use std::collections::BTreeMap;

use gdl::{EntryState, InstanceProperties, PropertyKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, gdl::Message)]
struct Tagged {
    name: String,
    tags: BTreeMap<String, ()>,
}

const TAGGED: PropertyKind<Tagged> = PropertyKind::new("tagged");

#[test]
#[ignore = "F4: canonical comparison drops null-valued map entries and conflates distinct keys"]
fn null_map_values_are_part_of_a_typed_propertys_identity() {
    let empty = Tagged {
        name: "a".into(),
        tags: BTreeMap::new(),
    };
    let tagged = Tagged {
        tags: BTreeMap::from([("gpu".into(), ())]),
        ..empty.clone()
    };
    assert_ne!(empty, tagged);
    let plain = serde_json::to_value(&tagged).expect("encodes");
    assert_eq!(
        serde_json::from_value::<Tagged>(plain).expect("decodes"),
        tagged,
        "the null-valued map entry round-trips without losing identity"
    );
    let first = TAGGED.encode(&empty).expect("an object key");
    let second = TAGGED.encode(&tagged).expect("an object key");
    assert_ne!(first.key(), second.key(), "their JSON objects differ");
    InstanceProperties::new()
        .set(
            &TAGGED,
            [(empty, EntryState::Ready), (tagged, EntryState::Draining)],
        )
        .expect("two distinct, valid message values are two properties");
}
