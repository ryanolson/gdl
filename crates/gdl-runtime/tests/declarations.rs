//! Property readers are held to the graph's supply contract.

use gdl::{FollowedPools, InstanceProperties, PropertyKind};
use gdl_runtime::{Placement, declare_properties};

mod contract {
    gdl_runtime::graph! {
        name = "reader", schema = 1;
        property shard: u32;
        source Ask: u32 -> stream u32;
        sink Serve: u32 -> stream u32;
        topology: Ask => Serve;
    }
}

#[test]
fn a_reader_cannot_register_a_kind_without_a_checked_need() {
    let manifest = contract::manifest();
    let placement = Placement::new()
        .segment("front", ["Ask"])
        .segment("engine", ["Serve"])
        .class("global", ["front"])
        .class("engines", ["engine"]);
    let layout = gdl_model::layout(&manifest, &placement).expect("layout");
    assert!(gdl_model::property_supply(&manifest, &layout, &[]).is_empty());

    let mut needed = manifest.clone();
    needed.properties[0].needs.push(gdl_model::PropertyNeed {
        node: "Ask".into(),
        class: "engines".into(),
    });
    let checked = gdl_model::property_supply(&needed, &layout, &[]);
    assert_eq!(checked.len(), 1, "{checked:?}");
    assert!(checked[0].contains("no entry segment"), "{checked:?}");

    let followed = FollowedPools::new();
    declare_properties(
        &manifest,
        &placement,
        "front",
        &contract::property_types(),
        &[],
        &InstanceProperties::new(),
        &followed,
    )
    .expect("runtime declarations");
    assert!(
        followed
            .register(&PropertyKind::<u32>::new("shard"))
            .is_err(),
        "a reader with no declared need registers shard even though engines has no provider"
    );
}
