//! Identity checks for message keys with null-valued map entries, and the
//! graph's declarations, which hold a segment's providers and readers to
//! the kinds it declares and to their types.

use std::collections::BTreeMap;

use gdl::{EntryState, InstanceProperties, PropertyKind, PropertyKinds, PropertyType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, gdl::Message)]
struct Tagged {
    name: String,
    tags: BTreeMap<String, ()>,
}

const TAGGED: PropertyKind<Tagged> = PropertyKind::new("tagged");

#[test]
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

/// Another type of key, under the name `tagged`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Named {
    name: String,
}

const OTHER: PropertyKind<Named> = PropertyKind::new("tagged");

fn named(name: &str) -> Named {
    Named { name: name.into() }
}
const REGION: PropertyKind<Tagged> = PropertyKind::new("region");

fn tagged(name: &str) -> Tagged {
    Tagged {
        name: name.into(),
        tags: BTreeMap::new(),
    }
}

fn refusal(result: gdl::Result<()>) -> String {
    result.expect_err("refused").to_string()
}

/// Once the runtime declares the kinds a segment's nodes provide, a node
/// sets only those, with the declared type; encoded entries are held to
/// the kind alone. A handle no runtime declared takes any kind, since
/// nothing reads it.
#[test]
fn a_declared_handle_sets_only_the_kinds_its_segment_provides() {
    let detached = InstanceProperties::new();
    detached
        .set(&REGION, [(tagged("a"), EntryState::Ready)])
        .expect("nothing reads a detached handle");

    let properties = InstanceProperties::new();
    properties
        .declare(vec![PropertyType::of::<Tagged>("tagged")])
        .expect("declared");
    properties
        .set(&TAGGED, [(tagged("a"), EntryState::Ready)])
        .expect("a provided kind, of its type");
    assert!(
        refusal(properties.set(&REGION, [(tagged("a"), EntryState::Ready)])).contains(
            "property kind `region` is not one this segment's nodes provide: the graph says `provides region` on the node that sets it"
        )
    );
    let wrong = refusal(properties.set(&OTHER, [(named("a"), EntryState::Ready)]));
    assert!(
        wrong.contains("property kind `tagged` is declared with keys of type `")
            && wrong.contains("Tagged`, not `properties::Named`"),
        "{wrong}"
    );
    let encoded = REGION.encode(&tagged("a")).expect("a key");
    assert!(
        refusal(properties.set_entries(
            "region",
            vec![gdl::PropertyEntry::new(encoded, EntryState::Ready)]
        ))
        .contains("not one this segment's nodes provide")
    );
}

/// A kind set before the runtime declares the handle is held to the
/// declaration when it comes, and the handle is then not declared.
#[test]
fn a_kind_set_before_the_declaration_is_held_to_it() {
    let properties = InstanceProperties::new();
    properties
        .set(&OTHER, [(named("a"), EntryState::Ready)])
        .expect("not declared yet");
    let wrong = refusal(properties.declare(vec![PropertyType::of::<Tagged>("tagged")]));
    assert!(wrong.contains("declared with keys of type"), "{wrong}");
    assert!(
        refusal(properties.declare(vec![PropertyType::of::<Tagged>("region")]))
            .contains("not one this segment's nodes provide")
    );
    properties
        .declare(vec![PropertyType::of::<Named>("tagged")])
        .expect("the type it was set with");
    let again = properties.declare(vec![
        PropertyType::of::<Named>("tagged"),
        PropertyType::of::<Tagged>("region"),
    ]);
    assert!(
        refusal(again).contains("declared twice, with other kinds"),
        "declared once"
    );
}

/// Once the runtime declares the graph's kinds, a node or a pool policy
/// reads only those, with the declared type, also those registered before.
#[test]
fn readers_register_only_the_graphs_kinds_with_their_types() {
    let kinds = PropertyKinds::new();
    kinds.register(&OTHER).expect("not declared yet");
    let wrong = refusal(kinds.declare(vec![PropertyType::of::<Tagged>("tagged")], &["tagged"]));
    assert!(wrong.contains("declared with keys of type"), "{wrong}");

    let kinds = PropertyKinds::new();
    kinds
        .declare(vec![PropertyType::of::<Tagged>("tagged")], &["tagged"])
        .expect("declared");
    kinds
        .register(&TAGGED)
        .expect("a declared kind, of its type");
    assert!(refusal(kinds.register(&REGION)).contains(
        "property kind `region` is not declared in the graph: it says `property region: <Type>;`"
    ));
    assert!(refusal(kinds.register(&OTHER)).contains("declared with keys of type"));
}

/// A node registers only a kind its segment needs: one a node or an entry
/// adapter of the segment says it reads, which the supply check holds to a
/// supply. A pool policy is held to its own `NEEDS` instead. A later
/// declaration of the same kinds adds its needs.
#[test]
fn a_node_registers_only_a_kind_its_segment_needs() {
    let declared = || vec![PropertyType::of::<Tagged>("tagged")];
    let kinds = PropertyKinds::new();
    kinds.declare(declared(), &[]).expect("declared");
    assert!(
        refusal(kinds.register(&TAGGED)).contains(
            "property kind `tagged` is not one this segment's nodes or entry adapters need"
        )
    );
    kinds
        .limited(&["tagged"], "the policy")
        .register(&TAGGED)
        .expect("a policy that needs it");
    kinds
        .declare(declared(), &["tagged"])
        .expect("a need added");
    kinds.register(&TAGGED).expect("a needed kind");

    let early = PropertyKinds::new();
    early.register(&TAGGED).expect("not declared yet");
    assert!(refusal(early.declare(declared(), &[])).contains("need"));
}
