//! Property readers are held to the graph's supply contract.

use gdl::{FollowedPools, InstanceProperties, PropertyKind};
use gdl_runtime::{EntryNeed, Placement, declare_properties};

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

/// One graph, as `graph!` takes it and as the text of a graph file.
macro_rules! both {
    ($($graph:tt)*) => {
        mod gated {
            gdl_runtime::graph! { $($graph)* }
        }
        const GATED: &str = stringify!($($graph)*);
    };
}

both! {
    name = "gated", schema = 1;
    property shard: u32;
    source Ask: u32 -> stream u32;
    operator Gate: (u32 -> stream u32) over (u32 -> stream u32) needs shard from west;
    operator Door: (u32 -> stream u32) over (u32 -> stream u32) needs shard from east;
    sink Serve: u32 -> stream u32 provides shard;
    topology: Ask => Gate { Door { Serve } };
}

const SHARD: PropertyKind<u32> = PropertyKind::new("shard");

fn gated_placement() -> Placement {
    Placement::new()
        .segment("front", ["Ask", "Gate", "Door"])
        .segment("engine", ["Serve"])
}

/// The handle `segment` of the graph with `manifest` holds once it is
/// declared, with `entry_needs`.
fn declared(
    manifest: &gdl_model::Manifest,
    segment: &str,
    entry_needs: &[EntryNeed],
) -> FollowedPools {
    let followed = FollowedPools::new();
    declare_properties(
        manifest,
        &gated_placement(),
        segment,
        &gated::property_types(),
        entry_needs,
        &InstanceProperties::new(),
        &followed,
    )
    .expect("declared");
    followed
}

fn class(followed: &FollowedPools, reader: &str) -> Option<String> {
    followed
        .reader(reader)
        .needed_class(&SHARD)
        .map(str::to_string)
}

/// Each graph node reads the class its own need names, in one segment with
/// two classes for one kind. A node that says nothing reads nothing, and a
/// need of another segment's node is not this segment's.
#[test]
fn a_graph_node_reads_the_class_its_need_names() {
    let front = declared(&gated::manifest(), "front", &[]);
    assert_eq!(class(&front, "Gate").as_deref(), Some("west"));
    assert_eq!(class(&front, "Door").as_deref(), Some("east"));
    assert_eq!(class(&front, "Ask"), None);
    assert_eq!(
        front
            .reader("Gate")
            .needed_class(&PropertyKind::<u32>::new("region")),
        None,
        "a kind it does not name"
    );
    let engine = declared(&gated::manifest(), "engine", &[]);
    assert_eq!(class(&engine, "Gate"), None, "not hosted here");
}

/// Each entry's adapters read the class their entry names, and two entries
/// can name two classes for one kind. An adapter of an entry with no need
/// reads nothing. An entry with a need and no adapter has no reader of the
/// class, and still lets a node of its segment register the kind.
#[test]
fn an_entry_adapter_reads_the_class_its_entry_names() {
    let needs = [
        EntryNeed {
            kind: "shard",
            class: "west",
            adapters: &["Ask_chat_f0", "Ask_chat_b0"],
        },
        EntryNeed {
            kind: "shard",
            class: "east",
            adapters: &["Ask_messages_op"],
        },
        EntryNeed {
            kind: "shard",
            class: "south",
            adapters: &[],
        },
    ];
    let graph = gated_without_needs();
    let front = declared(&graph, "front", &needs);
    assert_eq!(class(&front, "Ask_chat_f0").as_deref(), Some("west"));
    assert_eq!(class(&front, "Ask_chat_b0").as_deref(), Some("west"));
    assert_eq!(class(&front, "Ask_messages_op").as_deref(), Some("east"));
    assert_eq!(class(&front, "Ask_models_op"), None);
    assert_eq!(class(&front, "Gate"), None, "the graph names no need of it");

    let no_adapter = declared(&graph, "front", &needs[2..]);
    no_adapter.register(&SHARD).expect("a kind its entry needs");
}

/// The graph with no node's need.
fn gated_without_needs() -> gdl_model::Manifest {
    let mut manifest = gated::manifest();
    manifest.properties[0].needs.clear();
    manifest
}

/// A graph file and a `graph!` of the same text give a reader the same
/// class: the need crosses in the manifest, whichever way it is built.
#[test]
fn a_graph_file_and_graph_macro_agree_on_a_needs_class() {
    let graph = gdl_syntax::Graph::parse(GATED.parse().expect("tokens")).expect("graph");
    let file = graph
        .manifest(&|name| {
            Some(gdl_model::TypeInfo::new(
                name,
                true,
                gdl_model::Encoding::default(),
            ))
        })
        .expect("the file's manifest");
    let (from_file, from_macro) = (
        declared(&file, "front", &[]),
        declared(&gated::manifest(), "front", &[]),
    );
    for reader in ["Gate", "Door", "Ask"] {
        assert_eq!(
            class(&from_file, reader),
            class(&from_macro, reader),
            "{reader}"
        );
    }
    assert_eq!(class(&from_file, "Gate").as_deref(), Some("west"));
}
