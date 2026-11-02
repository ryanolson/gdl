//! A graph's property declarations: the kinds it declares, with their
//! types, and the nodes that provide and need each. They enter the
//! contract hash as edge types do, by the type's name, encoding and
//! version, and the supply check holds each reader of a class's pools to a
//! provider in an entry segment of the class, or to the class's
//! `[pools.<class>] provides`.

use std::collections::BTreeSet;

use gdl_model::{
    Direction, Encoding, HopInfo, Manifest, NodeInfo, Placement, PropertyConsumer, PropertyInfo,
    PropertyNeed, Role, TypeInfo, layout, property_supply,
};

/// `S => F { D }`: F's own hop crosses from `global` into `model_x`, so
/// D's segment is the one entry segment of `model_x`, and F's segment
/// chooses its pools.
fn manifest(properties: Vec<PropertyInfo>) -> Manifest {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let hop = |outer: &str, inner: &str| HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![TypeInfo::new("T", true, Default::default())],
        backward_types: vec![TypeInfo::new("T", true, Default::default())],
    };
    Manifest {
        name: "classed".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("F", Role::Operator),
            node("D", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("F", "D")],
        properties,
    }
}

/// `property shard: Shard;`, provided by D and needed by F from
/// `model_x`.
fn shard() -> PropertyInfo {
    PropertyInfo {
        kind: "shard".into(),
        ty: TypeInfo::new(":: protocols :: Shard", true, Encoding::Msgpack),
        providers: vec!["D".into()],
        needs: vec![need("F", "model_x")],
    }
}

fn need(node: &str, class: &str) -> PropertyNeed {
    PropertyNeed {
        node: node.into(),
        class: class.into(),
    }
}

/// The contract hash of `manifest(vec![])`, as it was before properties
/// were declared: a graph that declares none keeps its hash, so every
/// deployment of it still follows and offers to the others.
const UNDECLARED_HASH: u64 = 0x1035_d29f_e15f_887b;

#[test]
fn a_graph_without_properties_keeps_its_contract_hash() {
    assert_eq!(manifest(vec![]).contract_hash(), UNDECLARED_HASH);
}

/// Each part of a declaration is part of the contract: the kind's name,
/// its type's name and encoding, a provider, and a need's node and class.
/// So is a declaration added or removed. Pools whose graphs differ in any
/// of these never follow or offer to each other.
#[test]
fn each_part_of_a_declaration_changes_the_contract_hash() {
    let with = |change: &dyn Fn(&mut PropertyInfo)| {
        let mut property = shard();
        change(&mut property);
        manifest(vec![property]).contract_hash()
    };
    let hashes = [
        manifest(vec![]).contract_hash(),
        manifest(vec![shard()]).contract_hash(),
        with(&|p| p.kind = "model".into()),
        with(&|p| p.ty.name = ":: protocols :: Model".into()),
        with(&|p| p.ty.encoding = Encoding::Json),
        with(&|p| p.ty.encoding = Encoding::MsgpackArray),
        with(&|p| p.providers.clear()),
        with(&|p| p.providers.push("F".into())),
        with(&|p| p.needs.clear()),
        with(&|p| p.needs[0].class = "global".into()),
        with(&|p| p.needs[0].node = "S".into()),
        with(&|p| p.needs.push(need("S", "model_x"))),
        manifest(vec![
            shard(),
            PropertyInfo {
                kind: "region".into(),
                ..shard()
            },
        ])
        .contract_hash(),
    ];
    let distinct: BTreeSet<u64> = hashes.iter().copied().collect();
    assert_eq!(distinct.len(), hashes.len(), "{hashes:x?}");
}

/// The order declarations, providers and needs are written in means
/// nothing, so it does not change the hash.
#[test]
fn the_order_of_declarations_does_not_change_the_contract_hash() {
    let region = PropertyInfo {
        kind: "region".into(),
        providers: vec!["D".into(), "F".into()],
        needs: vec![need("F", "model_x"), need("S", "model_x")],
        ..shard()
    };
    let reordered = PropertyInfo {
        providers: vec!["F".into(), "D".into()],
        needs: vec![need("S", "model_x"), need("F", "model_x")],
        ..region.clone()
    };
    assert_eq!(
        manifest(vec![shard(), region]).contract_hash(),
        manifest(vec![reordered, shard()]).contract_hash()
    );
}

/// The generator reads a contract's manifest as TOML: the declarations
/// read back whole, and a manifest without them writes none.
#[test]
fn declarations_read_back_from_a_manifest() {
    let declared = manifest(vec![shard()]);
    let text = declared.to_toml();
    assert_eq!(Manifest::from_toml(&text).expect("a manifest"), declared);
    let undeclared = manifest(vec![]).to_toml();
    assert!(!undeclared.contains("properties"), "{undeclared}");
}

const SEGMENTS: &str =
    "[segment.a]\nnodes = ['S']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = ['D']\n";

const CLASSES: &str = "[class.global]\nsegments = ['a', 'b']\n[class.model_x]\nsegments = ['d']\n";

/// The supply problems of `properties` placed by `extra` after the
/// classes, with `outside` consumers.
fn supply(properties: Vec<PropertyInfo>, extra: &str, outside: &[PropertyConsumer]) -> Vec<String> {
    let manifest = manifest(properties);
    let placement =
        Placement::from_toml(&format!("{SEGMENTS}{CLASSES}{extra}")).expect("a placement");
    let layout = layout(&manifest, &placement).expect("a layout");
    property_supply(&manifest, &layout, outside)
}

fn outside(consumer: &str, kind: &str, class: &str, segment: Option<&str>) -> PropertyConsumer {
    PropertyConsumer {
        consumer: consumer.into(),
        kind: kind.into(),
        class: class.into(),
        segment: segment.map(str::to_string),
    }
}

/// A need is met by a node that provides the kind in an entry segment of
/// the class, or by the class's `[pools.<class>] provides`.
#[test]
fn a_need_is_met_by_an_entry_provider_or_by_the_pools_settings() {
    assert_eq!(supply(vec![shard()], "", &[]), Vec::<String>::new());
    let unprovided = PropertyInfo {
        providers: vec![],
        ..shard()
    };
    assert_eq!(
        supply(
            vec![unprovided],
            "[pools.model_x]\nprovides = ['shard']\n",
            &[]
        ),
        Vec::<String>::new()
    );
    let consumers = [
        outside("entry `messages`", "shard", "model_x", Some("b")),
        outside("pool policy `router`", "shard", "model_x", None),
    ];
    assert_eq!(supply(vec![shard()], "", &consumers), Vec::<String>::new());
}

/// A need that nothing meets fails, naming the kind, the consumer, and
/// the class. A provider outside the class's entry segments meets none:
/// only an entry instance reports its properties.
#[test]
fn an_unmet_need_names_the_kind_the_consumer_and_the_class() {
    let elsewhere = PropertyInfo {
        providers: vec!["F".into()],
        ..shard()
    };
    let consumers = [outside("pool policy `router`", "shard", "model_x", None)];
    assert_eq!(
        supply(vec![elsewhere], "", &consumers),
        [
            "node `F` needs property `shard` from class `model_x`, but no entry segment of class `model_x` (`d`) hosts a node that provides it, and `[pools.model_x] provides` does not name it",
            "pool policy `router` needs property `shard` from class `model_x`, but no entry segment of class `model_x` (`d`) hosts a node that provides it, and `[pools.model_x] provides` does not name it",
        ]
    );
}

/// A need of a kind the graph does not declare, of a class the placement
/// lacks or that no class sends to, or from a segment that does not
/// choose the class, is refused for what it is; so is a `provides` of a
/// kind the graph does not declare.
#[test]
fn a_need_the_layout_cannot_meet_is_refused_for_what_it_is() {
    let consumers = [
        outside("entry `messages`", "model", "model_x", Some("b")),
        outside("entry `chat`", "shard", "model_y", Some("b")),
        outside("entry `admin`", "shard", "global", Some("a")),
        outside("entry `front`", "shard", "model_x", Some("a")),
    ];
    assert_eq!(
        supply(
            vec![shard()],
            "[pools.model_x]\nprovides = ['region']\n",
            &consumers
        ),
        [
            "`[pools.model_x] provides` names property kind `region`, which the graph does not declare",
            "entry `messages` needs property kind `model`, which the graph does not declare",
            "entry `chat` needs property `shard` from class `model_y`, but the placement has no class `model_y`",
            "entry `admin` needs property `shard` from class `global`, but no class sends to class `global`, so none of its pools is chosen",
            "entry `front` needs property `shard` from class `model_x`, but its segment `a` chooses no pool of class `model_x`",
        ]
    );
}

/// A diagram draws the declarations: the title says each kind's type,
/// and a node's label what it provides and needs, with each need's class.
/// A graph that declares none draws as it did.
#[test]
fn a_diagram_draws_what_each_node_provides_and_needs() {
    let declared = manifest(vec![shard()]);
    let mermaid = declared
        .to_mermaid(None, Direction::LeftToRight)
        .expect("a diagram");
    assert!(
        mermaid.contains("title: \"classed; property shard: Shard\""),
        "{mermaid}"
    );
    assert!(
        mermaid.contains("n1[[\"F<br/>operator<br/>needs shard from model_x\"]]"),
        "{mermaid}"
    );
    assert!(
        mermaid.contains("n2[(\"D<br/>sink<br/>provides shard\")]"),
        "{mermaid}"
    );
    let dot = declared
        .to_dot(None, Direction::LeftToRight)
        .expect("a diagram");
    assert!(dot.contains("label=\"property shard: Shard\""), "{dot}");
    assert!(dot.contains("\"D\\nsink\\nprovides shard\""), "{dot}");
    let plain = manifest(vec![]);
    assert!(
        !plain
            .to_mermaid(None, Direction::LeftToRight)
            .expect("a diagram")
            .contains("provides")
    );
    assert!(
        !plain
            .to_dot(None, Direction::LeftToRight)
            .expect("a diagram")
            .contains("labelloc")
    );
}
