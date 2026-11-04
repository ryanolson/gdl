//! Unary nodes and `calls` in both hashes. The contract hash covers each
//! unary node (its name, its role, its request and response with their
//! encodings and wire revisions, and `idempotent`) and each node's `calls`,
//! in any order they are written. The placement hash covers the segments a
//! unary node is listed in, and its `[routing]` entry. A graph or a
//! placement without unary nodes keeps the hash it had before them, so
//! these are pinned as literals.

use std::collections::BTreeSet;

use gdl_model::{Encoding, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo, UnaryInfo};

fn ty(name: &str) -> TypeInfo {
    TypeInfo::new(name, true, Encoding::default())
}

fn node(name: &str, role: Role) -> NodeInfo {
    NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
        calls: vec![],
        unary: None,
    }
}

fn hop(outer: &str, inner: &str) -> HopInfo {
    HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![ty(":: p :: Prompt")],
        backward_types: vec![ty(":: p :: Chunk")],
    }
}

/// `S => R { D }`, and `nodes` after them.
fn manifest(more: Vec<NodeInfo>) -> Manifest {
    let mut nodes = vec![
        node("S", Role::Source),
        node("R", Role::Operator),
        node("D", Role::Sink),
    ];
    nodes.extend(more);
    Manifest {
        name: "called".into(),
        schema: 1,
        nodes,
        hops: vec![hop("S", "R"), hop("R", "D")],
        properties: vec![],
    }
}

/// `unary Tok: Text -> Tokens idempotent;`
fn tok() -> NodeInfo {
    NodeInfo {
        unary: Some(UnaryInfo {
            req: ty(":: p :: Text"),
            resp: ty(":: p :: Tokens"),
            idempotent: true,
        }),
        ..node("Tok", Role::Unary)
    }
}

/// `manifest(more)` with R's `calls` set to `services`.
fn calling(services: &[&str], more: Vec<NodeInfo>) -> Manifest {
    let mut manifest = manifest(more);
    manifest.nodes[1].calls = services.iter().map(|s| s.to_string()).collect();
    manifest
}

/// The contract hash of `manifest(vec![])`, taken before unary nodes
/// existed.
const PLAIN_HASH: u64 = 0xdc01_e0f6_f3a0_0748;

#[test]
fn a_graph_without_unary_nodes_keeps_its_contract_hash() {
    assert_eq!(manifest(vec![]).contract_hash(), PLAIN_HASH);
}

#[test]
fn each_part_of_a_unary_node_and_of_calls_changes_the_contract_hash() {
    let with = |change: &dyn Fn(&mut NodeInfo)| {
        let mut service = tok();
        change(&mut service);
        calling(&["Tok"], vec![service]).contract_hash()
    };
    let types = |change: &dyn Fn(&mut UnaryInfo)| {
        with(&|node: &mut NodeInfo| change(node.unary.as_mut().expect("types")))
    };
    let other = NodeInfo {
        name: "Other".into(),
        ..tok()
    };
    let hashes = [
        manifest(vec![]).contract_hash(),
        manifest(vec![tok()]).contract_hash(),
        calling(&["Tok"], vec![tok()]).contract_hash(),
        with(&|n| n.name = "Tok2".into()),
        types(&|t| t.req.name = ":: p :: Other".into()),
        types(&|t| t.req.encoding = Encoding::Json),
        types(&|t| t.resp.name = ":: p :: Other".into()),
        types(&|t| t.resp.encoding = Encoding::MsgpackArray),
        types(&|t| t.idempotent = false),
        // The service calls something itself.
        calling(
            &["Tok"],
            vec![
                NodeInfo {
                    calls: vec!["Other".into()],
                    ..tok()
                },
                other.clone(),
            ],
        )
        .contract_hash(),
        calling(&["Tok"], vec![tok(), other.clone()]).contract_hash(),
        calling(&["Tok", "Other"], vec![tok(), other.clone()]).contract_hash(),
        calling(&["Other"], vec![tok(), other]).contract_hash(),
    ];
    let distinct: BTreeSet<u64> = hashes.iter().copied().collect();
    assert_eq!(distinct.len(), hashes.len(), "{hashes:x?}");
}

/// The wire revision of an encoding enters the hash as an edge type's
/// does: the request and response are hashed by the same rule.
#[test]
fn a_unary_node_hashes_its_types_as_an_edge_hashes_its_type() {
    let edge = |name: &str| {
        let mut manifest = manifest(vec![]);
        manifest.hops[0].forward_types[0] = ty(name);
        manifest.contract_hash()
    };
    let service = |name: &str| {
        let mut service = tok();
        service.unary.as_mut().expect("types").req = ty(name);
        manifest(vec![service]).contract_hash()
    };
    assert_ne!(edge(":: p :: A"), edge(":: p :: B"));
    assert_ne!(service(":: p :: A"), service(":: p :: B"));
}

#[test]
fn the_order_of_unary_nodes_and_calls_does_not_change_the_contract_hash() {
    let other = NodeInfo {
        name: "Other".into(),
        ..tok()
    };
    let base = calling(&["Tok", "Other"], vec![tok(), other.clone()]);
    let reordered = calling(&["Other", "Tok"], vec![other.clone(), tok()]);
    assert_eq!(base.contract_hash(), reordered.contract_hash());

    // A unary node declared before the topology's nodes.
    let mut first = base.clone();
    let services = first.nodes.split_off(3);
    first.nodes.splice(0..0, services);
    assert_eq!(base.contract_hash(), first.contract_hash());
}

const SPLIT: &str = "[segment.front]\nnodes = ['S', 'R']\n[segment.engine]\nnodes = ['D']\n[routing]\nD = 'random'\n";

/// The placement hash of [`SPLIT`], taken before unary nodes existed.
const SPLIT_HASH: u64 = 0x234b_4159_917f_cc78;

fn hash(text: &str) -> u64 {
    Placement::from_toml(text).expect("a placement").hash()
}

#[test]
fn a_placement_without_unary_nodes_keeps_its_hash() {
    assert_eq!(hash(SPLIT), SPLIT_HASH);
}

#[test]
fn where_a_unary_node_is_listed_and_its_router_change_the_placement_hash() {
    let hashes = [
        hash(SPLIT),
        hash(&format!("{SPLIT}[segment.tok]\nnodes = ['Tok']\n")),
        hash(
            "[segment.front]\nnodes = ['S', 'R', 'Tok']\n[segment.engine]\nnodes = ['D']\n[routing]\nD = 'random'\n",
        ),
        hash(
            "[segment.front]\nnodes = ['S', 'R', 'Tok']\n[segment.engine]\nnodes = ['D', 'Tok']\n[routing]\nD = 'random'\n",
        ),
        hash(&format!(
            "{SPLIT}Tok = 'random'\n[segment.tok]\nnodes = ['Tok']\n"
        )),
        hash(&format!(
            "{SPLIT}Tok = {{ selector = 'affinity' }}\n[segment.tok]\nnodes = ['Tok']\n"
        )),
    ];
    let distinct: BTreeSet<u64> = hashes.iter().copied().collect();
    assert_eq!(distinct.len(), hashes.len(), "{hashes:x?}");
}
