//! `graph!` with unary nodes: the reference graph, with two unary nodes and
//! `calls` clauses. Each unary node has a declaration marker with its
//! types and a manifest entry, and no slot in `Bindings`, so the topology
//! binds and runs as it does without them.

mod common;

use std::marker::PhantomData;
use std::sync::Arc;

use common::*;
use gdl_runtime::{NodeDecl, Role, TypeInfo, UnaryDecl, UnaryInfo};

mod contract {
    use super::common::{Chunk, Out, Pre, Prompt, Raw, Tok};

    gdl_runtime::graph! {
        name = "reference", schema = 1;
        unary    Lookup: Prompt -> Pre idempotent;
        source   A: Prompt -> stream Chunk;
        operator B: (Prompt -> stream Chunk) over (Pre -> stream Out) calls Lookup, Score;
        forward  C: Pre -> Pre;
        sink     D: Pre -> stream Raw calls Score;
        backward E: Tok -> Out;
        backward F: Raw -> Tok;
        backward G: Chunk -> Chunk;
        unary    Score: Pre -> Tok calls Lookup;
        topology: A => B { C => D => F => E } => G;
    }
}

#[test]
fn the_manifest_has_the_unary_nodes_and_every_calls_clause() {
    let mut expected = reference_manifest(|_| true);
    let ty = |name: &str| TypeInfo::new(name, true, Default::default());
    let unary = |name: &str, req: &str, resp: &str, idempotent: bool| gdl_runtime::NodeInfo {
        name: name.into(),
        role: Role::Unary,
        binding: Some(name.to_lowercase()),
        calls: vec![],
        unary: Some(UnaryInfo {
            req: ty(req),
            resp: ty(resp),
            idempotent,
        }),
    };
    expected
        .nodes
        .insert(0, unary("Lookup", "Prompt", "Pre", true));
    expected.nodes.push(gdl_runtime::NodeInfo {
        calls: vec!["Lookup".into()],
        ..unary("Score", "Pre", "Tok", false)
    });
    let mut calls = |name: &str, services: &[&str]| {
        let node = expected
            .nodes
            .iter_mut()
            .find(|n| n.name == name)
            .expect("present");
        node.calls = services.iter().map(|s| s.to_string()).collect();
    };
    calls("B", &["Lookup", "Score"]);
    calls("D", &["Score"]);
    let generated = contract::manifest();
    assert_eq!(generated, expected);
    assert_ne!(
        generated.contract_hash(),
        reference_manifest(|_| true).contract_hash()
    );
}

#[test]
fn a_unary_node_declares_its_role_and_types() {
    fn same<T>(_: PhantomData<T>, _: PhantomData<T>) {}
    same(
        PhantomData::<<contract::nodes::Lookup as UnaryDecl>::Req>,
        PhantomData::<Prompt>,
    );
    same(
        PhantomData::<<contract::nodes::Lookup as UnaryDecl>::Resp>,
        PhantomData::<Pre>,
    );
    assert_eq!(<contract::nodes::Score as NodeDecl>::ROLE, Role::Unary);
    assert_eq!(<contract::nodes::Score as NodeDecl>::NAME, "Score");
}

/// The bindings are the reference graph's: one slot per pipeline node,
/// and none for a unary node.
#[test]
fn a_unary_node_takes_no_slot_in_the_bindings() {
    let events = Arc::new(Events::default());
    let _bound: contract::Bindings<NodeB, NodeC, NodeD, NodeE, NodeF, NodeG> =
        contract::Bindings::new()
            .b(NodeB {
                events: Arc::clone(&events),
            })
            .c(NodeC {
                events: Arc::clone(&events),
            })
            .d(NodeD {
                events: Arc::clone(&events),
            })
            .e(NodeE)
            .f(NodeF)
            .g(NodeG);
}
