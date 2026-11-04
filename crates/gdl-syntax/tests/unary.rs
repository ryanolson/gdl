//! Unary nodes in a graph's text: `unary Tok: Text -> Tokens;` declares a
//! service apart from the topology, `idempotent` after its signature marks
//! it safe to run twice, and `calls A, B` after any node's signature names
//! the unary nodes it calls. One parser serves `graph!` and graph files,
//! so these hold for both.

use gdl_model::{Encoding, NodeInfo, Role, TypeInfo, UnaryInfo};
use gdl_syntax::Graph;

const GRAPH: &str = r#"
    name = "called", schema = 1;
    property model: Model;
    unary    Tok:    Text -> Tokens idempotent;
    source   Http:   Req -> stream Chunk;
    operator Route:  (Req -> stream Chunk) over (Req -> stream Chunk) calls Tok, Overlap needs model from sets;
    unary    Overlap: Blocks -> Scores calls Tok;
    sink     Engine: Req -> stream Chunk provides model calls Tok;
    topology: Http => Route { Engine };
"#;

fn parse(text: &str) -> syn::Result<Graph> {
    Graph::parse(text.parse().expect("tokens"))
}

fn wire(name: &str) -> Option<TypeInfo> {
    Some(TypeInfo::new(format!("p::{name}"), true, Encoding::Json))
}

fn info(name: &str) -> TypeInfo {
    TypeInfo::new(format!(":: p :: {name}"), true, Encoding::Json)
}

#[test]
fn a_graph_declares_unary_nodes_and_who_calls_them() {
    let graph = parse(GRAPH).expect("a graph");
    let manifest = graph.manifest(&wire).expect("a manifest");
    let node = |name: &str| -> &NodeInfo { manifest.node(name).expect(name) };
    assert_eq!(
        node("Tok"),
        &NodeInfo {
            name: "Tok".into(),
            role: Role::Unary,
            binding: Some("tok".into()),
            calls: vec![],
            unary: Some(UnaryInfo {
                req: info("Text"),
                resp: info("Tokens"),
                idempotent: true,
            }),
        }
    );
    assert_eq!(
        node("Overlap").unary,
        Some(UnaryInfo {
            req: info("Blocks"),
            resp: info("Scores"),
            idempotent: false,
        })
    );
    assert_eq!(node("Overlap").calls, ["Tok"]);
    assert_eq!(node("Route").calls, ["Tok", "Overlap"]);
    assert_eq!(node("Engine").calls, ["Tok"]);
    assert!(node("Http").calls.is_empty());
    assert_eq!(node("Route").unary, None);
    // The clauses keep what they said before.
    assert_eq!(manifest.properties[0].providers, ["Engine"]);
    assert_eq!(manifest.properties[0].needs[0].node, "Route");
    // A unary node is on no hop.
    assert_eq!(manifest.hops.len(), 2);
    assert!(manifest.hops.iter().all(|hop| {
        hop.forward_path()
            .chain(hop.backward_path())
            .all(|n| n != "Tok")
    }));
}

#[test]
fn the_clauses_after_a_signature_come_in_any_order() {
    let reordered = GRAPH
        .replace(
            "calls Tok, Overlap needs model from sets",
            "needs model from sets calls Tok, Overlap",
        )
        .replace("provides model calls Tok", "calls Tok provides model");
    let one = parse(GRAPH).expect("a graph").manifest(&wire).expect("one");
    let two = parse(&reordered)
        .expect("a graph")
        .manifest(&wire)
        .expect("two");
    assert_eq!(one, two);
    let calls_first = GRAPH.replace(
        "Blocks -> Scores calls Tok;",
        "Blocks -> Scores calls Tok idempotent;",
    );
    let manifest = parse(&calls_first)
        .expect("a graph")
        .manifest(&wire)
        .expect("a manifest");
    assert!(
        manifest
            .node("Overlap")
            .expect("present")
            .unary
            .as_ref()
            .expect("present")
            .idempotent
    );
}

/// Each mistake is refused, naming the node and the clause.
#[test]
fn a_unary_node_or_a_calls_clause_that_cannot_hold_is_refused() {
    let cases = [
        (
            GRAPH.replace("calls Tok, Overlap", "calls Tok, Missing"),
            "node `Route` has `calls Missing`, but the graph declares no unary node `Missing`",
        ),
        (
            GRAPH.replace("unary    Tok:    Text -> Tokens idempotent;", "unary Tok: Text -> Tokens calls Overlap;"),
            "the `calls` clauses make a cycle: `Overlap` calls `Tok`, and `Tok` calls `Overlap`",
        ),
        (
            GRAPH.replace("Blocks -> Scores calls Tok;", "Blocks -> Scores calls Overlap;"),
            "`Overlap` calls `Overlap`",
        ),
        (
            GRAPH.replace("topology: Http => Route { Engine };", "topology: Http => Route { Tok => Engine };"),
            "unary node `Tok` is in the topology; a unary node is on no hop, and a node reaches it with `calls Tok`",
        ),
        (
            GRAPH.replace("topology: Http => Route { Engine };", "topology: Tok => Route { Engine };"),
            "unary node `Tok` is in the topology",
        ),
        (
            GRAPH.replace("calls Tok, Overlap", "calls Tok, Engine"),
            "node `Route` has `calls Engine`, but `Engine` is a sink in the topology; `calls` names only unary nodes",
        ),
        (
            GRAPH.replace("calls Tok, Overlap", "calls Tok, Tok"),
            "node `Route` has `calls Tok` twice",
        ),
        (
            GRAPH.replace("provides model calls Tok", "provides model calls Tok calls Overlap"),
            "`calls` is said once",
        ),
        (
            GRAPH.replace("Text -> Tokens idempotent", "Text -> Tokens idempotent idempotent"),
            "`idempotent` is said once",
        ),
        (
            GRAPH.replace("provides model calls Tok", "provides model idempotent"),
            "node `Engine` is a sink; only a unary node is `idempotent`",
        ),
        (
            GRAPH.replace("Text -> Tokens", "stream Text -> Tokens"),
            "a unary node takes one request and returns one response; neither is streamed",
        ),
        (
            GRAPH.replace("Text -> Tokens", "Text -> stream Tokens"),
            "a unary node takes one request and returns one response; neither is streamed",
        ),
        (
            GRAPH.replace("Text -> Tokens", "Head + stream Text -> Tokens"),
            "a unary node takes one request and returns one response; neither is streamed",
        ),
        (
            GRAPH.replace(
                "operator Route:  (Req -> stream Chunk) over (Req -> stream Chunk) calls Tok, Overlap needs model from sets",
                "identity Route: Req -> stream Chunk calls Tok",
            ),
            "a built-in identity has no implementation, so it calls no unary node",
        ),
        (
            GRAPH.replace("unary    Overlap", "unary    Route"),
            "node declared twice",
        ),
        (
            GRAPH.replace("Text -> Tokens idempotent;", "Text -> Tokens idempotent provides model;"),
            "unary node `Tok` has `provides model`; a unary node cannot provide or need a property",
        ),
        (
            GRAPH.replace("Blocks -> Scores calls Tok;", "Blocks -> Scores calls Tok needs model from sets;"),
            "unary node `Overlap` has `needs model from sets`; a unary node cannot provide or need a property",
        ),
        // A unary node gets a binding, so the binding names stay unique.
        (
            GRAPH.replace("unary    Overlap: Blocks -> Scores calls Tok;", "unary route: Blocks -> Scores;")
                .replace(", Overlap", ""),
            "both get the binding name `route`",
        ),
    ];
    for (text, why) in cases {
        let error = match parse(&text) {
            Ok(_) => panic!("accepted {text}"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains(why), "{why}: {error}");
    }
}

#[test]
fn a_graph_without_unary_nodes_has_no_calls() {
    let text = GRAPH
        .replace("unary    Tok:    Text -> Tokens idempotent;", "")
        .replace("unary    Overlap: Blocks -> Scores calls Tok;", "")
        .replace(" calls Tok, Overlap", "")
        .replace(" calls Tok", "");
    let manifest = parse(&text)
        .expect("a graph")
        .manifest(&wire)
        .expect("a manifest");
    assert!(
        manifest
            .nodes
            .iter()
            .all(|n| n.calls.is_empty() && n.unary.is_none())
    );
}

/// A remote call encodes the request and the response, so the types a
/// manifest records for a unary node carry their wire capability, as an
/// edge's do; the placement decides whether it matters.
#[test]
fn a_unary_nodes_types_carry_their_wire_capability() {
    let manifest = parse(GRAPH)
        .expect("a graph")
        .manifest(&|name| Some(TypeInfo::new(name, name != "Text", Encoding::Msgpack)))
        .expect("a manifest");
    let tok = manifest
        .node("Tok")
        .expect("present")
        .unary
        .as_ref()
        .expect("present");
    assert!(!tok.req.wire);
    assert!(tok.resp.wire);
}
