//! Every edge of a graph, with the type its sending end declares and the
//! type its receiving end declares: what a graph file's reader compares.

use gdl_syntax::{Edge, Graph, Request};
use proc_macro2::TokenStream;
use syn::Type;

fn graph(text: &str) -> Graph {
    Graph::parse(text.parse::<TokenStream>().expect("tokens")).expect("graph")
}

fn ty(ty: &Type) -> String {
    quote::quote!(#ty).to_string()
}

fn request(req: &Request) -> String {
    match &req.item {
        None => ty(&req.head),
        Some(item) => format!("{} + stream {}", ty(&req.head), ty(item)),
    }
}

fn show<T>(edges: &[Edge<'_, T>], f: impl Fn(&T) -> String) -> Vec<String> {
    edges
        .iter()
        .map(|e| format!("{} -> {}: {} / {}", e.from, e.to, f(e.sent), f(e.taken)))
        .collect()
}

#[test]
fn every_edge_pairs_what_one_end_sends_with_what_the_next_takes() {
    let graph = graph(
        r#"
        name = "g", schema = 1;
        source   A: Req -> stream Out;
        operator B: (Req -> stream Out) over (Pre -> stream Tok);
        forward  C: Pre -> Mid + stream Frame;
        sink     D: Midd + stream Frame -> stream Raw;
        backward E: Raw -> Tok;
        backward G: Out -> Outt;
        topology: A => B { C => D => E } => G;
        "#,
    );
    assert_eq!(
        show(&graph.forward_edges(), request),
        [
            "A -> B: Req / Req",
            "B -> C: Pre / Pre",
            "C -> D: Mid + stream Frame / Midd + stream Frame",
        ]
    );
    assert_eq!(
        show(&graph.backward_edges(), ty),
        [
            "B -> G: Out / Out",
            "G -> A: Outt / Out",
            "D -> E: Raw / Raw",
            "E -> B: Tok / Tok",
        ]
    );
}
