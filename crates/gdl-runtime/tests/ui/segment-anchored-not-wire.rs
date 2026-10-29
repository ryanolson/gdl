// A generated segment's `segment!` call checks that a response which
// returns through an anchor inside it is a wire type: one that is not
// fails at that anchor's line in the call, naming the message.

#![allow(dead_code)]

/// Not a wire type: no serde impls.
pub struct Opaque;

pub mod g {
    use super::Opaque;

    gdl_runtime::graph! {
        name = "g", schema = 1;
        source A: u32 -> stream Opaque;
        sink   B: u32 -> stream Opaque;
        topology: A => B;
    }
}

gdl_runtime::segment! {
    contract: crate::g;
    package: "g-a";
    segment: "a";
    segments: ["a"];
    placement: "[segment.a]\nnodes = [\"A\", \"B\"]\n";
    graph_config: "";
    roots: [A];
    crossings: [
        anchors B_to_A = A SourceDecl Item;
    ];
    feeds: [];
    nodes: [];
    policies: [];
    serve: none;
}

fn main() {}
