// A generated segment's `segment!` call checks that each message it sends
// or receives across a cut is a wire type: a crossing that carries one that
// is not fails at that crossing's line in the call, naming the message.

#![allow(dead_code)]

/// Not a wire type: no serde impls.
pub struct Opaque;

pub mod g {
    use super::Opaque;

    gdl_runtime::graph! {
        name = "g", schema = 1;
        source A: Opaque -> stream u32;
        sink   B: Opaque -> stream u32;
        topology: A => B;
    }
}

gdl_runtime::segment! {
    contract: crate::g;
    package: "g-a";
    segment: "a";
    segments: ["a", "b"];
    placement: "[segment.a]\nnodes = [\"A\"]\n\n[segment.b]\nnodes = [\"B\"]\n";
    graph_config: "";
    roots: [A];
    crossings: [
        crosses B_to_A = A SourceDecl Item;
        crosses A_to_B = B SinkDecl Req;
    ];
    feeds: [];
    nodes: [];
    policies: [];
    serve: none;
}

fn main() {}
