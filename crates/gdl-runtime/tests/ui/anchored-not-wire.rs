// A generated segment asserts that a response which returns through an
// anchor inside it is a wire type, as `cargo gdl generate` writes it: one
// that is not fails here, naming the message and the edge, with advice
// for a hop whose ends share the segment.

#![allow(dead_code)]

/// Not a wire type: no serde impls.
pub struct Opaque;

gdl_runtime::graph! {
    name = "g", schema = 1;
    source A: u32 -> stream Opaque;
    sink   B: u32 -> stream Opaque;
    topology: A => B;
}

#[allow(non_camel_case_types)]
type B_to_A = <nodes::A as gdl_runtime::SourceDecl>::Item;
const _: () = gdl_runtime::gdl::__private::anchors::<B_to_A>();

fn main() {}
