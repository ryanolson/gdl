// A generated segment asserts that each message it sends or receives across
// a cut is a wire type, as `cargo gdl generate` writes it: a crossing that
// carries one that is not fails here, naming the message and the crossing.

#![allow(dead_code)]

/// Not a wire type: no serde impls.
pub struct Opaque;

gdl_runtime::graph! {
    name = "g", schema = 1;
    source A: Opaque -> stream u32;
    sink   B: Opaque -> stream u32;
    topology: A => B;
}

#[allow(non_camel_case_types)]
type A_to_B = <nodes::B as gdl_runtime::SinkDecl>::Req;
#[allow(non_camel_case_types)]
type B_to_A = <nodes::A as gdl_runtime::SourceDecl>::Item;
const _: () = gdl_runtime::gdl::__private::crosses::<A_to_B>();
const _: () = gdl_runtime::gdl::__private::crosses::<B_to_A>();

fn main() {}
