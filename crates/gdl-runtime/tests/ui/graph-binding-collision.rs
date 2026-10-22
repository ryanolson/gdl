// The graph! macro must reject this with an error that points at the cause.

#![allow(dead_code)]

gdl_runtime::graph! {
    name = "g", schema = 1;
    source  A: u32 -> stream u32;
    forward FooBar: u32 -> u32;
    sink    foo_bar: u32 -> stream u32;
    topology: A => FooBar => foo_bar;
}

fn main() {}
