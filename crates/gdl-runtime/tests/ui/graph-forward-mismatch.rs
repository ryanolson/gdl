// The graph! macro must reject this with an error that points at the cause.

#![allow(dead_code)]

gdl_runtime::graph! {
    name = "g", schema = 1;
    source  A: u32 -> stream u32;
    forward C: u64 -> u64;
    sink    D: u64 -> stream u32;
    topology: A => C => D;
}

fn main() {}
