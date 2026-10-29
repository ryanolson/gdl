// A node provides only a declared property kind.

#![allow(dead_code)]

gdl_runtime::graph! {
    name = "g", schema = 1;
    source A: u32 -> stream u32;
    sink   D: u32 -> stream u32 provides shard;
    topology: A => D;
}

fn main() {}
