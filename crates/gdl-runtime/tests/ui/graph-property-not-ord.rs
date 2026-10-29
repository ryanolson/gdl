// A property's key type must be one a reader's index can sort and the hub
// can carry: graph! refuses one without `Ord` at its declaration.

#![allow(dead_code)]

#[derive(serde::Serialize, serde::Deserialize)]
struct Shard {
    name: String,
}

gdl_runtime::graph! {
    name = "g", schema = 1;
    property shard: Shard;
    source A: u32 -> stream u32;
    sink   D: u32 -> stream u32 provides shard;
    topology: A => D;
}

fn main() {}
