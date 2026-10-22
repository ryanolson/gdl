// With several roots no root is "the" source, so the graph defines no
// `SourceReq` or `SourceItem`: a use of either is an error that names it.

#![allow(dead_code)]

gdl_runtime::graph! {
    name = "g", schema = 1;
    source A: u32 -> stream u32;
    source Z: u64 -> stream u64;
    sink   D: u32 -> stream u32;
    sink   E: u64 -> stream u64;
    topology: A => D;
    topology: Z => E;
}

fn main() {
    let _: SourceReq = 0u32;
}
