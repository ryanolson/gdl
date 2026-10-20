// The item type of a streamed edge is checked like any edge type.
fn main() {}

gdl::graph! {
    name = "g", schema = 1;
    source A: u32 + stream u32 -> stream u32;
    sink   D: u32 + stream u64 -> stream u32;
    topology: A => D;
}
