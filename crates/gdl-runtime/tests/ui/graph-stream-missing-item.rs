// `stream` needs the item type after it.
fn main() {}

gdl_runtime::graph! {
    name = "g", schema = 1;
    source A: u32 + stream -> stream u32;
    sink   D: u32 + stream u32 -> stream u32;
    topology: A => D;
}
