// Only requests stream: a backward node's types are item types.
fn main() {}

gdl_runtime::graph! {
    name = "g", schema = 1;
    source   A: u32 -> stream u32;
    sink     D: u32 -> stream u32;
    backward F: u32 + stream u32 -> u32;
    topology: A => D => F;
}
