// A sink that takes a stream cannot follow an edge that carries a value.
fn main() {}

gdl::graph! {
    name = "g", schema = 1;
    source A: u32 -> stream u32;
    sink   D: u32 + stream u32 -> stream u32;
    topology: A => D;
}
