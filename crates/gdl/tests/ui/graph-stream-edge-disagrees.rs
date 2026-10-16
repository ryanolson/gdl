// A forward edge carries a stream at both of its ends or at neither.
fn main() {}

gdl::graph! {
    name = "g", schema = 1;
    source A: u32 + stream u32 -> stream u32;
    sink   D: u32 -> stream u32;
    topology: A => D;
}
