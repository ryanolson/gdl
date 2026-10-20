// The graph! macro must reject this with an error that points at the cause.

#![allow(dead_code)]

gdl::graph! {
    name = "g", schema = 1;
    source   A: u32 -> stream u32;
    operator B: (u32 -> stream u32) over (u32 -> stream u32);
    sink     D: u32 -> stream u32;
    topology: A => B => D;
}

fn main() {}
