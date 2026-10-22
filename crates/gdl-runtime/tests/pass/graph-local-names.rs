// Nodes whose names match names the expansion uses: parameters, locals for
// hops, a setter's generic, and a reserved keyword.

#![allow(dead_code, non_camel_case_types)]

gdl_runtime::graph! {
    name = "g", schema = 1;
    source   A: u32 -> stream u32;
    operator B: (u32 -> stream u32) over (u32 -> stream u32);
    forward  Deployment: u32 -> u32;
    forward  Bindings: u32 -> u32;
    forward  Implementation: u32 -> u32;
    forward  Bound: u32 -> u32;
    forward  Abstract: u32 -> u32;
    sink     D: u32 -> stream u32;
    backward Hop_1: u32 -> u32;
    backward Local_1: u32 -> u32;
    topology: A => B { Deployment => Bindings => Implementation => Bound => Abstract => D } => Hop_1 => Local_1;
}

fn absent(deployment: &gdl_runtime::Deployment) {
    let _ = assemble(deployment, Bindings::new());
}

fn main() {
    let _ = local(Bindings::new());
}
