// A node whose binding name matches a parameter of the generated functions.

#![allow(dead_code)]

gdl::graph! {
    name = "g", schema = 1;
    source  A: u32 -> stream u32;
    forward Deployment: u32 -> u32;
    forward Bindings: u32 -> u32;
    sink    D: u32 -> stream u32;
    topology: A => Deployment => Bindings => D;
}

fn absent(deployment: &gdl::Deployment) {
    let _ = assemble(deployment, Bindings::new());
}

fn main() {
    let _ = local(Bindings::new());
}
