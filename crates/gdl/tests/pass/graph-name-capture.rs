// Message types that share a name with a node or with a name the expansion
// introduces keep their meaning.

#![allow(dead_code)]

pub type D = String;
pub struct X;
pub struct DImpl;

gdl::graph! {
    name = "g", schema = 1;
    source  A: X -> stream DImpl;
    forward C: X -> D;
    sink    D: D -> stream DImpl;
    topology: A => C => D;
}

fn absent(deployment: &gdl::Deployment) {
    let _ = assemble(deployment, Bindings::new());
}

fn main() {
    let _ = manifest();
}
