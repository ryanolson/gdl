// A port type named relative to the invocation keeps its meaning in the
// expansion.

#![allow(dead_code)]

pub struct Req;
pub struct Item;

mod contract {
    gdl_runtime::graph! {
        name = "g", schema = 1;
        source  A: super::Req -> stream super::Item;
        sink    D: super::Req -> stream super::Item;
        topology: A => D;
    }

    pub fn absent(deployment: &gdl_runtime::Deployment) {
        let _ = assemble(deployment, Bindings::new());
    }
}

fn main() {
    let _ = contract::manifest();
}
