// Names from the standard prelude that the caller shadows. The expansion
// must not name them, so they stay unused here.

#![allow(dead_code, unused_imports, unused_macros)]

enum Shadow {
    None,
    Some(u8),
    Ok(u8),
}
use Shadow::*;

macro_rules! vec {
    ($($tokens:tt)*) => {
        ()
    };
}

gdl::graph! {
    name = "g", schema = 1;
    source  A: u32 -> stream u32;
    sink    D: u32 -> stream u32;
    topology: A => D;
}

fn absent(deployment: &gdl::Deployment) {
    let _ = assemble(deployment, Bindings::new());
}

fn main() {
    let _ = manifest();
}
