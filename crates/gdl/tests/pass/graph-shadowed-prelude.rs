// Names that the caller defines: items that shadow the standard prelude and
// primitive types, and constants named like the parameters of the generated
// functions (a parameter name is a pattern, and a constant in scope would
// turn it into a constant pattern). The expansion must not name them, so
// they stay unused here.

#![allow(dead_code, unused_imports, unused_macros, non_camel_case_types, non_upper_case_globals)]

struct str;

const deployment: u8 = 0;
const bindings: u8 = 0;
const implementation: u8 = 0;

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

fn absent(at: &gdl::Deployment) {
    let _ = assemble(at, Bindings::new());
}

fn main() {
    let _ = manifest();
}
