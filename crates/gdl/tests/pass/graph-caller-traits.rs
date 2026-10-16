// Callers' traits in scope whose method or function names match one the
// expansion calls.

#![allow(dead_code)]

trait MyCodec {
    fn codec(&self) {}
}

impl<T> MyCodec for T {}

trait MyFrom {
    fn from(text: &str) -> Self;
}

impl MyFrom for String {
    fn from(text: &str) -> Self {
        text.to_uppercase()
    }
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
