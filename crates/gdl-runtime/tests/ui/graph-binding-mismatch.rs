// The graph! macro must reject this with an error that points at the cause.

#![allow(dead_code)]

use gdl::{Cx, Items, Result};

gdl_runtime::graph! {
    name = "g", schema = 1;
    source A: u32 -> stream u32;
    sink   D: u32 -> stream u32;
    topology: A => D;
}

/// Takes a `String`, but node D takes a `u32`.
struct Wrong;

impl gdl::Sink for Wrong {
    type Req = String;
    type Item = u32;

    async fn generate(&self, _cx: Cx, _req: String) -> Result<Items<u32>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

fn main() {
    let _ = Bindings::new().d(Wrong);
}
