// A forward chain whose output does not match the inner end's request is a
// type error at the point the hop is used.

use std::sync::Arc;

use gdl::{Cx, Forward, Hop, Items, Next, Result, Sink, SinkStage};

struct ToText;

impl Forward for ToText {
    type In = u32;
    type Out = String;

    async fn forward(&self, _cx: &Cx, req: u32) -> Result<String> {
        Ok(req.to_string())
    }
}

struct Numbers;

impl Sink for Numbers {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, _cx: Cx, req: u32) -> Result<Items<u32>> {
        Ok(gdl::items(futures::stream::iter([Ok(req)])))
    }
}

async fn call<N: Next<u32, u32>>(next: N) {
    let _ = next.generate(Cx::new(), 1).await;
}

fn main() {
    let hop = Hop::new((Arc::new(ToText), ()), SinkStage::new(Arc::new(Numbers)), ());
    let _ = call(hop);
}
