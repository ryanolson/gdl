// A forward half with no backward half leaves its value on the stack: the
// backward chain's end takes only an empty one, so the hop is no `Next`.

use std::sync::Arc;

use gdl::{Around, Cx, Hop, Items, Next, Result, Sink, SinkStage, around};

struct Numbers;

impl Sink for Numbers {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, _cx: Cx, req: u32) -> Result<Items<u32>> {
        Ok(gdl::items(futures::stream::iter([Ok(req)])))
    }
}

/// An around node; `K` makes each one its own type.
struct Keep<const K: u32>;

impl<const K: u32> Around for Keep<K> {
    type UpReq = u32;
    type UpItem = u32;
    type DownReq = u32;
    type DownItem = u32;
    type State = u32;

    async fn forward(&self, _cx: &Cx, req: u32) -> Result<(u32, u32)> {
        Ok((req, K))
    }

    fn backward(&self, _cx: &Cx, state: u32, input: Items<u32>) -> Items<u32> {
        gdl::items(futures::StreamExt::map(input, move |item| item.map(|n| n + state)))
    }
}

async fn call<N: Next<u32, u32>>(next: N) {
    let _ = next.generate(Cx::new(), 1).await;
}

fn main() {
    let (forward, _backward) = around(Arc::new(Keep::<1>));
    let hop = Hop::new((forward, ()), SinkStage::new(Arc::new(Numbers)), ());
    let _ = call(hop);
}
