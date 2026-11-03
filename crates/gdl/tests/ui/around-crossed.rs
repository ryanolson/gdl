// Two pairs whose halves cross instead of nesting: the first backward half
// meets the other node's value on top of the stack.

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
    let (first_forward, first_backward) = around(Arc::new(Keep::<1>));
    let (second_forward, second_backward) = around(Arc::new(Keep::<2>));
    let hop = Hop::new(
        (first_forward, (second_forward, ())),
        SinkStage::new(Arc::new(Numbers)),
        (first_backward, (second_backward, ())),
    );
    let _ = call(hop);
}
