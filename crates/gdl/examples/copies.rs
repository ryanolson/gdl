//! Local hops over large items and a large request, for `tests/codegen.rs`,
//! which reads this example's release IR.
//!
//! - A node boundary, and the stream of a streamed request, hand each 2 KiB
//!   item on without a copy. `Copying`, a boundary that copies, is the
//!   control.
//! - A 4 KiB request goes through an endpoint's shape, an operator over a hop
//!   with one forward node and a join. The operator's future is built on the
//!   heap, not copied there. `boxed`, which boxes an operator's future as
//!   `Box::pin` does, is the control.
//!
//! Run, it sends one stream and one request through them and checks them.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use gdl::{
    Cx, Forward, Hop, Items, Join, Next, Operator, OperatorStage, Result, Sink, SinkStage, Source,
    Streamed, items,
};

/// An item about as large as a chat chunk, with a field to drop.
struct Big {
    text: String,
    words: [u64; 254],
}

/// Answers with `n` items.
struct Burst;

impl Sink for Burst {
    type Req = u64;
    type Item = Big;

    async fn generate(&self, _cx: Cx, n: u64) -> Result<Items<Big>> {
        Ok(items(futures::stream::iter((0..n).map(|i| {
            Ok(Big {
                text: i.to_string(),
                words: [i; 254],
            })
        }))))
    }
}

/// A boundary in the shape that copies: it holds the poll across a call.
struct Copying {
    inner: Items<Big>,
    ends: u32,
}

#[inline(never)]
fn ended(ends: &mut u32) {
    *ends += 1;
}

impl Stream for Copying {
    type Item = Result<Big>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Big>>> {
        let this = &mut *self;
        let polled = this.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Err(_)) | None) = &polled {
            ended(&mut this.ends);
        }
        polled
    }
}

/// A request as large as a few of an endpoint's (768 B).
struct Ask {
    text: String,
    words: [u64; 500],
}

/// Passes the request on, as an endpoint's translator does. It also holds
/// 16 KiB of its own while it waits, so that a copy of its future stands
/// out from any other copy here.
struct Relay;

impl Operator for Relay {
    type UpReq = Ask;
    type UpItem = u64;
    type DownReq = Ask;
    type DownItem = u64;

    async fn generate<N: Next<Ask, u64>>(&self, cx: Cx, ask: Ask, next: N) -> Result<Items<u64>> {
        let held = [ask.words[0]; 2048];
        let items = next.generate(cx, ask).await;
        std::hint::black_box(&held);
        items
    }
}

/// Changes the request on its way, as an endpoint's builder does.
struct Stamp;

impl Forward for Stamp {
    type In = Ask;
    type Out = Ask;

    async fn forward(&self, _cx: &Cx, mut ask: Ask) -> Result<Ask> {
        ask.words[1] = ask.words[0] + 1;
        Ok(ask)
    }
}

/// Answers with one item read from the request, as an engine would.
#[derive(Clone)]
struct Engine;

impl Next<Ask, u64> for Engine {
    async fn generate(&self, _cx: Cx, ask: Ask) -> Result<Items<u64>> {
        let answer = ask.words[1] + ask.text.len() as u64;
        Ok(items(futures::stream::iter([Ok(answer)])))
    }
}

/// The control: an operator's future built on the stack and copied whole
/// into its box.
#[inline(never)]
fn boxed<N: Next<Ask, u64>>(
    cx: Cx,
    ask: Ask,
    next: N,
) -> Pin<Box<dyn Future<Output = Result<Items<u64>>> + Send>> {
    Box::pin(Relay.generate(cx, ask, next))
}

fn main() {
    let graph = Source::new(Hop::new((), SinkStage::new(Arc::new(Burst)), ()));
    let below = Source::new(Hop::new((Arc::new(Stamp), ()), Join::new(Engine), ()));
    let endpoint = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay), below.clone()),
        (),
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let out = graph.call(Cx::new(), 3).await.expect("call");
        // A streamed request's items end at their first error.
        let (_, streamed) = Streamed::new((), out).into_parts();
        let copying = items(Copying {
            inner: streamed,
            ends: 0,
        });
        let got: Vec<Big> = copying.map(|item| item.expect("item")).collect().await;
        assert_eq!(got.len(), 3);
        assert!(
            got.iter()
                .all(|item| item.words[0].to_string() == item.text)
        );

        let ask = Ask {
            text: "ask".into(),
            words: [4; 500],
        };
        let answers: Vec<u64> = endpoint
            .call(Cx::new(), ask)
            .await
            .expect("call")
            .map(|item| item.expect("item"))
            .collect()
            .await;
        assert_eq!(answers, [8]);
        let ask = Ask {
            text: "ask".into(),
            words: [4; 500],
        };
        let answers: Vec<u64> = boxed(Cx::new(), ask, below)
            .await
            .expect("call")
            .map(|item| item.expect("item"))
            .collect()
            .await;
        assert_eq!(answers, [8]);
    });
}
