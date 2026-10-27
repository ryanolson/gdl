//! A local hop over items of 2 KiB, for `tests/codegen.rs`, which reads this
//! example's release IR: a node boundary hands each item on without a copy.
//! `Copying`, a boundary that copies, is the control. Run, it sends one
//! stream through each and checks it.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use gdl::{Cx, Hop, Items, Result, Sink, SinkStage, Source, items};

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

fn main() {
    let graph = Source::new(Hop::new((), SinkStage::new(Arc::new(Burst)), ()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let out = graph.call(Cx::new(), 3).await.expect("call");
        let copying = items(Copying {
            inner: out,
            ends: 0,
        });
        let got: Vec<Big> = copying.map(|item| item.expect("item")).collect().await;
        assert_eq!(got.len(), 3);
        assert!(
            got.iter()
                .all(|item| item.words[0].to_string() == item.text)
        );
    });
}
