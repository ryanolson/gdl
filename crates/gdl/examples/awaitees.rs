//! An operator whose body future reserves room for large awaitees in front
//! of its captured arguments, for `tests/codegen.rs`, which reads this
//! example's release IR.
//!
//! The body holds two 8 KiB awaitees across two waits, so the compiler
//! keeps them in the part of the future that comes first, before the
//! arguments. The new future leaves that room unwritten. `Direct` returns
//! the body's future as it is, and gdl builds it in its box: only the
//! arguments are written. `mapped` is the control: it builds the same body
//! wrapped in `.map(..)`, as gdl builds an operator's future. The
//! combinator takes the body's future by value, so the whole future, its
//! unwritten room too, is built on the stack and then copied.
//!
//! Run, it sends one request through `Direct` and one through the control.

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use futures::{FutureExt, StreamExt};
use gdl::{Cx, Hop, Items, Join, Next, Operator, OperatorStage, Result, Source, items};

/// A request as large as a few of an endpoint's.
struct Ask {
    text: String,
    words: [u64; 500],
}

/// Answers with one item read from the request.
#[derive(Clone)]
struct Engine;

impl Next<Ask, u64> for Engine {
    async fn generate(&self, _cx: Cx, ask: Ask) -> Result<Items<u64>> {
        let answer = ask.words[2] + ask.words[3] + ask.text.len() as u64;
        Ok(items(futures::stream::iter([Ok(answer)])))
    }
}

/// Holds 8 KiB across a wait, as a large awaitee does.
async fn settle(seed: u64) -> u64 {
    let table = [seed; 1024];
    let mut waited = false;
    std::future::poll_fn(|task| {
        if std::mem::replace(&mut waited, true) {
            Poll::Ready(())
        } else {
            task.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
    std::hint::black_box(&table)[(seed % 1024) as usize]
}

/// The context and `next` the body calls, as one value, as a node's
/// downstream handle holds them.
struct Below<N> {
    cx: Cx,
    next: N,
}

/// The operator's body. Both awaitees are started, then held across a wait
/// and their own awaits: a value held across more than one wait goes in
/// front of the arguments. Two of them make that room larger than either
/// one, so a copy of the room is told apart from the copy of an awaitee
/// into its place when the body starts, which the node's own code makes.
async fn body<N: Next<Ask, u64>>(mut ask: Ask, below: Below<N>) -> Result<Items<u64>> {
    let mut first = std::pin::pin!(settle(ask.words[0]));
    let mut second = std::pin::pin!(settle(ask.words[1]));
    let _ = futures::poll!(first.as_mut());
    let _ = futures::poll!(second.as_mut());
    tokio::task::yield_now().await;
    ask.words[2] = first.await;
    ask.words[3] = second.await;
    below.next.generate(below.cx, ask).await
}

/// Changes the body's output, as a node that maps its error type does.
fn finish(answer: Result<Items<u64>>) -> Result<Items<u64>> {
    answer
}

/// Returns the body's future as it is.
struct Direct;

impl Operator for Direct {
    type UpReq = Ask;
    type UpItem = u64;
    type DownReq = Ask;
    type DownItem = u64;

    fn generate<N: Next<Ask, u64>>(
        &self,
        cx: Cx,
        ask: Ask,
        next: N,
    ) -> impl Future<Output = Result<Items<u64>>> + Send {
        body(ask, Below { cx, next })
    }
}

/// The control: the body wrapped in `.map(..)`, built as gdl builds an
/// operator's future (`gdl::__runtime::boxed`), with a `build` of its own
/// so that its copy is told apart from gdl's.
#[inline(never)]
fn mapped<N: Next<Ask, u64>>(
    cx: Cx,
    ask: Ask,
    next: N,
) -> Pin<Box<impl Future<Output = Result<Items<u64>>> + Send>> {
    let slot = Box::new_uninit();
    Box::into_pin(Box::write(
        slot,
        build(|| body(ask, Below { cx, next }).map(finish)),
    ))
}

/// As gdl's `build`: out of line, so that the box is its return place.
#[inline(never)]
fn build<F>(make: impl FnOnce() -> F) -> F {
    make()
}

fn main() {
    let below = Source::new(Hop::new((), Join::new(Engine), ()));
    let direct = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Direct), below.clone()),
        (),
    ));
    let ask = || Ask {
        text: "ask".into(),
        words: [4; 500],
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let answers: Vec<u64> = direct
            .call(Cx::new(), ask())
            .await
            .expect("call")
            .map(|item| item.expect("item"))
            .collect()
            .await;
        assert_eq!(answers, [11]);
        let answers: Vec<u64> = mapped(Cx::new(), ask(), below)
            .await
            .expect("call")
            .map(|item| item.expect("item"))
            .collect()
            .await;
        assert_eq!(answers, [11]);
    });
}
