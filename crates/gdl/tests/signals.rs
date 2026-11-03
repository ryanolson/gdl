//! How cancellation reaches the contexts below a cancelled one: through a
//! waker that panics, across the yields of a task whose cooperative budget
//! runs out, and across a boundary's yield in a long ready stream.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};

use futures::StreamExt;
use gdl::{
    Cx, ErrorKind, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage, Source,
    items,
};

/// A waker that counts its wakes.
#[derive(Default)]
struct Counted(AtomicUsize);

impl Wake for Counted {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A waker whose first wake panics.
#[derive(Default)]
struct Panics(AtomicBool);

impl Wake for Panics {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.0.swap(true, Ordering::SeqCst) {
            panic!("a waker that panics");
        }
    }
}

/// A waiter at the root whose waker panics must not keep a waiter below
/// the root from its wake: the cancel reaches both, and a task waiting
/// below would otherwise sleep through a cancel it can see.
#[test]
fn a_panicking_root_cancel_waker_still_wakes_a_waiter_below() {
    let root = Cx::new();
    let child = root.child().child();
    let panics = Waker::from(Arc::new(Panics::default()));
    let counted = Arc::new(Counted::default());
    let below = Waker::from(Arc::clone(&counted));

    let mut at_root = Box::pin(root.cancelled());
    let mut at_child = Box::pin(child.cancelled());
    assert!(
        at_root
            .as_mut()
            .poll(&mut Context::from_waker(&panics))
            .is_pending()
    );
    assert!(
        at_child
            .as_mut()
            .poll(&mut Context::from_waker(&below))
            .is_pending()
    );

    let cancelled = catch_unwind(AssertUnwindSafe(|| root.cancel()));
    assert!(cancelled.is_err(), "the root's waker panicked");
    assert_eq!(
        counted.0.load(Ordering::SeqCst),
        1,
        "the waiter below the root was woken"
    );
    assert!(child.is_cancelled());
}

/// Answers with `n` ready items.
struct Burst;

impl Sink for Burst {
    type Req = u64;
    type Item = u64;

    async fn generate(&self, _cx: Cx, n: u64) -> Result<Items<u64>> {
        Ok(items(futures::stream::iter((0..n).map(Ok))))
    }
}

/// A pass-through operator.
struct Pass;

impl Operator for Pass {
    type UpReq = u64;
    type UpItem = u64;
    type DownReq = u64;
    type DownItem = u64;

    async fn generate<N: Next<u64, u64>>(&self, cx: Cx, req: u64, next: N) -> Result<Items<u64>> {
        let below = next.generate(cx, req).await?;
        Ok(items(below.map(|item| item)))
    }
}

fn hop<I>(inner: I) -> Hop<(), I, ()> {
    Hop::new((), inner, ())
}

fn op<H>(next: H) -> OperatorStage<Pass, H> {
    OperatorStage::new(Arc::new(Pass), next)
}

fn sink() -> SinkStage<Burst> {
    SinkStage::new(Arc::new(Burst))
}

/// Cancel `cx` from another thread, and wait until it has.
fn cancel_elsewhere(cx: &Cx) {
    let cx = cx.clone();
    std::thread::spawn(move || cx.cancel())
        .join()
        .expect("the cancelling thread");
}

/// What one request ended with: its items, or the kind of its error.
async fn drained(out: Result<Items<u64>>) -> std::result::Result<u64, ErrorKind> {
    let mut out = out.map_err(|error| error.kind())?;
    let mut count = 0;
    while let Some(item) = out.next().await {
        item.map_err(|error| error.kind())?;
        count += 1;
    }
    Ok(count)
}

/// Five nested operator hops spend six units of the task's budget on each
/// request, so in one task some requests find it spent part way down and
/// yield. A request cancelled during that yield ends cancelled; the others
/// are whole.
#[tokio::test]
async fn nested_ready_hops_still_see_a_cancel_across_budget_yields() {
    let five = op(hop(op(hop(op(hop(op(hop(op(hop(sink()))))))))));
    let chain = Source::new(hop(five));
    let mut yielded = 0;
    for _ in 0..200 {
        let root = Cx::new();
        let mut call = pin!(chain.generate(root.clone(), 1));
        let mut cancelled = false;
        let out = std::future::poll_fn(|task| {
            let polled = call.as_mut().poll(task);
            if polled.is_pending() && !cancelled {
                // The only wait in this chain is a budget yield.
                cancelled = true;
                cancel_elsewhere(&root);
            }
            polled
        })
        .await;
        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), drained(out))
            .await
            .expect("the request ended");
        if cancelled {
            yielded += 1;
            assert_eq!(ended, Err(ErrorKind::Cancelled));
        } else {
            assert_eq!(ended, Ok(1));
        }
    }
    assert!(yielded > 0, "no request yielded for budget");
}

/// A boundary yields after 1,024 ready items in a row, which makes the
/// boundaries above it wait. A cancel after that still ends the stream.
#[tokio::test]
async fn a_ready_stream_crossing_the_yield_interval_still_sees_a_cancel() {
    let chain = Source::new(hop(op(hop(sink()))));
    let whole = drained(chain.generate(Cx::new(), 3_000).await).await;
    assert_eq!(whole, Ok(3_000));

    let root = Cx::new();
    let mut out = chain.generate(root.clone(), 3_000).await.expect("a stream");
    let mut count = 0;
    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(item) = out.next().await {
            if item.is_err() {
                return item.map(|_| ()).map_err(|error| error.kind());
            }
            count += 1;
            if count == 1_500 {
                cancel_elsewhere(&root);
            }
        }
        Ok(())
    })
    .await
    .expect("the stream ended");
    assert_eq!(ended, Err(ErrorKind::Cancelled));
    assert!(count < 3_000, "the cancel ended the stream early");
}

/// As the cancel test above: a waiter registers only for a request whose
/// run waited, here for budget part way down. A request that runs straight
/// through registers nothing on its root.
#[tokio::test]
async fn a_ready_hop_registers_only_on_a_budget_yield() {
    let five = op(hop(op(hop(op(hop(op(hop(op(hop(sink()))))))))));
    let chain = Source::new(hop(five));
    let (mut yielded, mut registered) = (0, 0);
    for _ in 0..200 {
        let root = Cx::new();
        let mut call = pin!(chain.generate(root.clone(), 1));
        let mut waited = false;
        let out = std::future::poll_fn(|task| {
            let polled = call.as_mut().poll(task);
            waited |= polled.is_pending();
            polled
        })
        .await;
        assert_eq!(drained(out).await, Ok(1));
        if waited {
            yielded += 1;
            registered += usize::from(root.waited());
        } else {
            assert!(!root.waited(), "a request that never waited registered");
        }
    }
    assert!(yielded > 0, "no request yielded for budget");
    assert!(registered > 0, "no yield part way down registered a waiter");
}

/// The stop version of the first test. Today's stop wakes through one
/// `Notify` at the root, so this is not a test of the code it replaced.
#[test]
fn a_panicking_root_stop_waker_still_wakes_a_waiter_below() {
    let root = Cx::new();
    let child = root.child().child();
    let panics = Waker::from(Arc::new(Panics::default()));
    let counted = Arc::new(Counted::default());
    let below = Waker::from(Arc::clone(&counted));

    let mut at_root = Box::pin(root.stopping());
    let mut at_child = Box::pin(child.stopping());
    assert!(
        at_root
            .as_mut()
            .poll(&mut Context::from_waker(&panics))
            .is_pending()
    );
    assert!(
        at_child
            .as_mut()
            .poll(&mut Context::from_waker(&below))
            .is_pending()
    );

    let stopped = catch_unwind(AssertUnwindSafe(|| root.request_stop()));
    assert!(stopped.is_err(), "the root's waker panicked");
    assert_eq!(counted.0.load(Ordering::SeqCst), 1);
    assert!(child.is_stopping());
}
