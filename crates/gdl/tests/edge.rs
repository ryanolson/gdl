//! What the request's edge enforces for the scope it hands the stage below
//! it, the request scope that every call under it shares unless the call
//! has a scope of its own; and what such a call gives its operator when the
//! request ends. Each case runs both forms where they differ.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Poll;

use futures::{StreamExt, stream};
use gdl::{
    Backward, Cx, ErrorKind, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage,
    Source, items,
};

struct Relay<const SCOPED: bool>(Arc<OnceLock<Cx>>);

impl<const SCOPED: bool> Operator for Relay<SCOPED> {
    type UpReq = ();
    type UpItem = u32;
    type DownReq = ();
    type DownItem = u32;
    const CALL_SCOPE: bool = SCOPED;

    async fn generate<N: Next<(), u32>>(&self, cx: Cx, req: (), next: N) -> Result<Items<u32>> {
        self.0.set(cx.clone()).expect("one call");
        next.generate(cx, req).await
    }
}

struct Stall(Arc<AtomicUsize>);

struct Live(Arc<AtomicUsize>);

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Sink for Stall {
    type Req = ();
    type Item = u32;

    async fn generate(&self, _cx: Cx, (): ()) -> Result<Items<u32>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let _live = Live(Arc::clone(&self.0));
        std::future::pending().await
    }
}

async fn own_cancel<const SCOPED: bool>() -> bool {
    let own = Arc::new(OnceLock::new());
    let live = Arc::new(AtomicUsize::new(0));
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(Relay::<SCOPED>(Arc::clone(&own))),
            Hop::new((), SinkStage::new(Arc::new(Stall(Arc::clone(&live)))), ()),
        ),
        (),
    ));
    let mut call = Box::pin(graph.generate(Cx::new(), ()));
    assert!(futures::poll!(&mut call).is_pending());
    assert_eq!(live.load(Ordering::SeqCst), 1);
    own.get().expect("operator started").cancel();
    let ended = match futures::poll!(&mut call) {
        Poll::Ready(Err(error)) => {
            assert_eq!(error.kind(), ErrorKind::Cancelled);
            true
        }
        Poll::Pending => false,
        Poll::Ready(Ok(_)) => panic!("the sink never returns"),
    };
    println!(
        "scoped={SCOPED}: ended={ended}, live={}",
        live.load(Ordering::SeqCst)
    );
    drop(call);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    ended
}

/// A cancel of the operator's context ends a pending call below it, which
/// ignores its context, in both forms: the scoped call by its own race, the
/// default one by the edge's, which watches the scope it handed below.
#[tokio::test]
async fn a_scoped_call_ends_when_the_operator_cancels_its_context() {
    assert!(own_cancel::<true>().await);
}

#[tokio::test]
async fn request_scope_cancellation_ends_a_pending_nested_call() {
    assert!(
        own_cancel::<false>().await,
        "the edge watches the parent of the cancelled request scope"
    );
}

struct Ready;

impl Sink for Ready {
    type Req = ();
    type Item = u32;

    async fn generate(&self, _cx: Cx, (): ()) -> Result<Items<u32>> {
        Ok(items(stream::iter([Ok(1)])))
    }
}

struct Cancel(Cx);

impl Backward for Cancel {
    type In = u32;
    type Out = u32;

    fn backward(&self, _cx: &Cx, input: Items<u32>) -> Items<u32> {
        self.0.cancel();
        input
    }
}

struct SideEffect(Arc<AtomicUsize>);

impl Backward for SideEffect {
    type In = u32;
    type Out = u32;

    fn backward(&self, _cx: &Cx, input: Items<u32>) -> Items<u32> {
        self.0.fetch_add(1, Ordering::SeqCst);
        input
    }
}

/// No backward node is built for a request that is over, under the request
/// scope as in an attempt: the first cancels the request as it is built, and
/// the second is never built.
#[tokio::test]
async fn later_backward_nodes_do_not_start_after_request_cancel() {
    let count = Arc::new(AtomicUsize::new(0));
    let root = Cx::new();
    let hop = Hop::new(
        (),
        SinkStage::new(Arc::new(Ready)),
        (
            Arc::new(Cancel(root.clone())),
            (Arc::new(SideEffect(Arc::clone(&count))), ()),
        ),
    );
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<false>(Arc::default())), hop),
        (),
    ));
    let out = match graph.generate(root, ()).await {
        Ok(stream) => stream.collect::<Vec<_>>().await,
        Err(error) => vec![Err(error)],
    };
    assert_eq!(
        out.last()
            .expect("an item")
            .as_ref()
            .expect_err("an error")
            .kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "a cancelled request starts the later backward node"
    );
}

struct Retry;

impl Operator for Retry {
    type UpReq = ();
    type UpItem = u32;
    type DownReq = ();
    type DownItem = u32;

    async fn generate<N: Next<(), u32>>(&self, cx: Cx, (): (), next: N) -> Result<Items<u32>> {
        let mut first = next.generate(cx.clone(), ()).await?;
        Ok(items(
            stream::once(async move {
                assert!(first.next().await.expect("an item").is_err());
                match next.generate(cx, ()).await {
                    Ok(mut replacement) => replacement.next().await,
                    Err(_) => Some(Ok(99)),
                }
            })
            .filter_map(std::future::ready),
        ))
    }
}

struct RetryEnd {
    root: Cx,
    calls: AtomicUsize,
}

impl Sink for RetryEnd {
    type Req = ();
    type Item = u32;

    async fn generate(&self, _cx: Cx, (): ()) -> Result<Items<u32>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(items(stream::iter([Err(gdl::Error::node(
                "worker went away",
            ))])));
        }
        self.root.cancelled().await;
        Ok(items(stream::empty()))
    }
}

struct Keep;

impl Backward for Keep {
    type In = u32;
    type Out = u32;

    fn backward(&self, _cx: &Cx, input: Items<u32>) -> Items<u32> {
        input
    }
}

async fn kill_retry<N: Next<(), u32>>(graph: N, root: Cx, sink: Arc<RetryEnd>) -> Vec<Result<u32>> {
    let mut out = graph.generate(root.clone(), ()).await.expect("a stream");
    assert!(futures::poll!(out.next()).is_pending());
    assert_eq!(sink.calls.load(Ordering::SeqCst), 2, "retry is dispatching");
    root.cancel();
    out.collect().await
}

/// A retry that is running when its request is cancelled ends quietly under
/// the request scope, so the operator does not send the failure that caused
/// it; the edge alone reports `Cancelled`. With a backward node in the hop
/// below, and without one.
#[tokio::test]
async fn a_running_retry_with_a_backward_node_does_not_emit_its_stale_error() {
    let root = Cx::new();
    let sink = Arc::new(RetryEnd {
        root: root.clone(),
        calls: AtomicUsize::new(0),
    });
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(Retry),
            Hop::new((), SinkStage::new(Arc::clone(&sink)), (Arc::new(Keep), ())),
        ),
        (),
    ));
    let out = kill_retry(graph, root, sink).await;
    println!("retry with backward node: {out:?}");
    assert!(
        !out.iter().any(|item| matches!(item, Ok(99))),
        "the running retry returned Cancelled to its operator"
    );
    assert_eq!(kinds(&out), [Err(ErrorKind::Cancelled)]);
}

fn kinds(out: &[Result<u32>]) -> Vec<std::result::Result<u32, ErrorKind>> {
    out.iter()
        .map(|item| item.as_ref().map(|n| *n).map_err(|e| e.kind()))
        .collect()
}

#[tokio::test]
async fn a_running_retry_without_a_backward_node_ends_quietly() {
    let root = Cx::new();
    let sink = Arc::new(RetryEnd {
        root: root.clone(),
        calls: AtomicUsize::new(0),
    });
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(Retry),
            Hop::new((), SinkStage::new(Arc::clone(&sink)), ()),
        ),
        (),
    ));
    let out = kill_retry(graph, root, sink).await;
    println!("retry without backward node: {out:?}");
    assert!(!out.iter().any(|item| matches!(item, Ok(99))));
    assert_eq!(kinds(&out), [Err(ErrorKind::Cancelled)]);
}

struct Carry;

impl gdl::Forward for Carry {
    type In = ();
    type Out = Cx;

    async fn forward(&self, cx: &Cx, (): ()) -> Result<Cx> {
        Ok(cx.clone())
    }
}

struct CleanupEnd;

struct Cleanup(Cx);

impl futures::Stream for Cleanup {
    type Item = Result<u32>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl Sink for CleanupEnd {
    type Req = Cx;
    type Item = u32;

    async fn generate(&self, _cx: Cx, attempt: Cx) -> Result<Items<u32>> {
        Ok(items(Cleanup(attempt)))
    }
}

/// A clean end after a cleanup cancel, nested: a producer below a pass-through
/// call whose cleanup cancels the request scope it was given. The edge
/// watches that scope, so it reports `Cancelled`, not a clean end
/// (`boundaries.rs`, `a_clean_end_reports_cancellation_from_producer_cleanup`,
/// for one hop).
#[tokio::test]
async fn the_edge_reports_cancellation_from_a_nested_producers_cleanup() {
    let hop = Hop::new(
        (Arc::new(Carry), ()),
        SinkStage::new(Arc::new(CleanupEnd)),
        (),
    );
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<false>(Arc::default())), hop),
        (),
    ));
    let out = graph
        .generate(Cx::new(), ())
        .await
        .expect("stream")
        .collect::<Vec<_>>()
        .await;
    println!("cleanup cancellation: {out:?}");
    assert_eq!(out.len(), 1, "the request edge reports a clean success");
    assert_eq!(
        out[0].as_ref().expect_err("an error").kind(),
        ErrorKind::Cancelled
    );
}

struct WaitStream;

impl Sink for WaitStream {
    type Req = ();
    type Item = u32;

    async fn generate(&self, _cx: Cx, (): ()) -> Result<Items<u32>> {
        Ok(items(stream::iter([Ok(1)]).chain(stream::pending())))
    }
}

/// While the request streams, the edge watches its attempt, not the scope
/// it hands below: a node's cancel of that scope is cooperative, and the
/// stream waits on. The client's cancel is enforced: the edge ends at once.
#[tokio::test]
async fn while_streaming_a_nodes_cancel_is_cooperative_and_the_clients_is_enforced() {
    let own = Arc::new(OnceLock::new());
    let root = Cx::new();
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(Relay::<false>(Arc::clone(&own))),
            Hop::new((), SinkStage::new(Arc::new(WaitStream)), ()),
        ),
        (),
    ));
    let mut out = graph.generate(root.clone(), ()).await.expect("stream");
    assert_eq!(kinds(&[out.next().await.expect("an item")]), [Ok(1)]);
    own.get().expect("operator context").cancel();
    assert!(
        futures::poll!(out.next()).is_pending(),
        "a node's cancel is cooperative while the request streams"
    );
    root.cancel();
    let last = out.next().await.expect("the edge's ending");
    assert_eq!(kinds(&[last]), [Err(ErrorKind::Cancelled)]);
    assert!(out.next().await.is_none());
}

struct SelfCancel<const WAIT: bool>;

impl<const WAIT: bool> Sink for SelfCancel<WAIT> {
    type Req = ();
    type Item = u32;

    async fn generate(&self, cx: Cx, (): ()) -> Result<Items<u32>> {
        cx.cancel();
        if WAIT {
            tokio::task::yield_now().await;
        }
        Ok(items(stream::iter([Ok(1)])))
    }
}

/// The price of racing the scope a hop hands below: an inner end that
/// cancels its own context and returns at once delivers its ready items,
/// then `Cancelled`; one that waits once after the cancel is dropped by the
/// race, with `Cancelled` and no items.
#[tokio::test]
async fn one_wait_after_a_self_cancel_discards_the_ready_items() {
    let ready = Source::new(Hop::new(
        (),
        SinkStage::new(Arc::new(SelfCancel::<false>)),
        (),
    ));
    let out = ready
        .generate(Cx::new(), ())
        .await
        .expect("a ready dispatch returns")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(kinds(&out), [Ok(1), Err(ErrorKind::Cancelled)]);
    let waiting = Source::new(Hop::new(
        (),
        SinkStage::new(Arc::new(SelfCancel::<true>)),
        (),
    ));
    let result = waiting.generate(Cx::new(), ()).await;
    assert!(matches!(result, Err(ref e) if e.kind() == ErrorKind::Cancelled));
}
