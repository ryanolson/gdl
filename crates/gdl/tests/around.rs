//! An `Around` node in a local hop: the value its forward half keeps
//! reaches the backward half of the same attempt, and no other, and it is
//! dropped exactly once on every path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::Duration;

use futures::{StreamExt, stream};
use gdl::{
    Around, Backward, Cx, ErrorKind, Forward, Hop, Items, Next, Operator, OperatorStage, Result,
    Sink, SinkStage, Source, around, items,
};
use tokio::sync::Notify;

/// What happened to the states one node kept.
#[derive(Default)]
struct Counts {
    created: AtomicU64,
    received: AtomicU64,
    dropped: AtomicU64,
}

impl Counts {
    fn get(&self) -> (u64, u64, u64) {
        (
            self.created.load(SeqCst),
            self.received.load(SeqCst),
            self.dropped.load(SeqCst),
        )
    }
}

/// A kept value that counts its creation and its drop.
struct Counted {
    value: u64,
    counts: Arc<Counts>,
}

impl Counted {
    fn new(value: u64, counts: &Arc<Counts>) -> Self {
        counts.created.fetch_add(1, SeqCst);
        Self {
            value,
            counts: Arc::clone(counts),
        }
    }
}

impl Counted {
    /// Read through a method, so that a closure that reads it captures the
    /// whole value, not only the field (which would drop the rest early).
    fn value(&self) -> u64 {
        self.value
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.counts.dropped.fetch_add(1, SeqCst);
    }
}

/// Where a `Tag` panics, if anywhere.
#[derive(Clone, Copy, PartialEq)]
enum Panics {
    Never,
    InForward,
    InBackward,
    WhilePolled,
}

/// An around node. Its forward half keeps `K * 1000` plus the count of
/// requests it has seen; its backward half appends that value to each item.
/// `K` gives each nesting level its own node type.
struct Tag<const K: u64> {
    counts: Arc<Counts>,
    seen: AtomicU64,
    panics: Panics,
    stop: bool,
}

impl<const K: u64> Tag<K> {
    fn new(counts: &Arc<Counts>) -> Arc<Self> {
        Self::with(counts, Panics::Never, false)
    }

    fn with(counts: &Arc<Counts>, panics: Panics, stop: bool) -> Arc<Self> {
        Arc::new(Self {
            counts: Arc::clone(counts),
            seen: AtomicU64::new(0),
            panics,
            stop,
        })
    }
}

impl<const K: u64> Around for Tag<K> {
    type UpReq = u64;
    type UpItem = Vec<u64>;
    type DownReq = u64;
    type DownItem = Vec<u64>;
    type State = Counted;

    async fn forward(&self, _cx: &Cx, req: u64) -> Result<(u64, Counted)> {
        assert!(self.panics != Panics::InForward, "a panic in forward");
        let value = K * 1000 + self.seen.fetch_add(1, SeqCst);
        Ok((req, Counted::new(value, &self.counts)))
    }

    fn backward(&self, cx: &Cx, state: Counted, input: Items<Vec<u64>>) -> Items<Vec<u64>> {
        state.counts.received.fetch_add(1, SeqCst);
        assert!(self.panics != Panics::InBackward, "a panic in backward");
        if self.stop {
            cx.request_stop();
        }
        let panics = self.panics;
        items(input.map(move |item| {
            assert!(panics != Panics::WhilePolled, "a panic while polled");
            item.map(|mut trail| {
                trail.push(state.value());
                trail
            })
        }))
    }
}

/// Answers with `n` items, each `[n]`, until its context stops.
struct Count {
    stopped: Arc<AtomicU64>,
}

impl Count {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            stopped: Arc::default(),
        })
    }
}

impl Sink for Count {
    type Req = u64;
    type Item = Vec<u64>;

    async fn generate(&self, cx: Cx, n: u64) -> Result<Items<Vec<u64>>> {
        let stopped = Arc::clone(&self.stopped);
        let running = move |_: &u64| {
            let stopping = cx.is_stopping();
            if stopping {
                stopped.fetch_add(1, SeqCst);
            }
            futures::future::ready(!stopping)
        };
        Ok(items(
            stream::iter(0..n)
                .take_while(running)
                .map(move |_| Ok(vec![n])),
        ))
    }
}

/// Signals that it was called, then never answers.
struct Pending(Arc<Notify>);

impl Sink for Pending {
    type Req = u64;
    type Item = Vec<u64>;

    async fn generate(&self, _cx: Cx, _n: u64) -> Result<Items<Vec<u64>>> {
        self.0.notify_one();
        std::future::pending().await
    }
}

/// Fails every call.
struct Fails;

impl Sink for Fails {
    type Req = u64;
    type Item = Vec<u64>;

    async fn generate(&self, _cx: Cx, _n: u64) -> Result<Items<Vec<u64>>> {
        Err(gdl::Error::new(ErrorKind::Node, "the sink failed"))
    }
}

/// A forward node that fails every request.
struct Refuse;

impl Forward for Refuse {
    type In = u64;
    type Out = u64;

    async fn forward(&self, _cx: &Cx, _req: u64) -> Result<u64> {
        Err(gdl::Error::new(ErrorKind::Node, "refused"))
    }
}

/// A backward node that cancels `root` as it builds its stream: the
/// attempt is interrupted after the inner end returned.
struct CancelRoot(Cx);

impl Backward for CancelRoot {
    type In = Vec<u64>;
    type Out = Vec<u64>;

    fn backward(&self, _cx: &Cx, input: Items<Vec<u64>>) -> Items<Vec<u64>> {
        self.0.cancel();
        input
    }
}

/// A backward node that passes the stream on.
struct Pass;

impl Backward for Pass {
    type In = Vec<u64>;
    type Out = Vec<u64>;

    fn backward(&self, _cx: &Cx, input: Items<Vec<u64>>) -> Items<Vec<u64>> {
        input
    }
}

/// A hop whose only stages are one around node's halves.
type Wrapped<P, I> = Hop<(gdl::ForwardHalf<P>, ()), I, (gdl::BackwardHalf<P>, ())>;

/// A hop whose only stages are `tag`'s halves, over `inner`.
fn wrapped<const K: u64, I>(tag: Arc<Tag<K>>, inner: I) -> Wrapped<Tag<K>, I> {
    let (forward, backward) = around(tag);
    Hop::new((forward, ()), inner, (backward, ()))
}

async fn trails<N: Next<u64, Vec<u64>>>(next: &N, cx: Cx, n: u64) -> Vec<Vec<u64>> {
    let stream = next.generate(cx, n).await.expect("a stream");
    stream.map(|item| item.expect("an item")).collect().await
}

/// An operator that sends each request through `next` twice, one call
/// after the other, and answers with both streams in order.
struct Twice;

impl Operator for Twice {
    type UpReq = u64;
    type UpItem = Vec<u64>;
    type DownReq = u64;
    type DownItem = Vec<u64>;

    async fn generate<N: Next<u64, Vec<u64>>>(
        &self,
        cx: Cx,
        n: u64,
        next: N,
    ) -> Result<Items<Vec<u64>>> {
        let first: Vec<_> = next.generate(cx.clone(), n).await?.collect().await;
        let second: Vec<_> = next.generate(cx, n).await?.collect().await;
        Ok(items(stream::iter(first.into_iter().chain(second))))
    }
}

#[tokio::test]
async fn each_attempt_of_a_retry_gets_its_own_state() {
    let counts = Arc::default();
    let below = Source::new(wrapped(
        Tag::<1>::new(&counts),
        SinkStage::new(Count::new()),
    ));
    let graph = Source::new(Hop::new((), OperatorStage::new(Arc::new(Twice), below), ()));
    let got = trails(&graph, Cx::new(), 2).await;
    assert_eq!(
        got,
        [vec![2, 1000], vec![2, 1000], vec![2, 1001], vec![2, 1001]]
    );
    assert_eq!(counts.get(), (2, 2, 2));
}

/// Two attempts at once, their streams read in turn: neither sees the
/// other's state.
#[tokio::test]
async fn attempts_at_once_keep_their_own_states() {
    let counts = Arc::default();
    let hop = Source::new(wrapped(
        Tag::<1>::new(&counts),
        SinkStage::new(Count::new()),
    ));
    let cx = Cx::new();
    let (first, second) = futures::join!(hop.generate(cx.clone(), 3), hop.generate(cx, 3));
    let (first, second) = (first.expect("a stream"), second.expect("a stream"));
    assert_eq!(counts.get(), (2, 2, 0));
    let pairs: Vec<_> = first.zip(second).collect().await;
    let values: Vec<_> = pairs
        .into_iter()
        .map(|(a, b)| (a.expect("an item")[1], b.expect("an item")[1]))
        .collect();
    assert_eq!(values.len(), 3);
    let (a, b) = values[0];
    assert_ne!(a, b, "each attempt kept its own value");
    assert!(values.iter().all(|&pair| pair == (a, b)));
    assert_eq!(counts.get(), (2, 2, 2));
}

/// An operator drops its first stream unread, then calls `next` again:
/// the first state is gone before the second attempt starts.
#[tokio::test]
async fn a_recall_after_a_dropped_stream_starts_fresh() {
    let counts: Arc<Counts> = Arc::default();
    let hop = Source::new(wrapped(
        Tag::<1>::new(&counts),
        SinkStage::new(Count::new()),
    ));
    let cx = Cx::new();
    let first = hop.generate(cx.clone(), 1).await.expect("a stream");
    assert_eq!(counts.get(), (1, 1, 0));
    drop(first);
    assert_eq!(counts.get(), (1, 1, 1), "dropped before the second attempt");
    assert_eq!(trails(&hop, cx, 1).await, [vec![1, 1001]]);
    assert_eq!(counts.get(), (2, 2, 2));
}

/// A panic in an inner pair's forward half: the outer pair's state, kept
/// before it, drops once in the unwind, and the inner pair keeps none.
#[tokio::test]
async fn a_panic_in_forward_drops_the_states_kept_before_it() {
    let (outer, inner): (Arc<Counts>, Arc<Counts>) = (Arc::default(), Arc::default());
    let (outer_forward, outer_backward) = around(Tag::<1>::new(&outer));
    let (inner_forward, inner_backward) = around(Tag::<2>::with(&inner, Panics::InForward, false));
    let hop = Source::new(Hop::new(
        (outer_forward, (inner_forward, ())),
        SinkStage::new(Count::new()),
        (inner_backward, (outer_backward, ())),
    ));
    let joined = tokio::spawn(async move { hop.generate(Cx::new(), 1).await.map(drop) }).await;
    assert!(joined.expect_err("a panic").is_panic());
    assert_eq!(outer.get(), (1, 0, 1));
    assert_eq!(inner.get(), (0, 0, 0));
}

#[tokio::test]
async fn a_panic_in_backward_drops_its_state_once() {
    let counts: Arc<Counts> = Arc::default();
    let hop = Source::new(wrapped(
        Tag::<1>::with(&counts, Panics::InBackward, false),
        SinkStage::new(Count::new()),
    ));
    let joined = tokio::spawn(async move { hop.generate(Cx::new(), 1).await.map(drop) }).await;
    assert!(joined.expect_err("a panic").is_panic());
    assert_eq!(counts.get(), (1, 1, 1));
}

#[tokio::test]
async fn a_panic_while_the_backward_stream_is_polled_drops_its_state_once() {
    let counts: Arc<Counts> = Arc::default();
    let hop = Source::new(wrapped(
        Tag::<1>::with(&counts, Panics::WhilePolled, false),
        SinkStage::new(Count::new()),
    ));
    let joined = tokio::spawn(async move {
        let stream = hop.generate(Cx::new(), 1).await.expect("a stream");
        stream.collect::<Vec<_>>().await
    })
    .await;
    assert!(joined.expect_err("a panic").is_panic());
    assert_eq!(counts.get(), (1, 1, 1));
}

#[tokio::test]
async fn an_error_from_the_inner_end_drops_the_state_unreceived() {
    let counts: Arc<Counts> = Arc::default();
    let hop = Source::new(wrapped(
        Tag::<1>::new(&counts),
        SinkStage::new(Arc::new(Fails)),
    ));
    let error = hop
        .generate(Cx::new(), 1)
        .await
        .map(drop)
        .expect_err("the sink failed");
    assert_eq!(error.kind(), ErrorKind::Node);
    assert_eq!(counts.get(), (1, 0, 1));
}

#[tokio::test]
async fn an_error_from_a_later_forward_node_drops_the_state_unreceived() {
    let counts: Arc<Counts> = Arc::default();
    let (forward, backward) = around(Tag::<1>::new(&counts));
    let hop = Source::new(Hop::new(
        (forward, (Arc::new(Refuse), ())),
        SinkStage::new(Count::new()),
        (backward, ()),
    ));
    let error = hop
        .generate(Cx::new(), 1)
        .await
        .map(drop)
        .expect_err("refused");
    assert_eq!(error.kind(), ErrorKind::Node);
    assert_eq!(counts.get(), (1, 0, 1));
}

#[tokio::test]
async fn a_cancel_while_the_inner_end_waits_drops_the_state_unreceived() {
    let counts: Arc<Counts> = Arc::default();
    let called = Arc::new(Notify::new());
    let hop = Source::new(wrapped(
        Tag::<1>::new(&counts),
        SinkStage::new(Arc::new(Pending(Arc::clone(&called)))),
    ));
    let cx = Cx::new();
    let call = tokio::spawn({
        let cx = cx.clone();
        async move { hop.generate(cx, 1).await.map(drop) }
    });
    called.notified().await;
    assert_eq!(counts.get(), (1, 0, 0));
    cx.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect("the call ends")
        .expect("no panic");
    assert_eq!(result.expect_err("cancelled").kind(), ErrorKind::Cancelled);
    assert_eq!(counts.get(), (1, 0, 1));
}

#[tokio::test(start_paused = true)]
async fn a_deadline_while_the_inner_end_waits_drops_the_state_unreceived() {
    let counts: Arc<Counts> = Arc::default();
    let hop = Source::new(wrapped(
        Tag::<1>::new(&counts),
        SinkStage::new(Arc::new(Pending(Arc::new(Notify::new())))),
    ));
    let cx = Cx::new().with_timeout(Duration::from_millis(50));
    let error = hop
        .generate(cx, 1)
        .await
        .map(drop)
        .expect_err("past the deadline");
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded);
    assert_eq!(counts.get(), (1, 0, 1));
}

/// A backward node before the half cancels the request as it builds its
/// stream: the half is skipped, never called, and its state still drops
/// once.
#[tokio::test]
async fn a_half_skipped_by_an_interrupted_attempt_drops_its_state_unreceived() {
    let counts: Arc<Counts> = Arc::default();
    let cx = Cx::new();
    let (forward, backward) = around(Tag::<1>::new(&counts));
    let hop = Source::new(Hop::new(
        (forward, ()),
        SinkStage::new(Count::new()),
        (Arc::new(CancelRoot(cx.clone())), (backward, ())),
    ));
    let stream = hop.generate(cx.clone(), 1).await;
    assert_eq!(counts.get(), (1, 0, 1));
    if let Ok(stream) = stream {
        let ended: Vec<_> = stream.collect().await;
        assert!(ended.iter().all(|item| item.is_err()));
    }
    assert_eq!(counts.get(), (1, 0, 1));
}

#[tokio::test]
async fn a_stream_dropped_before_its_first_poll_drops_the_state() {
    let counts: Arc<Counts> = Arc::default();
    let hop = Source::new(wrapped(
        Tag::<1>::new(&counts),
        SinkStage::new(Count::new()),
    ));
    let stream = hop.generate(Cx::new(), 3).await.expect("a stream");
    assert_eq!(counts.get(), (1, 1, 0));
    drop(stream);
    assert_eq!(counts.get(), (1, 1, 1));
}

/// At a clean end the boundary drops the finished stream, and the state
/// with it, while the consumer still holds the response.
#[tokio::test]
async fn a_clean_end_drops_the_state_with_the_finished_stream() {
    let counts: Arc<Counts> = Arc::default();
    let hop = Source::new(wrapped(
        Tag::<1>::new(&counts),
        SinkStage::new(Count::new()),
    ));
    let mut stream = hop.generate(Cx::new(), 2).await.expect("a stream");
    while let Some(item) = stream.next().await {
        assert_eq!(item.expect("an item"), [2, 1000]);
    }
    assert_eq!(
        counts.get(),
        (1, 1, 1),
        "dropped at the end, not by the consumer"
    );
    drop(stream);
    assert_eq!(counts.get(), (1, 1, 1));
}

/// Nested pairs of two node types: each backward half gets its own
/// forward half's value, the inner one first.
#[tokio::test]
async fn nested_pairs_each_get_their_own_value() {
    let (outer, inner): (Arc<Counts>, Arc<Counts>) = (Arc::default(), Arc::default());
    let (outer_forward, outer_backward) = around(Tag::<1>::new(&outer));
    let (inner_forward, inner_backward) = around(Tag::<2>::new(&inner));
    let hop = Source::new(Hop::new(
        (outer_forward, (inner_forward, ())),
        SinkStage::new(Count::new()),
        (inner_backward, (outer_backward, ())),
    ));
    assert_eq!(trails(&hop, Cx::new(), 1).await, [vec![1, 2000, 1000]]);
    assert_eq!(outer.get(), (1, 1, 1));
    assert_eq!(inner.get(), (1, 1, 1));
}

/// The last backward half of a local hop gets the attempt's own context, as
/// a last backward node does: its stop reaches the inner end.
#[tokio::test]
async fn the_last_backward_half_stops_the_attempt_and_its_inner_end() {
    let counts = Arc::default();
    let sink = Count::new();
    let hop = Source::new(wrapped(
        Tag::<1>::with(&counts, Panics::Never, true),
        SinkStage::new(Arc::clone(&sink)),
    ));
    let got = trails(&hop, Cx::new(), 1000).await;
    assert!(got.is_empty(), "the inner end stopped at once");
    assert_eq!(sink.stopped.load(SeqCst), 1);
}

/// The control: a backward half that is not last gets a child scope, and
/// its stop does not reach the inner end.
#[tokio::test]
async fn a_backward_half_that_is_not_last_stops_only_its_own_scope() {
    let counts = Arc::default();
    let sink = Count::new();
    let (forward, backward) = around(Tag::<1>::with(&counts, Panics::Never, true));
    let hop = Source::new(Hop::new(
        (forward, ()),
        SinkStage::new(Arc::clone(&sink)),
        (backward, (Arc::new(Pass), ())),
    ));
    let got = trails(&hop, Cx::new(), 1000).await;
    assert_eq!(got.len(), 1000, "the inner end ran to its end");
    assert_eq!(sink.stopped.load(SeqCst), 0);
}

/// A hand-built pair works under an operator stage too, and the operator's
/// own attempt makes one state.
#[tokio::test]
async fn a_pair_around_an_operator_keeps_one_state_per_attempt() {
    let counts = Arc::default();
    let below = Source::new(Hop::new((), SinkStage::new(Count::new()), ()));
    let hop = Source::new(wrapped(
        Tag::<1>::new(&counts),
        OperatorStage::new(Arc::new(Twice), below),
    ));
    assert_eq!(
        trails(&hop, Cx::new(), 1).await,
        [vec![1, 1000], vec![1, 1000]]
    );
    assert_eq!(counts.get(), (1, 1, 1));
}
