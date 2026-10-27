//! Local composition with gdl alone: a node crate runs its nodes as nested
//! calls in one process, without the runtime or its transport
//! (`tests/dependencies.rs` keeps velo out of this crate's build).

use std::sync::Arc;

use futures::{StreamExt, stream};
use gdl::{
    Backward, Cx, Forward, Hop, Items, Join, Next, Operator, OperatorStage, Result, Sink,
    SinkStage, Source, items,
};

/// Uppercases the request.
struct Upper;

impl Forward for Upper {
    type In = String;
    type Out = String;

    async fn forward(&self, _cx: &Cx, req: String) -> Result<String> {
        Ok(req.to_uppercase())
    }
}

/// Answers with one item per character.
struct Spell;

impl Sink for Spell {
    type Req = String;
    type Item = String;

    async fn generate(&self, _cx: Cx, req: String) -> Result<Items<String>> {
        let letters: Vec<Result<String>> = req.chars().map(|c| Ok(c.to_string())).collect();
        Ok(items(stream::iter(letters)))
    }
}

/// Marks each item.
struct Mark;

impl Backward for Mark {
    type In = String;
    type Out = String;

    fn backward(&self, _cx: &Cx, input: Items<String>) -> Items<String> {
        items(input.map(|item| item.map(|text| format!("{text}!"))))
    }
}

async fn answer<N: Next<String, String>>(graph: &N, req: &str) -> Vec<String> {
    graph
        .generate(Cx::new(), req.to_string())
        .await
        .expect("a stream")
        .map(|item| item.expect("an item"))
        .collect()
        .await
}

#[tokio::test]
async fn a_hop_runs_its_forward_sink_and_backward_nodes() {
    let graph = Source::new(Hop::new(
        (Arc::new(Upper), ()),
        SinkStage::new(Arc::new(Spell)),
        (Arc::new(Mark), ()),
    ));
    assert_eq!(answer(&graph, "ab").await, ["A!", "B!"]);
}

#[tokio::test]
async fn a_join_runs_another_graph_as_its_inner_end() {
    let inner = Source::new(Hop::new(
        (Arc::new(Upper), ()),
        SinkStage::new(Arc::new(Spell)),
        (Arc::new(Mark), ()),
    ));
    let outer = Source::new(Hop::new((), Join::new(inner), (Arc::new(Mark), ())));
    assert_eq!(answer(&outer, "ab").await, ["A!!", "B!!"]);
}

/// Passes the request to `next` and its response back, unchanged.
struct Pass;

impl Operator for Pass {
    type UpReq = String;
    type UpItem = String;
    type DownReq = String;
    type DownItem = String;

    async fn generate<N: Next<String, String>>(
        &self,
        cx: Cx,
        req: String,
        next: N,
    ) -> Result<Items<String>> {
        next.generate(cx, req).await
    }
}

/// Answers as `Spell` does, with a large future: it holds a buffer across
/// an await.
struct Wide;

impl Sink for Wide {
    type Req = String;
    type Item = String;

    async fn generate(&self, cx: Cx, req: String) -> Result<Items<String>> {
        let buffer = [0_u8; 4096];
        tokio::task::yield_now().await;
        std::hint::black_box(&buffer);
        Spell.generate(cx, req).await
    }
}

/// A future is moved whole on every request, so a hop whose future held the
/// futures of every hop below it would cost more the deeper it sits. A hop
/// that nests another hop, through an operator or a join, holds that hop's
/// future on the heap, so its own future does not depend on what is below.
#[tokio::test]
async fn the_future_of_a_hop_does_not_hold_the_hops_below_it() {
    fn below<S: Sink<Req = String, Item = String>>(sink: S) -> Source<impl Next<String, String>> {
        Source::new(Hop::new((), SinkStage::new(Arc::new(sink)), ()))
    }
    fn size<N: Next<String, String>>(graph: &N) -> usize {
        std::mem::size_of_val(&graph.generate(Cx::new(), String::new()))
    }
    let narrow = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Pass), below(Spell)),
        (),
    ));
    let wide = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Pass), below(Wide)),
        (),
    ));
    assert_eq!(size(&narrow), size(&wide), "through an operator");

    let narrow = Source::new(Hop::new((), Join::new(below(Spell)), ()));
    let wide = Source::new(Hop::new((), Join::new(below(Wide)), ()));
    assert_eq!(size(&narrow), size(&wide), "through a join");

    assert_eq!(answer(&wide, "ab").await, ["a", "b"]);
}

/// A source's future is its hop's future. A future of its own around the
/// hop's would be moved whole, with the hop's inside it, each time a caller
/// awaits it.
#[test]
fn a_source_adds_nothing_to_its_hops_future() {
    let hop = Hop::new(
        (Arc::new(Upper), ()),
        SinkStage::new(Arc::new(Spell)),
        (Arc::new(Mark), ()),
    );
    let alone = std::mem::size_of_val(&hop.generate(Cx::new(), String::new()));
    let source = Source::new(hop);
    let called = std::mem::size_of_val(&source.call(Cx::new(), String::new()));
    assert_eq!(called, alone, "Source::call");
    let generated = std::mem::size_of_val(&Next::generate(&source, Cx::new(), String::new()));
    assert_eq!(generated, alone, "Source::generate");
}

/// Waits once, then answers like `Spell`; counts how often it is polled.
struct Once(Arc<std::sync::atomic::AtomicUsize>);

impl Sink for Once {
    type Req = String;
    type Item = String;

    async fn generate(&self, cx: Cx, req: String) -> Result<Items<String>> {
        let polls = Arc::clone(&self.0);
        let mut waited = false;
        std::future::poll_fn(move |task| {
            polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if std::mem::replace(&mut waited, true) {
                std::task::Poll::Ready(())
            } else {
                task.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await;
        Spell.generate(cx, req).await
    }
}

/// An inner end that waits is polled once each time its hop is: polling it
/// twice in one pass would repeat its work on every request that waits.
#[tokio::test]
async fn a_hop_polls_a_waiting_inner_end_once_a_pass() {
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let graph = Source::new(Hop::new(
        (),
        SinkStage::new(Arc::new(Once(Arc::clone(&polls)))),
        (),
    ));
    let mut passes = 0;
    let mut call = std::pin::pin!(graph.generate(Cx::new(), "ab".to_string()));
    let items = std::future::poll_fn(|task| {
        passes += 1;
        call.as_mut().poll(task)
    })
    .await
    .expect("a stream");
    assert_eq!(polls.load(std::sync::atomic::Ordering::Relaxed), passes);
    assert_eq!(items.count().await, 2);
}

/// A request far larger than anything else in a future, so a future's size
/// counts the places it holds the request.
struct Large([u64; 1024]);

/// Answers a large request with one number.
struct Sum;

impl Sink for Sum {
    type Req = Large;
    type Item = u64;

    async fn generate(&self, _cx: Cx, req: Large) -> Result<Items<u64>> {
        Ok(items(stream::iter([Ok(req.0[1])])))
    }
}

/// Passes a large request on.
struct Hand;

impl Operator for Hand {
    type UpReq = Large;
    type UpItem = u64;
    type DownReq = Large;
    type DownItem = u64;

    async fn generate<N: Next<Large, u64>>(
        &self,
        cx: Cx,
        req: Large,
        next: N,
    ) -> Result<Items<u64>> {
        next.generate(cx, req).await
    }
}

/// An inner end returns its node's future, or that future on the heap,
/// and holds no request of its own: a future around the node's would copy
/// the request into itself first, and then into the node's future.
#[test]
fn an_inner_end_adds_no_future_of_its_own() {
    use gdl::Inner;
    let sum = Arc::new(Sum);
    let alone = std::mem::size_of_val(&sum.generate(Cx::new(), Large([0; 1024])));
    let sink = SinkStage::new(Arc::clone(&sum));
    let called = std::mem::size_of_val(&sink.call(Cx::new(), Large([0; 1024])));
    assert_eq!(called, alone, "SinkStage::call");

    let below = Source::new(Hop::new((), SinkStage::new(sum), ()));
    let heap = std::mem::size_of::<std::pin::Pin<Box<()>>>();
    let operator = OperatorStage::new(Arc::new(Hand), below.clone());
    let called = std::mem::size_of_val(&operator.call(Cx::new(), Large([0; 1024])));
    assert_eq!(called, heap, "OperatorStage::call");
    let join = Join::<_, u64>::new(below);
    let called = std::mem::size_of_val(&join.call(Cx::new(), Large([0; 1024])));
    assert_eq!(called, heap, "Join::call");
}

/// A hop's future holds the request in as few places as it moves through:
/// as the hop took it, and as the attempt hands it to the inner end. A sink's
/// own future is the third place; an operator's is on the heap. Each further
/// place is one more copy of the whole request on each call.
#[test]
fn a_hop_holds_its_request_in_few_places() {
    let places = |future_size: usize| future_size / std::mem::size_of::<Large>();
    let over_sink = Hop::new((), SinkStage::new(Arc::new(Sum)), ());
    let size = std::mem::size_of_val(&over_sink.generate(Cx::new(), Large([0; 1024])));
    assert_eq!(places(size), 3, "a hop over a sink");
    let over_operator = Hop::new(
        (),
        OperatorStage::new(Arc::new(Hand), Source::new(over_sink)),
        (),
    );
    let size = std::mem::size_of_val(&over_operator.generate(Cx::new(), Large([0; 1024])));
    assert_eq!(places(size), 2, "a hop over an operator");
}
