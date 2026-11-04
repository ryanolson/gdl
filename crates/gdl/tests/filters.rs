//! A backward node that filters an endless ready source, in a hop below an
//! operator. Its loop never returns to the request's edge unless its input
//! yields: in an attempt the node boundary below it does, and under the
//! request scope the node declares `Backward::FILTERS`, which gives its
//! input the same yield. A node that does not declare it is a known
//! limitation (docs/src/operations/limitations.md).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{StreamExt, stream};
use gdl::{
    Backward, Cx, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage, Source, items,
};

struct Relay<const S: bool>;

impl<const S: bool> Operator for Relay<S> {
    type UpReq = ();
    type UpItem = u64;
    type DownReq = ();
    type DownItem = u64;
    const CALL_SCOPE: bool = S;

    async fn generate<N: Next<(), u64>>(&self, cx: Cx, req: (), next: N) -> Result<Items<u64>> {
        next.generate(cx, req).await
    }
}

struct Endless {
    root: Cx,
    produced: Arc<AtomicUsize>,
}

impl Sink for Endless {
    type Req = ();
    type Item = u64;

    async fn generate(&self, _cx: Cx, (): ()) -> Result<Items<u64>> {
        let root = self.root.clone();
        let produced = Arc::clone(&self.produced);
        Ok(items(stream::repeat_with(move || {
            if produced.fetch_add(1, Ordering::SeqCst) == 100 {
                root.cancel();
                eprintln!("root cancelled at item 101");
            }
            Ok(1)
        })))
    }
}

struct Filter;

impl Backward for Filter {
    type In = u64;
    type Out = u64;
    const FILTERS: bool = true;

    fn backward(&self, _cx: &Cx, input: Items<u64>) -> Items<u64> {
        items(input.filter(|item| std::future::ready(item.is_err())))
    }
}

async fn run<const S: bool>() {
    let root = Cx::new();
    let produced = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(Endless {
        root: root.clone(),
        produced: Arc::clone(&produced),
    });
    let inner = Hop::new((), SinkStage::new(sink), (Arc::new(Filter), ()));
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<S>), inner),
        (),
    ));
    let stream = graph.generate(root, ()).await.expect("stream");
    let out = stream.collect::<Vec<_>>().await;
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].as_ref().expect_err("an error").kind(),
        gdl::ErrorKind::Cancelled
    );
    let produced = produced.load(Ordering::SeqCst);
    assert!(
        produced < 101 + 1_024 + 64,
        "scoped={S}: {produced} items before the request ended"
    );
}

#[tokio::test]
async fn scoped_filter_returns_after_cancel() {
    run::<true>().await;
}

#[tokio::test]
async fn default_filter_returns_after_cancel() {
    run::<false>().await;
}

struct Finite(Arc<AtomicUsize>, Cx);

impl Sink for Finite {
    type Req = ();
    type Item = u64;

    async fn generate(&self, _cx: Cx, (): ()) -> Result<Items<u64>> {
        let produced = Arc::clone(&self.0);
        let root = self.1.clone();
        Ok(items(stream::iter(0..2_000_000_u64).map(move |n| {
            produced.fetch_add(1, Ordering::SeqCst);
            if n == 100 {
                root.cancel();
            }
            Ok(n)
        })))
    }
}

struct Undeclared;

impl Backward for Undeclared {
    type In = u64;
    type Out = u64;

    fn backward(&self, _cx: &Cx, input: Items<u64>) -> Items<u64> {
        items(input.filter(|item| std::future::ready(item.is_err())))
    }
}

/// The limitation: a backward filter that does not declare `FILTERS` reads
/// a ready source until it emits, under the request scope. Over a finite
/// source it reads all of it; over an endless one it would never return.
#[tokio::test]
async fn an_undeclared_backward_filter_reads_its_source_to_the_end() {
    let root = Cx::new();
    let produced = Arc::new(AtomicUsize::new(0));
    let inner = Hop::new(
        (),
        SinkStage::new(Arc::new(Finite(Arc::clone(&produced), root.clone()))),
        (Arc::new(Undeclared), ()),
    );
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<false>), inner),
        (),
    ));
    let out = graph
        .generate(root, ())
        .await
        .expect("stream")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out.len(), 1);
    assert_eq!(produced.load(Ordering::SeqCst), 2_000_000);
}
