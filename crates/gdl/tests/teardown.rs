//! Teardown in every form of call: for each shape of graph, each phase a
//! request can be in, and each way it can end, everything the request built
//! is dropped, and the caller sees why. A shape's graph is built once and
//! serves all its cases. A case waits only on what it observes: the sink's
//! count of live futures and streams.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures::{StreamExt, stream};
use gdl::{
    Backward, Cx, ErrorKind, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage,
    Source, items,
};

/// Where the request is when it ends.
#[derive(Clone, Copy, Debug)]
enum Phase {
    /// The sink's `generate` waits for ever.
    Dispatch,
    /// The sink's stream has sent one item and waits for ever.
    Waiting,
    /// The sink's stream is ready for ever.
    Ready,
}

/// How the request ends.
#[derive(Clone, Copy, Debug)]
enum End {
    /// The caller cancels the root.
    Cancel,
    /// The caller drops the call or the stream.
    Drop,
    /// The request's deadline passes.
    Deadline,
}

struct Req {
    phase: Phase,
    live: Arc<AtomicUsize>,
}

/// Counts itself live until it is dropped.
struct Live(Arc<AtomicUsize>);

impl Live {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(count))
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Ignores its context: only a drop ends what it builds.
struct End0;

impl Sink for End0 {
    type Req = Req;
    type Item = u32;

    async fn generate(&self, _cx: Cx, req: Req) -> Result<Items<u32>> {
        let live = Live::new(&req.live);
        match req.phase {
            Phase::Dispatch => {
                std::future::pending::<()>().await;
                unreachable!()
            }
            Phase::Waiting => {
                let mut sent = false;
                Ok(items(stream::poll_fn(move |_| {
                    let _ = &live;
                    if std::mem::replace(&mut sent, true) {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(Ok(1)))
                    }
                })))
            }
            Phase::Ready => Ok(items(stream::repeat_with(move || {
                let _ = &live;
                Ok(1)
            }))),
        }
    }
}

/// Calls `next` with its own context and maps the stream; `CALL_SCOPE` is
/// `S`.
struct Relay<const S: bool>;

impl<const S: bool> Operator for Relay<S> {
    type UpReq = Req;
    type UpItem = u32;
    type DownReq = Req;
    type DownItem = u32;
    const CALL_SCOPE: bool = S;

    async fn generate<N: Next<Req, u32>>(&self, cx: Cx, req: Req, next: N) -> Result<Items<u32>> {
        let below = next.generate(cx, req).await?;
        Ok(items(below.map(|item| item)))
    }
}

/// A backward node that maps its stream.
struct Map;

impl Backward for Map {
    type In = u32;
    type Out = u32;

    fn backward(&self, _cx: &Cx, input: Items<u32>) -> Items<u32> {
        items(input.map(|item| item))
    }
}

/// A backward node that passes only errors on, and says so.
struct Filter;

impl Backward for Filter {
    type In = u32;
    type Out = u32;
    const FILTERS: bool = true;

    fn backward(&self, _cx: &Cx, input: Items<u32>) -> Items<u32> {
        items(input.filter(|item| std::future::ready(item.is_err())))
    }
}

const PHASES: [Phase; 3] = [Phase::Dispatch, Phase::Waiting, Phase::Ready];
const ENDS: [End; 3] = [End::Cancel, End::Drop, End::Deadline];
const WITHIN: Duration = Duration::from_secs(5);

/// Every phase and every end through `graph`.
async fn every_case<N: Next<Req, u32>>(shape: &str, graph: N) {
    let graph = Arc::new(graph);
    for phase in PHASES {
        for end in ENDS {
            one_case(shape, &graph, phase, end).await;
        }
    }
}

async fn one_case<N: Next<Req, u32>>(shape: &str, graph: &Arc<N>, phase: Phase, end: End) {
    let case = format!("{shape}, {phase:?}, {end:?}");
    let live = Arc::new(AtomicUsize::new(0));
    let root = match end {
        End::Deadline => Cx::new().with_timeout(Duration::from_millis(20)),
        _ => Cx::new(),
    };
    let req = Req {
        phase,
        live: Arc::clone(&live),
    };
    let call = {
        let graph = Arc::clone(graph);
        let root = root.clone();
        tokio::spawn(async move {
            match graph.generate(root, req).await {
                Ok(mut out) => {
                    let mut last = None;
                    while let Some(item) = out.next().await {
                        last = Some(item.map_err(|e| e.kind()));
                    }
                    last
                }
                Err(error) => Some(Err(error.kind())),
            }
        })
    };
    // The request has reached its sink.
    tokio::time::timeout(WITHIN, async {
        while live.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{case}: the sink never ran"));
    match end {
        End::Cancel => root.cancel(),
        End::Drop => call.abort(),
        End::Deadline => {}
    }
    tokio::time::timeout(WITHIN, async {
        while live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{case}: the sink's work outlived the request"));
    let outcome = tokio::time::timeout(WITHIN, call)
        .await
        .unwrap_or_else(|_| panic!("{case}: the call did not end"));
    let want = match end {
        End::Cancel => Some(Err(ErrorKind::Cancelled)),
        End::Deadline => Some(Err(ErrorKind::DeadlineExceeded)),
        End::Drop => {
            assert!(outcome.is_err(), "{case}: the aborted call finished");
            return;
        }
    };
    assert_eq!(outcome.expect("the call's task"), want, "{case}");
}

fn sink() -> Hop<(), SinkStage<End0>, ()> {
    Hop::new((), SinkStage::new(Arc::new(End0)), ())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_default_call_tears_down_in_every_phase() {
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<false>), sink()),
        (),
    ));
    every_case("default", graph).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scoped_call_tears_down_in_every_phase() {
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<true>), sink()),
        (),
    ));
    every_case("scoped", graph).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_default_call_with_a_backward_node_tears_down_in_every_phase() {
    let below = Hop::new((), SinkStage::new(Arc::new(End0)), (Arc::new(Map), ()));
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<false>), below),
        (),
    ));
    every_case("backward", graph).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_default_call_with_a_declared_filter_tears_down_in_every_phase() {
    let below = Hop::new((), SinkStage::new(Arc::new(End0)), (Arc::new(Filter), ()));
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<false>), below),
        (),
    ));
    every_case("filter", graph).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_nested_default_calls_tear_down_in_every_phase() {
    let inner = Hop::new((), OperatorStage::new(Arc::new(Relay::<false>), sink()), ());
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(Arc::new(Relay::<false>), inner),
        (),
    ));
    every_case("nested", graph).await;
}
