//! An operator that turns its `next`'s clean end into a closing item, such
//! as closing replies, an SSE `[DONE]`, or a final usage chunk, over an
//! engine that ends its stream cleanly when its request is cancelled or its
//! deadline passes.
//!
//! Under the request scope (the default), a call whose request is over ends
//! quietly, and only the request's edge reports why. So an operator that
//! closes on every clean end closes a cut answer too. One `cx.check()` on
//! the clean end tells a kill from a real end; `CALL_SCOPE` does too, at a
//! per-item cost. `docs/src/guides/write-nodes.md` gives both forms.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{StreamExt, stream};
use gdl::{
    Cx, ErrorKind, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage, Source,
    items,
};

/// The closing item.
const CLOSE: u32 = 99;

/// The operator: passes its `next`'s items on, and emits [`CLOSE`] when
/// that stream ends cleanly. With `CHECK`, it calls `cx.check()` on the
/// clean end first and ends with that error instead. `CALL_SCOPE` is `S`.
struct Closer<const S: bool, const CHECK: bool> {
    emitted: Arc<Mutex<Vec<Result<u32, ErrorKind>>>>,
}

impl<const S: bool, const CHECK: bool> Operator for Closer<S, CHECK> {
    type UpReq = u32;
    type UpItem = u32;
    type DownReq = u32;
    type DownItem = u32;
    const CALL_SCOPE: bool = S;

    async fn generate<N: Next<u32, u32>>(&self, cx: Cx, req: u32, next: N) -> Result<Items<u32>> {
        let below = next.generate(cx.clone(), req).await?;
        let emitted = Arc::clone(&self.emitted);
        // `Some(stream)` while reading; `None` once the stream has ended.
        let out = stream::unfold(Some((below, cx)), |state| async move {
            let (mut below, cx) = state?;
            match below.next().await {
                Some(Ok(item)) => Some((Ok(item), Some((below, cx)))),
                Some(Err(error)) => Some((Err(error), None)),
                None => {
                    // Drop the producer first, as a node boundary does: its
                    // cleanup may be what cancels.
                    drop(below);
                    if CHECK && let Err(error) = cx.check() {
                        return Some((Err(error), None));
                    }
                    Some((Ok(CLOSE), None))
                }
            }
        })
        .inspect(move |item| {
            let item = item.as_ref().map(|n| *n).map_err(gdl::Error::kind);
            emitted.lock().expect("a lock").push(item);
        });
        Ok(items(out))
    }
}

/// The engine: one item, then a clean end once its context is cancelled or
/// its deadline passes, as an engine that polls a kill flag ends. With
/// `cancel`, it also cancels its context just before that end, so that the
/// request is both cancelled and past its deadline when it ends.
struct Engine {
    cancel: bool,
}

impl Sink for Engine {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, cx: Cx, req: u32) -> Result<Items<u32>> {
        let cancel = self.cancel;
        let end = stream::once(async move {
            let _why = cx.done().await;
            if cancel {
                cx.cancel();
            }
            None::<Result<u32>>
        })
        .filter_map(std::future::ready);
        Ok(items(stream::iter([Ok(req)]).chain(end)))
    }
}

/// How the request is killed.
#[derive(Clone, Copy, Debug)]
enum Kill {
    /// The caller cancels the request after it reads the first item.
    Cancel,
    /// The request's deadline passes while the engine waits.
    Deadline,
    /// The deadline passes while the engine waits, and the engine cancels
    /// its context before it ends: the deadline comes first, as at an
    /// attempt boundary.
    Both,
}

impl Kill {
    fn kind(self) -> ErrorKind {
        match self {
            Kill::Cancel => ErrorKind::Cancelled,
            Kill::Deadline | Kill::Both => ErrorKind::DeadlineExceeded,
        }
    }
}

/// What the operator emitted and what the request's edge reported.
struct Run {
    emitted: Vec<Result<u32, ErrorKind>>,
    out: Vec<Result<u32, ErrorKind>>,
}

/// One request through `Source(hop(closer(hop(engine))))`, killed by
/// `kill`, read to its end.
async fn run<const S: bool, const CHECK: bool>(kill: Kill) -> Run {
    let emitted = Arc::new(Mutex::new(Vec::new()));
    let closer = Closer::<S, CHECK> {
        emitted: Arc::clone(&emitted),
    };
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(closer),
            Hop::new(
                (),
                SinkStage::new(Arc::new(Engine {
                    cancel: matches!(kill, Kill::Both),
                })),
                (),
            ),
        ),
        (),
    ));
    let root = match kill {
        Kill::Cancel => Cx::new(),
        Kill::Deadline | Kill::Both => Cx::new().with_timeout(Duration::from_secs(1)),
    };
    let mut out = Vec::new();
    // The bound only turns a defect into a failure: time is paused, so the
    // deadline case advances the clock to the deadline at once.
    let read = async {
        match graph.generate(root.clone(), 7).await {
            Ok(mut items) => {
                while let Some(item) = items.next().await {
                    if let (Kill::Cancel, Ok(7)) = (kill, &item) {
                        root.cancel();
                    }
                    out.push(item.map_err(|e| e.kind()));
                }
            }
            Err(error) => out.push(Err(error.kind())),
        }
    };
    tokio::time::timeout(Duration::from_secs(60), read)
        .await
        .expect("the request ends");
    let emitted = emitted.lock().expect("a lock").clone();
    Run { emitted, out }
}

/// The hazard: with no check, a killed request's clean end reaches the
/// operator as a clean end, and it emits its closing item. The caller reads
/// that item, and only then the edge's report of the kill: a client that
/// stops at the closing item takes a cut answer as finished.
#[tokio::test(start_paused = true)]
async fn without_a_check_a_killed_request_still_closes() {
    for kill in [Kill::Cancel, Kill::Deadline, Kill::Both] {
        let run = run::<false, false>(kill).await;
        assert_eq!(run.emitted, [Ok(7), Ok(CLOSE)], "{kill:?}");
        assert_eq!(run.out, [Ok(7), Ok(CLOSE), Err(kill.kind())], "{kill:?}");
    }
}

/// One `cx.check()` on the clean end: the operator ends with the kill's
/// error and emits no closing item, and the edge reports the same error.
#[tokio::test(start_paused = true)]
async fn a_check_on_the_clean_end_ends_a_killed_request_without_closing() {
    for kill in [Kill::Cancel, Kill::Deadline, Kill::Both] {
        let run = run::<false, true>(kill).await;
        assert_eq!(run.emitted, [Ok(7), Err(kill.kind())], "{kill:?}");
        assert_eq!(run.out, [Ok(7), Err(kill.kind())], "{kill:?}");
    }
}

/// `CALL_SCOPE`: the call itself ends with the kill's error, so the
/// operator emits no closing item without a check of its own.
#[tokio::test(start_paused = true)]
async fn a_call_scope_ends_a_killed_request_without_closing() {
    for kill in [Kill::Cancel, Kill::Deadline, Kill::Both] {
        let run = run::<true, false>(kill).await;
        assert_eq!(run.emitted, [Ok(7), Err(kill.kind())], "{kill:?}");
        assert_eq!(run.out, [Ok(7), Err(kill.kind())], "{kill:?}");
    }
}

/// A control: a request that is not killed ends cleanly, and every form
/// closes it.
#[tokio::test(start_paused = true)]
async fn every_form_closes_a_request_that_ends_cleanly() {
    async fn clean<const S: bool, const CHECK: bool>() -> Run {
        let emitted = Arc::new(Mutex::new(Vec::new()));
        let closer = Closer::<S, CHECK> {
            emitted: Arc::clone(&emitted),
        };
        let graph = Source::new(Hop::new(
            (),
            OperatorStage::new(
                Arc::new(closer),
                Hop::new((), SinkStage::new(Arc::new(Ready)), ()),
            ),
            (),
        ));
        let out = graph
            .generate(Cx::new(), 7)
            .await
            .expect("a stream")
            .map(|item| item.map_err(|e| e.kind()))
            .collect()
            .await;
        let emitted = emitted.lock().expect("a lock").clone();
        Run { emitted, out }
    }
    for run in [
        clean::<false, false>().await,
        clean::<false, true>().await,
        clean::<true, false>().await,
    ] {
        assert_eq!(run.emitted, [Ok(7), Ok(CLOSE)]);
        assert_eq!(run.out, [Ok(7), Ok(CLOSE)]);
    }
}

/// An engine that answers one item and ends.
struct Ready;

impl Sink for Ready {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, _cx: Cx, req: u32) -> Result<Items<u32>> {
        Ok(items(stream::iter([Ok(req)])))
    }
}

struct Cleanup {
    cancel: bool,
}

impl Sink for Cleanup {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, cx: Cx, req: u32) -> Result<Items<u32>> {
        Ok(items(CleanupStream {
            cx,
            first: Some(req),
            cancel: self.cancel,
        }))
    }
}

struct CleanupStream {
    cx: Cx,
    first: Option<u32>,
    cancel: bool,
}

impl futures::Stream for CleanupStream {
    type Item = Result<u32>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _task: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.first.take().map(Ok))
    }
}

impl Drop for CleanupStream {
    fn drop(&mut self) {
        if self.cancel {
            self.cx.cancel();
        }
    }
}

async fn cleanup<const S: bool, const CHECK: bool>(cancel: bool) -> Run {
    let emitted = Arc::new(Mutex::new(Vec::new()));
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(Closer::<S, CHECK> {
                emitted: Arc::clone(&emitted),
            }),
            Hop::new((), SinkStage::new(Arc::new(Cleanup { cancel })), ()),
        ),
        (),
    ));
    let out = graph
        .generate(Cx::new(), 7)
        .await
        .expect("a stream")
        .map(|item| item.map_err(|error| error.kind()))
        .collect()
        .await;
    let emitted = emitted.lock().expect("a lock").clone();
    Run { emitted, out }
}

/// An end check must include cancellation caused by dropping the input.
#[tokio::test]
async fn a_check_rejects_a_cancel_made_by_downstream_cleanup() {
    let run = cleanup::<false, true>(true).await;
    assert_eq!(run.out, [Ok(7), Err(ErrorKind::Cancelled)]);
    assert_eq!(run.emitted, [Ok(7), Err(ErrorKind::Cancelled)]);
}

#[tokio::test]
async fn downstream_cleanup_controls() {
    for run in [
        cleanup::<false, false>(false).await,
        cleanup::<false, true>(false).await,
        cleanup::<true, false>(false).await,
    ] {
        assert_eq!(run.emitted, [Ok(7), Ok(CLOSE)]);
        assert_eq!(run.out, [Ok(7), Ok(CLOSE)]);
    }

    let unchecked = cleanup::<false, false>(true).await;
    assert_eq!(unchecked.emitted, [Ok(7), Ok(CLOSE)]);
    assert_eq!(unchecked.out, [Ok(7), Ok(CLOSE), Err(ErrorKind::Cancelled)]);

    let scoped = cleanup::<true, false>(true).await;
    assert_eq!(scoped.emitted, [Ok(7), Err(ErrorKind::Cancelled)]);
    assert_eq!(scoped.out, [Ok(7), Err(ErrorKind::Cancelled)]);
}
