//! What an operator's `next` gives under the request's scope (the default)
//! and under a scope per call (`Operator::CALL_SCOPE`). Each test runs one
//! operator in both forms and asserts where they differ.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use futures::{StreamExt, stream};
use gdl::{
    Cx, ErrorKind, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage, Source,
    items,
};

/// What a call does, chosen per test.
#[derive(Clone, Copy)]
enum Plan {
    /// Call `next` twice, one after the other, reading each to its end.
    Twice,
    /// Call `next` twice at once, read one item of each, drop the second.
    Hedge,
    /// Call `next` once and report how its stream ended.
    Once,
    /// Call `next` once and drop every item.
    Filter,
    /// Call `next` with a deadline 10 ms out.
    Narrower,
    /// Call `next` with a child scope, and cancel it after the first item.
    Child,
    /// Call `next` until a call succeeds, up to 4,096 times.
    Retry,
    /// Call `next` from a spawned task, and return what it sends on.
    Spawned,
}

/// The operator, with `CALL_SCOPE` set to `S`.
struct Op<const S: bool> {
    plan: Plan,
    seen: Arc<Mutex<Vec<String>>>,
}

impl<const S: bool> Operator for Op<S> {
    type UpReq = u32;
    type UpItem = u32;
    type DownReq = u32;
    type DownItem = u32;
    const CALL_SCOPE: bool = S;

    async fn generate<N: Next<u32, u32>>(&self, cx: Cx, req: u32, next: N) -> Result<Items<u32>> {
        let seen = Arc::clone(&self.seen);
        let note = move |what: String| seen.lock().expect("a lock").push(what);
        match self.plan {
            Plan::Twice => {
                let first: Vec<_> = next.generate(cx.clone(), req).await?.collect().await;
                note(format!("first: {}", outcome(&first)));
                let second: Vec<_> = match next.generate(cx, req + 1).await {
                    Ok(items) => items.collect().await,
                    Err(error) => vec![Err(error)],
                };
                note(format!("second: {}", outcome(&second)));
                Ok(items(stream::iter(first.into_iter().chain(second))))
            }
            Plan::Hedge => {
                let mut a = next.generate(cx.clone(), req).await?;
                let mut b = next.generate(cx, req + 1).await?;
                let _ = a.next().await;
                let _ = b.next().await;
                drop(b);
                Ok(a)
            }
            Plan::Once => {
                let below = next.generate(cx, req).await?;
                let read = stream::once(async move {
                    let all: Vec<_> = below.collect().await;
                    note(format!("below: {}", outcome(&all)));
                    stream::iter(all)
                });
                Ok(items(read.flatten()))
            }
            Plan::Filter => {
                let below = next.generate(cx, req).await?;
                Ok(items(
                    below.filter(|item| std::future::ready(item.is_err())),
                ))
            }
            Plan::Narrower => {
                let narrower = cx.with_timeout(Duration::from_millis(10));
                next.generate(narrower, req).await
            }
            Plan::Child => {
                let call = cx.child();
                let mut below = next.generate(call.clone(), req).await?;
                let first = below.next().await;
                note(format!("first: {}", first.is_some()));
                call.cancel();
                let rest: Vec<_> = below.collect().await;
                note(format!("after cancel: {}", outcome(&rest)));
                Ok(items(stream::iter(rest)))
            }
            Plan::Spawned => {
                let (tx, rx) = futures::channel::mpsc::unbounded();
                tokio::spawn(async move {
                    if let Ok(mut below) = next.generate(cx, req).await {
                        while let Some(item) = below.next().await {
                            if tx.unbounded_send(item).is_err() {
                                return;
                            }
                        }
                    }
                });
                Ok(items(rx))
            }
            Plan::Retry => {
                for _ in 0..4_096 {
                    if let Ok(items) = next.generate(cx.clone(), req).await {
                        return Ok(items);
                    }
                }
                Err(gdl::Error::node("gave up"))
            }
        }
    }
}

fn outcome(all: &[Result<u32>]) -> String {
    let ok = all.iter().filter(|item| item.is_ok()).count();
    match all.iter().find_map(|item| item.as_ref().err()) {
        Some(error) => format!("{ok} items, {:?}", error.kind()),
        None => format!("{ok} items, clean"),
    }
}

/// What the sink does, chosen per test.
#[derive(Clone, Copy)]
enum Answer {
    /// Three items. The first call cancels its own context at its end.
    CancelSelfFirst,
    /// Three ready items, then a cancel of the root and a clean end.
    CancelRootAt3,
    /// One item, then waits for ever, recording when its context is
    /// cancelled.
    OneThenWait,
    /// Ready items for ever; cancels the root after item 100.
    CancelRootAt100,
    /// Waits for ever.
    Stall,
    /// One item, then waits for ever.
    OneThenStall,
    /// One item, an `Err`, then one more item.
    ErrThenMore,
    /// Fails at once.
    Fail,
    /// Never answers and never reads its context; counts itself live in
    /// `produced` until its future is dropped.
    StallHolding,
}

struct End {
    answer: Answer,
    root: Mutex<Option<Cx>>,
    calls: AtomicUsize,
    produced: Arc<AtomicUsize>,
    cancelled: Arc<Mutex<Vec<u32>>>,
}

impl Sink for End {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, cx: Cx, req: u32) -> Result<Items<u32>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let cancelled = Arc::clone(&self.cancelled);
        let watched = cx.clone();
        tokio::spawn(async move {
            watched.cancelled().await;
            cancelled.lock().expect("a lock").push(req);
        });
        match self.answer {
            Answer::CancelSelfFirst => {
                let own = cx.clone();
                let end = stream::once(async move {
                    if call == 0 {
                        own.cancel();
                    }
                    None::<Result<u32>>
                })
                .filter_map(std::future::ready);
                Ok(items(stream::iter([Ok(1), Ok(2), Ok(3)]).chain(end)))
            }
            Answer::CancelRootAt3 => {
                let root = self.root.lock().expect("a lock").clone().expect("a root");
                let end = stream::once(async move {
                    root.cancel();
                    None::<Result<u32>>
                })
                .filter_map(std::future::ready);
                Ok(items(stream::iter([Ok(0), Ok(1), Ok(2)]).chain(end)))
            }
            Answer::OneThenWait | Answer::OneThenStall => {
                Ok(items(stream::iter([Ok(req)]).chain(stream::pending())))
            }
            Answer::CancelRootAt100 => {
                let root = self.root.lock().expect("a lock").clone().expect("a root");
                let produced = Arc::clone(&self.produced);
                Ok(items(stream::iter(0..2_000_000_u32).map(move |n| {
                    produced.fetch_add(1, Ordering::SeqCst);
                    if n == 100 {
                        root.cancel();
                    }
                    Ok(n)
                })))
            }
            Answer::Stall => Ok(items(stream::poll_fn(|_| {
                Poll::<Option<Result<u32>>>::Pending
            }))),
            Answer::ErrThenMore => Ok(items(stream::iter([
                Ok(1),
                Err(gdl::Error::node("failed")),
                Ok(2),
            ]))),
            Answer::Fail => Err(gdl::Error::node("down")),
            Answer::StallHolding => {
                struct Live(Arc<AtomicUsize>);
                impl Drop for Live {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                self.produced.fetch_add(1, Ordering::SeqCst);
                let _live = Live(Arc::clone(&self.produced));
                std::future::pending::<()>().await;
                unreachable!()
            }
        }
    }
}

struct Run {
    seen: Arc<Mutex<Vec<String>>>,
    sink: Arc<End>,
    out: Vec<std::result::Result<u32, ErrorKind>>,
}

/// One request through `Source(hop(op(hop(sink))))`, with `CALL_SCOPE`
/// `S`, read to its end or for at most `bound`.
async fn run<const S: bool>(plan: Plan, answer: Answer, root: Cx, bound: Duration) -> Run {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::new(End {
        answer,
        root: Mutex::new(Some(root.clone())),
        calls: AtomicUsize::new(0),
        produced: Arc::new(AtomicUsize::new(0)),
        cancelled: Arc::new(Mutex::new(Vec::new())),
    });
    let op = Op::<S> {
        plan,
        seen: Arc::clone(&seen),
    };
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(op),
            Hop::new((), SinkStage::new(Arc::clone(&sink)), ()),
        ),
        (),
    ));
    let mut out = Vec::new();
    let read = async {
        match graph.generate(root, 7).await {
            Ok(mut items) => {
                while let Some(item) = items.next().await {
                    out.push(item.map_err(|e| e.kind()));
                }
            }
            Err(error) => out.push(Err(error.kind())),
        }
    };
    let _ = tokio::time::timeout(bound, read).await;
    // Let the sinks' watchers run.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    Run { seen, sink, out }
}

fn seen(run: &Run) -> Vec<String> {
    run.seen.lock().expect("a lock").clone()
}

fn cancelled(run: &Run) -> Vec<u32> {
    let mut all = run.sink.cancelled.lock().expect("a lock").clone();
    all.sort_unstable();
    all
}

/// A clean end after a cleanup cancel, and the reach of a node's own cancel:
/// under the request's scope, a sink that cancels its own context cancels
/// the request scope, so the operator's next call ends at once, quietly:
/// the edge reports the cancel.
#[tokio::test]
async fn a_node_that_cancels_its_own_context_reaches_the_request_by_default() {
    let bound = Duration::from_secs(5);
    let scoped = run::<true>(Plan::Twice, Answer::CancelSelfFirst, Cx::new(), bound).await;
    let passed = run::<false>(Plan::Twice, Answer::CancelSelfFirst, Cx::new(), bound).await;
    assert_eq!(
        seen(&scoped),
        ["first: 3 items, Cancelled", "second: 3 items, clean"]
    );
    assert_eq!(
        seen(&passed),
        ["first: 3 items, clean", "second: 0 items, clean"]
    );
}

/// A call's stream that ends cancels that call's scope only in the scoped
/// form; by default its sink's `cx.cancelled()` resolves at the request's
/// end, and dropping a hedged call does not cancel it.
#[tokio::test]
async fn dropping_one_call_cancels_it_only_with_a_call_scope() {
    let bound = Duration::from_millis(200);
    let scoped = run::<true>(Plan::Hedge, Answer::OneThenWait, Cx::new(), bound).await;
    let root = Cx::new();
    let passed = run::<false>(Plan::Hedge, Answer::OneThenWait, root.clone(), bound).await;
    assert_eq!(
        cancelled(&scoped),
        [7, 8],
        "the dropped call, then the timeout"
    );
    assert_eq!(cancelled(&passed), [7, 8], "both, when the request ends");
    // The difference is when: read before the request's end.
    let root = Cx::new();
    let seen_early = Arc::new(Mutex::new(Vec::new()));
    for scoped in [true, false] {
        let sink = Arc::new(End {
            answer: Answer::OneThenWait,
            root: Mutex::new(None),
            calls: AtomicUsize::new(0),
            produced: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(Mutex::new(Vec::new())),
        });
        let inner = Hop::new((), SinkStage::new(Arc::clone(&sink)), ());
        let stream = if scoped {
            let op = Op::<true> {
                plan: Plan::Hedge,
                seen: Arc::default(),
            };
            Source::new(Hop::new((), OperatorStage::new(Arc::new(op), inner), ()))
                .generate(root.child(), 7)
                .await
                .expect("a value")
        } else {
            let op = Op::<false> {
                plan: Plan::Hedge,
                seen: Arc::default(),
            };
            Source::new(Hop::new((), OperatorStage::new(Arc::new(op), inner), ()))
                .generate(root.child(), 7)
                .await
                .expect("a value")
        };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        seen_early
            .lock()
            .expect("a value")
            .push((scoped, sink.cancelled.lock().expect("a lock").clone()));
        drop(stream);
    }
    assert_eq!(
        *seen_early.lock().expect("a lock"),
        [(true, vec![8]), (false, vec![])],
        "while the request runs, only the scoped form cancelled the dropped call"
    );
    let _ = passed.out;
}

/// A clean end after a cancel, as a converter meets it: the request is
/// cancelled, and the sink then ends cleanly. The operator sees `Cancelled`
/// from its own call only in the scoped form; by default it sees a clean
/// end, and the request's edge reports the cancel to the caller after it.
#[tokio::test]
async fn a_clean_end_after_a_cancel_reaches_the_operator_as_cancelled_only_with_a_call_scope() {
    let bound = Duration::from_secs(5);
    for scoped in [true, false] {
        let root = Cx::new();
        let run = if scoped {
            run::<true>(Plan::Once, Answer::CancelRootAt3, root, bound).await
        } else {
            run::<false>(Plan::Once, Answer::CancelRootAt3, root, bound).await
        };
        let below = seen(&run).pop().expect("the operator saw its stream end");
        assert_eq!(
            below.ends_with("Cancelled"),
            scoped,
            "scoped={scoped}: {below}"
        );
        assert_eq!(
            run.out.last(),
            Some(&Err(ErrorKind::Cancelled)),
            "scoped={scoped}"
        );
    }
}

/// A cancel inside a filter, and the fairness of a filter: an
/// operator that drops every item of an always-ready sink. With a scope per
/// call, the call's own check ends it within 64 items of the cancel. By
/// default nothing below the edge checks, and the filter reads its source
/// to the end inside one poll.
#[tokio::test]
async fn a_filter_over_a_ready_source_sees_a_cancel_only_through_a_call_scope() {
    let bound = Duration::from_secs(30);
    let root = Cx::new();
    let scoped = run::<true>(Plan::Filter, Answer::CancelRootAt100, root, bound).await;
    let after = scoped.sink.produced.load(Ordering::SeqCst) - 101;
    assert!(after < 64, "scoped: {after} items after the cancel");
    assert_eq!(scoped.out, [Err(ErrorKind::Cancelled)]);

    let root = Cx::new();
    let passed = run::<false>(Plan::Filter, Answer::CancelRootAt100, root, bound).await;
    let after = passed.sink.produced.load(Ordering::SeqCst) - 101;
    assert_eq!(
        after,
        2_000_000 - 101,
        "by default the filter reads to the end"
    );
    assert_eq!(passed.out, [Err(ErrorKind::Cancelled)]);
}

/// A call with a narrower deadline than its operator's gets the hop's own
/// attempt, whatever `CALL_SCOPE` says, so the deadline is enforced.
#[tokio::test(start_paused = true)]
async fn a_call_with_a_narrower_deadline_is_bounded_by_it_either_way() {
    let bound = Duration::from_secs(5);
    let scoped = run::<true>(Plan::Narrower, Answer::Stall, Cx::new(), bound).await;
    let passed = run::<false>(Plan::Narrower, Answer::Stall, Cx::new(), bound).await;
    assert_eq!(scoped.out, [Err(ErrorKind::DeadlineExceeded)]);
    assert_eq!(passed.out, [Err(ErrorKind::DeadlineExceeded)]);
}

/// A call under a child scope the operator made gets the hop's own attempt,
/// whatever `CALL_SCOPE` says, so cancelling that child ends the call.
#[tokio::test]
async fn a_call_under_a_child_scope_ends_when_the_child_is_cancelled_either_way() {
    let bound = Duration::from_secs(5);
    let scoped = run::<true>(Plan::Child, Answer::OneThenStall, Cx::new(), bound).await;
    let passed = run::<false>(Plan::Child, Answer::OneThenStall, Cx::new(), bound).await;
    let expected = ["first: true", "after cancel: 0 items, Cancelled"];
    assert_eq!(seen(&scoped), expected);
    assert_eq!(seen(&passed), expected);
}

/// A storm of retries: calls that fail at once still spend cooperative
/// budget under the default, so a cancel from another task gets in long
/// before 4,096 calls.
#[tokio::test]
async fn retries_that_fail_at_once_leave_room_for_a_cancel_by_default() {
    let root = Cx::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::new(End {
        answer: Answer::Fail,
        root: Mutex::new(None),
        calls: AtomicUsize::new(0),
        produced: Arc::new(AtomicUsize::new(0)),
        cancelled: Arc::new(Mutex::new(Vec::new())),
    });
    let op = Op::<false> {
        plan: Plan::Retry,
        seen,
    };
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(op),
            Hop::new((), SinkStage::new(Arc::clone(&sink)), ()),
        ),
        (),
    ));
    let watched = Arc::clone(&sink);
    let canceller = root.clone();
    let cancel = tokio::spawn(async move {
        while watched.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        canceller.cancel();
    });
    let result = graph.generate(root, 1).await;
    cancel.await.expect("a call");
    let calls = sink.calls.load(Ordering::SeqCst);
    assert!(matches!(result, Err(ref e) if e.kind() == ErrorKind::Cancelled));
    assert!(calls < 4_096, "{calls} calls before the cancel got in");
}

/// What a client detach leaves behind: an operator that calls `next` from a
/// task it spawned, over a sink whose dispatch never answers and never reads
/// its context. With a scope per call, the hop's race in that task ends the
/// dispatch when the request goes. By default nothing races it, and the
/// sink's future lives until the sink ends on its own.
#[tokio::test]
async fn a_spawned_call_is_ended_by_a_detach_only_with_a_call_scope() {
    for scoped in [true, false] {
        let sink = Arc::new(End {
            answer: Answer::StallHolding,
            root: Mutex::new(None),
            calls: AtomicUsize::new(0),
            produced: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(Mutex::new(Vec::new())),
        });
        let inner = Hop::new((), SinkStage::new(Arc::clone(&sink)), ());
        let stream = if scoped {
            let op = Op::<true> {
                plan: Plan::Spawned,
                seen: Arc::default(),
            };
            Source::new(Hop::new((), OperatorStage::new(Arc::new(op), inner), ()))
                .generate(Cx::new(), 7)
                .await
                .expect("a value")
        } else {
            let op = Op::<false> {
                plan: Plan::Spawned,
                seen: Arc::default(),
            };
            Source::new(Hop::new((), OperatorStage::new(Arc::new(op), inner), ()))
                .generate(Cx::new(), 7)
                .await
                .expect("a value")
        };
        while sink.produced.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        drop(stream);
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        let live = sink.produced.load(Ordering::SeqCst);
        assert_eq!(live, usize::from(!scoped), "scoped={scoped}: {live} live");
    }
}

/// A producer that sends items after an `Err`. In an attempt, the stream
/// ends at the `Err`. Under the request scope nothing between the operator
/// and the producer ends it, so the operator sees the items after it; the
/// request's edge ends the stream at the first `Err` that reaches it.
#[tokio::test]
async fn items_after_an_err_reach_a_default_scoped_operator() {
    let bound = Duration::from_secs(5);
    let scoped = run::<true>(Plan::Once, Answer::ErrThenMore, Cx::new(), bound).await;
    let passed = run::<false>(Plan::Once, Answer::ErrThenMore, Cx::new(), bound).await;
    assert_eq!(seen(&scoped), ["below: 1 items, Node"]);
    assert_eq!(seen(&passed), ["below: 2 items, Node"]);
    let expected = [Ok(1), Err(ErrorKind::Node)];
    assert_eq!(scoped.out, expected, "the edge ends at the first Err");
    assert_eq!(passed.out, expected, "the edge ends at the first Err");
}
