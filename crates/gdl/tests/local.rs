//! The reference graph with every node in one segment.

mod common;

use common::*;
use futures::StreamExt;
use gdl::{Cx, ErrorKind, Next};

#[tokio::test]
async fn items_pass_through_every_stage_in_order() {
    let events = Events::new();
    let graph = local_graph(&events);
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let out = graph
        .call(Cx::new(), prompt("p", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..3));
    // C ran before D and changed what D received.
    assert!(events.has("C:p"));
    events.wait_for("D:done:c(p)#0", WAIT).await;
}

#[tokio::test]
async fn an_operator_retries_from_inside_its_stream() {
    let events = Events::new();
    let graph = local_graph(&events);
    let plan = Plan {
        tokens: 2,
        fail_attempts: 2,
        mode: Mode::Retry { max: 3 },
        ..Plan::default()
    };
    let out = graph
        .call(Cx::new(), prompt("r", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..2));
    assert!(events.has("B:retry:0:node error"));
    assert!(events.has("B:retry:1:node error"));
    events.wait_for("D:done:c(r)#2", WAIT).await;
}

#[tokio::test]
async fn dropping_a_pending_concurrent_attempt_cancels_it_and_not_its_sibling() {
    let events = Events::new();
    let graph = local_graph(&events);
    let root = Cx::new();
    let plan = Plan {
        tokens: 3,
        item_delay_ms: 5,
        slow_attempt: Some((1, 60_000)),
        mode: Mode::Hedge,
        ..Plan::default()
    };
    let out = graph
        .call(root.clone(), prompt("h", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..3));
    assert!(events.has("B:hedge:winner:0"));
    events.wait_for("D:abandoned:c(h)#1", WAIT).await;
    assert!(events.cx_of("D:c(h)#1").is_cancelled(), "the loser's scope");
    events.wait_for("D:done:c(h)#0", WAIT).await;
    assert!(!root.is_cancelled());
}

#[tokio::test]
async fn dropping_an_established_concurrent_stream_cancels_it_and_not_its_sibling() {
    let events = Events::new();
    let graph = local_graph(&events);
    let root = Cx::new();
    let plan = Plan {
        tokens: 3,
        item_delay_ms: 5,
        slow_items: Some((1, 60_000)),
        mode: Mode::Hedge,
        ..Plan::default()
    };
    let out = graph
        .call(root.clone(), prompt("v", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..3));
    assert!(events.has("B:hedge:winner:0"));
    // The loser returned its stream (its items were slow, not its
    // `generate`), and that stream was dropped mid-flight.
    assert!(events.has("D:start:c(v)#1"));
    assert!(!events.has("D:abandoned:c(v)#1"));
    events.wait_for("D:cancelled:c(v)#1", WAIT).await;
    assert!(events.cx_of("D:c(v)#1").is_cancelled());
    events.wait_for("D:done:c(v)#0", WAIT).await;
    assert!(!root.is_cancelled());
}

#[tokio::test]
async fn dropping_a_pending_generate_cancels_only_that_attempt() {
    let events = Events::new();
    let graph = local_graph(&events);
    let plan = Plan {
        tokens: 2,
        slow_attempt: Some((0, 60_000)),
        mode: Mode::AbandonFirst { after_ms: 20 },
        ..Plan::default()
    };
    let out = graph
        .call(Cx::new(), prompt("a", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..2));
    assert!(events.has("B:abandoned:0"));
    events.wait_for("D:abandoned:c(a)#0", WAIT).await;
    assert!(events.cx_of("D:c(a)#0").is_cancelled());
    // Attempt 1 ran to completion; attempt 0's cancel did not reach it.
    events.wait_for("D:done:c(a)#1", WAIT).await;
}

#[tokio::test]
async fn dropping_the_source_stream_cancels_the_sink() {
    let events = Events::new();
    let graph = local_graph(&events);
    let plan = Plan {
        endless: true,
        item_delay_ms: 1,
        ..Plan::default()
    };
    let mut out = graph
        .call(Cx::new(), prompt("d", plan))
        .await
        .expect("call");
    assert_eq!(out.next().await, Some(Ok("g(b(e(f0)))".to_string())));
    drop(out);
    events.wait_for("D:cancelled:c(d)#0", WAIT).await;
    assert!(events.cx_of("D:c(d)#0").is_cancelled());
}

#[tokio::test]
async fn cancelling_the_root_context_ends_the_stream_and_the_sink() {
    let events = Events::new();
    let graph = local_graph(&events);
    let cx = Cx::new();
    let plan = Plan {
        endless: true,
        item_delay_ms: 1,
        ..Plan::default()
    };
    let mut out = graph
        .call(cx.clone(), prompt("k", plan))
        .await
        .expect("call");
    assert!(out.next().await.is_some_and(|item| item.is_ok()));
    cx.cancel();
    let rest = collect(out).await;
    let last = rest.last().expect("a terminal item");
    assert_eq!(
        last.as_ref().map_err(|e| e.kind()),
        Err(ErrorKind::Cancelled)
    );
    events.wait_for("D:cancelled:c(k)#0", WAIT).await;
}

#[tokio::test]
async fn stop_drains_buffered_output_in_order_then_ends_cleanly() {
    let events = Events::new();
    let graph = local_graph(&events);
    let cx = Cx::new();
    let plan = Plan {
        tokens: 3,
        hold_at_end: true,
        ..Plan::default()
    };
    let out = graph
        .call(cx.clone(), prompt("s", plan))
        .await
        .expect("call");
    // D has written all three items into its buffer and is waiting; nothing
    // has been read yet.
    events.wait_for("D:held:c(s)#0", WAIT).await;
    cx.request_stop();
    let mut want = expected(0..3);
    want.push(Ok(format!("g(b(e(f{STOP_MARK})))")));
    assert_eq!(collect(out).await, want);
    events.wait_for("D:stopped:c(s)#0", WAIT).await;
}

#[tokio::test]
async fn the_deadline_ends_the_stream_and_the_work() {
    let events = Events::new();
    let graph = local_graph(&events);
    let cx = Cx::new().with_timeout(ms(50));
    let plan = Plan {
        endless: true,
        item_delay_ms: 1,
        ..Plan::default()
    };
    let out = graph.call(cx, prompt("t", plan)).await.expect("call");
    let all = collect(out).await;
    let last = all.last().expect("a terminal item");
    assert_eq!(
        last.as_ref().map_err(|e| e.kind()),
        Err(ErrorKind::DeadlineExceeded)
    );
    events.wait_for("D:deadline:c(t)#0", WAIT).await;
}

#[tokio::test]
async fn a_deadline_that_passes_before_the_stream_exists_fails_the_call() {
    let events = Events::new();
    let graph = local_graph(&events);
    let cx = Cx::new().with_timeout(ms(20));
    let plan = Plan {
        tokens: 1,
        slow_attempt: Some((0, 60_000)),
        ..Plan::default()
    };
    let error = match graph.call(cx, prompt("x", plan)).await {
        Ok(_) => panic!("the call should fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded);
    events.wait_for("D:abandoned:c(x)#0", WAIT).await;
    assert!(events.cx_of("D:c(x)#0").is_cancelled());
}

#[tokio::test]
async fn nothing_is_delivered_after_an_error() {
    let events = Events::new();
    let graph = local_graph(&events);
    let plan = Plan {
        tokens: 5,
        fail_at: Some(2),
        ..Plan::default()
    };
    let out = graph
        .call(Cx::new(), prompt("e", plan))
        .await
        .expect("call");
    let all = collect(out).await;
    assert_eq!(all.len(), 3, "{all:?}");
    assert_eq!(&all[..2], &expected(0..2)[..]);
    assert_eq!(all[2].as_ref().map_err(|e| e.kind()), Err(ErrorKind::Node));
}

#[tokio::test]
async fn a_terminal_error_cancels_its_attempt_while_the_stream_is_still_held() {
    let events = Events::new();
    let hop = sink_hop(&events);
    let parent = Cx::new();
    let failing = Pre {
        text: "x".into(),
        attempt: 0,
        plan: Plan {
            tokens: 3,
            fail_at: Some(0),
            hold_after_error: true,
            ..Plan::default()
        },
    };
    let sibling = Pre {
        text: "y".into(),
        attempt: 1,
        plan: Plan {
            endless: true,
            item_delay_ms: 1,
            ..Plan::default()
        },
    };
    let mut failed = hop
        .generate(parent.clone(), failing)
        .await
        .expect("attempt");
    let mut running = hop
        .generate(parent.clone(), sibling)
        .await
        .expect("attempt");

    let first = failed.next().await.expect("an item");
    assert_eq!(first.map_err(|e| e.kind()), Err(ErrorKind::Node));
    assert!(failed.next().await.is_none());
    // `failed` is still held: the error alone must end the attempt.
    events.wait_for("D:cancelled:c(x)#0", WAIT).await;
    assert!(events.cx_of("D:c(x)#0").is_cancelled());

    // The sibling attempt and the parent are untouched.
    assert!(running.next().await.is_some_and(|item| item.is_ok()));
    assert!(!events.cx_of("D:c(y)#1").is_cancelled());
    assert!(!parent.is_cancelled());
    drop(failed);
}

#[tokio::test]
async fn a_cancelled_context_refuses_new_attempts() {
    let events = Events::new();
    let graph = local_graph(&events);
    let cx = Cx::new();
    cx.cancel();
    let error = match graph.call(cx, prompt("n", Plan::default())).await {
        Ok(_) => panic!("the call should fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    assert!(!events.has("B:start:n"));
}

/// A backward node that holds an error until the sink's own scope has been
/// cancelled, then forwards it.
struct HoldErrorUntilSinkCancelled {
    events: std::sync::Arc<Events>,
    sink_key: String,
}

impl gdl::Backward for HoldErrorUntilSinkCancelled {
    type In = Raw;
    type Out = Raw;

    fn backward(&self, _cx: &Cx, input: gdl::Items<Raw>) -> gdl::Items<Raw> {
        let events = std::sync::Arc::clone(&self.events);
        let key = self.sink_key.clone();
        gdl::items(input.then(move |item| {
            let events = std::sync::Arc::clone(&events);
            let key = key.clone();
            async move {
                if item.is_err() {
                    events.cx_of(&key).cancelled().await;
                }
                item
            }
        }))
    }
}

#[tokio::test]
async fn an_error_cancels_the_node_that_produced_it_even_when_held_downstream() {
    use std::sync::Arc;

    let events = Events::new();
    let hop = gdl::Hop::new(
        (),
        gdl::SinkStage::new(Arc::new(NodeD {
            events: Arc::clone(&events),
        })),
        (
            Arc::new(HoldErrorUntilSinkCancelled {
                events: Arc::clone(&events),
                sink_key: "D:h#0".into(),
            }),
            (),
        ),
    );
    let parent = Cx::new();
    let failing = Pre {
        text: "h".into(),
        attempt: 0,
        plan: Plan {
            tokens: 3,
            fail_at: Some(0),
            hold_after_error: true,
            ..Plan::default()
        },
    };
    let sibling = Pre {
        text: "s".into(),
        attempt: 1,
        plan: Plan {
            endless: true,
            item_delay_ms: 1,
            ..Plan::default()
        },
    };
    let failed = hop
        .generate(parent.clone(), failing)
        .await
        .expect("attempt");
    let mut running = hop
        .generate(parent.clone(), sibling)
        .await
        .expect("attempt");
    // The original error arrives once the sink, and only the sink, is
    // cancelled.
    let all = collect(failed).await;
    assert_eq!(
        all.iter()
            .map(|r| r.as_ref().map_err(|e| e.kind()).copied())
            .collect::<Vec<_>>(),
        vec![Err(ErrorKind::Node)]
    );
    events.wait_for("D:cancelled:h#0", WAIT).await;
    assert!(running.next().await.is_some_and(|item| item.is_ok()));
    assert!(!events.cx_of("D:s#1").is_cancelled());
    assert!(!parent.is_cancelled());
}

/// A forward node whose completion coincides with its attempt's cancel.
struct CancelsAsItFinishes;

impl gdl::Forward for CancelsAsItFinishes {
    type In = Pre;
    type Out = Pre;

    async fn forward(&self, cx: &Cx, req: Pre) -> gdl::Result<Pre> {
        cx.cancel();
        Ok(req)
    }
}

#[tokio::test]
async fn an_interruption_during_forward_work_stops_the_hop_before_its_sink() {
    use std::sync::Arc;

    let events = Events::new();
    let hop = gdl::Hop::new(
        (Arc::new(CancelsAsItFinishes), ()),
        gdl::SinkStage::new(Arc::new(NodeD {
            events: Arc::clone(&events),
        })),
        (),
    );
    let pre = Pre {
        text: "i".into(),
        attempt: 0,
        plan: Plan {
            tokens: 1,
            ..Plan::default()
        },
    };
    let error = match hop.generate(Cx::new(), pre).await {
        Ok(_) => panic!("an interrupted attempt must not succeed"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    assert!(!events.has("D:start:i#0"), "the sink must not run");
}

/// A forward node that records that it ran.
struct Records {
    events: std::sync::Arc<Events>,
    name: &'static str,
}

impl gdl::Forward for Records {
    type In = Pre;
    type Out = Pre;

    async fn forward(&self, _cx: &Cx, req: Pre) -> gdl::Result<Pre> {
        self.events.record(format!("{}:ran", self.name));
        Ok(req)
    }
}

#[tokio::test]
async fn an_interruption_in_one_forward_stage_stops_the_next_one() {
    use std::sync::Arc;

    let events = Events::new();
    let hop = gdl::Hop::new(
        (
            Arc::new(CancelsAsItFinishes),
            (
                Arc::new(Records {
                    events: Arc::clone(&events),
                    name: "second",
                }),
                (),
            ),
        ),
        gdl::SinkStage::new(Arc::new(NodeD {
            events: Arc::clone(&events),
        })),
        (),
    );
    let pre = Pre {
        text: "j".into(),
        attempt: 0,
        plan: Plan {
            tokens: 1,
            ..Plan::default()
        },
    };
    assert!(hop.generate(Cx::new(), pre).await.is_err());
    assert!(
        !events.has("second:ran"),
        "the second forward stage must not run"
    );
    assert!(!events.has("D:start:j#0"), "the sink must not run");
}

/// A sink whose stream is always ready and never ends.
struct Firehose;

impl gdl::Sink for Firehose {
    type Req = Pre;
    type Item = Raw;

    async fn generate(&self, _cx: Cx, _req: Pre) -> gdl::Result<gdl::Items<Raw>> {
        Ok(gdl::items(futures::stream::repeat_with(|| Ok(Raw(0)))))
    }
}

/// A backward node that drops every item, so it never yields one.
struct DropsAll;

impl gdl::Backward for DropsAll {
    type In = Raw;
    type Out = Raw;

    fn backward(&self, _cx: &Cx, input: gdl::Items<Raw>) -> gdl::Items<Raw> {
        gdl::items(input.filter_map(|_| futures::future::ready(None)))
    }
}

/// Run `work` on its own runtime thread and wait for it with a bound: a
/// stream that never yields cannot be timed out from inside its own task.
fn within<T: Send + 'static>(
    bound: std::time::Duration,
    work: impl std::future::Future<Output = T> + Send + 'static,
) -> Option<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _ = tx.send(runtime.block_on(work));
    });
    rx.recv_timeout(bound).ok()
}

#[test]
fn a_backward_node_that_never_yields_still_ends_on_cancel() {
    use std::sync::Arc;

    let ended = within(WAIT, async {
        let hop = gdl::Hop::new(
            (),
            gdl::SinkStage::new(Arc::new(Firehose)),
            (Arc::new(DropsAll), ()),
        );
        let cx = Cx::new();
        let mut out = hop
            .generate(
                cx.clone(),
                Pre {
                    text: "f".into(),
                    attempt: 0,
                    plan: Plan::default(),
                },
            )
            .await
            .expect("attempt");
        cx.cancel();
        out.next().await.map(|r| r.map_err(|e| e.kind()))
    });
    assert_eq!(ended, Some(Some(Err(ErrorKind::Cancelled))));
}

#[test]
fn a_backward_node_that_never_yields_still_ends_at_the_deadline() {
    use std::sync::Arc;

    let ended = within(WAIT, async {
        let hop = gdl::Hop::new(
            (),
            gdl::SinkStage::new(Arc::new(Firehose)),
            (Arc::new(DropsAll), ()),
        );
        let cx = Cx::new().with_timeout(ms(20));
        let mut out = hop
            .generate(
                cx,
                Pre {
                    text: "g".into(),
                    attempt: 0,
                    plan: Plan::default(),
                },
            )
            .await
            .expect("attempt");
        out.next().await.map(|r| r.map_err(|e| e.kind()))
    });
    assert_eq!(ended, Some(Some(Err(ErrorKind::DeadlineExceeded))));
}

#[test]
fn draining_an_always_ready_attempt_does_not_starve_its_canceller() {
    use std::sync::Arc;

    let ended = within(WAIT, async {
        let hop = gdl::Hop::new((), gdl::SinkStage::new(Arc::new(Firehose)), ());
        let cx = Cx::new();
        let started = Arc::new(tokio::sync::Notify::new());
        let canceller = {
            let cx = cx.clone();
            let started = Arc::clone(&started);
            tokio::spawn(async move {
                started.notified().await;
                cx.cancel();
            })
        };
        let mut out = hop
            .generate(
                cx,
                Pre {
                    text: "q".into(),
                    attempt: 0,
                    plan: Plan::default(),
                },
            )
            .await
            .expect("attempt");
        started.notify_one();
        let mut last = None;
        while let Some(item) = out.next().await {
            if item.is_err() {
                last = Some(item.map_err(|e| e.kind()));
                break;
            }
        }
        let _ = canceller.await;
        last
    });
    assert_eq!(ended, Some(Some(Err(ErrorKind::Cancelled))));
}

/// A sink that cancels the request just before it returns its stream.
struct CancelsAsItReturns {
    root: Cx,
}

impl gdl::Sink for CancelsAsItReturns {
    type Req = Pre;
    type Item = Raw;

    async fn generate(&self, _cx: Cx, _req: Pre) -> gdl::Result<gdl::Items<Raw>> {
        self.root.cancel();
        Ok(gdl::items(futures::stream::iter([Ok(Raw(1))])))
    }
}

/// A backward node that records each time it is invoked.
struct RecordsInvocation {
    events: std::sync::Arc<Events>,
}

impl gdl::Backward for RecordsInvocation {
    type In = Raw;
    type Out = Raw;

    fn backward(&self, _cx: &Cx, input: gdl::Items<Raw>) -> gdl::Items<Raw> {
        self.events.record("backward:invoked");
        input
    }
}

#[tokio::test]
async fn an_interruption_during_the_inner_call_stops_the_backward_stages() {
    use std::sync::Arc;

    let events = Events::new();
    let root = Cx::new();
    let hop = gdl::Hop::new(
        (),
        gdl::SinkStage::new(Arc::new(CancelsAsItReturns { root: root.clone() })),
        (
            Arc::new(RecordsInvocation {
                events: Arc::clone(&events),
            }),
            (),
        ),
    );
    let result = hop
        .generate(
            root,
            Pre {
                text: "r".into(),
                attempt: 0,
                plan: Plan::default(),
            },
        )
        .await;
    assert_eq!(result.err().map(|e| e.kind()), Some(ErrorKind::Cancelled));
    assert!(
        !events.has("backward:invoked"),
        "no backward stage may start"
    );
}
