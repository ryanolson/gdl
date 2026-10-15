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
