//! The reference graph with every node in one segment.

mod common;

use common::*;
use futures::StreamExt;
use gdl::{Cx, ErrorKind};

#[tokio::test]
async fn items_pass_through_every_stage_in_order() {
    let events = Events::new();
    let graph = local_graph(&events);
    let out = graph
        .call(
            Cx::new(),
            prompt(
                "p",
                Plan {
                    tokens: 3,
                    ..Plan::default()
                },
            ),
        )
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
async fn dropping_one_concurrent_attempt_leaves_the_other_running() {
    let events = Events::new();
    let graph = local_graph(&events);
    let plan = Plan {
        tokens: 3,
        item_delay_ms: 5,
        slow_attempt: Some((1, 60_000)),
        mode: Mode::Hedge,
        ..Plan::default()
    };
    let out = graph
        .call(Cx::new(), prompt("h", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..3));
    assert!(events.has("B:hedge:winner:0"));
    // The losing attempt was cancelled; the winner ran to completion.
    events.wait_for("D:cancelled:c(h)#1", WAIT).await;
    events.wait_for("D:done:c(h)#0", WAIT).await;
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
    events.wait_for("D:cancelled:c(a)#0", WAIT).await;
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
async fn stop_delivers_the_buffered_tail_then_ends_cleanly() {
    let events = Events::new();
    let graph = local_graph(&events);
    let cx = Cx::new();
    let plan = Plan {
        endless: true,
        item_delay_ms: 1,
        ..Plan::default()
    };
    let mut out = graph
        .call(cx.clone(), prompt("s", plan))
        .await
        .expect("call");
    assert!(out.next().await.is_some_and(|item| item.is_ok()));
    cx.request_stop();
    let rest = collect(out).await;
    // Every remaining item is a success, and the last is D's tail.
    assert!(rest.iter().all(|item| item.is_ok()), "{rest:?}");
    assert_eq!(rest.last(), Some(&Ok(format!("g(b(e(f{STOP_MARK})))"))));
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
    events.wait_for("D:cancelled:c(x)#0", WAIT).await;
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
    // Ending the stream after the error cancelled D, which was still trying
    // to emit.
    events.wait_for("D:cancelled:c(e)#0", WAIT).await;
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
