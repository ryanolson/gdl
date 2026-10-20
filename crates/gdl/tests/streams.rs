//! Streamed requests in one process: the streamed reference graph and the
//! forward nodes that turn a value into a stream and back.

mod common;

use common::streamed::{self, Brief, Note, Script, ask, bindings, replies, reply, words};
use common::{Events, WAIT, collect};
use futures::StreamExt;
use gdl::{Cx, Error, ErrorKind, Streamed, items};

#[tokio::test]
async fn a_streamed_request_flows_through_the_local_reference_graph() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    let input = words(&events, &["x", "y"], false);
    let out = graph
        .call(Cx::new(), ask("p", Script::default(), input))
        .await
        .expect("call");
    // D's count comes after its input ended: the response outlives the
    // request stream (half-close).
    assert_eq!(collect(out).await, replies("p", &["x", "y"]));
    events.wait_for("input:dropped", WAIT).await;
}

#[tokio::test]
async fn an_input_error_is_the_last_input_a_sink_sees() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    let input = streamed::words_with_error(&events, &["x"], Error::node("bad input"), &["y"]);
    let out = graph
        .call(Cx::new(), ask("p", Script::default(), input))
        .await
        .expect("call");
    let got = collect(out).await;
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(got[0], Ok(reply("c(b(p)):c(b(x))")));
    let error = got[1]
        .as_ref()
        .expect_err("the input error is the last item");
    assert_eq!(error.kind(), ErrorKind::Node);
    assert!(
        !events.has("input:sent:y"),
        "a word after the error was read: {:?}",
        events.snapshot()
    );
    // The input was dropped at its error, not when the request ended.
    assert!(events.has("input:dropped"));
}

#[tokio::test]
async fn nothing_follows_an_input_error_even_for_a_sink_that_reads_on() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    let input = streamed::words_with_error(&events, &["x"], Error::node("bad input"), &["y"]);
    let script = Script {
        read_past_error: true,
        ..Script::default()
    };
    let out = graph
        .call(Cx::new(), ask("p", script, input))
        .await
        .expect("call");
    // Across a segment the input ends at its first error; a local run must
    // give the same answer.
    assert_eq!(
        collect(out).await,
        vec![
            Ok(reply("c(b(p)):c(b(x))")),
            Ok(reply("c(b(p)):input-error:node error")),
            Ok(reply("c(b(p)):end:1")),
        ]
    );
    assert!(!events.has("input:sent:y"), "{:?}", events.snapshot());
}

#[tokio::test]
async fn a_sink_that_recovers_from_an_input_error_keeps_its_response() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    let input = streamed::words_with_error(&events, &["x"], Error::node("bad input"), &["y"]);
    let script = Script {
        recover: true,
        ..Script::default()
    };
    let out = graph
        .call(Cx::new(), ask("p", script, input))
        .await
        .expect("call");
    assert_eq!(
        collect(out).await,
        vec![
            Ok(reply("c(b(p)):c(b(x))")),
            Ok(reply("c(b(p)):input-error:node error")),
        ]
    );
    assert!(!events.has("input:sent:y"));
}

#[tokio::test]
async fn a_sink_that_finishes_early_drops_its_input() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    // The input never ends on its own.
    let input = words(&events, &["x"], true);
    let script = Script {
        take: Some(1),
        ..Script::default()
    };
    let out = graph
        .call(Cx::new(), ask("p", script, input))
        .await
        .expect("call");
    assert_eq!(
        collect(out).await,
        vec![Ok(reply("c(b(p)):c(b(x))")), Ok(reply("c(b(p)):end:1"))]
    );
    events.wait_for("input:dropped", WAIT).await;
}

#[tokio::test]
async fn dropping_the_response_drops_the_input() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    let input = words(&events, &["x"], true);
    let mut out = graph
        .call(Cx::new(), ask("p", Script::default(), input))
        .await
        .expect("call");
    assert_eq!(out.next().await, Some(Ok(reply("c(b(p)):c(b(x))"))));
    assert!(!events.has("input:dropped"));
    drop(out);
    events.wait_for("input:dropped", WAIT).await;
}

#[tokio::test]
async fn an_operator_retries_a_streamed_request_from_its_buffer() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    let input = words(&events, &["x", "y"], false);
    let script = Script {
        buffered_retry: true,
        fail_attempts: 1,
        ..Script::default()
    };
    let out = graph
        .call(Cx::new(), ask("p", script, input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, replies("p", &["x", "y"]));
    assert!(
        events.has("B:retry:0:node error"),
        "{:?}",
        events.snapshot()
    );
    assert!(events.has("D:start:c(b(p))#1"));
}

#[tokio::test]
async fn stop_reaches_a_forward_node_that_ends_its_input_on_stop() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    // The caller's input never ends: only C's stop ends what D reads.
    let input = words(&events, &["x"], true);
    let script = Script {
        stop_ends_input: true,
        ..Script::default()
    };
    let cx = Cx::new();
    let mut out = graph
        .call(cx.clone(), ask("p", script, input))
        .await
        .expect("call");
    assert_eq!(out.next().await, Some(Ok(reply("c(b(p)):c(b(x))"))));
    cx.request_stop();
    let rest = collect(out).await;
    assert_eq!(rest, vec![Ok(reply("c(b(p)):end:1"))]);
}

#[tokio::test]
async fn a_sink_may_read_its_input_before_it_returns_its_stream() {
    let events = Events::new();
    let graph = streamed::contract::local(bindings(&events));
    let input = words(&events, &["x", "y"], false);
    let script = Script {
        read_first: true,
        ..Script::default()
    };
    let out = graph
        .call(Cx::new(), ask("p", script, input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, replies("p", &["x", "y"]));
    assert!(events.has("D:read-first:c(b(p))#0"));
}

#[tokio::test]
async fn a_forward_node_turns_a_value_into_a_stream() {
    let events = Events::new();
    let graph =
        streamed::expanding::local(streamed::expanding::Bindings::new().x(streamed::NodeX).d(
            streamed::NodeD {
                events: std::sync::Arc::clone(&events),
            },
        ));
    let brief = Brief {
        text: "one two".to_string(),
        attempt: 0,
        script: Script::default(),
    };
    let out = graph.call(Cx::new(), brief).await.expect("call");
    let notes: Vec<_> = collect(out).await;
    assert_eq!(
        notes,
        vec![
            Ok(Note("one two:one".to_string())),
            Ok(Note("one two:two".to_string())),
            Ok(Note("one two:end:2".to_string())),
        ]
    );
}

#[tokio::test]
async fn a_forward_node_turns_a_stream_into_a_value() {
    let graph = streamed::collapsing::local(
        streamed::collapsing::Bindings::new()
            .y(streamed::NodeY)
            .s(streamed::NodeS),
    );
    let brief = Brief {
        text: "t".to_string(),
        attempt: 0,
        script: Script::default(),
    };
    let input = items(futures::stream::iter([
        Ok("a".to_string()),
        Ok("b".to_string()),
    ]));
    let out = graph
        .call(Cx::new(), Streamed::new(brief, input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, vec![Ok(Note("t:a+b".to_string()))]);
}
