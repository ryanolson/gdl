//! Streamed requests across segments under failure, cancellation, stop, and
//! limits. Every test ends by checking that every instance released what the
//! requests took, input anchors included.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::streamed::{self, Brief, Note, Script, ask, replies, reply, words};
use common::{Events, WAIT, collect, ms, settles, tight_velo_node, velo_node};
use futures::StreamExt;
use gdl::{Cx, Error, ErrorKind, Faults, Placement, RuntimeConfig, Streamed, items};

async fn nodes(n: usize) -> Vec<Arc<velo::Velo>> {
    let mut nodes = Vec::with_capacity(n);
    for _ in 0..n {
        nodes.push(velo_node().await);
    }
    nodes
}

async fn tight_nodes(n: usize) -> Vec<Arc<velo::Velo>> {
    let mut nodes = Vec::with_capacity(n);
    for _ in 0..n {
        nodes.push(tight_velo_node().await);
    }
    nodes
}

/// The input crosses once, into D's segment.
fn three_segments() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"])
}

/// C relays the input from `front` to `back`.
fn relayed() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "G"])
        .segment("mid", ["C"])
        .segment("back", ["D", "F", "E"])
}

fn deploy(
    nodes: &[Arc<velo::Velo>],
    placement: &Placement,
    events: &Arc<Events>,
    config: &dyn Fn(&str) -> RuntimeConfig,
) -> common::Cluster<impl gdl::Next<Streamed<streamed::Ask, String>, String> + gdl::Register + use<>>
{
    streamed::deploy_configured(
        &streamed::instances(nodes, placement),
        placement,
        events,
        config,
    )
}

fn defaults(_: &str) -> RuntimeConfig {
    RuntimeConfig::default()
}

/// `count` words named `w0`, `w1`, ...
fn many(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("w{i}")).collect()
}

fn refs(words: &[String]) -> Vec<&str> {
    words.iter().map(String::as_str).collect()
}

async fn outcome(result: gdl::Result<gdl::Items<String>>) -> Option<ErrorKind> {
    match result {
        Err(error) => Some(error.kind()),
        Ok(out) => collect(out)
            .await
            .last()
            .and_then(|r| r.as_ref().err())
            .map(Error::kind),
    }
}

// ----- cancel and lost messages -----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_streaming_reaches_the_input_and_releases_everything() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, &defaults);
    let input = words(&events, &["x"], true);
    let mut out = cluster
        .segment("front")
        .call(Cx::new(), ask("p", Script::default(), input))
        .await
        .expect("call");
    assert_eq!(out.next().await, Some(Ok(reply("c(b(p)):c(b(x))"))));
    drop(out);
    events.wait_for("input:dropped", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_setup_with_streamed_input_reaches_the_remote_sink() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, &defaults);
    // D reads its first word before it returns its stream, and none comes.
    let script = Script {
        read_first: true,
        ..Script::default()
    };
    let input = words(&events, &[], true);
    let front = cluster.segment("front");
    // Boxed so that dropping it drops the call itself.
    let mut call = Box::pin(front.call(Cx::new(), ask("p", script, input)));
    tokio::select! {
        _ = &mut call => panic!("the call returned while D was still reading"),
        () = events.wait_for("D:start:c(b(p))#0", WAIT) => {}
    }
    drop(call);
    events.wait_for("input:dropped", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_forward_ack_with_streamed_input_ends_in_one_completion_and_no_leak() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = |_: &str| RuntimeConfig {
        setup_timeout: ms(300),
        faults: Faults {
            delay_forward_ack: Some(Duration::from_secs(2)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, &config);
    let input = words(&events, &["x"], true);
    let result = cluster
        .segment("front")
        .call(Cx::new(), ask("k", Script::default(), input))
        .await;
    // The hand-off failed; the sender never learned where to send input.
    assert_eq!(result.err().map(|e| e.kind()), Some(ErrorKind::Transport));
    events.wait_for("D:start:c(b(k))#0", WAIT).await;
    events.wait_for("input:dropped", WAIT).await;
    // The receiver's input anchor, never attached, is retired with its run.
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_input_held_past_its_run_is_retired_when_the_run_ends() {
    let nodes = nodes(3).await;
    let events = Events::new();
    // The acknowledgement is lost, so the sender never attaches, and the
    // anchor's ticket pauses velo's unattached timeout. D hands the input to
    // a task of its own: only its run's end can release the anchor.
    let config = |_: &str| RuntimeConfig {
        setup_timeout: ms(300),
        faults: Faults {
            delay_forward_ack: Some(Duration::from_secs(2)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, &config);
    let script = Script {
        detach_input: true,
        ..Script::default()
    };
    let input = words(&events, &["x"], true);
    let result = cluster
        .segment("front")
        .call(Cx::new(), ask("dt", script, input))
        .await;
    assert_eq!(result.err().map(|e| e.kind()), Some(ErrorKind::Transport));
    events
        .wait_for("D:detached-input-ended:c(b(dt))#0", WAIT)
        .await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streamed_hand_off_after_its_deadline_is_refused_and_mints_nothing() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = |segment: &str| RuntimeConfig {
        faults: Faults {
            delay_forward_arrival: (segment == "engine").then(|| ms(400)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, &config);
    let input = words(&events, &["x"], true);
    let result = cluster
        .segment("front")
        .call(
            Cx::new().with_timeout(ms(150)),
            ask("l", Script::default(), input),
        )
        .await;
    assert_eq!(outcome(result).await, Some(ErrorKind::DeadlineExceeded));
    // Give the late delivery time to arrive: it must mint no anchor.
    tokio::time::sleep(ms(500)).await;
    assert_eq!(cluster.segment("engine").served().inputs, 0);
    assert!(!events.has("D:start:c(b(l))#0"));
    assert!(settles(&cluster).await);
}

// ----- failures -----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_input_sender_that_dies_ends_the_input_with_an_error() {
    let nodes = nodes(3).await;
    let mid = Arc::clone(&nodes[1]);
    let instances = vec![
        ("front".to_string(), Arc::clone(&nodes[0])),
        ("mid".to_string(), Arc::clone(&mid)),
        ("back".to_string(), Arc::clone(&nodes[2])),
    ];
    let events = Events::new();
    let config = RuntimeConfig {
        heartbeat_interval: Some(ms(100)),
        ..RuntimeConfig::default()
    };
    let cluster = streamed::deploy(&instances, &relayed(), &events, config);
    let input = words(&events, &["x"], true);
    let mut out = cluster
        .segment("front")
        .call(Cx::new(), ask("d", Script::default(), input))
        .await
        .expect("call");
    assert_eq!(out.next().await, Some(Ok(reply("c(b(d)):c(b(x))"))));
    // The relay that sends D its input goes away mid-stream. D must see an
    // error, not a clean end of input: its count would pass for success.
    mid.shutdown(velo::ShutdownPolicy::Timeout(ms(200))).await;
    let rest = tokio::time::timeout(ms(3_000), collect(out))
        .await
        .expect("the stream ends soon after the relay dies");
    let last = rest.last().expect("a terminal item");
    assert!(last.is_err(), "the input ended cleanly: {rest:?}");
    assert!(
        !rest
            .iter()
            .any(|r| r.as_ref().is_ok_and(|s| s.contains(":end:"))),
        "D counted a truncated input as complete: {rest:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receiver_that_drops_its_input_does_not_fail_the_attempt() {
    let nodes = nodes(3).await;
    let events = Events::new();
    // D finishes, and its run retires the input anchor, before the sender
    // learns where to send input.
    let config = |segment: &str| RuntimeConfig {
        faults: Faults {
            delay_forward_ack: (segment == "engine").then(|| ms(300)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, &config);
    let script = Script {
        ignore_input: true,
        ..Script::default()
    };
    let input = words(&events, &["x"], true);
    let out = cluster
        .segment("front")
        .call(Cx::new(), ask("r", script, input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, vec![Ok(reply("c(b(r)):ignored"))]);
    events.wait_for("input:dropped", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_input_error_reaches_a_remote_sink_as_in_the_local_run() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &relayed(), &events, &defaults);
    let input = streamed::words_with_error(&events, &["x"], Error::node("bad input"), &["y"]);
    let script = Script {
        read_past_error: true,
        ..Script::default()
    };
    let out = cluster
        .segment("front")
        .call(Cx::new(), ask("p", script, input))
        .await
        .expect("call");
    assert_eq!(
        collect(out).await,
        vec![
            Ok(reply("c(b(p)):c(b(x))")),
            Ok(reply("c(b(p)):input-error:node error")),
            Ok(reply("c(b(p)):end:1")),
        ]
    );
    assert!(!events.has("input:sent:y"));
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sink_that_finishes_early_stops_the_input_pump_silently() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &relayed(), &events, &defaults);
    let script = Script {
        take: Some(1),
        ..Script::default()
    };
    let input = words(&events, &["x"], true);
    let out = cluster
        .segment("front")
        .call(Cx::new(), ask("e", script, input))
        .await
        .expect("call");
    assert_eq!(
        collect(out).await,
        vec![Ok(reply("c(b(e)):c(b(x))")), Ok(reply("c(b(e)):end:1"))]
    );
    events.wait_for("input:dropped", WAIT).await;
    assert!(settles(&cluster).await);
}

// ----- stop -----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_reaches_a_relay_that_ends_its_input_on_stop() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &relayed(), &events, &defaults);
    let script = Script {
        stop_ends_input: true,
        ..Script::default()
    };
    let input = words(&events, &["x"], true);
    let cx = Cx::new();
    let mut out = cluster
        .segment("front")
        .call(cx.clone(), ask("s", script, input))
        .await
        .expect("call");
    assert_eq!(out.next().await, Some(Ok(reply("c(b(s)):c(b(x))"))));
    cx.request_stop();
    let rest = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the stop ended the input");
    // The local run's answer (`streams.rs`).
    assert_eq!(rest, vec![Ok(reply("c(b(s)):end:1"))]);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_reaches_a_relay_that_waits_for_it_before_reading_under_exhausted_credit() {
    let nodes = tight_nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &relayed(), &events, &defaults);
    let script = Script {
        wait_for_stop: true,
        ..Script::default()
    };
    // Far more ready words than the input's credit admits while C reads
    // none of them.
    let words_sent = many(64);
    let input = words(&events, &refs(&words_sent), false);
    let cx = Cx::new();
    let out = cluster
        .segment("front")
        .call(cx.clone(), ask("w", script, input))
        .await
        .expect("call");
    events.wait_for("input:sent:w3", WAIT).await;
    assert!(
        !events.has("input:sent:w63"),
        "the input was not held back by credit"
    );
    cx.request_stop();
    let rest = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the stop reached the relay through exhausted credit");
    assert_eq!(rest, vec![Ok(reply("c(b(w)):end:0"))]);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_reaches_a_waiting_relay_when_the_response_returns_to_its_own_worker() {
    // The forward path leaves at C and comes back to D on `front`, so D's
    // response anchor is on D's own worker: the stop reaches D through the
    // same-worker path, then goes up to C through exhausted input credit.
    let nodes = tight_nodes(2).await;
    let events = Events::new();
    let placement = Placement::new()
        .segment("front", ["A", "B", "D", "F", "E", "G"])
        .segment("mid", ["C"]);
    let cluster = deploy(&nodes, &placement, &events, &defaults);
    let script = Script {
        wait_for_stop: true,
        ..Script::default()
    };
    let words_sent = many(64);
    let input = words(&events, &refs(&words_sent), false);
    let cx = Cx::new();
    let out = cluster
        .segment("front")
        .call(cx.clone(), ask("sw", script, input))
        .await
        .expect("call");
    events.wait_for("input:sent:w3", WAIT).await;
    assert!(
        !events.has("input:sent:w63"),
        "the input was not held back by credit"
    );
    cx.request_stop();
    let rest = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the stop reached the relay");
    assert_eq!(rest, vec![Ok(reply("c(b(sw)):end:0"))]);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_reaches_a_relay_whose_input_has_ended() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &relayed(), &events, &defaults);
    let script = Script {
        tail_on_stop: true,
        ..Script::default()
    };
    let input = words(&events, &["x"], false);
    let cx = Cx::new();
    let mut out = cluster
        .segment("front")
        .call(cx.clone(), ask("t", script, input))
        .await
        .expect("call");
    assert_eq!(out.next().await, Some(Ok(reply("c(b(t)):c(b(x))"))));
    events.wait_for("input:dropped", WAIT).await;
    cx.request_stop();
    let rest = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the stop reached the relay after its input ended");
    assert_eq!(
        rest,
        vec![Ok(reply("c(b(t)):tail")), Ok(reply("c(b(t)):end:2"))]
    );
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_reaches_a_relay_that_turned_a_value_into_a_stream() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let placement = Placement::new()
        .segment("a", ["A"])
        .segment("x", ["X"])
        .segment("d", ["D"]);
    let held = Arc::clone(&events);
    let cluster = streamed::deploy_graph(
        &streamed::instances(&nodes, &placement),
        &streamed::expanding::manifest(),
        &placement,
        &defaults,
        move |deployment| {
            let bindings =
                streamed::expanding::Bindings::new()
                    .x(streamed::NodeX)
                    .d(streamed::NodeD {
                        events: Arc::clone(&held),
                    });
            streamed::expanding::assemble(deployment, bindings)
        },
    );
    let brief = Brief {
        text: "one two".to_string(),
        attempt: 0,
        script: Script {
            stop_ends_input: true,
            ..Script::default()
        },
    };
    let cx = Cx::new();
    let mut out = cluster
        .segment("a")
        .call(cx.clone(), brief)
        .await
        .expect("call");
    assert_eq!(out.next().await, Some(Ok(Note("one two:one".to_string()))));
    assert_eq!(out.next().await, Some(Ok(Note("one two:two".to_string()))));
    cx.request_stop();
    let rest = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the stop reached X");
    assert_eq!(rest, vec![Ok(Note("one two:end:2".to_string()))]);
    assert!(settles(&cluster).await);
}

// ----- held and stalled -----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_input_and_its_blocked_sender_are_released_at_the_deadline() {
    let nodes = tight_nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, &defaults);
    let script = Script {
        stall: true,
        ..Script::default()
    };
    let words_sent = many(64);
    let input = words(&events, &refs(&words_sent), false);
    let started = tokio::time::Instant::now();
    let out = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(800)), ask("h", script, input))
        .await
        .expect("call");
    events.wait_for("input:sent:w3", WAIT).await;
    let all = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the deadline ended the stream");
    assert_eq!(
        all.last().and_then(|r| r.as_ref().err()).map(Error::kind),
        Some(ErrorKind::DeadlineExceeded),
        "{all:?}"
    );
    assert!(
        started.elapsed() >= ms(700),
        "ended at {:?}",
        started.elapsed()
    );
    events.wait_for("input:dropped", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_input_does_not_block_another_requests_input() {
    let nodes = tight_nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, &defaults);
    let front = cluster.segment("front");
    let stalled = Script {
        stall: true,
        ..Script::default()
    };
    let words_sent = many(64);
    let input = words(&events, &refs(&words_sent), false);
    let held = front
        .call(Cx::new(), ask("st", stalled, input))
        .await
        .expect("stalled call");
    events.wait_for("input:sent:w3", WAIT).await;
    let other = Events::new();
    let input = words(&other, &["x", "y"], false);
    let out = front
        .call(Cx::new(), ask("ok", Script::default(), input))
        .await
        .expect("second call");
    let got = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the second request was not held up by the first");
    assert_eq!(got, replies("ok", &["x", "y"]));
    drop(held);
    assert!(settles(&cluster).await);
}

// ----- admission and egress -----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_holds_its_admission_until_its_relay_ends() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = |segment: &str| RuntimeConfig {
        admission_limit: if segment == "mid" { 1 } else { 1024 },
        admission_wait: ms(50),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &relayed(), &events, &config);
    let front = cluster.segment("front");
    let input = words(&events, &["x"], true);
    let mut first = front
        .call(Cx::new(), ask("q1", Script::default(), input))
        .await
        .expect("first");
    assert_eq!(first.next().await, Some(Ok(reply("c(b(q1)):c(b(x))"))));
    // The relay has handed the request on, and still holds its share.
    assert_eq!(cluster.segment("mid").admitted(), 1);
    let other = Events::new();
    let refused = front
        .call(
            Cx::new(),
            ask("q2", Script::default(), words(&other, &["y"], false)),
        )
        .await;
    assert_eq!(outcome(refused).await, Some(ErrorKind::Overloaded));
    drop(first);
    assert!(settles(&cluster).await);
    let out = front
        .call(
            Cx::new(),
            ask("q3", Script::default(), words(&other, &["z"], false)),
        )
        .await
        .expect("third");
    assert_eq!(collect(out).await, replies("q3", &["z"]));
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_input_sender_at_its_egress_quota_is_overloaded() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = |segment: &str| RuntimeConfig {
        egress_limit: if segment == "front" { 1 } else { 1024 },
        admission_wait: ms(50),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, &config);
    let front = cluster.segment("front");
    let input = words(&events, &["x"], true);
    let mut first = front
        .call(Cx::new(), ask("e1", Script::default(), input))
        .await
        .expect("first");
    assert!(first.next().await.is_some_and(|item| item.is_ok()));
    assert_eq!(front.egress_in_use(), 1, "the input pump holds the permit");
    let other = Events::new();
    let refused = front
        .call(
            Cx::new(),
            ask("e2", Script::default(), words(&other, &["y"], false)),
        )
        .await;
    assert_eq!(outcome(refused).await, Some(ErrorKind::Overloaded));
    // Refused before the hand-off: the engine never minted an anchor for it.
    assert_eq!(cluster.segment("engine").served().inputs, 1);
    drop(first);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receiver_that_refuses_egress_mints_no_input_anchor() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = |segment: &str| RuntimeConfig {
        egress_limit: if segment == "engine" { 1 } else { 1024 },
        admission_wait: ms(50),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, &config);
    let front = cluster.segment("front");
    let input = words(&events, &["x"], true);
    let mut first = front
        .call(Cx::new(), ask("g1", Script::default(), input))
        .await
        .expect("first");
    assert!(first.next().await.is_some_and(|item| item.is_ok()));
    let engine = cluster.segment("engine");
    let anchors = engine.active_anchors();
    let other = Events::new();
    let refused = front
        .call(
            Cx::new(),
            ask("g2", Script::default(), words(&other, &["y"], false)),
        )
        .await;
    assert_eq!(outcome(refused).await, Some(ErrorKind::Overloaded));
    assert_eq!(engine.served().inputs, 1);
    assert_eq!(engine.active_anchors(), anchors);
    drop(first);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn egress_hold_and_wait_at_the_limit_is_refused_not_hung() {
    let nodes = nodes(2).await;
    let events = Events::new();
    // The forward path leaves at C and returns to D on `front`. The outer
    // end holds its input pump's permit while D, on the same instance,
    // waits for another (ruling 29).
    let placement = Placement::new()
        .segment("front", ["A", "B", "D", "F", "E", "G"])
        .segment("mid", ["C"]);
    let config = |segment: &str| RuntimeConfig {
        egress_limit: if segment == "front" { 1 } else { 1024 },
        admission_wait: ms(50),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &placement, &events, &config);
    // The input stays open, so the input pump keeps its permit.
    let input = words(&events, &["x"], true);
    let result = tokio::time::timeout(
        WAIT,
        cluster
            .segment("front")
            .call(Cx::new(), ask("hw", Script::default(), input)),
    )
    .await
    .expect("refused, not hung");
    assert_eq!(outcome(result).await, Some(ErrorKind::Overloaded));
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_limit_of_one_serves_a_streamed_request_that_enters_a_segment_twice() {
    let nodes = nodes(2).await;
    let events = Events::new();
    let placement = Placement::new()
        .segment("front", ["A", "B", "D", "G"])
        .segment("side", ["C", "F", "E"]);
    let config = |_: &str| RuntimeConfig {
        admission_limit: 1,
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &placement, &events, &config);
    let input = words(&events, &["x", "y"], false);
    let out = cluster
        .segment("front")
        .call(Cx::new(), ask("a1", Script::default(), input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, replies("a1", &["x", "y"]));
    assert!(settles(&cluster).await);
}

// ----- ordering and deadlock -----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sink_that_reads_its_input_before_returning_its_stream_does_not_deadlock() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, &defaults);
    let script = Script {
        read_first: true,
        ..Script::default()
    };
    let input = words(&events, &["x", "y"], false);
    let out = tokio::time::timeout(
        WAIT,
        cluster
            .segment("front")
            .call(Cx::new(), ask("rf", script, input)),
    )
    .await
    .expect("the input pump started before the open signal")
    .expect("call");
    assert_eq!(collect(out).await, replies("rf", &["x", "y"]));
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_credit_deadlock_between_input_and_response_ends_at_the_deadline() {
    let nodes = tight_nodes(2).await;
    let events = Events::new();
    let placement = Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("back", ["D", "F", "E"]);
    let cluster = deploy(&nodes, &placement, &events, &defaults);
    // D sends many notes before it reads; the caller reads nothing until its
    // input is all sent. Each waits for the other's credit. Both directions
    // carry more items than an anchor buffers (256 frames) plus its credit.
    let script = Script {
        emit_first: 1024,
        ..Script::default()
    };
    let words_sent = many(1024);
    let input = words(&events, &refs(&words_sent), false);
    let started = tokio::time::Instant::now();
    let out = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(800)), ask("dl", script, input))
        .await
        .expect("call");
    // Past the deadline: nothing but the deadline can end the wait.
    let sent_all = events.wait_for_any(&["input:sent:w1023"], ms(1_200));
    let waited = std::panic::AssertUnwindSafe(sent_all);
    assert!(
        futures::FutureExt::catch_unwind(waited).await.is_err(),
        "no deadlock: the input was all sent"
    );
    let all = collect(out).await;
    assert_eq!(
        all.last().and_then(|r| r.as_ref().err()).map(Error::kind),
        Some(ErrorKind::DeadlineExceeded)
    );
    assert!(started.elapsed() < ms(800) + WAIT);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streamed_request_whose_forward_path_returns_to_its_outer_segment() {
    let nodes = nodes(2).await;
    let events = Events::new();
    let placement = Placement::new()
        .segment("front", ["A", "B", "D", "F", "E", "G"])
        .segment("mid", ["C"]);
    let cluster = deploy(&nodes, &placement, &events, &defaults);
    let input = words(&events, &["x", "y"], false);
    let out = cluster
        .segment("front")
        .call(Cx::new(), ask("rt", Script::default(), input))
        .await
        .expect("call");
    assert_eq!(collect(out).await, replies("rt", &["x", "y"]));
    let front = cluster.segment("front").served();
    assert_eq!(
        front.inputs, 1,
        "D's input anchor, minted on front: {front:?}"
    );
    assert!(settles(&cluster).await);
}

// ----- codec -----

mod odd {
    use serde::{Deserialize, Serialize, Serializer};

    /// A number that refuses to encode when it is 13.
    #[derive(Debug, Deserialize)]
    pub struct Odd(pub u32);

    impl Serialize for Odd {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.0 == 13 {
                return Err(serde::ser::Error::custom("thirteen"));
            }
            serializer.serialize_u32(self.0)
        }
    }

    gdl::graph! {
        name = "odd", schema = 1;
        source A: stream Odd -> stream u32;
        sink   S: stream Odd -> stream u32;
        topology: A => S;
    }
}

/// Answers each number with itself, and an input error with that error.
struct Echo;

impl gdl::Sink for Echo {
    type Req = Streamed<(), odd::Odd>;
    type Item = u32;

    async fn generate(&self, _cx: Cx, req: Self::Req) -> gdl::Result<gdl::Items<u32>> {
        let (_, input) = req.into_parts();
        Ok(items(input.map(|odd| odd.map(|odd::Odd(n)| n))))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_input_item_that_fails_to_encode_ends_the_input_with_a_codec_error() {
    let nodes = nodes(2).await;
    let placement = Placement::new().segment("a", ["A"]).segment("s", ["S"]);
    let cluster = streamed::deploy_graph(
        &streamed::instances(&nodes, &placement),
        &odd::manifest(),
        &placement,
        &defaults,
        |deployment| odd::assemble(deployment, odd::Bindings::new().s(Echo)),
    );
    let input = items(futures::stream::iter([
        Ok(odd::Odd(1)),
        Ok(odd::Odd(13)),
        Ok(odd::Odd(2)),
    ]));
    let out = cluster
        .segment("a")
        .call(Cx::new(), Streamed::new((), input))
        .await
        .expect("call");
    let got = collect(out).await;
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(got[0], Ok(1));
    assert_eq!(
        got[1].as_ref().err().map(Error::kind),
        Some(ErrorKind::Codec)
    );
    assert!(settles(&cluster).await);
}

// ----- tasks -----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streamed_requests_leave_no_tasks_behind() {
    let nodes = nodes(3).await;
    let events = Events::quiet();
    let cluster = deploy(&nodes, &relayed(), &events, &defaults);
    let run = |tag: String| {
        let front = cluster.segment("front");
        let events = Arc::clone(&events);
        async move {
            let input = words(&events, &["x", "y"], false);
            let out = front
                .call(Cx::new(), ask(&tag, Script::default(), input))
                .await
                .expect("call");
            let all = collect(out).await;
            assert!(all.iter().all(Result::is_ok), "{all:?}");
        }
    };
    let metrics = tokio::runtime::Handle::current().metrics();
    let steady = || async {
        let mut last = metrics.num_alive_tasks();
        loop {
            tokio::time::sleep(ms(100)).await;
            let now = metrics.num_alive_tasks();
            if now == last {
                return now;
            }
            last = now;
        }
    };
    for i in 0..200 {
        run(format!("warm{i}")).await;
    }
    assert!(settles(&cluster).await);
    let baseline = steady().await;
    for i in 0..200 {
        run(format!("t{i}")).await;
    }
    assert!(settles(&cluster).await);
    let deadline = tokio::time::Instant::now() + WAIT;
    while metrics.num_alive_tasks() > baseline {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} tasks alive, {baseline} after the first batch of 200",
            metrics.num_alive_tasks()
        );
        tokio::time::sleep(ms(10)).await;
    }
}

// ----- a stop a node asks for -----

mod stopping {
    gdl::graph! {
        name = "stopping", schema = 1;
        source  A: stream u32 -> stream u32;
        forward X: stream u32 -> stream u32;
        forward C: stream u32 -> stream u32;
        sink    D: stream u32 -> stream u32;
        topology: A => X => C => D;
    }
}

/// Ends the input it passes on when its context is asked to stop.
struct UntilStop;

impl gdl::Forward for UntilStop {
    type In = Streamed<(), u32>;
    type Out = Streamed<(), u32>;

    async fn forward(&self, cx: &Cx, req: Self::In) -> gdl::Result<Self::Out> {
        let (_, input) = req.into_parts();
        let cx = cx.clone();
        let input = input.take_until(async move { cx.stopping().await });
        Ok(Streamed::new((), items(input)))
    }
}

/// Asks its own context to stop as it passes the first item on.
struct StopAfterFirst;

impl gdl::Forward for StopAfterFirst {
    type In = Streamed<(), u32>;
    type Out = Streamed<(), u32>;

    async fn forward(&self, cx: &Cx, req: Self::In) -> gdl::Result<Self::Out> {
        let (_, input) = req.into_parts();
        let cx = cx.clone();
        let input = input.map(move |n| {
            cx.request_stop();
            n
        });
        Ok(Streamed::new((), items(input)))
    }
}

/// Answers with its input.
struct Drain;

impl gdl::Sink for Drain {
    type Req = Streamed<(), u32>;
    type Item = u32;

    async fn generate(&self, _cx: Cx, req: Self::Req) -> gdl::Result<gdl::Items<u32>> {
        Ok(req.into_parts().1)
    }
}

/// Drains its input, then waits for a stop before it answers with the count.
struct DrainThenAwaitStop;

impl gdl::Sink for DrainThenAwaitStop {
    type Req = Streamed<(), u32>;
    type Item = u32;

    async fn generate(&self, cx: Cx, req: Self::Req) -> gdl::Result<gdl::Items<u32>> {
        let (_, input) = req.into_parts();
        let counted = async move {
            let count = input.count().await as u32;
            cx.stopping().await;
            Ok(count)
        };
        Ok(items(futures::stream::once(counted)))
    }
}

async fn stop_from_a_node(placement: Placement) {
    let nodes = nodes(placement.segments().count()).await;
    let cluster = streamed::deploy_graph(
        &streamed::instances(&nodes, &placement),
        &stopping::manifest(),
        &placement,
        &defaults,
        |deployment| {
            let bindings = stopping::Bindings::new()
                .x(UntilStop)
                .c(StopAfterFirst)
                .d(Drain);
            stopping::assemble(deployment, bindings)
        },
    );
    // One item, then an input that never ends: only the stop C asks for,
    // reaching X, ends it.
    let input = items(futures::stream::iter([Ok(1)]).chain(futures::stream::pending()));
    let out = cluster
        .segment("a")
        .call(Cx::new().with_timeout(ms(3_000)), Streamed::new((), input))
        .await
        .expect("call");
    let got = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the stream ended");
    assert_eq!(got, vec![Ok(1)], "{placement:?}");
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_a_node_asks_for_reaches_the_stage_before_it_locally() {
    stop_from_a_node(Placement::new().segment("a", ["A", "X", "C", "D"])).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_a_node_asks_for_reaches_the_stage_before_it_across_segments() {
    let placement = Placement::new()
        .segment("a", ["A"])
        .segment("x", ["X"])
        .segment("c", ["C", "D"]);
    stop_from_a_node(placement).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_a_node_asks_for_reaches_a_forward_node_in_the_outer_segment() {
    let placement = Placement::new()
        .segment("a", ["A", "X"])
        .segment("c", ["C", "D"]);
    stop_from_a_node(placement).await;
}

async fn stop_from_a_relay(placement: Placement) -> Vec<gdl::Result<u32>> {
    let nodes = nodes(placement.segments().count()).await;
    let cluster = streamed::deploy_graph(
        &streamed::instances(&nodes, &placement),
        &stopping::manifest(),
        &placement,
        &defaults,
        |deployment| {
            let bindings = stopping::Bindings::new()
                .x(UntilStop)
                .c(StopAfterFirst)
                .d(DrainThenAwaitStop);
            stopping::assemble(deployment, bindings)
        },
    );
    // A finite input: the stop C asks for must reach D after the input is
    // over, with no input sender left upstream to carry it.
    let input = items(futures::stream::iter([Ok(1)]));
    let out = cluster
        .segment("a")
        .call(Cx::new().with_timeout(ms(3_000)), Streamed::new((), input))
        .await
        .expect("call");
    let got = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the stream ended");
    assert!(settles(&cluster).await);
    got
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_a_relay_asks_for_reaches_the_sink_locally() {
    let got = stop_from_a_relay(Placement::new().segment("a", ["A", "X", "C", "D"])).await;
    assert_eq!(got, vec![Ok(1)]);
}

/// The accepted limit of ruling 26: once a relay's own input has ended, no
/// stage is left upstream to carry a stop it asks for, and velo gives a
/// producer no way to signal its consumer outside the data. The stop does
/// not reach the sink; the deadline ends the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_a_relay_asks_for_after_its_input_ended_waits_for_the_deadline() {
    let placement = Placement::new()
        .segment("a", ["A"])
        .segment("x", ["X", "C"])
        .segment("d", ["D"]);
    let got = stop_from_a_relay(placement).await;
    let kinds: Vec<_> = got
        .iter()
        .map(|r| r.as_ref().map_err(Error::kind))
        .collect();
    assert_eq!(kinds, vec![Err(ErrorKind::DeadlineExceeded)]);
}
