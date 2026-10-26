//! Streamed requests across segments under failure, cancellation, stop, and
//! limits. Every test ends by checking that every instance released what the
//! requests took, input anchors included.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::streamed::{self, Brief, Note, Script, ask, replies, reply, words};
use common::{Events, TIGHT_SLOT_BYTES, WAIT, collect, ms, settles, tight_velo_node, velo_node};
use futures::StreamExt;
use gdl::{Cx, Error, ErrorKind, Streamed, items};
use gdl_runtime::{Faults, Placement, RuntimeConfig};

async fn nodes(n: usize) -> Vec<velo::Velo> {
    let mut nodes = Vec::with_capacity(n);
    for _ in 0..n {
        nodes.push(velo_node().await);
    }
    nodes
}

async fn tight_nodes(n: usize) -> Vec<velo::Velo> {
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
    nodes: &[velo::Velo],
    placement: &Placement,
    events: &Arc<Events>,
    config: &dyn Fn(&str) -> RuntimeConfig,
) -> common::Cluster<
    impl gdl::Next<Streamed<streamed::Ask, String>, String> + gdl_runtime::Register + use<>,
> {
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

/// More words than a tight node's stream slot holds, whatever a word's
/// frame costs: every frame is at least a byte, so these fill the slot's
/// byte budget twice over.
fn past_credit() -> Vec<String> {
    many(2 * TIGHT_SLOT_BYTES as usize)
}

/// Whether the last of `words` was sent.
fn last_sent(events: &Events, words: &[String]) -> bool {
    let last = words.last().expect("words");
    events.has(&format!("input:sent:{last}"))
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
    // The hand-off failed, and may have run, as it did; the sender never
    // learned where to send input.
    assert_eq!(
        result.err().map(|e| e.kind()),
        Some(ErrorKind::OutcomeUnknown)
    );
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
    assert_eq!(
        result.err().map(|e| e.kind()),
        Some(ErrorKind::OutcomeUnknown)
    );
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
    let mid = nodes[1].clone();
    let instances = vec![
        ("front".to_string(), nodes[0].clone()),
        ("mid".to_string(), mid.clone()),
        ("back".to_string(), nodes[2].clone()),
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
    let words_sent = past_credit();
    let input = words(&events, &refs(&words_sent), false);
    let cx = Cx::new();
    let out = cluster
        .segment("front")
        .call(cx.clone(), ask("w", script, input))
        .await
        .expect("call");
    events.wait_for("input:sent:w3", WAIT).await;
    assert!(
        !last_sent(&events, &words_sent),
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
    let words_sent = past_credit();
    let input = words(&events, &refs(&words_sent), false);
    let cx = Cx::new();
    let out = cluster
        .segment("front")
        .call(cx.clone(), ask("sw", script, input))
        .await
        .expect("call");
    events.wait_for("input:sent:w3", WAIT).await;
    assert!(
        !last_sent(&events, &words_sent),
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
    let words_sent = past_credit();
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
    let words_sent = past_credit();
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
    // waits for another.
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

    gdl_runtime::graph! {
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
    // velo starts some of its own long-lived tasks lazily (stream workers,
    // ordered message lanes), sometimes only during the second batch, so a
    // steady count is not a full one. Those are few and bounded; a task kept
    // per request would add 200.
    const VELO_LAZY: usize = 16;
    let deadline = tokio::time::Instant::now() + WAIT;
    while metrics.num_alive_tasks() > baseline + VELO_LAZY {
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
    gdl_runtime::graph! {
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

/// The accepted limit on a relay's stop: once a relay's own input has ended, no
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

// ----- a receiver that drops its input while its sender is blocked -----

/// Reads one number, then holds its input unread until `drop_input`, drops
/// it, and answers 42 on `respond`, or 0 if it is asked to stop first.
struct DropBlockedInput {
    drop_input: Arc<tokio::sync::Notify>,
    respond: Arc<tokio::sync::Notify>,
    events: Arc<Events>,
}

impl gdl::Sink for DropBlockedInput {
    type Req = Streamed<(), odd::Odd>;
    type Item = u32;

    async fn generate(&self, cx: Cx, req: Self::Req) -> gdl::Result<gdl::Items<u32>> {
        let (_, mut input) = req.into_parts();
        assert!(input.next().await.is_some_and(|n| n.is_ok()));
        let (drop_input, respond) = (Arc::clone(&self.drop_input), Arc::clone(&self.respond));
        let events = Arc::clone(&self.events);
        let answer = async move {
            drop_input.notified().await;
            drop(input);
            events.record("S:dropped-input");
            tokio::select! {
                () = cx.stopping() => Ok(0),
                () = respond.notified() => Ok(42),
            }
        };
        Ok(items(futures::stream::once(answer)))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_an_input_whose_sender_is_blocked_does_not_stop_the_response() {
    // `a` hosts the input's sender, and counts when it waits for credit.
    let (sender, registry) = common::metered_tight_velo_node().await;
    let nodes = vec![sender, tight_velo_node().await];
    let placement = Placement::new().segment("a", ["A"]).segment("s", ["S"]);
    let events = Events::new();
    let (drop_input, respond) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let sink = |events: &Arc<Events>| DropBlockedInput {
        drop_input: Arc::clone(&drop_input),
        respond: Arc::clone(&respond),
        events: Arc::clone(events),
    };
    let held = Arc::clone(&events);
    let cluster = streamed::deploy_graph(
        &streamed::instances(&nodes, &placement),
        &odd::manifest(),
        &placement,
        &defaults,
        move |deployment| odd::assemble(deployment, odd::Bindings::new().s(sink(&held))),
    );
    // Far more numbers than the input's credit admits while S reads none.
    let sent = Arc::clone(&events);
    let input = items(futures::stream::iter(0..4096u32).map(move |n| {
        sent.record(format!("input:sent:{n}"));
        Ok(odd::Odd(n + 20))
    }));
    let a = cluster.segment("a");
    let mut out = a
        .call(Cx::new(), Streamed::new((), input))
        .await
        .expect("call");
    // The pump's send waits for credit: the case where a cancel and a stop
    // arrive together.
    tokio::time::timeout(WAIT, async {
        while common::backpressured(&registry) == 0.0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the input's sender waited for credit");
    assert!(
        !events.has("input:sent:4095"),
        "the input was not held back"
    );
    assert_eq!(a.egress_in_use(), 1, "the input pump is running");
    // The receiver drops its input while the pump waits for credit: velo
    // cancels the pump, and fires its stop token as part of that cancel.
    drop_input.notify_one();
    events.wait_for("S:dropped-input", WAIT).await;
    let deadline = tokio::time::Instant::now() + WAIT;
    while a.egress_in_use() > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pump did not end"
        );
        tokio::time::sleep(ms(5)).await;
    }
    // A stop taken for the cancel would reach S within this window.
    tokio::time::sleep(ms(200)).await;
    respond.notify_one();
    assert_eq!(out.next().await, Some(Ok(42)));
    assert_eq!(out.next().await, None);
    assert!(settles(&cluster).await);
}

// ----- a relay whose receiver drops its input -----

mod teardown {
    gdl_runtime::graph! {
        name = "teardown", schema = 1;
        source  A: stream u32 -> stream u32;
        forward X: stream u32 -> stream u32;
        sink    D: stream u32 -> stream u32;
        topology: A => X => D;
    }
}

/// Passes its input on, after registering many waiters on its context's
/// cancel: they widen the window between a cancel of the relay's context
/// and the cancel of its input anchor, in which a stop watcher that took
/// the cancel for a stop would send one upstream.
struct CrowdedRelay;

impl gdl::Forward for CrowdedRelay {
    type In = Streamed<(), u32>;
    type Out = Streamed<(), u32>;

    async fn forward(&self, cx: &Cx, req: Self::In) -> gdl::Result<Self::Out> {
        let mut registered = Vec::new();
        for _ in 0..200 {
            let cx = cx.clone();
            let (ready, wait) = tokio::sync::oneshot::channel();
            registered.push(wait);
            tokio::spawn(async move {
                let cancelled = cx.cancelled();
                tokio::pin!(cancelled);
                assert!(futures::poll!(&mut cancelled).is_pending());
                let _ = ready.send(());
                cancelled.await;
            });
        }
        for ready in registered {
            ready.await.expect("registered");
        }
        Ok(req)
    }
}

/// Reads one number, drops its input on `drop_input`, and answers 42 on
/// `respond`, or 0 if it is asked to stop first.
struct DropRelayedInput {
    drop_input: Arc<tokio::sync::Notify>,
    respond: Arc<tokio::sync::Notify>,
}

impl gdl::Sink for DropRelayedInput {
    type Req = Streamed<(), u32>;
    type Item = u32;

    async fn generate(&self, cx: Cx, req: Self::Req) -> gdl::Result<gdl::Items<u32>> {
        let (_, mut input) = req.into_parts();
        assert_eq!(input.next().await, Some(Ok(1)));
        let (drop_input, respond) = (Arc::clone(&self.drop_input), Arc::clone(&self.respond));
        Ok(items(futures::stream::once(async move {
            drop_input.notified().await;
            drop(input);
            tokio::select! {
                biased;
                () = cx.stopping() => Ok(0),
                () = respond.notified() => Ok(42),
            }
        })))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_relay_whose_receiver_drops_its_input_does_not_stop_the_response() {
    let nodes = nodes(3).await;
    let placement = Placement::new()
        .segment("a", ["A"])
        .segment("x", ["X"])
        .segment("d", ["D"]);
    let (drop_input, respond) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let sink = (Arc::clone(&drop_input), Arc::clone(&respond));
    let cluster = streamed::deploy_graph(
        &streamed::instances(&nodes, &placement),
        &teardown::manifest(),
        &placement,
        &defaults,
        move |deployment| {
            let d = DropRelayedInput {
                drop_input: Arc::clone(&sink.0),
                respond: Arc::clone(&sink.1),
            };
            teardown::assemble(deployment, teardown::Bindings::new().x(CrowdedRelay).d(d))
        },
    );
    // The race is narrow: many requests give it many chances.
    for iteration in 0..300 {
        let input = items(futures::stream::iter([Ok(1)]).chain(futures::stream::pending()));
        let mut out = cluster
            .segment("a")
            .call(Cx::new().with_timeout(WAIT), Streamed::new((), input))
            .await
            .expect("call");
        drop_input.notify_one();
        let early = tokio::time::timeout(ms(10), out.next()).await;
        assert!(
            early.is_err(),
            "request {iteration}: the input's teardown ended the response: {early:?}"
        );
        respond.notify_one();
        assert_eq!(out.next().await, Some(Ok(42)), "request {iteration}");
        assert_eq!(out.next().await, None);
    }
    assert!(settles(&cluster).await);
}

/// Asks its own context to stop as it passes the first item on, and ends
/// its output there.
struct StopAndFinish;

impl gdl::Forward for StopAndFinish {
    type In = Streamed<(), u32>;
    type Out = Streamed<(), u32>;

    async fn forward(&self, cx: &Cx, req: Self::In) -> gdl::Result<Self::Out> {
        let (_, input) = req.into_parts();
        let cx = cx.clone();
        let input = input
            .map(move |n| {
                cx.request_stop();
                n
            })
            .take(1);
        Ok(Streamed::new((), items(input)))
    }
}

async fn stop_as_a_relay_finishes(placement: Placement) -> Vec<gdl::Result<u32>> {
    let nodes = nodes(placement.segments().count()).await;
    let cluster = streamed::deploy_graph(
        &streamed::instances(&nodes, &placement),
        &stopping::manifest(),
        &placement,
        &defaults,
        |deployment| {
            let bindings = stopping::Bindings::new()
                .x(UntilStop)
                .c(StopAndFinish)
                .d(DrainThenAwaitStop);
            stopping::assemble(deployment, bindings)
        },
    );
    // The source's input stays open: C asks for a stop while its sender is
    // live, then ends its own output at once.
    let input = items(futures::stream::iter([Ok(1)]).chain(futures::stream::pending()));
    let out = cluster
        .segment("a")
        .call(Cx::new().with_timeout(ms(1_000)), Streamed::new((), input))
        .await
        .expect("call");
    let got = tokio::time::timeout(WAIT, collect(out))
        .await
        .expect("the stream ended");
    assert!(settles(&cluster).await);
    got
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_a_relay_asks_for_as_it_finishes_reaches_the_sink_locally() {
    let got = stop_as_a_relay_finishes(Placement::new().segment("a", ["A", "X", "C", "D"])).await;
    assert_eq!(got, vec![Ok(1)]);
}

/// The same limit as `..._after_its_input_ended_...`: a relay's stop goes
/// up through its input only while its run lives. When the run ends right
/// after the stop, retiring its input cancels its sender, and velo fires
/// the sender's stop token with that cancel, so the sender takes both for
/// the receiver leaving. The stop does not reach the sink; the deadline
/// ends the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_a_relay_asks_for_as_it_finishes_waits_for_the_deadline() {
    let placement = Placement::new()
        .segment("a", ["A", "X"])
        .segment("c", ["C"])
        .segment("d", ["D"]);
    let got = stop_as_a_relay_finishes(placement).await;
    let kinds: Vec<_> = got
        .iter()
        .map(|r| r.as_ref().map_err(Error::kind))
        .collect();
    assert_eq!(kinds, vec![Err(ErrorKind::DeadlineExceeded)]);
}

/// A streamed hand-off that a full engine declines is offered to the
/// other engine, and its input goes to the anchor the other engine
/// minted: every word reaches it, and the engine that declined minted
/// none for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declined_streamed_hand_off_sends_its_input_where_it_was_taken() {
    let nodes = nodes(4).await;
    let instances: Vec<(String, velo::Velo)> = ["front", "engine", "engine", "post"]
        .into_iter()
        .map(String::from)
        .zip(nodes.iter().cloned())
        .collect();
    let events = Events::new();
    // Each engine admits one request, and refuses at once when full.
    let config = |segment: &str| match segment {
        "engine" => RuntimeConfig {
            admission_limit: 1,
            admission_wait: Duration::ZERO,
            ..RuntimeConfig::default()
        },
        _ => RuntimeConfig::default(),
    };
    let cluster = streamed::deploy_configured(&instances, &three_segments(), &events, &config);
    let front = cluster.segment("front");
    let engines = || {
        cluster
            .segments
            .iter()
            .filter(|(name, _)| name == "engine")
            .map(|(_, segment)| (segment.served(), segment.load().declined))
            .collect::<Vec<_>>()
    };
    // Round robin: the first engine holds a request whose input stays
    // open; the second takes one that ends.
    let mut held = front
        .call(
            Cx::new(),
            ask("held", Script::default(), words(&events, &["h"], true)),
        )
        .await
        .expect("routed");
    assert_eq!(
        held.next().await.expect("an item"),
        replies("held", &["h"]).remove(0)
    );
    let out = front
        .call(
            Cx::new(),
            ask("next", Script::default(), words(&events, &["n"], false)),
        )
        .await
        .expect("routed");
    assert_eq!(collect(out).await, replies("next", &["n"]));
    let before = engines();
    assert_eq!(
        (before[0].0.inputs, before[1].0.inputs),
        (1, 1),
        "the turns were not as expected"
    );

    // The first engine's turn: it is full, declines, and the second takes
    // the request and its input.
    let out = front
        .call(
            Cx::new(),
            ask(
                "offered",
                Script::default(),
                words(&events, &["x", "y", "z"], false),
            ),
        )
        .await
        .expect("the second engine took it");
    assert_eq!(collect(out).await, replies("offered", &["x", "y", "z"]));
    let after = engines();
    assert_eq!(after[0].1, 1, "the first engine did not decline");
    assert_eq!(
        (after[0].0.forwards, after[0].0.inputs),
        (before[0].0.forwards, before[0].0.inputs),
        "the first engine took the hand-off or minted an input for it"
    );
    assert_eq!(
        (after[1].0.forwards, after[1].0.inputs),
        (before[1].0.forwards + 1, before[1].0.inputs + 1),
    );
    drop(held);
    assert!(settles(&cluster).await);
}
