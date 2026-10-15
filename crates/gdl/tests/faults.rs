//! Failure, cancellation, and limits across segments. Every test ends by
//! checking that every instance released what the requests took.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use futures::StreamExt;
use gdl::{Cx, ErrorKind, Faults, Placement, RuntimeConfig};

async fn nodes(n: usize) -> Vec<Arc<velo::Velo>> {
    let mut nodes = Vec::with_capacity(n);
    for _ in 0..n {
        nodes.push(velo_node().await);
    }
    nodes
}

/// The placement from the design discussion: the response comes back through
/// a third segment.
fn three_segments() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"])
}

fn kinds(items: &[gdl::Result<String>]) -> Vec<Result<String, ErrorKind>> {
    items
        .iter()
        .map(|r| r.clone().map_err(|e| e.kind()))
        .collect()
}

fn last_kind(items: &[gdl::Result<String>]) -> Option<ErrorKind> {
    items
        .last()
        .and_then(|r| r.as_ref().err())
        .map(|e| e.kind())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_streaming_reaches_the_remote_sink_and_releases_everything() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, RuntimeConfig::default());
    let plan = Plan {
        endless: true,
        item_delay_ms: 2,
        ..Plan::default()
    };
    let mut out = cluster
        .segment("front")
        .call(Cx::new(), prompt("s", plan))
        .await
        .expect("call");
    assert!(out.next().await.is_some_and(|item| item.is_ok()));
    drop(out);
    events.wait_for("D:cancelled:c(s)#0", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_setup_reaches_the_remote_sink() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, RuntimeConfig::default());
    let plan = Plan {
        tokens: 1,
        slow_attempt: Some((0, 60_000)),
        ..Plan::default()
    };
    // The call does not return until the sink has its stream, so cancelling
    // during setup is dropping the pending call.
    tokio::select! {
        _ = cluster.segment("front").call(Cx::new(), prompt("u", plan)) => {
            panic!("the call returned while the sink was still setting up");
        }
        () = events.wait_for("D:start:c(u)#0", WAIT) => {}
    }
    events.wait_for("D:abandoned:c(u)#0", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_forward_work_ends_at_the_deadline() {
    let nodes = nodes(2).await;
    let placement = Placement::new()
        .segment("front", ["A", "B", "D", "F", "E", "G"])
        .segment("side", ["C"]);
    let events = Events::new();
    let cluster = deploy(&nodes, &placement, &events, RuntimeConfig::default());
    let plan = Plan {
        tokens: 1,
        forward_delay_ms: 60_000,
        ..Plan::default()
    };
    // As in local composition, a failure before the sink has its stream is
    // the call's error.
    let result = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(200)), prompt("w", plan))
        .await;
    assert_eq!(
        result.err().map(|e| e.kind()),
        Some(ErrorKind::DeadlineExceeded)
    );
    events.wait_for("C:abandoned:w", WAIT).await;
    assert!(!events.has("D:start:c(w)#0"));
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hand_off_that_arrives_after_its_deadline_is_refused() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = RuntimeConfig {
        faults: Faults {
            delay_forward_arrival: Some(ms(400)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, config);
    let plan = Plan {
        tokens: 1,
        ..Plan::default()
    };
    let result = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(150)), prompt("l", plan))
        .await;
    let outcome = match result {
        Err(error) => error.kind(),
        Ok(out) => last_kind(&collect(out).await).expect("a terminal error"),
    };
    assert_eq!(outcome, ErrorKind::DeadlineExceeded);
    // Give the late delivery time to arrive; it must not start the sink.
    tokio::time::sleep(ms(500)).await;
    assert!(!events.has("D:start:c(l)#0"), "{:?}", events.snapshot());
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_open_reply_ends_in_one_error_and_no_leak() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = RuntimeConfig {
        setup_timeout: ms(300),
        faults: Faults {
            delay_open_reply: Some(Duration::from_secs(2)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, config);
    let plan = Plan {
        tokens: 1,
        ..Plan::default()
    };
    let result = cluster
        .segment("front")
        .call(Cx::new(), prompt("o", plan))
        .await;
    assert_eq!(result.err().map(|e| e.kind()), Some(ErrorKind::Transport));
    // The forward hand-off was never sent.
    assert!(!events.has("D:start:c(o)#0"));
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_forward_ack_ends_in_one_completion_and_no_leak() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = RuntimeConfig {
        setup_timeout: ms(300),
        faults: Faults {
            delay_forward_ack: Some(Duration::from_secs(2)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, config);
    let plan = Plan {
        endless: true,
        item_delay_ms: 2,
        ..Plan::default()
    };
    let result = cluster
        .segment("front")
        .call(Cx::new(), prompt("k", plan))
        .await;
    // The caller sees exactly one outcome: the hand-off failed.
    assert_eq!(result.err().map(|e| e.kind()), Some(ErrorKind::Transport));
    // The accepted work started, and the abandoned response stream stops it.
    events.wait_for("D:start:c(k)#0", WAIT).await;
    let stopped = |e: &str| e == "D:cancelled:c(k)#0" || e == "D:dropped:c(k)#0";
    let deadline = tokio::time::Instant::now() + WAIT;
    while !events.snapshot().iter().any(|e| stopped(e)) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{:?}",
            events.snapshot()
        );
        tokio::time::sleep(ms(10)).await;
    }
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_producer_that_never_opens_ends_the_request_at_its_deadline() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = RuntimeConfig {
        faults: Faults {
            skip_inner_attach: true,
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, config);
    let plan = Plan {
        tokens: 1,
        ..Plan::default()
    };
    let result = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(300)), prompt("n", plan))
        .await;
    assert_eq!(
        result.err().map(|e| e.kind()),
        Some(ErrorKind::DeadlineExceeded)
    );
    assert!(settles(&cluster).await);
}

/// The backward path crosses into two intermediate runs: D@s1, F@s2, E@s3,
/// and back to B@s0.
fn two_intermediate_runs() -> Placement {
    Placement::new()
        .segment("s0", ["A", "B", "C", "G"])
        .segment("s1", ["D"])
        .segment("s2", ["F"])
        .segment("s3", ["E"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_and_failing_backward_streams_match_the_local_run() {
    let nodes = nodes(4).await;
    let events = Events::new();
    let placement = two_intermediate_runs();
    let cluster = deploy(&nodes, &placement, &events, RuntimeConfig::default());
    let local = local_graph(&events);
    for (node, behavior) in [
        ("F", "empty"),
        ("F", "fail"),
        ("F", "ignore"),
        ("E", "empty"),
        ("E", "fail"),
        ("E", "ignore"),
        ("G", "fail"),
    ] {
        let run = |text: String| {
            let mut cx = Cx::new();
            cx.insert_meta(node, behavior);
            (
                cx,
                prompt(
                    &text,
                    Plan {
                        tokens: 3,
                        ..Plan::default()
                    },
                ),
            )
        };
        let (cx, req) = run(format!("l{node}{behavior}"));
        let want = collect(local.call(cx, req).await.expect("local")).await;
        let (cx, req) = run(format!("r{node}{behavior}"));
        let got = collect(cluster.segment("s0").call(cx, req).await.expect("remote")).await;
        assert_eq!(kinds(&got), kinds(&want), "{node}={behavior}");
        assert!(settles(&cluster).await, "{node}={behavior}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_upstream_still_activates_the_backward_runs() {
    let nodes = nodes(4).await;
    let events = Events::new();
    let cluster = deploy(
        &nodes,
        &two_intermediate_runs(),
        &events,
        RuntimeConfig::default(),
    );
    // D never produces an item.
    let silent = Plan {
        endless: true,
        item_delay_ms: 60_000,
        ..Plan::default()
    };

    // F ignores its input: one item, then the end, without waiting for D.
    let mut cx = Cx::new();
    cx.insert_meta("F", "ignore");
    let out = cluster
        .segment("s0")
        .call(cx, prompt("pi", silent.clone()))
        .await
        .expect("call");
    assert_eq!(collect(out).await, vec![Ok("g(b(e(pf)))".to_string())]);

    // F emits a prefix before reading its input: the prefix arrives while D
    // is still silent.
    let mut cx = Cx::new();
    cx.insert_meta("F", "prefix");
    let mut out = cluster
        .segment("s0")
        .call(cx, prompt("pp", silent))
        .await
        .expect("call");
    let first = tokio::time::timeout(WAIT, out.next())
        .await
        .expect("prefix");
    assert_eq!(first, Some(Ok("g(b(e(pf)))".to_string())));
    drop(out);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_limit_of_one_serves_a_request_that_enters_a_segment_twice() {
    let nodes = nodes(2).await;
    let placement = Placement::new()
        .segment("front", ["A", "B", "D", "G"])
        .segment("side", ["C", "F", "E"]);
    let events = Events::new();
    let config = RuntimeConfig {
        admission_limit: 1,
        admission_wait: ms(50),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &placement, &events, config);
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let out = cluster
        .segment("front")
        .call(Cx::new(), prompt("a", plan))
        .await
        .expect("call");
    assert_eq!(collect(out).await, expected(0..3));
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_root_beyond_its_attempt_limit_is_overloaded() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = RuntimeConfig {
        attempt_limit: 1,
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, config);
    let plan = Plan {
        tokens: 2,
        item_delay_ms: 20,
        mode: Mode::Hedge,
        ..Plan::default()
    };
    let result = cluster
        .segment("front")
        .call(Cx::new(), prompt("h", plan))
        .await;
    let outcome = match result {
        Err(error) => error.kind(),
        Ok(out) => last_kind(&collect(out).await).expect("a terminal error"),
    };
    assert_eq!(outcome, ErrorKind::Overloaded);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_at_capacity_refuses_a_new_root() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = RuntimeConfig {
        admission_limit: 1,
        admission_wait: ms(50),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, config);
    let busy = Plan {
        endless: true,
        item_delay_ms: 2,
        ..Plan::default()
    };
    let mut first = cluster
        .segment("front")
        .call(Cx::new(), prompt("c1", busy.clone()))
        .await
        .expect("first");
    assert!(first.next().await.is_some_and(|item| item.is_ok()));
    let second = cluster
        .segment("front")
        .call(Cx::new(), prompt("c2", busy))
        .await;
    assert_eq!(second.err().map(|e| e.kind()), Some(ErrorKind::Overloaded));
    drop(first);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_drains_output_already_buffered_across_segments() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, RuntimeConfig::default());
    let cx = Cx::new();
    let plan = Plan {
        tokens: 8,
        hold_at_end: true,
        ..Plan::default()
    };
    let out = cluster
        .segment("front")
        .call(cx.clone(), prompt("b", plan))
        .await
        .expect("call");
    events.wait_for("D:held:c(b)#0", WAIT).await;
    cx.request_stop();
    let mut want = expected(0..8);
    want.push(Ok(format!("g(b(e(f{STOP_MARK})))")));
    assert_eq!(collect(out).await, want);
    events.wait_for("D:stopped:c(b)#0", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_producer_whose_node_goes_away_ends_the_stream_with_an_error() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = RuntimeConfig {
        heartbeat_interval: Some(ms(100)),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, config);
    let plan = Plan {
        endless: true,
        item_delay_ms: 5,
        ..Plan::default()
    };
    let mut out = cluster
        .segment("front")
        .call(Cx::new(), prompt("x", plan))
        .await
        .expect("call");
    assert!(out.next().await.is_some_and(|item| item.is_ok()));
    // The engine's node shuts down mid-stream.
    nodes[1]
        .shutdown(velo::ShutdownPolicy::Timeout(ms(200)))
        .await;
    let rest = collect(out).await;
    let last = rest.last().expect("a terminal item");
    assert!(last.is_err(), "the stream must not end cleanly: {rest:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn instances_of_one_segment_share_the_load() {
    let nodes = nodes(4).await;
    let instances = vec![
        ("front".to_string(), Arc::clone(&nodes[0])),
        ("engine".to_string(), Arc::clone(&nodes[1])),
        ("engine".to_string(), Arc::clone(&nodes[2])),
        ("post".to_string(), Arc::clone(&nodes[3])),
    ];
    let events = Events::new();
    let cluster = deploy_instances(
        &instances,
        &three_segments(),
        &events,
        RuntimeConfig::default(),
    );
    for n in 0..6 {
        let plan = Plan {
            tokens: 1,
            ..Plan::default()
        };
        let out = cluster.segments[0]
            .1
            .call(Cx::new(), prompt(&format!("i{n}"), plan))
            .await
            .expect("call");
        assert_eq!(collect(out).await, expected(0..1));
    }
    let served: Vec<usize> = cluster
        .segments
        .iter()
        .filter(|(name, _)| name == "engine")
        .map(|(_, segment)| segment.served().0)
        .collect();
    assert_eq!(served, vec![3, 3], "round robin over two engine instances");
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producing_streams_are_bounded_by_the_egress_quota() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let config = RuntimeConfig {
        egress_limit: 1,
        admission_wait: ms(50),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &three_segments(), &events, config);
    let front = cluster.segment("front");
    let busy = Plan {
        endless: true,
        item_delay_ms: 2,
        ..Plan::default()
    };

    let mut first = front
        .call(Cx::new(), prompt("q1", busy.clone()))
        .await
        .expect("first");
    assert!(first.next().await.is_some_and(|item| item.is_ok()));
    // The engine and the post segment each produce one stream for it.
    assert_eq!(cluster.segment("engine").egress_in_use(), 1);
    assert_eq!(cluster.segment("post").egress_in_use(), 1);
    let refused = front.call(Cx::new(), prompt("q2", busy.clone())).await;
    assert_eq!(refused.err().map(|e| e.kind()), Some(ErrorKind::Overloaded));

    // Ending the first request frees the quota.
    drop(first);
    assert!(settles(&cluster).await);
    let deadline = tokio::time::Instant::now() + WAIT;
    while cluster.segments.iter().any(|(_, s)| s.egress_in_use() > 0) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "egress was not released"
        );
        tokio::time::sleep(ms(5)).await;
    }
    let plan = Plan {
        tokens: 2,
        ..Plan::default()
    };
    let out = front
        .call(Cx::new(), prompt("q3", plan))
        .await
        .expect("third");
    assert_eq!(collect(out).await, expected(0..2));
    assert!(settles(&cluster).await);
}

/// With segment clocks within the configured skew, a remote stage never runs
/// past the source's deadline, whichever way its clock is off. The remote
/// sink must stop on its own deadline, before the source's: a stage that
/// ran late would instead be stopped by the source's cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_deadlines_never_run_late_with_clocks_off_either_way() {
    for offset_ms in [-40_i64, 40] {
        let nodes = nodes(3).await;
        let events = Events::new();
        let config = RuntimeConfig {
            clock_skew: ms(50),
            faults: Faults {
                clock_offset_nanos: offset_ms * 1_000_000,
                ..Faults::default()
            },
            ..RuntimeConfig::default()
        };
        let cluster = deploy(&nodes, &three_segments(), &events, config);
        let cx = Cx::new().with_timeout(ms(400));
        let source_deadline = cx.deadline().expect("deadline").instant();
        let plan = Plan {
            endless: true,
            item_delay_ms: 2,
            ..Plan::default()
        };
        let text = format!("t{offset_ms}");
        let out = cluster
            .segment("front")
            .call(cx, prompt(&text, plan))
            .await
            .expect("call");
        let consumer = tokio::spawn(collect(out));
        events
            .wait_for(&format!("D:deadline:c({text})#0"), WAIT)
            .await;
        assert!(
            tokio::time::Instant::now() <= source_deadline,
            "offset {offset_ms} ms: the remote sink ran past the source's deadline"
        );
        let all = consumer.await.expect("consumer");
        assert_eq!(last_kind(&all), Some(ErrorKind::DeadlineExceeded));
        assert!(settles(&cluster).await);
    }
}

// ----- milestone 3 review, pass 1 -----

/// Outcome of one call: an error from the call itself, or the items.
fn outcome(
    result: gdl::Result<gdl::Items<String>>,
) -> futures::future::BoxFuture<'static, Result<Vec<Result<String, ErrorKind>>, ErrorKind>> {
    Box::pin(async move {
        match result {
            Err(error) => Err(error.kind()),
            Ok(out) => Ok(kinds(&collect(out).await)),
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_generate_is_not_masked_by_input_ignoring_backward_nodes() {
    let nodes = nodes(4).await;
    let events = Events::new();
    let cluster = deploy(
        &nodes,
        &two_intermediate_runs(),
        &events,
        RuntimeConfig::default(),
    );
    let local = local_graph(&events);
    for ignoring in ["F", "E", "G"] {
        let run = |text: &str| {
            let mut cx = Cx::new();
            cx.insert_meta(ignoring, "ignore");
            let plan = Plan {
                tokens: 3,
                fail_generate: true,
                ..Plan::default()
            };
            (cx, prompt(text, plan))
        };
        let (cx, req) = run(&format!("la{ignoring}"));
        let want = outcome(local.call(cx, req).await).await;
        let (cx, req) = run(&format!("ra{ignoring}"));
        let got = outcome(cluster.segment("s0").call(cx, req).await).await;
        assert_eq!(got, want, "{ignoring} ignores its input");
        assert!(settles(&cluster).await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_node_context_is_cancelled_however_generate_ends() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, RuntimeConfig::default());
    // generate fails
    let plan = Plan {
        helper: true,
        fail_generate: true,
        ..Plan::default()
    };
    let _ = outcome(
        cluster
            .segment("front")
            .call(Cx::new(), prompt("hf", plan))
            .await,
    )
    .await;
    events.wait_for("D:helper-ended:c(hf)#0", WAIT).await;
    // generate is still pending at the deadline
    let plan = Plan {
        helper: true,
        slow_attempt: Some((0, 60_000)),
        ..Plan::default()
    };
    let _ = outcome(
        cluster
            .segment("front")
            .call(Cx::new().with_timeout(ms(200)), prompt("hp", plan))
            .await,
    )
    .await;
    events.wait_for("D:helper-ended:c(hp)#0", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_reaches_a_remote_sink_whose_generate_is_still_pending() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, RuntimeConfig::default());
    let cx = Cx::new().with_timeout(ms(5_000));
    let stopper = {
        let cx = cx.clone();
        let events = Arc::clone(&events);
        tokio::spawn(async move {
            events.wait_for("D:start:c(st)#0", WAIT).await;
            cx.request_stop();
        })
    };
    let plan = Plan {
        stop_aware_start: true,
        ..Plan::default()
    };
    let got = outcome(cluster.segment("front").call(cx, prompt("st", plan)).await).await;
    stopper.await.expect("stopper");
    assert_eq!(got, Ok(vec![Ok(format!("g(b(e(f{STOP_MARK})))"))]));
    events.wait_for("D:stopped-at-start:c(st)#0", WAIT).await;
    assert!(settles(&cluster).await);
}

/// The caller keeps its response stream but stops reading it: past the
/// deadline, the producing segments must let go even though their sends
/// are blocked on the stalled consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_producer_blocked_on_a_stalled_consumer_ends_at_the_deadline() {
    let nodes = vec![
        tight_velo_node().await,
        tight_velo_node().await,
        tight_velo_node().await,
    ];
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, RuntimeConfig::default());
    let plan = Plan {
        endless: true,
        ..Plan::default()
    };
    let mut out = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(300)), prompt("sb", plan))
        .await
        .expect("call");
    assert!(out.next().await.is_some_and(|item| item.is_ok()));
    // Hold `out` unread past the deadline.
    tokio::time::sleep(ms(600)).await;
    for name in ["engine", "post"] {
        let segment = cluster.segment(name);
        let deadline = tokio::time::Instant::now() + WAIT;
        while segment.admitted() > 0 || segment.egress_in_use() > 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{name} still holds the request while the consumer is stalled"
            );
            tokio::time::sleep(ms(10)).await;
        }
    }
    drop(out);
    assert!(settles(&cluster).await);
}

/// A producer that finished, with output still buffered, while the caller
/// holds its stream unread: the reply anchors are released at the deadline,
/// not when the caller eventually drops the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_output_is_released_at_the_deadline_while_the_stream_is_held() {
    let nodes = nodes(3).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &three_segments(), &events, RuntimeConfig::default());
    let plan = Plan {
        tokens: 200,
        ..Plan::default()
    };
    let out = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(300)), prompt("fo", plan))
        .await
        .expect("call");
    events.wait_for("D:start:c(fo)#0", WAIT).await;
    tokio::time::sleep(ms(600)).await;
    let deadline = tokio::time::Instant::now() + WAIT;
    while cluster.segments.iter().any(|(_, s)| s.active_anchors() > 0) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "reply anchors outlived the deadline while the stream was held"
        );
        tokio::time::sleep(ms(10)).await;
    }
    drop(out);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_without_a_deadline_gets_the_runtime_default() {
    let nodes = nodes(2).await;
    let placement = Placement::new()
        .segment("front", ["A", "B", "D", "F", "E", "G"])
        .segment("side", ["C"]);
    let events = Events::new();
    let config = RuntimeConfig {
        default_deadline: ms(300),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &placement, &events, config);
    let plan = Plan {
        tokens: 1,
        forward_delay_ms: 60_000,
        ..Plan::default()
    };
    // No deadline on the request; the caller abandons it while C is stuck.
    let out = cluster
        .segment("front")
        .call(Cx::new(), prompt("nd", plan))
        .await;
    drop(out);
    events.wait_for("C:abandoned:nd", WAIT).await;
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_requests_leave_no_tasks_behind() {
    let nodes = nodes(3).await;
    let events = Events::new();
    // The default deadline is far off: a task that waits for it would still
    // be alive when this test counts.
    let cluster = deploy(&nodes, &three_segments(), &events, RuntimeConfig::default());
    let run = |tag: String| {
        let plan = Plan {
            tokens: 3,
            ..Plan::default()
        };
        let front = cluster.segment("front");
        async move {
            let out = front
                .call(Cx::new(), prompt(&tag, plan))
                .await
                .expect("call");
            let all = collect(out).await;
            assert!(all.iter().all(Result::is_ok), "{all:?}");
        }
    };
    // The fabric opens connections and their tasks lazily, and a short
    // warm-up does not always open all of them, so the count is taken once
    // it is steady after a full batch, and must come back to it after a
    // second. A task kept per request would add one per request.
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

// ----- milestone 3 review, pass 2 -----

fn two_segments() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "F", "E", "G"])
        .segment("engine", ["D"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_reaches_the_remote_sink_while_the_stream_is_held_unread() {
    let nodes = nodes(2).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &two_segments(), &events, RuntimeConfig::default());
    let plan = Plan {
        endless: true,
        item_delay_ms: 2,
        ..Plan::default()
    };
    let cx = Cx::new();
    let _held = cluster
        .segment("front")
        .call(cx.clone(), prompt("hc", plan))
        .await
        .expect("call");
    cx.cancel();
    events.wait_for("D:cancelled:c(hc)#0", WAIT).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_reaches_the_remote_sink_while_the_stream_is_held_unread() {
    let nodes = nodes(2).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &two_segments(), &events, RuntimeConfig::default());
    let plan = Plan {
        endless: true,
        item_delay_ms: 2,
        ..Plan::default()
    };
    let cx = Cx::new();
    let _held = cluster
        .segment("front")
        .call(cx.clone(), prompt("hs", plan))
        .await
        .expect("call");
    cx.request_stop();
    events.wait_for("D:stopped:c(hs)#0", WAIT).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_stream_delivers_nothing_buffered_after_its_deadline() {
    let nodes = nodes(2).await;
    let events = Events::new();
    let cluster = deploy(&nodes, &two_segments(), &events, RuntimeConfig::default());
    // More than the boundary's check interval, all buffered by the time the
    // deadline passes: unreleased, they would be delivered before the
    // boundary noticed the deadline.
    let plan = Plan {
        tokens: 500,
        ..Plan::default()
    };
    let mut out = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(300)), prompt("hb", plan))
        .await
        .expect("call");
    assert!(matches!(out.next().await, Some(Ok(_))));
    tokio::time::sleep(ms(600)).await;
    let next = out.next().await;
    assert_eq!(
        next.as_ref()
            .map(|item| item.as_ref().map_err(|e| e.kind())),
        Some(Err(ErrorKind::DeadlineExceeded)),
        "{next:?}"
    );
    drop(out);
    assert!(settles(&cluster).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delayed_hand_off_ack_does_not_hold_remote_work_past_the_deadline() {
    let nodes = nodes(3).await;
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("side", ["C"])
        .segment("engine", ["D"]);
    let events = Events::new();
    let config = RuntimeConfig {
        faults: Faults {
            delay_forward_ack: Some(ms(4_000)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &placement, &events, config);
    let plan = Plan {
        tokens: 1,
        ..Plan::default()
    };
    let result = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(300)), prompt("da", plan))
        .await;
    assert_eq!(
        result.err().map(|e| e.kind()),
        Some(ErrorKind::DeadlineExceeded)
    );
    // The side segment handed the request on and is waiting for the delayed
    // acknowledgement; the request's deadline, not the delay, must end that.
    let released = tokio::time::Instant::now() + ms(1_500);
    while cluster.segment("side").admitted() > 0 {
        assert!(
            tokio::time::Instant::now() < released,
            "the side segment still holds the request after its deadline"
        );
        tokio::time::sleep(ms(10)).await;
    }
}

#[tokio::test]
async fn a_zero_limit_is_refused_at_deployment() {
    let node = velo_node().await;
    let placement = Placement::new().segment("all", REFERENCE_NODES);
    let mut roster = gdl::Roster::new();
    roster.add("all", node.peer_info());
    for config in [
        RuntimeConfig {
            attempt_limit: 0,
            ..RuntimeConfig::default()
        },
        RuntimeConfig {
            admission_limit: 0,
            ..RuntimeConfig::default()
        },
        RuntimeConfig {
            egress_limit: 0,
            ..RuntimeConfig::default()
        },
    ] {
        let result = gdl::Deployment::new(
            Arc::clone(&node),
            &reference_manifest(|_| true),
            &placement,
            "all",
            &roster,
            config,
        );
        assert!(result.is_err(), "a zero limit was accepted");
    }
}

// ----- milestone 3 review, pass 3 -----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_reaches_the_remote_sink_while_its_hand_off_is_unacknowledged() {
    let nodes = nodes(2).await;
    let events = Events::new();
    let config = RuntimeConfig {
        faults: Faults {
            delay_forward_ack: Some(ms(3_000)),
            ..Faults::default()
        },
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &two_segments(), &events, config);
    let plan = Plan {
        tokens: 1,
        stop_aware_start: true,
        ..Plan::default()
    };
    let cx = Cx::new();
    let call = cluster
        .segment("front")
        .call(cx.clone(), prompt("sa", plan));
    let stop = async {
        events.wait_for("D:start:c(sa)#0", WAIT).await;
        cx.request_stop();
        // Well before the acknowledgement arrives.
        events
            .wait_for("D:stopped-at-start:c(sa)#0", ms(1_000))
            .await;
    };
    let (result, ()) = tokio::join!(call, stop);
    drop(result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_waiting_for_egress_does_not_hold_its_share_past_the_deadline() {
    let nodes = nodes(2).await;
    let events = Events::new();
    let config = RuntimeConfig {
        egress_limit: 1,
        admission_wait: ms(3_000),
        ..RuntimeConfig::default()
    };
    let cluster = deploy(&nodes, &two_segments(), &events, config);
    let busy = Plan {
        endless: true,
        item_delay_ms: 5,
        ..Plan::default()
    };
    // Holds the engine's only egress permit.
    let _first = cluster
        .segment("front")
        .call(Cx::new(), prompt("e1", busy))
        .await
        .expect("first");
    let plan = Plan {
        tokens: 1,
        ..Plan::default()
    };
    let result = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(300)), prompt("e2", plan))
        .await;
    assert!(result.is_err());
    let released = tokio::time::Instant::now() + ms(1_000);
    while cluster.segment("engine").admitted() > 1 {
        assert!(
            tokio::time::Instant::now() < released,
            "the engine still holds the second request after its deadline"
        );
        tokio::time::sleep(ms(10)).await;
    }
}
