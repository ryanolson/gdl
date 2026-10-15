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
    let out = cluster
        .segment("front")
        .call(Cx::new(), prompt("u", plan))
        .await
        .expect("call");
    events.wait_for("D:start:c(u)#0", WAIT).await;
    drop(out);
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
    let out = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(200)), prompt("w", plan))
        .await
        .expect("call");
    let all = collect(out).await;
    assert_eq!(
        last_kind(&all),
        Some(ErrorKind::DeadlineExceeded),
        "{all:?}"
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
    let out = cluster
        .segment("front")
        .call(Cx::new().with_timeout(ms(300)), prompt("n", plan))
        .await
        .expect("call");
    let all = collect(out).await;
    assert_eq!(
        last_kind(&all),
        Some(ErrorKind::DeadlineExceeded),
        "{all:?}"
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
