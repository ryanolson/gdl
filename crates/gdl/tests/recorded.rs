//! What the runtime records for each segment a request passes through: the
//! request path (from the request's arrival until it is handed on or its
//! inner end starts its stream), the response path (each stream the
//! segment sends on), and, in the source's segment, the request end to
//! end. Over one, two, and three segments of the reference graph.

mod common;

use std::collections::BTreeMap;

use common::*;
use futures::StreamExt;
use gdl::prometheus::{Encoder, Registry, TextEncoder};
use gdl::{Cx, Placement, RuntimeConfig};

/// The error kinds `error_type` takes.
const KINDS: [&str; 8] = [
    "node",
    "deadline_exceeded",
    "truncated",
    "not_hosted",
    "overloaded",
    "transport",
    "codec",
    "placement",
];

/// A deployment and the registry of each of its segments.
struct Deployed<H> {
    cluster: Cluster<H>,
    registries: BTreeMap<String, Registry>,
    placement: Placement,
    _nodes: Vec<velo::Velo>,
}

async fn deployed(
    placement: Placement,
    configure: &dyn Fn(&str) -> RuntimeConfig,
) -> Deployed<impl gdl::Next<Prompt, Chunk> + gdl::Register + use<>> {
    let mut instances = Vec::new();
    let mut registries = BTreeMap::new();
    let mut nodes = Vec::new();
    for (name, _) in placement.segments() {
        let node = velo_node().await;
        nodes.push(node.clone());
        instances.push((name.to_string(), node));
        registries.insert(name.to_string(), Registry::new());
    }
    let events = Events::quiet();
    let cluster = deploy_configured(&instances, &placement, &events, &|segment| RuntimeConfig {
        metrics: Some(registries[segment].clone()),
        ..configure(segment)
    });
    Deployed {
        cluster,
        registries,
        placement,
        _nodes: nodes,
    }
}

impl<H: gdl::Next<Prompt, Chunk>> Deployed<H> {
    async fn call(&self, cx: Cx, plan: Plan) -> gdl::Result<gdl::Items<Chunk>> {
        let call = self
            .cluster
            .segment(&source_segment(&self.placement))
            .call(cx, prompt("m", plan));
        tokio::time::timeout(WAIT, call)
            .await
            .expect("the call did not return in time")
    }

    /// Call, and read the response to its end, or to its first error.
    async fn run(&self, cx: Cx, plan: Plan) -> Vec<gdl::Result<Chunk>> {
        match self.call(cx, plan).await {
            Ok(out) => collect(out).await,
            Err(error) => vec![Err(error)],
        }
    }

    /// The segment that hosts `node`.
    fn host(&self, node: &str) -> String {
        self.placement
            .segments()
            .find(|(_, nodes)| nodes.iter().any(|n| n == node))
            .map(|(name, _)| name.to_string())
            .expect("placed")
    }

    /// Every series of `metric` in `segment`'s registry, by its labels
    /// other than the identity ones.
    fn series(&self, segment: &str, metric: &str) -> Vec<(BTreeMap<String, String>, f64)> {
        let mut text = Vec::new();
        TextEncoder::new()
            .encode(&self.registries[segment].gather(), &mut text)
            .expect("text");
        let text = String::from_utf8(text).expect("utf-8");
        text.lines()
            .filter(|l| l.starts_with(&format!("{metric}{{")))
            .map(|line| {
                let (labels, value) = line[metric.len() + 1..].split_once("} ").expect("a series");
                let labels = labels
                    .split(',')
                    .map(|pair| {
                        let (k, v) = pair.split_once('=').expect("a label");
                        (k.to_string(), v.trim_matches('"').to_string())
                    })
                    .collect();
                (labels, value.parse().expect("a value"))
            })
            .collect()
    }

    /// The value of `metric` at the entry `endpoint`, with the extra labels
    /// `with`, in the registry of the segment that hosts the entry; `None`
    /// when there is no such series.
    fn get(&self, metric: &str, endpoint: &str, with: &[(&str, &str)]) -> Option<f64> {
        let mut want: BTreeMap<String, String> = with
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        want.insert("dynamo_endpoint".into(), endpoint.into());
        self.series(&self.host(endpoint), metric)
            .into_iter()
            .find(|(labels, _)| want.iter().all(|(k, v)| labels.get(k) == Some(v)))
            .map(|(_, value)| value)
    }

    fn value(&self, metric: &str, endpoint: &str, with: &[(&str, &str)]) -> f64 {
        self.get(metric, endpoint, with)
            .unwrap_or_else(|| panic!("no {metric} for {endpoint} {with:?}"))
    }

    fn count(&self, histogram: &str, endpoint: &str) -> f64 {
        self.value(&format!("{histogram}_count"), endpoint, &[])
    }

    /// A histogram's sum, in seconds.
    fn sum(&self, histogram: &str, endpoint: &str) -> f64 {
        self.value(&format!("{histogram}_sum"), endpoint, &[])
    }

    /// Errors of `kind` counted anywhere in the deployment: on the request
    /// path and the response path of every segment, not end to end.
    fn errors_anywhere(&self, kind: &str) -> f64 {
        self.registries
            .keys()
            .flat_map(|segment| {
                ["errors_total", "response_errors_total"]
                    .into_iter()
                    .flat_map(move |metric| self.series(segment, metric))
            })
            .filter(|(labels, _)| labels.get("error_type").map(String::as_str) == Some(kind))
            .map(|(_, value)| value)
            .sum()
    }

    /// Requests and responses in flight in every segment.
    fn inflight(&self) -> f64 {
        self.registries
            .keys()
            .flat_map(|segment| {
                ["inflight_requests", "inflight_responses"]
                    .into_iter()
                    .flat_map(move |metric| self.series(segment, metric))
            })
            .map(|(_, value)| value)
            .sum()
    }

    /// Wait until no segment holds a request or a response.
    async fn idle(&self) {
        let deadline = tokio::time::Instant::now() + WAIT;
        while self.inflight() != 0.0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "still in flight: {}",
                self.inflight()
            );
            tokio::time::sleep(ms(5)).await;
        }
    }
}

/// The next item of `out`, bounded.
async fn next(out: &mut gdl::Items<Chunk>) -> gdl::Result<Chunk> {
    tokio::time::timeout(WAIT, out.next())
        .await
        .expect("no item in time")
        .expect("an item")
}

fn one() -> Placement {
    Placement::new().segment("front", ["A", "B", "C", "D", "F", "E", "G"])
}

fn two() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "F", "E", "G"])
        .segment("engine", ["D"])
}

fn three() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"])
}

/// The forward path stays in the front while the response crosses: the
/// outer end runs the sink itself, in a run with a visit of its own.
fn forward_local() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "D", "G"])
        .segment("post", ["F", "E"])
}

/// The backward path comes back to the front for F, a run the outer end
/// opens itself, before it goes to the post segment for E.
fn revisiting() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "F", "G"])
        .segment("engine", ["D"])
        .segment("post", ["E"])
}

fn plan(tokens: u32) -> Plan {
    Plan {
        tokens,
        ..Plan::default()
    }
}

/// An entry, and whether its visits have a request path.
type Entry = (&'static str, bool);

/// The entries of the reference placements: the source's (A), the
/// engine's `gdl.fwd` (D), and the backward run the post segment opens
/// (F).
fn entries(placement: &Placement) -> Vec<&'static str> {
    let segments = placement.segments().count();
    ["A", "D", "F"][..segments].to_vec()
}

/// Each placement with its entries.
fn placements() -> Vec<(Placement, Vec<Entry>)> {
    vec![
        (one(), vec![("A", true)]),
        (two(), vec![("A", true), ("D", true)]),
        (three(), vec![("A", true), ("D", true), ("F", false)]),
        (
            forward_local(),
            vec![("A", true), ("D", false), ("F", false)],
        ),
        (
            revisiting(),
            vec![("A", true), ("D", true), ("F", false), ("E", false)],
        ),
    ]
}

/// A request is one visit of each segment it passes: a request and a
/// response at the source, with the request end to end; a request and a
/// response where the sink runs; a response where a backward run runs.
/// Nothing is recorded for a node that is no entry, and nothing is in
/// flight once the request ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_is_one_visit_of_each_segment() {
    for (placement, entries) in placements() {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let out = d.call(Cx::new(), plan(2)).await.expect("call");
        assert_eq!(collect(out).await, expected(0..2));
        d.idle().await;
        for (entry, request) in &entries {
            let requests = d.get("requests_total", entry, &[]);
            if *request {
                assert_eq!(requests, Some(1.0), "{entry}");
                assert_eq!(d.count("request_path_duration_seconds", entry), 1.0);
            } else {
                assert_eq!(requests, None, "{entry} has no request path");
            }
            assert_eq!(d.value("responses_total", entry, &[]), 1.0, "{entry}");
            for histogram in [
                "response_time_to_first_item_seconds",
                "response_time_after_first_item_seconds",
            ] {
                assert_eq!(d.count(histogram, entry), 1.0, "{entry}: {histogram}");
            }
            for kind in KINDS {
                let with = [("error_type", kind)];
                assert_eq!(d.get("response_errors_total", entry, &with), Some(0.0));
            }
            assert_eq!(d.value("response_cancellation_total", entry, &[]), 0.0);
            let e2e = d.get("end_to_end_requests_total", entry, &[]);
            assert_eq!(e2e, (*entry == "A").then_some(1.0), "{entry}");
        }
        for histogram in [
            "end_to_end_time_to_first_item_seconds",
            "end_to_end_duration_seconds",
        ] {
            assert_eq!(d.count(histogram, "A"), 1.0, "{histogram}");
        }
        let entries: Vec<&str> = entries.iter().map(|(entry, _)| *entry).collect();
        for node in ["B", "C", "D", "E", "F", "G"]
            .into_iter()
            .filter(|n| !entries.contains(n))
        {
            for metric in ["requests_total", "responses_total"] {
                assert_eq!(d.get(metric, node, &[]), None, "{node} is no entry");
            }
        }
        assert_eq!(
            d.series(&d.host("A"), "request_duration_seconds_count"),
            vec![],
            "nothing per node"
        );
    }
}

/// The parts add up. With a delay before the hand-off (C, in the front),
/// in the sink's `generate` (the engine's request path), and before the
/// sink's first item (the engine's time to its first item), each figure
/// covers its own delay and none of the others, and the parts sum to the
/// time to the first item end to end, less the wire. Per node they would
/// nest: the front's would hold all three. Runs for about half a second,
/// the sum of the delays.
///
/// The same holds for an operator that returns its stream before it calls
/// `next` (B retrying, with one attempt): the hand-off it makes later, not
/// its return, ends the front's request path, which so holds C's delay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_parts_add_up_to_the_first_item_end_to_end() {
    const SLACK: f64 = 0.06;
    for mode in [Mode::Plain, Mode::Retry { max: 1 }] {
        let d = deployed(three(), &|_| RuntimeConfig::default()).await;
        let delays = Plan {
            forward_delay_ms: 100,
            slow_attempt: Some((0, 150)),
            slow_items: Some((0, 200)),
            mode,
            ..plan(1)
        };
        assert_eq!(d.run(Cx::new(), delays).await, expected(0..1));
        d.idle().await;
        let parts = [
            ("request_path_duration_seconds", "A", 0.100),
            ("request_path_duration_seconds", "D", 0.150),
            ("response_time_to_first_item_seconds", "D", 0.200),
            ("response_time_to_first_item_seconds", "F", 0.0),
            ("response_time_to_first_item_seconds", "A", 0.0),
        ];
        let mut total = 0.0;
        for (histogram, entry, delay) in parts {
            let seconds = d.sum(histogram, entry);
            assert!(
                seconds >= delay && seconds < delay + SLACK,
                "{mode:?}: {histogram} at {entry}: {seconds} s for a delay of {delay} s"
            );
            total += seconds;
        }
        let end_to_end = d.sum("end_to_end_time_to_first_item_seconds", "A");
        assert!(
            total <= end_to_end && end_to_end - total < SLACK,
            "{mode:?}: the parts sum to {total} s, the first item came after {end_to_end} s"
        );
    }
}

/// The hand-off is timed when it is sent: an acknowledgement the engine
/// delays (after it has admitted the request, so its own request path is
/// already running) is in no segment's figure, and the parts still sum to
/// no more than the time end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_acknowledgement_is_counted_nowhere() {
    let d = deployed(two(), &|segment| {
        let mut config = RuntimeConfig::default();
        if segment == "engine" {
            config.faults.delay_forward_ack = Some(ms(150));
        }
        config
    })
    .await;
    assert_eq!(d.run(Cx::new(), plan(1)).await, expected(0..1));
    d.idle().await;
    let front = d.sum("request_path_duration_seconds", "A");
    assert!(
        front < 0.1,
        "the front's request path held the ack: {front} s"
    );
    let total = front
        + d.sum("request_path_duration_seconds", "D")
        + d.sum("response_time_to_first_item_seconds", "D")
        + d.sum("response_time_to_first_item_seconds", "A");
    let end_to_end = d.sum("end_to_end_time_to_first_item_seconds", "A");
    assert!(end_to_end >= 0.15, "{end_to_end} s");
    assert!(total <= end_to_end, "{total} s against {end_to_end} s");
}

/// An error is counted once, in the segment that raised it, and once end
/// to end: a sink that fails to start, a sink whose stream fails, and a
/// forward node that fails. A segment that relays the error does not count
/// it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_is_counted_once_where_it_was_raised() {
    for placement in [one(), two(), three()] {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let sink = if d.host("D") == d.host("A") { "A" } else { "D" };
        let cases = [
            // The sink fails before its stream exists: its request path.
            (
                Plan {
                    fail_generate: true,
                    ..plan(2)
                },
                "errors_total",
                sink,
            ),
            // The sink's stream fails: its response path.
            (
                Plan {
                    fail_at: Some(0),
                    ..plan(2)
                },
                "response_errors_total",
                sink,
            ),
            // The forward node fails before the hand-off: the front's
            // request path.
            (
                Plan {
                    fail_forward: true,
                    ..plan(2)
                },
                "errors_total",
                "A",
            ),
        ];
        for (n, (failing, metric, entry)) in cases.into_iter().enumerate() {
            let out = d.run(Cx::new(), failing).await;
            assert!(out.last().is_some_and(|item| item.is_err()), "{out:?}");
            d.idle().await;
            let counted = (n + 1) as f64;
            let with = [("error_type", "node")];
            assert_eq!(d.errors_anywhere("node"), counted, "case {n}");
            assert_eq!(
                d.value("end_to_end_errors_total", "A", &with),
                counted,
                "case {n}"
            );
            assert!(d.value(metric, entry, &with) >= 1.0, "case {n}: {metric}");
        }
        if sink == "D" {
            assert_eq!(d.value("requests_total", "D", &[]), 2.0, "C failed first");
        }
    }
}

/// A stream its consumer drops is a cancellation of each segment's
/// response and of the request end to end, not an error; one dropped
/// after an error item is that error, not a cancellation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_stream_is_a_cancellation_of_every_response() {
    for placement in [one(), two(), three()] {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let endless = Plan {
            endless: true,
            item_delay_ms: 1,
            ..plan(0)
        };
        let mut out = d.call(Cx::new(), endless).await.expect("call");
        next(&mut out).await.expect("not an error");
        drop(out);
        d.idle().await;
        for entry in entries(&d.placement) {
            assert_eq!(
                d.value("response_cancellation_total", entry, &[]),
                1.0,
                "{entry}"
            );
        }
        assert_eq!(d.value("end_to_end_cancellation_total", "A", &[]), 1.0);
        for kind in KINDS {
            assert_eq!(d.errors_anywhere(kind), 0.0, "{kind}");
        }

        let failing = Plan {
            fail_at: Some(0),
            hold_after_error: true,
            ..plan(3)
        };
        let mut out = d.call(Cx::new(), failing).await.expect("call");
        assert!(next(&mut out).await.is_err());
        drop(out);
        d.idle().await;
        let with = [("error_type", "node")];
        assert_eq!(d.value("end_to_end_errors_total", "A", &with), 1.0);
        assert_eq!(d.value("end_to_end_cancellation_total", "A", &[]), 1.0);
        assert_eq!(d.errors_anywhere("node"), 1.0);
    }
}

/// A deadline ends the sink's stream on the sink's clock, which runs ahead
/// of the front's by the clock skew allowance: the sink's segment counts
/// it, and the front relays it. A stream held unread holds its responses
/// in flight until then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadline_is_counted_where_its_clock_ended_the_stream() {
    // A wide allowance: the front's own deadline comes 200 ms after the
    // engine's, so the engine's error always reaches it first.
    let d = deployed(two(), &|_| RuntimeConfig {
        clock_skew: ms(200),
        ..RuntimeConfig::default()
    })
    .await;
    let endless = Plan {
        endless: true,
        item_delay_ms: 1,
        ..plan(0)
    };
    let out = d
        .call(Cx::new().with_timeout(ms(400)), endless)
        .await
        .expect("call");
    assert_eq!(d.value("inflight_responses", "D", &[]), 1.0);
    assert_eq!(d.value("inflight_responses", "A", &[]), 1.0);
    let items = collect(out).await;
    assert_eq!(
        items
            .last()
            .and_then(|i| i.as_ref().err())
            .map(|e| e.kind()),
        Some(gdl::ErrorKind::DeadlineExceeded)
    );
    d.idle().await;
    let with = [("error_type", "deadline_exceeded")];
    assert_eq!(d.value("response_errors_total", "D", &with), 1.0);
    assert_eq!(d.value("response_errors_total", "A", &with), 0.0);
    assert_eq!(d.value("end_to_end_errors_total", "A", &with), 1.0);
    assert_eq!(d.value("response_cancellation_total", "A", &[]), 0.0);
}

/// An operator that calls its `next` twice is one visit of its segment and
/// two of the segment below; the first attempt's error is the engine's,
/// and the request succeeds end to end. Where the sink shares the
/// operator's segment, the error ends no stream that leaves the segment,
/// and nothing counts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operators_attempts_are_visits_of_the_segment_below() {
    for placement in [one(), two()] {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let retried = Plan {
            fail_attempts: 1,
            mode: Mode::Retry { max: 2 },
            ..plan(2)
        };
        assert_eq!(d.run(Cx::new(), retried).await, expected(0..2));
        d.idle().await;
        assert_eq!(d.value("requests_total", "A", &[]), 1.0);
        assert_eq!(d.value("end_to_end_requests_total", "A", &[]), 1.0);
        let with = [("error_type", "node")];
        assert_eq!(d.value("end_to_end_errors_total", "A", &with), 0.0);
        if d.host("D") == d.host("A") {
            assert_eq!(d.errors_anywhere("node"), 0.0);
        } else {
            assert_eq!(d.value("requests_total", "D", &[]), 2.0);
            assert_eq!(d.value("response_errors_total", "D", &with), 1.0);
            assert_eq!(d.errors_anywhere("node"), 1.0);
        }
    }
}

/// A request refused at a segment's admission is no visit of that
/// segment: the sender, whose hand-off failed, counts the refusal, and so
/// does the request end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_request_is_counted_by_its_sender() {
    let d = deployed(two(), &|segment| match segment {
        "engine" => RuntimeConfig {
            admission_limit: 1,
            admission_wait: ms(20),
            ..RuntimeConfig::default()
        },
        _ => RuntimeConfig::default(),
    })
    .await;
    let endless = Plan {
        endless: true,
        item_delay_ms: 1,
        ..plan(0)
    };
    let mut held = d.call(Cx::new(), endless.clone()).await.expect("held");
    next(&mut held).await.expect("not an error");
    let refused = d.run(Cx::new(), plan(1)).await;
    assert!(
        refused
            .iter()
            .any(|item| matches!(item, Err(e) if e.kind() == gdl::ErrorKind::Overloaded)),
        "{refused:?}"
    );
    assert_eq!(
        d.value("requests_total", "D", &[]),
        1.0,
        "only the held one"
    );
    drop(held);
    d.idle().await;
    let with = [("error_type", "overloaded")];
    assert_eq!(d.value("errors_total", "A", &with), 1.0);
    assert_eq!(d.value("end_to_end_errors_total", "A", &with), 1.0);
    assert_eq!(d.errors_anywhere("overloaded"), 1.0);
}

/// A request with no deadline of its own gets the runtime's default when
/// it crosses: held unread past it and then dropped, it is
/// `deadline_exceeded` end to end, not a cancellation. Here the source's
/// own hop crosses, and the engine's entry is the operator B, whose
/// request path ends when the sink below it starts its stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_deadline_classifies_a_dropped_request() {
    let alone = Placement::new()
        .segment("front", ["A"])
        .segment("engine", ["B", "C", "D", "E", "F", "G"]);
    let d = deployed(alone, &|_| RuntimeConfig {
        default_deadline: ms(200),
        ..RuntimeConfig::default()
    })
    .await;
    let endless = Plan {
        endless: true,
        item_delay_ms: 1,
        ..plan(0)
    };
    let called = tokio::time::Instant::now();
    let out = d.call(Cx::new(), endless).await.expect("call");
    assert_eq!(d.value("requests_total", "B", &[]), 1.0);
    assert_eq!(d.count("request_path_duration_seconds", "B"), 1.0);
    // Held unread until the engine's stream has ended at the deadline,
    // which it sees early by the clock skew allowance, and until the
    // front's own has passed: the attempt took it after `called`, within
    // milliseconds.
    let with = [("error_type", "deadline_exceeded")];
    let deadline = tokio::time::Instant::now() + WAIT;
    while d.value("response_errors_total", "B", &with) == 0.0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the engine's stream never ended"
        );
        tokio::time::sleep(ms(5)).await;
    }
    tokio::time::sleep_until(called + ms(300)).await;
    drop(out);
    d.idle().await;
    assert_eq!(d.value("end_to_end_errors_total", "A", &with), 1.0);
    assert_eq!(d.value("end_to_end_cancellation_total", "A", &[]), 0.0);
}

/// A request that waits for a held roster before its hand-off waits in the
/// front: the wait is on the front's request path, which ends when the
/// hand-off leaves, not when the request reaches the code that sends it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_for_the_roster_is_on_the_request_path() {
    let placement = two();
    let (front_node, engine_node) = (velo_node().await, velo_node().await);
    let mut roster = gdl::Roster::new();
    roster.add("front", front_node.peer_info());
    roster.add("engine", engine_node.peer_info());
    roster.hold();
    let events = Events::quiet();
    let manifest = reference_manifest(|_| true);
    let registry = Registry::new();
    let start = |name: &str, node: &velo::Velo, metrics: Option<Registry>| {
        let config = RuntimeConfig {
            metrics,
            ..RuntimeConfig::default()
        };
        let deployment =
            gdl::Deployment::new(node.clone(), &manifest, &placement, name, &roster, config)
                .expect("deployment");
        let root = reference_graph(&deployment, &events).expect("graph");
        deployment.start(root).expect("start")
    };
    let front = start("front", &front_node, Some(registry.clone()));
    let _engine = start("engine", &engine_node, None);
    let (out, ()) = tokio::join!(
        tokio::time::timeout(WAIT, front.call(Cx::new(), prompt("held", plan(1)))),
        async {
            tokio::time::sleep(ms(150)).await;
            roster.release();
        }
    );
    let out = out.expect("the call did not return in time").expect("call");
    assert_eq!(collect(out).await, expected(0..1));
    let mut text = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut text)
        .expect("text");
    let text = String::from_utf8(text).expect("utf-8");
    let held = text
        .lines()
        .find(|line| {
            line.starts_with("request_path_duration_seconds_sum{")
                && line.contains("dynamo_endpoint=\"A\"")
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("no request path for A in:\n{text}"));
    assert!(
        held >= 0.15,
        "the front's request path left out the wait: {held} s"
    );
}
