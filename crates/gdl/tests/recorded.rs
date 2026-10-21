//! What the runtime records for every node a segment hosts: requests,
//! errors by kind, cancellations, requests in flight, and their duration,
//! taken where the runtime calls each node, over one segment and over two.

mod common;

use std::collections::BTreeMap;

use common::*;
use futures::StreamExt;
use gdl::prometheus::{Encoder, Registry, TextEncoder};
use gdl::{Cx, Placement, RuntimeConfig};

/// Every node of the reference graph.
const NODES: [&str; 7] = ["A", "B", "C", "D", "E", "F", "G"];

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
        self.cluster
            .segment(&source_segment(&self.placement))
            .call(cx, prompt("m", plan))
            .await
    }

    /// The value of `metric` for `node`, with the extra labels `with`, in
    /// the registry of the segment that hosts it.
    fn value(&self, metric: &str, node: &str, with: &[(&str, &str)]) -> f64 {
        let segment = self
            .placement
            .segments()
            .find(|(_, nodes)| nodes.iter().any(|n| n == node))
            .map(|(name, _)| name.to_string())
            .expect("placed");
        let mut text = Vec::new();
        TextEncoder::new()
            .encode(&self.registries[&segment].gather(), &mut text)
            .expect("text");
        let text = String::from_utf8(text).expect("utf-8");
        let mut want: BTreeMap<String, String> = with
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        want.insert("dynamo_endpoint".into(), node.into());
        for line in text
            .lines()
            .filter(|l| l.starts_with(&format!("{metric}{{")))
        {
            let (labels, value) = line[metric.len() + 1..].split_once("} ").expect("a series");
            let labels: BTreeMap<String, String> = labels
                .split(',')
                .map(|pair| {
                    let (k, v) = pair.split_once('=').expect("a label");
                    (k.to_string(), v.trim_matches('"').to_string())
                })
                .collect();
            if want.iter().all(|(k, v)| labels.get(k) == Some(v)) && labels.len() == want.len() {
                return value.parse().expect("a value");
            }
        }
        panic!("no {metric} for {node} {with:?} in:\n{text}")
    }

    fn requests(&self, node: &str) -> f64 {
        self.value("requests_total", node, &[])
    }

    fn errors(&self, node: &str, kind: &str) -> f64 {
        self.value("errors_total", node, &[("error_type", kind)])
    }

    fn cancellations(&self, node: &str) -> f64 {
        self.value("cancellation_total", node, &[])
    }

    fn inflight(&self, node: &str) -> f64 {
        self.value("inflight_requests", node, &[])
    }

    fn durations(&self, node: &str) -> f64 {
        self.value("request_duration_seconds_count", node, &[])
    }

    /// A node's duration histogram: each bucket's bound and cumulative
    /// count, `+Inf` last, then the sum.
    fn histogram(&self, node: &str) -> (Vec<(String, f64)>, f64) {
        let bounds = [
            "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "20",
            "30", "60", "120", "300", "600", "+Inf",
        ];
        let buckets = bounds
            .iter()
            .map(|le| {
                let count = self.value("request_duration_seconds_bucket", node, &[("le", le)]);
                (le.to_string(), count)
            })
            .collect();
        (
            buckets,
            self.value("request_duration_seconds_sum", node, &[]),
        )
    }

    /// Wait until no node has a request in flight.
    async fn idle(&self) {
        let deadline = tokio::time::Instant::now() + WAIT;
        while NODES.iter().any(|n| self.inflight(n) != 0.0) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "requests still in flight: {:?}",
                NODES.map(|n| self.inflight(n))
            );
            tokio::time::sleep(ms(5)).await;
        }
    }
}

fn one() -> Placement {
    Placement::new().segment("front", NODES)
}

fn two() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "F", "E", "G"])
        .segment("engine", ["D"])
}

fn plan(tokens: u32) -> Plan {
    Plan {
        tokens,
        ..Plan::default()
    }
}

/// A request that succeeds is one request of every node, the source and
/// the backward nodes included, none in flight once it ends, and one
/// duration each; nothing is an error or a cancellation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_is_one_request_of_every_node() {
    for placement in [one(), two()] {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let out = d.call(Cx::new(), plan(2)).await.expect("call");
        assert_eq!(collect(out).await, expected(0..2));
        d.idle().await;
        for node in NODES {
            assert_eq!(d.requests(node), 1.0, "{node}");
            assert_eq!(d.durations(node), 1.0, "{node}");
            let (buckets, sum) = d.histogram(node);
            assert!(
                buckets.windows(2).all(|pair| pair[0].1 <= pair[1].1),
                "{node}: not cumulative: {buckets:?}"
            );
            assert_eq!(buckets.last().map(|b| b.1), Some(1.0), "{node}: +Inf");
            assert!(sum > 0.0 && sum < 5.0, "{node}: sum {sum}");
            assert_eq!(d.errors(node, "node"), 0.0, "{node}");
            assert_eq!(d.cancellations(node), 0.0, "{node}");
        }
    }
}

/// A sink that fails is an error of the sink, of kind `node`, and of every
/// node its failure passes through; the forward node before it succeeded.
/// A forward node that fails is its own error, and no request of the sink.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_is_an_error_of_its_kind() {
    for placement in [one(), two()] {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let failing = Plan {
            fail_generate: true,
            ..plan(2)
        };
        let out = d.call(Cx::new(), failing).await;
        let _ = match out {
            Ok(out) => collect(out).await,
            Err(error) => vec![Err(error)],
        };
        d.idle().await;
        assert_eq!(d.errors("D", "node"), 1.0);
        assert_eq!(d.errors("C", "node"), 0.0);
        assert_eq!(d.errors("A", "node"), 1.0);
        assert_eq!(d.cancellations("D"), 0.0);

        let forward = Plan {
            fail_forward: true,
            ..plan(2)
        };
        let out = d.call(Cx::new(), forward).await;
        let _ = match out {
            Ok(out) => collect(out).await,
            Err(error) => vec![Err(error)],
        };
        d.idle().await;
        assert_eq!(d.errors("C", "node"), 1.0);
        assert_eq!(d.requests("C"), 2.0);
        assert_eq!(d.requests("D"), 1.0, "a failed forward node calls no sink");
    }
}

/// A stream its consumer drops is a cancellation of every node still
/// producing it, not an error; one dropped after an error item is that
/// error, not a cancellation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_stream_is_a_cancellation_unless_it_failed() {
    for placement in [one(), two()] {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let endless = Plan {
            endless: true,
            item_delay_ms: 1,
            ..plan(0)
        };
        let mut out = d.call(Cx::new(), endless).await.expect("call");
        out.next().await.expect("an item").expect("not an error");
        drop(out);
        d.idle().await;
        for node in ["A", "B", "D", "E", "F", "G"] {
            assert_eq!(d.cancellations(node), 1.0, "{node}");
            assert_eq!(d.errors(node, "node"), 0.0, "{node}");
        }
        assert_eq!(d.cancellations("C"), 0.0, "C had returned");

        let failing = Plan {
            fail_at: Some(0),
            hold_after_error: true,
            ..plan(3)
        };
        let mut out = d.call(Cx::new(), failing).await.expect("call");
        assert!(out.next().await.expect("an item").is_err());
        drop(out);
        d.idle().await;
        for node in ["A", "D"] {
            assert_eq!(d.errors(node, "node"), 1.0, "{node}");
            assert_eq!(d.cancellations(node), 1.0, "{node}: only the first request");
        }
    }
}

/// A request whose deadline passes is `deadline_exceeded` at every node it
/// holds, whether the node's stream ends with the deadline's error or is
/// dropped after it; a stream held unread holds the nodes in flight until
/// then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadline_is_an_error_not_a_cancellation() {
    for placement in [one(), two()] {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let endless = Plan {
            endless: true,
            item_delay_ms: 1,
            ..plan(0)
        };
        let out = d
            .call(Cx::new().with_timeout(ms(300)), endless)
            .await
            .expect("call");
        // Held unread: the sink has a request in flight.
        assert_eq!(d.inflight("D"), 1.0);
        let items = collect(out).await;
        assert_eq!(
            items
                .last()
                .and_then(|i| i.as_ref().err())
                .map(|e| e.kind()),
            Some(gdl::ErrorKind::DeadlineExceeded)
        );
        d.idle().await;
        for node in ["A", "D"] {
            assert_eq!(d.errors(node, "deadline_exceeded"), 1.0, "{node}");
            assert_eq!(d.cancellations(node), 0.0, "{node}");
        }
    }
}

/// An operator that calls its `next` twice is one request of the
/// operator and two of each node below it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operators_attempts_are_requests_of_the_nodes_below() {
    for placement in [one(), two()] {
        let d = deployed(placement, &|_| RuntimeConfig::default()).await;
        let retried = Plan {
            fail_attempts: 1,
            mode: Mode::Retry { max: 2 },
            ..plan(2)
        };
        let out = d.call(Cx::new(), retried).await.expect("call");
        assert_eq!(collect(out).await, expected(0..2));
        d.idle().await;
        for (node, requests) in [
            ("A", 1.0),
            ("B", 1.0),
            ("G", 1.0),
            ("C", 2.0),
            ("D", 2.0),
            ("F", 2.0),
            ("E", 2.0),
        ] {
            assert_eq!(d.requests(node), requests, "{node}");
        }
        assert_eq!(d.errors("D", "node"), 1.0, "the first attempt failed");
        assert_eq!(d.errors("B", "node"), 0.0, "the operator recovered");
    }
}

/// A request refused at a segment's admission is no request of its nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_request_records_nothing_where_it_was_refused() {
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
    held.next().await.expect("an item").expect("not an error");
    let refused = d.call(Cx::new(), plan(1)).await;
    let refused = match refused {
        Ok(out) => collect(out).await,
        Err(error) => vec![Err(error)],
    };
    assert!(
        refused
            .iter()
            .any(|item| matches!(item, Err(e) if e.kind() == gdl::ErrorKind::Overloaded)),
        "{refused:?}"
    );
    assert_eq!(d.requests("D"), 1.0, "the refused request reached no sink");
    drop(held);
    d.idle().await;
    assert_eq!(
        d.errors("A", "overloaded"),
        1.0,
        "the source saw the refusal"
    );
}

/// A request with no deadline of its own gets the runtime's default when
/// it crosses: held unread past it and then dropped, it is
/// `deadline_exceeded` at the source, whose own hop crosses here, as at
/// the sink, not a cancellation. (Where the source's hop stays in its
/// segment, the source's request has no deadline, and a drop is a
/// cancellation.)
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
    // Held unread until the sink has ended at the deadline, which it sees
    // early by the clock skew allowance, and until the source's own has
    // passed: the attempt took it after `called`, within milliseconds.
    let deadline = tokio::time::Instant::now() + WAIT;
    while d.errors("D", "deadline_exceeded") == 0.0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the sink never ended"
        );
        tokio::time::sleep(ms(5)).await;
    }
    tokio::time::sleep_until(called + ms(300)).await;
    drop(out);
    d.idle().await;
    assert_eq!(d.errors("A", "deadline_exceeded"), 1.0);
    assert_eq!(d.cancellations("A"), 0.0);
}
