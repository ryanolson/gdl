//! The figures of the calls a segment's nodes make through the clients
//! they take in their builds: counted always, by caller and service, with
//! errors by kind, and timed only when the segment records durations.

use std::sync::Arc;
use std::time::Duration;

use prometheus::Registry;

use super::*;
use crate::Deadline;

/// Answers its word count; `fail` fails with `Node`, `wait` never answers.
struct Words;

impl Build for Words {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Unary for Words {
    type Req = String;
    type Resp = usize;

    async fn call(&self, _cx: &Cx, text: String) -> Result<usize> {
        match text.as_str() {
            "fail" => Err(Error::node("no words")),
            "wait" => std::future::pending().await,
            _ => Ok(text.split_whitespace().count()),
        }
    }
}

/// Segment `front`, where `Router` calls `Words`, which it hosts, with its
/// figures in a registry of their own, timed when `durations`.
fn recorded(durations: bool) -> (UnaryServices, Registry) {
    let services = UnaryServices::new(
        "front",
        [
            ("Router".to_string(), vec!["Words".to_string()]),
            ("Words".to_string(), Vec::new()),
        ],
    );
    let registry = Registry::new();
    services.register(&registry, durations).expect("registered");
    services.serve("Words", Arc::new(Words)).expect("served");
    (services, registry)
}

/// `Router`'s client of `Words`, once the segment is built and ready.
async fn client(services: &UnaryServices) -> UnaryClient<String, usize> {
    let client = services
        .scope("Router", async {
            UnaryClient::<String, usize>::current("Words")
        })
        .await
        .expect("the client");
    services.built();
    services.ready().await.expect("ready");
    client
}

/// The registry as a scrape reads it. Read as text: the proto's accessors
/// differ with prometheus's `protobuf` feature, which the workspace turns
/// on and this crate alone does not.
fn scrape(registry: &Registry) -> String {
    prometheus::TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encoded")
}

/// Whether the scrape has family `name`.
fn has(registry: &Registry, name: &str) -> bool {
    scrape(registry).contains(&format!("# TYPE {name} "))
}

/// Each series of `name` in the scrape: its labels and its value.
fn series(registry: &Registry, name: &str) -> Vec<(Vec<(String, String)>, f64)> {
    scrape(registry)
        .lines()
        .filter_map(|line| line.strip_prefix(name)?.strip_prefix('{'))
        .map(|line| {
            let (labels, value) = line.rsplit_once("} ").expect("a value");
            let labels = labels
                .split(',')
                .filter(|pair| !pair.is_empty())
                .map(|pair| {
                    let (name, value) = pair.split_once('=').expect("a label");
                    (name.to_string(), value.trim_matches('"').to_string())
                })
                .collect();
            (labels, value.parse().expect("a number"))
        })
        .collect()
}

/// The value of the one series of family `name` whose labels include
/// `labels`, with the labels every call figure carries for `Router`'s calls
/// of `Words` in its own segment: a counter's value, or a histogram's
/// count.
fn value(registry: &Registry, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    if !has(registry, name) {
        return None;
    }
    let read = if scrape(registry).contains(&format!("# TYPE {name} histogram")) {
        format!("{name}_count")
    } else {
        name.to_string()
    };
    let wanted: Vec<(&str, &str)> = [
        ("caller", "Router"),
        ("service", "Words"),
        ("placement", "local"),
        ("segment", "front"),
    ]
    .into_iter()
    .chain(labels.iter().copied())
    .collect();
    let mut found = series(registry, &read).into_iter().filter(|(have, _)| {
        wanted
            .iter()
            .all(|(name, value)| have.iter().any(|(n, v)| n == name && v == value))
    });
    let (_, value) = found.next()?;
    assert!(
        found.next().is_none(),
        "one series of {name} for {labels:?}"
    );
    Some(value)
}

/// Each call is counted under its caller, its service, where the copy
/// called runs, and that copy's segment, and each error under its kind.
/// Without durations, no call is timed.
#[tokio::test]
async fn each_call_counts_and_its_error_counts_by_kind() {
    let (services, registry) = recorded(false);
    let client = client(&services).await;
    let cx = Cx::new();
    assert_eq!(client.call(&cx, "a b".to_string()).await.expect("ok"), 2);
    client
        .call(&cx, "fail".to_string())
        .await
        .expect_err("failed");
    assert_eq!(value(&registry, "calls_total", &[]), Some(2.0));
    assert_eq!(
        value(&registry, "call_errors_total", &[("error_type", "node")]),
        Some(1.0)
    );
    assert_eq!(
        value(&registry, "call_errors_total", &[("error_type", "routing")]),
        Some(0.0)
    );
    assert_eq!(value(&registry, "call_cancellation_total", &[]), Some(0.0));
    assert!(
        !has(&registry, "call_duration_seconds"),
        "timed without durations"
    );
}

/// With durations, each call that ends is timed.
#[tokio::test]
async fn a_call_is_timed_when_the_segment_records_durations() {
    let (services, registry) = recorded(true);
    let client = client(&services).await;
    let cx = Cx::new();
    client.call(&cx, "a".to_string()).await.expect("ok");
    client
        .call(&cx, "fail".to_string())
        .await
        .expect_err("failed");
    assert_eq!(value(&registry, "call_duration_seconds", &[]), Some(2.0));
}

/// A call dropped before it ends is a cancellation, or `deadline_exceeded`
/// once its deadline has passed. A call counts from when it is made, so one
/// dropped before its first poll is a cancellation too.
#[tokio::test(start_paused = true)]
async fn a_dropped_call_counts_as_cancelled_or_past_its_deadline() {
    let (services, registry) = recorded(true);
    let client = client(&services).await;
    let cx = Cx::new();
    let mut call = Box::pin(client.call(&cx, "wait".to_string()));
    assert!(futures::poll!(call.as_mut()).is_pending());
    drop(call);
    drop(client.call(&cx, "never polled".to_string()));
    let bounded = Cx::new().with_deadline(Deadline::after(Duration::from_millis(5)));
    let mut call = Box::pin(client.call(&bounded, "wait".to_string()));
    assert!(futures::poll!(call.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(10)).await;
    drop(call);
    assert_eq!(value(&registry, "calls_total", &[]), Some(3.0));
    assert_eq!(value(&registry, "call_cancellation_total", &[]), Some(2.0));
    assert_eq!(
        value(
            &registry,
            "call_errors_total",
            &[("error_type", "deadline_exceeded")]
        ),
        Some(1.0)
    );
    assert_eq!(value(&registry, "call_duration_seconds", &[]), Some(3.0));
}

/// Two clients one caller takes of one service count in one series, and a
/// call made during the build counts as the `routing` error it is.
#[tokio::test]
async fn one_callers_clients_of_a_service_share_its_series() {
    let (services, registry) = recorded(false);
    let early = services
        .scope("Router", async {
            let client = UnaryClient::<String, usize>::current("Words")?;
            client.call(&Cx::new(), "a".to_string()).await
        })
        .await;
    assert_eq!(early.expect_err("during build").kind(), ErrorKind::Routing);
    let client = client(&services).await;
    client.call(&Cx::new(), "a".to_string()).await.expect("ok");
    assert_eq!(value(&registry, "calls_total", &[]), Some(2.0));
    assert_eq!(
        value(&registry, "call_errors_total", &[("error_type", "routing")]),
        Some(1.0)
    );
}

/// A link to a copy in another segment counts under `remote` and that
/// copy's segment, in the same families.
#[tokio::test]
async fn a_link_counts_under_remote_and_the_segment_it_calls() {
    let services = UnaryServices::new("front", [("Router".to_string(), vec!["Words".to_string()])]);
    let registry = Registry::new();
    services.register(&registry, false).expect("registered");
    // The runtime's link, here a local client that stands in for it.
    services
        .add("Words", "tok", UnaryClient::local(Arc::new(Words)))
        .expect("linked");
    let client = services
        .scope("Router", async {
            UnaryClient::<String, usize>::current("Words")
        })
        .await
        .expect("the client");
    client.call(&Cx::new(), "a".to_string()).await.expect("ok");
    let calls = series(&registry, "calls_total");
    assert_eq!(calls.len(), 1, "one caller of one service");
    let (mut labels, value) = calls.into_iter().next().expect("a series");
    labels.sort();
    assert_eq!(
        labels,
        [
            ("caller", "Router"),
            ("placement", "remote"),
            ("segment", "tok"),
            ("service", "Words"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()))
    );
    assert_eq!(value, 1.0);
}

/// A client a node is handed, not taken in a build the runtime runs, has
/// no caller: its calls are not counted.
#[tokio::test]
async fn a_hand_made_client_records_nothing() {
    let (services, registry) = recorded(true);
    let client = UnaryClient::local(Arc::new(Words));
    client.call(&Cx::new(), "a".to_string()).await.expect("ok");
    drop(services);
    assert!(series(&registry, "calls_total").is_empty());
}
