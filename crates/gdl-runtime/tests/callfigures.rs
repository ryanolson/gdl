//! The figures of calls to a unary service, the same whether the copy
//! called runs in the caller's segment or in another: the caller counts
//! each call and its errors by kind, and times it while it records
//! durations, and the service's segment records each call it accepted as a
//! visit of the service node. The service's segment runs on a velo node in
//! this process, over TCP on an OS-assigned port.

mod common;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{WAIT, velo_node};
use gdl::Unary;
use gdl::{Build, Cx, Deadline, Error, ErrorKind, Items, Next, Operator, RequestId, Result, Sink};
use gdl::{UnaryClient, UnaryServices, items};
use gdl_runtime::{
    Calls, CodecEntry, EdgeCodec, Placement, Roster, RuntimeConfig, Segment, wire_codec,
};
use prometheus::Registry;
use prometheus::proto::{MetricFamily, MetricType};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// A request: what the service does is its first word.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ask {
    /// The text.
    pub text: String,
}

/// A response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Told {
    /// The text.
    pub text: String,
}

mod contract {
    use super::{Ask, Told};

    gdl_runtime::graph! {
        name = "callfigures", schema = 1;
        unary    Tok: Ask -> Told;
        source   Front: Ask -> stream Told;
        operator Router: (Ask -> stream Told) over (Ask -> stream Told) calls Tok;
        sink     Echo: Ask -> stream Told;
        topology: Front => Router { Echo };
    }
}

fn ask(text: &str) -> Ask {
    Ask {
        text: text.to_string(),
    }
}

/// `Tok`. By the request's first word: `told` answers its text, `busy`
/// fails with `Overloaded` after it ran, and `hold` answers once `open` is
/// notified. It notes each run's request id, and whether the run was part
/// of a visit the runtime records.
#[derive(Default)]
struct Service {
    open: Notify,
    runs: Mutex<Vec<(RequestId, bool)>>,
}

impl Build for Service {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        unreachable!("built by the tests")
    }
}

impl Unary for Service {
    type Req = Ask;
    type Resp = Told;

    async fn call(&self, cx: &Cx, req: Ask) -> Result<Told> {
        self.runs
            .lock()
            .expect("runs")
            .push((cx.id(), cx.visit().is_some()));
        match req.text.split_whitespace().next() {
            Some("busy") => Err(Error::new(ErrorKind::Overloaded, "busy downstream")),
            Some("hold") => {
                self.open.notified().await;
                Ok(Told { text: req.text })
            }
            _ => Ok(Told { text: req.text }),
        }
    }
}

/// Calls `Tok` on each request, and sends the request on.
struct Router {
    tok: UnaryClient<Ask, Told>,
}

impl Operator for Router {
    type UpReq = Ask;
    type UpItem = Told;
    type DownReq = Ask;
    type DownItem = Told;

    async fn generate<N>(&self, cx: Cx, req: Ask, next: N) -> Result<Items<Told>>
    where
        N: Next<Ask, Told>,
    {
        self.tok.call(&cx, req.clone()).await?;
        next.generate(cx, req).await
    }
}

/// Answers with its request's text.
struct Echo;

impl Sink for Echo {
    type Req = Ask;
    type Item = Told;

    async fn generate(&self, _cx: Cx, req: Ask) -> Result<Items<Told>> {
        Ok(items(futures::stream::iter([Ok(Told { text: req.text })])))
    }
}

/// The router's segment and the tokenizer's own.
const SPLIT: &str = "[segment.front]\nnodes = ['Front', 'Router', 'Echo']\n\
                     [segment.tok]\nnodes = ['Tok']\n";

fn split() -> Placement {
    Placement::from_toml(SPLIT).expect("a placement")
}

/// The calls of segment `front`'s nodes, as the manifest declares them.
fn services(segment: &str) -> UnaryServices {
    UnaryServices::new(
        segment,
        contract::manifest()
            .nodes
            .iter()
            .map(|node| (node.name.clone(), node.calls.clone())),
    )
}

/// `Router`'s client of `Tok`, once `services` is built and ready.
async fn client(services: &UnaryServices) -> UnaryClient<Ask, Told> {
    let client = services
        .scope("Router", async { UnaryClient::<Ask, Told>::current("Tok") })
        .await
        .expect("the client");
    services.built();
    services.ready().await.expect("ready");
    client
}

/// A client of the copy of `Tok` in `Router`'s own segment, with its call
/// figures in a registry of their own, timed.
async fn local(service: Arc<Service>) -> (UnaryClient<Ask, Told>, Registry) {
    let services = services("front");
    let registry = Registry::new();
    services.register(&registry, true).expect("registered");
    services.serve("Tok", service).expect("served");
    (client(&services).await, registry)
}

/// The started split layout: the front, with `Router`'s client of `Tok`
/// in segment `tok` and its call figures in `calls`, timed, and `tok`,
/// configured as `tok_config` with its figures in `visits`.
struct Remote<F, T> {
    client: UnaryClient<Ask, Told>,
    calls: Registry,
    visits: Registry,
    _front: Segment<F>,
    _tok: Segment<T>,
}

async fn remote(
    service: Arc<Service>,
    placement: Placement,
    tok_config: RuntimeConfig,
) -> Remote<impl Sized + use<>, impl Sized + use<>> {
    let manifest = contract::manifest();
    let (front_node, tok_node) = (velo_node().await, velo_node().await);
    let mut roster = Roster::new();
    roster.add("front".to_string(), front_node.peer_info());
    roster.add("tok".to_string(), tok_node.peer_info());
    let visits = Registry::new();
    let deployment = gdl_runtime::Deployment::new(
        tok_node,
        &manifest,
        &placement,
        "tok",
        &roster,
        RuntimeConfig {
            metrics: Some(visits.clone()),
            ..tok_config
        },
    )
    .expect("the tok deployment");
    let root =
        contract::assemble(&deployment, contract::Bindings::new().tok(service)).expect("assembled");
    let tok = deployment.start(root).expect("started");

    // As a segment binary's builder links its nodes: before it builds them.
    let calls = Calls::default();
    let services = services("front");
    let registry = Registry::new();
    services.register(&registry, true).expect("registered");
    services
        .add(
            "Tok",
            "tok",
            calls
                .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
                .expect("a link"),
        )
        .expect("linked");
    let client = client(&services).await;
    let deployment = gdl_runtime::Deployment::new(
        front_node,
        &manifest,
        &placement,
        "front",
        &roster,
        RuntimeConfig {
            calls,
            ..RuntimeConfig::default()
        },
    )
    .expect("the front deployment");
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new()
            .router(Router {
                tok: client.clone(),
            })
            .echo(Echo),
    )
    .expect("assembled");
    Remote {
        client,
        calls: registry,
        visits,
        _front: deployment.start(root).expect("started"),
        _tok: tok,
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("done in time")
}

fn family(registry: &Registry, name: &str) -> Option<MetricFamily> {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
}

/// The value of the one series of family `name` whose labels include
/// `labels`: a counter's or gauge's value, or a histogram's count.
fn value(registry: &Registry, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let family = family(registry, name)?;
    let mut found = family.get_metric().iter().filter(|metric| {
        labels.iter().all(|(name, value)| {
            metric
                .get_label()
                .iter()
                .any(|pair| pair.name() == *name && pair.value() == *value)
        })
    });
    let metric = found.next()?;
    assert!(
        found.next().is_none(),
        "one series of {name} for {labels:?}"
    );
    Some(match family.get_field_type() {
        MetricType::COUNTER => metric.get_counter().value(),
        MetricType::GAUGE => metric.get_gauge().value(),
        _ => metric.get_histogram().get_sample_count() as f64,
    })
}

/// Each call family's name, with the label names of its series.
fn shapes(registry: &Registry) -> BTreeSet<(String, BTreeSet<String>)> {
    registry
        .gather()
        .into_iter()
        .filter(|family| family.name().starts_with("call"))
        .flat_map(|family| {
            let name = family.name().to_string();
            family
                .get_metric()
                .iter()
                .map(|metric| {
                    let labels = metric
                        .get_label()
                        .iter()
                        .map(|pair| pair.name().to_string())
                        .collect();
                    (name.clone(), labels)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The same calls of the same service, made to the copy in the caller's
/// segment and to the copy in another, are read under the same names and
/// labels: only `placement` and `segment` say where the copy ran. Each
/// call counts, its error by kind, and its duration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_read_the_same_figures_in_both_placements() {
    let (here, local) = local(Arc::new(Service::default())).await;
    let there = remote(
        Arc::new(Service::default()),
        split(),
        RuntimeConfig::default(),
    )
    .await;
    for client in [&here, &there.client] {
        let cx = Cx::new();
        bounded(client.call(&cx, ask("told hi")))
            .await
            .expect("answered");
        let busy = bounded(client.call(&cx, ask("busy"))).await;
        assert_eq!(busy.expect_err("busy").kind(), ErrorKind::Overloaded);
    }
    let names: BTreeSet<String> = shapes(&local).into_iter().map(|(name, _)| name).collect();
    assert_eq!(
        names,
        [
            "call_cancellation_total",
            "call_duration_seconds",
            "call_errors_total",
            "calls_total"
        ]
        .map(String::from)
        .into(),
    );
    assert_eq!(shapes(&local), shapes(&there.calls));
    for (registry, placement, segment) in
        [(&local, "local", "front"), (&there.calls, "remote", "tok")]
    {
        let at = [
            ("caller", "Router"),
            ("service", "Tok"),
            ("placement", placement),
            ("segment", segment),
        ];
        let with = |extra: &[(&'static str, &'static str)]| -> Vec<(&str, &str)> {
            at.iter().chain(extra).copied().collect()
        };
        assert_eq!(
            value(registry, "calls_total", &at),
            Some(2.0),
            "{placement}"
        );
        assert_eq!(
            value(
                registry,
                "call_errors_total",
                &with(&[("error_type", "overloaded")])
            ),
            Some(1.0),
            "{placement}"
        );
        assert_eq!(
            value(registry, "call_duration_seconds", &at),
            Some(2.0),
            "{placement}"
        );
    }
}

/// The service's segment records each call it accepted as a visit of the
/// service node, run under the request's id: its figures and logs line up
/// with the caller's. The error the service raised counts there too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_service_records_each_accepted_call_as_a_visit_under_the_requests_id() {
    let service = Arc::new(Service::default());
    let there = remote(Arc::clone(&service), split(), RuntimeConfig::default()).await;
    let (cx, other) = (Cx::new(), Cx::new());
    bounded(there.client.call(&cx, ask("told hi")))
        .await
        .expect("answered");
    let _ = bounded(there.client.call(&other, ask("busy"))).await;
    let runs = service.runs.lock().expect("runs").clone();
    assert_eq!(runs, [(cx.id(), true), (other.id(), true)]);
    let at = [("dynamo_endpoint", "Tok")];
    assert_eq!(value(&there.visits, "requests_total", &at), Some(2.0));
    assert_eq!(
        value(
            &there.visits,
            "errors_total",
            &[("dynamo_endpoint", "Tok"), ("error_type", "overloaded")]
        ),
        Some(1.0)
    );
    assert_eq!(
        value(&there.visits, "request_path_duration_seconds", &at),
        Some(2.0)
    );
    assert_eq!(value(&there.visits, "inflight_requests", &at), Some(0.0));
}

/// A refused call never ran: the caller counts the refusal as
/// `overloaded`, and the service's segment records no visit for it. The
/// service's segment shows the call permits it holds while its accepted
/// calls run, and none once their replies are sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_counts_at_the_caller_and_the_held_permits_show() {
    let service = Arc::new(Service::default());
    let there = remote(
        Arc::clone(&service),
        split(),
        RuntimeConfig {
            call_limit: Some(1),
            ..RuntimeConfig::default()
        },
    )
    .await;
    let held = tokio::spawn({
        let client = there.client.clone();
        async move { client.call(&Cx::new(), ask("hold")).await }
    });
    // Running: the service noted it, and its permit is held.
    bounded(async {
        while service.runs.lock().expect("runs").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(value(&there.visits, "call_permits_held", &[]), Some(1.0));
    let refused = bounded(there.client.call(&Cx::new(), ask("told hi"))).await;
    assert_eq!(refused.expect_err("refused").kind(), ErrorKind::Overloaded);
    service.open.notify_one();
    bounded(held)
        .await
        .expect("joined")
        .expect("answered once open");
    // The permit comes back once velo has sent the reply, which may be
    // just after the caller read it.
    bounded(async {
        while value(&there.visits, "call_permits_held", &[]) != Some(0.0) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let caller = [("caller", "Router"), ("placement", "remote")];
    assert_eq!(value(&there.calls, "calls_total", &caller), Some(2.0));
    assert_eq!(
        value(
            &there.calls,
            "call_errors_total",
            &[
                ("caller", "Router"),
                ("placement", "remote"),
                ("error_type", "overloaded")
            ]
        ),
        Some(1.0)
    );
    assert_eq!(
        value(
            &there.visits,
            "requests_total",
            &[("dynamo_endpoint", "Tok")]
        ),
        Some(1.0),
        "a visit for the refused call"
    );
    assert_eq!(service.runs.lock().expect("runs").len(), 1);
}

/// A reply rejected by the service segment's size limit raises `Codec`
/// there, after the service ran. Its visit must retain that failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_past_call_reply_max_counts_codec_at_the_service() {
    let service = Arc::new(Service::default());
    let there = remote(
        Arc::clone(&service),
        split(),
        RuntimeConfig {
            call_reply_max: 1024,
            ..RuntimeConfig::default()
        },
    )
    .await;
    let cx = Cx::new();
    bounded(there.client.call(&cx, ask("told control")))
        .await
        .expect("the control fits");
    let error = bounded(
        there
            .client
            .call(&cx, ask(&format!("told {}", "x".repeat(2048)))),
    )
    .await
    .expect_err("the reply exceeds the service's limit");
    assert_eq!(error.kind(), ErrorKind::Codec);
    assert!(error.message().contains("call_reply_max"), "{error}");
    assert_eq!(service.runs.lock().expect("runs").len(), 2);
    assert_eq!(
        value(&there.calls, "calls_total", &[("caller", "Router")]),
        Some(2.0)
    );
    assert_eq!(
        value(
            &there.calls,
            "call_errors_total",
            &[("caller", "Router"), ("error_type", "codec")]
        ),
        Some(1.0)
    );
    let endpoint = [("dynamo_endpoint", "Tok")];
    assert_eq!(value(&there.visits, "requests_total", &endpoint), Some(2.0));
    assert_eq!(
        value(&there.visits, "inflight_requests", &endpoint),
        Some(0.0)
    );
    assert_eq!(
        value(&there.visits, "request_path_duration_seconds", &endpoint),
        Some(2.0)
    );
    assert_eq!(
        value(
            &there.visits,
            "errors_total",
            &[("dynamo_endpoint", "Tok"), ("error_type", "codec")]
        ),
        Some(1.0),
        "the service recorded success before its reply was replaced by Codec"
    );
}

/// One kind of reply, as a guard row: what the call sends, what the service
/// segment is set to, and the outcome both ends must record.
struct Case {
    name: &'static str,
    ask: String,
    /// The service segment's `call_reply_max`, when not the default.
    reply_max: Option<usize>,
    /// Whether the service's replies are compressed under a `[codec]` entry.
    compressed: bool,
    /// The request's deadline.
    deadline: Option<Duration>,
    /// The `error_type` of a call to another segment, at the caller and at
    /// the service alike; `None` for an answer.
    remote: Option<&'static str>,
    /// The same call to the copy in the caller's segment, which has no
    /// wire and no bound of its own: `None` where it would not end.
    local: Option<Option<&'static str>>,
}

/// The guard of the rule that a service's visit ends with the reply the
/// caller reads: for each kind of reply, the service's segment records the
/// outcome its caller records. A new way for a reply to end belongs in this
/// table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_service_records_the_outcome_its_caller_reads() {
    let large = |text: &str| format!("told {text}");
    let cases = [
        Case {
            name: "an answer",
            ask: "told hi".to_string(),
            reply_max: None,
            compressed: false,
            deadline: None,
            remote: None,
            local: Some(None),
        },
        Case {
            name: "the service's own error",
            ask: "busy".to_string(),
            reply_max: None,
            compressed: false,
            deadline: None,
            remote: Some("overloaded"),
            local: Some(Some("overloaded")),
        },
        Case {
            name: "an answer past call_reply_max, replaced by its fallback",
            ask: large(&"x".repeat(2048)),
            reply_max: Some(1024),
            compressed: false,
            deadline: None,
            remote: Some("codec"),
            local: Some(None),
        },
        Case {
            name: "a compressed answer that fits",
            ask: large(&"x".repeat(8192)),
            reply_max: Some(1024),
            compressed: true,
            deadline: None,
            remote: None,
            local: Some(None),
        },
        Case {
            name: "a compressed answer that does not fit",
            ask: large(&incompressible(8192)),
            reply_max: Some(1024),
            compressed: true,
            deadline: None,
            remote: Some("codec"),
            local: Some(None),
        },
        Case {
            name: "a service the call's bound cuts short",
            ask: "hold".to_string(),
            reply_max: None,
            compressed: false,
            deadline: Some(Duration::from_millis(200)),
            remote: Some("deadline_exceeded"),
            local: None,
        },
    ];
    for case in cases {
        let placement = match case.compressed {
            true => split().codec(
                "Tok",
                CodecEntry::call(None, Some(EdgeCodec::MsgpackZstd)).floor(1024),
            ),
            false => split(),
        };
        let mut config = RuntimeConfig::default();
        if let Some(max) = case.reply_max {
            config.call_reply_max = max;
        }
        let cx = || match case.deadline {
            Some(after) => Cx::new().with_deadline(Deadline::after(after)),
            None => Cx::new(),
        };
        let there = remote(Arc::new(Service::default()), placement, config).await;
        let called = bounded(there.client.call(&cx(), ask(&case.ask))).await;
        assert_eq!(
            called.err().map(|error| error.kind()),
            case.remote.map(kind),
            "{}: the call",
            case.name
        );
        // The caller can see its bound pass before the service's segment
        // writes its reply; wait for the visit to end.
        let at = [("dynamo_endpoint", "Tok")];
        bounded(async {
            while value(&there.visits, "inflight_requests", &at) != Some(0.0) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let caller = [("caller", "Router"), ("placement", "remote")];
        assert_eq!(value(&there.calls, "calls_total", &caller), Some(1.0));
        assert_eq!(value(&there.visits, "requests_total", &at), Some(1.0));
        let called = recorded(&there.calls, "call_errors_total", &caller);
        let served = recorded(&there.visits, "errors_total", &at);
        assert_eq!(called.as_deref(), case.remote, "{}: the caller", case.name);
        assert_eq!(served, called, "{}: the service", case.name);

        if let Some(expected) = case.local {
            let (here, local) = local(Arc::new(Service::default())).await;
            let _ = bounded(here.call(&cx(), ask(&case.ask))).await;
            let at = [("caller", "Router"), ("placement", "local")];
            assert_eq!(value(&local, "calls_total", &at), Some(1.0));
            assert_eq!(
                recorded(&local, "call_errors_total", &at).as_deref(),
                expected,
                "{}: the local call",
                case.name
            );
        }
    }
}

/// The `error_type` of the one error that family `errors` counted in the
/// series whose labels include `labels`, or `None` for none.
fn recorded(registry: &Registry, errors: &str, labels: &[(&str, &str)]) -> Option<String> {
    let family = family(registry, errors)?;
    let counted: Vec<String> = family
        .get_metric()
        .iter()
        .filter(|metric| {
            labels.iter().all(|(name, value)| {
                metric
                    .get_label()
                    .iter()
                    .any(|pair| pair.name() == *name && pair.value() == *value)
            })
        })
        .filter(|metric| metric.get_counter().value() > 0.0)
        .map(|metric| {
            let kind = metric
                .get_label()
                .iter()
                .find(|pair| pair.name() == "error_type")
                .expect("an error_type");
            assert_eq!(metric.get_counter().value(), 1.0, "one error");
            kind.value().to_string()
        })
        .collect();
    assert!(counted.len() <= 1, "one error at most: {counted:?}");
    counted.into_iter().next()
}

/// The kind whose `error_type` is `label`.
fn kind(label: &str) -> ErrorKind {
    match label {
        "overloaded" => ErrorKind::Overloaded,
        "codec" => ErrorKind::Codec,
        "deadline_exceeded" => ErrorKind::DeadlineExceeded,
        other => panic!("no kind for {other}"),
    }
}

/// `len` characters that zstd does not shrink: a fixed pseudo-random
/// sequence of 64 symbols.
fn incompressible(len: usize) -> String {
    const SET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    (0..len)
        .map(|_| {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            SET[((z ^ (z >> 31)) % 64) as usize] as char
        })
        .collect()
}
