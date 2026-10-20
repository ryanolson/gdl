//! The reference graph used across the test suites:
//!
//! ```text
//! A ----------> B --------> C -----> D
//! A <--- G <--- B <--- E <--- F <--- D
//! ```
//!
//! Every node is data-driven: the request carries a `Plan` that tells the
//! nodes how to behave, so the same implementations serve every test and,
//! later, every placement across segments. Nodes record what happens to them
//! in a shared `Events` log, which tests wait on instead of sleeping.

#![allow(dead_code)]

pub mod streamed;
pub mod system;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use gdl::{
    Backward, Cx, Error, ErrorKind, Forward, Hop, Items, Next, Operator, OperatorStage, Result,
    Sink, SinkStage, Source, items,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

/// The segment of a launch in a test that starts none: it never holds a
/// request, so a drain waits only for its peers.
pub struct Idle;

impl gdl::Drains for Idle {
    fn admitted(&self) -> usize {
        0
    }
}

// ----- messages -----

/// A's request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prompt {
    pub text: String,
    pub plan: Plan,
}

/// What the nodes do for one request.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    /// How many items D emits (ignored when `endless`).
    pub tokens: u32,
    /// Delay before each D item.
    pub item_delay_ms: u64,
    /// D emits an `Err` at this index, then keeps trying to emit more.
    pub fail_at: Option<u32>,
    /// Attempts numbered below this fail before their first item.
    pub fail_attempts: u32,
    /// D never ends on its own.
    pub endless: bool,
    /// D sleeps this long in `generate` for this attempt, before returning.
    pub slow_attempt: Option<(u32, u64)>,
    /// D uses this item delay instead of `item_delay_ms` for this attempt:
    /// its stream exists at once but its items are slow.
    pub slow_items: Option<(u32, u64)>,
    /// After emitting an `Err`, D waits for its context to end instead of
    /// trying to emit more, so a test can see whether the attempt was
    /// cancelled while the consumer still holds the stream.
    pub hold_after_error: bool,
    /// After its `tokens` items, D waits for a stop or the end of its
    /// context instead of finishing.
    pub hold_at_end: bool,
    /// C sleeps this long before passing the request on.
    pub forward_delay_ms: u64,
    /// C cancels its context, then passes the request on.
    pub cancel_in_forward: bool,
    /// D's `generate` fails instead of returning a stream.
    pub fail_generate: bool,
    /// D spawns a helper that waits for D's context to be cancelled, then
    /// records `D:helper-ended`.
    pub helper: bool,
    /// D's `generate` waits for a stop, then returns its tail.
    pub stop_aware_start: bool,
    /// How B calls its `next`.
    pub mode: Mode,
}

/// B's strategy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub enum Mode {
    /// One attempt.
    #[default]
    Plain,
    /// Retry from inside the response stream while an attempt fails before
    /// its first item, up to `max` attempts.
    Retry { max: u32 },
    /// Two concurrent attempts; keep whichever yields first, drop the other.
    Hedge,
    /// Give attempt 0 `after_ms` to return its stream, abandon it, then run
    /// attempt 1.
    AbandonFirst { after_ms: u64 },
}

/// B to D.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pre {
    pub text: String,
    pub attempt: u32,
    pub plan: Plan,
}

/// D to F.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Raw(pub u32);

/// F to E.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tok(pub String);

/// E to B.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Out(pub String);

/// B to G to A.
pub type Chunk = String;

/// The item D emits when it is asked to stop: its "buffered tail".
pub const STOP_MARK: u32 = 999;

// ----- events -----

/// A shared log of what nodes did. Tests wait on it with a bound.
#[derive(Default)]
pub struct Events {
    /// Records nothing: for benchmarks, where a growing log would be
    /// measured along with the runtime.
    quiet: bool,
    log: Mutex<Vec<String>>,
    changed: Notify,
    contexts: Mutex<HashMap<String, Cx>>,
}

impl Events {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn quiet() -> Arc<Self> {
        Arc::new(Self {
            quiet: true,
            ..Self::default()
        })
    }

    pub fn record(&self, event: impl Into<String>) {
        if self.quiet {
            return;
        }
        self.log.lock().expect("events lock").push(event.into());
        self.changed.notify_waiters();
    }

    /// Publish the context a node received, so a test can check the actual
    /// state of its cancellation scope rather than infer it from events.
    pub fn publish_cx(&self, key: impl Into<String>, cx: &Cx) {
        if self.quiet {
            return;
        }
        self.contexts
            .lock()
            .expect("contexts lock")
            .insert(key.into(), cx.clone());
    }

    /// The context a node published under `key`.
    pub fn cx_of(&self, key: &str) -> Cx {
        self.contexts
            .lock()
            .expect("contexts lock")
            .get(key)
            .cloned()
            .unwrap_or_else(|| panic!("no context published as {key:?}"))
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.log.lock().expect("events lock").clone()
    }

    pub fn has(&self, event: &str) -> bool {
        self.snapshot().iter().any(|e| e == event)
    }

    /// Wait until `event` is logged; panic with the log after `within`.
    pub async fn wait_for(&self, event: &str, within: Duration) {
        self.wait_for_any(&[event], within).await;
    }

    /// Wait until one of `events` is logged; panic with the log after
    /// `within`.
    pub async fn wait_for_any(&self, events: &[&str], within: Duration) {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if events.iter().any(|event| self.has(event)) {
                return;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                panic!(
                    "timed out waiting for {events:?}; log: {:?}",
                    self.snapshot()
                );
            }
        }
    }
}

/// How long a test waits for an event before failing.
pub const WAIT: Duration = Duration::from_secs(5);

/// Records an event when dropped, unless disarmed.
pub struct OnDrop {
    events: Arc<Events>,
    event: Option<String>,
}

impl OnDrop {
    pub fn new(events: Arc<Events>, event: String) -> Self {
        Self {
            events,
            event: Some(event),
        }
    }

    pub fn disarm(mut self) {
        self.event = None;
    }
}

impl Drop for OnDrop {
    fn drop(&mut self) {
        if let Some(event) = self.event.take() {
            self.events.record(event);
        }
    }
}

// ----- nodes -----

pub struct NodeB {
    pub events: Arc<Events>,
}

impl Operator for NodeB {
    type UpReq = Prompt;
    type UpItem = Chunk;
    type DownReq = Pre;
    type DownItem = Out;

    async fn generate<N>(&self, cx: Cx, req: Prompt, next: N) -> Result<Items<Chunk>>
    where
        N: Next<Pre, Out>,
    {
        self.events.record(format!("B:start:{}", req.text));
        let pre = move |attempt| Pre {
            text: req.text.clone(),
            attempt,
            plan: req.plan.clone(),
        };
        let out = match pre(0).plan.mode {
            Mode::Plain => next.generate(cx, pre(0)).await?,
            Mode::Retry { max } => retrying(cx, next, pre, max, Arc::clone(&self.events)),
            Mode::Hedge => hedged(cx, next, pre, Arc::clone(&self.events)).await?,
            Mode::AbandonFirst { after_ms } => {
                let first =
                    tokio::time::timeout(ms(after_ms), next.generate(cx.clone(), pre(0))).await;
                if first.is_err() {
                    self.events.record("B:abandoned:0");
                }
                next.generate(cx, pre(1)).await?
            }
        };
        Ok(items(out.map(|item| item.map(|Out(s)| format!("b({s})")))))
    }
}

/// Retry while an attempt fails before its first item. `next` lives inside
/// the returned stream, which is the case the attempt rules must support.
fn retrying<N>(
    cx: Cx,
    next: N,
    pre: impl Fn(u32) -> Pre + Send + 'static,
    max: u32,
    events: Arc<Events>,
) -> Items<Out>
where
    N: Next<Pre, Out>,
{
    let (tx, rx) = mpsc::channel(4);
    tokio::spawn(async move {
        for attempt in 0..max {
            let mut out = match next.generate(cx.clone(), pre(attempt)).await {
                Ok(out) => out,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            match out.next().await {
                Some(Err(e)) if attempt + 1 < max => {
                    events.record(format!("B:retry:{attempt}:{}", e.kind()));
                    continue;
                }
                Some(first) => {
                    if tx.send(first).await.is_err() {
                        return;
                    }
                    while let Some(item) = out.next().await {
                        if tx.send(item).await.is_err() {
                            return;
                        }
                    }
                    return;
                }
                None => return,
            }
        }
    });
    receiver(rx)
}

/// Two concurrent attempts; the one that yields first wins and the loser is
/// dropped mid-flight, which must cancel only the loser.
async fn hedged<N>(
    cx: Cx,
    next: N,
    pre: impl Fn(u32) -> Pre,
    events: Arc<Events>,
) -> Result<Items<Out>>
where
    N: Next<Pre, Out>,
{
    let first_item = |attempt: u32| {
        let next = next.clone();
        let cx = cx.clone();
        let req = pre(attempt);
        Box::pin(async move {
            let mut out = next.generate(cx, req).await?;
            let first = out.next().await;
            Ok::<_, Error>((attempt, first, out))
        })
    };
    let (winner, _loser) = match futures::future::select(first_item(0), first_item(1)).await {
        futures::future::Either::Left((won, loser)) => (won?, loser),
        futures::future::Either::Right((won, loser)) => (won?, loser),
    };
    let (attempt, first, rest) = winner;
    events.record(format!("B:hedge:winner:{attempt}"));
    Ok(items(futures::stream::iter(first).chain(rest)))
}

pub struct NodeC {
    pub events: Arc<Events>,
}

impl Forward for NodeC {
    type In = Pre;
    type Out = Pre;

    async fn forward(&self, cx: &Cx, mut req: Pre) -> Result<Pre> {
        self.events.record(format!("C:{}", req.text));
        if req.plan.cancel_in_forward {
            cx.cancel();
        }
        if req.plan.forward_delay_ms > 0 {
            let abandoned = OnDrop::new(
                Arc::clone(&self.events),
                format!("C:abandoned:{}", req.text),
            );
            tokio::time::sleep(ms(req.plan.forward_delay_ms)).await;
            abandoned.disarm();
        }
        req.text = format!("c({})", req.text);
        Ok(req)
    }
}

pub struct NodeD {
    pub events: Arc<Events>,
}

impl Sink for NodeD {
    type Req = Pre;
    type Item = Raw;

    async fn generate(&self, cx: Cx, req: Pre) -> Result<Items<Raw>> {
        let key = format!("{}#{}", req.text, req.attempt);
        let events = Arc::clone(&self.events);
        events.publish_cx(format!("D:{key}"), &cx);
        events.record(format!("D:start:{key}"));
        let plan = req.plan;

        if plan.helper {
            let cx = cx.clone();
            let events = Arc::clone(&events);
            let key = key.clone();
            tokio::spawn(async move {
                cx.cancelled().await;
                events.record(format!("D:helper-ended:{key}"));
            });
        }
        if plan.fail_generate {
            events.record(format!("D:generate-failed:{key}"));
            return Err(Error::node("generate failed"));
        }
        if plan.stop_aware_start {
            tokio::select! {
                () = cx.stopping() => {
                    events.record(format!("D:stopped-at-start:{key}"));
                    return Ok(items(futures::stream::iter([Ok(Raw(STOP_MARK))])));
                }
                reason = cx.done() => return Err(reason),
            }
        }

        if let Some((attempt, delay)) = plan.slow_attempt
            && attempt == req.attempt
        {
            // Dropping a pending attempt drops this future rather than waking
            // it. The drop guard shows the future was destroyed; tests check
            // the published context to see that the attempt was cancelled.
            let abandoned = OnDrop::new(Arc::clone(&events), format!("D:abandoned:{key}"));
            tokio::select! {
                () = cx.cancelled() => return Err(Error::cancelled()),
                () = tokio::time::sleep(ms(delay)) => {}
            }
            abandoned.disarm();
        }
        if req.attempt < plan.fail_attempts {
            events.record(format!("D:fail:{key}"));
            return Ok(items(futures::stream::once(async {
                Err(Error::node("transient"))
            })));
        }

        let item_delay = match plan.slow_items {
            Some((attempt, delay)) if attempt == req.attempt => delay,
            _ => plan.item_delay_ms,
        };
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut index = 0;
            while plan.endless || index < plan.tokens {
                tokio::select! {
                    biased;
                    reason = cx.done() => {
                        events.record(format!("D:{}:{key}", end_label(&reason)));
                        return;
                    }
                    () = cx.stopping() => {
                        // Deliver the buffered tail, then end normally.
                        let _ = tx.send(Ok(Raw(STOP_MARK))).await;
                        events.record(format!("D:stopped:{key}"));
                        return;
                    }
                    // No timer when there is no delay: a zero sleep still
                    // waits for the next timer tick (about a millisecond).
                    () = pause(item_delay) => {}
                }
                let failing = plan.fail_at == Some(index);
                let item = if failing {
                    Err(Error::node(format!("D failed at {index}")))
                } else {
                    Ok(Raw(index))
                };
                if tx.send(item).await.is_err() {
                    events.record(format!("D:dropped:{key}"));
                    return;
                }
                if failing && plan.hold_after_error {
                    let reason = cx.done().await;
                    events.record(format!("D:{}:{key}", end_label(&reason)));
                    return;
                }
                index += 1;
            }
            if plan.hold_at_end {
                events.record(format!("D:held:{key}"));
                tokio::select! {
                    biased;
                    reason = cx.done() => {
                        events.record(format!("D:{}:{key}", end_label(&reason)));
                        return;
                    }
                    () = cx.stopping() => {
                        let _ = tx.send(Ok(Raw(STOP_MARK))).await;
                        events.record(format!("D:stopped:{key}"));
                        return;
                    }
                }
            }
            events.record(format!("D:done:{key}"));
        });
        Ok(receiver(rx))
    }
}

async fn pause(delay_ms: u64) {
    if delay_ms > 0 {
        tokio::time::sleep(ms(delay_ms)).await;
    }
}

fn end_label(reason: &Error) -> &'static str {
    match reason.kind() {
        ErrorKind::DeadlineExceeded => "deadline",
        _ => "cancelled",
    }
}

pub struct NodeF;

impl Backward for NodeF {
    type In = Raw;
    type Out = Tok;

    fn backward(&self, cx: &Cx, input: Items<Raw>) -> Items<Tok> {
        shaped(cx, "F", input, Tok("pf".into()), |Raw(i)| {
            Tok(format!("f{i}"))
        })
    }
}

pub struct NodeE;

impl Backward for NodeE {
    type In = Tok;
    type Out = Out;

    fn backward(&self, cx: &Cx, input: Items<Tok>) -> Items<Out> {
        shaped(cx, "E", input, Out("pe".into()), |Tok(s)| {
            Out(format!("e({s})"))
        })
    }
}

pub struct NodeG;

impl Backward for NodeG {
    type In = Chunk;
    type Out = Chunk;

    fn backward(&self, cx: &Cx, input: Items<Chunk>) -> Items<Chunk> {
        shaped(cx, "G", input, "pg".to_string(), |s| format!("g({s})"))
    }
}

/// A backward node's behavior, chosen by request metadata under its own name
/// so it crosses segments: `empty` ends at once, `fail` fails at once,
/// `ignore` emits one item without reading its input, `prefix` emits one item
/// before passing its input through, `discard` spends about 200 us on each
/// input item and emits none, as a filter that rejects everything does.
/// Anything else maps each item.
fn shaped<I: Send + 'static, O: Send + 'static>(
    cx: &Cx,
    name: &str,
    input: Items<I>,
    prefix: O,
    map: impl Fn(I) -> O + Send + Sync + 'static,
) -> Items<O> {
    match cx.meta().get(name).map(String::as_str) {
        Some("empty") => items(futures::stream::empty()),
        Some("fail") => {
            let error = Error::node(format!("{name} failed"));
            items(futures::stream::once(async move { Err(error) }))
        }
        Some("ignore") => items(futures::stream::once(async move { Ok(prefix) })),
        Some(mode) if mode.starts_with("watch:") => {
            // A helper started under the node's context, as a node that
            // offloads work would: it must end when that context does.
            let key = mode["watch:".len()..].to_string();
            let watched = cx.clone();
            tokio::spawn(async move {
                watched.cancelled().await;
                ended_helpers().lock().expect("helpers").insert(key);
            });
            items(input.map(move |item| item.map(&map)))
        }
        Some("discard") => items(input.filter_map(|_| {
            std::thread::sleep(Duration::from_micros(200));
            futures::future::ready(None)
        })),
        Some("prefix") => items(
            futures::stream::once(async move { Ok(prefix) })
                .chain(input.map(move |item| item.map(&map))),
        ),
        _ => items(input.map(move |item| item.map(&map))),
    }
}

/// Helpers started by `watch:<key>` mode whose node context has ended.
pub fn ended_helpers() -> &'static Mutex<std::collections::BTreeSet<String>> {
    static ENDED: std::sync::OnceLock<Mutex<std::collections::BTreeSet<String>>> =
        std::sync::OnceLock::new();
    ENDED.get_or_init(Mutex::default)
}

// ----- assembly -----

/// Hop 1 on its own (C, D, F, E), as B's `next`.
pub fn sink_hop(events: &Arc<Events>) -> impl Next<Pre, Out> {
    Hop::new(
        (
            Arc::new(NodeC {
                events: Arc::clone(events),
            }),
            (),
        ),
        SinkStage::new(Arc::new(NodeD {
            events: Arc::clone(events),
        })),
        (Arc::new(NodeF), (Arc::new(NodeE), ())),
    )
}

/// The whole reference graph in one segment.
pub fn local_graph(events: &Arc<Events>) -> Source<impl Next<Prompt, Chunk>> {
    let hop1 = Hop::new(
        (
            Arc::new(NodeC {
                events: Arc::clone(events),
            }),
            (),
        ),
        SinkStage::new(Arc::new(NodeD {
            events: Arc::clone(events),
        })),
        (Arc::new(NodeF), (Arc::new(NodeE), ())),
    );
    let hop0 = Hop::new(
        (),
        OperatorStage::new(
            Arc::new(NodeB {
                events: Arc::clone(events),
            }),
            hop1,
        ),
        (Arc::new(NodeG), ()),
    );
    Source::new(hop0)
}

/// What A sees for D items `0..n`.
pub fn expected(indices: impl IntoIterator<Item = u32>) -> Vec<Result<Chunk>> {
    indices
        .into_iter()
        .map(|i| Ok(format!("g(b(e(f{i})))")))
        .collect()
}

pub fn prompt(text: &str, plan: Plan) -> Prompt {
    Prompt {
        text: text.to_string(),
        plan,
    }
}

pub fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn receiver<T: Send + 'static>(rx: mpsc::Receiver<Result<T>>) -> Items<T> {
    items(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

/// Collect a stream with a bound, so a hang fails the test instead of the runner.
pub async fn collect<T>(stream: impl Stream<Item = Result<T>>) -> Vec<Result<T>> {
    tokio::time::timeout(WAIT, stream.collect::<Vec<_>>())
        .await
        .expect("stream did not end in time")
}

// ----- the reference graph as data -----

/// The reference manifest. `wire` decides which edge types are serializable,
/// so tests can make any edge non-wire.
pub fn reference_manifest(wire: impl Fn(&str) -> bool) -> gdl::Manifest {
    use gdl::{HopInfo, Manifest, NodeInfo, Role, TypeInfo};
    let node = |name: &str, role| NodeInfo {
        name: name.to_string(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let ty = |name: &str| TypeInfo::new(name, wire(name));
    let names = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    Manifest {
        name: "reference".to_string(),
        schema: 1,
        nodes: vec![
            node("A", Role::Source),
            node("B", Role::Operator),
            node("C", Role::Forward),
            node("D", Role::Sink),
            node("E", Role::Backward),
            node("F", Role::Backward),
            node("G", Role::Backward),
        ],
        hops: vec![
            HopInfo {
                outer: "A".to_string(),
                forward: vec![],
                inner: "B".to_string(),
                backward: names(&["G"]),
                forward_types: vec![ty("Prompt")],
                backward_types: vec![ty("Chunk"), ty("Chunk")],
            },
            HopInfo {
                outer: "B".to_string(),
                forward: names(&["C"]),
                inner: "D".to_string(),
                backward: names(&["F", "E"]),
                forward_types: vec![ty("Pre"), ty("Pre")],
                backward_types: vec![ty("Raw"), ty("Tok"), ty("Out")],
            },
        ],
    }
}

/// The reference graph's nodes.
pub const REFERENCE_NODES: [&str; 7] = ["A", "B", "C", "D", "E", "F", "G"];

/// Every set partition of `items`, as segment lists. Restricted growth
/// strings enumerate each partition exactly once (Bell(7) = 877).
pub fn partitions(items: &[&str]) -> Vec<Vec<Vec<String>>> {
    fn grow(
        items: &[&str],
        index: usize,
        blocks: &mut Vec<Vec<String>>,
        out: &mut Vec<Vec<Vec<String>>>,
    ) {
        if index == items.len() {
            out.push(blocks.clone());
            return;
        }
        for block in 0..blocks.len() {
            blocks[block].push(items[index].to_string());
            grow(items, index + 1, blocks, out);
            blocks[block].pop();
        }
        blocks.push(vec![items[index].to_string()]);
        grow(items, index + 1, blocks, out);
        blocks.pop();
    }
    let mut out = Vec::new();
    grow(items, 0, &mut Vec::new(), &mut out);
    out
}

/// A placement with one segment per block, named `s0`, `s1`, ...
pub fn placement_of(blocks: &[Vec<String>]) -> gdl::Placement {
    blocks
        .iter()
        .enumerate()
        .fold(gdl::Placement::new(), |placement, (index, nodes)| {
            placement.segment(format!("s{index}"), nodes.iter().cloned())
        })
}

// ----- distributed -----

use gdl::{
    BwdEnd, BwdStep, DOperator, DSink, Deployment, FwdEnd, FwdStep, Register, Roster,
    RuntimeConfig, Segment, wire_codec,
};

/// A velo node whose streams buffer very little: a stalled consumer blocks
/// its producer after a few small items instead of after a megabyte.
pub async fn tight_velo_node() -> velo::Velo {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let transport = velo::transports::tcp::TcpTransportBuilder::new()
        .from_listener(listener)
        .expect("listener")
        .build()
        .expect("transport");
    let mux = velo::streaming::MuxConfig {
        initial_credit: 4,
        slot_byte_budget: 1024,
        ..velo::streaming::MuxConfig::default()
    };
    velo::Velo::builder()
        .add_transport(Arc::new(transport))
        .mux_only()
        .messenger_mux(mux)
        .expect("mux config")
        .build()
        .await
        .expect("velo node")
}

/// A tight velo node (see [`tight_velo_node`]) that counts how often its
/// producers waited for credit, read through [`backpressured`].
pub async fn metered_tight_velo_node() -> (velo::Velo, prometheus::Registry) {
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(velo::VeloMetrics::register(&registry).expect("metrics"));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let transport = velo::transports::tcp::TcpTransportBuilder::new()
        .from_listener(listener)
        .expect("listener")
        .build()
        .expect("transport");
    let mux = velo::streaming::MuxConfig {
        initial_credit: 4,
        slot_byte_budget: 1024,
        ..velo::streaming::MuxConfig::default()
    };
    let node = velo::Velo::builder()
        .add_transport(Arc::new(transport))
        .mux_only()
        .messenger_mux(mux)
        .expect("mux config")
        .metrics(metrics)
        .build()
        .await
        .expect("velo node");
    (node, registry)
}

/// How many times the producers of a metered node waited for credit.
pub fn backpressured(registry: &prometheus::Registry) -> f64 {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == "velo_streaming_producer_send_backpressure_total")
        .and_then(|family| family.get_metric().first())
        .map_or(0.0, |metric| metric.get_counter().value())
}

/// A velo node on loopback TCP with OS-assigned ports.
pub async fn velo_node() -> velo::Velo {
    gdl::bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("velo node")
}

/// The reference graph as one segment sees it, with every node bound: a
/// "fat" build that can serve any segment of any placement.
pub fn reference_graph(
    deployment: &Deployment,
    events: &Arc<Events>,
) -> gdl::Result<impl Next<Prompt, Chunk> + Register + use<>> {
    let hop1 = deployment.hop(
        1,
        FwdStep::new(
            Arc::new(NodeC {
                events: Arc::clone(events),
            }),
            wire_codec!(Pre),
            FwdEnd::new(wire_codec!(Pre)),
        ),
        DSink::new(Arc::new(NodeD {
            events: Arc::clone(events),
        })),
        BwdStep::new(
            Arc::new(NodeF),
            wire_codec!(Raw),
            BwdStep::new(
                Arc::new(NodeE),
                wire_codec!(Tok),
                BwdEnd::new(wire_codec!(Out)),
            ),
        ),
    )?;
    deployment.hop(
        0,
        FwdEnd::new(wire_codec!(Prompt)),
        DOperator::new(
            Arc::new(NodeB {
                events: Arc::clone(events),
            }),
            hop1,
        ),
        BwdStep::new(
            Arc::new(NodeG),
            wire_codec!(Chunk),
            BwdEnd::new(wire_codec!(Chunk)),
        ),
    )
}

/// One running deployment of the reference graph: one segment per velo node.
pub struct Cluster<H> {
    pub segments: Vec<(String, Segment<H>)>,
}

/// The segment hosting `node`.
impl<H> Cluster<H> {
    pub fn segment(&self, name: &str) -> &Segment<H> {
        &self
            .segments
            .iter()
            .find(|(segment, _)| segment == name)
            .unwrap_or_else(|| panic!("no segment {name}"))
            .1
    }
}

/// Deploy the reference graph under `placement` on `nodes` (one node per
/// segment, in the placement's segment order).
pub fn deploy(
    nodes: &[velo::Velo],
    placement: &gdl::Placement,
    events: &Arc<Events>,
    config: RuntimeConfig,
) -> Cluster<impl Next<Prompt, Chunk> + Register + use<>> {
    let names: Vec<String> = placement
        .segments()
        .map(|(name, _)| name.to_string())
        .collect();
    assert!(names.len() <= nodes.len(), "not enough velo nodes");
    let instances: Vec<(String, velo::Velo)> =
        names.into_iter().zip(nodes.iter().cloned()).collect();
    deploy_instances(&instances, placement, events, config)
}

/// Deploy the reference graph with explicit instances: each entry is one
/// velo node serving the named segment. A segment may have several.
pub fn deploy_instances(
    instances: &[(String, velo::Velo)],
    placement: &gdl::Placement,
    events: &Arc<Events>,
    config: RuntimeConfig,
) -> Cluster<impl Next<Prompt, Chunk> + Register + use<>> {
    deploy_configured(instances, placement, events, &|_| config.clone())
}

/// Deploy the reference graph with each segment's runtime settings chosen
/// by `config`.
pub fn deploy_configured(
    instances: &[(String, velo::Velo)],
    placement: &gdl::Placement,
    events: &Arc<Events>,
    config: &dyn Fn(&str) -> RuntimeConfig,
) -> Cluster<impl Next<Prompt, Chunk> + Register + use<>> {
    let manifest = reference_manifest(|_| true);
    let mut roster = Roster::new();
    for (name, node) in instances {
        roster.add(name.clone(), node.peer_info());
    }
    let segments = instances
        .iter()
        .map(|(name, node)| {
            let deployment = Deployment::new(
                node.clone(),
                &manifest,
                placement,
                name,
                &roster,
                config(name),
            )
            .expect("deployment");
            let root = reference_graph(&deployment, events).expect("graph");
            (name.clone(), deployment.start(root).expect("start"))
        })
        .collect();
    Cluster { segments }
}

/// Wait until every instance has released everything requests took.
pub async fn settles<H>(cluster: &Cluster<H>) -> bool {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let busy = cluster.segments.iter().any(|(_, s)| {
            s.admitted() > 0
                || s.attempting() > 0
                || s.active_anchors() > 0
                || s.egress_in_use() > 0
        });
        if !busy {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(ms(5)).await;
    }
}

/// The segment that hosts the source.
pub fn source_segment(placement: &gdl::Placement) -> String {
    placement
        .segments()
        .find(|(_, nodes)| nodes.iter().any(|n| n == "A"))
        .map(|(name, _)| name.to_string())
        .expect("A is placed")
}
