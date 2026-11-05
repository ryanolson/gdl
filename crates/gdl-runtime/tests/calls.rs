//! Calls to a unary service in another segment: one velo call to the
//! service segment's `gdl.call` handler, bounded by the request's deadline
//! or `call_timeout`. The caller's segment and the service's run on velo
//! nodes in this process, over TCP on OS-assigned ports. Each request says
//! what the service does with it, so one service serves every test, and
//! the service reports what it saw on a channel, which the tests wait on.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{WAIT, velo_node};
use futures::StreamExt;
use gdl::{Affinity, AffinityConfig, Choice, Instances, Projected, Projection, Request, Selector};
use gdl::{Build, Cx, Deadline, Error, ErrorKind, Items, Next, Operator, Result, Sink, Unary};
use gdl::{UnaryClient, items};
use gdl_runtime::{Placement, Roster, RuntimeConfig, Segment, wire_codec};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

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
        name = "calls", schema = 1;
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

/// What the service saw: a run that began, with its context's deadline;
/// and a worker it started that saw its context cancelled, and whether the
/// deadline had passed by then.
#[derive(Debug)]
enum Seen {
    Began(Option<Deadline>),
    Cancelled { expired: bool },
}

/// One instance of `Tok`. By the request's first word:
/// - `told <text>` answers `told <text>`;
/// - `busy` fails with `Overloaded`, after it ran;
/// - `big <n>` answers `n` bytes;
/// - `hold` starts a worker that waits for its context's cancellation,
///   and never answers;
/// - `spawn` starts that worker and answers at once;
/// - `meta <key>` answers with the value of its context's metadata entry
///   `key`, or `none`.
struct Service {
    runs: AtomicUsize,
    seen: mpsc::UnboundedSender<Seen>,
}

impl Service {
    fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<Seen>) {
        let (seen, rx) = mpsc::unbounded_channel();
        let service = Arc::new(Self {
            runs: AtomicUsize::new(0),
            seen,
        });
        (service, rx)
    }

    fn watch(&self, cx: &Cx) {
        let (cx, seen) = (cx.clone(), self.seen.clone());
        tokio::spawn(async move {
            cx.cancelled().await;
            let expired = cx.deadline().is_some_and(Deadline::is_expired);
            let _ = seen.send(Seen::Cancelled { expired });
        });
    }
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
        self.runs.fetch_add(1, Ordering::Relaxed);
        let _ = self.seen.send(Seen::Began(cx.deadline()));
        let (word, rest) = req.text.split_once(' ').unwrap_or((&req.text, ""));
        match word {
            "told" => Ok(Told {
                text: format!("told {rest}"),
            }),
            "busy" => Err(Error::new(ErrorKind::Overloaded, "busy downstream")),
            "big" => Ok(Told {
                text: "x".repeat(rest.parse().expect("a size")),
            }),
            "hold" => {
                self.watch(cx);
                std::future::pending().await
            }
            "meta" => Ok(Told {
                text: cx
                    .meta()
                    .get(rest)
                    .cloned()
                    .unwrap_or_else(|| "none".into()),
            }),
            "spawn" => {
                self.watch(cx);
                Ok(Told {
                    text: "spawned".to_string(),
                })
            }
            other => panic!("no request `{other}`"),
        }
    }
}

/// Asks `Tok` about each request, and sends the request on with its answer.
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
        let told = self.tok.call(&cx, req.clone()).await?;
        next.generate(cx, ask(&format!("{} [{}]", req.text, told.text)))
            .await
    }
}

/// Answers with its request's text.
struct Echo;

impl Sink for Echo {
    type Req = Ask;
    type Item = Told;

    async fn generate(&self, _cx: Cx, req: Ask) -> Result<Items<Told>> {
        Ok(items(futures::stream::iter([Ok(Told {
            text: format!("echo {}", req.text),
        })])))
    }
}

/// The router's segment and the tokenizer's own.
const SPLIT: &str = "[segment.front]\nnodes = ['Front', 'Router', 'Echo']\n\
                     [segment.tok]\nnodes = ['Tok']\n";

fn placement(text: &str) -> Placement {
    Placement::from_toml(text).expect("a placement")
}

/// A started front, with a client of `Tok` beside its router's, and the
/// started instances of `tok`, each with its service and what it saw.
struct Deployed<F, T> {
    front: Segment<F>,
    client: UnaryClient<Ask, Told>,
    toks: Vec<Tok<T>>,
}

struct Tok<T> {
    service: Arc<Service>,
    seen: mpsc::UnboundedReceiver<Seen>,
    _segment: Segment<T>,
}

impl<T> Tok<T> {
    async fn next(&mut self) -> Seen {
        tokio::time::timeout(WAIT, self.seen.recv())
            .await
            .expect("the service saw something in time")
            .expect("the service is there")
    }
}

/// One front under placement `front_at`, and `toks` instances of `tok`
/// under `tok_at`, each with its settings. The front's links are made
/// before its deployment, as a segment binary makes them before it builds
/// its nodes.
async fn deploy(
    toks: usize,
    front_at: &str,
    tok_at: &str,
    front_config: RuntimeConfig,
    tok_config: RuntimeConfig,
) -> Deployed<impl Next<Ask, Told> + gdl_runtime::Register + use<>, impl Sized + use<>> {
    let manifest = contract::manifest();
    let front_node = velo_node().await;
    let mut tok_nodes = Vec::new();
    for _ in 0..toks {
        tok_nodes.push(velo_node().await);
    }
    let mut roster = Roster::new();
    roster.add("front".to_string(), front_node.peer_info());
    for node in &tok_nodes {
        roster.add("tok".to_string(), node.peer_info());
    }
    let mut deployed = Vec::new();
    for node in tok_nodes {
        let (service, seen) = Service::new();
        let deployment = gdl_runtime::Deployment::new(
            node,
            &manifest,
            &placement(tok_at),
            "tok",
            &roster,
            tok_config.clone(),
        )
        .expect("the tok deployment");
        let root = contract::assemble(
            &deployment,
            contract::Bindings::new().tok(Arc::clone(&service)),
        )
        .expect("assembled");
        deployed.push(Tok {
            service,
            seen,
            _segment: deployment.start(root).expect("started"),
        });
    }
    let link = |config: &RuntimeConfig| {
        config
            .calls
            .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
            .expect("a link")
    };
    let (router, client) = (link(&front_config), link(&front_config));
    let deployment = gdl_runtime::Deployment::new(
        front_node,
        &manifest,
        &placement(front_at),
        "front",
        &roster,
        front_config,
    )
    .expect("the front deployment");
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new()
            .router(Router { tok: router })
            .echo(Echo),
    )
    .expect("assembled");
    Deployed {
        front: deployment.start(root).expect("started"),
        client,
        toks: deployed,
    }
}

async fn split()
-> Deployed<impl Next<Ask, Told> + gdl_runtime::Register + use<>, impl Sized + use<>> {
    deploy(
        1,
        SPLIT,
        SPLIT,
        RuntimeConfig::default(),
        RuntimeConfig::default(),
    )
    .await
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("done in time")
}

/// A call reaches the service in the other segment and its answer comes
/// back; through the router, the graph answers with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_reaches_the_service_in_another_segment_and_its_answer_returns() {
    let deployed = split().await;
    let told = bounded(deployed.client.call(&Cx::new(), ask("told hi")))
        .await
        .expect("answered");
    assert_eq!(told.text, "told hi");
    let items = bounded(deployed.front.call::<Ask, Told>(Cx::new(), ask("told yo")))
        .await
        .expect("a stream");
    let items: Vec<Result<Told>> = bounded(items.collect()).await;
    let texts: Vec<String> = items
        .into_iter()
        .map(|item| item.expect("an item").text)
        .collect();
    assert_eq!(texts, ["echo told yo [told yo]"]);
    assert_eq!(deployed.toks[0].service.runs.load(Ordering::Relaxed), 2);
}

/// Before its deployment starts, a link's call fails at once with
/// `Routing`: as made during the build, then, once the nodes are built, as
/// made before the launch.
#[tokio::test]
async fn a_call_before_the_segment_starts_fails_at_once_with_routing() {
    let calls = gdl_runtime::Calls::default();
    let client = calls
        .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
        .expect("a link");
    let error = client
        .call(&Cx::new(), ask("told hi"))
        .await
        .expect_err("not started");
    assert_eq!(error.kind(), ErrorKind::Routing);
    assert_eq!(error.message(), "called during build");
    calls.built();
    let error = client
        .call(&Cx::new(), ask("told hi"))
        .await
        .expect_err("not started");
    assert_eq!(error.kind(), ErrorKind::Routing);
    assert_eq!(error.message(), "called before the segment launched");
}

/// A call from a deployment of another placement reaches the handler,
/// which refuses it with `Placement` before the service runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_from_another_placement_is_refused_with_placement() {
    let other = format!("{SPLIT}[routing]\nTok = 'random'\n");
    let deployed = deploy(
        1,
        &other,
        SPLIT,
        RuntimeConfig::default(),
        RuntimeConfig::default(),
    )
    .await;
    let error = bounded(deployed.client.call(&Cx::new(), ask("told hi")))
        .await
        .expect_err("another placement");
    assert_eq!(error.kind(), ErrorKind::Placement, "{error}");
    assert_eq!(deployed.toks[0].service.runs.load(Ordering::Relaxed), 0);
}

/// A call whose request has a deadline is bound by it: the service runs
/// under a context with that deadline, and once it passes the caller gets
/// `DeadlineExceeded` and the service's context is cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_passed_bound_ends_the_call_with_deadline_exceeded_and_the_service_sees_it() {
    let mut deployed = split().await;
    let cx = Cx::new().with_timeout(Duration::from_millis(300));
    let error = bounded(deployed.client.call(&cx, ask("hold")))
        .await
        .expect_err("past its bound");
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");
    let tok = &mut deployed.toks[0];
    let Seen::Began(Some(seen)) = tok.next().await else {
        panic!("the service ran without a deadline");
    };
    // The deadline crossed as wall-clock time: the same instant.
    assert_eq!(
        seen.unix_nanos(),
        cx.deadline().expect("a deadline").unix_nanos()
    );
    assert!(
        matches!(tok.next().await, Seen::Cancelled { expired: true }),
        "the service's context is cancelled at the bound"
    );
}

/// A request with no deadline is bound by `call_timeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_without_a_deadline_is_bound_by_call_timeout() {
    let config = RuntimeConfig {
        call_timeout: Duration::from_millis(300),
        ..RuntimeConfig::default()
    };
    let mut deployed = deploy(1, SPLIT, SPLIT, config, RuntimeConfig::default()).await;
    let error = bounded(deployed.client.call(&Cx::new(), ask("hold")))
        .await
        .expect_err("past its bound");
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");
    let tok = &mut deployed.toks[0];
    assert!(matches!(tok.next().await, Seen::Began(Some(_))));
    assert!(matches!(
        tok.next().await,
        Seen::Cancelled { expired: true }
    ));
}

/// The known limit: a caller that drops its call tells the service
/// nothing. The service runs on until the call's bound, and only then is
/// its context cancelled (docs/src/operations/limitations.md).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_call_leaves_the_service_running_until_its_bound() {
    let mut deployed = split().await;
    let cx = Cx::new().with_timeout(Duration::from_millis(500));
    {
        let call = deployed.client.call(&cx, ask("hold"));
        let mut call = std::pin::pin!(call);
        let tok = &mut deployed.toks[0];
        tokio::select! {
            _ = call.as_mut() => panic!("the call ended"),
            seen = tok.next() => assert!(matches!(seen, Seen::Began(_))),
        }
        // Dropped here, while the service runs.
    }
    let seen = deployed.toks[0].next().await;
    assert!(
        matches!(seen, Seen::Cancelled { expired: true }),
        "the service ran until its bound, not until the drop: {seen:?}"
    );
}

/// However a call ends, the service's context is cancelled: a worker it
/// started with a clone of the context ends with the call, long before
/// the call's bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_the_service_started_ends_with_its_call() {
    let mut deployed = split().await;
    let told = bounded(deployed.client.call(&Cx::new(), ask("spawn")))
        .await
        .expect("answered");
    assert_eq!(told.text, "spawned");
    let tok = &mut deployed.toks[0];
    assert!(matches!(tok.next().await, Seen::Began(Some(_))));
    assert!(
        matches!(tok.next().await, Seen::Cancelled { expired: false }),
        "cancelled when the call ended, not at its bound"
    );
}

/// An error from a service that ran reaches the caller as it is, whatever
/// its kind, and the call is not made again: not on the same instance, and
/// not on the other one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_completed_error_reaches_the_caller_as_it_is() {
    let deployed = deploy(
        2,
        SPLIT,
        SPLIT,
        RuntimeConfig::default(),
        RuntimeConfig::default(),
    )
    .await;
    let error = bounded(deployed.client.call(&Cx::new(), ask("busy")))
        .await
        .expect_err("busy");
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    assert_eq!(error.message(), "busy downstream");
    let runs: usize = deployed
        .toks
        .iter()
        .map(|tok| tok.service.runs.load(Ordering::Relaxed))
        .sum();
    assert_eq!(runs, 1, "the service ran once");
}

/// Calls go round the service's instances in turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_go_round_the_services_instances() {
    let deployed = deploy(
        2,
        SPLIT,
        SPLIT,
        RuntimeConfig::default(),
        RuntimeConfig::default(),
    )
    .await;
    for n in 0..4 {
        bounded(deployed.client.call(&Cx::new(), ask(&format!("told {n}"))))
            .await
            .expect("answered");
    }
    for tok in &deployed.toks {
        assert_eq!(tok.service.runs.load(Ordering::Relaxed), 2);
    }
}

/// A reply larger than the service segment's `call_reply_max` fails the
/// call with `Codec`, naming the service and the size: the service ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_over_call_reply_max_fails_the_call_with_codec() {
    let config = RuntimeConfig {
        call_reply_max: 1024,
        ..RuntimeConfig::default()
    };
    let deployed = deploy(1, SPLIT, SPLIT, RuntimeConfig::default(), config).await;
    let told = bounded(deployed.client.call(&Cx::new(), ask("big 900")))
        .await
        .expect("under the limit");
    assert_eq!(told.text.len(), 900);
    let error = bounded(deployed.client.call(&Cx::new(), ask("big 2000")))
        .await
        .expect_err("over the limit");
    assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
    assert!(
        error.message().contains("the reply of unary node `Tok` is")
            && error.message().contains("bytes, over `call_reply_max`"),
        "{error}"
    );
    assert_eq!(deployed.toks[0].service.runs.load(Ordering::Relaxed), 2);
}

/// `call_reply_max` is 8 MiB unless set, and must leave room in velo's
/// 16 MiB frame for the reply's tag and velo's own header: at most 16 MiB
/// less 64 KiB. It must also hold the error that replaces a reply past it:
/// at least `CALL_REPLY_MIN`. `call_timeout` is 30 s and must be positive.
#[test]
fn the_call_settings_default_and_are_bounded() {
    let config = RuntimeConfig::default();
    assert_eq!(config.call_reply_max, 8 << 20);
    assert_eq!(config.call_timeout, Duration::from_secs(30));
    let read = |text: &str| toml::from_str::<RuntimeConfig>(text);
    let set = read("call_timeout_ms = 500\ncall_reply_max = 4096\n").expect("settings");
    assert_eq!(set.call_timeout, Duration::from_millis(500));
    assert_eq!(set.call_reply_max, 4096);
    let most = (16 << 20) - (64 << 10);
    assert_eq!(
        read(&format!("call_reply_max = {most}\n"))
            .expect("the most")
            .call_reply_max,
        most
    );
    assert!(read(&format!("call_reply_max = {}\n", most + 1)).is_err());
    // The least: room for the error that replaces a reply past the limit.
    let least = RuntimeConfig::CALL_REPLY_MIN;
    assert_eq!(
        read(&format!("call_reply_max = {least}\n"))
            .expect("the least")
            .call_reply_max,
        least
    );
    let refused = read(&format!("call_reply_max = {}\n", least - 1)).expect_err("below the least");
    assert!(
        refused
            .to_string()
            .contains(&format!("must be from {least} to")),
        "{refused}"
    );
    for refused in [
        "call_reply_max = 16777216\n",
        "call_reply_max = 0\n",
        "call_timeout_ms = 0\n",
    ] {
        assert!(read(refused).is_err(), "{refused}");
    }
}

/// The router and the tokenizer's own segment, with calls to `Tok` routed
/// through selector `name`.
fn routed(name: &str) -> String {
    format!("{SPLIT}[routing]\nTok = {{ selector = '{name}' }}\n")
}

/// Front settings that route through `affinity` keyed on the `session`
/// metadata entry, as `[selector.affinity] key = "session"` builds it.
fn by_session() -> RuntimeConfig {
    let mut config = RuntimeConfig::default();
    config
        .selectors
        .add(
            "affinity",
            Affinity::new(AffinityConfig {
                key: Some("session".to_string()),
                through: false,
            }),
        )
        .expect("registered");
    config
}

/// How many calls each instance of `Tok` ran.
fn runs<T>(toks: &[Tok<T>]) -> Vec<usize> {
    toks.iter()
        .map(|tok| tok.service.runs.load(Ordering::Relaxed))
        .collect()
}

/// `affinity` keyed on `session` sends every call of one session to one
/// instance, whatever its text: the key is the metadata entry the call
/// carries from the caller's context.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn affinity_sends_every_call_of_a_session_to_one_instance() {
    let deployed = deploy(
        3,
        &routed("affinity"),
        &routed("affinity"),
        by_session(),
        RuntimeConfig::default(),
    )
    .await;
    let mut cx = Cx::new();
    cx.insert_meta("session", "s-1");
    for n in 0..64 {
        bounded(deployed.client.call(&cx, ask(&format!("told {n} words"))))
            .await
            .expect("answered");
    }
    let mut runs = runs(&deployed.toks);
    runs.sort_unstable();
    assert_eq!(runs, [0, 0, 64], "one instance ran every call");
}

/// The control: without the entry, `affinity` hashes the whole encoded
/// request, so calls with other texts reach other instances.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn affinity_without_the_entry_hashes_the_whole_request() {
    let deployed = deploy(
        3,
        &routed("affinity"),
        &routed("affinity"),
        by_session(),
        RuntimeConfig::default(),
    )
    .await;
    for n in 0..64 {
        bounded(
            deployed
                .client
                .call(&Cx::new(), ask(&format!("told {n} words"))),
        )
        .await
        .expect("answered");
    }
    let runs = runs(&deployed.toks);
    assert_eq!(runs.iter().sum::<usize>(), 64);
    assert!(
        runs.iter().filter(|&&ran| ran > 0).count() > 1,
        "the calls spread: {runs:?}"
    );
}

/// The part of a call's request that [`ByWord`] reads.
#[derive(Deserialize, Projection)]
#[gdl(of = Ask)]
struct Text {
    text: String,
}

/// Sends a call whose text starts `told a` to the instance with the lowest
/// id, and any other to the one with the highest: it reads the call's
/// request, through a projection of one of its fields.
struct ByWord;

impl Selector for ByWord {
    type Head = Projected<Text>;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Self::Head>,
        instances: Instances<'a>,
    ) -> Result<Choice<'a>> {
        let low = request.head().text.starts_with("told a");
        let chosen = if low {
            instances.iter().min_by_key(|instance| instance.id())
        } else {
            instances.iter().max_by_key(|instance| instance.id())
        };
        Ok(Choice::new(chosen.expect("an instance")))
    }
}

/// A selector sees the call's request as its head: one that reads a field
/// of it through a projection routes each call by that field.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selector_reads_a_field_of_the_calls_request() {
    let mut front = RuntimeConfig::default();
    front.selectors.add("by_word", ByWord).expect("registered");
    let deployed = deploy(
        2,
        &routed("by_word"),
        &routed("by_word"),
        front,
        RuntimeConfig::default(),
    )
    .await;
    for word in ["a", "b", "a", "b", "a"] {
        bounded(
            deployed
                .client
                .call(&Cx::new(), ask(&format!("told {word}"))),
        )
        .await
        .expect("answered");
    }
    let mut runs = runs(&deployed.toks);
    runs.sort_unstable();
    assert_eq!(runs, [2, 3], "each word went to one instance");
}

/// Sends every call to the first instance, tells the service `picked = x`,
/// and leases a [`Held`] for each call.
struct Tagging(Arc<AtomicUsize>);

/// Counts itself among the leases its selector gave while it lives.
struct Held(Arc<AtomicUsize>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Selector for Tagging {
    type Head = gdl::Encoded;

    async fn select<'a>(
        &'a self,
        _: Request<'a, Self::Head>,
        instances: Instances<'a>,
    ) -> Result<Choice<'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Choice::new(instances.get(0).expect("an instance"))
            .with_meta("picked", "x")
            .with_lease(Held(Arc::clone(&self.0))))
    }
}

/// What a selector's choice adds to a call's metadata reaches the
/// service's context, and its lease is held while the call runs and
/// dropped once it ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selectors_meta_reaches_the_service_and_its_lease_lasts_the_call() {
    let held = Arc::new(AtomicUsize::new(0));
    let mut front = RuntimeConfig::default();
    front
        .selectors
        .add("tagging", Tagging(Arc::clone(&held)))
        .expect("registered");
    let mut deployed = deploy(
        1,
        &routed("tagging"),
        &routed("tagging"),
        front,
        RuntimeConfig::default(),
    )
    .await;
    let told = bounded(deployed.client.call(&Cx::new(), ask("meta picked")))
        .await
        .expect("answered");
    assert_eq!(told.text, "x", "the service saw the selector's meta");
    assert_eq!(held.load(Ordering::SeqCst), 0, "dropped with the reply");
    assert!(matches!(deployed.toks[0].next().await, Seen::Began(_)));

    let cx = Cx::new().with_timeout(Duration::from_millis(500));
    let call = deployed.client.call(&cx, ask("hold"));
    let mut call = std::pin::pin!(call);
    tokio::select! {
        _ = call.as_mut() => panic!("the call ended"),
        seen = deployed.toks[0].next() => assert!(matches!(seen, Seen::Began(_))),
    }
    assert_eq!(held.load(Ordering::SeqCst), 1, "held while the call runs");
    let error = bounded(call).await.expect_err("past its bound");
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");
    assert_eq!(
        held.load(Ordering::SeqCst),
        0,
        "dropped when the call ended"
    );
}

/// Reads the head as a type other than the call's request.
struct ByNumber;

impl Selector for ByNumber {
    type Head = u64;

    async fn select<'a>(
        &'a self,
        _: Request<'a, Self::Head>,
        instances: Instances<'a>,
    ) -> Result<Choice<'a>> {
        Ok(Choice::new(instances.get(0).expect("an instance")))
    }
}

/// A selector that reads the head as another type than the call's request
/// is refused when the segment starts, naming both, as a crossing's is.
#[tokio::test]
async fn a_selector_that_reads_another_type_than_the_request_is_refused() {
    let node = velo_node().await;
    let mut roster = Roster::new();
    roster.add("front".to_string(), node.peer_info());
    let mut config = RuntimeConfig::default();
    config
        .selectors
        .add("by_number", ByNumber)
        .expect("registered");
    let link = config
        .calls
        .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
        .expect("a link");
    let deployment = gdl_runtime::Deployment::new(
        node,
        &contract::manifest(),
        &placement(&routed("by_number")),
        "front",
        &roster,
        config,
    )
    .expect("the selector resolves");
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new()
            .router(Router { tok: link })
            .echo(Echo),
    )
    .expect("assembled");
    let error = deployment.start(root).err().expect("refused");
    assert_eq!(error.kind(), ErrorKind::Placement, "{error}");
    assert!(
        error
            .message()
            .contains("selector `by_number` reads the head as `u64`")
            && error.message().contains("unary node `Tok`"),
        "{error}"
    );
}

/// A segment whose calls the placement routes through a selector that
/// nothing registers, and gdl does not have, is refused when it is made,
/// naming the call.
#[tokio::test]
async fn a_selector_on_calls_that_nothing_registers_is_refused() {
    let node = velo_node().await;
    let mut roster = Roster::new();
    roster.add("front".to_string(), node.peer_info());
    let error = gdl_runtime::Deployment::new(
        node,
        &contract::manifest(),
        &placement(&routed("by_text")),
        "front",
        &roster,
        RuntimeConfig::default(),
    )
    .err()
    .expect("refused");
    assert_eq!(error.kind(), ErrorKind::Placement);
    assert!(
        error.message().contains(
            "segment `front` calls unary node `Tok` through selector `by_text`, which nothing registers and gdl does not have"
        ),
        "{error}"
    );
}

/// A segment's name as its hub services spell it.
fn hex(segment: &str) -> String {
    segment.bytes().map(|b| format!("{b:02x}")).collect()
}

/// A remote call is a send: the caller's segment waits at its serving
/// barrier for the service's segment, which registers `gdl.call` before it
/// says it serves. Held between loaded and that registration, the service's
/// segment keeps the caller's from serving; once it starts and serves, the
/// caller's serves too. Both instances and the hub run in this process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_callers_segment_serves_only_once_the_services_segment_does() {
    use common::pools::settings;
    use velo::discovery::ServiceDiscovery;

    const SEGMENTS: &[&str] = &["front", "tok"];
    let spec =
        gdl_runtime::DeploymentSpec::new(contract::manifest(), placement(SPLIT)).expect("a spec");
    let hub = velo_hub::Hub::builder(common::pools::hub_config())
        .extension(gdl_hub::GraphAdmission::new(spec.clone()))
        .serve()
        .await
        .expect("the hub serves");
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        "",
    );
    let (front_process, tok_process) = tokio::join!(
        gdl_runtime::Process::start(&config),
        gdl_runtime::Process::start(&config)
    );
    let (front_process, tok_process) = (front_process.expect("front"), tok_process.expect("tok"));
    let (front, tok) = tokio::join!(
        config.launch(&front_process, "front", SEGMENTS, &spec, async { Ok(()) }),
        config.launch(&tok_process, "tok", SEGMENTS, &spec, async { Ok(()) }),
    );
    let (front, tok) = (front.expect("front loaded"), tok.expect("tok loaded"));

    // The front starts and serves; `tok` is loaded, and goes no further.
    let runtime = config.runtime.clone();
    let router = runtime
        .calls
        .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
        .expect("a link");
    let deployment = gdl_runtime::Deployment::new(
        front.node(),
        &spec.graph,
        &spec.placement,
        "front",
        front.roster(),
        runtime,
    )
    .expect("the front deployment");
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new()
            .router(Router { tok: router })
            .echo(Echo),
    )
    .expect("assembled");
    let started = deployment.start(root).expect("started");
    let (front_serves, mut front_served) = tokio::sync::oneshot::channel();
    let front_serving = tokio::spawn(front.serve(started, |segment, shutdown| async move {
        let answer = segment.call::<Ask, Told>(Cx::new(), ask("told hi")).await;
        let _ = front_serves.send(answer.is_ok());
        shutdown.cancelled().await;
        Ok(())
    }));

    // The front has said it serves, so it is at its barrier.
    let services = velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("a hub client")
        .services();
    let serving = format!("gdl-serving-{}", hex("front"));
    let deadline = tokio::time::Instant::now() + WAIT;
    while services
        .get_instances(&serving)
        .await
        .unwrap_or_default()
        .is_empty()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the front never said it serves"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Its body does not run while `tok` has no handler: the barrier waits.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut front_served)
            .await
            .is_err(),
        "the front served before the service's segment did"
    );

    // `tok` registers its handler, then serves: the front's body runs, and
    // its first call is answered.
    let (service, _seen) = Service::new();
    let deployment = gdl_runtime::Deployment::new(
        tok.node(),
        &spec.graph,
        &spec.placement,
        "tok",
        tok.roster(),
        // Its own: a clone of the front's would share the front's links.
        RuntimeConfig::default(),
    )
    .expect("the tok deployment");
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new().tok(Arc::clone(&service)),
    )
    .expect("assembled");
    let started = deployment.start(root).expect("started");
    let tok_serving = tokio::spawn(tok.serve(started, |_segment, shutdown| async move {
        shutdown.cancelled().await;
        Ok(())
    }));
    let answered = bounded(front_served).await.expect("the front's body ran");
    assert!(answered, "the front's first call was answered");

    front_process.stop();
    tok_process.stop();
    let _ = bounded(front_serving).await;
    let _ = bounded(tok_serving).await;
}

/// The service's segment holds its hosted services, and its handler only
/// reaches them: once the segment is dropped, so is the service, though
/// velo keeps the handler as long as its node runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hosted_service_is_dropped_with_its_segment() {
    let deployed = split().await;
    bounded(deployed.client.call(&Cx::new(), ask("told hi")))
        .await
        .expect("answered");
    let Deployed {
        front, mut toks, ..
    } = deployed;
    let tok = toks.pop().expect("one instance");
    let service = Arc::downgrade(&tok.service);
    drop(tok);
    assert!(
        service.upgrade().is_none(),
        "the service outlived its segment"
    );
    drop(front);
}
