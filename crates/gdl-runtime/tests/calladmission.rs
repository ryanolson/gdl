//! A call to a unary service in another segment takes a permit of the
//! service segment's `call_limit` before the service runs, apart from the
//! requests' admission, and its reply holds the permit until velo lets the
//! reply's bytes go, or until the call's bound plus `call_reply_grace`. A
//! full or closed instance refuses a call, and the caller tries the next
//! instance; a call that may have run is tried again only when the graph
//! marks the service `idempotent`. The caller's segment and the service's
//! run on velo nodes in this process, over TCP on OS-assigned ports. Each
//! wait is for something observed, and bounded.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{WAIT, velo_node};
use futures::StreamExt;
use gdl::{Build, Cx, Deadline, Error, ErrorKind, Items, Next, Operator, Result, Sink, Unary};
use gdl::{UnaryClient, items};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, Segment, wire_codec};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// A request: what each node does is its first word.
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
        name = "call-admission", schema = 1;
        unary    Tok: Ask -> Told;
        source   Front: Ask -> stream Told;
        operator Router: (Ask -> stream Told) over (Ask -> stream Told) calls Tok;
        sink     Echo: Ask -> stream Told;
        topology: Front => Router { Echo };
    }
}

/// The same graph, its service marked `idempotent`.
mod marked {
    use super::{Ask, Told};

    gdl_runtime::graph! {
        name = "call-admission", schema = 1;
        unary    Tok: Ask -> Told idempotent;
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

/// Points a node waits at until the test opens them, by key.
fn gates() -> &'static (Mutex<BTreeSet<String>>, Notify) {
    static GATES: std::sync::OnceLock<(Mutex<BTreeSet<String>>, Notify)> =
        std::sync::OnceLock::new();
    GATES.get_or_init(Default::default)
}

fn open(key: &str) {
    let (opened, changed) = gates();
    opened.lock().expect("gates").insert(key.to_string());
    changed.notify_waiters();
}

/// Wait until the test opens `key`, or a bound past every wait here.
async fn gate(key: &str) {
    let (opened, changed) = gates();
    let _ = tokio::time::timeout(WAIT * 4, async {
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if opened.lock().expect("gates").contains(key) {
                return;
            }
            notified.await;
        }
    })
    .await;
}

/// A count that a test can wait on.
#[derive(Default)]
struct Count {
    n: AtomicUsize,
    changed: Notify,
}

impl Count {
    fn add(&self, by: isize) {
        if by >= 0 {
            self.n.fetch_add(by.unsigned_abs(), Ordering::SeqCst);
        } else {
            self.n.fetch_sub(by.unsigned_abs(), Ordering::SeqCst);
        }
        self.changed.notify_waiters();
    }

    fn get(&self) -> usize {
        self.n.load(Ordering::SeqCst)
    }

    /// Wait, bounded, until the count is `n`.
    async fn reaches(&self, n: usize) {
        tokio::time::timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.get() == n {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the count reached {n} in time; it is {}", self.get()));
    }
}

/// One instance of `Tok`. By the request's first word:
/// - `told <text>` answers `told <text>`;
/// - `busy` fails with `Overloaded`, after it ran;
/// - `gate <key>` waits until the test opens `key`, then answers.
#[derive(Default)]
struct Service {
    runs: Count,
    running: Count,
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

    async fn call(&self, _cx: &Cx, req: Ask) -> Result<Told> {
        self.runs.add(1);
        let (word, rest) = req.text.split_once(' ').unwrap_or((&req.text, ""));
        match word {
            "told" => Ok(Told {
                text: format!("told {rest}"),
            }),
            "busy" => Err(Error::new(ErrorKind::Overloaded, "busy downstream")),
            "gate" => {
                self.running.add(1);
                gate(rest).await;
                self.running.add(-1);
                Ok(Told {
                    text: format!("gated {rest}"),
                })
            }
            other => panic!("no request `{other}`"),
        }
    }
}

/// Asks `Tok` about each request, unless its first word is `pass`, and
/// sends it on.
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
        if !req.text.starts_with("pass") {
            self.tok.call(&cx, req.clone()).await?;
        }
        next.generate(cx, req).await
    }
}

/// Answers with its request's text; `pass hold <key>` waits until the test
/// opens `key` first, counted in `held`.
#[derive(Clone, Default)]
struct Echo {
    held: Arc<Count>,
}

impl Sink for Echo {
    type Req = Ask;
    type Item = Told;

    async fn generate(&self, _cx: Cx, req: Ask) -> Result<Items<Told>> {
        if let Some(key) = req.text.strip_prefix("pass hold ") {
            self.held.add(1);
            gate(key).await;
            self.held.add(-1);
        }
        Ok(items(futures::stream::iter([Ok(Told {
            text: format!("echo {}", req.text),
        })])))
    }
}

/// The router's segment and the service's own.
const SPLIT: &str = "[segment.front]\nnodes = ['Front', 'Router', 'Echo']\n\
                     [segment.tok]\nnodes = ['Tok']\n";

/// The service's segment also runs the pipeline's sink, so it admits
/// requests and serves calls.
const SHARED: &str = "[segment.front]\nnodes = ['Front', 'Router']\n\
                      [segment.tok]\nnodes = ['Echo', 'Tok']\n";

fn placement(text: &str) -> Placement {
    Placement::from_toml(text).expect("a placement")
}

/// A started front, with a client of `Tok` beside its router's, and the
/// started instances of `tok`.
struct Deployed<F, T> {
    front: Segment<F>,
    client: UnaryClient<Ask, Told>,
    toks: Vec<Tok<T>>,
}

struct Tok<T> {
    service: Arc<Service>,
    echo: Echo,
    segment: Segment<T>,
}

impl<T> Tok<T> {
    /// Wait, bounded, until the instance holds no request and no call.
    async fn idle(&self) {
        tokio::time::timeout(WAIT, async {
            while self.segment.admitted() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("idle in time; it holds {}", self.segment.admitted()));
    }
}

/// How the deployments differ: the placement, each instance's settings,
/// and what the front's roster holds.
struct Setup<'a> {
    at: &'a str,
    front: RuntimeConfig,
    toks: Vec<RuntimeConfig>,
    /// Instances of `tok` in the front's roster before the real ones, as
    /// peers that are not instances of this deployment.
    strays: Vec<velo::PeerInfo>,
    /// Leave the real instances of `tok` out of the front's roster.
    unlisted: bool,
}

impl Default for Setup<'_> {
    fn default() -> Self {
        Self {
            at: SPLIT,
            front: RuntimeConfig::default(),
            toks: vec![RuntimeConfig::default()],
            strays: Vec::new(),
            unlisted: false,
        }
    }
}

macro_rules! deploy {
    ($contract:ident, $setup:expr) => {{
        let setup: Setup<'_> = $setup;
        let manifest = $contract::manifest();
        let front_node = velo_node().await;
        let mut tok_nodes = Vec::new();
        for _ in &setup.toks {
            tok_nodes.push(velo_node().await);
        }
        // Two rosters: a roster's clones share what it routes to.
        let (mut roster, mut tok_roster) = (Roster::new(), Roster::new());
        for listed in [&mut roster, &mut tok_roster] {
            listed.add("front".to_string(), front_node.peer_info());
            for stray in &setup.strays {
                listed.add("tok".to_string(), stray.clone());
            }
        }
        for node in &tok_nodes {
            tok_roster.add("tok".to_string(), node.peer_info());
            if !setup.unlisted {
                roster.add("tok".to_string(), node.peer_info());
            }
        }
        let mut toks = Vec::new();
        for (node, config) in tok_nodes.into_iter().zip(setup.toks) {
            let service = Arc::new(Service::default());
            let echo = Echo::default();
            let deployment = Deployment::new(
                node,
                &manifest,
                &placement(setup.at),
                "tok",
                &tok_roster,
                config,
            )
            .expect("the tok deployment");
            let root = $contract::assemble(
                &deployment,
                $contract::Bindings::new()
                    .tok(Arc::clone(&service))
                    .echo(echo.clone()),
            )
            .expect("assembled");
            toks.push(Tok {
                service,
                echo,
                segment: deployment.start(root).expect("started"),
            });
        }
        let link = |config: &RuntimeConfig| {
            config
                .calls
                .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
                .expect("a link")
        };
        let (router, client) = (link(&setup.front), link(&setup.front));
        let deployment = Deployment::new(
            front_node,
            &manifest,
            &placement(setup.at),
            "front",
            &roster,
            setup.front,
        )
        .expect("the front deployment");
        let root = $contract::assemble(
            &deployment,
            $contract::Bindings::new()
                .router(Router { tok: router })
                .echo(Echo::default()),
        )
        .expect("assembled");
        (
            Deployed {
                front: deployment.start(root).expect("started"),
                client,
                toks,
            },
            roster,
        )
    }};
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("done in time")
}

/// A call through `client`, in a task of its own, with a deadline `within`
/// from now.
fn spawn_call(
    client: &UnaryClient<Ask, Told>,
    text: &str,
    within: Duration,
) -> tokio::task::JoinHandle<Result<Told>> {
    let (client, text) = (client.clone(), text.to_string());
    tokio::spawn(async move {
        let cx = Cx::new().with_deadline(Deadline::after(within));
        client.call(&cx, ask(&text)).await
    })
}

/// A request through the front, its answer read whole.
async fn request<F: Next<Ask, Told>>(front: &Segment<F>, text: &str) -> Result<Vec<Told>> {
    let out = front.call::<Ask, Told>(Cx::new(), ask(text)).await?;
    bounded(out.collect::<Vec<_>>()).await.into_iter().collect()
}

fn limited(limit: usize) -> RuntimeConfig {
    RuntimeConfig {
        call_limit: Some(limit),
        ..RuntimeConfig::default()
    }
}

/// A full instance refuses a call before its service runs, and the caller
/// tries the next instance, which answers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_instance_refuses_before_the_service_runs_and_the_call_moves_on() {
    let (deployed, _) = deploy!(
        contract,
        Setup {
            toks: vec![limited(1), RuntimeConfig::default()],
            ..Setup::default()
        }
    );
    let (full, other) = (&deployed.toks[0], &deployed.toks[1]);
    // Round robin: the first call goes to the first instance, and holds its
    // only permit.
    let held = spawn_call(&deployed.client, "gate full-moves-on", WAIT);
    full.service.running.reaches(1).await;
    let second = bounded(deployed.client.call(&Cx::new(), ask("told b"))).await;
    assert_eq!(second.expect("answered").text, "told b");
    // The third goes to the full instance first, which refuses it.
    let third = bounded(deployed.client.call(&Cx::new(), ask("told c"))).await;
    assert_eq!(third.expect("answered by the other").text, "told c");
    assert_eq!(full.service.runs.get(), 1, "the full instance ran a call");
    assert_eq!(other.service.runs.get(), 2);
    open("full-moves-on");
    bounded(held).await.expect("joined").expect("answered");
    full.idle().await;
    other.idle().await;
}

/// When every instance tried refuses, the call fails with the last
/// refusal, `Overloaded`, and no service ran it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_instance_full_fails_the_call_with_overloaded() {
    let (deployed, _) = deploy!(
        contract,
        Setup {
            toks: vec![limited(1)],
            ..Setup::default()
        }
    );
    let tok = &deployed.toks[0];
    let held = spawn_call(&deployed.client, "gate all-full", WAIT);
    tok.service.running.reaches(1).await;
    let error = bounded(deployed.client.call(&Cx::new(), ask("told x")))
        .await
        .expect_err("refused");
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    assert!(error.message().contains("call_limit"), "{error}");
    assert_eq!(tok.service.runs.get(), 1);
    open("all-full");
    bounded(held).await.expect("joined").expect("answered");
    tok.idle().await;
}

/// A call takes a permit of its own, never its request's share: one
/// request that makes `call_limit` calls at once has them all running,
/// and one more is refused. The instance counts each as admitted until
/// it ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_request_making_many_calls_at_once_takes_a_permit_for_each() {
    const CALLS: usize = 1_000;
    let (deployed, _) = deploy!(
        contract,
        Setup {
            toks: vec![limited(CALLS)],
            ..Setup::default()
        }
    );
    let tok = &deployed.toks[0];
    // One request's context for every call.
    let cx = Cx::new().with_deadline(Deadline::after(WAIT * 2));
    let calls: Vec<_> = (0..CALLS)
        .map(|_| {
            let (client, cx) = (deployed.client.clone(), cx.clone());
            tokio::spawn(async move { client.call(&cx, ask("gate many")).await })
        })
        .collect();
    tok.service.running.reaches(CALLS).await;
    assert_eq!(tok.segment.admitted(), CALLS, "a permit for each call");
    let refused = bounded(deployed.client.call(&cx, ask("told one more")))
        .await
        .expect_err("past the limit");
    assert_eq!(refused.kind(), ErrorKind::Overloaded, "{refused}");
    open("many");
    for call in calls {
        bounded(call).await.expect("joined").expect("answered");
    }
    tok.idle().await;
}

/// A call's reply holds its permit until velo lets the reply's bytes go,
/// not until the call's bound and its grace: with a grace far past the
/// test, the permits come back once the replies are sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_permit_returns_once_the_reply_is_sent() {
    let (deployed, _) = deploy!(
        contract,
        Setup {
            toks: vec![RuntimeConfig {
                call_limit: Some(1),
                call_reply_grace: Duration::from_secs(600),
                ..RuntimeConfig::default()
            }],
            ..Setup::default()
        }
    );
    for n in 0..3 {
        let told = bounded(deployed.client.call(&Cx::new(), ask(&format!("told {n}")))).await;
        assert_eq!(told.expect("answered").text, format!("told {n}"));
        // The next call finds the only permit free once velo sent this
        // reply, which may be just after the caller read it.
        deployed.toks[0].idle().await;
    }
}

/// A reply velo does not let go of, as one queued behind a caller that
/// does not read, holds its permit: new calls are refused. Each permit
/// comes back once its call's bound and `call_reply_grace` pass, though
/// the bytes are still held. The service reads the bound less the
/// `clock_skew` allowance, so the permits may come back that much early.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_held_unsent_keeps_its_permit_until_the_bound_and_the_grace() {
    let kept = Arc::new(Mutex::new(Vec::new()));
    let mut tok = RuntimeConfig {
        call_limit: Some(2),
        call_reply_grace: Duration::from_millis(300),
        ..RuntimeConfig::default()
    };
    tok.faults.retain_call_replies = Some(Arc::clone(&kept));
    let (deployed, _) = deploy!(
        contract,
        Setup {
            toks: vec![tok],
            ..Setup::default()
        }
    );
    let bound = Duration::from_millis(400);
    let started = tokio::time::Instant::now();
    for n in 0..2 {
        let cx = Cx::new().with_deadline(Deadline::after(bound));
        let told = bounded(deployed.client.call(&cx, ask(&format!("told {n}")))).await;
        assert_eq!(told.expect("answered").text, format!("told {n}"));
    }
    let tok = &deployed.toks[0];
    assert_eq!(
        tok.segment.admitted(),
        2,
        "the held replies hold their permits"
    );
    let refused = bounded(deployed.client.call(&Cx::new(), ask("told more")))
        .await
        .expect_err("no permit is free");
    assert_eq!(refused.kind(), ErrorKind::Overloaded, "{refused}");
    assert_eq!(tok.service.runs.get(), 2, "the refused call ran");
    tok.idle().await;
    let skew = RuntimeConfig::default().clock_skew;
    assert!(
        started.elapsed() + skew >= bound + Duration::from_millis(300),
        "released before the bound and the grace: {:?}",
        started.elapsed()
    );
    assert_eq!(
        kept.lock().expect("kept").len(),
        2,
        "the bytes are still held"
    );
    let told = bounded(deployed.client.call(&Cx::new(), ask("told again"))).await;
    assert_eq!(told.expect("answered").text, "told again");
}

/// A call answered with the service's own error is never tried again as a
/// refusal is, whatever the error's kind: the service ran once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_completed_error_is_never_tried_again() {
    let (deployed, _) = deploy!(
        contract,
        Setup {
            toks: vec![RuntimeConfig::default(), RuntimeConfig::default()],
            ..Setup::default()
        }
    );
    let error = bounded(deployed.client.call(&Cx::new(), ask("busy")))
        .await
        .expect_err("the service's error");
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    assert_eq!(error.message(), "busy downstream");
    let runs: usize = deployed.toks.iter().map(|t| t.service.runs.get()).sum();
    assert_eq!(runs, 1, "tried again");
}

/// A call that may have run, its reply lost after the send, fails with
/// `OutcomeUnknown` and is not tried again, unless the graph marks the
/// service `idempotent`: then the next instance runs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_that_may_have_run_is_tried_again_only_when_idempotent() {
    let lossy = || {
        let mut config = RuntimeConfig::default();
        config.faults.lose_call_replies = Some(Arc::new(AtomicUsize::new(1)));
        config
    };
    let two = || vec![RuntimeConfig::default(), RuntimeConfig::default()];
    let (plain, _) = deploy!(
        contract,
        Setup {
            front: lossy(),
            toks: two(),
            ..Setup::default()
        }
    );
    let error = bounded(plain.client.call(&Cx::new(), ask("told once")))
        .await
        .expect_err("the reply was lost");
    assert_eq!(error.kind(), ErrorKind::OutcomeUnknown, "{error}");
    let runs: usize = plain.toks.iter().map(|t| t.service.runs.get()).sum();
    assert_eq!(runs, 1, "tried again");

    let (marked, _) = deploy!(
        marked,
        Setup {
            front: lossy(),
            toks: two(),
            ..Setup::default()
        }
    );
    let told = bounded(marked.client.call(&Cx::new(), ask("told twice"))).await;
    assert_eq!(told.expect("answered by the next").text, "told twice");
    let runs: Vec<usize> = marked.toks.iter().map(|t| t.service.runs.get()).collect();
    assert_eq!(runs, [1, 1], "the next instance ran it again");
}

/// A send velo refuses before the transport, to a peer with no `gdl.call`
/// handler, never left, so the call moves on to the next instance for any
/// service.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_velo_did_not_send_moves_to_the_next_instance() {
    // A node of no deployment, listed first, under an id of its own.
    let stray = velo_node().await;
    let peer = velo::PeerInfo::new(
        velo::InstanceId::new_v4(),
        stray.peer_info().worker_address().clone(),
    );
    let (deployed, _) = deploy!(
        contract,
        Setup {
            strays: vec![peer],
            ..Setup::default()
        }
    );
    let told = bounded(deployed.client.call(&Cx::new(), ask("told on"))).await;
    assert_eq!(told.expect("answered by the instance").text, "told on");
    assert_eq!(deployed.toks[0].service.runs.get(), 1);
}

/// With no instance of the service's segment to route to, a call fails
/// with `Routing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_with_no_instance_to_route_to_fails_with_routing() {
    let (deployed, _) = deploy!(
        contract,
        Setup {
            unlisted: true,
            ..Setup::default()
        }
    );
    let error = bounded(deployed.client.call(&Cx::new(), ask("told none")))
        .await
        .expect_err("nothing to route to");
    assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
    assert_eq!(deployed.toks[0].service.runs.get(), 0);
}

/// The only instance of the service, retired but not yet closed, is still
/// routed to, and serves a call made after its retirement: an admitted
/// request that needs it still gets its call. Once it closes, it refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_only_instance_serves_calls_after_it_retires_until_it_closes() {
    let (deployed, roster) = deploy!(contract, Setup::default());
    let tok = &deployed.toks[0];
    roster.retire("tok", tok.segment.peer_info().instance_id());
    let answer = bounded(request(&deployed.front, "told retired")).await;
    assert_eq!(answer.expect("answered")[0].text, "echo told retired");
    assert_eq!(tok.service.runs.get(), 1);
    tok.segment.close();
    let error = bounded(deployed.client.call(&Cx::new(), ask("told closed")))
        .await
        .expect_err("refused once closed");
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    assert_eq!(tok.service.runs.get(), 1, "a closed instance ran a call");
}

/// The only instance retires and closes while a call runs there: the call
/// completes, and the instance holds nothing once it has.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_only_instance_closing_mid_call_completes_the_call() {
    let (deployed, roster) = deploy!(contract, Setup::default());
    let tok = &deployed.toks[0];
    let call = spawn_call(&deployed.client, "gate mid-call", WAIT);
    tok.service.running.reaches(1).await;
    roster.retire("tok", tok.segment.peer_info().instance_id());
    tok.segment.close();
    assert_eq!(
        tok.segment.admitted(),
        1,
        "drain does not wait for the call"
    );
    open("mid-call");
    let told = bounded(call).await.expect("joined");
    assert_eq!(told.expect("completed").text, "gated mid-call");
    tok.idle().await;
}

/// A closed instance refuses calls, and the caller moves to a live one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_instance_refuses_and_the_call_moves_to_a_live_one() {
    let (deployed, _) = deploy!(
        contract,
        Setup {
            toks: vec![RuntimeConfig::default(), RuntimeConfig::default()],
            ..Setup::default()
        }
    );
    deployed.toks[0].segment.close();
    for n in 0..4 {
        let told = bounded(deployed.client.call(&Cx::new(), ask(&format!("told {n}")))).await;
        assert_eq!(told.expect("answered").text, format!("told {n}"));
    }
    assert_eq!(deployed.toks[0].service.runs.get(), 0);
    assert_eq!(deployed.toks[1].service.runs.get(), 4);
}

/// Calls and requests are admitted apart: calls holding every permit of
/// `call_limit` refuse none of the pipeline requests the same instance
/// admits, and requests holding every admission permit refuse no call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_at_their_limit_and_requests_at_theirs_do_not_refuse_each_other() {
    const LIMIT: usize = 2;
    let (deployed, _) = deploy!(
        contract,
        Setup {
            at: SHARED,
            toks: vec![RuntimeConfig {
                admission_limit: LIMIT,
                call_limit: Some(LIMIT),
                ..RuntimeConfig::default()
            }],
            ..Setup::default()
        }
    );
    let tok = &deployed.toks[0];
    let calls: Vec<_> = (0..LIMIT)
        .map(|_| spawn_call(&deployed.client, "gate burst-calls", WAIT * 2))
        .collect();
    tok.service.running.reaches(LIMIT).await;
    for n in 0..LIMIT + 1 {
        let answer = bounded(request(&deployed.front, &format!("pass {n}"))).await;
        assert_eq!(answer.expect("admitted")[0].text, format!("echo pass {n}"));
    }
    open("burst-calls");
    for call in calls {
        bounded(call).await.expect("joined").expect("answered");
    }
    tok.idle().await;

    // The reverse: requests hold every admission permit.
    let front = Arc::new(deployed.front);
    let requests: Vec<_> = (0..LIMIT)
        .map(|_| {
            let front = Arc::clone(&front);
            tokio::spawn(async move { request(&front, "pass hold burst-requests").await })
        })
        .collect();
    tok.echo.held.reaches(LIMIT).await;
    let refused = bounded(request(&front, "pass over")).await;
    assert!(refused.is_err(), "the requests' limit holds");
    for n in 0..LIMIT {
        let told = bounded(deployed.client.call(&Cx::new(), ask(&format!("told {n}")))).await;
        assert_eq!(told.expect("a call is admitted").text, format!("told {n}"));
    }
    open("burst-requests");
    for request in requests {
        bounded(request).await.expect("joined").expect("answered");
    }
    tok.idle().await;
}

/// A graph in two classes whose service calls another, so the service's
/// segment, in a class a hop enters, counts what may still send.
mod chained {
    use super::{Ask, Told};

    gdl_runtime::graph! {
        name = "call-senders", schema = 1;
        unary  Inner: Ask -> Told;
        unary  Tok: Ask -> Told calls Inner;
        source Front: Ask -> stream Told;
        sink   Echo: Ask -> stream Told calls Tok;
        topology: Front => Echo;
    }
}

/// `Front` in `global`; `Echo`, `Tok`, and `Inner` apart in `model_x`.
const CHAINED: &str = "[segment.front]\nnodes = ['Front']\n\
                       [segment.mx]\nnodes = ['Echo']\n\
                       [segment.tok]\nnodes = ['Tok']\n\
                       [segment.inner]\nnodes = ['Inner']\n\
                       [class.global]\nsegments = ['front']\n\
                       [class.model_x]\nsegments = ['mx', 'tok', 'inner']\n";

/// In a segment of a draining pool's class whose service calls a service
/// in another segment, a call it accepted counts as one that may still
/// send until the call ends: the service may call another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepted_call_may_send_until_it_ends_where_the_service_calls_another() {
    let manifest = chained::manifest();
    let (mx_node, tok_node) = (velo_node().await, velo_node().await);
    let mut roster = Roster::new();
    roster.add("mx".to_string(), mx_node.peer_info());
    roster.add("tok".to_string(), tok_node.peer_info());
    let service = Arc::new(Service::default());
    let deployment = Deployment::new(
        tok_node,
        &manifest,
        &placement(CHAINED),
        "tok",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("the tok deployment");
    let root = chained::assemble(
        &deployment,
        chained::Bindings::new().tok(Arc::clone(&service)),
    )
    .expect("assembled");
    let tok = deployment.start(root).expect("started");
    let caller = RuntimeConfig::default();
    let client = caller
        .calls
        .link::<Ask, Told>("Tok", wire_codec!(Ask), wire_codec!(Told))
        .expect("a link");
    let mut caller_roster = Roster::new();
    caller_roster.add("mx".to_string(), mx_node.peer_info());
    caller_roster.add("tok".to_string(), tok.peer_info());
    let deployment = Deployment::new(
        mx_node,
        &manifest,
        &placement(CHAINED),
        "mx",
        &caller_roster,
        caller,
    )
    .expect("the caller's deployment");
    let root = chained::assemble(&deployment, chained::Bindings::new().echo(Echo::default()))
        .expect("assembled");
    let _caller = deployment.start(root).expect("started");

    assert_eq!(tok.senders(), 0);
    let call = spawn_call(&client, "gate chained", WAIT);
    service.running.reaches(1).await;
    assert_eq!(tok.senders(), 1, "the accepted call may still send");
    open("chained");
    bounded(call).await.expect("joined").expect("answered");
    assert_eq!(tok.senders(), 0, "the call ended");
}

/// Sends each call to the first instance in the roster that has not
/// declined it, and records each list of declined instances it is shown;
/// leases a count of the choices it holds.
struct FirstNotDeclined {
    declined: Mutex<Vec<usize>>,
    leased: Arc<AtomicUsize>,
}

/// One choice's lease, counted while it lives.
struct Leased(Arc<AtomicUsize>);

impl Drop for Leased {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl gdl::Selector for FirstNotDeclined {
    type Head = gdl::Encoded;

    async fn select<'a>(
        &'a self,
        request: gdl::Request<'a, Self::Head>,
        instances: gdl::Instances<'a>,
    ) -> Result<gdl::Choice<'a>> {
        let declined = request.declined();
        self.declined.lock().expect("declined").push(declined.len());
        let chosen = instances
            .iter()
            .find(|instance| !declined.contains(&instance.id()))
            .or_else(|| instances.get(0))
            .expect("an instance");
        self.leased.fetch_add(1, Ordering::SeqCst);
        Ok(gdl::Choice::new(chosen).with_lease(Leased(Arc::clone(&self.leased))))
    }
}

/// A call through a selector takes a permit as any call does: a full
/// instance refuses it, and the selector chooses again, told that the full
/// instance declined, with the declined choice's lease ended first. The
/// other instance answers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_through_a_selector_moves_on_from_a_full_instance() {
    let leased = Arc::new(AtomicUsize::new(0));
    let selector = Arc::new(FirstNotDeclined {
        declined: Mutex::new(Vec::new()),
        leased: Arc::clone(&leased),
    });
    let mut front = RuntimeConfig::default();
    front
        .selectors
        .add_shared("first", Arc::clone(&selector))
        .expect("registered");
    let routed = format!("{SPLIT}[routing]\nTok = {{ selector = 'first' }}\n");
    let (deployed, _) = deploy!(
        contract,
        Setup {
            at: &routed,
            front,
            toks: vec![limited(1), RuntimeConfig::default()],
            ..Setup::default()
        }
    );
    let (full, other) = (&deployed.toks[0], &deployed.toks[1]);
    let held = spawn_call(&deployed.client, "gate selected-full", WAIT);
    full.service.running.reaches(1).await;
    assert_eq!(leased.load(Ordering::SeqCst), 1, "the held call's lease");
    let told = bounded(deployed.client.call(&Cx::new(), ask("told on"))).await;
    assert_eq!(told.expect("answered by the other").text, "told on");
    assert_eq!(full.service.runs.get(), 1, "the full instance ran a call");
    assert_eq!(other.service.runs.get(), 1);
    assert_eq!(
        *selector.declined.lock().expect("declined"),
        [0, 0, 1],
        "the second choice was told of the refusal"
    );
    assert_eq!(
        leased.load(Ordering::SeqCst),
        1,
        "a declined choice's lease"
    );
    open("selected-full");
    bounded(held).await.expect("joined").expect("answered");
    assert_eq!(leased.load(Ordering::SeqCst), 0);
    full.idle().await;
}
