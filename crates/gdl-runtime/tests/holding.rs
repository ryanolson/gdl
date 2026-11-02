//! An instance of a pool retired through its hub, as the operator's
//! `retire: request` does: it retires as a first stop does, and runs its
//! leave hooks, but the request is no stop. Once its ledger is empty it
//! says `detached`, then `drained`, and holds, a member of its hub with
//! its velo node up, until a stop, its pool's drain, a lost lease, or the
//! hold's bound ends it.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed. A retirement takes at least the setup timeout of 1 s, so
//! these tests take a few seconds each.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::boundary::{
    Model, Running, Spread, model_spread, model_spread_proxied, run_graph_in, spec_of,
};
use common::pools::{
    Nothing, Resolving, followed, global_hub, global_pool, launch, pool_hub_of, settings, until,
};
use common::proxy::Proxy;
use common::system::send;
use common::{Chunk, Events, Idle, Plan, Prompt, WAIT, expected, prompt, reference_graph};
use futures::StreamExt;
use gdl::{Cx, Handoff, Leave, LeaveCause, Next, Routing};
use gdl_model::{SegmentPhase, segment_service};
use gdl_runtime::{
    DeploymentSpec, Placement, PoolState, Pools, ProcessConfig, Register, RuntimeConfig,
};
use gdl_watch::{InstancePhase, Instances, PoolInstance};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use velo::InstanceId;
use velo::discovery::ServiceDiscovery;
use velo_hub::Hub;
use velo_hub::client::HubClient;

const M1: &str = "model-x-01-us-east-1";

/// Every instance's `[runtime]` table: a drain bound longer than any wait
/// here, so that an instance leaves for what it saw, not at its bound.
const LONG: &str = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n";

/// The longest a test waits for a response.
const ANSWER: Duration = Duration::from_secs(30);

/// Long enough for several of the hub's polls and the drain's, for a test
/// that something does not happen.
const WHILE: Duration = Duration::from_millis(600);

/// What a stop of a held instance may take: an idle velo node stops at
/// once, and its hub answers. The bound is the 1 s the runtime gives velo
/// when the stop's own bound has passed, and a margin for a busy host.
const SHUTDOWN: Duration = Duration::from_secs(3);

/// The property part with C apart: `f` runs B, E, and F; `c` runs C; `d`
/// runs D.
fn request_only() -> Placement {
    Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "E", "F"])
        .segment("c", ["C"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "c", "d"])
}

/// Every segment of [`request_only`].
const SEGMENTS: &[&str] = &["a", "f", "c", "d"];

/// A leave hook that says how many times it ran, and with what, and ends
/// once routing has moved and the test lets it.
struct Ran {
    leaves: Mutex<Vec<Leave>>,
    finish: watch::Sender<bool>,
}

impl Ran {
    /// A hook that ends as soon as routing has moved.
    fn new() -> Arc<Self> {
        Arc::new(Self {
            leaves: Mutex::default(),
            finish: watch::Sender::new(true),
        })
    }

    /// A hook that ends once the test calls [`Ran::finish`], which holds
    /// the instance's exit until then.
    fn held() -> Arc<Self> {
        Arc::new(Self {
            leaves: Mutex::default(),
            finish: watch::Sender::new(false),
        })
    }

    fn finish(&self) {
        self.finish.send_replace(true);
    }

    /// Register this hook on `config`'s lifecycle.
    fn on(self: &Arc<Self>, config: &ProcessConfig<Nothing>) {
        let ran = Arc::clone(self);
        config
            .runtime
            .lifecycle
            .on_leave("kv", move |leave: Leave| async move {
                ran.leaves.lock().expect("lock").push(leave.clone());
                let _ = leave.routed().await;
                let mut finish = ran.finish.subscribe();
                let _ = finish.wait_for(|finish| *finish).await;
                Handoff::Done
            })
            .expect("registered");
    }

    fn times(&self) -> usize {
        self.leaves.lock().expect("lock").len()
    }

    /// The `Leave` of its one run, once it has run.
    async fn leave(&self) -> Leave {
        eventually("the hook started", || self.times() == 1).await;
        self.leaves.lock().expect("lock")[0].clone()
    }
}

/// Change the process settings of the `n`th instance only: of the interior
/// ones in the order the test names them, then of the entry instances.
fn nth(
    n: usize,
    change: impl Fn(&mut ProcessConfig<Nothing>) + Sync,
) -> impl Fn(&str, &mut ProcessConfig<Nothing>) + Sync {
    let seen = AtomicUsize::new(0);
    move |_, config| {
        if seen.fetch_add(1, Ordering::SeqCst) == n {
            change(config);
        }
    }
}

/// Wait until `done` says so, bounded by [`WAIT`].
async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} in time"));
}

/// The pool's list of its members.
async fn listed(hub: &Hub) -> Vec<PoolInstance> {
    let response = send(
        hub.control_addr(),
        b"GET /v1/ext/gdl-pool/instances HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    )
    .await;
    let body = response.split("\r\n\r\n").nth(1).expect("a body");
    let list: Instances = serde_json::from_str(body).expect("the list");
    list.instances
}

/// `id`'s row of its hub's list.
async fn row(hub: &Hub, id: InstanceId) -> Option<PoolInstance> {
    listed(hub)
        .await
        .into_iter()
        .find(|instance| instance.peer.instance_id() == id)
}

/// Ask `hub` to retire `id` in the registration it lists now, as a
/// controller does, and return the status code.
async fn ask(hub: &Hub, id: InstanceId) -> u16 {
    let incarnation = row(hub, id)
        .await
        .expect("a member")
        .incarnation
        .expect("the hub lists it");
    let body = format!("{{\"incarnation\":{incarnation}}}");
    let request = format!(
        "POST /v1/ext/gdl-pool/instances/{id}/retire HTTP/1.1\r\nHost: t\r\nConnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    let response = send(hub.control_addr(), request.as_bytes()).await;
    response
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status code")
}

/// Wait until `hub` lists `id` in `phase`.
async fn until_listed(hub: &Hub, id: InstanceId, phase: InstancePhase) {
    tokio::time::timeout(WAIT * 2, async {
        loop {
            if row(hub, id).await.is_some_and(|row| row.phase == phase) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{id} not listed {phase:?} in time"));
}

/// A client of `hub` that holds no registration.
fn client(hub: &Hub) -> HubClient {
    HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture")
}

/// Whether `id` of `segment` is in `phase` at `hub`.
async fn announced(hub: &Hub, segment: &str, id: InstanceId, phase: SegmentPhase) -> bool {
    tokio::time::timeout(
        WAIT,
        client(hub)
            .services()
            .get_instances(&segment_service(segment, phase)),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub answers")
    .contains(&id)
}

fn id<H>(instance: &Running<H>) -> InstanceId {
    instance.node.instance_id()
}

/// No process setting changed.
fn unchanged(_: &str, _: &mut ProcessConfig<Nothing>) {}

/// The global pool and its chooser, and the model pool [`M1`] of
/// [`request_only`].
struct Fleet<C, H> {
    global: Hub,
    chooser: Running<C>,
    pool: Spread<H>,
    proxy: Option<Proxy>,
    events: Arc<Events>,
}

/// A [`Fleet`] with `entries` instances of `f` and one instance of each of
/// `interior`, all serving, with the chooser following the pool, whose
/// instances reach their hub through a proxy if `proxied`.
async fn fleet(
    spec: &DeploymentSpec,
    entries: usize,
    interior: &[&'static str],
    proxied: bool,
    process: &(dyn Fn(&str, &mut ProcessConfig<Nothing>) + Sync),
) -> Fleet<impl Next<Prompt, Chunk> + Register + use<>, impl Next<Prompt, Chunk> + Register + use<>>
{
    let directory = Resolving::new();
    let events = Events::new();
    let (global, _) = pool_hub_of(spec, global_hub(), &directory).await;
    let chooser = chooser(spec, &global).await;
    let ready = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries,
    };
    let assemble = |deployment: &gdl_runtime::Deployment| reference_graph(deployment, &events);
    let (pool, proxy) = if proxied {
        let (pool, proxy) = model_spread_proxied(
            spec, &directory, ready, SEGMENTS, interior, LONG, process, &assemble,
        )
        .await;
        (pool, Some(proxy))
    } else {
        let pool = model_spread(
            spec, &directory, ready, SEGMENTS, interior, LONG, process, &assemble,
        )
        .await;
        (pool, None)
    };
    let pools: Pools = chooser.pools.clone().expect("a chooser");
    until(&pools, "the model pool followed", |pools| {
        followed(pools, "f", M1).is_some_and(|m| {
            m.state == PoolState::Ready && m.eligible.len() == entries && m.load.is_some()
        })
    })
    .await;
    Fleet {
        global,
        chooser,
        pool,
        proxy,
        events,
    }
}

/// A chooser, an instance of `a` in the global pool at `hub`.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{LONG}", global_pool());
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let runtime = RuntimeConfig {
        selectors: gdl::Selectors::new(),
        ..config.runtime.clone()
    };
    let quiet = Events::quiet();
    run_graph_in(&config, "a", SEGMENTS, spec, runtime, &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

/// Send `text` with `plan` through `chooser`: its answer, read at once.
fn request<H: Next<Prompt, Chunk> + Send + Sync + 'static>(
    chooser: &Running<H>,
    text: &str,
    plan: Plan,
) -> JoinHandle<Vec<gdl::Result<Chunk>>> {
    let segment = Arc::clone(&chooser.segment);
    let request = prompt(text, plan);
    tokio::spawn(async move {
        let out = match segment.call(Cx::new(), request).await {
            Ok(out) => out,
            Err(error) => return vec![Err(error)],
        };
        tokio::time::timeout(ANSWER, out.collect::<Vec<_>>())
            .await
            .expect("the answer ended in time")
    })
}

async fn answer(answer: JoinHandle<Vec<gdl::Result<Chunk>>>) -> Vec<gdl::Result<Chunk>> {
    tokio::time::timeout(ANSWER, answer)
        .await
        .expect("the answer in time")
        .expect("the request's task")
}

/// A request held at C until the test lets it go.
fn gated() -> Plan {
    Plan {
        tokens: 3,
        gated: true,
        ..Plan::default()
    }
}

fn plain() -> Plan {
    Plan {
        tokens: 3,
        ..Plan::default()
    }
}

/// Let the request `text`, held at C, go on.
fn release(events: &Events, text: &str) {
    events.record(format!("test:release:{text}"));
}

/// The instance in `interior` that took the forward hand-off of a request
/// held at C, once one has.
async fn holder<H>(interior: &[Running<H>], events: &Events, text: &str) -> usize {
    events.wait_for(&format!("C:{text}"), WAIT).await;
    interior
        .iter()
        .position(|instance| instance.forwards() > 0)
        .expect("one took it")
}

/// Wait until `instance` refuses new work.
async fn closed<H>(instance: &Running<H>) {
    eventually("the instance closed", || instance.segment.is_closed()).await;
}

impl<C, H> Fleet<C, H> {
    /// End every instance left, and the hubs: with a second stop, since
    /// the last instance of a segment waits for its drain's bound.
    async fn shut(self) {
        let Spread {
            hub,
            entries,
            interior,
            ..
        } = self.pool;
        for instance in interior.into_iter().chain(entries) {
            instance.process.stop();
            instance.process.stop();
            let _ = instance.ended(WAIT).await;
        }
        self.chooser.process.stop();
        self.chooser.process.stop();
        let _ = self.chooser.ended(WAIT).await;
        for hub in [self.global, hub] {
            hub.shutdown().await.expect("valid test fixture");
        }
    }
}

/// An interior instance asked to retire is avoided by its peers, runs its
/// hook, reaches an empty ledger, says `detached` and then `drained`, and
/// stays a member while work goes elsewhere, until it is stopped; the stop
/// then ends it within velo's shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interior_instance_asked_to_retire_is_avoided_drains_and_holds() {
    let spec = spec_of(request_only());
    let ran = Ran::new();
    let hooked = Arc::clone(&ran);
    let process = nth(1, move |config| hooked.on(config));
    let mut fleet = fleet(&spec, 1, &["c", "d", "d"], false, &process).await;
    let held = fleet.pool.interior.remove(1);
    let other = &fleet.pool.interior[1];
    let hub = &fleet.pool.hub;
    assert_eq!(ask(hub, id(&held)).await, 202);

    let leave = ran.leave().await;
    assert_eq!(leave.cause(), LeaveCause::Stop, "a request is no new cause");
    assert_eq!(
        tokio::time::timeout(WAIT, leave.routed())
            .await
            .expect("routing moved in time"),
        Routing::Confirmed,
        "its peers avoid it"
    );
    until_listed(hub, id(&held), InstancePhase::Drained).await;
    assert!(
        announced(hub, "d", id(&held), SegmentPhase::Detached).await,
        "`drained` came without `detached`"
    );
    assert!(held.segment.is_closed());
    assert_eq!(held.segment.admitted(), 0);

    let served = other.forwards();
    assert_eq!(
        answer(request(&fleet.chooser, "after", plain())).await,
        expected(0..3),
        "the pool goes on without it"
    );
    assert_eq!(other.forwards(), served + 1, "its peer took the request");
    tokio::time::sleep(WHILE).await;
    assert!(held.running(), "it left without being stopped");
    assert!(row(hub, id(&held)).await.is_some(), "it left its hub");

    let gone = id(&held);
    let stopping = Instant::now();
    held.process.stop();
    held.ended(SHUTDOWN)
        .await
        .expect("a held instance exits well");
    assert!(stopping.elapsed() < SHUTDOWN);
    assert!(row(hub, gone).await.is_none(), "it left its hub");
    assert_eq!(ran.times(), 1, "the hook runs once");
    fleet.shut().await;
}

/// The hold has a bound of its own: an instance nothing stops exits past
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_nothing_stops_exits_past_the_holds_bound() {
    const HOLD: Duration = Duration::from_secs(2);
    let spec = spec_of(request_only());
    let process = nth(1, |config| config.runtime.hold = Some(HOLD));
    let mut fleet = fleet(&spec, 1, &["c", "d", "d"], false, &process).await;
    let held = fleet.pool.interior.remove(1);
    let hub = &fleet.pool.hub;
    let asked = Instant::now();
    assert_eq!(ask(hub, id(&held)).await, 202);
    until_listed(hub, id(&held), InstancePhase::Drained).await;
    assert!(held.running(), "it left at once");
    held.ended(WAIT)
        .await
        .expect("past its bound it exits well");
    assert!(
        asked.elapsed() >= HOLD,
        "it left after {:?}, within its bound",
        asked.elapsed()
    );
    fleet.shut().await;
}

/// A hold's default outlasts the operator's `drainBound`, which the render
/// sets to the drain's own bound: 30 s past it, the pod's stop margin.
#[test]
fn the_runtime_settings_read_the_holds_bound() {
    assert_eq!(RuntimeConfig::default().hold, None);
    let read: RuntimeConfig = toml::from_str("hold_ms = 2500\n").expect("a setting");
    assert_eq!(read.hold, Some(Duration::from_millis(2500)));
}

/// A stop while it drains ends the hold before it starts, and is no forced
/// stop: the request it holds is answered, and the instance exits once it
/// has drained, since a request does not count as a stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_while_it_drains_ends_the_hold_and_cuts_nothing() {
    let spec = spec_of(request_only());
    let fleet = fleet(&spec, 1, &["c", "c", "d"], false, &unchanged).await;
    let Fleet { pool, .. } = &fleet;
    let held = request(&fleet.chooser, "held", gated());
    let holder = holder(&pool.interior, &fleet.events, "held").await;
    let retiree = &pool.interior[holder];
    assert_eq!(ask(&pool.hub, id(retiree)).await, 202);
    closed(retiree).await;
    retiree.process.stop();
    tokio::time::sleep(WHILE).await;
    assert!(retiree.running(), "it left with a request in its ledger");
    assert_eq!(retiree.segment.admitted(), 1);
    release(&fleet.events, "held");
    assert_eq!(answer(held).await, expected(0..3), "the request was cut");
    eventually("it exited once drained", || !retiree.running()).await;
    fleet.shut().await;
}

/// Only the second signal forces the stop, as on any stop: the first ends
/// the hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_second_stop_forces_an_instance_asked_to_retire() {
    let spec = spec_of(request_only());
    let mut fleet = fleet(&spec, 1, &["c", "c", "d"], false, &unchanged).await;
    let held = request(&fleet.chooser, "held", gated());
    let holder = holder(&fleet.pool.interior, &fleet.events, "held").await;
    let retiree = fleet.pool.interior.remove(holder);
    assert_eq!(ask(&fleet.pool.hub, id(&retiree)).await, 202);
    closed(&retiree).await;
    retiree.process.stop();
    tokio::time::sleep(WHILE).await;
    assert!(retiree.running(), "the first stop forced it");
    retiree.process.stop();
    let error = retiree.ended(WAIT).await.expect_err("the second forces it");
    assert_eq!(error.kind(), gdl::ErrorKind::Cancelled);
    release(&fleet.events, "held");
    let _ = answer(held).await;
    fleet.shut().await;
}

/// A repeated request changes nothing: the hook runs once, and one stop
/// still ends the instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeated_request_changes_nothing() {
    let spec = spec_of(request_only());
    let ran = Ran::new();
    let hooked = Arc::clone(&ran);
    let process = nth(1, move |config| hooked.on(config));
    let mut fleet = fleet(&spec, 1, &["c", "d", "d"], false, &process).await;
    let held = fleet.pool.interior.remove(1);
    let hub = &fleet.pool.hub;
    assert_eq!(ask(hub, id(&held)).await, 202);
    assert_eq!(ask(hub, id(&held)).await, 202);
    until_listed(hub, id(&held), InstancePhase::Drained).await;
    assert_eq!(ask(hub, id(&held)).await, 202);
    tokio::time::sleep(WHILE).await;
    assert!(held.running());
    assert_eq!(ran.times(), 1, "the hook runs once");
    held.process.stop();
    held.ended(SHUTDOWN).await.expect("one stop ends it");
    fleet.shut().await;
}

/// A request after a stop does nothing: the instance exits once drained,
/// and does not hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_after_a_stop_does_not_hold_the_instance() {
    let spec = spec_of(request_only());
    let fleet = fleet(&spec, 1, &["c", "c", "d"], false, &unchanged).await;
    let Fleet { pool, .. } = &fleet;
    let held = request(&fleet.chooser, "held", gated());
    let holder = holder(&pool.interior, &fleet.events, "held").await;
    let stopped = &pool.interior[holder];
    stopped.process.stop();
    closed(stopped).await;
    assert_eq!(ask(&pool.hub, id(stopped)).await, 202);
    tokio::time::sleep(WHILE).await;
    assert!(stopped.running(), "it left with a request in its ledger");
    release(&fleet.events, "held");
    assert_eq!(answer(held).await, expected(0..3), "the request was cut");
    eventually("it exited once drained", || !stopped.running()).await;
    fleet.shut().await;
}

/// A drain cut at its bound with a request left says nothing of `drained`:
/// the instance holds, since on Kubernetes an exit only restarts its
/// container, and says `drained` if its ledger empties later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_cut_at_its_bound_holds_and_says_drained_when_the_ledger_empties() {
    let spec = spec_of(request_only());
    let shorter = |_: &str, config: &mut ProcessConfig<Nothing>| {
        config.runtime.drain = Some(Duration::from_millis(1500));
    };
    let fleet = fleet(&spec, 1, &["c", "c", "d"], false, &shorter).await;
    let Fleet { pool, .. } = &fleet;
    let held = request(&fleet.chooser, "held", gated());
    let holder = holder(&pool.interior, &fleet.events, "held").await;
    let retiree = &pool.interior[holder];
    assert_eq!(ask(&pool.hub, id(retiree)).await, 202);
    closed(retiree).await;
    tokio::time::sleep(Duration::from_millis(1500) + WHILE).await;
    assert!(retiree.running(), "it left at the drain's bound");
    assert!(
        !announced(&pool.hub, "c", id(retiree), SegmentPhase::Drained).await,
        "`drained` with a request in its ledger"
    );
    release(&fleet.events, "held");
    assert_eq!(answer(held).await, expected(0..3));
    until_listed(&pool.hub, id(retiree), InstancePhase::Drained).await;
    assert!(retiree.running());
    fleet.shut().await;
}

/// A pool's drain stops every instance, and ends only once none is a
/// member: a held instance goes at once, rather than holding the drain to
/// the hold's bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn its_pools_drain_ends_the_hold() {
    let spec = spec_of(request_only());
    let mut fleet = fleet(&spec, 1, &["c", "d", "d"], false, &unchanged).await;
    let held = fleet.pool.interior.remove(1);
    let hub = &fleet.pool.hub;
    assert_eq!(ask(hub, id(&held)).await, 202);
    until_listed(hub, id(&held), InstancePhase::Drained).await;
    fleet.pool.readiness.drain().await.expect("the pool drains");
    held.ended(WAIT)
        .await
        .expect("its pool's drain ended the hold");
    fleet.shut().await;
}

/// A lost lease ends the hold, with the lease's error: a held instance's
/// lease is what keeps it a member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_lease_ends_the_hold() {
    let spec = spec_of(request_only());
    let mut fleet = fleet(&spec, 1, &["c", "d", "d"], false, &unchanged).await;
    let held = fleet.pool.interior.remove(1);
    let hub = &fleet.pool.hub;
    assert_eq!(ask(hub, id(&held)).await, 202);
    until_listed(hub, id(&held), InstancePhase::Drained).await;
    let member = hub.store().get(id(&held)).expect("registered");
    hub.store()
        .remove(id(&held), member.incarnation)
        .await
        .expect("removed");
    held.ended(WAIT).await.expect_err("the lease's error");
    fleet.shut().await;
}

/// An entry instance, which hosts no source, says `drained` too: it is what
/// the operator reads of it, and the hold is the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_instance_asked_to_retire_is_avoided_and_holds() {
    let spec = spec_of(request_only());
    let ran = Ran::new();
    let hooked = Arc::clone(&ran);
    let process = nth(2, move |config| hooked.on(config));
    let mut fleet = fleet(&spec, 2, &["c", "d"], false, &process).await;
    let held = fleet.pool.entries.remove(0);
    let hub = &fleet.pool.hub;
    assert_eq!(ask(hub, id(&held)).await, 202);
    let leave = ran.leave().await;
    assert_eq!(
        tokio::time::timeout(WAIT * 2, leave.routed())
            .await
            .expect("routing moved in time"),
        Routing::Confirmed,
        "the chooser avoids it"
    );
    until_listed(hub, id(&held), InstancePhase::Drained).await;
    assert_eq!(
        answer(request(&fleet.chooser, "after", plain())).await,
        expected(0..3),
        "the pool goes on without it"
    );
    assert!(held.running());
    held.process.stop();
    held.ended(SHUTDOWN)
        .await
        .expect("a held instance exits well");
    fleet.shut().await;
}

/// An instance of a pool no hop enters follows its pool's watch for its
/// request too, as the frontends of a global pool must.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_of_a_pool_no_hop_enters_hears_its_request() {
    let spec = spec_of(request_only());
    let fleet = fleet(&spec, 1, &["c", "d"], false, &unchanged).await;
    let front = &fleet.chooser;
    assert_eq!(ask(&fleet.global, id(front)).await, 202);
    until_listed(&fleet.global, id(front), InstancePhase::Drained).await;
    closed(front).await;
    assert!(front.running());
    fleet.shut().await;
}

/// A request heard before the instance serves stops it as a stop at the
/// barrier does, and it holds all the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_before_the_instance_serves_retires_and_holds_it() {
    let directory = Resolving::new();
    let spec = common::pools::spec();
    let (hub, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let extra = format!("{}{LONG}", global_pool());
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let ran = Ran::new();
    ran.on(&config);
    let (process, launched) = launch(&config, "a").await.expect("launched");
    let me = process.instance_id();
    assert_eq!(ask(&hub, me).await, 202);
    let serving = tokio::spawn(launched.serve(Idle, |_, shutdown| async move {
        shutdown.cancelled().await;
        Ok(())
    }));
    ran.leave().await;
    until_listed(&hub, me, InstancePhase::Drained).await;
    tokio::time::sleep(WHILE).await;
    assert!(!serving.is_finished(), "it left without being stopped");
    process.stop();
    let _ = tokio::time::timeout(SHUTDOWN, serving)
        .await
        .expect("a stop ends the hold");
    hub.shutdown().await.expect("valid test fixture");
}

/// Under a plain stop too, an interior instance says `detached` first and
/// `drained` last, and neither while a request is in its ledger: a reader
/// that takes it for "holds nothing, sends nothing" reads it right. The
/// proxy holds `drained` on its way to the hub, so that the test reads
/// what the instance had said by then; the hook keeps the instance there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_interior_instance_says_detached_then_drained_only_when_empty() {
    let spec = spec_of(request_only());
    let ran = [Ran::held(), Ran::held()];
    let hooks = ran.clone();
    let seen = AtomicUsize::new(0);
    let process = move |segment: &str, config: &mut ProcessConfig<Nothing>| {
        if segment == "c" {
            hooks[seen.fetch_add(1, Ordering::SeqCst)].on(config);
        }
    };
    let fleet = fleet(&spec, 1, &["c", "c", "d"], true, &process).await;
    let Fleet { pool, proxy, .. } = &fleet;
    let proxy = proxy.as_ref().expect("proxied");
    let hub = &pool.hub;
    let held = request(&fleet.chooser, "held", gated());
    let holder = holder(&pool.interior, &fleet.events, "held").await;
    let stopped = &pool.interior[holder];
    let me = id(stopped);
    proxy.hold(b"gdl-drained");
    stopped.process.stop();
    closed(stopped).await;
    tokio::time::sleep(WHILE).await;
    assert_eq!(stopped.segment.admitted(), 1, "the request is still held");
    assert!(
        !announced(hub, "c", me, SegmentPhase::Detached).await,
        "`detached` with a request in its ledger"
    );
    assert!(
        !announced(hub, "c", me, SegmentPhase::Drained).await,
        "`drained` with a request in its ledger"
    );

    let holding = tokio::time::timeout(WAIT, proxy.holding());
    let (reached, answered) = tokio::join!(holding, async {
        release(&fleet.events, "held");
        answer(held).await
    });
    reached.expect("`drained` reached the proxy");
    assert_eq!(answered, expected(0..3), "the request was cut");
    assert!(stopped.segment.is_closed());
    assert_eq!(stopped.segment.admitted(), 0);
    assert!(
        announced(hub, "c", me, SegmentPhase::Detached).await,
        "`drained` came before `detached`"
    );
    assert!(!announced(hub, "c", me, SegmentPhase::Drained).await);
    proxy.release();
    ran[holder].finish();
    ran[1 - holder].finish();
    eventually("it left", || !stopped.running()).await;
    fleet.shut().await;
}
