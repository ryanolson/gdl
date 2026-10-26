//! A routed hand-off's failover, between real velo nodes over TCP: a
//! sending runtime of segment `a` and entries of segment `d` that answer
//! as each test says, with raw handlers, so that every kind of answer can
//! be given on purpose.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use gdl::__runtime::Typed;
use gdl::{Cx, Error, ErrorKind, Result};

use crate::dist::counting::Counted;
use crate::dist::envelope::{self, Accepted, Forward, VERSION, WireCx};
use crate::dist::fabric::{Fabric, Refusing, ReplyTo, bind_node};
use crate::dist::offers::Refused;
use crate::dist::roster::{Policy, Roster};
use crate::dist::runtime::{Entries, Router, Runtime, RuntimeConfig, Within};

/// The longest a test waits for an answer it expects.
const WAIT: Duration = Duration::from_secs(5);

/// How one entry answers its `n`th offer, from 0, given the envelope.
type Answering = Arc<dyn Fn(usize, Forward) -> BoxFuture<'static, Vec<u8>> + Send + Sync>;

/// An entry of `d` on a velo node of its own.
struct Entry {
    node: velo::Velo,
    /// The offers that reached it.
    offers: Arc<AtomicUsize>,
    /// The envelopes it was offered, in order.
    seen: Arc<Mutex<Vec<Forward>>>,
}

impl Entry {
    fn offers(&self) -> usize {
        self.offers.load(Ordering::SeqCst)
    }

    fn id(&self) -> velo::InstanceId {
        self.node.instance_id()
    }
}

/// A sender of `a`, and the entries of `d` it routes to, in roster order.
struct Fixture {
    rt: Runtime,
    entries: Vec<Entry>,
}

/// `S => D`, with `S` in `a` and `D` in `d`: one class. Each call is a
/// deployment of its own, so a fixture makes one.
fn layout() -> gdl_model::Layout {
    gdl_model::layout(&manifest(), &placement()).expect("valid test fixture")
}

fn placement() -> gdl_model::Placement {
    gdl_model::Placement::new()
        .segment("a", ["S"])
        .segment("d", ["D"])
}

/// A graph with a name no other fixture has, in this process or in
/// another copy of it: an entry stopped by one test leaves its port to the
/// OS, and a sender that reaches whatever node binds it next must find no
/// handler of its deployment there (see
/// `an_address_another_deployment_holds_is_not_offered_to`).
fn manifest() -> gdl_model::Manifest {
    static GRAPHS: AtomicUsize = AtomicUsize::new(0);
    gdl_model::Manifest {
        name: format!(
            "offers-{}-{}",
            std::process::id(),
            GRAPHS.fetch_add(1, Ordering::Relaxed)
        ),
        schema: 1,
        nodes: vec![
            gdl_model::NodeInfo {
                name: "S".into(),
                role: gdl_model::Role::Source,
                binding: None,
            },
            gdl_model::NodeInfo {
                name: "D".into(),
                role: gdl_model::Role::Sink,
                binding: Some("d".into()),
            },
        ],
        hops: vec![gdl_model::HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![gdl_model::TypeInfo::new("T", true, Default::default())],
            backward_types: vec![gdl_model::TypeInfo::new("T", true, Default::default())],
        }],
    }
}

/// A sender with `config`, and one entry per answer, each answering as
/// it says; `None` registers no handler, so that velo refuses to send to
/// it.
async fn fixture(answers: Vec<Option<Answering>>, config: RuntimeConfig) -> Fixture {
    fixture_after(None, answers, config).await
}

/// [`fixture`], with `first` in the roster ahead of the entries.
async fn fixture_after(
    first: Option<velo::PeerInfo>,
    answers: Vec<Option<Answering>>,
    config: RuntimeConfig,
) -> Fixture {
    let sender = bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("a velo node");
    let mut roster = Roster::new();
    if let Some(first) = first {
        roster.add("d", first);
    }
    let mut nodes = Vec::new();
    for _ in &answers {
        let node = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        roster.add("d", node.peer_info());
        nodes.push(node);
    }
    let rt = Runtime::new(
        Fabric::new(sender, None, None, Duration::from_secs(30)),
        layout(),
        "a".into(),
        &roster,
        config,
        None,
    )
    .expect("a runtime");
    let entries = nodes
        .into_iter()
        .zip(answers)
        .map(|(node, answering)| {
            let offers = Arc::new(AtomicUsize::new(0));
            let seen = Arc::new(Mutex::new(Vec::new()));
            if let Some(answering) = answering {
                let (offers, seen) = (Arc::clone(&offers), Arc::clone(&seen));
                Fabric::new(node.clone(), None, None, Duration::from_secs(30))
                    .register(&rt.names().forward, move |bytes| {
                        let forward: Forward = envelope::decode(&bytes).expect("an envelope");
                        let n = offers.fetch_add(1, Ordering::SeqCst);
                        let answer = answering(n, envelope::decode(&bytes).expect("again"));
                        seen.lock().expect("lock").push(forward);
                        answer
                    })
                    .expect("a handler");
            }
            Entry { node, offers, seen }
        })
        .collect();
    Fixture { rt, entries }
}

fn reply(answer: Result<Accepted>) -> Vec<u8> {
    envelope::encode(&answer).expect("encode")
}

/// Takes every offer.
fn accepting() -> Option<Answering> {
    Some(Arc::new(|_, _| Box::pin(async { reply(Ok(None)) })))
}

/// Declines every offer with `kind`, naming the offer.
fn declining(kind: ErrorKind) -> Option<Answering> {
    Some(Arc::new(move |n, _| {
        Box::pin(async move { reply(Err(Error::new(kind, format!("refusal {n}")))) })
    }))
}

/// Declines its first offer with `Overloaded`, and takes the rest.
fn declining_once() -> Option<Answering> {
    Some(Arc::new(|n, _| {
        Box::pin(async move {
            if n == 0 {
                reply(Err(Error::new(ErrorKind::Overloaded, "full")))
            } else {
                reply(Ok(None))
            }
        })
    }))
}

fn config(instance_failovers: usize) -> RuntimeConfig {
    RuntimeConfig {
        instance_failovers,
        ..RuntimeConfig::default()
    }
}

fn hand_off(rt: &Runtime, cx: &Cx) -> Forward {
    Forward {
        version: VERSION,
        identity: rt.identity,
        hop: 0,
        pos: 1,
        cx: WireCx::of(cx),
        reply: ReplyTo::test_value(7),
        payload: vec![1, 2, 3],
    }
}

/// Send one hand-off from `fixture`'s sender by `router`, bounded.
async fn send(fixture: &Fixture, router: &Router, cx: &Cx) -> Result<()> {
    let envelope = hand_off(&fixture.rt, cx);
    tokio::time::timeout(
        WAIT,
        fixture.rt.send_forward("d", router, cx, envelope, false),
    )
    .await
    .expect("answered in time")
    .map(drop)
}

const ROUND_ROBIN: Router = Router::Roster(Within::Policy(Policy::RoundRobin));

fn selector<S: gdl::Selector>(selector: Arc<S>) -> Router {
    Router::Roster(Within::Selector {
        name: Arc::from("scripted"),
        selector: Arc::new(Typed(selector)),
        encoding: Default::default(),
    })
}

/// A policy's hand-off that its instance declines is taken by the next
/// one the policy picks; with no failover, the refusal is the request's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declined_hand_off_is_offered_to_the_next_pick() {
    let offered = fixture(
        vec![declining(ErrorKind::Overloaded), accepting()],
        config(2),
    )
    .await;
    send(&offered, &ROUND_ROBIN, &Cx::new())
        .await
        .expect("the second instance took it");
    let offers = |fixture: &Fixture| {
        fixture
            .entries
            .iter()
            .map(Entry::offers)
            .collect::<Vec<_>>()
    };
    assert_eq!(offers(&offered), [1, 1]);

    let none = fixture(
        vec![declining(ErrorKind::Overloaded), accepting()],
        config(0),
    )
    .await;
    let error = send(&none, &ROUND_ROBIN, &Cx::new())
        .await
        .expect_err("a refusal is final with no failover");
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    assert_eq!(offers(&none), [1, 0]);
}

/// A scripted selector: on its `n`th call it chooses the instance
/// `picks[n]` names (the last one for every later call), with a lease
/// and the meta `choice<n>`, and records what it was told.
struct Scripted {
    picks: Vec<velo::InstanceId>,
    /// `declined()` at each call, as raw ids.
    declined: Mutex<Vec<Vec<u128>>>,
    /// The leases held when each call began.
    live_at: Mutex<Vec<usize>>,
    live: Arc<AtomicUsize>,
}

impl Scripted {
    fn new(picks: Vec<velo::InstanceId>) -> Arc<Self> {
        Arc::new(Self {
            picks,
            declined: Mutex::default(),
            live_at: Mutex::default(),
            live: Arc::default(),
        })
    }

    fn declined(&self) -> Vec<Vec<u128>> {
        self.declined.lock().expect("lock").clone()
    }

    fn live_at(&self) -> Vec<usize> {
        self.live_at.lock().expect("lock").clone()
    }
}

struct Lease(Arc<AtomicUsize>);

impl Drop for Lease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl gdl::Selector for Scripted {
    type Head = gdl::Encoded;

    async fn select<'a>(
        &'a self,
        request: gdl::Request<'a, gdl::Encoded>,
        instances: gdl::Instances<'a>,
    ) -> Result<gdl::Choice<'a>> {
        let call = {
            let mut declined = self.declined.lock().expect("lock");
            declined.push(request.declined().iter().map(|id| id.as_u128()).collect());
            declined.len() - 1
        };
        self.live_at
            .lock()
            .expect("lock")
            .push(self.live.load(Ordering::SeqCst));
        let pick = self.picks[call.min(self.picks.len() - 1)];
        let chosen = instances
            .iter()
            .find(|instance| instance.id().as_u128() == pick.as_u128())
            .expect("the scripted instance is offered");
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(gdl::Choice::new(chosen)
            .with_lease(Lease(Arc::clone(&self.live)))
            .with_meta(format!("choice{call}"), "x"))
    }
}

/// A selector is asked again after a decline and told who declined; it
/// may choose the same instance, which takes the request the second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_selector_may_choose_an_instance_that_declined() {
    let fixture = fixture(vec![declining_once(), accepting()], config(2)).await;
    let first = fixture.entries[0].id();
    let scripted = Scripted::new(vec![first]);
    send(&fixture, &selector(Arc::clone(&scripted)), &Cx::new())
        .await
        .expect("taken the second time");
    assert_eq!(scripted.declined(), [vec![], vec![first.as_u128()]]);
    assert_eq!(fixture.entries[0].offers(), 2);
    assert_eq!(fixture.entries[1].offers(), 0);
}

/// A declined choice's lease is dropped before the selector chooses
/// again, and its meta is not sent with the next offer; the request's
/// own meta is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declined_choice_leaves_no_lease_and_no_meta() {
    let fixture = fixture(
        vec![declining(ErrorKind::Overloaded), accepting()],
        config(2),
    )
    .await;
    let (first, second) = (fixture.entries[0].id(), fixture.entries[1].id());
    let scripted = Scripted::new(vec![first, second]);
    let mut cx = Cx::new();
    cx.insert_meta("session", "s1");
    let envelope = hand_off(&fixture.rt, &cx);
    let handed = tokio::time::timeout(
        WAIT,
        fixture
            .rt
            .send_forward("d", &selector(Arc::clone(&scripted)), &cx, envelope, false),
    )
    .await
    .expect("answered in time")
    .expect("the second instance took it");
    assert_eq!(scripted.live_at(), [0, 0], "a declined lease was held");
    assert_eq!(scripted.live.load(Ordering::SeqCst), 1, "the taken lease");
    drop(handed);
    assert_eq!(scripted.live.load(Ordering::SeqCst), 0);
    let seen = fixture.entries[1].seen.lock().expect("lock");
    let meta = seen[0]
        .cx
        .clone()
        .arrive(Duration::ZERO, || 0)
        .expect("live");
    let keys: Vec<&str> = meta.meta().keys().map(String::as_str).collect();
    assert_eq!(keys, ["choice1", "session"]);
}

/// An unknown outcome (no answer within the setup timeout) and an error
/// that is not a decline are the request's: no other instance is offered
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_outcome_or_another_error_is_not_offered_again() {
    let silent: Answering = Arc::new(|_, _| Box::pin(futures::future::pending()));
    // Short, so that the unanswered offer ends soon.
    let quick = RuntimeConfig {
        setup_timeout: Duration::from_millis(300),
        ..config(2)
    };
    let unknown = fixture(vec![Some(silent), accepting()], quick).await;
    let error = send(&unknown, &ROUND_ROBIN, &Cx::new())
        .await
        .expect_err("an unanswered hand-off fails");
    assert_eq!(error.kind(), ErrorKind::OutcomeUnknown, "{error}");
    assert!(!error.message().contains("into class"), "{error}");
    assert_eq!(unknown.entries[1].offers(), 0, "offered again");

    let failing = fixture(vec![declining(ErrorKind::Node), accepting()], config(2)).await;
    let error = send(&failing, &ROUND_ROBIN, &Cx::new())
        .await
        .expect_err("a node error is final");
    assert_eq!(error.kind(), ErrorKind::Node, "{error}");
    assert_eq!(failing.entries[1].offers(), 0, "offered again");
}

/// When every offer misses, the request fails with the last refusal; a
/// hand-off that never left replaces no refusal, and is the error only
/// when no instance refused it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_miss_fails_with_the_last_refusal() {
    let refusing = fixture(
        vec![
            declining(ErrorKind::Overloaded),
            declining(ErrorKind::Placement),
        ],
        config(2),
    )
    .await;
    let error = send(&refusing, &ROUND_ROBIN, &Cx::new())
        .await
        .expect_err("every instance refused");
    // Three offers, round robin: the first instance's second refusal.
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    assert_eq!(error.message(), "refusal 1");
    assert_eq!(refusing.entries[0].offers(), 2);
    assert_eq!(refusing.entries[1].offers(), 1);

    // The turns: unsent, refused, unsent.
    let mixed = fixture(vec![None, declining(ErrorKind::Overloaded)], config(2)).await;
    let error = send(&mixed, &ROUND_ROBIN, &Cx::new())
        .await
        .expect_err("nothing took it");
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    assert_eq!(mixed.entries[1].offers(), 1);

    let unsent = fixture(vec![None, None], config(2)).await;
    let error = send(&unsent, &ROUND_ROBIN, &Cx::new())
        .await
        .expect_err("nothing was sent");
    assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
}

/// A request whose deadline passes, or that is cancelled, while its offer
/// is declined is not offered again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_that_ended_during_an_offer_is_not_offered_again() {
    for deadline in [true, false] {
        let cx = if deadline {
            Cx::new().with_timeout(Duration::from_millis(200))
        } else {
            Cx::new()
        };
        let ending = cx.clone();
        // Declines only once the request has ended.
        let late: Answering = Arc::new(move |_, _| {
            let ending = ending.clone();
            Box::pin(async move {
                if !deadline {
                    ending.cancel();
                }
                ending.done().await;
                reply(Err(Error::new(ErrorKind::Overloaded, "full")))
            })
        });
        let fixture = fixture(vec![Some(late), accepting()], config(2)).await;
        let error = send(&fixture, &ROUND_ROBIN, &cx)
            .await
            .expect_err("the request ended");
        let ended = if deadline {
            ErrorKind::DeadlineExceeded
        } else {
            ErrorKind::Cancelled
        };
        assert_eq!(error.kind(), ended, "{error}");
        assert_eq!(fixture.entries[1].offers(), 0, "offered again");
    }
}

/// A sender whose own instance is the first of `d`'s, with a
/// hand-written entry that refuses every hand-off with `kind`, and
/// another instance that takes every offer.
struct RefusingHere {
    rt: Runtime,
    /// The hand-offs this instance's entry refused.
    refused: Arc<AtomicUsize>,
    /// The envelopes the other instance was offered.
    seen: Arc<Mutex<Vec<Forward>>>,
    /// Kept: the runtime holds its entries weakly.
    _entries: Arc<Entries>,
    /// Kept: the node stops when its last handle drops.
    _other: velo::Velo,
}

async fn refusing_here(kind: ErrorKind) -> RefusingHere {
    let sender = bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("a velo node");
    let other = bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("a velo node");
    // This instance first, so that round robin chooses it first.
    let mut roster = Roster::new();
    roster.add("d", sender.peer_info());
    roster.add("d", other.peer_info());
    let rt = Runtime::new(
        Fabric::new(sender, None, None, Duration::from_secs(30)),
        layout(),
        "a".into(),
        &roster,
        config(2),
        None,
    )
    .expect("a runtime");
    let refused = Arc::new(AtomicUsize::new(0));
    let mut entries = Entries::default();
    entries.add_forward(0, 1, {
        let refused = Arc::clone(&refused);
        Arc::new(move |mut envelope: Forward| {
            refused.fetch_add(1, Ordering::SeqCst);
            // As an entry leaves an envelope whose context it read.
            envelope.cx = WireCx::default();
            Box::pin(async move {
                Err(Refused {
                    error: Error::new(kind, "refused here"),
                    decline: Default::default(),
                    envelope: Box::new(envelope),
                })
            })
        })
    });
    let entries = Arc::new(entries);
    rt.set_entries(&entries);
    let seen = Arc::new(Mutex::new(Vec::new()));
    Fabric::new(other.clone(), None, None, Duration::from_secs(30))
        .register(&rt.names().forward, {
            let seen = Arc::clone(&seen);
            move |bytes| {
                let forward: Forward = envelope::decode(&bytes).expect("an envelope");
                seen.lock().expect("lock").push(forward);
                Box::pin(async { reply(Ok(None)) })
            }
        })
        .expect("a handler");
    RefusingHere {
        rt,
        refused,
        seen,
        _entries: entries,
        _other: other,
    }
}

/// A hand-off this instance's own entry declines, by a direct call, is
/// given back whole and offered to another instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hand_off_this_instance_declines_is_offered_again() {
    let here = refusing_here(ErrorKind::Overloaded).await;
    let rt = &here.rt;
    let mut cx = Cx::new();
    cx.insert_meta("session", "s1");
    tokio::time::timeout(
        WAIT,
        rt.send_forward("d", &ROUND_ROBIN, &cx, hand_off(rt, &cx), false),
    )
    .await
    .expect("answered in time")
    .expect("the other instance took it");
    assert_eq!(here.refused.load(Ordering::SeqCst), 1);
    let seen = here.seen.lock().expect("lock");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].payload, [1, 2, 3]);
    assert_eq!(seen[0].reply.handle_u128(), 7);
    let arrived = seen[0]
        .cx
        .clone()
        .arrive(Duration::ZERO, || 0)
        .expect("live");
    assert_eq!(arrived.id(), cx.id());
    assert_eq!(arrived.meta(), cx.meta());
}

/// A refusal by this instance's own entry that is not a decline (the
/// request's deadline, or any other error) is the request's: it is not
/// offered to another instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_here_that_is_not_a_decline_is_final() {
    for kind in [ErrorKind::DeadlineExceeded, ErrorKind::Node] {
        let here = refusing_here(kind).await;
        let rt = &here.rt;
        let cx = Cx::new();
        let error = tokio::time::timeout(
            WAIT,
            rt.send_forward("d", &ROUND_ROBIN, &cx, hand_off(rt, &cx), false),
        )
        .await
        .expect("answered in time")
        .map(drop)
        .expect_err("a final refusal");
        assert_eq!(error.kind(), kind, "{error}");
        assert_eq!(here.refused.load(Ordering::SeqCst), 1);
        assert_eq!(
            here.seen.lock().expect("lock").len(),
            0,
            "a {kind:?} refusal was offered again"
        );
    }
}

/// Send enough hand-offs from `fixture`'s sender that velo's response
/// table has its memory, so that a later send allocates only what each
/// send does.
///
/// velo gives each call the next of its 65,535 response slots in turn, and
/// marks the slot in a dashmap set. The first slot that lands in a shard of
/// that set allocates the shard's table, and dashmap makes
/// `(cores * 4).next_power_of_two()` shards: 128 on 20 cores, 512 on 72.
/// So a fresh sender's sends often allocate one more, and on a big host
/// every one of 30 sends can. After twice as many sends as shards, about
/// one send in seven still does, and the fewest of 30 is the steady cost.
async fn warm(fixture: &Fixture) {
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    for _ in 0..2 * (cores * 4).next_power_of_two() {
        send(fixture, &ROUND_ROBIN, &Cx::new())
            .await
            .expect("accepted");
    }
}

/// The allocations each of `routers` makes to send one hand-off that is
/// accepted, from `fixture`'s sender to its one entry, done by `send`:
/// the fewest over a few sends, after [`warm`] and a first send by each.
async fn allocations<'a, F>(fixture: &'a Fixture, routers: &'a [Router], send: F) -> Vec<u64>
where
    F: Fn(&'a Runtime, &'a Router, Cx, Forward) -> BoxFuture<'a, Result<()>>,
{
    let mut fewest = vec![u64::MAX; routers.len()];
    for round in 0..31 {
        for (at, router) in routers.iter().enumerate() {
            let mut cx = Cx::new();
            cx.insert_meta("session", "s1");
            let envelope = hand_off(&fixture.rt, &cx);
            let sending = Counted::new(send(&fixture.rt, router, cx, envelope));
            let (sent, made) = tokio::time::timeout(WAIT, sending)
                .await
                .expect("answered in time");
            sent.expect("accepted");
            if round > 0 {
                fewest[at] = fewest[at].min(made);
            }
        }
    }
    fewest
}

/// A hand-off as it was before hand-offs were offers: the same choice,
/// then an encoding, a velo call, and its answer's decoding.
fn called<'a>(
    rt: &'a Runtime,
    router: &'a Router,
    cx: Cx,
    envelope: Forward,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let mut envelope = envelope;
        let Router::Roster(within) = router else {
            panic!("these hand-offs route in the roster");
        };
        let (instance, _held) = rt.choose("d", within, &cx, &mut envelope, &[]).await?;
        let bytes = envelope::encode(&envelope)?;
        let reply = rt
            .fabric
            .call(
                instance,
                &rt.names().forward,
                bytes,
                rt.config.setup_timeout,
            )
            .await?;
        envelope::decode::<Result<Accepted, Error>>(&reply)?.map(drop)
    })
}

fn offered<'a>(
    rt: &'a Runtime,
    router: &'a Router,
    cx: Cx,
    envelope: Forward,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        rt.send_forward("d", router, &cx, envelope, false)
            .await
            .map(drop)
    })
}

/// Replaces the request's `session` meta, which the request sets too.
struct Replacing;

impl gdl::Selector for Replacing {
    type Head = gdl::Encoded;

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, gdl::Encoded>,
        instances: gdl::Instances<'a>,
    ) -> Result<gdl::Choice<'a>> {
        let chosen = instances.get(0).expect("never empty");
        Ok(gdl::Choice::new(chosen).with_meta("session", "chosen"))
    }
}

/// An accepted first offer allocates what a hand-off did before it was
/// an offer, with a policy and with a selector, and the failover bound
/// changes nothing on that path: nothing for a failover is made before an
/// offer misses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accepted_first_offer_allocates_what_a_call_did() {
    let routers = [ROUND_ROBIN, selector(Arc::new(Replacing))];
    let two = fixture(vec![accepting()], config(2)).await;
    let none = fixture(vec![accepting()], config(0)).await;
    warm(&two).await;
    warm(&none).await;
    let calls = allocations(&two, &routers, called).await;
    let offers = allocations(&two, &routers, offered).await;
    let unbounded = allocations(&none, &routers, offered).await;
    assert_eq!(offers, calls, "an offer against a call");
    assert_eq!(unbounded, offers, "no failover against two");
}

/// The bytes of `send_forward`'s future before hand-offs were offers
/// (`0c7a1a4`, this build's settings): it is part of every request's task,
/// at the outer end and at each stage that hands a request on.
const SEND_FORWARD_BEFORE: usize = 912;

/// The first offer's future is no larger than a hand-off's was: what a
/// failover keeps lives in the boxed call a miss makes, and the envelope
/// is not held twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_offer_is_no_larger_than_a_hand_off_was() {
    let fixture = fixture(vec![accepting()], config(2)).await;
    let cx = Cx::new();
    let sending =
        fixture
            .rt
            .send_forward("d", &ROUND_ROBIN, &cx, hand_off(&fixture.rt, &cx), false);
    let size = std::mem::size_of_val(&sending);
    assert!(
        size <= SEND_FORWARD_BEFORE,
        "send_forward's future is {size} bytes, against {SEND_FORWARD_BEFORE} before offers"
    );
}

/// Answers with its request.
struct Echo;

impl gdl::Sink for Echo {
    type Req = String;
    type Item = String;

    async fn generate(&self, _: Cx, req: String) -> Result<gdl::Items<String>> {
        Ok(gdl::items(futures::stream::iter([Ok(req)])))
    }
}

/// A hand-off that this instance's own entry declines (the real entry of
/// a segment it hosts, retiring) is given back and offered to another
/// instance; the entry took nothing for it: no hand-off served, no share
/// held, no anchor minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hand_off_this_instance_retiring_declines_is_offered_again() {
    use crate::dist::segment::Deployment;
    use crate::wire::Codec;

    let here = bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("a velo node");
    let other = bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("a velo node");
    // This instance first, so that round robin chooses it first.
    let mut roster = Roster::new();
    roster.add("d", here.peer_info());
    roster.add("d", other.peer_info());
    let deployment = Deployment::new(here, &manifest(), &placement(), "d", &roster, config(2))
        .expect("a deployment");
    let codec = || Some(Codec::<String>::new(Default::default()));
    let hop = deployment
        .hop(
            0,
            crate::dist::stages::FwdEnd::new(codec()),
            crate::dist::stages::DSink::new(Arc::new(Echo)),
            crate::dist::stages::BwdEnd::new(codec()),
        )
        .expect("the hop");
    let segment = deployment.start(hop).expect("started");
    let rt = segment.runtime();
    let seen = Arc::new(AtomicUsize::new(0));
    Fabric::new(other.clone(), None, None, Duration::from_secs(30))
        .register(&rt.names().forward, {
            let seen = Arc::clone(&seen);
            move |_| {
                seen.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { reply(Ok(None)) })
            }
        })
        .expect("a handler");
    let anchors = segment.active_anchors();
    segment.close();
    let cx = Cx::new();
    tokio::time::timeout(
        WAIT,
        rt.send_forward("d", &ROUND_ROBIN, &cx, hand_off(rt, &cx), false),
    )
    .await
    .expect("answered in time")
    .expect("the other instance took it");
    assert_eq!(seen.load(Ordering::SeqCst), 1, "not offered again");
    assert_eq!(segment.load().declined, 1, "this instance did not decline");
    assert_eq!(segment.served().forwards, 0, "this instance took it");
    assert_eq!(segment.admitted(), 0, "a share was kept");
    assert_eq!(segment.active_anchors(), anchors, "an anchor was minted");
}

/// Takes the first instance, and fails on every later call with a
/// routing error of its own.
struct FailingAgain(AtomicUsize);

impl gdl::Selector for FailingAgain {
    type Head = gdl::Encoded;

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, gdl::Encoded>,
        instances: gdl::Instances<'a>,
    ) -> Result<gdl::Choice<'a>> {
        if self.0.fetch_add(1, Ordering::SeqCst) > 0 {
            return Err(Error::routing("the selector's own error"));
        }
        Ok(gdl::Choice::new(instances.get(0).expect("never empty")))
    }
}

/// When the routing table has nothing left to route to by the time the
/// router would choose again, the request fails with the refusal it had,
/// as when no failover is left, by a policy and by a selector; a
/// selector's own error is still the request's, as it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_roster_emptied_after_a_refusal_fails_with_the_refusal() {
    let routers = [
        ROUND_ROBIN,
        selector(Arc::new(FailingAgain(AtomicUsize::new(0)))),
    ];
    for router in &routers {
        let sender = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let only = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let mut roster = Roster::new();
        roster.add("d", only.peer_info());
        let live = Arc::clone(roster.live());
        let rt = Runtime::new(
            Fabric::new(sender, None, None, Duration::from_secs(30)),
            layout(),
            "a".into(),
            &roster,
            config(2),
            None,
        )
        .expect("a runtime");
        // The only instance declines, and leaves before the answer is
        // read: nothing is left to choose.
        let id = only.instance_id();
        Fabric::new(only.clone(), None, None, Duration::from_secs(30))
            .register(&rt.names().forward, move |_| {
                live.leave("d", id);
                Box::pin(async { reply(Err(Error::new(ErrorKind::Overloaded, "full"))) })
            })
            .expect("a handler");
        let cx = Cx::new();
        let error = tokio::time::timeout(
            WAIT,
            rt.send_forward("d", router, &cx, hand_off(&rt, &cx), false),
        )
        .await
        .expect("answered in time")
        .map(drop)
        .expect_err("nothing took it");
        assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
        assert_eq!(error.message(), "full");
    }

    // Control: with an instance left, the selector's own error on its
    // second choice is the request's.
    let fixture = fixture(
        vec![declining(ErrorKind::Overloaded), accepting()],
        config(2),
    )
    .await;
    let router = selector(Arc::new(FailingAgain(AtomicUsize::new(0))));
    let error = send(&fixture, &router, &Cx::new())
        .await
        .expect_err("the selector failed");
    assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
    assert_eq!(error.message(), "the selector's own error");
    assert_eq!(fixture.entries[1].offers(), 0);
}

/// An instance that stopped before this sender's first contact with it is
/// one velo proves the hand-off never reached: its handshake fails before
/// the send is admitted, because no connection opens. So the hand-off is
/// offered to another instance, and with none, fails as unsent
/// (`Transport`), not as an unknown outcome. An instance that stops after
/// its handshake is the other case
/// (`an_unknown_outcome_or_another_error_is_not_offered_again`).
///
/// The stopped instance is an address that refuses connections
/// ([`Refusing`]), not a node this test stops: a stopped node's port can be
/// listening again (another node took it, or its own listener outlived
/// its shutdown), and a hand-off sent there waits out velo's handshake
/// timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_gone_before_first_contact_is_unsent_and_offered_again() {
    for alone in [false, true] {
        let answers = if alone { vec![] } else { vec![accepting()] };
        // The first instance in the roster, which round robin offers to
        // first.
        let gone = Refusing::new();
        let fixture = fixture_after(Some(gone.peer()), answers, config(2)).await;
        let result = send(&fixture, &ROUND_ROBIN, &Cx::new()).await;
        if alone {
            let error = result.expect_err("nothing could take it");
            assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        } else {
            result.expect("the other instance took it");
            assert_eq!(fixture.entries[0].offers(), 1);
        }
    }
}

/// An instance whose address another deployment's node now holds, as
/// when it stopped and the OS gave its port to that node, is not offered
/// to: velo's handshake does not check who answers at an address, but that
/// node has no handler of this deployment, so the hand-off is unsent. Tests
/// running side by side (other copies of this binary too) are such
/// neighbours, which is why each fixture is a deployment of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_address_another_deployment_holds_is_not_offered_to() {
    let neighbour = fixture(vec![accepting()], config(2)).await;
    let taken = neighbour.entries[0].node.peer_info();
    let sender = bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("a velo node");
    let mut roster = Roster::new();
    roster.add(
        "d",
        velo::PeerInfo::new(velo::InstanceId::new_v4(), taken.worker_address().clone()),
    );
    let rt = Runtime::new(
        Fabric::new(sender, None, None, Duration::from_secs(30)),
        layout(),
        "a".into(),
        &roster,
        config(2),
        None,
    )
    .expect("a runtime");
    let stale = Fixture {
        rt,
        entries: Vec::new(),
    };
    let result = send(&stale, &ROUND_ROBIN, &Cx::new()).await;
    assert_eq!(neighbour.entries[0].offers(), 0, "the neighbour took it");
    let error = result.expect_err("nothing could take it");
    assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
}

/// A segment declines offers by its own policy as full or gone, and takes
/// them again; a drain's refusal says gone, and outlasts the segment's
/// own, which neither replaces nor undoes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drains_refusal_outlasts_the_segments_own() {
    use crate::dist::offers::Decline;

    let fixture = fixture(vec![accepting()], config(0)).await;
    let rt = &fixture.rt;
    let declined = || rt.check_offer().err().map(|refusal| refusal.decline);
    assert_eq!(declined(), None);
    rt.decline_offers_as(Decline::Full);
    assert_eq!(declined(), Some(Decline::Full));
    rt.decline_offers_as(Decline::Gone);
    assert_eq!(declined(), Some(Decline::Gone));
    rt.take_offers();
    assert_eq!((declined(), rt.declines_offers()), (None, false));

    rt.decline_offers_as(Decline::Full);
    rt.decline_offers();
    let refusal = rt.check_offer().expect_err("declined");
    assert_eq!(refusal.decline, Decline::Gone);
    assert_eq!(refusal.error.kind(), ErrorKind::Overloaded);
    assert!(
        refusal.error.message().contains("draining"),
        "{}",
        refusal.error
    );
    rt.take_offers();
    assert_eq!(declined(), Some(Decline::Gone));
    rt.decline_offers_as(Decline::Full);
    assert_eq!(declined(), Some(Decline::Gone));
}

/// A draining entry must tell a stale chooser it is gone even while
/// admitted requests hold every permit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_draining_entry_still_declines_as_gone() {
    use crate::dist::hop::Register;
    use crate::dist::offers::Decline;
    use crate::dist::segment::Deployment;
    use crate::dist::stages::{BwdEnd, DSink, FwdEnd};
    use crate::wire::Codec;

    let node = bind_node(([127, 0, 0, 1], 0).into())
        .await
        .expect("a velo node");
    let placement = placement().class("global", ["a"]).class("model_x", ["d"]);
    let deployment = Deployment::new(
        node,
        &manifest(),
        &placement,
        "d",
        &Roster::new(),
        RuntimeConfig {
            admission_limit: 1,
            admission_wait: Duration::from_millis(20),
            ..config(0)
        },
    )
    .expect("a deployment");
    let codec = || Some(Codec::<String>::new(Default::default()));
    let hop = deployment
        .hop(
            0,
            FwdEnd::new(codec()),
            DSink::new(Arc::new(Echo)),
            BwdEnd::new(codec()),
        )
        .expect("the hop");
    let mut entries = Entries::default();
    hop.register(&mut entries);
    let segment = deployment.start(hop).expect("started");
    let rt = segment.runtime();
    assert!(rt.offered(0), "the hand-off crosses into this class");
    let entry = entries.forward(0, 1).expect("the entry");
    segment.decline_offers();
    let refusal = tokio::time::timeout(WAIT, entry(hand_off(rt, &Cx::new())))
        .await
        .expect("answered in time")
        .expect_err("the drain refuses with spare capacity");
    assert_eq!(refusal.decline, Decline::Gone, "the control");

    let held = rt.admission.admit(7).await.expect("the only permit");
    assert_eq!(segment.admitted(), 1);
    let refusal = tokio::time::timeout(WAIT, entry(hand_off(rt, &Cx::new())))
        .await
        .expect("answered in time")
        .expect_err("the drain refuses at capacity");
    drop(held);
    assert_eq!(refusal.error.kind(), ErrorKind::Overloaded);
    assert_eq!(
        refusal.decline,
        Decline::Gone,
        "admission hid the drain: {}",
        refusal.error
    );
}
