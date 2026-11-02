//! A live session keeps its entry instance while instances join its pool.
//! A chooser routes into a model pool with `affinity`, keyed on the
//! session, with `through` set. A pool policy keeps each session's cutoff
//! as a set router keeps it in its pin: the row's highest join stamp
//! (`Pool::joined_through`) when the session first chooses the pool, and
//! the row's again once it offers no instance at or below the cutoff
//! (`Pool::joined_from`). Each turn after the first carries its cutoff in
//! the caller's context (`JoinedThrough`), as the set router's lookup puts
//! it there.
//!
//! Every hub and instance runs in this process on OS-assigned ports, with
//! real pool hubs and the global hub's relay between them. Each wait is
//! bounded and for something observed. Each entry instance logs the turns
//! it served, so a test reads which instance served each session. A test
//! makes 600 to 2,400 turns, 32 at a time, and the rolling replacement
//! starts and stops 11 instances, so the longest takes several seconds.

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::boundary::{
    Model, RUNTIME, Running, instance_of, model_of, placement, run_graph, spec_of,
};
use common::pools::{Resolving, followed, global_hub, global_pool, pool_hub_of, settings, until};
use common::{Chunk, Events, Plan, Prompt, collect, expected, prompt, reference_graph};
use futures::StreamExt;
use gdl::{
    Affinity, AffinityConfig, Choice, Cx, Encoded, Instances, JoinedThrough, Next, NoPool, Pool,
    PoolChoice, PoolPolicies, PoolPolicy, PoolRequest, Request, RoundRobin, Selector, Selectors,
};
use gdl_runtime::velo::InstanceId;
use gdl_runtime::{Deployment, DeploymentSpec, Pools, Register, RequestRouter, RuntimeConfig};
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";

/// The sessions of each test.
const SESSIONS: usize = 200;

/// Each session's cutoff, by its key, as a set router's pins keep it.
type Pins = Arc<Mutex<HashMap<String, JoinedThrough>>>;

/// `round_robin` over the class's pools, which keeps each session's cutoff
/// from the row it chose: when the session has none, when the row is of a
/// later hub, and when the row offers no instance at or below it, as long
/// as the row's join order is known.
#[derive(Clone, Default)]
struct Pinning {
    pins: Pins,
    /// How many cutoffs moved up because the row offered none at or below.
    advanced: Arc<AtomicUsize>,
}

impl PoolPolicy for Pinning {
    type Head = Encoded;

    fn choose(
        &self,
        request: PoolRequest<'_, Encoded>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let session = request.cx().meta().get("session").cloned();
        let at = RoundRobin
            .choose_where(request, pools, |_| true)
            .ok_or_else(NoPool::new)?;
        let pool = &pools[at];
        if let (Some(session), Some(through)) = (session, pool.joined_through()) {
            let fresh = JoinedThrough {
                epoch: pool.epoch(),
                joined: through,
            };
            let mut pins = self.pins.lock().expect("lock");
            match pins.get(&session) {
                None => {
                    pins.insert(session, fresh);
                }
                Some(pin) if pin.epoch < fresh.epoch => {
                    pins.insert(session, fresh);
                }
                Some(pin)
                    if pin.epoch == fresh.epoch
                        && pool.joined_from().is_some_and(|from| from > pin.joined) =>
                {
                    pins.insert(session, fresh);
                    self.advanced.fetch_add(1, Ordering::SeqCst);
                }
                Some(_) => {}
            }
        }
        Ok(PoolChoice::new(at))
    }
}

/// The selector the crossing names: `affinity` on the session, reading
/// its cutoff.
fn sessions() -> Affinity {
    Affinity::new(AffinityConfig {
        key: Some("session".into()),
        through: true,
    })
}

/// The reference graph, its pool chosen by [`Pinning`], and its instance by
/// `selector`.
fn spec(selector: &str) -> DeploymentSpec {
    spec_of(
        placement()
            .route("B", RequestRouter::Selector(selector.into()))
            .pool_policy(
                "model_x",
                gdl_model::PoolPolicy {
                    policy: "pinning".into(),
                    provides: Vec::new(),
                },
            ),
    )
}

/// The model pool [`M1`] with `entries` entry instances.
fn pool(entries: usize) -> Model<'static> {
    Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries,
    }
}

/// A chooser, an instance of `a` in the global pool at `hub`, with
/// `selector` registered as `name` and `pinning` as its pool policy.
async fn chooser<S: Selector>(
    spec: &DeploymentSpec,
    hub: &Hub,
    name: &str,
    selector: S,
    pinning: &Pinning,
) -> Running<impl Next<Prompt, Chunk> + Register + use<S>> {
    let mut selectors = Selectors::new();
    selectors.add(name, selector).expect("a selector");
    let mut policies = PoolPolicies::new();
    policies
        .add("pinning", pinning.clone())
        .expect("a pool policy");
    let extra = format!("{}{RUNTIME}", global_pool());
    let mut config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    config.runtime.pool_policies = policies.clone();
    let runtime = RuntimeConfig {
        selectors,
        pool_policies: policies,
        ..config.runtime.clone()
    };
    run_graph(&config, "a", spec, runtime, &|deployment| {
        reference_graph(deployment, &Events::quiet())
    })
    .await
    .expect("the chooser serves")
}

fn id<H>(running: &Running<H>) -> InstanceId {
    running.segment.peer_info().instance_id()
}

/// A global hub, a chooser, and the model pool [`M1`], whose entry
/// instances the chooser routes to with [`sessions`].
struct Fleet<H, C> {
    spec: DeploymentSpec,
    /// The pool directory the hubs share.
    _directory: Resolving,
    hubs: Vec<Hub>,
    chooser: Running<C>,
    pins: Pinning,
    /// Each entry instance, with the log of the requests it served.
    entries: Vec<(Running<H>, Arc<Events>)>,
    /// The pool's `d`, which makes its quorum.
    sink: Option<Running<H>>,
    /// What each instance of the pool serves, logging to the events given.
    assemble: fn(&Deployment, &Arc<Events>) -> gdl::Result<H>,
    /// How many rounds of turns were made, which names each round's turns.
    rounds: AtomicUsize,
}

/// The graph each instance of the pool serves.
fn graph(
    deployment: &Deployment,
    events: &Arc<Events>,
) -> gdl::Result<impl Next<Prompt, Chunk> + Register + use<>> {
    reference_graph(deployment, events)
}

/// Start a [`Fleet`] whose model pool has `entries` entry instances, and
/// wait until the chooser routes to each.
async fn fleet(
    entries: usize,
) -> Fleet<impl Next<Prompt, Chunk> + Register, impl Next<Prompt, Chunk> + Register> {
    let spec = spec("sessions");
    let directory = Resolving::new();
    let global = pool_hub_of(&spec, global_hub(), &directory).await.0;
    let pins = Pinning::default();
    let chooser = chooser(&spec, &global, "sessions", sessions(), &pins).await;
    // The first entry instance starts with the pool's `d`, which waits for
    // it, and shares its log, in which `d` writes no `B`; each later one
    // joins with a log of its own.
    let first = Events::new();
    let mut model = model_of(&spec, &directory, pool(1), &|r| r, &|deployment| {
        graph(deployment, &first)
    })
    .await;
    let entry = model.entries.pop().expect("the first entry");
    let mut fleet = Fleet {
        spec,
        _directory: directory,
        hubs: vec![global, model.hub],
        chooser,
        pins,
        entries: vec![(entry, first)],
        sink: Some(model.sink),
        assemble: graph,
        rounds: AtomicUsize::new(0),
    };
    fleet.routes_to_its_entries().await;
    for _ in 1..entries {
        fleet.join().await;
    }
    fleet
}

impl<H, C> Fleet<H, C>
where
    H: Next<Prompt, Chunk> + Register + Send + Sync + 'static,
    C: Next<Prompt, Chunk>,
{
    fn tables(&self) -> Pools {
        self.chooser.pools.clone().expect("a chooser")
    }

    /// Wait until the chooser's table of [`M1`] holds the entry instances
    /// running now, and no other.
    async fn routes_to_its_entries(&self) {
        let ids: BTreeSet<u128> = self
            .entries
            .iter()
            .map(|(entry, _)| id(entry).as_u128())
            .collect();
        until(&self.tables(), "the pool's entries in the table", |pools| {
            followed(pools, "f", M1).is_some_and(|m| {
                m.eligible
                    .iter()
                    .map(|id| id.as_u128())
                    .collect::<BTreeSet<_>>()
                    == ids
            })
        })
        .await;
    }

    /// One more entry instance, once the chooser routes to it: its id.
    async fn join(&mut self) -> InstanceId {
        let events = Events::new();
        let assemble = self.assemble;
        let entry = instance_of(&self.spec, &self.hubs[1], pool(1), "f", &|deployment| {
            assemble(deployment, &events)
        })
        .await;
        let joined = id(&entry);
        self.entries.push((entry, events));
        self.routes_to_its_entries().await;
        joined
    }

    /// Stop the entry instance `at`, once the chooser no longer routes to
    /// it: its id. Its drain goes on meanwhile, with nothing to drain.
    async fn leave(&mut self, at: usize) -> InstanceId {
        let (entry, _) = self.entries.remove(at);
        let gone = id(&entry);
        let stopped = tokio::spawn(entry.stop());
        self.routes_to_its_entries().await;
        drop(stopped);
        gone
    }

    /// Wait until each entry instance has served a request whole: its own
    /// roster then holds an instance of every segment after it. The pool
    /// takes work once its hub counts a new `d`, which an entry's roster
    /// can learn a moment later, and a request it takes meanwhile fails.
    /// Probes have keys of their own and no cutoff, so they spread over
    /// every instance and change no session's.
    async fn every_entry_serves(&self) {
        let want: BTreeSet<u128> = self
            .entries
            .iter()
            .map(|(entry, _)| id(entry).as_u128())
            .collect();
        let mut served = BTreeSet::new();
        let probed = tokio::time::timeout(common::WAIT, async {
            for n in 0.. {
                if served == want {
                    return;
                }
                let key = format!("probe-{n}");
                let mut cx = Cx::new();
                cx.insert_meta("session", key.as_str());
                let plan = Plan {
                    tokens: 1,
                    ..Plan::default()
                };
                let Ok(out) = self.chooser.segment.call(cx, prompt(&key, plan)).await else {
                    continue;
                };
                if collect(out).await != expected(0..1) {
                    continue;
                }
                let start = format!("B:start:{key}");
                if let Some((entry, _)) = self.entries.iter().find(|(_, events)| events.has(&start))
                {
                    served.insert(id(entry).as_u128());
                }
            }
        })
        .await;
        assert!(probed.is_ok(), "only {served:?} of {want:?} served");
    }

    /// A turn of each of `sessions`, with its cutoff when `cut` and it has
    /// one, 32 at a time: the instance that served each.
    async fn turns_cut(&self, sessions: &[String], cut: bool) -> Vec<InstanceId> {
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let named = |session: &str| format!("{session}#{round}");
        futures::stream::iter(sessions)
            .for_each_concurrent(32, |session| async move {
                let mut cx = Cx::new();
                cx.insert_meta("session", session.as_str());
                let pin = self.pins.pins.lock().expect("lock").get(session).copied();
                if let Some(pin) = pin.filter(|_| cut) {
                    cx.insert_ext(pin);
                }
                let plan = Plan {
                    tokens: 1,
                    ..Plan::default()
                };
                let out = self
                    .chooser
                    .segment
                    .call(cx, prompt(&named(session), plan))
                    .await
                    .expect("routed");
                assert_eq!(collect(out).await, expected(0..1), "session {session}");
            })
            .await;
        let mut served: HashMap<String, InstanceId> = HashMap::new();
        for (entry, events) in &self.entries {
            for event in events.snapshot() {
                if let Some(text) = event.strip_prefix("B:start:") {
                    let again = served.insert(text.to_string(), id(entry));
                    assert!(again.is_none() || !text.ends_with(&format!("#{round}")));
                }
            }
        }
        sessions
            .iter()
            .map(|session| {
                *served
                    .get(&named(session))
                    .unwrap_or_else(|| panic!("no instance served {session}"))
            })
            .collect()
    }

    /// A turn of each of `sessions`, with its cutoff.
    async fn turns(&self, sessions: &[String]) -> Vec<InstanceId> {
        self.turns_cut(sessions, true).await
    }

    async fn stop(self) {
        for hub in self.hubs {
            hub.shutdown().await.expect("valid test fixture");
        }
    }
}

/// `count` session keys, starting at `from`.
fn keys(from: usize, count: usize) -> Vec<String> {
    (from..from + count)
        .map(|n| format!("session-{n}"))
        .collect()
}

/// How many turns each instance served.
fn spread(served: &[InstanceId]) -> BTreeMap<u128, usize> {
    let mut spread = BTreeMap::new();
    for id in served {
        *spread.entry(id.as_u128()).or_insert(0) += 1;
    }
    spread
}

/// Records the cutoff each choice's context carried.
#[derive(Clone, Default)]
struct Probe(Arc<Mutex<Vec<Option<JoinedThrough>>>>);

impl Selector for Probe {
    type Head = Encoded;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Encoded>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        let cut = request.cx().ext::<JoinedThrough>().copied();
        self.0.lock().expect("lock").push(cut);
        Ok(Choice::new(instances.get(0).expect("never empty")))
    }
}

/// The cutoff a caller puts in the context it calls the chooser with
/// reaches the selector of the crossing into the pool, in the same
/// segment, and the pool policy reads the row's join stamps from the pool
/// hub, through the relay: known, and in the hub's registration order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cutoff_in_the_callers_context_reaches_the_selector() {
    let spec = spec("probe");
    let directory = Resolving::new();
    let global = pool_hub_of(&spec, global_hub(), &directory).await.0;
    let probe = Probe::default();
    let pins = Pinning::default();
    let chooser = chooser(&spec, &global, "probe", probe.clone(), &pins).await;
    let model = model_of(&spec, &directory, pool(2), &|r| r, &|deployment| {
        reference_graph(deployment, &Events::quiet())
    })
    .await;
    let pools = chooser.pools.clone().expect("a chooser");
    until(&pools, "both entries in the table", |pools| {
        followed(pools, "f", M1).is_some_and(|m| m.eligible.len() == 2)
    })
    .await;

    let cut = JoinedThrough {
        epoch: 9,
        joined: 4,
    };
    for carried in [None, Some(cut)] {
        let mut cx = Cx::new();
        cx.insert_meta("session", "s");
        if let Some(cut) = carried {
            cx.insert_ext(cut);
        }
        let plan = Plan {
            tokens: 1,
            ..Plan::default()
        };
        let out = chooser
            .segment
            .call(cx, prompt("s", plan))
            .await
            .expect("routed");
        assert_eq!(collect(out).await, expected(0..1));
    }
    assert_eq!(*probe.0.lock().expect("lock"), [None, Some(cut)]);
    let pin = pins.pins.lock().expect("lock").get("s").copied();
    let pin = pin.expect("the policy kept the session's cutoff");
    assert!(pin.epoch > 0 && pin.joined > 0, "{pin:?}");
    for hub in [global, model.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A pool of 5 serves 200 sessions, each of which keeps its first turn's
/// cutoff. A sixth instance joins: no session moves, though without its
/// cutoff about a sixth of them would, and 200 new sessions spread over
/// all 6.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_moves_no_session_and_new_sessions_use_every_instance() {
    let mut fleet = fleet(5).await;
    let old = keys(0, SESSIONS);
    let first = fleet.turns(&old).await;
    assert_eq!(spread(&first).len(), 5, "{:?}", spread(&first));

    let joiner = fleet.join().await;
    assert_eq!(fleet.turns(&old).await, first, "a join moved a session");
    // The control: without its cutoff, a session that scores highest on
    // the joiner goes there.
    let uncut = fleet.turns_cut(&old, false).await;
    assert!(uncut.contains(&joiner), "no session would have moved");

    let new = fleet.turns(&keys(SESSIONS, SESSIONS)).await;
    let spread = spread(&new);
    assert_eq!(spread.len(), 6, "{spread:?}");
    assert!(spread[&joiner.as_u128()] > 0);
    fleet.stop().await;
}

/// An instance's own decline sends a session's turn to the other instance
/// inside its cutoff. A full instance declines for its whole pool, as
/// `docs/src/operations/limitations.md` says: with no other pool and a
/// policy that does not re-queue, the turn fails with `Overloaded`, and
/// the other instance inside the cutoff is never offered it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_instance_declines_for_its_pool_and_an_own_decline_walks_the_cutoff() {
    let fleet = fleet(2).await;
    fleet.every_entry_serves().await;
    let sessions = keys(0, 64);
    let first = fleet.turns(&sessions).await;
    assert_eq!(spread(&first).len(), 2, "both instances serve");
    let at = fleet
        .entries
        .iter()
        .position(|(entry, _)| id(entry) == first[0])
        .expect("the serving instance");
    let other = &fleet.entries[1 - at].0;
    let pin = fleet.pins.pins.lock().expect("lock")[&sessions[0]];
    assert!(pin.epoch > 0 && pin.joined > 0);

    fleet.entries[at]
        .0
        .segment
        .decline_offers_as(gdl::Decline::Instance);
    let alternate = fleet.turns(&sessions[..1]).await;
    assert_eq!(
        alternate[0],
        id(other),
        "an instance's own decline reaches the other instance inside the cutoff"
    );

    fleet.entries[at]
        .0
        .segment
        .decline_offers_as(gdl::Decline::Full);
    let offered = other.offered();
    let mut cx = Cx::new();
    cx.insert_meta("session", sessions[0].as_str());
    cx.insert_ext(pin);
    let plan = Plan {
        tokens: 1,
        ..Plan::default()
    };
    let result = fleet
        .chooser
        .segment
        .call(cx, prompt("full-probe", plan))
        .await
        .map(drop);
    let tried = other.offered() - offered;
    fleet.entries[at].0.segment.take_offers();
    fleet.stop().await;
    let error = result.expect_err("a full instance refuses for its pool");
    assert_eq!(error.kind(), gdl::ErrorKind::Overloaded, "{error}");
    assert_eq!(tried, 0, "a full decline was offered to another instance");
}

/// When one instance of 5 leaves, only its sessions move, and they spread
/// over the 4 that remain; every other session stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_moves_only_its_sessions_and_spreads_them() {
    let mut fleet = fleet(5).await;
    let sessions = keys(0, SESSIONS);
    let first = fleet.turns(&sessions).await;
    let gone = fleet.leave(2).await;
    let then = fleet.turns(&sessions).await;
    let mut moved = Vec::new();
    for (session, (was, now)) in sessions.iter().zip(first.iter().zip(&then)) {
        if *was == gone {
            moved.push(*now);
        } else {
            assert_eq!(
                was, now,
                "session {session} moved, though its instance stayed"
            );
        }
    }
    assert!(
        !moved.is_empty(),
        "no session was on the instance that left"
    );
    assert!(
        spread(&moved).len() >= 3.min(moved.len()),
        "the leaver's {} sessions went to {:?}",
        moved.len(),
        spread(&moved)
    );
    fleet.stop().await;
}

/// A sixth instance joins a pool of 5 whose 200 sessions keep their
/// cutoffs. Then the pool stops taking work for a while: its `d` leaves,
/// so it is `unavailable`, which hides every entry instance, and the
/// follow may end. A new `d` makes it `ready` again, and the relay exposes
/// the same instances with the stamps they joined with: every session
/// returns to its instance, and none goes to the sixth, which it would
/// without its cutoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_return_when_their_instances_are_exposed_again() {
    let mut fleet = fleet(5).await;
    let sessions = keys(0, SESSIONS);
    let first = fleet.turns(&sessions).await;
    let joiner = fleet.join().await;
    let uncut = fleet.turns_cut(&sessions, false).await;
    assert!(uncut.contains(&joiner), "no session would have moved");

    fleet.sink.take().expect("the sink").stop().await;
    until(&fleet.tables(), "the pool's entries hidden", |pools| {
        followed(pools, "f", M1).is_none_or(|m| m.eligible.is_empty())
    })
    .await;
    let assemble = fleet.assemble;
    let sink = instance_of(&fleet.spec, &fleet.hubs[1], pool(1), "d", &|deployment| {
        assemble(deployment, &Events::quiet())
    })
    .await;
    fleet.sink = Some(sink);
    fleet.routes_to_its_entries().await;
    fleet.every_entry_serves().await;
    assert_eq!(fleet.turns(&sessions).await, first, "a session moved");
    fleet.stop().await;
}

/// Every instance of a pool of 5 is replaced, one at a time: a new one
/// joins, then an old one leaves. A session moves only when its own
/// instance leaves, and never on a join. Once every instance it was
/// ranked among has left, its cutoff moves up to the instances the pool
/// has then, so a later join moves it no more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolling_replacement_moves_a_session_only_when_its_instance_leaves() {
    let mut fleet = fleet(5).await;
    let sessions = keys(0, SESSIONS);
    let mut serving = fleet.turns(&sessions).await;
    let mut moves = 0;
    for _ in 0..5 {
        fleet.join().await;
        assert_eq!(
            fleet.turns(&sessions).await,
            serving,
            "a join moved a session"
        );
        let gone = fleet.leave(0).await;
        let then = fleet.turns(&sessions).await;
        for (session, (was, now)) in sessions.iter().zip(serving.iter().zip(&then)) {
            if was != now {
                assert_eq!(
                    *was, gone,
                    "session {session} moved off an instance that stayed"
                );
                moves += 1;
            }
        }
        serving = then;
    }
    assert!(
        moves >= SESSIONS,
        "{moves} moves: every session's first instance left"
    );
    fleet.join().await;
    assert_eq!(
        fleet.turns(&sessions).await,
        serving,
        "a join after the roll moved a session"
    );
    assert_eq!(
        fleet.pins.advanced.load(Ordering::SeqCst),
        SESSIONS,
        "each cutoff moved up once, after the last old instance left"
    );
    fleet.stop().await;
}
