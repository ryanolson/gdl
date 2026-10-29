//! An entry instance that declines an offer as its own
//! ([`Decline::Instance`]): the chooser offers the request to each other
//! entry instance of the pool once, before it leaves the pool for another.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use gdl::__runtime::Typed;
use gdl::{Cx, Error, ErrorKind, NoPool, Result};
use gdl_pools::PoolState;
use velo::Velo;

use super::super::{EntryTable, PoolTable, Pools, pool_id, targets};
use super::tests::{layout, layout_under};
use crate::dist::envelope::{Accepted, Forward, VERSION, WireCx, decode, encode};
use crate::dist::fabric::{Fabric, ReplyTo, bind_node};
use crate::dist::offers::{Decline, refusal};
use crate::dist::roster::{Policy, Roster, selector_id};
use crate::dist::runtime::{Router, Runtime, RuntimeConfig, Within};

/// Full refusals stay Overloaded while at least one offered pool is not
/// gone. Concurrent acceptances and gone refusals can move the mark between
/// pools, but the marks here never coexist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gone_marks_from_different_moments_do_not_turn_full_refusals_into_routing() {
    let fleet = Fleet::new(
        &[
            ("m1", &[Serves::None(Decline::Full)]),
            ("m2", &[Serves::None(Decline::Full)]),
        ],
        RuntimeConfig::default(),
    )
    .await;
    let router = fleet.router(
        "round_robin",
        &gdl::PoolPolicies::new(),
        Within::Policy(Policy::RoundRobin),
    );
    let control = fleet
        .send(&router, "model")
        .await
        .expect_err("both entries refuse");
    assert_eq!(
        control.kind(),
        ErrorKind::Overloaded,
        "the control: {control}"
    );
    assert_eq!(fleet.taken(), [vec![1], vec![1]]);
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let changing = fleet.pools.clone();
    let stopped = Arc::clone(&done);
    let changes = std::thread::spawn(move || {
        let table = changing.table("d").expect("followed");
        let a = &table.views[0];
        let b = &table.views[1];
        while !stopped.load(Ordering::Acquire) {
            changing.damping().accept(a.id());
            changing.damping().gone(b);
            changing.damping().accept(b.id());
            changing.damping().gone(a);
        }
    });
    // Repeated offers let the reads straddle mark changes. Marks read at
    // separate moments meet such an interleaving within about 2,000
    // requests; 4,000 take about 2 s.
    let mut routing = None;
    for turn in 0..4_000 {
        let error = fleet
            .send(&router, "model")
            .await
            .expect_err("both entries refuse");
        let offered = fleet.taken();
        if error.kind() == ErrorKind::Routing && offered.iter().all(|pool| pool == &[1]) {
            routing = Some((turn, offered, error));
            break;
        }
    }
    done.store(true, Ordering::Release);
    changes.join().expect("mark changes completed");
    assert!(
        routing.is_none(),
        "Routing while at most one pool was ever gone: {routing:?}"
    );
}

/// With no failover, a request whose one pool declines as gone fails with
/// `Routing` when no other pool may take it, as the next request does, and
/// with the refusal when another pool may.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_no_failover_a_lone_gone_pool_fails_with_routing() {
    let config = || RuntimeConfig {
        pool_failovers: 0,
        ..RuntimeConfig::default()
    };
    let fleet = Fleet::new(&[("m1", &[Serves::None(Decline::Gone)])], config()).await;
    let router = fleet.router(
        "round_robin",
        &gdl::PoolPolicies::new(),
        Within::Policy(Policy::RoundRobin),
    );
    let first = fleet.send(&router, "model").await.expect_err("gone");
    let next = fleet.send(&router, "model").await.expect_err("no pool");
    assert_eq!(fleet.taken(), [vec![1]], "a gone pool was offered again");
    assert_eq!(next.kind(), ErrorKind::Routing, "the control: {next}");
    assert_eq!(first.kind(), ErrorKind::Routing, "{first}");

    let fleet = Fleet::new(
        &[
            ("m1", &[Serves::None(Decline::Gone)]),
            ("m2", &[Serves::All]),
        ],
        config(),
    )
    .await;
    let router = fleet.router(
        "round_robin",
        &gdl::PoolPolicies::new(),
        Within::Policy(Policy::RoundRobin),
    );
    let error = fleet.send(&router, "model").await.expect_err("gone");
    assert_eq!(fleet.taken(), [vec![1], vec![0]]);
    assert_eq!(
        error.kind(),
        ErrorKind::Overloaded,
        "another pool may take it: {error}"
    );
    fleet.send(&router, "model").await.expect("m2 took it");
}

/// What an entry instance answers an offer.
#[derive(Clone, Copy)]
enum Serves {
    /// Takes every request.
    All,
    /// Declines every request, for this reason.
    None(Decline),
    /// Takes a request whose payload is this key, and declines any other
    /// as the instance's own: it serves one key of the model.
    Key(&'static str),
    /// Declines a request whose payload is this model as the instance's
    /// own, and takes any other: it does not serve that model.
    Not(&'static str),
}

/// A chooser in `global` and the entry instances of the `model_x` pools
/// it follows, each answering as it serves: the chooser's runtime, its
/// tables, and the offers each entry got, pool by pool, in the order of
/// the pool's entries.
struct Fleet {
    rt: Runtime,
    pools: Pools,
    offers: Vec<Vec<Arc<AtomicUsize>>>,
    _entries: Vec<Velo>,
}

impl Fleet {
    /// The fleet of `pools`, each a name and how each of its entry
    /// instances serves, with the chooser's runtime under `config`.
    async fn new(pools: &[(&str, &[Serves])], config: RuntimeConfig) -> Self {
        let chooser = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let mut roster = Roster::new();
        let mut entries = Vec::new();
        for (_, serves) in pools {
            for _ in serves.iter() {
                let entry = bind_node(([127, 0, 0, 1], 0).into())
                    .await
                    .expect("a velo node");
                roster.add("d", entry.peer_info());
                entries.push(entry);
            }
        }
        let rt = Runtime::new(
            Fabric::new(chooser, None, None, Duration::from_secs(30)),
            layout(),
            "a".into(),
            &roster,
            config,
            None,
        )
        .expect("a runtime");
        let tables = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
        );
        let mut offers = Vec::new();
        let mut each = entries.iter();
        let mut followed = Vec::new();
        for (name, serves) in pools {
            let mut counts = Vec::new();
            let mut ids = Vec::new();
            for serves in serves.iter().copied() {
                let entry = each.next().expect("an entry per instance");
                let count = Arc::new(AtomicUsize::new(0));
                counts.push(Arc::clone(&count));
                ids.push(entry.instance_id());
                Fabric::new(entry.clone(), None, None, Duration::from_secs(30))
                    .register(&rt.names().forward, move |message| {
                        count.fetch_add(1, Ordering::SeqCst);
                        let reply = answer(serves, &message);
                        Box::pin(async move { reply })
                    })
                    .expect("a handler");
            }
            offers.push(counts);
            followed.push(Arc::new(PoolTable {
                class: "model_x".into(),
                pool: (*name).into(),
                id: pool_id("model_x", name),
                generation: 1,
                region: "home".into(),
                state: PoolState::Ready,
                load: super::super::tests::idle(),
                eligible: ids.clone().into(),
                offered: ids.iter().copied().map(selector_id).collect(),
                retired: Box::new([]),
                unread: false,
                turn: Arc::new(AtomicUsize::new(0)),
                offering: true,
                revision: 1,
                changes: 0,
                leases: Arc::default(),
            }));
        }
        tables.store("d", EntryTable::new(followed.into()));
        Self {
            rt,
            pools: tables,
            offers,
            _entries: entries,
        }
    }

    /// The crossing into `model_x`, choosing the pool by `policy`, of
    /// those `policies` registers, and the instance within it as `within`
    /// says.
    fn router(&self, policy: &str, policies: &gdl::PoolPolicies, within: Within) -> Router {
        let layout = layout_under(gdl_model::PoolPolicy {
            policy: policy.into(),
        });
        let mut targets = targets(
            &layout,
            "a",
            &self.pools,
            policies,
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let target = targets.remove(&(0, 1)).expect("the class cut");
        Router::Pools(Arc::new(super::super::Crossing::new(
            target,
            within,
            Default::default(),
        )))
    }

    /// Send one request whose payload is `key` through `router`.
    async fn send(&self, router: &Router, key: &str) -> Result<()> {
        let cx = Cx::new();
        let envelope = Forward {
            version: VERSION,
            identity: self.rt.identity,
            hop: 0,
            pos: 1,
            cx: WireCx::of(&cx),
            reply: ReplyTo::test_value(7),
            payload: key.as_bytes().to_vec().into(),
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            self.rt.send_forward("d", router, &cx, envelope, false),
        )
        .await
        .expect("answered in time")
        .map(drop)
    }

    /// The offers each entry got, pool by pool, since the last call.
    fn taken(&self) -> Vec<Vec<usize>> {
        self.offers
            .iter()
            .map(|pool| {
                pool.iter()
                    .map(|count| count.swap(0, Ordering::SeqCst))
                    .collect()
            })
            .collect()
    }

    /// Whether the pool `name` is damped, or held gone.
    fn damped(&self, name: &str) -> (bool, bool) {
        let shown = self
            .pools
            .followed("d")
            .into_iter()
            .find(|pool| pool.pool == name)
            .expect("followed");
        (shown.damped.is_some(), shown.gone)
    }
}

/// What an entry that serves as `serves` answers `message`.
fn answer(serves: Serves, message: &[u8]) -> Vec<u8> {
    let accepted = || encode(&Ok::<Accepted, Error>(None)).expect("encode");
    let declined =
        |decline| refusal(&Error::new(ErrorKind::Overloaded, "declined"), decline).expect("encode");
    match serves {
        Serves::All => accepted(),
        Serves::None(decline) => declined(decline),
        Serves::Key(key) => {
            let forward: Forward = decode(message).expect("a forward hand-off");
            if forward.payload[..] == *key.as_bytes() {
                accepted()
            } else {
                declined(Decline::Instance)
            }
        }
        Serves::Not(model) => {
            let forward: Forward = decode(message).expect("a forward hand-off");
            if forward.payload[..] == *model.as_bytes() {
                declined(Decline::Instance)
            } else {
                accepted()
            }
        }
    }
}

/// Takes the first instance it is given.
struct First(Arc<AtomicUsize>);

impl gdl::Selector for First {
    type Head = gdl::Encoded;

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, gdl::Encoded>,
        instances: gdl::Instances<'a>,
    ) -> Result<gdl::Choice<'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(gdl::Choice::new(instances.get(0).expect("never empty")))
    }
}

/// How the instance within a pool is chosen: `round_robin`, or a
/// selector that takes the first instance it is given, with its count of
/// choices.
fn withins() -> [(Within, Option<Arc<AtomicUsize>>); 2] {
    let chosen = Arc::new(AtomicUsize::new(0));
    [
        (Within::Policy(Policy::RoundRobin), None),
        (
            Within::Selector {
                name: Arc::from("first"),
                selector: Arc::new(Typed(Arc::new(First(Arc::clone(&chosen))))),
                encoding: Default::default(),
            },
            Some(chosen),
        ),
    ]
}

/// A pool whose entry instances A and B each serve one key of the model:
/// a request for B's key is offered to A first, A declines it as its own,
/// and B serves it, in the same pool. The pool is not damped, and the
/// other pool gets nothing. Under `round_robin` the next request, for A's
/// key, is offered to B first; a selector that takes the first instance
/// offers it to A, which takes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_one_instance_cannot_serve_goes_to_another_of_its_pool() {
    for (within, chosen) in withins() {
        let fleet = Fleet::new(
            &[
                ("m1", &[Serves::Key("key-a"), Serves::Key("key-b")]),
                ("m2", &[Serves::All]),
            ],
            RuntimeConfig::default(),
        )
        .await;
        let selector = chosen.is_some();
        let router = fleet.router("round_robin", &gdl::PoolPolicies::new(), within);
        fleet.send(&router, "key-b").await.expect("B took it");
        assert_eq!(
            fleet.taken(),
            [vec![1, 1], vec![0]],
            "selector {selector}: A first, then B, and not m2"
        );
        assert_eq!(fleet.damped("m1"), (false, false), "selector {selector}");
        if let Some(chosen) = &chosen {
            assert_eq!(chosen.load(Ordering::SeqCst), 2, "a choice per offer");
        }
        // A new crossing, so that the pool policy's turn is m1's again.
        let router = fleet.router(
            "round_robin",
            &gdl::PoolPolicies::new(),
            withins_again(&chosen),
        );
        fleet.send(&router, "key-a").await.expect("A took it");
        let expected = if selector {
            [vec![1, 0], vec![0]]
        } else {
            [vec![1, 1], vec![0]]
        };
        assert_eq!(fleet.taken(), expected, "selector {selector}");
    }
}

/// The `within` of [`withins`] that `chosen` says it was, made again for a
/// new crossing.
fn withins_again(chosen: &Option<Arc<AtomicUsize>>) -> Within {
    match chosen {
        None => Within::Policy(Policy::RoundRobin),
        Some(chosen) => Within::Selector {
            name: Arc::from("first"),
            selector: Arc::new(Typed(Arc::new(First(Arc::clone(chosen))))),
            encoding: Default::default(),
        },
    }
}

/// When every entry instance of a pool declines as its own, each is
/// offered the request once, the pool is neither damped nor held gone,
/// and the next pool takes it. Those offers use none of
/// `pool_failovers`: with none, the pool's instances are each still
/// offered it, and the request then fails with the last refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_whose_every_instance_declines_is_left_for_another() {
    let declines = Serves::None(Decline::Instance);
    for (within, _) in withins() {
        let fleet = Fleet::new(
            &[("m1", &[declines; 3]), ("m2", &[Serves::All])],
            RuntimeConfig::default(),
        )
        .await;
        let router = fleet.router("round_robin", &gdl::PoolPolicies::new(), within);
        fleet.send(&router, "key").await.expect("m2 took it");
        assert_eq!(fleet.taken(), [vec![1, 1, 1], vec![1]]);
        assert_eq!(fleet.damped("m1"), (false, false));
    }
    for (within, _) in withins() {
        let fleet = Fleet::new(
            &[("m1", &[declines; 3]), ("m2", &[Serves::All])],
            RuntimeConfig {
                pool_failovers: 0,
                ..RuntimeConfig::default()
            },
        )
        .await;
        let router = fleet.router("round_robin", &gdl::PoolPolicies::new(), within);
        let error = fleet.send(&router, "key").await.expect_err("no pool left");
        assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
        assert_eq!(fleet.taken(), [vec![1, 1, 1], vec![0]]);
    }
}

/// A set that serves two models, where every entry instance declines one
/// of them as its own, is left only by the requests for that model: an
/// `acme-legal` request is offered to each of its instances once, then
/// taken by the other set, and the set is neither damped nor held gone,
/// so the `llama-70b` requests right after still reach it, in turn with
/// the other set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_model_one_set_cannot_serve_leaves_its_other_models_there() {
    let without = Serves::Not("acme-legal");
    for (within, _) in withins() {
        let fleet = Fleet::new(
            &[("m1", &[without; 2]), ("m2", &[Serves::All])],
            RuntimeConfig::default(),
        )
        .await;
        let router = fleet.router("round_robin", &gdl::PoolPolicies::new(), within);
        fleet.send(&router, "acme-legal").await.expect("m2 took it");
        assert_eq!(fleet.taken(), [vec![1, 1], vec![1]]);
        assert_eq!(fleet.damped("m1"), (false, false));
        for _ in 0..4 {
            fleet.send(&router, "llama-70b").await.expect("taken");
        }
        let taken = fleet.taken();
        assert_eq!(
            (taken[0].iter().sum::<usize>(), taken[1][0]),
            (2, 2),
            "the set that cannot serve acme-legal lost its llama-70b requests: {taken:?}"
        );
        assert_eq!(fleet.damped("m1"), (false, false));
    }
}

/// A full or a gone decline from one entry instance is about its pool, as
/// before: the chooser damps the pool or holds it gone, and offers the
/// request to another pool, never to the pool's other instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_or_gone_instance_still_leaves_its_pool() {
    for (decline, damped) in [
        (Decline::Full, (true, false)),
        (Decline::Gone, (false, true)),
    ] {
        for (within, _) in withins() {
            let fleet = Fleet::new(
                &[
                    ("m1", &[Serves::None(decline), Serves::All]),
                    ("m2", &[Serves::All]),
                ],
                RuntimeConfig::default(),
            )
            .await;
            let router = fleet.router("round_robin", &gdl::PoolPolicies::new(), within);
            fleet.send(&router, "key").await.expect("m2 took it");
            assert_eq!(fleet.taken(), [vec![1, 0], vec![1]], "{decline:?}");
            assert_eq!(fleet.damped("m1"), damped, "{decline:?}");
        }
    }
}

/// A pool policy's lease that counts its acceptances and its drops.
struct Counting {
    accepted: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl gdl::ChoiceLease for Counting {
    fn accepted(&mut self) {
        self.accepted.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for Counting {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Chooses by round robin, with a [`Counting`] lease on each choice, and
/// counts its choices.
#[derive(Clone, Default)]
struct Leasing {
    chosen: Arc<AtomicUsize>,
    accepted: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl gdl::PoolPolicy for Leasing {
    type Head = gdl::Encoded;

    fn choose(
        &self,
        request: gdl::PoolRequest<'_, gdl::Encoded>,
        pools: &[gdl::Pool],
    ) -> std::result::Result<gdl::PoolChoice, NoPool> {
        let at = gdl::RoundRobin
            .choose_where(request, pools, |_| true)
            .ok_or_else(NoPool::new)?;
        self.chosen.fetch_add(1, Ordering::SeqCst);
        Ok(gdl::PoolChoice::new(at).with_lease(Counting {
            accepted: Arc::clone(&self.accepted),
            dropped: Arc::clone(&self.dropped),
        }))
    }
}

/// An instance's decline is not its pool's: the pool policy is not asked
/// again, and its lease on the choice learns that the pool took the
/// request when another of its instances did. When every instance of the
/// pool declines, the policy's lease on it ends untold, and the next
/// pool's choice has its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pool_policys_lease_learns_that_another_instance_took_it() {
    for (within, _) in withins() {
        let fleet = Fleet::new(
            &[
                ("m1", &[Serves::None(Decline::Instance), Serves::All]),
                ("m2", &[Serves::All]),
            ],
            RuntimeConfig::default(),
        )
        .await;
        let leasing = Leasing::default();
        let mut policies = gdl::PoolPolicies::new();
        policies
            .add("leasing", leasing.clone())
            .expect("registered");
        let router = fleet.router("leasing", &policies, within);
        fleet.send(&router, "key").await.expect("B took it");
        assert_eq!(fleet.taken(), [vec![1, 1], vec![0]]);
        let counts = |leasing: &Leasing| {
            [&leasing.chosen, &leasing.accepted, &leasing.dropped]
                .map(|count| count.load(Ordering::SeqCst))
        };
        // The request's response was dropped with `send`'s result.
        assert_eq!(counts(&leasing), [1, 1, 1], "chosen, accepted, dropped");
    }
    for (within, _) in withins() {
        let fleet = Fleet::new(
            &[
                ("m1", &[Serves::None(Decline::Instance); 2]),
                ("m2", &[Serves::All]),
            ],
            RuntimeConfig::default(),
        )
        .await;
        let leasing = Leasing::default();
        let mut policies = gdl::PoolPolicies::new();
        policies
            .add("leasing", leasing.clone())
            .expect("registered");
        let router = fleet.router("leasing", &policies, within);
        fleet.send(&router, "key").await.expect("m2 took it");
        assert_eq!(fleet.taken(), [vec![1, 1], vec![1]]);
        let counts = [&leasing.chosen, &leasing.accepted, &leasing.dropped]
            .map(|count| count.load(Ordering::SeqCst));
        assert_eq!(counts, [2, 1, 2], "chosen, accepted, dropped");
    }
}
