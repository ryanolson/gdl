//! A pool policy that re-queues a request for the pool that declined it
//! as full ([`gdl::PoolPolicy::requeue`]): the chooser offers the request
//! to that pool again, with no failover counted and no damping, and the
//! wait between the offers allocates nothing beyond its timer. A pool
//! that declines over its budget is never asked about: the request goes
//! to another pool at once, and past every pool's budget to one forced
//! placement with no view.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use gdl::{
    ChoiceLease, Cx, Decline, Encoded, Error, ErrorKind, NoPool, Pool, PoolChoice, PoolPolicy,
    PoolRequest,
};
use gdl_pools::PoolState;
use prometheus::Registry;
use velo::Velo;

use super::super::lease::Leases;
use super::super::meter::tests::{offers, scraped};
use super::super::meter::{BUDGET_FORCED, FAILOVERS, PoolMeters, REQUEUE_DEADLINES, REQUEUES};
use super::super::{EntryTable, PoolTable, Pools, pool_id, targets};
use super::tests::layout_under;
use crate::dist::counting::Counted;
use crate::dist::envelope::{Accepted, Forward, VERSION, WireCx, encode};
use crate::dist::fabric::{Fabric, ReplyTo, bind_node};
use crate::dist::offers::refusal;
use crate::dist::roster::{Policy, Roster, selector_id};
use crate::dist::runtime::{Router, Runtime, RuntimeConfig, Within};

/// Chooses the first pool, by name, that the request has not left, or,
/// if `stubborn`, the first pool, whether the request left it or not;
/// re-queues the request for a pool that declined it as full, if
/// `requeue`, and would for one over its budget, were it asked. Gives a lease with each choice when `told` is set, which
/// counts each acceptance it is told of. Records why the first pool
/// declined the request, as each choice reads it, in `seen`.
#[derive(Default)]
struct Keeping {
    requeue: bool,
    /// Whether its choices put the view on their offers.
    view: bool,
    stubborn: bool,
    told: Option<Arc<AtomicUsize>>,
    seen: Arc<std::sync::Mutex<Vec<Option<Decline>>>>,
}

impl PoolPolicy for Keeping {
    type Head = Encoded;

    fn choose(
        &self,
        request: PoolRequest<'_, Encoded>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        self.seen
            .lock()
            .expect("not poisoned")
            .push(request.declined(&pools[0]));
        let open = |pool: &Pool| self.stubborn || !request.is_excluded(pool);
        let at = pools
            .iter()
            .position(|pool| pool.takes_work() && open(pool))
            .ok_or_else(NoPool::new)?;
        let choice = PoolChoice::new(at);
        let choice = if self.view {
            choice.with_view()
        } else {
            choice
        };
        Ok(match &self.told {
            Some(told) => choice.with_lease(Told(Arc::clone(told))),
            None => choice,
        })
    }

    fn requeue(&self, request: PoolRequest<'_, Encoded>, pool: &Pool) -> bool {
        self.requeue
            && matches!(
                request.declined(pool),
                Some(Decline::Full | Decline::Budget)
            )
    }
}

/// Counts the acceptances it is told of.
struct Told(Arc<AtomicUsize>);

impl ChoiceLease for Told {
    fn accepted(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A chooser and the one entry instance of each of the pools `m1`, `m2`
/// and `m3`. Each entry declines as full as many offers as its count of
/// declines left, then takes every offer, or declines it as not present
/// once it is set to.
struct Fleet {
    rt: Runtime,
    pools: Pools,
    registry: Registry,
    entries: Vec<Entry>,
    _nodes: Vec<Velo>,
}

/// How one pool's entry answers, and the offers it got.
struct Entry {
    /// The offers it declines as full before any other answer.
    full: Arc<AtomicUsize>,
    /// Whether it declines the offers after those as not present.
    absent: Arc<AtomicBool>,
    /// Whether its declines in `full` say over its budget, not full.
    budget: Arc<AtomicBool>,
    /// When above 0, it declines over its budget every offer whose view is
    /// of an earlier summary than this, as a pool's gate does.
    newer_than: Arc<std::sync::atomic::AtomicU64>,
    /// Whether it declines those as full, not over its budget, so that a
    /// policy re-queues the request.
    stale_full: Arc<AtomicBool>,
    /// Whether it takes an offer with no view all the same, as a gate takes
    /// a pinned turn, or a placement forced past every pool's budget.
    takes_pinned: Arc<AtomicBool>,
    offered: Arc<AtomicUsize>,
    /// The keys of the meta each offer carried, in order.
    metas: Arc<Mutex<Vec<Vec<String>>>>,
}

const POOLS: [&str; 3] = ["m1", "m2", "m3"];

impl Fleet {
    async fn new(config: RuntimeConfig) -> Self {
        let chooser = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let mut roster = Roster::new();
        let mut nodes = Vec::new();
        for _ in POOLS {
            let node = bind_node(([127, 0, 0, 1], 0).into())
                .await
                .expect("a velo node");
            roster.add("d", node.peer_info());
            nodes.push(node);
        }
        let rt = Runtime::new(
            Fabric::new(chooser, None, None, Duration::from_secs(30)),
            layout_under(gdl_model::PoolPolicy {
                policy: "keeping".into(),
                provides: Vec::new(),
            }),
            "a".into(),
            &roster,
            config,
            None,
        )
        .expect("a runtime");
        let refused = Error::new(ErrorKind::Overloaded, "refused");
        let full = refusal(&refused, Decline::Full).expect("encode");
        let not_present = refusal(&refused, Decline::NotPresent).expect("encode");
        let over_budget = refusal(&refused, Decline::Budget).expect("encode");
        let accepted = encode(&Ok::<Accepted, Error>(None)).expect("encode");
        let registry = Registry::new();
        let meters = PoolMeters::register(&registry).expect("registered");
        let mut entries = Vec::new();
        let mut tables = Vec::new();
        for (node, name) in nodes.iter().zip(POOLS) {
            let entry = Entry {
                full: Arc::new(AtomicUsize::new(0)),
                absent: Arc::new(AtomicBool::new(false)),
                budget: Arc::new(AtomicBool::new(false)),
                newer_than: Arc::default(),
                stale_full: Arc::default(),
                takes_pinned: Arc::default(),
                offered: Arc::new(AtomicUsize::new(0)),
                metas: Arc::default(),
            };
            let (left, absent, budget, newer_than, stale_full, takes_pinned, offered, metas) = (
                Arc::clone(&entry.full),
                Arc::clone(&entry.absent),
                Arc::clone(&entry.budget),
                Arc::clone(&entry.newer_than),
                Arc::clone(&entry.stale_full),
                Arc::clone(&entry.takes_pinned),
                Arc::clone(&entry.offered),
                Arc::clone(&entry.metas),
            );
            entries.push(entry);
            let (replies_budget, replies_full) = (over_budget.clone(), full.clone());
            let replies = [
                full.clone(),
                not_present.clone(),
                accepted.clone(),
                over_budget.clone(),
            ];
            Fabric::new(node.clone(), None, None, Duration::from_secs(30))
                .register(&rt.names().forward, move |message| {
                    offered.fetch_add(1, Ordering::SeqCst);
                    let forward = Forward::read(&message).expect("a forward hand-off");
                    let stale = newer_than.load(Ordering::SeqCst);
                    let view = forward.payload.view();
                    let pinned = view.is_none() && takes_pinned.load(Ordering::SeqCst);
                    if stale > 0 && !pinned && view.is_none_or(|view| view.sequence < stale) {
                        let reply = if stale_full.load(Ordering::SeqCst) {
                            replies_full.clone()
                        } else {
                            replies_budget.clone()
                        };
                        return Box::pin(async move { reply });
                    }
                    // An offer that arrives past its deadline is answered
                    // with the request's own error, as an entry does.
                    let cx = match forward
                        .cx
                        .arrive(Duration::ZERO, gdl::__runtime::wall_now_nanos)
                    {
                        Ok(cx) => cx,
                        Err(error) => {
                            let reply = encode(&Err::<Accepted, Error>(error)).expect("encode");
                            return Box::pin(async move { reply });
                        }
                    };
                    metas
                        .lock()
                        .expect("not poisoned")
                        .push(cx.meta().keys().cloned().collect());
                    let full = left
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                    let reply = if full && budget.load(Ordering::SeqCst) {
                        replies[3].clone()
                    } else if full {
                        replies[0].clone()
                    } else if absent.load(Ordering::SeqCst) {
                        replies[1].clone()
                    } else {
                        replies[2].clone()
                    };
                    Box::pin(async move { reply })
                })
                .expect("a handler");
            let id = node.instance_id();
            tables.push(Arc::new(PoolTable {
                class: "model_x".into(),
                pool: name.into(),
                id: pool_id("model_x", name),
                generation: 1,
                epoch: 0,
                region: "home".into(),
                state: PoolState::Ready,
                load: super::super::tests::idle(),
                eligible: Box::new([id]),
                offered: Box::new([selector_id(id)]),
                retired: Box::new([]),
                unread: false,
                turn: Arc::new(AtomicUsize::new(0)),
                offering: true,
                revision: 1,
                changes: 0,
                leases: Arc::new(Leases::counted(Some(
                    meters.figures("model_x", name, "home"),
                ))),
                properties_version: 0,
            }));
        }
        let pools = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
            gdl::PropertyKinds::new(),
        );
        pools.store("d", EntryTable::new(tables.into()));
        Self {
            rt,
            pools,
            registry,
            entries,
            _nodes: nodes,
        }
    }

    /// The crossing into `model_x`, choosing its pool by `policy`.
    fn router(&self, policy: Keeping) -> Router {
        self.router_within(policy, Within::Policy(Policy::RoundRobin))
    }

    /// [`Fleet::router`], choosing the instance within the pool as
    /// `within` says.
    fn router_within(&self, policy: Keeping, within: Within) -> Router {
        let mut policies = gdl::PoolPolicies::new();
        policies.add("keeping", policy).expect("registered");
        let layout = layout_under(gdl_model::PoolPolicy {
            policy: "keeping".into(),
            provides: Vec::new(),
        });
        let mut targets = targets(
            &layout,
            "a",
            &self.pools,
            &policies,
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        Router::Pools(Arc::new(super::Crossing::new(
            target,
            within,
            Default::default(),
        )))
    }

    /// Have the entries of the pools decline, as full, as many offers as
    /// `declines` gives each, in pool order.
    fn decline(&self, declines: [usize; 3]) {
        for (entry, count) in self.entries.iter().zip(declines) {
            entry.full.store(count, Ordering::SeqCst);
        }
    }

    /// The offers each pool's entry got.
    fn offered(&self) -> [usize; 3] {
        let mut counts = [0; 3];
        for (count, entry) in counts.iter_mut().zip(&self.entries) {
            *count = entry.offered.load(Ordering::SeqCst);
        }
        counts
    }

    /// Send one request through `router` with `cx`, and the allocations
    /// its sending future made on the polling thread.
    async fn send(&self, router: &Router, cx: &Cx) -> (gdl::Result<()>, u64) {
        let envelope = Forward {
            version: VERSION,
            identity: self.rt.identity,
            hop: 0,
            pos: 1,
            cx: WireCx::of(cx),
            reply: ReplyTo::test_value(7),
            payload: vec![1, 2, 3].into(),
        };
        let send = Counted::new(self.rt.send_forward("d", router, cx, envelope, false));
        let (sent, made) = tokio::time::timeout(Duration::from_secs(5), send)
            .await
            .expect("answered in time");
        (sent.map(drop), made)
    }

    /// The fewest allocations over a few sends of a request that the
    /// entries answer as `declines` says, after a first that meets the
    /// peers.
    async fn fewest(&self, router: &Router, declines: [usize; 3]) -> u64 {
        let mut fewest = u64::MAX;
        for round in 0..31 {
            self.decline(declines);
            let (sent, made) = self.send(router, &Cx::new()).await;
            sent.expect("a pool took it");
            if round > 0 {
                fewest = fewest.min(made);
            }
        }
        fewest
    }

    /// Store the table again with each pool's summary numbered
    /// `sequence`, of hub epoch 1.
    fn number(&self, sequence: u64) {
        self.store_numbered(sequence, true);
    }

    /// [`Fleet::number`], with `m1` taking work or, if not `m1_works`,
    /// with no entry instance to route to.
    fn store_numbered(&self, sequence: u64, m1_works: bool) {
        let table = self.pools.table("d").expect("the table");
        let pools: Vec<Arc<PoolTable>> = table
            .pools
            .iter()
            .map(|shown| {
                let load = shown.load.map(|load| {
                    let summary = gdl::LoadSummary {
                        sequence,
                        ..*load.summary()
                    };
                    gdl::Load::new(summary, load.arrived())
                });
                Arc::new(PoolTable {
                    class: shown.class.clone(),
                    pool: shown.pool.clone(),
                    id: shown.id,
                    generation: shown.generation,
                    epoch: 1,
                    region: shown.region.clone(),
                    state: shown.state,
                    load,
                    eligible: if m1_works || shown.pool != POOLS[0] {
                        shown.eligible.clone()
                    } else {
                        Box::new([])
                    },
                    offered: if m1_works || shown.pool != POOLS[0] {
                        shown.offered.clone()
                    } else {
                        Box::new([])
                    },
                    retired: shown.retired.clone(),
                    unread: shown.unread,
                    turn: Arc::clone(&shown.turn),
                    offering: shown.offering,
                    revision: shown.revision,
                    changes: shown.changes,
                    leases: Arc::clone(&shown.leases),
                    properties_version: shown.properties_version,
                })
            })
            .collect();
        drop(table);
        self.pools.store("d", EntryTable::new(pools.into()));
    }

    /// The requests re-queued for `m1`, and those of them that ended at
    /// their deadline.
    fn requeues(&self) -> (f64, f64) {
        let labels = [("class", "model_x"), ("pool", POOLS[0]), ("region", "home")];
        let count = |family| scraped(&self.registry, family, &labels).expect("a series");
        (count(REQUEUES), count(REQUEUE_DEADLINES))
    }

    /// The placements forced past every pool's budget, under each pool.
    fn forced(&self) -> Vec<f64> {
        POOLS
            .iter()
            .map(|pool| {
                let labels = [("class", "model_x"), ("pool", *pool)];
                scraped(&self.registry, BUDGET_FORCED, &labels).expect("a series")
            })
            .collect()
    }

    /// The failovers counted under each pool.
    fn failovers(&self) -> Vec<f64> {
        POOLS
            .iter()
            .map(|pool| {
                let labels = [("class", "model_x"), ("pool", *pool)];
                scraped(&self.registry, FAILOVERS, &labels).expect("a series")
            })
            .collect()
    }
}

/// A short backoff, so that a wait with no offer ending meanwhile is
/// short.
fn backoff() -> RuntimeConfig {
    RuntimeConfig {
        pool_requeue_backoff: Duration::from_millis(1),
        ..RuntimeConfig::default()
    }
}

/// A request re-queued for the pool that declined it as full is offered
/// to that pool again until it takes it: no other pool is offered it, no
/// failover is counted, the pool is not damped, and the policy's lease is
/// told of the acceptance. The control: a policy that does not re-queue
/// offers the request to the next pool, counts a failover, and damps the
/// pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_queued_request_is_offered_to_its_pool_again_with_no_failover() {
    let fleet = Fleet::new(backoff()).await;
    let told = Arc::new(AtomicUsize::new(0));
    let router = fleet.router(Keeping {
        requeue: true,
        told: Some(Arc::clone(&told)),
        ..Keeping::default()
    });
    fleet.decline([3, 0, 0]);
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    sent.expect("m1 took it");
    assert_eq!(fleet.offered(), [4, 0, 0]);
    assert_eq!(fleet.failovers(), [0.0, 0.0, 0.0]);
    assert_eq!(offers(&fleet.registry, "m1", "declined_full"), Some(3.0));
    assert_eq!(offers(&fleet.registry, "m1", "accepted"), Some(1.0));
    assert_eq!(told.load(Ordering::SeqCst), 1, "the lease was not told");
    let m1 = &fleet.pools.followed("d")[0];
    assert!(m1.damped.is_none(), "{m1:?}");

    let router = fleet.router(Keeping::default());
    fleet.decline([1, 0, 0]);
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    sent.expect("m2 took it");
    assert_eq!(fleet.offered(), [5, 1, 0], "the control");
    assert_eq!(fleet.failovers(), [0.0, 1.0, 0.0]);
    let m1 = &fleet.pools.followed("d")[0];
    assert!(m1.damped.is_some(), "{m1:?}");
}

/// A pool that declines over its budget is not asked about, even by a
/// policy that re-queues: the request goes to the next pool at once, as
/// after a property not present, with a failover counted and the reason
/// shown to the policy. The pool is not damped, so the next request is
/// offered to it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_budget_decline_sends_the_request_to_another_pool_at_once() {
    let fleet = Fleet::new(backoff()).await;
    fleet.entries[0].budget.store(true, Ordering::SeqCst);
    let seen = Arc::default();
    let router = fleet.router(Keeping {
        requeue: true,
        seen: Arc::clone(&seen),
        ..Keeping::default()
    });
    fleet.decline([1, 0, 0]);
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    sent.expect("m2 took it");
    assert_eq!(fleet.offered(), [1, 1, 0]);
    assert_eq!(fleet.failovers(), [0.0, 1.0, 0.0]);
    assert_eq!(offers(&fleet.registry, "m1", "declined_budget"), Some(1.0));
    assert_eq!(fleet.requeues(), (0.0, 0.0), "not re-queued");
    assert_eq!(fleet.forced(), [0.0, 0.0, 0.0], "not forced");
    assert_eq!(
        *seen.lock().expect("not poisoned"),
        [None, Some(Decline::Budget)],
        "the policy read the reason"
    );
    let m1 = &fleet.pools.followed("d")[0];
    assert!(m1.damped.is_none(), "{m1:?}");
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    sent.expect("m1 took it");
    assert_eq!(fleet.offered(), [2, 1, 0], "offered m1 again at once");
}

/// A fleet whose every pool its policy may choose declines a request
/// over its budget: the request is offered once more, with no view, to
/// the pool the policy chooses with those declines cleared, which takes
/// it. The forced placement counts under that pool, and nothing waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_pool_over_its_budget_gets_one_forced_offer_with_no_view() {
    let fleet = Fleet::new(backoff()).await;
    fleet.number(1);
    for entry in &fleet.entries {
        entry.newer_than.store(3, Ordering::SeqCst);
        entry.takes_pinned.store(true, Ordering::SeqCst);
    }
    let router = fleet.router(Keeping {
        requeue: true,
        view: true,
        ..Keeping::default()
    });
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    sent.expect("m1 took the forced offer");
    assert_eq!(fleet.offered(), [2, 1, 1]);
    assert_eq!(fleet.forced(), [1.0, 0.0, 0.0]);
    for pool in POOLS {
        assert_eq!(
            offers(&fleet.registry, pool, "declined_budget"),
            Some(1.0),
            "{pool}"
        );
    }
    assert_eq!(fleet.requeues(), (0.0, 0.0), "not re-queued");
}

/// The forced offer clears only the budget's declines: a pool that
/// declined for another reason stays left, and the policy chooses among
/// the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forced_offer_keeps_the_other_declines() {
    let fleet = Fleet::new(backoff()).await;
    fleet.number(1);
    fleet.entries[0].absent.store(true, Ordering::SeqCst);
    for entry in &fleet.entries[1..] {
        entry.newer_than.store(3, Ordering::SeqCst);
        entry.takes_pinned.store(true, Ordering::SeqCst);
    }
    let router = fleet.router(Keeping {
        view: true,
        ..Keeping::default()
    });
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    sent.expect("m2 took the forced offer");
    assert_eq!(fleet.offered(), [1, 2, 1], "m1, not present, stays left");
    assert_eq!(fleet.forced(), [0.0, 1.0, 0.0]);
}

/// A forced offer that its pool declines as full takes the ordinary path:
/// a policy that re-queues keeps the request with the pool, whose
/// re-offers carry no view either, so that no budget declines them. A
/// policy that does not re-queue leaves the pool, and the request fails
/// with no second forced offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forced_offer_declined_as_full_takes_the_ordinary_path() {
    for requeue in [true, false] {
        let fleet = Fleet::new(backoff()).await;
        fleet.number(1);
        for entry in &fleet.entries {
            entry.newer_than.store(3, Ordering::SeqCst);
            entry.takes_pinned.store(true, Ordering::SeqCst);
        }
        fleet.decline([1, 0, 0]);
        let router = fleet.router(Keeping {
            requeue,
            view: true,
            ..Keeping::default()
        });
        let (sent, _) = fleet.send(&router, &Cx::new()).await;
        assert_eq!(fleet.forced(), [1.0, 0.0, 0.0], "requeue: {requeue}");
        if requeue {
            sent.expect("m1 took the re-offer");
            assert_eq!(fleet.offered(), [3, 1, 1]);
            assert_eq!(fleet.requeues().0, 1.0);
        } else {
            let error = sent.expect_err("no pool took it");
            assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
            assert_eq!(fleet.offered(), [2, 1, 1], "one forced offer");
            let m1 = &fleet.pools.followed("d")[0];
            assert!(m1.damped.is_some(), "a full decline damps: {m1:?}");
        }
    }
}

/// A re-queued request is offered again with the view of the pool's row
/// as the re-offer's table shows it: once that table shows a newer
/// summary, the pool takes it, before its deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_queued_request_carries_its_pools_newer_view() {
    let fleet = Fleet::new(backoff()).await;
    fleet.number(1);
    fleet.entries[0].newer_than.store(2, Ordering::SeqCst);
    fleet.entries[0].stale_full.store(true, Ordering::SeqCst);
    let router = fleet.router(Keeping {
        requeue: true,
        view: true,
        ..Keeping::default()
    });
    let cx = Cx::new().with_timeout(Duration::from_secs(4));
    let sending = fleet.send(&router, &cx);
    tokio::pin!(sending);
    // Declined at summary 1, again and again: the same view.
    let waited = tokio::time::timeout(Duration::from_millis(200), &mut sending).await;
    assert!(waited.is_err(), "taken before a newer summary");
    assert!(fleet.offered()[0] > 1, "re-queued: {:?}", fleet.offered());
    assert_eq!(fleet.offered()[1..], [0, 0]);
    fleet.number(2);
    let (sent, _) = tokio::time::timeout(Duration::from_secs(3), sending)
        .await
        .expect("taken in time");
    sent.expect("m1 took it at summary 2");
    assert_eq!(fleet.offered()[1..], [0, 0], "no other pool");
}

/// A choice that asked for the view of a pool whose row had no numbered
/// summary carries none; its re-offers still take the row's view once one
/// arrives, so the pool takes it then.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_queued_request_takes_a_view_its_first_offer_could_not_carry() {
    let fleet = Fleet::new(backoff()).await;
    fleet.number(0);
    fleet.entries[0].newer_than.store(2, Ordering::SeqCst);
    fleet.entries[0].stale_full.store(true, Ordering::SeqCst);
    let router = fleet.router(Keeping {
        requeue: true,
        view: true,
        ..Keeping::default()
    });
    let cx = Cx::new().with_timeout(Duration::from_secs(4));
    let sending = fleet.send(&router, &cx);
    tokio::pin!(sending);
    let waited = tokio::time::timeout(Duration::from_millis(200), &mut sending).await;
    assert!(waited.is_err(), "taken with no view");
    fleet.number(2);
    let (sent, _) = tokio::time::timeout(Duration::from_secs(3), sending)
        .await
        .expect("taken in time");
    sent.expect("m1 took it at summary 2");
    assert_eq!(fleet.offered()[1..], [0, 0], "no other pool");
}

/// A pool that never takes the re-queued request keeps it until its
/// deadline, which fails it with `DeadlineExceeded`; no other pool is
/// offered it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_queued_request_fails_at_its_deadline() {
    let fleet = Fleet::new(backoff()).await;
    let router = fleet.router(Keeping {
        requeue: true,
        ..Keeping::default()
    });
    fleet.decline([usize::MAX, 0, 0]);
    let cx = Cx::new().with_timeout(Duration::from_millis(200));
    let (sent, _) = fleet.send(&router, &cx).await;
    let error = sent.expect_err("m1 never took it");
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");
    let [m1, m2, m3] = fleet.offered();
    assert!(m1 > 1 && m2 == 0 && m3 == 0, "{:?}", fleet.offered());
    assert_eq!(fleet.failovers(), [0.0, 0.0, 0.0]);
    assert_eq!(fleet.requeues(), (1.0, 1.0));
}

/// A hop bounds its hand-off by the request's deadline, as `bounded` in
/// the hop does: once the deadline passes, the bound answers first, and
/// the hand-off's future is dropped wherever it is, in a wait or with an
/// offer in flight. The re-queued request still counts as ended at its
/// deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_queue_dropped_by_the_hops_bound_counts_its_deadline() {
    let fleet = Fleet::new(backoff()).await;
    let router = fleet.router(Keeping {
        requeue: true,
        ..Keeping::default()
    });
    fleet.decline([usize::MAX, 0, 0]);
    let cx = Cx::new().with_timeout(Duration::from_millis(200));
    let sending = fleet.send(&router, &cx);
    let error = tokio::select! {
        biased;
        reason = cx.done() => reason,
        (sent, _) = sending => sent.expect_err("m1 never took it"),
    };
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");
    assert_eq!(fleet.requeues(), (1.0, 1.0));
}

/// What a re-queued request allocates, against today's failover. Each
/// round of a re-queue, a wait and one more offer to the same pool, costs
/// what one more offer of a failover costs, and its timer: the wait
/// itself allocates nothing else. Here every wait ends on its backoff, as
/// no offer to the pool ends meanwhile. A request re-queued once costs no
/// more than one that fails over once, and each further round adds the
/// same count.
///
/// Each figure is the fewest of 30 sends. Run beside the other offer
/// tests on 8 threads, one figure came out one allocation over its
/// fewest in 1 run of 200. So each figure keeps its fewest over up to
/// three attempts, and the test stops at the first attempt whose figures
/// hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wait_allocates_nothing_beyond_its_timer() {
    let fleet = Fleet::new(RuntimeConfig {
        pool_failovers: 2,
        ..backoff()
    })
    .await;
    let requeue = fleet.router(Keeping {
        requeue: true,
        ..Keeping::default()
    });
    let failover = fleet.router(Keeping::default());
    let mut timer = u64::MAX;
    for _ in 0..5 {
        let sleep = tokio::time::sleep(Duration::from_millis(1));
        timer = timer.min(Counted::new(sleep).await.1);
    }
    let (mut first, mut failed_over, mut requeued) = (u64::MAX, [u64::MAX; 2], [u64::MAX; 3]);
    let holds = |failed_over: [u64; 2], requeued: [u64; 3]| {
        let round = requeued[1] - requeued[0];
        requeued[2] - requeued[1] == 2 * round
            && round == failed_over[1] - failed_over[0] + timer
            && requeued[0] <= failed_over[0]
    };
    for _ in 0..3 {
        first = first.min(fleet.fewest(&failover, [0, 0, 0]).await);
        for (fewest, declines) in failed_over.iter_mut().zip([[1, 0, 0], [1, 1, 0]]) {
            *fewest = (*fewest).min(fleet.fewest(&failover, declines).await);
        }
        for (fewest, declines) in requeued.iter_mut().zip([[1, 0, 0], [2, 0, 0], [4, 0, 0]]) {
            *fewest = (*fewest).min(fleet.fewest(&requeue, declines).await);
        }
        if holds(failed_over, requeued) {
            break;
        }
    }
    println!(
        "allocations: a timer {timer}; accepted first offer {first}; failover once {}, \
         twice {}; re-queued once {}, twice {}, four times {}",
        failed_over[0], failed_over[1], requeued[0], requeued[1], requeued[2]
    );
    let round = requeued[1] - requeued[0];
    assert_eq!(requeued[2] - requeued[1], 2 * round, "{requeued:?}");
    assert_eq!(
        round,
        failed_over[1] - failed_over[0] + timer,
        "a round's cost against a failover's offer and a timer"
    );
    assert!(
        requeued[0] <= failed_over[0],
        "re-queued once {} > failed over once {}",
        requeued[0],
        failed_over[0]
    );
}

/// A pool that re-queued a request, then declines it as not present,
/// sends it on once, as a set router moves a session whose pool dropped
/// its model: the next pool takes it, with one failover counted, and the
/// policy's next choice reads the pool's last reason, not present.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_queued_request_that_its_pool_declines_as_not_present_moves_once() {
    let fleet = Fleet::new(backoff()).await;
    let seen = Arc::default();
    let router = fleet.router(Keeping {
        requeue: true,
        seen: Arc::clone(&seen),
        ..Keeping::default()
    });
    fleet.decline([2, 0, 0]);
    fleet.entries[0].absent.store(true, Ordering::SeqCst);
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    sent.expect("m2 took it");
    assert_eq!(fleet.offered(), [3, 1, 0]);
    assert_eq!(fleet.failovers(), [0.0, 1.0, 0.0]);
    assert_eq!(
        *seen.lock().expect("not poisoned"),
        [None, Some(Decline::NotPresent)],
        "the choices' reads of m1's decline"
    );
}

/// A policy that chooses a pool it left again is bounded by
/// `pool_failovers`, re-queue or not: it is asked to re-queue the request
/// for the pool once, and each later decline counts as a failover.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_policy_that_chooses_its_pool_again_is_bounded_by_the_failovers() {
    let fleet = Fleet::new(backoff()).await;
    let router = fleet.router(Keeping {
        requeue: true,
        stubborn: true,
        ..Keeping::default()
    });
    fleet.decline([1, 0, 0]);
    fleet.entries[0].absent.store(true, Ordering::SeqCst);
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    let error = sent.expect_err("m1 never takes it");
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    // The first offer, one re-queued offer, then two failovers.
    assert_eq!(fleet.offered(), [4, 0, 0]);
    assert_eq!(fleet.failovers(), [2.0, 0.0, 0.0]);
}

/// Takes the first instance, and adds the meta `chose-<n>` for its `n`th
/// choice.
struct Stamping(AtomicUsize);

impl gdl::Selector for Stamping {
    type Head = Encoded;

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, Encoded>,
        instances: gdl::Instances<'a>,
    ) -> gdl::Result<gdl::Choice<'a>> {
        let nth = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        let chosen = instances.get(0).expect("never empty");
        Ok(gdl::Choice::new(chosen).with_meta(format!("chose-{nth}"), "x"))
    }
}

/// A request re-queued under a selector asks the selector for an instance
/// of its pool each round, and each offer carries only the meta of its own
/// round's choice: a declined round's meta does not reach the next offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_queued_request_asks_its_selector_each_round() {
    let fleet = Fleet::new(backoff()).await;
    let stamping = Arc::new(Stamping(AtomicUsize::new(0)));
    let router = fleet.router_within(
        Keeping {
            requeue: true,
            ..Keeping::default()
        },
        Within::Selector {
            name: Arc::from("stamping"),
            selector: Arc::new(gdl::__runtime::Typed(Arc::clone(&stamping))),
            encoding: Default::default(),
        },
    );
    fleet.decline([2, 0, 0]);
    let (sent, _) = fleet.send(&router, &Cx::new()).await;
    sent.expect("m1 took it");
    assert_eq!(fleet.offered(), [3, 0, 0]);
    assert_eq!(stamping.0.load(Ordering::SeqCst), 3, "a choice per round");
    let metas = fleet.entries[0].metas.lock().expect("not poisoned").clone();
    assert_eq!(metas, [["chose-1"], ["chose-2"], ["chose-3"]]);
}
