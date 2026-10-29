//! A pool policy that keeps a request with one pool re-queues it there
//! when the pool declines it as full: the chooser offers it to the same
//! pool again once an offer it made to the pool ends, or after a short
//! backoff, until the pool takes it or the request's deadline passes. It
//! is never offered to another pool while its pool takes work, no
//! failover is counted, and the pool is not damped. A policy that does
//! not ask for this fails over, as before.
//!
//! The chooser counts each re-queue, and each re-queued request that
//! ended at its deadline, under the pool, on its metrics endpoint, beside
//! what it holds of each pool and how long each offer took.
//!
//! The pinned pool has an admission limit of 1, and a request held in it
//! by the test fills it. Every hub and instance runs in this process on
//! OS-assigned ports; every wait is bounded and for something observed.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::boundary::{
    Model, ModelPool, RUNTIME, Running, model_configured, model_of, placement, run_recorded,
    spec_of,
};
use common::pools::{REGION, Resolving, followed, global_pool, pool_hub_of, settings, until};
use common::{Chunk, Events, Plan, Prompt, WAIT, collect, expected, prompt, reference_graph};
use gdl::{
    Cx, Decline, Encoded, ErrorKind, Next, NoPool, Pool, PoolChoice, PoolPolicies, PoolPolicy,
    PoolRequest,
};
use gdl_runtime::{Deployment, DeploymentSpec, PoolState, Pools, Register, RuntimeConfig};
use tokio::task::JoinHandle;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// Sends every request to `M1` while it takes work, and to another pool
/// once it does not, or once it declined the request; re-queues the
/// request for `M1` when it declines it as full, if `requeue`.
struct Pinned {
    requeue: bool,
}

impl PoolPolicy for Pinned {
    type Head = Encoded;

    fn choose(
        &self,
        request: PoolRequest<'_, Encoded>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let open = |pool: &Pool| pool.takes_work() && !request.is_excluded(pool);
        pools
            .iter()
            .position(|pool| pool.name() == M1 && open(pool))
            .or_else(|| pools.iter().position(open))
            .map(PoolChoice::new)
            .ok_or_else(NoPool::new)
    }

    fn requeue(&self, request: PoolRequest<'_, Encoded>, pool: &Pool) -> bool {
        self.requeue && pool.name() == M1 && request.declined(pool) == Some(Decline::Full)
    }
}

/// The reference graph, whose `model_x` pools `pinned` chooses.
fn spec() -> DeploymentSpec {
    spec_of(placement().pool_policy(
        "model_x",
        gdl_model::PoolPolicy {
            policy: "pinned".into(),
            provides: Vec::new(),
        },
    ))
}

/// A chooser at the global pool's hub that chooses by [`Pinned`], which
/// re-queues if `requeue`, and waits up to `backoff_ms` before it offers
/// a re-queued request again when no offer to its pool ends first. Its
/// system listener is on, and serves its metrics ([`scrape`]).
async fn chooser(
    spec: &DeploymentSpec,
    hub: &velo_hub::Hub,
    requeue: bool,
    backoff_ms: u64,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!(
        "[system]\nhost = \"127.0.0.1\"\nport = 0\n{}{RUNTIME}pool_requeue_backoff_ms = {backoff_ms}\n",
        global_pool()
    );
    let mut config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let mut policies = PoolPolicies::new();
    policies
        .add("pinned", Pinned { requeue })
        .expect("registered");
    config.runtime.pool_policies = policies;
    let quiet = Events::quiet();
    let runtime = config.runtime.clone();
    run_recorded(config, "a", spec, runtime, &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

/// The prefix every series of a segment's registry carries by default.
const PREFIX: &str = "dynamo_component_";

/// What `chooser`'s metrics endpoint serves now.
async fn scrape<H>(chooser: &Running<H>) -> Scraped {
    let address = chooser.process.system_address().expect("a system listener");
    let (code, body) = common::system::get(address, "/metrics").await;
    assert_eq!(code, 200, "{body}");
    Scraped(body)
}

/// A scrape of a metrics endpoint, in Prometheus' text format.
struct Scraped(String);

impl Scraped {
    /// The value of the series of `name` (without the prefix) whose
    /// labels include `labels`; `None` when there is none.
    fn value(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let name = format!("{PREFIX}{name}{{");
        self.0
            .lines()
            .filter(|line| line.starts_with(&name))
            .find(|line| {
                labels
                    .iter()
                    .all(|(label, value)| line.contains(&format!("{label}=\"{value}\"")))
            })
            .map(|line| {
                let value = line.rsplit(' ').next().expect("a value");
                value.parse().unwrap_or_else(|_| panic!("a number: {line}"))
            })
    }

    /// [`Scraped::value`] of a family of `M1`'s or `M2`'s series, which
    /// carry the pool's class and region.
    fn pool(&self, name: &str, pool: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let mut all = vec![("class", "model_x"), ("pool", pool), ("region", REGION)];
        all.extend_from_slice(labels);
        self.value(name, &all)
    }

    /// The re-queues of requests for `M1`, and those that ended at their
    /// deadline.
    fn requeues(&self) -> (Option<f64>, Option<f64>) {
        (
            self.pool("pool_requeues_total", M1, &[]),
            self.pool("pool_requeue_deadlines_total", M1, &[]),
        )
    }
}

/// `M1`, ready, whose one entry instance admits one request at a time;
/// its nodes log to `events`.
async fn pinned_pool(
    spec: &DeploymentSpec,
    directory: &Resolving,
    events: &Arc<Events>,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    let one = |segment: &str, runtime: RuntimeConfig| match segment {
        "f" => RuntimeConfig {
            admission_limit: 1,
            ..runtime
        },
        _ => runtime,
    };
    model_configured(
        spec,
        directory,
        ready(M1, "m1-hub:1337"),
        &one,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// `M2`, degraded, which takes any request offered to it.
async fn other_pool(
    spec: &DeploymentSpec,
    directory: &Resolving,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    let quiet = Events::quiet();
    let pool = Model {
        name: M2,
        hub: "m2-hub:1337",
        quorum: 2,
        entries: 1,
    };
    model_of(spec, directory, pool, &|runtime| runtime, &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
}

/// A `ready` pool: one entry instance makes its quorum.
fn ready(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries: 1,
    }
}

/// The global pool's hub, the chooser, `M1` and `M2`, with the chooser
/// following both pools, and a request held in `M1`, which fills it.
struct Setup<C, P, O> {
    hub: velo_hub::Hub,
    chooser: Running<C>,
    pools: Pools,
    m1: ModelPool<P>,
    m2: ModelPool<O>,
    events: Arc<Events>,
    held: JoinHandle<gdl::Result<()>>,
}

async fn setup(
    requeue: bool,
    backoff_ms: u64,
) -> Setup<
    impl Next<Prompt, Chunk> + Register + use<>,
    impl Next<Prompt, Chunk> + Register + use<>,
    impl Next<Prompt, Chunk> + Register + use<>,
> {
    let spec = spec();
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _) = pool_hub_of(&spec, common::pools::global_hub(), &directory).await;
    let chooser = chooser(&spec, &hub, requeue, backoff_ms).await;
    let m1 = pinned_pool(&spec, &directory, &events).await;
    let m2 = other_pool(&spec, &directory).await;
    let pools = chooser.pools.clone().expect("a chooser");
    until(&pools, "both model pools followed", |pools| {
        followed(pools, "f", M1).is_some_and(|m| m.state == PoolState::Ready)
            && followed(pools, "f", M2).is_some_and(|m| m.state == PoolState::Degraded)
    })
    .await;
    let held = gated(&chooser, &events, "held").await;
    Setup {
        hub,
        chooser,
        pools,
        m1,
        m2,
        events,
        held,
    }
}

impl<C, P, O> Setup<C, P, O> {
    /// Let the held request go on, wait for its answer, and shut the hubs.
    async fn end(self) {
        release(&self.events, "held");
        tokio::time::timeout(WAIT, self.held)
            .await
            .expect("the held request answered in time")
            .expect("the request's task")
            .expect("the held request answered");
        for hub in [self.hub, self.m1.hub, self.m2.hub] {
            hub.shutdown().await.expect("valid test fixture");
        }
    }

    /// How many offers `M1`'s entry instance declined.
    fn declined(&self) -> usize {
        self.m1.entries[0].segment.load().declined
    }

    /// Wait until `M1`'s entry instance has declined `count` offers.
    async fn declines(&self, count: usize) {
        tokio::time::timeout(WAIT, async {
            while self.declined() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{count} declines in time, saw {}", self.declined()));
    }
}

/// Send a request whose C waits, in the model pool, until the test
/// releases it, and return its answer once that C has it: the request is
/// admitted at the pool's entry, and holds its one permit.
async fn gated<H: Next<Prompt, Chunk> + Send + Sync + 'static>(
    chooser: &Running<H>,
    events: &Arc<Events>,
    text: &str,
) -> JoinHandle<gdl::Result<()>> {
    let plan = Plan {
        tokens: 3,
        gated: true,
        ..Plan::default()
    };
    let answer = send(chooser, prompt(text, plan), Cx::new());
    events.wait_for(&format!("C:{text}"), WAIT).await;
    answer
}

/// Let the gated request `text` go on.
fn release(events: &Events, text: &str) {
    events.record(format!("test:release:{text}"));
}

/// Send `request` with `cx` through the chooser, in a task: `Ok` once all
/// three items came, or the error.
fn send<H: Next<Prompt, Chunk> + Send + Sync + 'static>(
    chooser: &Running<H>,
    request: Prompt,
    cx: Cx,
) -> JoinHandle<gdl::Result<()>> {
    let segment = Arc::clone(&chooser.segment);
    tokio::spawn(async move {
        let out = segment.call(cx, request).await?;
        let answer = collect(out).await;
        if let Some(Err(error)) = answer.iter().find(|item| item.is_err()) {
            return Err(error.clone());
        }
        assert_eq!(answer, expected(0..3));
        Ok(())
    })
}

/// A request that streams three items.
fn asked(text: &str) -> Prompt {
    prompt(
        text,
        Plan {
            tokens: 3,
            ..Plan::default()
        },
    )
}

/// The forward hand-offs that reached `pool`, taken or refused.
fn offers<H>(pool: &ModelPool<H>) -> usize {
    pool.entries.iter().map(Running::offered).sum()
}

/// The pinned pool fills, and declines the next request as full. The
/// chooser re-queues it for that pool: it holds no lease on the pool while
/// it waits, does not damp the pool, and offers the request to no other
/// pool. Once the request that fills the pool ends, which ends an offer
/// the chooser made to it, the chooser offers the request there again, and
/// the pool serves it. The backoff is longer than the test may wait, so
/// only the end of that offer can wake the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_pool_serves_the_request_it_re_queued_once_room_returns() {
    let setup = setup(true, 60_000).await;
    let waiting = send(&setup.chooser, asked("waiting"), Cx::new());
    setup.declines(1).await;
    // The entry counts its decline before its answer reaches the chooser,
    // which then ends the declined offer's lease: wait for that, so that
    // the one lease left is the held request's.
    tokio::time::timeout(WAIT, async {
        while followed(&setup.pools, "f", M1).is_none_or(|m| m.leases != 1) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "a lease held while the request waits: {:?}",
            followed(&setup.pools, "f", M1)
        )
    });
    let m = followed(&setup.pools, "f", M1).expect("followed");
    assert!(m.damped.is_none() && !m.gone, "the pool was damped: {m:?}");
    assert_eq!(offers(&setup.m2), 0, "offered to the other pool");
    assert!(!waiting.is_finished(), "the request did not wait");

    release(&setup.events, "held");
    tokio::time::timeout(WAIT, waiting)
        .await
        .expect("the re-queued request answered in time")
        .expect("the request's task")
        .expect("the pinned pool served the re-queued request");
    assert_eq!(
        (setup.m1.forwards(), setup.m2.forwards(), offers(&setup.m2)),
        (2, 0, 0)
    );
    assert_eq!(setup.declined(), 1, "offered again before room returned");
    assert_eq!(
        scrape(&setup.chooser).await.requeues(),
        (Some(1.0), Some(0.0))
    );
    setup.end().await;
}

/// A pinned pool that stays full keeps the re-queued request until its
/// deadline, which fails it with `DeadlineExceeded`; it is never offered
/// to another pool. The short backoff offers it to the pool again and
/// again meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pool_that_stays_full_fails_its_re_queued_request_at_the_deadline() {
    let setup = setup(true, 20).await;
    let deadline = Duration::from_millis(1500);
    let waiting = send(
        &setup.chooser,
        asked("waiting"),
        Cx::new().with_timeout(deadline),
    );
    let error = tokio::time::timeout(WAIT, waiting)
        .await
        .expect("the request ended in time")
        .expect("the request's task")
        .expect_err("the pool stayed full");
    assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");
    assert!(setup.declined() >= 2, "offered once: {}", setup.declined());
    assert_eq!((offers(&setup.m2), setup.m1.forwards()), (0, 1));
    // Re-queued once, however many times it was offered again.
    assert_eq!(
        scrape(&setup.chooser).await.requeues(),
        (Some(1.0), Some(1.0))
    );
    setup.end().await;
}

/// The control: a policy that does not re-queue fails over, as before.
/// The full pool's decline damps it, and the other pool serves the
/// request at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_policy_that_does_not_re_queue_fails_over_from_a_full_pool() {
    let setup = setup(false, 60_000).await;
    tokio::time::timeout(WAIT, send(&setup.chooser, asked("moved"), Cx::new()))
        .await
        .expect("answered in time")
        .expect("the request's task")
        .expect("the other pool served it");
    assert_eq!((setup.m1.forwards(), setup.m2.forwards()), (1, 1));
    let m = followed(&setup.pools, "f", M1).expect("followed");
    assert!(m.damped.is_some(), "the full pool was not damped: {m:?}");
    assert_eq!(
        scrape(&setup.chooser).await.requeues(),
        (Some(0.0), Some(0.0))
    );
    setup.end().await;
}

/// A pool that starts to drain while a request waits for it no longer
/// takes work: the request leaves it, once, and the other pool serves it.
/// Before the drain, the pool declined it twice, so it waited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_re_queued_request_leaves_a_pool_that_drains() {
    let setup = setup(true, 20).await;
    let waiting = send(&setup.chooser, asked("waiting"), Cx::new());
    setup.declines(2).await;
    assert_eq!(offers(&setup.m2), 0, "offered to the other pool");
    setup.m1.readiness.drain().await.expect("the pool drains");
    tokio::time::timeout(WAIT, waiting)
        .await
        .expect("answered in time")
        .expect("the request's task")
        .expect("the other pool served it");
    assert_eq!((setup.m1.forwards(), setup.m2.forwards()), (1, 1));
    assert_eq!(
        scrape(&setup.chooser).await.requeues(),
        (Some(1.0), Some(0.0))
    );
    setup.end().await;
}

/// The model a request wants, as a property.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct Served {
    name: String,
}

const SERVED: gdl::PropertyKind<Served> = gdl::PropertyKind::new("served");

/// A pinned pool whose entry lists the model a request wants as
/// `starting`, as during an engine's restart, declines the request as
/// full. The chooser re-queues it, and offers it again on its backoff,
/// since no offer to the pool ends meanwhile: once the model is `ready`,
/// the pool serves it. It is never offered to the other pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_waits_for_its_pool_while_the_property_it_wants_starts() {
    let spec = spec();
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = pool_hub_of(&spec, common::pools::global_hub(), &directory).await;
    let chooser = chooser(&spec, &hub, true, 20).await;
    let model = || Served {
        name: "restarting".into(),
    };
    let properties = gdl::InstanceProperties::new();
    properties
        .wants(&SERVED, |prompt: &Prompt| Served {
            name: prompt.text.clone(),
        })
        .expect("registered");
    properties
        .set(&SERVED, [(model(), gdl::EntryState::Starting)])
        .expect("a list");
    let given = properties.clone();
    let entry = move |runtime: RuntimeConfig| RuntimeConfig {
        properties: given.clone(),
        ..runtime
    };
    let m1 = model_of(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        &entry,
        &|deployment: &Deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let m2 = other_pool(&spec, &directory).await;
    let pools = chooser.pools.clone().expect("a chooser");
    until(&pools, "both model pools followed", |pools| {
        followed(pools, "f", M1).is_some_and(|m| m.state == PoolState::Ready)
            && followed(pools, "f", M2).is_some_and(|m| m.state == PoolState::Degraded)
    })
    .await;

    let waiting = send(&chooser, asked("restarting"), Cx::new());
    tokio::time::timeout(WAIT, async {
        while m1.entries[0].segment.load().declined < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("declined twice while the model starts");
    assert!(!waiting.is_finished(), "the request did not wait");
    let m = followed(&pools, "f", M1).expect("followed");
    assert!(m.damped.is_none(), "the pool was damped: {m:?}");

    properties
        .set(&SERVED, [(model(), gdl::EntryState::Ready)])
        .expect("a list");
    tokio::time::timeout(WAIT, waiting)
        .await
        .expect("answered in time")
        .expect("the request's task")
        .expect("the pinned pool served it once the model was ready");
    assert_eq!((m1.forwards(), m2.forwards(), offers(&m2)), (1, 0, 0));
    assert_eq!(scrape(&chooser).await.requeues(), (Some(1.0), Some(0.0)));
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The chooser's metrics endpoint shows, for each of the two pools it
/// follows, the load summary it holds, the summary's age, the pool's state
/// and whether it takes work, each labelled with the pool's class, name
/// and region; and the round trip of each offer, by how it ended, as many
/// as `pool_offers_total` counts. Known offers: the held request to `M1`,
/// accepted; then one that `M1` declines as full and `M2` takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_chooser_shows_what_it_holds_of_each_pool_and_its_offers_round_trips() {
    let setup = setup(false, 60_000).await;
    tokio::time::timeout(WAIT, send(&setup.chooser, asked("moved"), Cx::new()))
        .await
        .expect("answered in time")
        .expect("the request's task")
        .expect("the other pool served it");
    // The held request holds `M1`'s one permit: its summary says so, once
    // one made after it reached the chooser.
    until(&setup.pools, "each pool's summary, M1's full", |pools| {
        followed(pools, "f", M1)
            .and_then(|m| m.load)
            .is_some_and(|load| load.admitted == 1)
            && followed(pools, "f", M2).is_some_and(|m| m.load.is_some())
    })
    .await;
    let scraped = scrape(&setup.chooser).await;
    let text = &scraped.0;

    for (pool, admitted, limit) in [(M1, 1.0, 1.0), (M2, 0.0, 1024.0)] {
        assert_eq!(
            scraped.pool("pool_load_admitted", pool, &[]),
            Some(admitted),
            "{pool}: {text}"
        );
        assert_eq!(scraped.pool("pool_load_limit", pool, &[]), Some(limit));
        assert_eq!(scraped.pool("pool_load_instances", pool, &[]), Some(1.0));
        assert!(
            scraped.pool("pool_load_refusals", pool, &[]).is_some(),
            "{pool}: {text}"
        );
        // Made by the pool's hub at most a few periods ago.
        let age = scraped
            .pool("pool_load_age_seconds", pool, &[])
            .expect("an age");
        assert!((0.0..5.0).contains(&age), "{pool}: {age}");
        assert_eq!(scraped.pool("pool_takes_work", pool, &[]), Some(1.0));
    }
    for (pool, state) in [(M1, "ready"), (M2, "degraded")] {
        for other in ["ready", "degraded", "unavailable", "draining"] {
            assert_eq!(
                scraped.pool("pool_state", pool, &[("state", other)]),
                Some(f64::from(u8::from(other == state))),
                "{pool} {other}: {text}"
            );
        }
    }

    let outcomes = [
        "accepted",
        "declined_full",
        "declined_gone",
        "declined_instance",
        "declined_not_present",
        "unsent",
        "outcome_unknown",
        "failed",
    ];
    for (pool, made) in [
        (M1, [("accepted", 1.0), ("declined_full", 1.0)].as_slice()),
        (M2, [("accepted", 1.0)].as_slice()),
    ] {
        for outcome in outcomes {
            let expected = made
                .iter()
                .find(|(of, _)| *of == outcome)
                .map_or(0.0, |(_, count)| *count);
            let offered = scraped.value(
                "pool_offers_total",
                &[("class", "model_x"), ("pool", pool), ("outcome", outcome)],
            );
            assert_eq!(offered, Some(expected), "{pool} {outcome}");
            let by = [("outcome", outcome)];
            let count = scraped.pool("pool_offer_round_trip_seconds_count", pool, &by);
            assert_eq!(count, Some(expected), "{pool} {outcome}: {text}");
            let inf = [("outcome", outcome), ("le", "+Inf")];
            let inf = scraped.pool("pool_offer_round_trip_seconds_bucket", pool, &inf);
            assert_eq!(inf, Some(expected), "{pool} {outcome}");
            let sum = scraped
                .pool("pool_offer_round_trip_seconds_sum", pool, &by)
                .expect("a sum");
            assert_eq!(sum > 0.0, expected > 0.0, "{pool} {outcome}: {sum}");
        }
    }
    assert_eq!(
        scraped.pool("pool_requeues_total", M2, &[]),
        Some(0.0),
        "{text}"
    );
    setup.end().await;
}
