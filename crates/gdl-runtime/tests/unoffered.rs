//! A global pool's hub marks a followed model pool not offered when the
//! pool drains or its link is lost; each chooser of the global pool stops
//! offering to it, and once it holds no offer to it, says so to its hub;
//! once every chooser has, the hub closes the follow. Until then the
//! choosers keep the pool's entry instances registered, so that what they
//! offered to it is answered.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed. The tests that hold a request show that nothing closes for
//! longer than an instance's drain (2 s), so each takes a few seconds.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::boundary::{
    Model, ModelPool, RUNTIME, Running, ask, model_of, model_proxied, placement, run_graph, spec_of,
};
use common::pools::{Resolving, followed, global_hub, global_pool, pool_hub_of, settings, until};
use common::proxy::Mode;
use common::proxy::Proxy;
use common::{Chunk, Events, Plan, Prompt, WAIT, collect, expected, prompt, reference_graph};
use futures::{Stream, StreamExt};
use gdl::{Cx, Next, Selectors};
use gdl_hub::{PoolReadiness, PoolTiming};
use gdl_runtime::{Deployment, DeploymentSpec, PoolState, Pools, Register, RuntimeConfig};
use gdl_watch::FollowEvent;
use tokio::task::JoinHandle;
use velo::InstanceId;
use velo::discovery::ServiceDiscovery as _;
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";

/// How long an instance keeps a peer registered after it is in no table:
/// the `drain_ms` of [`RUNTIME`].
const DRAIN: Duration = Duration::from_secs(2);

/// A `ready` pool: one entry instance makes its quorum.
fn ready(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries: 1,
    }
}

/// The global pool's hub, and its extension.
async fn global_hub_of(
    spec: &DeploymentSpec,
    directory: &Resolving,
) -> (Hub, Arc<PoolReadiness<Resolving>>) {
    pool_hub_of(spec, global_hub(), directory).await
}

/// A chooser, an instance of `a` in the global pool at `hub`.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    chooser_at(spec, hub.public_addr().port(), hub.control_addr().port()).await
}

/// A chooser that reaches its hub's public listener at `public` and its
/// control listener at `control`.
async fn chooser_at(
    spec: &DeploymentSpec,
    public: u16,
    control: u16,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_pool());
    let config = settings(public, control, 5_000, &extra);
    let runtime = RuntimeConfig {
        selectors: Selectors::new(),
        ..config.runtime.clone()
    };
    let quiet = Events::quiet();
    run_graph(&config, "a", spec, runtime, &|deployment: &Deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

/// A model pool of the reference graph whose nodes log to `events`.
async fn pool(
    spec: &DeploymentSpec,
    directory: &Resolving,
    events: &Arc<Events>,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    model_of(
        spec,
        directory,
        ready(M1, "m1-hub:1337"),
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// The chooser's tables.
fn tables<H>(chooser: &Running<H>) -> Pools {
    chooser
        .pools
        .clone()
        .expect("a global instance in a pool chooses")
}

/// The id of `instance`.
fn id<H>(instance: &Running<H>) -> InstanceId {
    instance.node.instance_id()
}

/// Wait until the chooser follows [`M1`] ready, offered, with its entry
/// instance and a load summary.
async fn offered(pools: &Pools) {
    until(pools, "the model pool offered", |pools| {
        followed(pools, "f", M1).is_some_and(|m| {
            m.state == PoolState::Ready && m.offered && m.eligible.len() == 1 && m.load.is_some()
        })
    })
    .await;
}

/// Send a request whose C waits, in the model pool, until the test
/// releases it, and return the request's answer once that C has it: the
/// chooser holds its offer to the pool until the response ends.
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
    let segment = Arc::clone(&chooser.segment);
    let request = prompt(text, plan);
    let answer = tokio::spawn(async move {
        let out = segment.call(Cx::new(), request).await?;
        let answer = collect(out).await;
        if let Some(Err(error)) = answer.iter().find(|item| item.is_err()) {
            return Err(error.clone());
        }
        assert_eq!(answer, expected(0..3));
        Ok(())
    });
    events.wait_for(&format!("C:{text}"), WAIT).await;
    answer
}

/// Let the gated request `text` go on.
fn release(events: &Events, text: &str) {
    events.record(format!("test:release:{text}"));
}

/// Read the relay until an event `last` takes, within `within`: `None`
/// if none came in time.
async fn relayed(
    watch: &mut (impl Stream<Item = FollowEvent> + Unpin),
    within: Duration,
    last: impl Fn(&FollowEvent) -> bool,
) -> Option<FollowEvent> {
    tokio::time::timeout(within, async {
        loop {
            let event = watch.next().await.expect("the relay goes on");
            if last(&event) {
                return event;
            }
        }
    })
    .await
    .ok()
}

fn is_dropped(event: &FollowEvent) -> bool {
    matches!(event, FollowEvent::Dropped { pool, .. } if pool == M1)
}

fn is_unoffered(event: &FollowEvent) -> bool {
    matches!(event, FollowEvent::Offering { pool, offered: false, .. } if pool == M1)
}

/// Wait until the chooser has let go of `instance`: velo no longer has it
/// registered.
async fn unregistered(pools: &Pools, instance: InstanceId) {
    until(pools, "the entry instance unregistered", |pools| {
        !pools.is_registered(instance)
    })
    .await;
}

/// A model pool that drains is marked not offered, at the follow's next
/// revision; each chooser of the global pool stops offering to it and
/// acknowledges, and once both have, the global's hub closes the follow,
/// the choosers let go of it, and the model pool's hub sees its follower
/// go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_pool_is_let_go_once_every_chooser_acknowledged() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let (first, second) = (chooser(&spec, &hub).await, chooser(&spec, &hub).await);
    let m1 = pool(&spec, &directory, &quiet).await;
    for chooser in [&first, &second] {
        offered(&tables(chooser)).await;
        ask(&chooser.segment).await.expect("answered");
    }
    let mut relay = global.follows();

    m1.readiness.drain().await.expect("the pool drains");
    let marked = relayed(&mut relay, WAIT, is_unoffered)
        .await
        .expect("marked not offered in time");
    let FollowEvent::Offering {
        generation,
        revision,
        ..
    } = marked
    else {
        unreachable!("an offering");
    };
    assert_eq!(revision, 2, "not the follow's next revision");
    let closed = relayed(&mut relay, WAIT, is_dropped)
        .await
        .expect("closed in time");
    assert_eq!(
        closed,
        FollowEvent::Dropped {
            class: "model_x".into(),
            pool: M1.into(),
            generation,
        }
    );
    for chooser in [&first, &second] {
        let pools = tables(chooser);
        until(&pools, "the chooser let go of the pool", |pools| {
            followed(pools, "f", M1).is_none()
        })
        .await;
    }
    let mut following = global.following();
    tokio::time::timeout(
        WAIT,
        following.wait_for(|pools| !pools.keys().any(|(_, pool)| pool == M1)),
    )
    .await
    .expect("the global's hub no longer follows the pool in time")
    .expect("the extension runs");
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|pools| pools.is_empty()))
        .await
        .expect("the model pool's hub saw its follower go in time")
        .expect("the extension runs");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An instance of the chooser's segment that runs without `[pool]`, under
/// the same hub, follows nothing and offers nothing to another pool, so
/// it does not keep the global's follow of a draining pool from closing.
/// Such an instance waits at its launch for the instances of every
/// segment it sends to, which are in another hub; it is a member of the
/// global's hub meanwhile, which is when it could hold the follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_without_a_pool_holds_no_follow_open() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, &quiet).await;
    offered(&tables(&chooser)).await;
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        60_000,
        RUNTIME,
    );
    let runtime = RuntimeConfig {
        selectors: Selectors::new(),
        ..config.runtime.clone()
    };
    let plain = tokio::spawn({
        let (spec, quiet) = (spec.clone(), Arc::clone(&quiet));
        async move {
            let _ = run_graph(&config, "a", &spec, runtime, &|deployment: &Deployment| {
                reference_graph(deployment, &quiet)
            })
            .await;
        }
    });
    let client = velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture");
    let loaded = gdl_model::segment_service("a", gdl_model::SegmentPhase::Loaded);
    tokio::time::timeout(WAIT, async {
        while client
            .services()
            .get_instances(&loaded)
            .await
            .map_or(0, |instances| instances.len())
            < 2
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the instance without a pool is a member of the hub in time");
    let mut relay = global.follows();

    m1.readiness.drain().await.expect("the pool drains");
    relayed(&mut relay, WAIT, is_dropped)
        .await
        .expect("closed though an instance without a pool is a member");
    plain.abort();
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A request offered to the pool before it drained holds the chooser's
/// lease on the pool's follow: the chooser shows the pool not offered,
/// with the lease, and does not acknowledge, so the follow stays open and
/// the entry instance registered for longer than an instance's drain, and
/// past the global hub's bound on a lost chooser ([`LOST`]), since this
/// one still watches the relay, until the response ends. The request is
/// answered; then the follow closes, and the chooser lets go of the entry
/// instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_offer_keeps_the_follow_open_until_its_response_ends() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = global_hub_lost(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, &events).await;
    let pools = tables(&chooser);
    offered(&pools).await;
    let entry = id(&m1.entries[0]);
    let held = gated(&chooser, &events, "held").await;
    let mut relay = global.follows();

    m1.readiness.drain().await.expect("the pool drains");
    until(&pools, "the pool not offered, its lease held", |pools| {
        followed(pools, "f", M1).is_some_and(|m| !m.offered && m.leases == 1)
    })
    .await;
    assert!(
        relayed(
            &mut relay,
            (DRAIN + Duration::from_secs(1)).max(LOST * 2),
            is_dropped
        )
        .await
        .is_none(),
        "closed while an offer is held"
    );
    assert!(
        pools.is_registered(entry),
        "let go of while an offer is held"
    );
    assert!(followed(&pools, "f", M1).is_some_and(|m| !m.offered && m.leases == 1));

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    relayed(&mut relay, WAIT, is_dropped)
        .await
        .expect("closed once the response ended");
    tokio::time::timeout(DRAIN + WAIT, unregistered(&pools, entry))
        .await
        .expect("let go of within the drain");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A follow whose link is lost, here at a proxy in front of the model
/// pool's hub that cuts every connection, is marked not offered at once.
/// The chooser keeps the entry instance registered while it holds an offer
/// to it, however long that takes, and the request is answered; then the
/// follow closes, and the chooser lets go of the instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_link_is_not_offered_at_once_and_kept_until_its_leases_end() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = model_proxied(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        RUNTIME,
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let pools = tables(&chooser);
    offered(&pools).await;
    let entry = id(&m1.entries[0]);
    let held = gated(&chooser, &events, "held").await;
    let mut relay = global.follows();

    proxy.set(Mode::Refuse);
    relayed(&mut relay, WAIT, is_unoffered)
        .await
        .expect("marked not offered at once");
    until(&pools, "the pool not offered, its lease held", |pools| {
        followed(pools, "f", M1).is_some_and(|m| !m.offered && m.leases == 1)
    })
    .await;
    assert!(
        relayed(&mut relay, DRAIN + Duration::from_secs(1), is_dropped)
            .await
            .is_none(),
        "closed while an offer is held"
    );
    assert!(
        pools.is_registered(entry),
        "let go of while an offer is held"
    );

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    relayed(&mut relay, WAIT, is_dropped)
        .await
        .expect("closed once the response ended");
    tokio::time::timeout(DRAIN + WAIT, unregistered(&pools, entry))
        .await
        .expect("let go of within the drain");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry instance's bound in [`a_drain_ends_early_once_its_followers_let_go`]:
/// a setup timeout of 10 s and twice the summary period.
const LONG_BOUND: Duration = Duration::from_secs(12);

/// A long bound's runtime settings: a setup timeout of 10 s.
const LONG: &str = "[runtime]\nsetup_timeout_ms = 10000\ndrain_ms = 2000\n";

/// Wait, within `within`, until `instance`, an entry instance, declines
/// offers.
async fn declining<H>(instance: &Running<H>, within: Duration) {
    tokio::time::timeout(within, async {
        while !instance.segment.declines_offers() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the entry instance declined offers in time");
}

/// A draining pool whose only follower, the global pool, lets go of it
/// once its chooser holds no offer to it ends its entry instance's bound
/// early: the entry declines offers from then, long before its bound would
/// have run out, and the pool's hub sees no follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_ends_early_once_its_followers_let_go() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = common::boundary::model_summarized(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONG,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let pools = tables(&chooser);
    offered(&pools).await;
    ask(&chooser.segment).await.expect("answered");
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|pools| pools.len() == 1))
        .await
        .expect("the global follows the pool in time")
        .expect("the extension runs");

    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the pool drains");
    declining(&m1.entries[0], LONG_BOUND / 2).await;
    assert!(
        started.elapsed() < LONG_BOUND / 2,
        "the bound was not ended early: {:?}",
        started.elapsed()
    );
    assert!(m1.readiness.followers().borrow().is_empty());
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// How long an entry instance of [`RUNTIME`]'s pools takes new work after
/// its pool's drain reaches it: the setup timeout and twice the summary
/// period.
const BOUND: Duration = Duration::from_secs(3);

/// A follower that does not let go, since its chooser holds an offer to
/// the pool, leaves the bound to end the entry instance's intake: it
/// declines offers only once the bound has passed, while the global still
/// follows the pool. Once the response ends, the global lets go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bound_ends_a_drain_whose_follower_holds_an_offer() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, &events).await;
    let pools = tables(&chooser);
    offered(&pools).await;
    let held = gated(&chooser, &events, "held").await;

    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the pool drains");
    declining(&m1.entries[0], BOUND + WAIT).await;
    assert!(
        started.elapsed() >= BOUND,
        "declined {:?} after the drain began, within its bound",
        started.elapsed()
    );
    assert_eq!(
        m1.readiness.followers().borrow().len(),
        1,
        "the global let go while its chooser holds an offer"
    );
    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|pools| pools.is_empty()))
        .await
        .expect("the global let go in time")
        .expect("the extension runs");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A follower whose watch is lost, here at a proxy in front of the model
/// pool's hub that cuts every connection, has not let go of the pool: its
/// chooser still holds an offer to it. The draining pool's entry keeps
/// taking work, as its bound allows; once the offer has ended and the
/// global's hub can reach the pool's hub again, the global lets go, and
/// the entry declines offers well before its bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_watch_is_no_release() {
    const LONGER: &str = "[runtime]\nsetup_timeout_ms = 20000\ndrain_ms = 2000\n";
    // A setup timeout of 20 s and twice the summary period.
    const LONGER_BOUND: Duration = Duration::from_secs(22);
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = model_proxied(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONGER,
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let pools = tables(&chooser);
    offered(&pools).await;
    let held = gated(&chooser, &events, "held").await;

    proxy.set(Mode::Refuse);
    until(&pools, "the pool not offered, its lease held", |pools| {
        followed(pools, "f", M1).is_some_and(|m| !m.offered && m.leases == 1)
    })
    .await;
    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the pool drains");
    let early = tokio::time::timeout(Duration::from_secs(3), declining(&m1.entries[0], WAIT));
    assert!(
        early.await.is_err(),
        "the entry declined while the follower that lost its watch holds an offer"
    );

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    proxy.set(Mode::Forward);
    declining(&m1.entries[0], LONGER_BOUND).await;
    assert!(
        started.elapsed() < LONGER_BOUND - Duration::from_secs(5),
        "declined only at the bound, {:?} after the drain began",
        started.elapsed()
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A follower whose watch is lost while its chooser holds an offer, and
/// whose watch then outlives the model pool's lease (2 s here) without a
/// release, is still no release: the draining pool's entry keeps taking
/// work, as its bound allows, though the pool no longer shows the follower.
/// Once the offer ends and the global's hub can reach the pool's hub
/// again, its release comes, and the entry declines offers well before its
/// bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_watch_past_its_lease_is_no_release() {
    const LONGER: &str = "[runtime]\nsetup_timeout_ms = 20000\ndrain_ms = 2000\n";
    // A setup timeout of 20 s and twice the summary period.
    const LONGER_BOUND: Duration = Duration::from_secs(22);
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = common::boundary::model_proxied_leased(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONGER,
        Duration::from_secs(2),
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let pools = tables(&chooser);
    offered(&pools).await;
    let held = gated(&chooser, &events, "held").await;

    proxy.set(Mode::Refuse);
    until(&pools, "the pool not offered, its lease held", |pools| {
        followed(pools, "f", M1).is_some_and(|m| !m.offered && m.leases == 1)
    })
    .await;
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|pools| pools.is_empty()))
        .await
        .expect("the lost watch's lease passed in time")
        .expect("the extension runs");
    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the pool drains");
    let early = tokio::time::timeout(Duration::from_secs(4), declining(&m1.entries[0], WAIT));
    assert!(
        early.await.is_err(),
        "the entry declined on a lost watch's lease, while its follower holds an offer"
    );

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    proxy.set(Mode::Forward);
    declining(&m1.entries[0], LONGER_BOUND).await;
    assert!(
        started.elapsed() < LONGER_BOUND - Duration::from_secs(5),
        "declined only at the bound, {:?} after the drain began",
        started.elapsed()
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A follow link that replaces its watch of the model pool, at an event of
/// a kind the global's hub does not know, opens the new watch under the
/// same follow: the model pool's hub counts the follower on through the
/// replacement, never as a watch closed without a release, so that well
/// past the pool's lease (2 s here) it still lists the global; and once
/// the pool drains and the global lets go, its entry declines offers well
/// before its bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_watch_is_not_a_lost_follower() {
    const LONGER: &str = "[runtime]\nsetup_timeout_ms = 20000\ndrain_ms = 2000\n";
    // A setup timeout of 20 s and twice the summary period.
    const LONGER_BOUND: Duration = Duration::from_secs(22);
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = common::boundary::model_proxied_leased(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONGER,
        Duration::from_secs(2),
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let pools = tables(&chooser);
    offered(&pools).await;
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|pools| pools.len() == 1))
        .await
        .expect("the global follows the pool in time")
        .expect("the extension runs");

    // The next load summary, sent once a second, comes as a kind the
    // global's hub does not know.
    proxy.rewrite_once(br#""event":"load""#, br#""event":"xoad""#);
    tokio::time::timeout(WAIT, async {
        while global.unknown_events() == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the global's hub read an unknown event in time");
    offered(&pools).await;
    // Past the lease, and a summary period after it, when a lost watch
    // would have been marked lost.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(
        m1.readiness.followers().borrow().len(),
        1,
        "the replaced watch was counted as closed without a release"
    );

    let started = std::time::Instant::now();
    m1.readiness.drain().await.expect("the pool drains");
    declining(&m1.entries[0], LONGER_BOUND).await;
    assert!(
        started.elapsed() < LONGER_BOUND / 2,
        "declined only at the bound, {:?} after the drain began",
        started.elapsed()
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// How long the global's hub waits for a chooser whose watch of its relay
/// is lost before it stops waiting for its acknowledgements, in the tests
/// that lose one.
const LOST: Duration = Duration::from_secs(1);

/// The global pool's hub, waiting [`LOST`] for a chooser whose watch of
/// its relay is lost, and its extension.
async fn global_hub_lost(
    spec: &DeploymentSpec,
    directory: &Resolving,
) -> (Hub, Arc<PoolReadiness<Resolving>>) {
    let timing = PoolTiming {
        lost_chooser: LOST,
        ..common::pools::timing()
    };
    common::pools::pool_hub_timed(spec, global_hub(), directory, timing).await
}

/// Whether `instance` is still registered at `hub` as an instance of `a`.
async fn member(hub: &Hub, instance: InstanceId) -> bool {
    let client = velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture");
    let loaded = gdl_model::segment_service("a", gdl_model::SegmentPhase::Loaded);
    client
        .services()
        .get_instances(&loaded)
        .await
        .expect("the hub answers")
        .contains(&instance)
}

/// Cut the chooser's watch of the global hub's relay, at a proxy in front
/// of the hub's public listener, and wait until the hub has seen it close:
/// the relay's watchers drop to `left`. The chooser stays a member: it
/// registered on the control listener, which the proxy does not cut, and
/// velo carries its heartbeats.
async fn cut(proxy: &Proxy, global: &PoolReadiness<Resolving>, left: usize) {
    proxy.set(Mode::Refuse);
    let mut watchers = global.follows_watchers();
    tokio::time::timeout(WAIT, watchers.wait_for(|open| *open == left))
        .await
        .expect("the chooser's watch of the relay closed in time")
        .expect("the extension runs");
}

/// Wait until the drained pool's hub has withdrawn its record and refuses
/// a watch.
async fn withdrawn(readiness: &PoolReadiness<Resolving>) {
    tokio::time::timeout(WAIT, async {
        while readiness.watch().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the drained pool withdrawn in time");
}

/// A chooser whose watch of its hub's relay is lost, at a proxy in front
/// of the global hub's public listener, stays a member of the hub, but
/// cannot learn that a draining pool is not offered, so it never says that
/// it holds no offer to it. Once [`LOST`] has passed, the global's hub
/// stops waiting for it: it closes the follow and lets go of the pool, so
/// the pool's entry declines offers long before its own bound, the pool's
/// instances drain and leave, and its record is withdrawn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chooser_whose_relay_watch_is_lost_is_not_waited_for_past_the_bound() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = global_hub_lost(&spec, &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let chooser = chooser_at(&spec, proxy.port, hub.control_addr().port()).await;
    let m1 = common::boundary::model_summarized(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONG,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let pools = tables(&chooser);
    offered(&pools).await;
    ask(&chooser.segment).await.expect("answered");
    let mut relay = global.follows();
    // The bound runs from the watch's close, which is after this.
    let started = std::time::Instant::now();
    cut(&proxy, &global, 1).await;

    m1.readiness.drain().await.expect("the pool drains");
    relayed(&mut relay, LOST + WAIT, is_dropped)
        .await
        .expect("closed once the lost chooser's bound passed");
    assert!(
        started.elapsed() >= LOST,
        "closed {:?} after the watch was cut, within the bound",
        started.elapsed()
    );
    assert!(
        member(&hub, id(&chooser)).await,
        "the chooser left the hub, which closes the follow without the bound"
    );
    let mut followers = m1.readiness.followers();
    tokio::time::timeout(WAIT, followers.wait_for(|pools| pools.is_empty()))
        .await
        .expect("the model pool's hub saw its follower go in time")
        .expect("the extension runs");
    declining(&m1.entries[0], WAIT).await;
    assert!(
        started.elapsed() < LONG_BOUND / 2,
        "declined only at the entry's bound, {:?} after the watch was cut",
        started.elapsed()
    );
    let ModelPool {
        hub: model_hub,
        readiness,
        entries,
        sink,
    } = m1;
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    sink.ended(WAIT)
        .await
        .expect("the interior instance drained");
    withdrawn(&readiness).await;
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A chooser whose watch of its hub's relay is lost while it holds a
/// streamed response from the draining pool is counted out at [`LOST`] as
/// any lost chooser is, and the global's hub lets go of the pool; but its
/// velo link to the entry instance is up, so the entry, which declines new
/// offers from then, finishes the response before it leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_chooser_counted_out_still_gets_its_response() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = global_hub_lost(&spec, &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let chooser = chooser_at(&spec, proxy.port, hub.control_addr().port()).await;
    let m1 = common::boundary::model_summarized(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONG,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let pools = tables(&chooser);
    offered(&pools).await;
    let held = gated(&chooser, &events, "held").await;
    let mut relay = global.follows();
    cut(&proxy, &global, 1).await;

    m1.readiness.drain().await.expect("the pool drains");
    relayed(&mut relay, LOST + WAIT, is_dropped)
        .await
        .expect("closed once the lost chooser's bound passed");
    declining(&m1.entries[0], WAIT).await;
    assert!(!held.is_finished(), "the held response ended early");

    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held response was cut");
    let ModelPool {
        hub: model_hub,
        readiness,
        entries,
        sink,
    } = m1;
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    sink.ended(WAIT)
        .await
        .expect("the interior instance drained");
    withdrawn(&readiness).await;
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A chooser that the global's hub counted out while it held a streamed
/// response from the draining pool watches its relay again while the
/// response streams. Its new snapshot no longer shows the pool, which the
/// hub let go of, but the chooser keeps the entry instance registered for
/// as long as it holds the response: the items go on past the chooser's
/// drain (2 s), and a client that drops the response still cancels it in
/// the pool. Once the response has ended, the chooser lets go of the
/// instance. The response streams for longer than the drain, to show that
/// the instance outlives it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_counted_out_chooser_that_watches_again_keeps_its_response() {
    // The model pool's instances keep what they hold for longer than the
    // chooser's drain, as a deployment's default drain lets them: a
    // retiring instance stops at its own drain, whatever it holds.
    const LONG_DRAIN: &str = "[runtime]\nsetup_timeout_ms = 10000\ndrain_ms = 30000\n";
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = global_hub_lost(&spec, &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let chooser = chooser_at(&spec, proxy.port, hub.control_addr().port()).await;
    let m1 = common::boundary::model_summarized(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        LONG_DRAIN,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    let pools = tables(&chooser);
    offered(&pools).await;
    let entry = followed(&pools, "f", M1).expect("followed").eligible[0];
    let plan = Plan {
        endless: true,
        item_delay_ms: 20,
        ..Plan::default()
    };
    let mut out = chooser
        .segment
        .call(Cx::new(), prompt("held", plan))
        .await
        .expect("the pool takes the request");
    let mut next = async || {
        tokio::time::timeout(WAIT, out.next())
            .await
            .expect("an item in time")
            .expect("the response goes on")
    };
    assert!(next().await.is_ok());
    let mut relay = global.follows();
    cut(&proxy, &global, 1).await;

    m1.readiness.drain().await.expect("the pool drains");
    relayed(&mut relay, LOST + WAIT, is_dropped)
        .await
        .expect("closed once the lost chooser's bound passed");
    proxy.set(Mode::Forward);
    until(&pools, "the chooser watched again", |pools| {
        followed(pools, "f", M1).is_none()
    })
    .await;
    let past = std::time::Instant::now() + DRAIN + Duration::from_secs(1);
    while std::time::Instant::now() < past {
        let item = next().await;
        assert!(item.is_ok(), "the response failed: {item:?}");
    }
    assert!(
        pools.is_registered(entry),
        "the entry instance was unregistered while its response is held"
    );

    drop(out);
    events.wait_for("D:cancelled:c(held)#0", WAIT).await;
    unregistered(&pools, entry).await;
    let ModelPool {
        hub: model_hub,
        readiness,
        entries,
        sink,
    } = m1;
    for entry in entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    sink.ended(WAIT)
        .await
        .expect("the interior instance drained");
    withdrawn(&readiness).await;
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
