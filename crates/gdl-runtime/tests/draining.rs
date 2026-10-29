//! A model pool drains as a whole: its hub says it is `draining`, and the
//! choosers that follow it stop choosing it; its entry instances take the
//! offers that reach them for a bound, then decline new ones, finish what
//! they took, and announce that they have drained; its interior instances
//! serve until every entry instance has drained or left, then drain; and
//! once every instance has left, the hub withdraws the pool's record. An
//! entry instance can also decline offers by its own policy, with no drain:
//! for its pool, or as its own, which leaves the request in its pool.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed. A test that watches an entry instance stop taking work runs
//! for at least its bound, 3 s at the shortest settings a pool allows (a
//! summary period of 1 s), so these tests take several seconds each.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::boundary::{
    Model, ModelPool, RUNTIME, Running, ask, model_configured, model_of, model_proxied, placement,
    run_graph, spec_of,
};
use common::pools::{Resolving, followed, global_hub, global_pool, pool_hub_of, settings, until};
use common::proxy::Mode;
use common::{Chunk, Events, Plan, Prompt, WAIT, collect, expected, prompt, reference_graph};
use futures::StreamExt;
use gdl::{Cx, ErrorKind, FollowedPools, Next, Selectors};
use gdl_hub::PoolReadiness;
use gdl_model::{SegmentPhase, segment_service};
use gdl_pools::{PoolDirectory, PoolEvent, PoolRecord};
use gdl_runtime::{
    Decline, Deployment, DeploymentSpec, PoolState, Pools, Register, RequestRouter, RuntimeConfig,
};
use gdl_watch::PoolWatchEvent;
use tokio::task::JoinHandle;
use velo::InstanceId;
use velo::discovery::{ServiceDiscovery, ServiceEvent};
use velo_hub::Hub;
use velo_hub::client::HubClient;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// A `ready` pool: one entry instance makes its quorum.
fn ready(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries: 1,
    }
}

/// A `degraded` pool: two entry instances make its quorum; it has one.
/// A chooser offers to it only when no ready pool takes the request.
fn degraded(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 2,
        entries: 1,
    }
}

/// The global pool's hub.
async fn global_hub_of(spec: &DeploymentSpec, directory: &Resolving) -> Hub {
    pool_hub_of(spec, global_hub(), directory).await.0
}

/// A chooser, an instance of `a` in the global pool at `hub`.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    reading_chooser(spec, hub, &FollowedPools::new()).await
}

/// A chooser whose nodes read the pools it follows through `followed`.
async fn reading_chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
    followed: &FollowedPools,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_pool());
    let mut config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    config.runtime.followed = followed.clone();
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
    pool: Model<'_>,
    events: &Arc<Events>,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    model_of(spec, directory, pool, &|runtime| runtime, &|deployment| {
        reference_graph(deployment, events)
    })
    .await
}

/// The chooser's tables.
fn tables<H>(chooser: &Running<H>) -> Pools {
    chooser
        .pools
        .clone()
        .expect("a global instance in a pool chooses")
}

/// Wait until the chooser follows each of `pools` in its state, with one
/// entry instance and a load summary.
async fn following(pools: &Pools, expected: &[(&str, PoolState)]) {
    until(pools, "the model pools followed", |pools| {
        expected.iter().all(|(pool, state)| {
            followed(pools, "f", pool)
                .is_some_and(|m| m.state == *state && m.eligible.len() == 1 && m.load.is_some())
        })
    })
    .await;
}

/// Wait until the chooser no longer has `pool` to choose: it follows it
/// `draining`, or no longer follows it.
async fn unchosen(pools: &Pools, pool: &str) {
    until(pools, "the pool drained", |pools| {
        followed(pools, "f", pool).is_none_or(|m| m.state == PoolState::Draining)
    })
    .await;
}

/// The forward hand-offs that reached each of `pools`, taken or refused.
fn offered<H>(pools: &[&ModelPool<H>]) -> Vec<usize> {
    pools
        .iter()
        .map(|pool| pool.entries.iter().map(Running::offered).sum())
        .collect()
}

/// The forward hand-offs that reached `pool`, taken or refused.
fn offers<H>(pool: &ModelPool<H>) -> usize {
    pool.entries.iter().map(Running::offered).sum()
}

/// The record `directory` holds for the model pool `pool`, if any.
async fn record(directory: &Resolving, pool: &str) -> Option<PoolRecord> {
    let mut watch = std::pin::pin!(directory.watch("model_x", None));
    let mut held = None;
    let read = async {
        while let Some(Ok(event)) = watch.next().await {
            match event {
                PoolEvent::Current(record) if record.name == pool => held = Some(record),
                PoolEvent::Synced { .. } => return,
                _ => {}
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the directory's snapshot in time");
    held
}

/// A drained pool says it is `draining` on its health endpoint, its
/// record, and its watch by the time the drain is answered; a chooser
/// stops choosing it, and the other pool takes the requests. With every
/// pool draining, a request fails at once with `Routing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_pool_is_not_chosen() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let m2 = pool(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready), (M2, PoolState::Degraded)]).await;
    ask(&chooser.segment).await.expect("answered");
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (1, 0),
        "the ready pool first"
    );

    let health = m1.readiness.drain().await.expect("the pool drains");
    assert_eq!(health.state, PoolState::Draining);
    assert!(!health.healthy(), "a draining pool's health answers 503");
    let mut watch = m1.readiness.watch().await.expect("the pool's watch");
    let first = tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("the snapshot in time");
    assert!(
        matches!(
            first,
            Some(PoolWatchEvent::Pool {
                state: PoolState::Draining,
                ..
            })
        ),
        "{first:?}"
    );
    let published = tokio::time::timeout(WAIT, async {
        loop {
            let held = record(&directory, M1).await;
            if held.as_ref().is_none_or(|r| r.state == PoolState::Draining) {
                return held;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the record says draining in time");
    assert!(published.is_some(), "the record was withdrawn at once");

    unchosen(&pools, M1).await;
    let before = offered(&[&m1, &m2]);
    ask(&chooser.segment)
        .await
        .expect("the other pool answered");
    assert_eq!(
        offered(&[&m1, &m2])[0],
        before[0],
        "the draining pool was offered a request"
    );
    assert_eq!(m2.forwards(), 1);

    m2.readiness.drain().await.expect("the pool drains");
    unchosen(&pools, M2).await;
    let error = ask(&chooser.segment).await.expect_err("no pool takes work");
    assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// How long an entry instance of these pools takes new work after its
/// pool's drain reaches it: the setup timeout of [`RUNTIME`] and twice
/// the load summary period, [`SUMMARY`].
const BOUND: Duration = Duration::from_secs(3);

/// The longest a test waits for the bound to pass: the bound, and the
/// usual wait for what comes after it.
const PAST_BOUND: Duration = Duration::from_secs(3 + 5);

/// Send a request whose C waits, in the model pool, until the test
/// releases it, and return the request's answer once that C has it: the
/// request is admitted at the pool's entry, and has yet to reach `d`.
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

/// Wait until `instance`, an entry instance, declines offers: its bound
/// has passed since it learned of its pool's drain.
async fn declining<H>(instance: &Running<H>) {
    tokio::time::timeout(PAST_BOUND, async {
        while !instance.segment.declines_offers() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the entry instance declined offers in time");
}

/// Wait until `instance` has closed its entries: it stopped taking work.
async fn closed<H>(instance: &Running<H>, within: Duration) {
    tokio::time::timeout(within, async {
        while !instance.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the instance closed its entries in time");
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

/// The instances of `segment` at `hub` in `phase` now.
async fn now_in(hub: &Hub, segment: &str, phase: SegmentPhase) -> Vec<InstanceId> {
    tokio::time::timeout(
        WAIT,
        client(hub)
            .services()
            .get_instances(&segment_service(segment, phase)),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub answers")
}

/// The instances of `segment` at `hub` in `phase`, as they change: the
/// set as it is, then each change.
async fn phase(
    hub: &Hub,
    segment: &str,
    phase: SegmentPhase,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = ServiceEvent> + Send>> {
    let client = client(hub);
    tokio::time::timeout(
        WAIT,
        client
            .services()
            .watch_instances(&segment_service(segment, phase)),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub serves the watch")
}

/// Whether `events` has shown `instance` in its service so far.
fn shows(
    events: &mut (impl futures::Stream<Item = ServiceEvent> + Unpin),
    instance: InstanceId,
) -> bool {
    use futures::FutureExt;
    let mut shown = false;
    while let Some(Some(event)) = events.next().now_or_never() {
        match event {
            ServiceEvent::Initial(ids) => shown |= ids.contains(&instance),
            ServiceEvent::Added(id) => shown |= id == instance,
            _ => {}
        }
    }
    shown
}

/// Wait until `events` shows `instance` in its service.
async fn shown(
    events: &mut (impl futures::Stream<Item = ServiceEvent> + Unpin),
    instance: InstanceId,
) {
    tokio::time::timeout(WAIT, async {
        loop {
            match events.next().await.expect("the watch goes on") {
                ServiceEvent::Initial(ids) if ids.contains(&instance) => return,
                ServiceEvent::Added(id) if id == instance => return,
                _ => {}
            }
        }
    })
    .await
    .expect("the instance was shown in time");
}

/// The id of `instance`.
fn id<H>(instance: &Running<H>) -> InstanceId {
    instance.node.instance_id()
}

/// The proxy that hides a pool's drain from the hub that follows it hides
/// one its hub sends just after the test turns it on, even when the relay
/// next runs with the change and the hub's bytes both ready, as a busy
/// host leaves it. A current-thread runtime, stepped by hand, makes that
/// so on any host: one turn of its I/O driver sees the hub's bytes and a
/// signal that ends the step, so the relay is woken but does not run; the
/// mode then changes, and the next step runs the relay. Repeated, since a
/// relay that took either first at random would pass a round by chance.
#[test]
fn the_proxy_hides_a_drain_ready_as_its_mode_changes() {
    use std::io::Write as _;
    use tokio::io::AsyncReadExt as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let hub = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let proxy = runtime.block_on(common::proxy::Proxy::new(
        hub.local_addr().expect("an address"),
    ));
    let mut client = runtime
        .block_on(tokio::net::TcpStream::connect(("127.0.0.1", proxy.port)))
        .expect("the proxy accepts");
    let signals = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let signal = runtime
        .block_on(tokio::net::TcpStream::connect(
            signals.local_addr().expect("an address"),
        ))
        .expect("connected");
    let (mut signalling, _) = signals.accept().expect("accepted");
    // The relay connects to the hub once the runtime runs it.
    hub.set_nonblocking(true).expect("nonblocking");
    let (mut sent, _) = runtime.block_on(async {
        tokio::time::timeout(WAIT, async {
            loop {
                match hub.accept() {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    accepted => return accepted.expect("accepted"),
                }
            }
        })
        .await
        .expect("the relay connected in time")
    });
    sent.set_nonblocking(false).expect("blocking");

    for round in 0..20 {
        proxy.set(Mode::Forward);
        // The relay, queued by the change, runs before the timer fires: it
        // takes the change and waits on both again.
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(1)).await });
        sent.write_all(b"\"draining\"").expect("the hub sends");
        signalling.write_all(b"x").expect("signalled");
        runtime.block_on(async {
            let mut byte = [0u8; 1];
            loop {
                signal.readable().await.expect("readable");
                match signal.try_read(&mut byte) {
                    Ok(1) => break,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    other => panic!("the signal: {other:?}"),
                }
            }
            // Read to the end of what came, so that the next round's
            // signal is a new readiness, seen only by a turn of the driver.
            assert!(signal.try_read(&mut byte).is_err());
        });
        proxy.set(Mode::Undrained);
        let mut got = [0u8; 10];
        runtime
            .block_on(async { tokio::time::timeout(WAIT, client.read_exact(&mut got)).await })
            .expect("relayed in time")
            .expect("relayed");
        assert_eq!(
            &got,
            b"\"ready\"   ",
            "round {round}: {}",
            String::from_utf8_lossy(&got)
        );
    }
}

/// A chooser whose view of a pool is stale offers to it after the pool
/// began to drain: the pool's entry takes the offer until the bound has
/// passed since it learned of the drain, and declines it after, and the
/// chooser offers the request to another pool. The chooser follows the
/// draining pool through a proxy that hides the drain from its hub but
/// passes on the pool's load summaries, as a follow link that lags would
/// for a while.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offer_within_the_bound_is_taken_and_one_after_it_is_declined() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (e1, quiet) = (Events::new(), Events::quiet());
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = model_proxied(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        RUNTIME,
        &|deployment| reference_graph(deployment, &e1),
    )
    .await;
    let m2 = pool(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready), (M2, PoolState::Degraded)]).await;
    proxy.set(Mode::Undrained);
    // Held at the entry, so that the entry is still there once it stops
    // taking work. Taken before the drain, so that the only offer that
    // must arrive within the bound is the one asked for next.
    let held = gated(&chooser, &e1, "held").await;

    let started = Instant::now();
    m1.readiness.drain().await.expect("the pool drains");
    ask(&chooser.segment)
        .await
        .expect("the draining pool answered");
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (2, 0),
        "taken within the bound"
    );
    declining(&m1.entries[0]).await;
    assert!(
        started.elapsed() >= BOUND,
        "the entry stopped taking work {:?} after the drain began",
        started.elapsed()
    );
    assert!(
        followed(&pools, "f", M1)
            .is_some_and(|m| m.state == PoolState::Ready && m.eligible.len() == 1),
        "the chooser's view of the pool is not stale"
    );

    let before = (offers(&m1), offers(&m2));
    ask(&chooser.segment)
        .await
        .expect("the other pool answered");
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (before.0 + 1, before.1 + 1),
        "not offered to the draining pool, then the other"
    );
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (2, 1),
        "declined past the bound"
    );
    release(&e1, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A draining pool's decline tells a stale chooser that the pool is gone,
/// not full: the chooser offers it none of the requests after it, also
/// once a damping window has passed, while its view of the pool is still
/// the stale one. The proxy hides the drain from the chooser's hub, as a
/// follow link that lags would. Besides the bound, it waits out the first
/// damping window (1 s), which is what it proves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_pools_decline_keeps_a_stale_chooser_away() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (e1, quiet) = (Events::new(), Events::quiet());
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let (m1, proxy) = model_proxied(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        RUNTIME,
        &|deployment| reference_graph(deployment, &e1),
    )
    .await;
    let m2 = pool(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready), (M2, PoolState::Degraded)]).await;
    proxy.set(Mode::Undrained);

    m1.readiness.drain().await.expect("the pool drains");
    // Held at the entry, so that the entry is still there once it stops
    // taking work.
    let held = gated(&chooser, &e1, "held").await;
    declining(&m1.entries[0]).await;
    let before = (offers(&m1), offers(&m2));
    ask(&chooser.segment)
        .await
        .expect("the other pool answered");
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (before.0 + 1, before.1 + 1),
        "declined by the draining pool, then taken by the other"
    );

    // Past the first damping window, which a full pool's decline opens.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let stale =
        |m: &gdl_runtime::FollowedPool| m.state == PoolState::Ready && m.eligible.len() == 1;
    let m = followed(&pools, "f", M1).expect("followed");
    assert!(stale(&m), "the chooser's view of the pool is not stale");
    assert!(m.gone && m.damped.is_none(), "{m:?}");
    for _ in 0..4 {
        ask(&chooser.segment)
            .await
            .expect("the other pool answered");
    }
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (before.0 + 1, before.1 + 5),
        "the draining pool was offered requests again"
    );
    release(&e1, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered");
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry instance declines offers by its segment's own policy, as gone
/// or as full, with no drain, and counts each as a refusal in its load. A chooser passes a gone pool over, also once
/// the entry takes offers again, since its follow has not changed. It
/// damps a full pool, and a lone full pool takes the request again once
/// the entry takes offers again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_declines_by_its_own_policy_as_gone_or_full() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let m2 = pool(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready), (M2, PoolState::Degraded)]).await;

    m1.entries[0].segment.decline_offers_as(Decline::Gone);
    ask(&chooser.segment)
        .await
        .expect("the other pool answered");
    assert_eq!((offers(&m1), offers(&m2)), (1, 1));
    assert_eq!((m1.forwards(), m2.forwards()), (0, 1));
    assert_eq!(
        m1.entries[0].segment.load().declined,
        1,
        "a refusal to report"
    );
    let m = followed(&pools, "f", M1).expect("followed");
    assert!(m.gone && m.state == PoolState::Ready, "{m:?}");
    m1.entries[0].segment.take_offers();
    for _ in 0..3 {
        ask(&chooser.segment)
            .await
            .expect("the other pool answered");
    }
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (1, 4),
        "a gone pool was offered work with its follow unchanged"
    );

    m2.entries[0].segment.decline_offers_as(Decline::Full);
    let error = ask(&chooser.segment)
        .await
        .expect_err("no pool took the request");
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    assert_eq!(offers(&m2), 5);
    let m = followed(&pools, "f", M2).expect("followed");
    assert!(!m.gone && m.damped.is_some(), "{m:?}");
    m2.entries[0].segment.take_offers();
    ask(&chooser.segment)
        .await
        .expect("the lone full pool answered");
    assert_eq!((m1.forwards(), m2.forwards()), (0, 5));
    assert_eq!(followed(&pools, "f", M2).expect("followed").damped, None);
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A request whose only pools are gone fails with `Routing`, also the one
/// that hears the last gone decline, as every request after it does. A
/// request that a pool declined as full, the control, fails with
/// `Overloaded`, also when the pool it was offered next declined as gone:
/// the full pool takes the next request once it has room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_request_that_hears_the_last_gone_decline_fails_with_routing() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let m2 = pool(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready), (M2, PoolState::Degraded)]).await;

    m1.entries[0].segment.decline_offers_as(Decline::Full);
    m2.entries[0].segment.decline_offers_as(Decline::Gone);
    let full = ask(&chooser.segment)
        .await
        .expect_err("no pool took the request");
    assert_eq!((offers(&m1), offers(&m2)), (1, 1));
    let shown = (
        followed(&pools, "f", M1).expect("followed"),
        followed(&pools, "f", M2).expect("followed"),
    );
    assert!(shown.0.damped.is_some() && shown.1.gone, "{shown:?}");
    m1.entries[0].segment.take_offers();
    ask(&chooser.segment)
        .await
        .expect("the lone full pool answered");

    m1.entries[0].segment.decline_offers_as(Decline::Gone);
    let first = ask(&chooser.segment)
        .await
        .expect_err("the last pool declined as gone");
    assert_eq!((offers(&m1), offers(&m2)), (3, 1));
    assert!(followed(&pools, "f", M1).expect("followed").gone);
    let next = ask(&chooser.segment).await.expect_err("every pool is gone");
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (3, 1),
        "a gone pool was offered"
    );
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }

    assert_eq!(full.kind(), ErrorKind::Overloaded, "the control: {full}");
    assert_eq!(next.kind(), ErrorKind::Routing, "the control: {next}");
    assert_eq!(
        first.kind(),
        ErrorKind::Routing,
        "the request that heard the last gone decline: {first}"
    );
    assert_eq!(first.to_string(), next.to_string());
}

/// The last gone refusal must also produce Routing when it uses the
/// final permitted failover: the next request already sees every pool gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_gone_decline_at_the_failover_limit_is_routing() {
    const M3: &str = "model-x-03-us-east-1";
    assert_eq!(RuntimeConfig::default().pool_failovers, 2);
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let m2 = pool(&spec, &directory, ready(M2, "m2-hub:1337"), &quiet).await;
    let m3 = pool(&spec, &directory, ready(M3, "m3-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(
        &pools,
        &[
            (M1, PoolState::Ready),
            (M2, PoolState::Ready),
            (M3, PoolState::Ready),
        ],
    )
    .await;
    for model in [&m1, &m2, &m3] {
        model.entries[0].segment.decline_offers_as(Decline::Gone);
    }
    let first = ask(&chooser.segment)
        .await
        .expect_err("all three pools declined as gone");
    assert_eq!(offered(&[&m1, &m2, &m3]), [1, 1, 1]);
    for name in [M1, M2, M3] {
        assert!(followed(&pools, "f", name).expect("followed").gone);
    }
    let next = ask(&chooser.segment)
        .await
        .expect_err("the next request has no pool");
    assert_eq!(next.kind(), ErrorKind::Routing, "the control: {next}");
    assert_eq!(offered(&[&m1, &m2, &m3]), [1, 1, 1]);
    for hub in [hub, m1.hub, m2.hub, m3.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
    assert_eq!(
        first.kind(),
        ErrorKind::Routing,
        "the request that heard the final permitted gone refusal: {first}"
    );
}

/// A node in the global pool reads a pool its chooser holds gone as gone,
/// while its state is `ready`, its entry instance is eligible, and fresh
/// load summaries arrive: the chooser passes it over, so a gate in front
/// of it answers it as not ready. A pool the chooser does not hold gone
/// reads as not gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_reads_a_pool_gone_by_its_own_policy_as_gone() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let node = FollowedPools::new();
    let chooser = reading_chooser(&spec, &hub, &node).await;
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready)]).await;
    let gone = || {
        node.read("model_x", |pools| pools.is_gone(M1))
            .expect("the chooser sends to model_x")
    };
    ask(&chooser.segment).await.expect("answered");
    assert!(!gone(), "a pool that takes work read gone");

    m1.entries[0].segment.decline_offers_as(Decline::Gone);
    ask(&chooser.segment)
        .await
        .expect_err("the only pool declined as gone");
    let marked = Instant::now();
    until(&pools, "a load summary made after the mark", |pools| {
        followed(pools, "f", M1)
            .and_then(|m| m.load_age)
            .is_some_and(|age| age < marked.elapsed())
    })
    .await;
    let m = followed(&pools, "f", M1).expect("followed");
    assert!(
        m.gone && m.state == PoolState::Ready && m.eligible.len() == 1,
        "{m:?}"
    );
    assert!(
        gone(),
        "the node reads a pool the chooser passes over as not gone"
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry instance declines offers as its own, by its segment's own
/// policy: each request it is offered goes to the other entry instance of
/// its pool, which takes it, and never to the other pool. Its pool is
/// neither damped nor held gone, and the instance's load report counts no
/// refusal, which would read as a saturated pool under `home_first`. Once
/// the instance takes offers again, round robin offers it requests again,
/// and it takes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_that_declines_as_its_own_leaves_the_request_in_its_pool() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(
        &spec,
        &directory,
        Model {
            entries: 2,
            ..ready(M1, "m1-hub:1337")
        },
        &quiet,
    )
    .await;
    let m2 = pool(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    until(&pools, "the model pools followed", |pools| {
        followed(pools, "f", M1)
            .is_some_and(|m| m.state == PoolState::Ready && m.eligible.len() == 2)
            && followed(pools, "f", M2).is_some_and(|m| m.state == PoolState::Degraded)
    })
    .await;

    let (own, other) = (&m1.entries[0], &m1.entries[1]);
    own.segment.decline_offers_as(Decline::Instance);
    for _ in 0..4 {
        ask(&chooser.segment)
            .await
            .expect("the other instance answered");
    }
    assert_eq!((own.forwards(), other.forwards(), m2.forwards()), (0, 4, 0));
    assert_eq!(
        own.offered(),
        2,
        "round robin offers the declining instance every other request first"
    );
    assert_eq!(offers(&m2), 0, "the other pool was offered a request");
    let m = followed(&pools, "f", M1).expect("followed");
    assert!(!m.gone && m.damped.is_none(), "{m:?}");
    assert_eq!(
        own.segment.load().declined,
        0,
        "the instance's own declines count as its pool's refusals in its load report"
    );

    own.segment.take_offers();
    for _ in 0..4 {
        ask(&chooser.segment).await.expect("the pool answered");
    }
    assert_eq!((own.forwards(), other.forwards(), m2.forwards()), (2, 6, 0));
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry instance stops taking new work at its bound even when its
/// serving body is slow to finish once told to stop: the offer of a stale
/// chooser after the bound is declined, and the other pool answers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_declines_after_its_bound_while_its_body_finishes() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (e1, quiet) = (Events::new(), Events::quiet());
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    // A drain bound longer than the test, so that it does not cut the
    // body short.
    let (m1, proxy) = model_proxied(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 60000\n",
        &|deployment| reference_graph(deployment, &e1),
    )
    .await;
    let m2 = pool(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready), (M2, PoolState::Degraded)]).await;
    proxy.set(Mode::Undrained);
    let entry = &m1.entries[0];
    entry.body.send_replace(true);

    m1.readiness.drain().await.expect("the pool drains");
    ask(&chooser.segment)
        .await
        .expect("the draining pool answered");
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (1, 0),
        "taken within the bound"
    );
    declining(entry).await;
    let before = (offers(&m1), offers(&m2));
    ask(&chooser.segment)
        .await
        .expect("the other pool answered");
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (before.0 + 1, before.1 + 1),
        "not offered to the draining pool, then the other"
    );
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (1, 1),
        "taken past the bound"
    );
    entry.body.send_replace(false);
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The entry and C, cut apart inside the model pool: `f` runs B, D, E,
/// and F, and `d` runs C, so a request the entry took crosses to `d` and
/// back into `f` for D.
fn through_the_interior() -> gdl_runtime::Placement {
    gdl_runtime::Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "D", "E", "F"])
        .segment("d", ["C"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"])
}

/// An entry instance that stops taking offers at its bound still takes
/// the hand-offs inside its pool that a request it took makes: a request
/// held at C, in `d`, past the bound, comes back into `f` for D and is
/// answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_takes_the_continuations_of_its_requests_past_its_bound() {
    let spec = spec_of(through_the_interior());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
    following(&tables(&chooser), &[(M1, PoolState::Ready)]).await;
    ask(&chooser.segment).await.expect("answered");
    let held = gated(&chooser, &events, "held").await;

    m1.readiness.drain().await.expect("the pool drains");
    let entry = &m1.entries[0];
    declining(entry).await;
    assert_eq!(entry.segment.admitted(), 1, "the request is admitted");
    assert!(
        !entry.segment.is_closed(),
        "the entry closed with a request"
    );
    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the request came back into the entry and was answered");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// Hands D to any entry instance but the one `avoid` names, by its id,
/// once it names one.
#[derive(Clone, Default)]
struct Elsewhere {
    avoid: Arc<std::sync::Mutex<Option<u128>>>,
}

impl gdl::Selector for Elsewhere {
    type Head = gdl::Encoded;

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, gdl::Encoded>,
        instances: gdl::Instances<'a>,
    ) -> gdl::Result<gdl::Choice<'a>> {
        let avoid = *self.avoid.lock().expect("lock");
        let chosen = instances
            .iter()
            .find(|instance| Some(instance.id().as_u128()) != avoid)
            .or_else(|| instances.get(0))
            .expect("never empty");
        Ok(gdl::Choice::new(chosen))
    }
}

/// An entry instance that took no request of its own still takes, past
/// its bound and once it has announced that it drained, the hand-offs
/// inside its pool that a request another entry instance took makes: the
/// pool's entry instances are its drain sources too. Here `d` hands D to
/// the entry instance that did not take the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_takes_the_continuations_of_another_entrys_requests() {
    let spec =
        spec_of(through_the_interior().route("D", RequestRouter::Selector("elsewhere".into())));
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let elsewhere = Elsewhere::default();
    let configure = |segment: &str, runtime: RuntimeConfig| match segment {
        "d" => {
            let mut selectors = Selectors::new();
            selectors
                .add("elsewhere", elsewhere.clone())
                .expect("a selector");
            RuntimeConfig {
                selectors,
                ..runtime
            }
        }
        _ => runtime,
    };
    let two = Model {
        entries: 2,
        ..ready(M1, "m1-hub:1337")
    };
    let m1 = model_configured(&spec, &directory, two, &configure, &|deployment| {
        reference_graph(deployment, &events)
    })
    .await;
    until(
        &tables(&chooser),
        "both entry instances followed",
        |pools| followed(pools, "f", M1).is_some_and(|m| m.eligible.len() == 2 && m.load.is_some()),
    )
    .await;
    let held = gated(&chooser, &events, "held").await;
    let (taker, other) = match m1.entries[0].segment.admitted() {
        1 => (&m1.entries[0], &m1.entries[1]),
        _ => (&m1.entries[1], &m1.entries[0]),
    };
    *elsewhere.avoid.lock().expect("lock") = Some(id(taker).as_u128());
    let mut drained = phase(&m1.hub, "f", SegmentPhase::Drained).await;

    m1.readiness.drain().await.expect("the pool drains");
    shown(&mut drained, id(other)).await;
    // An entry instance that closed once it had drained itself would close
    // within a question to the hub; nothing shows that one that waits for
    // the others will not, so it is given several of its questions' time.
    let early = tokio::time::timeout(Duration::from_millis(500), async {
        while !other.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    assert!(
        early.await.is_err(),
        "an entry instance closed while another holds a request"
    );
    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the other entry instance took D and the request was answered");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A request the entry took before its pool drained is answered after the
/// entry stopped taking work, and the entry announces that it has drained
/// only once it holds no request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_announces_drained_once_its_requests_are_answered() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
    following(&tables(&chooser), &[(M1, PoolState::Ready)]).await;
    let held = gated(&chooser, &events, "held").await;
    let mut drained = phase(&m1.hub, "f", SegmentPhase::Drained).await;

    m1.readiness.drain().await.expect("the pool drains");
    let entry = &m1.entries[0];
    declining(entry).await;
    assert_eq!(entry.segment.admitted(), 1, "the request admitted before");
    assert!(
        !shows(&mut drained, id(entry)),
        "the entry announced drained while it holds a request"
    );
    release(&events, "held");
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the held request answered in time")
        .expect("the request's task")
        .expect("the held request answered after the entry stopped taking work");
    shown(&mut drained, id(entry)).await;
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An interior instance of a draining pool serves while an entry instance
/// still holds a request that will call it, even once the entry stopped
/// taking work, and drains only after the entry has drained: on its own,
/// or when it is stopped while the pool drains, since the pool's entry
/// instances are its drain sources then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interior_instance_serves_until_the_entry_instances_have_drained() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    for stopped in [false, true] {
        let events = Events::new();
        let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
        following(&tables(&chooser), &[(M1, PoolState::Ready)]).await;
        let held = gated(&chooser, &events, "held").await;
        let mut retired = phase(&m1.hub, "d", SegmentPhase::Retired).await;

        m1.readiness.drain().await.expect("the pool drains");
        declining(&m1.entries[0]).await;
        let sink = &m1.sink;
        assert!(!sink.segment.is_closed(), "the interior instance closed");
        assert!(
            !shows(&mut retired, id(sink)),
            "the interior instance retired while an entry instance serves"
        );
        if stopped {
            // The sink learned of the drain from its hub's watch, as the
            // entry did three seconds before.
            sink.process.stop();
            shown(&mut retired, id(sink)).await;
            // An instance that counted the sources outside its pool would
            // close within a question to the hub of retiring; nothing
            // shows that one which counts the entry instances will not,
            // so it is given several of its own questions' time.
            let early = tokio::time::timeout(Duration::from_millis(500), async {
                while !sink.segment.is_closed() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            assert!(
                early.await.is_err(),
                "the stopped interior instance closed while an entry instance holds a request"
            );
        }
        release(&events, "held");
        tokio::time::timeout(WAIT, held)
            .await
            .expect("the held request answered in time")
            .expect("the request's task")
            .expect("the interior instance served the request");
        // The sink closes only once its own question to the hub found the
        // entry drained or gone, and an entry's `drained` stays until it
        // leaves, which takes it out of `serving` too.
        closed(sink, WAIT).await;
        let entry = id(&m1.entries[0]);
        assert!(
            now_in(&m1.hub, "f", SegmentPhase::Drained)
                .await
                .contains(&entry)
                || !now_in(&m1.hub, "f", SegmentPhase::Serving)
                    .await
                    .contains(&entry),
            "the interior instance closed before the entry instance drained or left"
        );
        let ModelPool {
            hub: model_hub,
            entries,
            sink,
            ..
        } = m1;
        for entry in entries {
            entry.ended(WAIT).await.expect("the entry drained");
        }
        sink.ended(WAIT)
            .await
            .expect("the interior instance drained");
        model_hub.shutdown().await.expect("valid test fixture");
        until(&tables(&chooser), "the drained pool dropped", |pools| {
            followed(pools, "f", M1).is_none()
        })
        .await;
    }
    hub.shutdown().await.expect("valid test fixture");
}

/// Once every instance of a draining pool has drained and left, on its
/// own, the pool's hub withdraws its record, and the global pool's hub
/// that followed it drops it from its relay, so that its choosers no
/// longer follow it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drained_pool_is_withdrawn_once_its_instances_have_left() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global): (Hub, Arc<PoolReadiness<Resolving>>) =
        pool_hub_of(&spec, global_hub(), &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready)]).await;
    ask(&chooser.segment).await.expect("answered");

    m1.readiness.drain().await.expect("the pool drains");
    let ModelPool {
        hub: model_hub,
        entries,
        sink,
        ..
    } = m1;
    for entry in entries {
        entry.ended(PAST_BOUND).await.expect("the entry drained");
    }
    sink.ended(WAIT)
        .await
        .expect("the interior instance drained");
    let withdrawn = tokio::time::timeout(WAIT, async {
        while record(&directory, M1).await.is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        withdrawn.is_ok(),
        "the drained pool's record was not withdrawn"
    );
    let mut following = global.following();
    tokio::time::timeout(
        WAIT,
        following.wait_for(|pools| !pools.keys().any(|(_, pool)| pool == M1)),
    )
    .await
    .expect("the global hub dropped the drained pool in time")
    .expect("the extension runs");
    until(&pools, "the chooser no longer follows the pool", |pools| {
        followed(pools, "f", M1).is_none()
    })
    .await;
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// What a node in the global pool reads of the model pool `pool`: its
/// state, if its chooser follows it, and whether it is withdrawn.
fn read(followed: &FollowedPools, pool: &str) -> (Option<PoolState>, bool) {
    followed
        .read("model_x", |pools| {
            (
                pools.pool(pool).map(gdl::Pool::state),
                pools.is_withdrawn(pool),
            )
        })
        .expect("the chooser sends to model_x")
}

/// A node in the global pool reads each model pool as its chooser holds
/// it. A pool that drains reads `ready`, then `draining` while a request
/// the chooser sent it holds its follow, then withdrawn once its
/// instances have left and its hub has withdrawn its record. A pool whose
/// hub stops without a drain reads as neither followed nor withdrawn: its
/// lease runs out, and silence is never a decommission. A chooser that
/// starts after the withdrawal never saw the pool, and reads it so too:
/// a gate answers it as not available (503), not as removed (404). The
/// drain waits out the entry's bound, so this test takes several seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_reads_a_drained_pool_as_withdrawn_and_a_stopped_one_as_not() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global): (Hub, Arc<PoolReadiness<Resolving>>) =
        pool_hub_of(&spec, global_hub(), &directory).await;
    let node = FollowedPools::new();
    let chooser = reading_chooser(&spec, &hub, &node).await;
    assert!(node.is_bound(), "the launch binds the handle");
    let m1 = pool(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
    let pools = tables(&chooser);
    following(&pools, &[(M1, PoolState::Ready)]).await;
    assert_eq!(read(&node, M1), (Some(PoolState::Ready), false));

    let held = gated(&chooser, &events, "held").await;
    m1.readiness.drain().await.expect("the pool drains");
    until(&pools, "the node reads the pool draining", |_| {
        read(&node, M1) == (Some(PoolState::Draining), false)
    })
    .await;
    release(&events, "held");
    held.await
        .expect("the request ran")
        .expect("the request was answered");
    let ModelPool {
        hub: m1_hub,
        entries,
        sink,
        ..
    } = m1;
    for entry in entries {
        entry.ended(PAST_BOUND).await.expect("the entry drained");
    }
    sink.ended(WAIT)
        .await
        .expect("the interior instance drained");
    until(&pools, "the node reads the pool withdrawn", |_| {
        read(&node, M1) == (None, true)
    })
    .await;

    let m2 = pool(&spec, &directory, ready(M2, "m2-hub:1337"), &events).await;
    following(&pools, &[(M2, PoolState::Ready)]).await;
    assert_eq!(read(&node, M2), (Some(PoolState::Ready), false));
    // Once it follows the pool that came after, the late chooser has read
    // its hub's relay, and the withdrawal is not in it.
    let late = FollowedPools::new();
    let later = reading_chooser(&spec, &hub, &late).await;
    following(&tables(&later), &[(M2, PoolState::Ready)]).await;
    assert_eq!(read(&late, M1), (None, false), "never seen");

    m2.hub.shutdown().await.expect("valid test fixture");
    until(&pools, "the stopped pool dropped", |pools| {
        followed(pools, "f", M2).is_none()
    })
    .await;
    assert_eq!(read(&node, M2), (None, false), "a stop is not a withdrawal");
    assert_eq!(read(&node, M1), (None, true));
    for hub in [hub, m1_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
