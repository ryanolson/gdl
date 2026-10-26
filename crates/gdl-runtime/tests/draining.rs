//! A model cell drains as a whole: its hub says it is `draining`, and the
//! choosers that follow it stop choosing it; its entry instances take the
//! offers that reach them for a bound, then decline new ones, finish what
//! they took, and announce that they have drained; its interior instances
//! serve until every entry instance has drained or left, then drain; and
//! once every instance has left, the hub withdraws the cell's record. An
//! entry instance can also decline offers by its own policy, with no drain.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed. A test that watches an entry instance stop taking work runs
//! for at least its bound, 3 s at the shortest settings a cell allows (a
//! summary period of 1 s), so these tests take several seconds each.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::boundary::{
    Model, ModelCell, RUNTIME, Running, ask, model_configured, model_of, model_proxied, placement,
    run_graph, spec_of,
};
use common::cells::{Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until};
use common::proxy::Mode;
use common::{Chunk, Events, Plan, Prompt, WAIT, collect, expected, prompt, reference_graph};
use futures::StreamExt;
use gdl::{Cx, ErrorKind, FollowedCells, Next, Selectors};
use gdl_cells::{CellDirectory, CellEvent, CellRecord};
use gdl_hub::CellReadiness;
use gdl_model::{SegmentPhase, segment_service};
use gdl_runtime::{
    CellState, Cells, Decline, Deployment, DeploymentSpec, Register, RequestRouter, RuntimeConfig,
};
use gdl_watch::CellWatchEvent;
use tokio::task::JoinHandle;
use velo::InstanceId;
use velo::discovery::{ServiceDiscovery, ServiceEvent};
use velo_hub::Hub;
use velo_hub::client::HubClient;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// A `ready` cell: one entry instance makes its quorum.
fn ready(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries: 1,
    }
}

/// A `degraded` cell: two entry instances make its quorum; it has one.
/// A chooser offers to it only when no ready cell takes the request.
fn degraded(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 2,
        entries: 1,
    }
}

/// The global cell's hub.
async fn global_hub_of(spec: &DeploymentSpec, directory: &Resolving) -> Hub {
    cell_hub_of(spec, global_hub(), directory).await.0
}

/// A chooser, an instance of `a` in the global cell at `hub`.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    reading_chooser(spec, hub, &FollowedCells::new()).await
}

/// A chooser whose nodes read the cells it follows through `followed`.
async fn reading_chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
    followed: &FollowedCells,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_cell());
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

/// A model cell of the reference graph whose nodes log to `events`.
async fn cell(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    events: &Arc<Events>,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    model_of(spec, directory, cell, &|runtime| runtime, &|deployment| {
        reference_graph(deployment, events)
    })
    .await
}

/// The chooser's tables.
fn tables<H>(chooser: &Running<H>) -> Cells {
    chooser
        .cells
        .clone()
        .expect("a global instance in a cell chooses")
}

/// Wait until the chooser follows each of `cells` in its state, with one
/// entry instance and a load summary.
async fn following(cells: &Cells, expected: &[(&str, CellState)]) {
    until(cells, "the model cells followed", |cells| {
        expected.iter().all(|(cell, state)| {
            followed(cells, "f", cell)
                .is_some_and(|m| m.state == *state && m.eligible.len() == 1 && m.load.is_some())
        })
    })
    .await;
}

/// Wait until the chooser no longer has `cell` to choose: it follows it
/// `draining`, or no longer follows it.
async fn unchosen(cells: &Cells, cell: &str) {
    until(cells, "the cell drained", |cells| {
        followed(cells, "f", cell).is_none_or(|m| m.state == CellState::Draining)
    })
    .await;
}

/// The forward hand-offs that reached each of `cells`, taken or refused.
fn offered<H>(cells: &[&ModelCell<H>]) -> Vec<usize> {
    cells
        .iter()
        .map(|cell| cell.entries.iter().map(Running::offered).sum())
        .collect()
}

/// The forward hand-offs that reached `cell`, taken or refused.
fn offers<H>(cell: &ModelCell<H>) -> usize {
    cell.entries.iter().map(Running::offered).sum()
}

/// The record `directory` holds for the model cell `cell`, if any.
async fn record(directory: &Resolving, cell: &str) -> Option<CellRecord> {
    let mut watch = std::pin::pin!(directory.watch("model_x", None));
    let mut held = None;
    let read = async {
        while let Some(Ok(event)) = watch.next().await {
            match event {
                CellEvent::Current(record) if record.name == cell => held = Some(record),
                CellEvent::Synced { .. } => return,
                _ => {}
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the directory's snapshot in time");
    held
}

/// A drained cell says it is `draining` on its health endpoint, its
/// record, and its watch by the time the drain is answered; a chooser
/// stops choosing it, and the other cell takes the requests. With every
/// cell draining, a request fails at once with `Routing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_cell_is_not_chosen() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let m2 = cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready), (M2, CellState::Degraded)]).await;
    ask(&chooser.segment).await.expect("answered");
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (1, 0),
        "the ready cell first"
    );

    let health = m1.readiness.drain().await.expect("the cell drains");
    assert_eq!(health.state, CellState::Draining);
    assert!(!health.healthy(), "a draining cell's health answers 503");
    let mut watch = m1.readiness.watch().await.expect("the cell's watch");
    let first = tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("the snapshot in time");
    assert!(
        matches!(
            first,
            Some(CellWatchEvent::Cell {
                state: CellState::Draining,
                ..
            })
        ),
        "{first:?}"
    );
    let published = tokio::time::timeout(WAIT, async {
        loop {
            let held = record(&directory, M1).await;
            if held.as_ref().is_none_or(|r| r.state == CellState::Draining) {
                return held;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the record says draining in time");
    assert!(published.is_some(), "the record was withdrawn at once");

    unchosen(&cells, M1).await;
    let before = offered(&[&m1, &m2]);
    ask(&chooser.segment)
        .await
        .expect("the other cell answered");
    assert_eq!(
        offered(&[&m1, &m2])[0],
        before[0],
        "the draining cell was offered a request"
    );
    assert_eq!(m2.forwards(), 1);

    m2.readiness.drain().await.expect("the cell drains");
    unchosen(&cells, M2).await;
    let error = ask(&chooser.segment).await.expect_err("no cell takes work");
    assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// How long an entry instance of these cells takes new work after its
/// cell's drain reaches it: the setup timeout of [`RUNTIME`] and twice
/// the load summary period, [`SUMMARY`].
const BOUND: Duration = Duration::from_secs(3);

/// The longest a test waits for the bound to pass: the bound, and the
/// usual wait for what comes after it.
const PAST_BOUND: Duration = Duration::from_secs(3 + 5);

/// Send a request whose C waits, in the model cell, until the test
/// releases it, and return the request's answer once that C has it: the
/// request is admitted at the cell's entry, and has yet to reach `d`.
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
/// has passed since it learned of its cell's drain.
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

/// The proxy that hides a cell's drain from the hub that follows it hides
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

/// A chooser whose view of a cell is stale offers to it after the cell
/// began to drain: the cell's entry takes the offer until the bound has
/// passed since it learned of the drain, and declines it after, and the
/// chooser offers the request to another cell. The chooser follows the
/// draining cell through a proxy that hides the drain from its hub but
/// passes on the cell's load summaries, as a follow link that lags would
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
    let m2 = cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready), (M2, CellState::Degraded)]).await;
    proxy.set(Mode::Undrained);
    // Held at the entry, so that the entry is still there once it stops
    // taking work. Taken before the drain, so that the only offer that
    // must arrive within the bound is the one asked for next.
    let held = gated(&chooser, &e1, "held").await;

    let started = Instant::now();
    m1.readiness.drain().await.expect("the cell drains");
    ask(&chooser.segment)
        .await
        .expect("the draining cell answered");
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
        followed(&cells, "f", M1)
            .is_some_and(|m| m.state == CellState::Ready && m.eligible.len() == 1),
        "the chooser's view of the cell is not stale"
    );

    let before = (offers(&m1), offers(&m2));
    ask(&chooser.segment)
        .await
        .expect("the other cell answered");
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (before.0 + 1, before.1 + 1),
        "not offered to the draining cell, then the other"
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

/// A draining cell's decline tells a stale chooser that the cell is gone,
/// not full: the chooser offers it none of the requests after it, also
/// once a damping window has passed, while its view of the cell is still
/// the stale one. The proxy hides the drain from the chooser's hub, as a
/// follow link that lags would. Besides the bound, it waits out the first
/// damping window (1 s), which is what it proves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_cells_decline_keeps_a_stale_chooser_away() {
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
    let m2 = cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready), (M2, CellState::Degraded)]).await;
    proxy.set(Mode::Undrained);

    m1.readiness.drain().await.expect("the cell drains");
    // Held at the entry, so that the entry is still there once it stops
    // taking work.
    let held = gated(&chooser, &e1, "held").await;
    declining(&m1.entries[0]).await;
    let before = (offers(&m1), offers(&m2));
    ask(&chooser.segment)
        .await
        .expect("the other cell answered");
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (before.0 + 1, before.1 + 1),
        "declined by the draining cell, then taken by the other"
    );

    // Past the first damping window, which a full cell's decline opens.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let stale =
        |m: &gdl_runtime::FollowedCell| m.state == CellState::Ready && m.eligible.len() == 1;
    let m = followed(&cells, "f", M1).expect("followed");
    assert!(stale(&m), "the chooser's view of the cell is not stale");
    assert!(m.gone && m.damped.is_none(), "{m:?}");
    for _ in 0..4 {
        ask(&chooser.segment)
            .await
            .expect("the other cell answered");
    }
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (before.0 + 1, before.1 + 5),
        "the draining cell was offered requests again"
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
/// or as full, with no drain. A chooser passes a gone cell over, also once
/// the entry takes offers again, since its follow has not changed. It
/// damps a full cell, and a lone full cell takes the request again once
/// the entry takes offers again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_declines_by_its_own_policy_as_gone_or_full() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let m2 = cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready), (M2, CellState::Degraded)]).await;

    m1.entries[0].segment.decline_offers_as(Decline::Gone);
    ask(&chooser.segment)
        .await
        .expect("the other cell answered");
    assert_eq!((offers(&m1), offers(&m2)), (1, 1));
    assert_eq!((m1.forwards(), m2.forwards()), (0, 1));
    let m = followed(&cells, "f", M1).expect("followed");
    assert!(m.gone && m.state == CellState::Ready, "{m:?}");
    m1.entries[0].segment.take_offers();
    for _ in 0..3 {
        ask(&chooser.segment)
            .await
            .expect("the other cell answered");
    }
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (1, 4),
        "a gone cell was offered work with its follow unchanged"
    );

    m2.entries[0].segment.decline_offers_as(Decline::Full);
    let error = ask(&chooser.segment)
        .await
        .expect_err("no cell took the request");
    assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
    assert_eq!(offers(&m2), 5);
    let m = followed(&cells, "f", M2).expect("followed");
    assert!(!m.gone && m.damped.is_some(), "{m:?}");
    m2.entries[0].segment.take_offers();
    ask(&chooser.segment)
        .await
        .expect("the lone full cell answered");
    assert_eq!((m1.forwards(), m2.forwards()), (0, 5));
    assert_eq!(followed(&cells, "f", M2).expect("followed").damped, None);
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry instance stops taking new work at its bound even when its
/// serving body is slow to finish once told to stop: the offer of a stale
/// chooser after the bound is declined, and the other cell answers it.
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
    let m2 = cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready), (M2, CellState::Degraded)]).await;
    proxy.set(Mode::Undrained);
    let entry = &m1.entries[0];
    entry.body.send_replace(true);

    m1.readiness.drain().await.expect("the cell drains");
    ask(&chooser.segment)
        .await
        .expect("the draining cell answered");
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (1, 0),
        "taken within the bound"
    );
    declining(entry).await;
    let before = (offers(&m1), offers(&m2));
    ask(&chooser.segment)
        .await
        .expect("the other cell answered");
    assert_eq!(
        (offers(&m1), offers(&m2)),
        (before.0 + 1, before.1 + 1),
        "not offered to the draining cell, then the other"
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

/// The entry and C, cut apart inside the model cell: `f` runs B, D, E,
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
/// the hand-offs inside its cell that a request it took makes: a request
/// held at C, in `d`, past the bound, comes back into `f` for D and is
/// answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_takes_the_continuations_of_its_requests_past_its_bound() {
    let spec = spec_of(through_the_interior());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
    following(&tables(&chooser), &[(M1, CellState::Ready)]).await;
    ask(&chooser.segment).await.expect("answered");
    let held = gated(&chooser, &events, "held").await;

    m1.readiness.drain().await.expect("the cell drains");
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
/// inside its cell that a request another entry instance took makes: the
/// cell's entry instances are its drain sources too. Here `d` hands D to
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
        |cells| followed(cells, "f", M1).is_some_and(|m| m.eligible.len() == 2 && m.load.is_some()),
    )
    .await;
    let held = gated(&chooser, &events, "held").await;
    let (taker, other) = match m1.entries[0].segment.admitted() {
        1 => (&m1.entries[0], &m1.entries[1]),
        _ => (&m1.entries[1], &m1.entries[0]),
    };
    *elsewhere.avoid.lock().expect("lock") = Some(id(taker).as_u128());
    let mut drained = phase(&m1.hub, "f", SegmentPhase::Drained).await;

    m1.readiness.drain().await.expect("the cell drains");
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

/// A request the entry took before its cell drained is answered after the
/// entry stopped taking work, and the entry announces that it has drained
/// only once it holds no request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_announces_drained_once_its_requests_are_answered() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
    following(&tables(&chooser), &[(M1, CellState::Ready)]).await;
    let held = gated(&chooser, &events, "held").await;
    let mut drained = phase(&m1.hub, "f", SegmentPhase::Drained).await;

    m1.readiness.drain().await.expect("the cell drains");
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

/// An interior instance of a draining cell serves while an entry instance
/// still holds a request that will call it, even once the entry stopped
/// taking work, and drains only after the entry has drained: on its own,
/// or when it is stopped while the cell drains, since the cell's entry
/// instances are its drain sources then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interior_instance_serves_until_the_entry_instances_have_drained() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    for stopped in [false, true] {
        let events = Events::new();
        let m1 = cell(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
        following(&tables(&chooser), &[(M1, CellState::Ready)]).await;
        let held = gated(&chooser, &events, "held").await;
        let mut retired = phase(&m1.hub, "d", SegmentPhase::Retired).await;

        m1.readiness.drain().await.expect("the cell drains");
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
            // An instance that counted the sources outside its cell would
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
        let ModelCell {
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
        until(&tables(&chooser), "the drained cell dropped", |cells| {
            followed(cells, "f", M1).is_none()
        })
        .await;
    }
    hub.shutdown().await.expect("valid test fixture");
}

/// Once every instance of a draining cell has drained and left, on its
/// own, the cell's hub withdraws its record, and the global cell's hub
/// that followed it drops it from its relay, so that its choosers no
/// longer follow it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drained_cell_is_withdrawn_once_its_instances_have_left() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global): (Hub, Arc<CellReadiness<Resolving>>) =
        cell_hub_of(&spec, global_hub(), &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready)]).await;
    ask(&chooser.segment).await.expect("answered");

    m1.readiness.drain().await.expect("the cell drains");
    let ModelCell {
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
        "the drained cell's record was not withdrawn"
    );
    let mut following = global.following();
    tokio::time::timeout(
        WAIT,
        following.wait_for(|cells| !cells.keys().any(|(_, cell)| cell == M1)),
    )
    .await
    .expect("the global hub dropped the drained cell in time")
    .expect("the extension runs");
    until(&cells, "the chooser no longer follows the cell", |cells| {
        followed(cells, "f", M1).is_none()
    })
    .await;
    for hub in [hub, model_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// What a node in the global cell reads of the model cell `cell`: its
/// state, if its chooser follows it, and whether it is withdrawn.
fn read(followed: &FollowedCells, cell: &str) -> (Option<CellState>, bool) {
    followed
        .read("model_x", |cells| {
            (
                cells.cell(cell).map(gdl::Cell::state),
                cells.is_withdrawn(cell),
            )
        })
        .expect("the chooser sends to model_x")
}

/// A node in the global cell reads each model cell as its chooser holds
/// it. A cell that drains reads `ready`, then `draining` while a request
/// the chooser sent it holds its follow, then withdrawn once its
/// instances have left and its hub has withdrawn its record. A cell whose
/// hub stops without a drain reads as neither followed nor withdrawn: its
/// lease runs out, and silence is never a decommission. A chooser that
/// starts after the withdrawal never saw the cell, and reads it so too:
/// a gate answers it as not available (503), not as removed (404). The
/// drain waits out the entry's bound, so this test takes several seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_reads_a_drained_cell_as_withdrawn_and_a_stopped_one_as_not() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _global): (Hub, Arc<CellReadiness<Resolving>>) =
        cell_hub_of(&spec, global_hub(), &directory).await;
    let node = FollowedCells::new();
    let chooser = reading_chooser(&spec, &hub, &node).await;
    assert!(node.is_bound(), "the launch binds the handle");
    let m1 = cell(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready)]).await;
    assert_eq!(read(&node, M1), (Some(CellState::Ready), false));

    let held = gated(&chooser, &events, "held").await;
    m1.readiness.drain().await.expect("the cell drains");
    until(&cells, "the node reads the cell draining", |_| {
        read(&node, M1) == (Some(CellState::Draining), false)
    })
    .await;
    release(&events, "held");
    held.await
        .expect("the request ran")
        .expect("the request was answered");
    let ModelCell {
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
    until(&cells, "the node reads the cell withdrawn", |_| {
        read(&node, M1) == (None, true)
    })
    .await;

    let m2 = cell(&spec, &directory, ready(M2, "m2-hub:1337"), &events).await;
    following(&cells, &[(M2, CellState::Ready)]).await;
    assert_eq!(read(&node, M2), (Some(CellState::Ready), false));
    // Once it follows the cell that came after, the late chooser has read
    // its hub's relay, and the withdrawal is not in it.
    let late = FollowedCells::new();
    let later = reading_chooser(&spec, &hub, &late).await;
    following(&tables(&later), &[(M2, CellState::Ready)]).await;
    assert_eq!(read(&late, M1), (None, false), "never seen");

    m2.hub.shutdown().await.expect("valid test fixture");
    until(&cells, "the stopped cell dropped", |cells| {
        followed(cells, "f", M2).is_none()
    })
    .await;
    assert_eq!(read(&node, M2), (None, false), "a stop is not a withdrawal");
    assert_eq!(read(&node, M1), (None, true));
    for hub in [hub, m1_hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
