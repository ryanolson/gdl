//! A stop tells the nodes of a segment that their instance is leaving: each
//! registered hook starts at the stop, on every way to it, and learns
//! through `Leave::routing` how sure the runtime is that the instance is
//! out of routing. See docs/src/concepts/lifecycle.md.
//!
//! Every hub and instance runs in this process on OS-assigned ports. Each
//! wait is for something the test can see. Those that prove a thing does
//! not happen wait a few of the observer's polls. A test of a bound lasts
//! at least that bound, which is why the pool tests here take seconds.

mod common;
#[path = "leaving/pools.rs"]
mod in_pools;

use std::sync::Arc;
use std::time::Duration;

use common::proxy::Proxy;
use common::reference_manifest;
use common::{Events, Idle, Plan, WAIT, collect, expected, prompt, reference_graph};
use figment::providers::{Format, Toml};
use gdl::{Cx, Handoff, Leave, LeaveCause, Lifecycle, Routing, Unconfirmed};
use gdl_runtime::{
    Deployment, DeploymentSpec, Hosted, Launch, Placement, Process, ProcessConfig, RuntimeConfig,
};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use velo::InstanceId;
use velo::discovery::ServiceDiscovery;

/// Long enough for several of the observer's polls, each 100 ms.
const POLLS: Duration = Duration::from_millis(400);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NODES: &'static [&'static str] = &[];
    const ADAPTERS: &'static [&'static str] = &[];
    const GRAPH_NODES: &'static [&'static str] = &[];
}

/// A hook the test holds: it hands the test its `Leave` as it starts, and
/// ends when the test says so.
struct Hook {
    started: mpsc::UnboundedReceiver<Leave>,
    finish: Option<oneshot::Sender<Handoff>>,
}

impl Hook {
    fn on(lifecycle: &Lifecycle) -> Self {
        Self::named(lifecycle, "kv")
    }

    fn named(lifecycle: &Lifecycle, name: &str) -> Self {
        let (starts, started) = mpsc::unbounded_channel();
        let (finish, finishes) = oneshot::channel();
        lifecycle
            .on_leave(name, move |leave: Leave| async move {
                let _ = starts.send(leave);
                finishes
                    .await
                    .unwrap_or_else(|_| Handoff::Failed("the test went away".into()))
            })
            .expect("registered");
        Self {
            started,
            finish: Some(finish),
        }
    }

    /// The `Leave` it started with, once it has.
    async fn leave(&mut self) -> Leave {
        tokio::time::timeout(WAIT, self.started.recv())
            .await
            .expect("the hook started in time")
            .expect("the hook ran")
    }

    fn finish(&mut self, handoff: Handoff) {
        let _ = self.finish.take().expect("finished once").send(handoff);
    }
}

/// Where `routing` moves to, within `within`.
async fn routed(leave: &Leave, within: Duration) -> Routing {
    tokio::time::timeout(within, leave.routed())
        .await
        .expect("routing moved in time")
}

/// That `routing` does not move for a few of the observer's polls.
async fn stays_pending(leave: &Leave, why: &str) {
    let moved = tokio::time::timeout(POLLS, leave.routed()).await;
    assert!(moved.is_err(), "{why}: routing read {moved:?}");
}

/// The front and the engine: the front sends to the engine.
fn two() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("engine", ["C", "D"])
}

/// Three segments in a line.
fn line() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("mid", ["C"])
        .segment("tail", ["D"])
}

/// A hub for the reference graph under `placement`, on OS-assigned ports.
async fn hub(placement: &Placement) -> velo_hub::Hub {
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    config.server.ttl = Duration::from_secs(30);
    config.server.prune_interval = Duration::from_millis(100);
    let spec = DeploymentSpec::new(reference_manifest(|_| true), placement.clone())
        .expect("valid test fixture");
    velo_hub::Hub::builder(config)
        .extension(gdl_hub::GraphAdmission::new(spec))
        .serve()
        .await
        .expect("valid test fixture")
}

/// Settings at `hub`, with `extra` before its `[runtime]` table and
/// `runtime` in it.
fn settings(hub: &velo_hub::Hub, extra: &str, runtime: &str) -> ProcessConfig<Nothing> {
    settings_through(hub, hub.control_addr().port(), extra, runtime)
}

/// [`settings`], for an instance that reaches the hub's control port on
/// `control`.
fn settings_through(
    hub: &velo_hub::Hub,
    control: u16,
    extra: &str,
    runtime: &str,
) -> ProcessConfig<Nothing> {
    let text = format!(
        "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {}, control_port = {control}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n{extra}\n[runtime]\n{runtime}\n",
        hub.public_addr().port(),
    );
    ProcessConfig::from_figment(figment::Figment::from(Toml::string(&text))).expect("settings")
}

/// An instance of `segment`: its process, and its launch.
async fn launch(
    config: &ProcessConfig<Nothing>,
    segment: &'static str,
    placement: &Placement,
) -> (Process, Launch) {
    let process = Process::start(config).await.expect("process");
    let names: Vec<&'static str> = placement
        .segments()
        .map(|(name, _)| &*Box::leak(name.to_string().into_boxed_str()))
        .collect();
    let segments: &'static [&'static str] = Box::leak(names.into_boxed_slice());
    let spec = DeploymentSpec::new(reference_manifest(|_| true), placement.clone())
        .expect("valid test fixture");
    let launch = config
        .launch(&process, segment, segments, &spec, async { Ok(()) })
        .await
        .expect("launched");
    (process, launch)
}

/// An instance's serving task.
type Served = JoinHandle<gdl::Result<()>>;

/// Serve `launch` with a body that ends at the stop, and wait until it
/// runs.
async fn serve(launch: Launch) -> Served {
    let (running, runs) = oneshot::channel();
    let served = tokio::spawn(launch.serve(Idle, |_, shutdown| async move {
        let _ = running.send(());
        shutdown.cancelled().await;
        Ok(())
    }));
    tokio::time::timeout(WAIT, runs)
        .await
        .expect("ready in time")
        .expect("the body ran");
    served
}

/// Wait until the instance has left, and say how its serving ended.
async fn left(served: Served) -> gdl::Result<()> {
    tokio::time::timeout(WAIT * 2, served)
        .await
        .expect("left in time")
        .expect("the serving task")
}

/// Wait until `who` have promised `hub` to avoid `retired`.
async fn avoided(hub: &velo_hub::Hub, retired: InstanceId, who: &[InstanceId]) {
    let services = client(hub).services();
    let name = format!("gdl-avoids-{:032x}", retired.as_u128());
    let deadline = Instant::now() + WAIT;
    loop {
        let avoiding = services.get_instances(&name).await.unwrap_or_default();
        if who.iter().all(|id| avoiding.contains(id)) {
            return;
        }
        assert!(Instant::now() < deadline, "{who:?} never avoided {retired}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn client(hub: &velo_hub::Hub) -> velo_hub::client::HubClient {
    velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture")
}

/// A hook of an instance whose only sender has not promised anything reads
/// `Pending`, however long, and `Confirmed` as the promise lands. The front
/// reaches the hub through a proxy, which holds its promise back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routing_is_confirmed_only_once_every_sender_has_acknowledged() {
    let placement = two();
    let hub = hub(&placement).await;
    let proxy = Proxy::new(hub.control_addr()).await;
    let replicas = "[min_replicas]\nengine = 2\n";
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\n";
    let front_config = settings_through(&hub, proxy.port, replicas, timing);
    let config = settings(&hub, replicas, timing);
    let leaving = settings(&hub, replicas, timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine), (other_process, other)) = tokio::join!(
        launch(&front_config, "front", &placement),
        launch(&leaving, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let (front_id, engine_id) = (front.node().instance_id(), engine.node().instance_id());
    let (front_served, engine_served, other_served) =
        tokio::join!(serve(front), serve(engine), serve(other));

    proxy.hold(b"gdl-avoids");
    engine_process.stop();
    let leave = hook.leave().await;
    assert_eq!(leave.cause(), LeaveCause::Stop);
    assert!(!leave.external());
    tokio::time::timeout(WAIT, proxy.holding())
        .await
        .expect("the front's promise reached the proxy");
    stays_pending(&leave, "no sender has promised").await;

    proxy.release();
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    avoided(&hub, engine_id, &[front_id]).await;

    hook.finish(Handoff::Done);
    left(engine_served).await.expect("drained");
    front_process.stop();
    other_process.stop();
    left(front_served).await.expect("drained");
    left(other_served).await.expect("drained");
}

/// `Confirmed` does not mean quiet. A hand-off sent before the front's
/// promise and held on its way lands after it and is served, and
/// `drained()` waits for the instance to close and hold nothing. The
/// setup timeout must outlast the gate's release, which is why this takes
/// seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_hand_off_lands_and_drained_waits_for_the_drains_own_point() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 3000\n";
    let replicas = "[min_replicas]\nengine = 2\n";
    let config = settings(&hub, replicas, timing);
    let leaving = settings(&hub, replicas, timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let (a_events, b_events) = (Events::new(), Events::new());
    let ((front_process, front), (a_process, a), (b_process, b)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let patient = || RuntimeConfig {
        setup_timeout: Duration::from_secs(5),
        ..RuntimeConfig::default()
    };
    let start = |launch: &Launch, segment: &str, events: &Arc<Events>, runtime| {
        let deployment = Deployment::new(
            launch.node(),
            &reference_manifest(|_| true),
            &placement,
            segment,
            launch.roster(),
            runtime,
        )
        .expect("deployment");
        let root = reference_graph(&deployment, events).expect("graph");
        deployment.start(root).expect("start")
    };
    let gate = Arc::new(gdl_runtime::Gate::default());
    let gated = RuntimeConfig {
        faults: gdl_runtime::Faults {
            forward_gate: Some(Arc::clone(&gate)),
            ..gdl_runtime::Faults::default()
        },
        ..patient()
    };
    let front_segment = start(&front, "front", &Events::quiet(), patient());
    let a_segment = start(&a, "engine", &a_events, gated);
    let b_segment = start(&b, "engine", &b_events, patient());
    let serving = |launch: Launch, segment| {
        let (tx, rx) = oneshot::channel();
        let served = tokio::spawn(launch.serve(segment, |segment, shutdown| async move {
            let _ = tx.send(segment);
            shutdown.cancelled().await;
            Ok(())
        }));
        (rx, served)
    };
    let (front_rx, front_served) = serving(front, front_segment);
    let (a_rx, a_served) = serving(a, a_segment);
    let (b_rx, b_served) = serving(b, b_segment);
    let front_segment = front_rx.await.expect("ready");
    let a_segment = a_rx.await.expect("ready");
    let _ = b_rx.await.expect("ready");

    let send = |plan: Plan, text: &str, cx: Cx| {
        let front = Arc::clone(&front_segment);
        let text = text.to_string();
        tokio::spawn(async move {
            match front.call(cx, prompt(&text, plan)).await {
                Ok(out) => Ok(collect(out).await),
                Err(error) => Err(error),
            }
        })
    };
    let plain = Plan {
        tokens: 1,
        ..Plan::default()
    };
    // The front's picks alternate between the two engines. Find where the
    // turn stands: send until one waits at the first engine's gate.
    let mut n = 0;
    loop {
        let mut request = send(plain.clone(), &format!("probe{n}"), Cx::new());
        n += 1;
        tokio::select! {
            () = gate.arrived.notified() => {
                gate.release.notify_one();
                let answer = request.await.expect("the request's task");
                assert_eq!(answer.expect("answered"), expected(0..1));
                break;
            }
            done = &mut request => {
                assert_eq!(done.expect("the request's task").expect("answered"), expected(0..1));
            }
        }
    }
    assert_eq!(
        send(plain, "second", Cx::new())
            .await
            .expect("the request's task")
            .expect("answered"),
        expected(0..1)
    );
    // Held at the gate, and then, admitted, held open at its end.
    let held = Cx::new();
    let late = send(
        Plan {
            tokens: 1,
            hold_at_end: true,
            ..Plan::default()
        },
        "late",
        held.clone(),
    );
    tokio::time::timeout(WAIT, gate.arrived.notified())
        .await
        .expect("the late request reached the engine");

    a_process.stop();
    let leave = hook.leave().await;
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    assert!(
        !a_segment.is_closed(),
        "closed within its setup timeout of the promise"
    );
    let served = a_segment.served().forwards;
    gate.release.notify_one();
    a_events.wait_for("D:held:c(late)#0", WAIT).await;
    assert_eq!(a_segment.served().forwards, served + 1);

    let deadline = Instant::now() + WAIT;
    while !a_segment.is_closed() {
        assert!(Instant::now() < deadline, "the engine never closed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        tokio::time::timeout(POLLS, leave.drained()).await.is_err(),
        "drained() resolved while the instance held a request"
    );
    assert!(a_segment.admitted() > 0);

    held.request_stop();
    tokio::time::timeout(WAIT, leave.drained())
        .await
        .expect("the instance drained");
    assert!(a_segment.is_closed());
    assert_eq!(a_segment.admitted(), 0);
    let answer = tokio::time::timeout(WAIT, late)
        .await
        .expect("answered in time")
        .expect("the request's task")
        .expect("answered");
    assert!(answer.iter().all(Result::is_ok), "{answer:?}");

    hook.finish(Handoff::Done);
    left(a_served).await.expect("drained");
    front_process.stop();
    b_process.stop();
    left(front_served).await.expect("drained");
    left(b_served).await.expect("drained");
}

/// The serving body finishes only once a hook has started, and only after
/// routing has moved: the hooks and the observer run beside the body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_held_on_its_hook_does_not_hold_the_hook_or_routing() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\n";
    let replicas = "[min_replicas]\nengine = 2\n";
    let config = settings(&hub, replicas, timing);
    let leaving = settings(&hub, replicas, timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine), (other_process, other)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let (running, runs) = oneshot::channel();
    let (hook_started, started) = oneshot::channel::<()>();
    let (release, released) = oneshot::channel::<()>();
    let engine_served = tokio::spawn(engine.serve(Idle, |_, shutdown| async move {
        let _ = running.send(());
        shutdown.cancelled().await;
        let _ = started.await;
        let _ = released.await;
        Ok(())
    }));
    let (front_served, other_served) = tokio::join!(serve(front), serve(other));
    runs.await.expect("the body ran");

    engine_process.stop();
    let leave = hook.leave().await;
    hook_started.send(()).expect("the body waits for the hook");
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    assert!(
        !engine_served.is_finished(),
        "the body has not been released"
    );

    release.send(()).expect("the body waits");
    tokio::time::timeout(WAIT, leave.drained())
        .await
        .expect("the instance drained");
    hook.finish(Handoff::Done);
    left(engine_served).await.expect("drained");
    front_process.stop();
    other_process.stop();
    left(front_served).await.expect("drained");
    left(other_served).await.expect("drained");
}

/// A stop while the instance waits at its barrier starts the hooks too, and
/// routing moves by the same rule. The tail never serves, so the mid
/// never passes its barrier, and the front has said it serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_at_the_barrier_starts_the_hooks_the_same_way() {
    let placement = line();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\n";
    let replicas = "[min_replicas]\nmid = 2\n";
    let config = settings(&hub, replicas, timing);
    let leaving = settings(&hub, replicas, timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let (
        (front_process, front),
        (mid_process, mid),
        (other_process, other),
        (_tail_process, _tail),
    ) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "mid", &placement),
        launch(&config, "mid", &placement),
        launch(&config, "tail", &placement),
    );
    let (mid_id, front_id) = (mid.node().instance_id(), front.node().instance_id());
    let front_served = tokio::spawn(front.serve(Idle, |_, shutdown| async move {
        shutdown.cancelled().await;
        Ok(())
    }));
    let (ran, body) = oneshot::channel::<()>();
    let mid_served = tokio::spawn(mid.serve(Idle, |_, _| async move {
        let _ = ran.send(());
        Ok(())
    }));
    let other_served = tokio::spawn(other.serve(Idle, |_, shutdown| async move {
        shutdown.cancelled().await;
        Ok(())
    }));
    let services = client(&hub).services();
    let serving = format!("gdl-serving-{}", hex("mid"));
    let deadline = Instant::now() + WAIT;
    while !services
        .get_instances(&serving)
        .await
        .unwrap_or_default()
        .contains(&mid_id)
    {
        assert!(Instant::now() < deadline, "the mid never said it serves");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    mid_process.stop();
    let leave = hook.leave().await;
    assert_eq!(leave.cause(), LeaveCause::Stop);
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    avoided(&hub, mid_id, &[front_id]).await;
    tokio::time::timeout(WAIT, leave.drained())
        .await
        .expect("the instance drained");
    hook.finish(Handoff::Nothing);
    let stopped = left(mid_served).await.expect_err("stopped while starting");
    assert!(stopped.to_string().contains("starting"), "{stopped}");
    assert!(body.await.is_err(), "the body ran");
    for process in [front_process, other_process] {
        process.stop();
    }
    let _ = left(front_served).await;
    let _ = left(other_served).await;
}

/// A segment's name in a hub service's name.
fn hex(segment: &str) -> String {
    segment.bytes().map(|b| format!("{b:02x}")).collect()
}

/// The last instance of a segment is avoided by nobody, so it reads
/// `Unconfirmed(Bound)`, and with `drain_ms` below `leave_after`, at half
/// the bound, not at `leave_after`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_last_instance_reads_the_bound_at_half_the_drain() {
    let placement = two();
    let hub = hub(&placement).await;
    let config = settings(&hub, "", "drain_ms = 1600\nsetup_timeout_ms = 300\n");
    let leaving = settings(&hub, "", "drain_ms = 1600\nsetup_timeout_ms = 300\n");
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    let stopped = Instant::now();
    engine_process.stop();
    let leave = hook.leave().await;
    assert_eq!(*leave.routing().borrow(), Routing::Pending);
    assert_eq!(
        routed(&leave, WAIT).await,
        Routing::Unconfirmed(Unconfirmed::Bound)
    );
    let took = stopped.elapsed();
    assert!(
        took >= Duration::from_millis(790),
        "read the bound after {took:?}, before half the drain"
    );
    hook.finish(Handoff::Done);
    left(engine_served).await.expect("drained");
    assert!(
        stopped.elapsed() < Duration::from_millis(1600) + Duration::from_secs(9),
        "left {:?} after the stop",
        stopped.elapsed()
    );
    front_process.stop();
    left(front_served).await.expect("drained");
}

/// `leave_after` below half the drain's bound decides the fallback, from
/// the retirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leave_after_counts_from_the_retirement() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\nleave_after_ms = 300\n";
    let config = settings(&hub, "", timing);
    let leaving = settings(&hub, "", timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    let stopped = Instant::now();
    engine_process.stop();
    let leave = hook.leave().await;
    assert_eq!(
        routed(&leave, WAIT).await,
        Routing::Unconfirmed(Unconfirmed::Bound)
    );
    let took = stopped.elapsed();
    assert!(
        took >= Duration::from_millis(290) && took < Duration::from_secs(10),
        "read the bound after {took:?}, not {:?} after the retirement",
        Duration::from_millis(300)
    );
    hook.finish(Handoff::Done);
    front_process.stop();
    left(front_served).await.expect("drained");
    left(engine_served).await.expect("drained");
}

/// A lost lease moves routing at once, wherever the stop stands. Here the
/// instance is waiting for peers that would not answer before its bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_lost_during_a_stop_moves_routing_at_once() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\n";
    let config = settings(&hub, "", timing);
    let leaving = settings(&hub, "", timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let engine_id = engine.node().instance_id();
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    engine_process.stop();
    let leave = hook.leave().await;
    stays_pending(&leave, "the last engine, well inside its bound").await;
    lose_lease(&hub, engine_id).await;
    let at = Instant::now();
    assert_eq!(
        routed(&leave, WAIT).await,
        Routing::Unconfirmed(Unconfirmed::LeaseLost)
    );
    // A heartbeat is 200 ms, and the lease's grace 1 s.
    assert!(at.elapsed() < Duration::from_secs(5), "{:?}", at.elapsed());
    hook.finish(Handoff::Done);
    let _ = left(engine_served).await;
    front_process.stop();
    left(front_served).await.expect("drained");
}

/// A lease lost while serving is the stop itself: the hooks start with
/// that cause, and routing reads it at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_lease_starts_the_hooks_with_its_cause() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 2000\nsetup_timeout_ms = 300\n";
    let config = settings(&hub, "", timing);
    let leaving = settings(&hub, "", timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (_engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let engine_id = engine.node().instance_id();
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    lose_lease(&hub, engine_id).await;
    let leave = hook.leave().await;
    assert_eq!(leave.cause(), LeaveCause::LeaseLost);
    assert_eq!(
        *leave.routing().borrow(),
        Routing::Unconfirmed(Unconfirmed::LeaseLost)
    );
    hook.finish(Handoff::Failed("no hub".into()));
    left(engine_served).await.expect_err("the lease's error");
    front_process.stop();
    left(front_served).await.expect("drained");
}

/// Take `id`'s registration away from the hub: its lease is lost.
async fn lose_lease(hub: &velo_hub::Hub, id: InstanceId) {
    let member = hub.store().get(id).expect("the instance registered");
    tokio::time::timeout(WAIT, hub.store().remove(id, member.incarnation))
        .await
        .expect("removed in time")
        .expect("removed");
}

/// A frontend's clients are not gdl senders, so `external` says so, however
/// confirmed routing is: nothing sends to it, so it reads `Confirmed` at
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_frontend_is_external_whatever_routing_says() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 2000\nsetup_timeout_ms = 300\n";
    let config = settings(&hub, "", timing);
    let leaving = settings(&hub, "", timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&leaving, "front", &placement),
        launch(&config, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    front_process.stop();
    let leave = hook.leave().await;
    assert!(leave.external());
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    hook.finish(Handoff::Nothing);
    left(front_served).await.expect("drained");
    engine_process.stop();
    left(engine_served).await.expect("drained");
}

/// How long the slowest of a hook that ignores its context, whose bound
/// is `leave_ms`, may hold the exit: the runtime does not wait for it, so
/// the exit is the drain's. The hook's sibling keeps its own outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hook_that_ignores_its_context_is_timed_out_and_does_not_hold_the_exit() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 2000\nsetup_timeout_ms = 300\nleave_ms = 600\n";
    let config = settings(&hub, "", timing);
    let mut leaving = settings(&hub, "", timing);
    let registry = prometheus::Registry::new();
    leaving.runtime.metrics = Some(registry.clone());
    let (seen, mut heard) = mpsc::unbounded_channel();
    leaving
        .runtime
        .lifecycle
        .on_leave("stuck", move |leave: Leave| async move {
            let _ = seen.send(leave);
            std::future::pending().await
        })
        .expect("registered");
    leaving
        .runtime
        .lifecycle
        .on_leave("flush", |_| async { Handoff::Done })
        .expect("registered");
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    let stopped = Instant::now();
    engine_process.stop();
    let leave = tokio::time::timeout(WAIT, heard.recv())
        .await
        .expect("the hook started")
        .expect("the hook ran");
    assert!(leave.deadline() <= stopped + Duration::from_millis(700));
    let deadline = Instant::now() + WAIT;
    while outcomes(&registry, "stuck").len() < 2 {
        assert!(Instant::now() < deadline, "the outcome never showed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The token, which no clock moves: the stop's end has not dropped it
    // yet, since the last engine drains until half of its bound and past.
    assert!(
        leave.cx().cancel_token().is_cancelled(),
        "a hook past its deadline is told, and not by the clock alone"
    );
    assert!(!engine_served.is_finished());
    left(engine_served).await.expect("drained");
    let took = stopped.elapsed();
    assert!(
        took >= Duration::from_millis(590),
        "left {took:?} after the stop, before the hook's deadline"
    );
    // The bound, then what the exit may add to it.
    assert!(
        took < Duration::from_millis(2000) + Duration::from_secs(9),
        "left {took:?} after the stop"
    );
    assert_eq!(
        outcomes(&registry, "stuck"),
        [("pending".into(), 0), ("timed_out".into(), 1)]
    );
    assert_eq!(
        outcomes(&registry, "flush"),
        [("done".into(), 1), ("pending".into(), 0)]
    );
    front_process.stop();
    left(front_served).await.expect("drained");
}

/// A hook that panics is its own failure: the others go on, and the
/// instance leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hook_that_panics_is_recorded_failed() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 2000\nsetup_timeout_ms = 300\n";
    let config = settings(&hub, "", timing);
    let mut leaving = settings(&hub, "", timing);
    let registry = prometheus::Registry::new();
    leaving.runtime.metrics = Some(registry.clone());
    leaving
        .runtime
        .lifecycle
        .on_leave("broken", |_| async { panic!("a hook's panic") })
        .expect("registered");
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    engine_process.stop();
    left(engine_served).await.expect("drained");
    assert_eq!(
        outcomes(&registry, "broken"),
        [("failed".into(), 1), ("pending".into(), 0)]
    );
    front_process.stop();
    left(front_served).await.expect("drained");
}

/// Each hook reads `pending` from when the instance serves, then its one
/// outcome: what the operator reads before it deletes a pod.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_outcome_reads_pending_then_how_the_hook_ended() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 2000\nsetup_timeout_ms = 300\n";
    let config = settings(&hub, "", timing);
    let mut leaving = settings(&hub, "", timing);
    let registry = prometheus::Registry::new();
    leaving.runtime.metrics = Some(registry.clone());
    let mut kv = Hook::named(&leaving.runtime.lifecycle, "kv");
    let mut flush = Hook::named(&leaving.runtime.lifecycle, "flush");
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    for hook in ["kv", "flush"] {
        assert_eq!(outcomes(&registry, hook), [("pending".into(), 1)]);
    }
    assert_eq!(seconds(&registry, "kv"), None);

    engine_process.stop();
    let _ = kv.leave().await;
    let _ = flush.leave().await;
    assert_eq!(outcomes(&registry, "kv"), [("pending".into(), 1)]);

    kv.finish(Handoff::Partial { moved: 2, left: 1 });
    let deadline = Instant::now() + WAIT;
    while outcomes(&registry, "kv").len() < 2 {
        assert!(Instant::now() < deadline, "the outcome never showed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        outcomes(&registry, "kv"),
        [("partial".into(), 1), ("pending".into(), 0)]
    );
    assert!(seconds(&registry, "kv").is_some_and(|seconds| seconds >= 0.0));
    assert_eq!(outcomes(&registry, "flush"), [("pending".into(), 1)]);
    flush.finish(Handoff::Nothing);
    left(engine_served).await.expect("drained");
    assert_eq!(
        outcomes(&registry, "flush"),
        [("nothing".into(), 1), ("pending".into(), 0)]
    );
    front_process.stop();
    left(front_served).await.expect("drained");
}

/// An instance with no hook exports no `leave_outcome` series.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_with_no_hook_exports_no_outcome() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 2000\nsetup_timeout_ms = 300\n";
    let config = settings(&hub, "", timing);
    let mut leaving = settings(&hub, "", timing);
    let registry = prometheus::Registry::new();
    leaving.runtime.metrics = Some(registry.clone());
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));
    engine_process.stop();
    left(engine_served).await.expect("drained");
    let names: Vec<String> = registry
        .gather()
        .iter()
        .map(|family| family.name().to_string())
        .collect();
    assert!(
        !names.iter().any(|name| name.contains("leave_")),
        "{names:?}"
    );
    front_process.stop();
    left(front_served).await.expect("drained");
}

/// Each `(outcome, value)` the registry holds for `hook`, sorted.
fn outcomes(registry: &prometheus::Registry, hook: &str) -> Vec<(String, i64)> {
    let mut outcomes: Vec<(String, i64)> = registry
        .gather()
        .iter()
        .filter(|family| family.name().ends_with("leave_outcome"))
        .flat_map(|family| family.get_metric())
        .filter(|metric| label(metric, "hook") == hook)
        .map(|metric| (label(metric, "outcome"), metric.get_gauge().value() as i64))
        .collect();
    outcomes.sort();
    outcomes
}

/// How long `hook` ran, once it has ended.
fn seconds(registry: &prometheus::Registry, hook: &str) -> Option<f64> {
    registry
        .gather()
        .iter()
        .filter(|family| family.name().ends_with("leave_seconds"))
        .flat_map(|family| family.get_metric())
        .find(|metric| label(metric, "hook") == hook)
        .map(|metric| metric.get_gauge().value())
}

fn label(metric: &prometheus::proto::Metric, name: &str) -> String {
    metric
        .get_label()
        .iter()
        .find(|pair| pair.name() == name)
        .map(|pair| pair.value().to_string())
        .unwrap_or_default()
}

/// A second stop ends everything at once, the hooks too: their context is
/// cancelled, and the instance does not wait for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_stop_ends_the_hooks_at_once() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\n";
    let config = settings(&hub, "", timing);
    let leaving = settings(&hub, "", timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    engine_process.stop();
    let leave = hook.leave().await;
    assert!(!leave.cx().is_cancelled());
    engine_process.stop();
    let ended = left(engine_served).await.expect_err("a second stop");
    assert!(ended.to_string().contains("second"), "{ended}");
    assert!(leave.cx().is_cancelled(), "a forced stop tells the hooks");
    front_process.stop();
    let _ = left(front_served).await;
}

/// With no hook the observer waits for the drain, so a long serving body
/// costs the hub nothing it did not cost before. With one it asks at
/// once, since a hook waits on the answer. The hub's members are read only
/// by those questions here, through a proxy that holds them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_hook_makes_the_observer_ask_before_the_body_ends() {
    for hooked in [false, true] {
        let placement = two();
        let hub = hub(&placement).await;
        let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\n";
        let config = settings(&hub, "", timing);
        let proxy = Proxy::new(hub.public_addr()).await;
        let text = format!(
            "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {}, control_port = {}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n[runtime]\n{timing}\n",
            proxy.port,
            hub.control_addr().port(),
        );
        let leaving: ProcessConfig<Nothing> =
            ProcessConfig::from_figment(figment::Figment::from(Toml::string(&text)))
                .expect("settings");
        let hook = hooked.then(|| Hook::on(&leaving.runtime.lifecycle));
        let ((front_process, front), (engine_process, engine)) = tokio::join!(
            launch(&config, "front", &placement),
            launch(&leaving, "engine", &placement),
        );
        let (release, released) = oneshot::channel::<()>();
        let (running, runs) = oneshot::channel();
        let engine_served = tokio::spawn(engine.serve(Idle, |_, shutdown| async move {
            let _ = running.send(());
            shutdown.cancelled().await;
            let _ = released.await;
            Ok(())
        }));
        let front_served = serve(front).await;
        runs.await.expect("the body ran");

        proxy.hold(b"/v1/ext/gdl/members");
        engine_process.stop();
        let asked = tokio::time::timeout(POLLS, proxy.holding()).await.is_ok();
        assert_eq!(asked, hooked, "asked before the body ended");

        proxy.release();
        let _ = release.send(());
        if let Some(mut hook) = hook {
            hook.finish(Handoff::Nothing);
        }
        // The last engine: it leaves once the source has drained.
        front_process.stop();
        left(front_served).await.expect("drained");
        left(engine_served).await.expect("drained");
    }
}

/// Once every source that can reach it has drained, the last instance of a
/// segment knows: no peer avoids it, and none needs to. The front's own
/// hook keeps it a member that has drained: one that left would be a
/// sender gone, which is (A).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sources_drain_tells_the_last_instance() {
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\n";
    let config = settings(&hub, "", timing);
    let mut front_hook = Hook::on(&config.runtime.lifecycle);
    let leaving = settings(&hub, "", timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
    );
    let (front_served, engine_served) = tokio::join!(serve(front), serve(engine));

    engine_process.stop();
    let leave = hook.leave().await;
    stays_pending(&leave, "its source has not drained").await;
    front_process.stop();
    assert_eq!(routed(&leave, WAIT).await, Routing::SourcesDone);
    hook.finish(Handoff::Done);
    let _ = front_hook.leave().await;
    front_hook.finish(Handoff::Done);
    left(front_served).await.expect("drained");
    left(engine_served).await.expect("drained");
}

/// When a segment closed.
#[derive(Default)]
struct Closing(std::sync::Mutex<Option<Instant>>);

impl gdl_runtime::Drains for Closing {
    fn admitted(&self) -> usize {
        0
    }

    fn close(&self) {
        *self.0.lock().expect("lock") = Some(Instant::now());
    }
}

/// A hook has the observer ask before the serving body ends, and that
/// changes nothing of the drain: it still gives a hand-off the setup
/// timeout once its wait begins, as it does with no hook, however long
/// ago the senders promised.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hook_does_not_shorten_the_drain() {
    const SETUP: Duration = Duration::from_millis(300);
    let placement = two();
    let hub = hub(&placement).await;
    let timing = "drain_ms = 20000\nsetup_timeout_ms = 300\n";
    let replicas = "[min_replicas]\nengine = 2\n";
    let config = settings(&hub, replicas, timing);
    let leaving = settings(&hub, replicas, timing);
    let mut hook = Hook::on(&leaving.runtime.lifecycle);
    let ((front_process, front), (engine_process, engine), (other_process, other)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&leaving, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let (running, runs) = oneshot::channel();
    let (release, released) = oneshot::channel::<()>();
    let (seen, closing) = oneshot::channel();
    let engine_served = tokio::spawn(engine.serve(
        Closing::default(),
        |segment, shutdown| async move {
            let _ = running.send(());
            let _ = seen.send(segment);
            shutdown.cancelled().await;
            let _ = released.await;
            Ok(())
        },
    ));
    let (front_served, other_served) = tokio::join!(serve(front), serve(other));
    runs.await.expect("the body ran");
    let closing = closing.await.expect("the body ran");

    engine_process.stop();
    let leave = hook.leave().await;
    assert_eq!(routed(&leave, WAIT).await, Routing::Confirmed);
    // Longer than the setup timeout since the promise, with the body held.
    tokio::time::sleep(SETUP + POLLS).await;
    let ended = Instant::now();
    release.send(()).expect("the body waits");
    tokio::time::timeout(WAIT, leave.drained())
        .await
        .expect("the instance drained");
    hook.finish(Handoff::Done);
    left(engine_served).await.expect("drained");
    let closed = closing.0.lock().expect("lock").expect("it closed");
    assert!(
        closed >= ended + SETUP,
        "closed {:?} after the body ended, within the setup timeout",
        closed - ended
    );
    front_process.stop();
    other_process.stop();
    left(front_served).await.expect("drained");
    left(other_served).await.expect("drained");
}

#[test]
fn the_runtime_settings_read_the_leave_times() {
    let config = RuntimeConfig::default();
    assert_eq!((config.leave, config.leave_after), (None, None));
    let read: RuntimeConfig =
        toml::from_str("leave_ms = 1500\nleave_after_ms = 2500\n").expect("both are settings");
    assert_eq!(read.leave, Some(Duration::from_millis(1500)));
    assert_eq!(read.leave_after, Some(Duration::from_millis(2500)));
}
