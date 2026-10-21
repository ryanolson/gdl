//! Live rosters: instances that join a running deployment, and instances
//! retired one at a time while the others serve. Each instance is a
//! process of its own (its own `Process` and velo node, over loopback TCP
//! on OS-assigned ports), and they meet through a real hub, or a roster
//! directory, in this test process.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use common::{Events, Plan, collect, expected, prompt, reference_graph, reference_manifest};
use figment::providers::{Format, Toml};
use gdl::{
    Cx, Deployment, DeploymentSpec, Drains, ErrorKind, Hosted, Launch, Placement, Process,
    ProcessConfig, RuntimeConfig,
};
use serde::Deserialize;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use velo::InstanceId;
use velo::discovery::ServiceDiscovery;

/// The longest a test waits for something it expects.
const WAIT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NAMES: &'static [&'static str] = &[];
}

/// The front and the engine: the front sends to the engine.
fn two() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("engine", ["C", "D"])
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

/// One deployment's settings: its hub, the drain's bound, and `extra`.
fn settings(hub: &velo_hub::Hub, drain_ms: u64, extra: &str) -> ProcessConfig<Nothing> {
    let text = format!(
        "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {}, control_port = {}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n{extra}\n[runtime]\ndrain_ms = {drain_ms}\nsetup_timeout_ms = 300\n",
        hub.public_addr().port(),
        hub.control_addr().port(),
    );
    ProcessConfig::from_figment(figment::Figment::from(Toml::string(&text))).expect("settings")
}

/// The hub's view, as a test asks it.
fn client(hub: &velo_hub::Hub) -> velo_hub::client::HubClient {
    velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture")
}

/// Wait until every instance of the deployment at `hub` that `who` names
/// has promised to avoid `retired`; the instant the test saw it.
async fn avoided(hub: &velo_hub::Hub, retired: InstanceId, who: &[InstanceId]) -> Instant {
    let services = client(hub).services();
    let name = format!("gdl-avoids-{:032x}", retired.as_u128());
    let deadline = Instant::now() + WAIT;
    loop {
        let avoiding = services.get_instances(&name).await.unwrap_or_default();
        if who.iter().all(|id| avoiding.contains(id)) {
            return Instant::now();
        }
        assert!(Instant::now() < deadline, "{who:?} never avoided {retired}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// An instance launched for `segment`: its process, and its launch.
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

/// Start the reference graph's `segment` on `launch` with `runtime`.
fn start(
    launch: &Launch,
    segment: &str,
    placement: &Placement,
    events: &Arc<Events>,
    runtime: RuntimeConfig,
) -> gdl::Segment<impl gdl::Next<common::Prompt, common::Chunk> + gdl::Register + use<>> {
    let deployment = Deployment::new(
        launch.node(),
        &reference_manifest(|_| true),
        placement,
        segment,
        launch.roster(),
        runtime,
    )
    .expect("deployment");
    let root = reference_graph(&deployment, events).expect("graph");
    deployment.start(root).expect("start")
}

/// Serve `segment` on `launch` until its process is stopped. The segment
/// comes back as soon as the body runs, that is once the instance is
/// ready; the task ends when the instance has left.
fn serve<S: Drains>(
    launch: Launch,
    segment: S,
) -> (oneshot::Receiver<Arc<S>>, JoinHandle<gdl::Result<()>>) {
    let (tx, rx) = oneshot::channel();
    let served = tokio::spawn(launch.serve(segment, |segment, shutdown| async move {
        let _ = tx.send(segment);
        shutdown.cancelled().await;
        Ok(())
    }));
    (rx, served)
}

async fn ready<S>(rx: oneshot::Receiver<Arc<S>>) -> Arc<S> {
    tokio::time::timeout(WAIT, rx)
        .await
        .expect("ready in time")
        .expect("the body ran")
}

async fn left(served: JoinHandle<gdl::Result<()>>) -> Instant {
    tokio::time::timeout(WAIT * 2, served)
        .await
        .expect("left in time")
        .expect("the serving task")
        .expect("served and drained");
    Instant::now()
}

/// Requests sent back to back by `lanes` callers, each checked whole,
/// until the traffic is stopped.
struct Traffic {
    done: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
    stopping: Arc<AtomicBool>,
    lanes: Vec<JoinHandle<()>>,
}

impl Traffic {
    fn start<N>(front: &Arc<gdl::Segment<N>>, lanes: usize) -> Self
    where
        N: gdl::Next<common::Prompt, common::Chunk> + Send + Sync + 'static,
    {
        let (done, failed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let stopping = Arc::new(AtomicBool::new(false));
        let lanes = (0..lanes)
            .map(|lane| {
                let (front, done, failed, stopping) = (
                    Arc::clone(front),
                    Arc::clone(&done),
                    Arc::clone(&failed),
                    Arc::clone(&stopping),
                );
                tokio::spawn(async move {
                    let mut n = 0;
                    while !stopping.load(Ordering::Relaxed) {
                        let plan = Plan {
                            tokens: 3,
                            item_delay_ms: 10,
                            ..Plan::default()
                        };
                        let text = format!("t{lane}-{n}");
                        n += 1;
                        let answer = match front.call(Cx::new(), prompt(&text, plan)).await {
                            Ok(out) => collect(out).await,
                            Err(error) => vec![Err(error)],
                        };
                        if answer != expected(0..3) {
                            eprintln!("{text}: {answer:?}");
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        Self {
            done,
            failed,
            stopping,
            lanes,
        }
    }

    /// Wait until `more` requests have finished after this call.
    async fn more(&self, more: usize) {
        let at_least = self.done.load(Ordering::Relaxed) + more;
        let deadline = Instant::now() + WAIT;
        while self.done.load(Ordering::Relaxed) < at_least {
            assert!(Instant::now() < deadline, "the traffic stalled");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Stop, and say how many requests failed.
    async fn stop(self) -> usize {
        self.stopping.store(true, Ordering::Relaxed);
        for lane in self.lanes {
            lane.await.expect("a traffic lane");
        }
        self.failed.load(Ordering::Relaxed)
    }
}

/// A short setup timeout: the grace a retired instance gives hand-offs
/// sent before its peers stopped routing to it.
fn quick() -> RuntimeConfig {
    RuntimeConfig {
        setup_timeout: Duration::from_millis(300),
        ..RuntimeConfig::default()
    }
}

/// One of two engine instances is stopped while requests flow: the front
/// stops routing to it and says so, it finishes what it holds and leaves,
/// long before its bound, and no request fails. While it drains, its
/// health says `draining`, and its `inflight_requests` falls to zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_of_two_engines_retires_while_requests_flow() {
    let placement = two();
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "[min_replicas]\nengine = 2\n");
    let probed = settings(
        &hub,
        20_000,
        "[min_replicas]\nengine = 2\n[system]\nhost = \"127.0.0.1\"\nport = 0\n",
    );
    let events = Events::quiet();
    let ((front_process, front), (first_process, first), (second_process, second)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&probed, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let (front_id, first_id) = (front.node().instance_id(), first.node().instance_id());
    let spec = DeploymentSpec::new(reference_manifest(|_| true), placement.clone())
        .expect("valid test fixture");
    let registry = first_process.registry(&spec, "engine").expect("registry");
    let metered = RuntimeConfig {
        metrics: Some(registry.clone()),
        ..quick()
    };
    let front_segment = start(&front, "front", &placement, &events, quick());
    let first_segment = start(&first, "engine", &placement, &events, metered);
    let second_segment = start(&second, "engine", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (first_rx, first_served) = serve(first, first_segment);
    let (second_rx, second_served) = serve(second, second_segment);
    let front_segment = ready(front_rx).await;
    let (first_segment, second_segment) = (ready(first_rx).await, ready(second_rx).await);

    let traffic = Traffic::start(&front_segment, 4);
    traffic.more(20).await;
    assert!(first_segment.served().forwards > 0 && second_segment.served().forwards > 0);

    let stopped = Instant::now();
    first_process.stop();
    let address = first_process.system_address().expect("a system listener");
    let (code, body) = common::system::get(address, "/health").await;
    assert_eq!(code, 503, "{body}");
    assert!(body.contains("\"phase\":\"draining\""), "{body}");
    let promised = avoided(&hub, first_id, &[front_id]).await;
    let exited = left(first_served).await;
    assert!(
        exited >= promised,
        "it left before the front stopped routing to it"
    );
    assert!(
        exited - stopped < Duration::from_secs(5),
        "a lone retirement waited {:?}, near its 20 s bound",
        exited - stopped
    );
    let inflight: f64 = registry
        .gather()
        .iter()
        .filter(|family| family.name().ends_with("inflight_requests"))
        .flat_map(|family| family.get_metric().iter().map(|m| m.get_gauge().value()))
        .sum();
    assert_eq!(inflight, 0.0, "it left with requests in flight");

    // The remaining engine takes everything.
    traffic.more(20).await;
    assert_eq!(traffic.stop().await, 0, "a request failed");

    front_process.stop();
    second_process.stop();
    left(front_served).await;
    left(second_served).await;
}

/// Three segments in a line: the front sends to the middle, the middle to
/// the tail.
fn line() -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("mid", ["C"])
        .segment("tail", ["D"])
}

/// One plain request through `front`, checked whole.
async fn ask<N>(front: &gdl::Segment<N>, text: &str) -> gdl::Result<()>
where
    N: gdl::Next<common::Prompt, common::Chunk>,
{
    let plan = Plan {
        tokens: 2,
        ..Plan::default()
    };
    let answer = collect(front.call(Cx::new(), prompt(text, plan)).await?).await;
    assert_eq!(answer, expected(0..2), "{text}");
    Ok(())
}

/// Two engine instances of three are stopped one after the other while
/// requests flow: each leaves once the front avoids it, and the third
/// serves on. No request fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_of_three_engines_retire_one_after_the_other() {
    let placement = two();
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "[min_replicas]\nengine = 3\n");
    let events = Events::quiet();
    let ((front_process, front), (a_process, a), (b_process, b), (c_process, c)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
        launch(&config, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let front_id = front.node().instance_id();
    let (a_id, b_id) = (a.node().instance_id(), b.node().instance_id());
    let front_segment = start(&front, "front", &placement, &events, quick());
    let (a_segment, b_segment, c_segment) = (
        start(&a, "engine", &placement, &events, quick()),
        start(&b, "engine", &placement, &events, quick()),
        start(&c, "engine", &placement, &events, quick()),
    );
    let (front_rx, front_served) = serve(front, front_segment);
    let (a_rx, a_served) = serve(a, a_segment);
    let (b_rx, b_served) = serve(b, b_segment);
    let (c_rx, c_served) = serve(c, c_segment);
    let front_segment = ready(front_rx).await;
    let (_, _, c_segment) = (ready(a_rx).await, ready(b_rx).await, ready(c_rx).await);

    let traffic = Traffic::start(&front_segment, 4);
    traffic.more(20).await;
    a_process.stop();
    let promised = avoided(&hub, a_id, &[front_id]).await;
    assert!(left(a_served).await >= promised);
    traffic.more(20).await;
    b_process.stop();
    let promised = avoided(&hub, b_id, &[front_id]).await;
    assert!(left(b_served).await >= promised);
    let before = c_segment.served().forwards;
    traffic.more(20).await;
    assert!(
        c_segment.served().forwards >= before + 20,
        "the third did not take over"
    );
    assert_eq!(traffic.stop().await, 0, "a request failed");

    front_process.stop();
    c_process.stop();
    left(front_served).await;
    left(c_served).await;
}

/// The last engine instance, stopped while the front serves, is not
/// avoided: the front has nowhere else to send. It keeps taking requests,
/// and its drain ends only at its bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_last_instance_serves_until_its_bound() {
    let placement = two();
    let hub = hub(&placement).await;
    let config = settings(&hub, 1_500, "");
    let events = Events::quiet();
    let ((front_process, front), (engine_process, engine)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
    );
    let front_segment = start(&front, "front", &placement, &events, quick());
    let engine_segment = start(&engine, "engine", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (engine_rx, engine_served) = serve(engine, engine_segment);
    let front_segment = ready(front_rx).await;
    let engine_segment = ready(engine_rx).await;

    let stopped = Instant::now();
    engine_process.stop();
    // Retired, and still the front's only engine: it takes this request.
    let before = engine_segment.served().forwards;
    ask(&front_segment, "after the stop")
        .await
        .expect("answered");
    assert_eq!(engine_segment.served().forwards, before + 1);
    let exited = left(engine_served).await;
    assert!(
        exited - stopped >= Duration::from_millis(1_400),
        "it left after {:?}, before its bound",
        exited - stopped
    );
    front_process.stop();
    left(front_served).await;
}

/// The front holds an engine's long answer unread when the engine is
/// stopped. The front avoids the engine at once, since another serves, but
/// the engine stays until velo has handed the answer on, which the front's
/// reading allows; the answer arrives whole across the engine's exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retired_instance_stays_until_its_answer_is_handed_on() {
    let placement = two();
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "[min_replicas]\nengine = 2\n");
    let events = Events::new();
    let ((front_process, front), (a_process, a), (b_process, b)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let (front_id, a_id) = (front.node().instance_id(), a.node().instance_id());
    let front_segment = start(&front, "front", &placement, &events, quick());
    let a_segment = start(&a, "engine", &placement, &events, quick());
    let b_segment = start(&b, "engine", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (a_rx, a_served) = serve(a, a_segment);
    let (b_rx, b_served) = serve(b, b_segment);
    let front_segment = ready(front_rx).await;
    let a_segment = ready(a_rx).await;
    let _ = ready(b_rx).await;

    // Far more than velo's credit window lets reach the front unread.
    let plan = Plan {
        tokens: 400,
        ..Plan::default()
    };
    // Round robin: one of two consecutive requests is the first engine's.
    let mut held = None;
    for n in 0..2 {
        let before = a_segment.served().forwards;
        let text = format!("long{n}");
        let out = front_segment
            .call(Cx::new(), prompt(&text, plan.clone()))
            .await
            .expect("the request starts");
        events.wait_for(&format!("D:done:c({text})#0"), WAIT).await;
        if a_segment.served().forwards > before {
            held = Some(out);
        } else {
            assert_eq!(collect(out).await, expected(0..400));
        }
    }
    let out = held.expect("the first engine took one");
    a_process.stop();
    avoided(&hub, a_id, &[front_id]).await;
    // Its grace is 300 ms; it may not leave while the answer is unsent.
    // Absence is shown by a bounded wait: nothing marks it.
    let mut a_served = a_served;
    let early = tokio::time::timeout(Duration::from_secs(1), &mut a_served).await;
    assert!(early.is_err(), "it left with its answer unsent");
    assert_eq!(collect(out).await, expected(0..400), "the answer was cut");
    left(a_served).await;

    front_process.stop();
    b_process.stop();
    left(front_served).await;
    left(b_served).await;
}

/// A joiner ends a lone instance's retirement: the retired engine is the
/// front's only one, so it is not avoided, until a new engine is ready.
/// Then it is, and it leaves long before its bound. No request fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_ends_a_lone_instances_retirement() {
    let placement = two();
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "");
    let events = Events::quiet();
    let ((front_process, front), (old_process, old)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
    );
    let (front_id, old_id) = (front.node().instance_id(), old.node().instance_id());
    let front_segment = start(&front, "front", &placement, &events, quick());
    let old_segment = start(&old, "engine", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (old_rx, old_served) = serve(old, old_segment);
    let front_segment = ready(front_rx).await;
    let _ = ready(old_rx).await;

    let traffic = Traffic::start(&front_segment, 4);
    traffic.more(10).await;
    let stopped = Instant::now();
    old_process.stop();
    traffic.more(10).await;
    let (new_process, new) = launch(&config, "engine", &placement).await;
    let new_segment = start(&new, "engine", &placement, &events, quick());
    let (new_rx, new_served) = serve(new, new_segment);
    let new_segment = ready(new_rx).await;
    let promised = avoided(&hub, old_id, &[front_id]).await;
    let exited = left(old_served).await;
    assert!(exited >= promised);
    assert!(
        exited - stopped < Duration::from_secs(10),
        "it waited for its bound"
    );
    let before = new_segment.served().forwards;
    traffic.more(20).await;
    assert!(new_segment.served().forwards >= before + 20);
    assert_eq!(traffic.stop().await, 0, "a request failed");

    front_process.stop();
    new_process.stop();
    left(front_served).await;
    left(new_served).await;
}

/// A joiner is routed to once it is ready, and not before: a middle
/// instance whose barrier waits for a tail instance that is loaded but not
/// serving has said it serves, not that it is ready, and the front sends
/// it nothing until it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_is_routed_to_once_it_is_ready_and_not_before() {
    let placement = line();
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "");
    let events = Events::quiet();
    let ((front_process, front), (mid_process, mid), (tail_process, tail)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "mid", &placement),
        launch(&config, "tail", &placement),
    );
    let front_segment = start(&front, "front", &placement, &events, quick());
    let mid_segment = start(&mid, "mid", &placement, &events, quick());
    let tail_segment = start(&tail, "tail", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (mid_rx, mid_served) = serve(mid, mid_segment);
    let (tail_rx, tail_served) = serve(tail, tail_segment);
    let front_segment = ready(front_rx).await;
    let _ = (ready(mid_rx).await, ready(tail_rx).await);

    // A second tail, loaded and started, not yet serving; then a second
    // middle, whose roster holds it.
    let (late_tail_process, late_tail) = launch(&config, "tail", &placement).await;
    let late_tail_segment = start(&late_tail, "tail", &placement, &events, quick());
    let (joiner_process, joiner) = launch(&config, "mid", &placement).await;
    // The joiner's own log: what its forward node sees.
    let seen = Events::new();
    let joiner_segment = start(&joiner, "mid", &placement, &seen, quick());
    let (joiner_rx, joiner_served) = serve(joiner, joiner_segment);
    // The joiner announces `serving`, then waits at its barrier.
    let services = client(&hub).services();
    let serving = format!("gdl-serving-{}", hex("mid"));
    let deadline = Instant::now() + WAIT;
    while services
        .get_instances(&serving)
        .await
        .unwrap_or_default()
        .len()
        < 2
    {
        assert!(
            Instant::now() < deadline,
            "the joiner never announced serving"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for n in 0..20 {
        ask(&front_segment, &format!("early{n}"))
            .await
            .expect("answered");
    }
    assert!(
        seen.snapshot().is_empty(),
        "routed to before it was ready: {:?}",
        seen.snapshot()
    );
    let (late_tail_rx, late_tail_served) = serve(late_tail, late_tail_segment);
    let _ = (ready(late_tail_rx).await, ready(joiner_rx).await);
    let deadline = Instant::now() + WAIT;
    let mut n = 0;
    while seen.snapshot().is_empty() {
        assert!(Instant::now() < deadline, "never routed to once ready");
        ask(&front_segment, &format!("late{n}"))
            .await
            .expect("answered");
        n += 1;
    }

    for process in [
        &front_process,
        &mid_process,
        &tail_process,
        &late_tail_process,
        &joiner_process,
    ] {
        process.stop();
    }
    for served in [
        front_served,
        mid_served,
        tail_served,
        late_tail_served,
        joiner_served,
    ] {
        left(served).await;
    }
}

/// A segment's name as its hub services spell it.
fn hex(segment: &str) -> String {
    segment.bytes().map(|b| format!("{b:02x}")).collect()
}

/// Two instances join at once. The new front takes the new middle into its
/// roster from `loaded`, as every instance does at launch, and routes to it
/// as soon as it serves; the middle's own barrier still waits for a tail
/// that is not serving, so the request waits at its held roster, and goes
/// on once that tail serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_waits_at_a_joiners_held_roster() {
    let placement = line();
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "");
    let events = Events::quiet();
    let ((front_process, front), (mid_process, mid), (tail_process, tail)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "mid", &placement),
        launch(&config, "tail", &placement),
    );
    let front_segment = start(&front, "front", &placement, &events, quick());
    let mid_segment = start(&mid, "mid", &placement, &events, quick());
    let tail_segment = start(&tail, "tail", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (mid_rx, mid_served) = serve(mid, mid_segment);
    let (tail_rx, tail_served) = serve(tail, tail_segment);
    let _ = (
        ready(front_rx).await,
        ready(mid_rx).await,
        ready(tail_rx).await,
    );

    let (late_tail_process, late_tail) = launch(&config, "tail", &placement).await;
    let late_tail_segment = start(&late_tail, "tail", &placement, &events, quick());
    let (joiner_process, joiner) = launch(&config, "mid", &placement).await;
    let seen = Events::new();
    // Long enough to hold a request while the test looks.
    let patient = RuntimeConfig {
        setup_timeout: Duration::from_secs(5),
        ..RuntimeConfig::default()
    };
    let joiner_segment = start(&joiner, "mid", &placement, &seen, patient);
    let (joiner_rx, joiner_served) = serve(joiner, joiner_segment);
    let (new_front_process, new_front) = launch(&config, "front", &placement).await;
    let new_front_segment = start(&new_front, "front", &placement, &events, quick());
    let (new_front_rx, new_front_served) = serve(new_front, new_front_segment);
    let new_front_segment = ready(new_front_rx).await;

    // Two requests: round robin sends one to each middle.
    let asked: Vec<JoinHandle<gdl::Result<()>>> = (0..2)
        .map(|n| {
            let front = Arc::clone(&new_front_segment);
            tokio::spawn(async move { ask(&front, &format!("held{n}")).await })
        })
        .collect();
    let deadline = Instant::now() + WAIT;
    while seen.snapshot().is_empty() {
        assert!(
            Instant::now() < deadline,
            "the new front never routed to the joiner"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The joiner ran its forward node, and holds the hand-off onward.
    assert!(
        asked.iter().any(|request| !request.is_finished()),
        "nothing waited at the held roster"
    );
    let (late_tail_rx, late_tail_served) = serve(late_tail, late_tail_segment);
    let _ = (ready(late_tail_rx).await, ready(joiner_rx).await);
    for request in asked {
        tokio::time::timeout(WAIT, request)
            .await
            .expect("answered in time")
            .expect("the request's task")
            .expect("answered whole");
    }

    let processes = [
        &front_process,
        &mid_process,
        &tail_process,
        &late_tail_process,
        &joiner_process,
        &new_front_process,
    ];
    for process in processes {
        process.stop();
    }
    for served in [
        front_served,
        mid_served,
        tail_served,
        late_tail_served,
        joiner_served,
        new_front_served,
    ] {
        left(served).await;
    }
}

/// A front that joins a running deployment serves: its requests cross to
/// the engine and back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_source_serves() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "G"])
        .segment("engine", ["C", "D"])
        .segment("post", ["F", "E"]);
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "");
    let events = Events::quiet();
    let ((front_process, front), (engine_process, engine), (post_process, post)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
        launch(&config, "post", &placement),
    );
    let front_segment = start(&front, "front", &placement, &events, quick());
    let engine_segment = start(&engine, "engine", &placement, &events, quick());
    let post_segment = start(&post, "post", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (engine_rx, engine_served) = serve(engine, engine_segment);
    let (post_rx, post_served) = serve(post, post_segment);
    let _ = (
        ready(front_rx).await,
        ready(engine_rx).await,
        ready(post_rx).await,
    );

    let (joiner_process, joiner) = launch(&config, "front", &placement).await;
    let joiner_segment = start(&joiner, "front", &placement, &events, quick());
    let (joiner_rx, joiner_served) = serve(joiner, joiner_segment);
    let joiner_segment = ready(joiner_rx).await;
    for n in 0..10 {
        ask(&joiner_segment, &format!("joined{n}"))
            .await
            .expect("answered");
    }

    for process in [
        &front_process,
        &engine_process,
        &post_process,
        &joiner_process,
    ] {
        process.stop();
    }
    for served in [front_served, engine_served, post_served, joiner_served] {
        left(served).await;
    }
}

/// A backward segment that joins is opened by the front once it is ready,
/// and the engine attaches to the anchors it mints, though the engine may
/// not have heard of it yet: velo finds it through the hub.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joining_backward_segment_is_opened_and_attached_to() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "G"])
        .segment("engine", ["C", "D"])
        .segment("post", ["F", "E"]);
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "");
    let events = Events::quiet();
    let ((front_process, front), (engine_process, engine), (post_process, post)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
        launch(&config, "post", &placement),
    );
    let front_segment = start(&front, "front", &placement, &events, quick());
    let engine_segment = start(&engine, "engine", &placement, &events, quick());
    let post_segment = start(&post, "post", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (engine_rx, engine_served) = serve(engine, engine_segment);
    let (post_rx, post_served) = serve(post, post_segment);
    let front_segment = ready(front_rx).await;
    let _ = (ready(engine_rx).await, ready(post_rx).await);

    let (joiner_process, joiner) = launch(&config, "post", &placement).await;
    let joiner_segment = start(&joiner, "post", &placement, &events, quick());
    let (joiner_rx, joiner_served) = serve(joiner, joiner_segment);
    let joiner_segment = ready(joiner_rx).await;
    let deadline = Instant::now() + WAIT;
    let mut n = 0;
    while joiner_segment.served().runs == 0 {
        assert!(Instant::now() < deadline, "the joiner was never opened");
        ask(&front_segment, &format!("opened{n}"))
            .await
            .expect("answered");
        n += 1;
    }
    for n in 0..10 {
        ask(&front_segment, &format!("after{n}"))
            .await
            .expect("answered");
    }

    for process in [
        &front_process,
        &engine_process,
        &post_process,
        &joiner_process,
    ] {
        process.stop();
    }
    for served in [front_served, engine_served, post_served, joiner_served] {
        left(served).await;
    }
}

/// A front held at its rendezvous (it wants three engines, and two run)
/// has announced `loaded`, so a retiring engine waits for it to stop
/// routing there. It follows membership from before `loaded`, so it says
/// so at once, and the engine leaves long before its bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_held_at_its_rendezvous_still_avoids_a_retired_instance() {
    let placement = two();
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "[min_replicas]\nengine = 2\n");
    let events = Events::quiet();
    let ((front_process, front), (a_process, a), (b_process, b)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let (front_id, a_id) = (front.node().instance_id(), a.node().instance_id());
    let front_segment = start(&front, "front", &placement, &events, quick());
    let a_segment = start(&a, "engine", &placement, &events, quick());
    let b_segment = start(&b, "engine", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (a_rx, a_served) = serve(a, a_segment);
    let (b_rx, b_served) = serve(b, b_segment);
    let _ = (ready(front_rx).await, ready(a_rx).await, ready(b_rx).await);

    let waiting = settings(&hub, 20_000, "[min_replicas]\nengine = 3\n");
    let held_process = Process::start(&waiting).await.expect("process");
    let held_id = held_process.instance_id();
    let spec = DeploymentSpec::new(reference_manifest(|_| true), placement.clone())
        .expect("valid test fixture");
    let held = tokio::spawn({
        let held_process = held_process.clone();
        async move {
            waiting
                .launch(&held_process, "front", &["front", "engine"], &spec, async {
                    Ok(())
                })
                .await
                .map(|_| ())
        }
    });
    let services = client(&hub).services();
    let loaded = format!("gdl-loaded-{}", hex("front"));
    let deadline = Instant::now() + WAIT;
    while !services
        .get_instances(&loaded)
        .await
        .unwrap_or_default()
        .contains(&held_id)
    {
        assert!(Instant::now() < deadline, "the held front never loaded");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let stopped = Instant::now();
    a_process.stop();
    avoided(&hub, a_id, &[front_id, held_id]).await;
    let exited = left(a_served).await;
    assert!(
        exited - stopped < Duration::from_secs(5),
        "it waited for the held front"
    );
    assert!(!held.is_finished(), "the held front's rendezvous ended");
    held.abort();

    front_process.stop();
    b_process.stop();
    left(front_served).await;
    left(b_served).await;
}

/// A hand-off the front sent to an engine before it stopped routing there,
/// held on its way until the engine has closed, is refused with
/// `Overloaded` rather than admitted and cut. A request the engine had
/// already admitted finishes, and the engine leaves after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hand_off_arriving_after_the_close_is_refused() {
    let placement = two();
    let hub = hub(&placement).await;
    let config = settings(&hub, 20_000, "[min_replicas]\nengine = 2\n");
    let (a_events, b_events) = (Events::new(), Events::new());
    let ((front_process, front), (a_process, a), (b_process, b)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let (front_id, a_id) = (front.node().instance_id(), a.node().instance_id());
    // The front waits as long as a hand-off may take; the engine's grace
    // is short, so the close comes while the hand-off is held.
    let patient = RuntimeConfig {
        setup_timeout: Duration::from_secs(5),
        ..RuntimeConfig::default()
    };
    let gate = Arc::new(gdl::Gate::default());
    let gated = RuntimeConfig {
        faults: gdl::Faults {
            forward_gate: Some(Arc::clone(&gate)),
            ..gdl::Faults::default()
        },
        ..quick()
    };
    let front_segment = start(&front, "front", &placement, &Events::quiet(), patient);
    let a_segment = start(&a, "engine", &placement, &a_events, gated);
    let b_segment = start(&b, "engine", &placement, &b_events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (a_rx, a_served) = serve(a, a_segment);
    let (b_rx, b_served) = serve(b, b_segment);
    let front_segment = ready(front_rx).await;
    let a_segment = ready(a_rx).await;
    let _ = ready(b_rx).await;

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
    let whole = |answer: gdl::Result<Vec<gdl::Result<common::Chunk>>>| {
        assert_eq!(answer.expect("answered"), expected(0..1));
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
                whole(request.await.expect("the request's task"));
                break;
            }
            done = &mut request => whole(done.expect("the request's task")),
        }
    }
    // The second engine's turn, then the first's: a request it admits,
    // held open at its end.
    whole(
        send(plain.clone(), "second", Cx::new())
            .await
            .expect("the request's task"),
    );
    let held_cx = Cx::new();
    let hold = Plan {
        tokens: 1,
        hold_at_end: true,
        ..Plan::default()
    };
    let admitted = send(hold, "hold", held_cx.clone());
    tokio::time::timeout(WAIT, gate.arrived.notified())
        .await
        .expect("the held request reached the first engine");
    gate.release.notify_one();
    a_events.wait_for("D:held:c(hold)#0", WAIT).await;
    // The second's turn again, then one held on its way to the first.
    whole(
        send(plain.clone(), "third", Cx::new())
            .await
            .expect("the request's task"),
    );
    let late = send(plain, "late", Cx::new());
    tokio::time::timeout(WAIT, gate.arrived.notified())
        .await
        .expect("the late request reached the first engine");

    a_process.stop();
    avoided(&hub, a_id, &[front_id]).await;
    let deadline = Instant::now() + WAIT;
    while !a_segment.is_closed() {
        assert!(Instant::now() < deadline, "the engine never closed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let served = a_segment.served().forwards;
    gate.release.notify_one();
    let refused = tokio::time::timeout(WAIT, late)
        .await
        .expect("answered in time")
        .expect("the request's task");
    assert_eq!(
        refused.err().map(|error| error.kind()),
        Some(ErrorKind::Overloaded),
        "a hand-off after the close was not refused"
    );
    assert_eq!(
        a_segment.served().forwards,
        served,
        "the late hand-off was taken"
    );
    held_cx.request_stop();
    let answer = tokio::time::timeout(WAIT, admitted)
        .await
        .expect("answered in time")
        .expect("the request's task")
        .expect("answered");
    assert!(answer.iter().all(Result::is_ok), "{answer:?}");
    assert!(
        b_events.has("D:done:c(second)#0"),
        "the turns were not as expected"
    );
    left(a_served).await;

    front_process.stop();
    b_process.stop();
    left(front_served).await;
    left(b_served).await;
}

/// Over a roster directory: one of two engine instances is stopped while
/// requests flow. The front avoids it with a file, it leaves long before
/// its bound, and no request fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_a_roster_directory_one_of_two_engines_retires() {
    let placement = two();
    let dir = tempfile::Builder::new()
        .prefix("gdl-rosters-")
        .tempdir()
        .expect("roster directory");
    let text = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[instances]\nfront = 1\nengine = 2\n[runtime]\ndrain_ms = 20000\nsetup_timeout_ms = 300\n",
        dir.path().display()
    );
    let config: ProcessConfig<Nothing> =
        ProcessConfig::from_figment(figment::Figment::from(Toml::string(&text))).expect("settings");
    let events = Events::quiet();
    let ((front_process, front), (a_process, a), (b_process, b)) = tokio::join!(
        launch(&config, "front", &placement),
        launch(&config, "engine", &placement),
        launch(&config, "engine", &placement),
    );
    let (front_id, a_id) = (front.node().instance_id(), a.node().instance_id());
    let front_segment = start(&front, "front", &placement, &events, quick());
    let a_segment = start(&a, "engine", &placement, &events, quick());
    let b_segment = start(&b, "engine", &placement, &events, quick());
    let (front_rx, front_served) = serve(front, front_segment);
    let (a_rx, a_served) = serve(a, a_segment);
    let (b_rx, b_served) = serve(b, b_segment);
    let front_segment = ready(front_rx).await;
    let _ = (ready(a_rx).await, ready(b_rx).await);

    let traffic = Traffic::start(&front_segment, 4);
    traffic.more(20).await;
    let stopped = Instant::now();
    a_process.stop();
    let exited = left(a_served).await;
    assert!(
        exited - stopped < Duration::from_secs(5),
        "it waited for its bound"
    );
    let promise = dir
        .path()
        .join("avoids")
        .join(a_id.to_string())
        .join(format!("front.{front_id}"));
    assert!(
        promise.exists(),
        "the front left no promise at {}",
        promise.display()
    );
    traffic.more(20).await;
    assert_eq!(traffic.stop().await, 0, "a request failed");

    front_process.stop();
    b_process.stop();
    left(front_served).await;
    left(b_served).await;
}
