//! A deployment-wide stop: every instance is stopped at once, and none
//! exits under a request admitted before the stop. A source drains the
//! requests it holds; every other instance keeps serving until each source
//! that can reach it has drained, then until it holds no request.
mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Events, Plan, collect, expected, prompt, reference_graph, reference_manifest};
use figment::providers::{Format, Toml};
use futures::StreamExt;
use gdl::{
    Cx, Deployment, DeploymentSpec, Hosted, Launch, Placement, Process, ProcessConfig,
    RuntimeConfig,
};
use serde::Deserialize;
use tokio::time::Instant;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NAMES: &'static [&'static str] = &[];
}

/// The longest any case waits for something it expects.
const WAIT: Duration = Duration::from_secs(10);

/// One instance each of `front` and `side`, each a process of its own,
/// met in a roster directory, drained within `drain_ms`.
async fn launched(
    placement: &Placement,
    drain_ms: u64,
) -> (tempfile::TempDir, (Process, Launch), (Process, Launch)) {
    let spec = DeploymentSpec::new(reference_manifest(|_| true), placement.clone())
        .expect("valid test fixture");
    let dir = tempfile::Builder::new()
        .prefix("gdl-drain-")
        .tempdir()
        .expect("roster directory");
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[instances]\nfront = 1\nside = 1\n[runtime]\ndrain_ms = {drain_ms}\n",
        dir.path().display()
    );
    let config: ProcessConfig<Nothing> =
        ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings)))
            .expect("settings");
    let (front, side) = (
        Process::start(&config).await.expect("process"),
        Process::start(&config).await.expect("process"),
    );
    let segments: &[&str] = &["front", "side"];
    let (front_launch, side_launch) = tokio::join!(
        config.launch(&front, "front", segments, &spec, async { Ok(()) }),
        config.launch(&side, "side", segments, &spec, async { Ok(()) })
    );
    (
        dir,
        (front, front_launch.expect("front meets")),
        (side, side_launch.expect("side meets")),
    )
}

/// Every instance is stopped while a request streams through both: the
/// request completes, and the side exits only after the front, the source,
/// has drained it.
async fn a_request_in_flight_survives_a_stop_of_everything(placement: Placement) {
    let (_dir, (front, front_launch), (side, side_launch)) = launched(&placement, 10_000).await;
    let manifest = reference_manifest(|_| true);
    let events = Events::new();
    let start = |launch: &Launch, name: &str| {
        let deployment = Deployment::new(
            launch.node(),
            &manifest,
            &placement,
            name,
            launch.roster(),
            RuntimeConfig::default(),
        )
        .expect("deployment");
        let root = reference_graph(&deployment, &events).expect("graph");
        deployment.start(root).expect("start")
    };
    let (front_segment, side_segment) =
        (start(&front_launch, "front"), start(&side_launch, "side"));
    let plan = Plan {
        tokens: 6,
        item_delay_ms: 100,
        ..Plan::default()
    };
    let drained = Arc::new(Mutex::new(None));
    let front_drained = Arc::clone(&drained);
    let both = (front.clone(), side.clone());
    let front_served = front_launch.serve(front_segment, move |segment, _| async move {
        let mut out = segment.call(Cx::new(), prompt("drain", plan)).await?;
        let first = tokio::time::timeout(WAIT, out.next())
            .await
            .expect("the first item in time")
            .expect("a first item");
        // The request is in flight: everything stops at once.
        both.0.stop();
        both.1.stop();
        let mut answer = vec![first];
        answer.extend(collect(out).await);
        assert_eq!(answer, expected(0..6), "the request was cut short");
        *front_drained.lock().expect("lock") = Some(Instant::now());
        Ok(())
    });
    let side_served = async {
        let served = side_launch
            .serve(side_segment, |_, shutdown| async move {
                shutdown.cancelled().await;
                Ok(())
            })
            .await;
        (served, Instant::now())
    };
    let (front_served, side_served) =
        tokio::time::timeout(WAIT, async { tokio::join!(front_served, side_served) })
            .await
            .expect("both stopped in time");
    front_served.expect("the front served");
    let (side_served, side_left) = side_served;
    side_served.expect("the side served");
    let front_drained = drained.lock().expect("lock").expect("the front drained");
    assert!(
        side_left >= front_drained,
        "the side left before the front had drained"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_everything_waits_for_the_source() {
    a_request_in_flight_survives_a_stop_of_everything(
        Placement::new()
            .segment("front", ["A", "B", "F", "E", "G"])
            .segment("side", ["C", "D"]),
    )
    .await;
}

/// A forward path that leaves the front and returns to it makes the two
/// segments send to each other; neither waits for the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_returning_path_drains_without_waiting_on_itself() {
    a_request_in_flight_survives_a_stop_of_everything(
        Placement::new()
            .segment("front", ["A", "B", "D", "F", "E", "G"])
            .segment("side", ["C"]),
    )
    .await;
}

/// A source that never finishes draining holds the others only for the
/// drain's bound; a second stop then ends it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_is_bounded() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("side", ["C", "D"]);
    let (_dir, (front, front_launch), (side, side_launch)) = launched(&placement, 400).await;
    let (front_running, front_runs) = tokio::sync::oneshot::channel();
    let front_served = tokio::spawn(front_launch.serve((), |_, shutdown| async move {
        let _ = front_running.send(());
        shutdown.cancelled().await;
        std::future::pending::<gdl::Result<()>>().await
    }));
    let (side_running, side_runs) = tokio::sync::oneshot::channel();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
    let side_served = tokio::spawn(side_launch.serve((), |_, shutdown| async move {
        let _ = side_running.send(());
        shutdown.cancelled().await;
        let _ = stopped_tx.send(Instant::now());
        Ok(())
    }));
    // Both serve before the stop: a stop while starting ends the launch.
    for runs in [front_runs, side_runs] {
        tokio::time::timeout(WAIT, runs)
            .await
            .expect("serving in time")
            .expect("the body ran");
    }
    front.stop();
    side.stop();
    let stopped = tokio::time::timeout(WAIT, stopped_rx)
        .await
        .expect("the side stopped in time")
        .expect("the side's body ran");
    tokio::time::timeout(WAIT, side_served)
        .await
        .expect("the side left within the wait")
        .expect("the side's task")
        .expect("the side served");
    let waited = stopped.elapsed();
    assert!(
        waited >= Duration::from_millis(350),
        "the side did not wait for the front: {waited:?}"
    );
    front.stop();
    let front_served = tokio::time::timeout(WAIT, front_served)
        .await
        .expect("a second stop ended the front")
        .expect("the front's task");
    assert!(
        front_served.is_err_and(|e| e.to_string().contains("second signal")),
        "the front drained on its own"
    );
}
