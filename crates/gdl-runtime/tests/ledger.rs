//! What an instance's `/metrics` says of the work it holds, for an
//! operator that drains the instance before it deletes its pod: the roots
//! its admission ledger holds and the ledger's limit, the egress permits
//! its streams hold, its open anchors, and its phase. Each is read at the
//! scrape from what the instance already counts.

mod common;

use std::time::Duration;

use common::system::get;
use common::{Events, Plan, collect, prompt, reference_graph, reference_manifest};
use figment::providers::{Format, Toml};
use futures::StreamExt;
use gdl::Cx;
use gdl_runtime::{
    Deployment, DeploymentSpec, Hosted, Launch, Placement, Process, ProcessConfig, RuntimeConfig,
};
use serde::Deserialize;
use tokio::time::Instant;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NODES: &'static [&'static str] = &[];
    const ADAPTERS: &'static [&'static str] = &[];
    const GRAPH_NODES: &'static [&'static str] = &[];
}

/// The longest any case waits for something it expects.
const WAIT: Duration = Duration::from_secs(10);

/// The admission limit each instance is given: not the default, so that
/// the scrape is seen to read the instance's own.
const ADMISSION_LIMIT: usize = 8;

/// One instance each of `front` and `side`, each a process of its own with
/// a system listener, met in a roster directory, with the process's
/// registry made for its segment.
async fn launched(
    spec: &DeploymentSpec,
) -> (
    tempfile::TempDir,
    [(Process, Launch, gdl::prometheus::Registry); 2],
) {
    let dir = tempfile::Builder::new()
        .prefix("gdl-ledger-")
        .tempdir()
        .expect("roster directory");
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[instances]\nfront = 1\nside = 1\n[runtime]\ndrain_ms = 10000\n[system]\nport = 0\n",
        dir.path().display()
    );
    let config: ProcessConfig<Nothing> =
        ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings)))
            .expect("settings");
    let (front, side) = (
        Process::start(&config).await.expect("process"),
        Process::start(&config).await.expect("process"),
    );
    let registry = |process: &Process, segment| process.registry(spec, segment).expect("registry");
    let (front_registry, side_registry) = (registry(&front, "front"), registry(&side, "side"));
    let segments: &[&str] = &["front", "side"];
    let (front_launch, side_launch) = tokio::join!(
        config.launch(&front, "front", segments, spec, async { Ok(()) }),
        config.launch(&side, "side", segments, spec, async { Ok(()) })
    );
    (
        dir,
        [
            (front, front_launch.expect("front meets"), front_registry),
            (side, side_launch.expect("side meets"), side_registry),
        ],
    )
}

/// The value of the series of `name`, with the default prefix, in the
/// scrape of `process`; `None` when it has no such series. Every family
/// here has one series per instance, or one per `phase`.
async fn read(process: &Process, name: &str, phase: Option<&str>) -> Option<f64> {
    let address = process.system_address().expect("a listener");
    let (code, scrape) = get(address, "/metrics").await;
    assert_eq!(code, 200, "{scrape}");
    let name = format!("dynamo_component_{name}{{");
    scrape
        .lines()
        .filter(|line| line.starts_with(&name))
        .find(|line| phase.is_none_or(|phase| line.contains(&format!("phase=\"{phase}\""))))
        .map(|line| {
            let (_, value) = line.rsplit_once(' ').expect("a value");
            value.parse().expect("a number")
        })
}

/// Wait until the series of `name` in the scrape of `process` is `value`.
async fn reaches(process: &Process, name: &str, phase: Option<&str>, value: f64) {
    let started = Instant::now();
    loop {
        let now = read(process, name, phase).await;
        if now == Some(value) {
            return;
        }
        assert!(
            started.elapsed() < WAIT,
            "{name} {phase:?} is {now:?}, not {value}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A streamed request held open in the engine holds its root in the
/// engine's ledger and its egress permit, and the front holds the anchor
/// the stream comes to. A stop moves the engine's instance to draining
/// while it still holds them. Once the stream ends and velo has handed its
/// output on, the ledger and the permits are empty, and the phase stays
/// draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the drain gauges: /metrics has no phase, ledger, egress, or anchor series"]
async fn the_scrape_shows_what_an_instance_holds_until_velo_retires_it() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("side", ["C", "D"]);
    let manifest = reference_manifest(|_| true);
    let spec = DeploymentSpec::new(manifest.clone(), placement.clone()).expect("valid fixture");
    let (
        _dir,
        [
            (front, front_launch, front_registry),
            (side, side_launch, side_registry),
        ],
    ) = launched(&spec).await;
    let events = Events::new();
    let start = |launch: &Launch, name: &str, registry: &gdl::prometheus::Registry| {
        let deployment = Deployment::new(
            launch.node(),
            &manifest,
            &placement,
            name,
            launch.roster(),
            RuntimeConfig {
                admission_limit: ADMISSION_LIMIT,
                metrics: Some(registry.clone()),
                ..RuntimeConfig::default()
            },
        )
        .expect("deployment");
        let root = reference_graph(&deployment, &events).expect("graph");
        deployment.start(root).expect("start")
    };
    let front_segment = start(&front_launch, "front", &front_registry);
    let side_segment = start(&side_launch, "side", &side_registry);
    front.serve_metrics(front_registry).expect("served");
    side.serve_metrics(side_registry).expect("served");
    let (holding, held) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let front_served = tokio::spawn(front_launch.serve(
        front_segment,
        move |segment, shutdown| async move {
            let plan = Plan {
                tokens: 2,
                hold_at_end: true,
                ..Plan::default()
            };
            let cx = Cx::new();
            let mut out = segment.call(cx.clone(), prompt("held", plan)).await?;
            for _ in 0..2 {
                tokio::time::timeout(WAIT, out.next())
                    .await
                    .expect("an item in time")
                    .expect("an item")
                    .expect("not an error");
            }
            let _ = holding.send(());
            let _ = released.await;
            cx.request_stop();
            let rest = collect(out).await;
            assert!(rest.iter().all(Result::is_ok), "{rest:?}");
            shutdown.cancelled().await;
            Ok(())
        },
    ));
    let side_served = tokio::spawn(side_launch.serve(side_segment, |_, shutdown| async move {
        shutdown.cancelled().await;
        Ok(())
    }));
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the request is held in time")
        .expect("the front's body ran");

    assert_eq!(
        read(&side, "instance_phase", Some("ready")).await,
        Some(1.0)
    );
    assert_eq!(
        read(&side, "instance_phase", Some("draining")).await,
        Some(0.0)
    );
    assert_eq!(read(&side, "admission_admitted", None).await, Some(1.0));
    assert_eq!(
        read(&side, "admission_limit", None).await,
        Some(ADMISSION_LIMIT as f64)
    );
    assert_eq!(read(&side, "egress_in_use", None).await, Some(1.0));
    let anchors = read(&front, "active_anchors", None).await;
    assert!(
        anchors.is_some_and(|anchors| anchors >= 1.0),
        "the front holds the anchor its response comes to: {anchors:?}"
    );

    // Retired: the side drains, and still holds the request.
    side.stop();
    reaches(&side, "instance_phase", Some("draining"), 1.0).await;
    assert_eq!(
        read(&side, "instance_phase", Some("ready")).await,
        Some(0.0)
    );
    assert_eq!(read(&side, "admission_admitted", None).await, Some(1.0));
    assert_eq!(read(&side, "egress_in_use", None).await, Some(1.0));

    // The stream ends, the front reads it to its end, and velo retires it.
    let _ = release.send(());
    reaches(&side, "admission_admitted", None, 0.0).await;
    reaches(&side, "egress_in_use", None, 0.0).await;
    reaches(&front, "admission_admitted", None, 0.0).await;
    reaches(&front, "active_anchors", None, 0.0).await;
    assert_eq!(
        read(&side, "instance_phase", Some("draining")).await,
        Some(1.0)
    );

    front.stop();
    for served in [front_served, side_served] {
        tokio::time::timeout(WAIT, served)
            .await
            .expect("the instance left")
            .expect("its task")
            .expect("it served");
    }
}
