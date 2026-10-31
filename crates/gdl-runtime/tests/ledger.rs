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
/// a system listener and the further `[system]` settings `system`, met in
/// a roster directory, with the process's registry made for its segment.
async fn launched(
    spec: &DeploymentSpec,
    system: &str,
) -> (
    tempfile::TempDir,
    [(Process, Launch, gdl::prometheus::Registry); 2],
) {
    let dir = tempfile::Builder::new()
        .prefix("gdl-ledger-")
        .tempdir()
        .expect("roster directory");
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[instances]\nfront = 1\nside = 1\n[runtime]\ndrain_ms = 10000\n[system]\nport = 0\n{system}",
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
    let scrape = scrape(process).await;
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

/// The value of the series of `name`, with the default prefix, at the
/// entry `endpoint`, in the scrape of `process`.
async fn read_at(process: &Process, name: &str, endpoint: &str) -> Option<f64> {
    let name = format!("dynamo_component_{name}{{");
    let endpoint = format!("dynamo_endpoint=\"{endpoint}\"");
    scrape(process)
        .await
        .lines()
        .filter(|line| line.starts_with(&name) && line.contains(&endpoint))
        .map(|line| {
            let (_, value) = line.rsplit_once(' ').expect("a value");
            value.parse().expect("a number")
        })
        .next()
}

/// What the `/metrics` of `process` answers.
async fn scrape(process: &Process) -> String {
    let address = process.system_address().expect("a listener");
    let (code, scrape) = get(address, "/metrics").await;
    assert_eq!(code, 200, "{scrape}");
    scrape
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
    ) = launched(&spec, "").await;
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
    let off = scrape(&side).await;
    assert!(
        !off.contains("velo_"),
        "velo's metrics are off by default: {off}"
    );
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
    reaches(&side, "active_anchors", None, 0.0).await;
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

/// With `velo_metrics`, each instance also serves velo's own families:
/// under velo's names, not the registry's prefix, with the instance's
/// identity labels, each family once in the scrape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn velos_families_are_served_beside_the_registry_when_asked() {
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
    ) = launched(&spec, "velo_metrics = true\n").await;
    let events = Events::new();
    let start = |launch: &Launch, name: &str, registry: &gdl::prometheus::Registry| {
        let deployment = Deployment::new(
            launch.node(),
            &manifest,
            &placement,
            name,
            launch.roster(),
            RuntimeConfig {
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
    let (answered, answer) = tokio::sync::oneshot::channel();
    let front_served = tokio::spawn(front_launch.serve(
        front_segment,
        move |segment, shutdown| async move {
            let out = segment
                .call(Cx::new(), prompt("counted", Plan::default()))
                .await?;
            let items = collect(out).await;
            assert!(items.iter().all(Result::is_ok), "{items:?}");
            let _ = answered.send(());
            shutdown.cancelled().await;
            Ok(())
        },
    ));
    let side_served = tokio::spawn(side_launch.serve(side_segment, |_, shutdown| async move {
        shutdown.cancelled().await;
        Ok(())
    }));
    tokio::time::timeout(WAIT, answer)
        .await
        .expect("answered in time")
        .expect("the front's body ran");

    for (process, segment) in [(&front, "front"), (&side, "side")] {
        let scrape = scrape(process).await;
        let families: Vec<&str> = scrape
            .lines()
            .filter_map(|line| line.strip_prefix("# TYPE "))
            .filter_map(|line| line.split(' ').next())
            .collect();
        let mut once = families.clone();
        once.sort_unstable();
        once.dedup();
        assert_eq!(once.len(), families.len(), "a family twice: {scrape}");
        for family in [
            "velo_streaming_active_anchors",
            "velo_transport_frames_total",
        ] {
            assert!(
                families.contains(&family),
                "{segment} has no {family}: {scrape}"
            );
        }
        assert!(
            !families.iter().any(|family| family.contains("_velo_")),
            "velo's families take no prefix: {scrape}"
        );
        let worker = format!("worker_id=\"{:032x}\"", process.instance_id().as_u128());
        let component = format!("dynamo_component=\"{segment}\"");
        for line in scrape.lines().filter(|line| line.starts_with("velo_")) {
            assert!(
                line.contains(&worker) && line.contains(&component),
                "{segment}: {line}"
            );
        }
    }

    front.stop();
    side.stop();
    for served in [front_served, side_served] {
        tokio::time::timeout(WAIT, served)
            .await
            .expect("the instance left")
            .expect("its task")
            .expect("it served");
    }
}

/// A `metrics_prefix` under which the runtime's names are served under
/// names that start with `velo_` would serve a family twice once velo's
/// metrics are on: `velo_streaming` serves `active_anchors` as velo's
/// `velo_streaming_active_anchors`. The registry is refused, with the
/// setting on or off, so turning it on cannot change what a fleet serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_runtime_gauge_cannot_collide_with_the_velo_bridge() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("side", ["C", "D"]);
    let spec = DeploymentSpec::new(reference_manifest(|_| true), placement).expect("valid fixture");
    for velo in ["true", "false"] {
        for prefix in ["velo_streaming", "velo"] {
            let dir = tempfile::Builder::new()
                .prefix("gdl-ledger-")
                .tempdir()
                .expect("roster directory");
            let settings = format!(
                "listen = \"127.0.0.1:0\"\nroster = {:?}\n[system]\nport = 0\nvelo_metrics = {velo}\nmetrics_prefix = {prefix:?}\n",
                dir.path().display()
            );
            let config: ProcessConfig<Nothing> =
                ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings)))
                    .expect("settings");
            let process = Process::start(&config).await.expect("process");
            let Err(error) = process.registry(&spec, "front") else {
                panic!("a registry under `{prefix}` with velo_metrics = {velo}");
            };
            let error = error.to_string();
            assert!(
                error.contains(&format!("{prefix:?}"))
                    && error.contains(&format!("`{prefix}_requests_total`")),
                "{error}"
            );
        }
    }
    // A prefix that only starts like velo's is not refused.
    let dir = tempfile::Builder::new()
        .prefix("gdl-ledger-")
        .tempdir()
        .expect("roster directory");
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\n[system]\nport = 0\nvelo_metrics = true\nmetrics_prefix = \"velocity\"\n",
        dir.path().display()
    );
    let config: ProcessConfig<Nothing> =
        ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings)))
            .expect("settings");
    let process = Process::start(&config).await.expect("process");
    process
        .registry(&spec, "front")
        .expect("a registry under `velocity`");
}

/// A request that a source calls in its own segment does not pass through
/// admission or egress, and opens no anchor: a draining instance can hold
/// it while every drain gauge reads 0. `inflight_requests` at the source's
/// endpoint counts it, and the instance has not exited: the gauges show
/// the drain's progress, and only the exit completes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_request_is_not_reported_as_drained_while_its_body_still_holds_it() {
    let placement = Placement::new().segment("only", common::REFERENCE_NODES);
    let manifest = reference_manifest(|_| true);
    let spec = DeploymentSpec::new(manifest.clone(), placement.clone()).expect("valid fixture");
    let dir = tempfile::Builder::new()
        .prefix("gdl-local-drain-")
        .tempdir()
        .expect("roster directory");
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[runtime]\ndrain_ms = 10000\n[system]\nport = 0\n",
        dir.path().display()
    );
    let config: ProcessConfig<Nothing> =
        ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings)))
            .expect("settings");
    let process = Process::start(&config).await.expect("process");
    let registry = process.registry(&spec, "only").expect("registry");
    let launch = config
        .launch(&process, "only", &["only"], &spec, async { Ok(()) })
        .await
        .expect("launch");
    let deployment = Deployment::new(
        launch.node(),
        &manifest,
        &placement,
        "only",
        launch.roster(),
        RuntimeConfig {
            metrics: Some(registry.clone()),
            ..RuntimeConfig::default()
        },
    )
    .expect("deployment");
    let events = Events::new();
    let root = reference_graph(&deployment, &events).expect("graph");
    let segment = deployment.start(root).expect("start");
    process.serve_metrics(registry).expect("served");
    let (holding, held) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let served = tokio::spawn(launch.serve(segment, move |segment, shutdown| async move {
        let cx = Cx::new();
        let mut out = segment
            .call(
                cx.clone(),
                prompt(
                    "local-held",
                    Plan {
                        tokens: 1,
                        hold_at_end: true,
                        ..Plan::default()
                    },
                ),
            )
            .await?;
        assert!(out.next().await.is_some_and(|item| item.is_ok()));
        holding.send(()).expect("the test waits for the request");
        released.await.expect("the test releases the request");
        cx.request_stop();
        assert!(collect(out).await.iter().all(Result::is_ok));
        shutdown.cancelled().await;
        Ok(())
    }));
    tokio::time::timeout(WAIT, held)
        .await
        .expect("the request starts in time")
        .expect("the body holds the request");
    process.stop();
    reaches(&process, "instance_phase", Some("draining"), 1.0).await;
    assert!(!served.is_finished(), "the body still holds its request");
    for gauge in ["admission_admitted", "egress_in_use", "active_anchors"] {
        assert_eq!(
            read(&process, gauge, None).await,
            Some(0.0),
            "{gauge}: admission does not see a local request"
        );
    }
    assert_eq!(
        read_at(&process, "inflight_requests", "A").await,
        Some(1.0),
        "the source's endpoint counts the local request"
    );
    assert!(!served.is_finished(), "the instance has not exited");
    release.send(()).expect("the body waits for release");
    tokio::time::timeout(WAIT, served)
        .await
        .expect("the instance drains")
        .expect("its task")
        .expect("it served");
    assert_eq!(read_at(&process, "inflight_requests", "A").await, Some(0.0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "F3: the producer-anchor release fixture never gives the producer an anchor to release"]
async fn the_producer_anchor_release_fixture_first_exercises_a_producer_owned_anchor() {
    let nodes = [common::velo_node().await, common::velo_node().await];
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("side", ["C", "D"]);
    let registry = gdl::prometheus::Registry::new();
    let events = Events::new();
    let cluster = common::deploy_configured(
        &[
            ("front".to_string(), nodes[0].clone()),
            ("side".to_string(), nodes[1].clone()),
        ],
        &placement,
        &events,
        &|segment| RuntimeConfig {
            metrics: (segment == "side").then(|| registry.clone()),
            ..RuntimeConfig::default()
        },
    );
    let cx = Cx::new();
    let mut out = cluster
        .segment("front")
        .call(
            cx.clone(),
            prompt(
                "producer-anchor",
                Plan {
                    tokens: 2,
                    hold_at_end: true,
                    ..Plan::default()
                },
            ),
        )
        .await
        .expect("request");
    for _ in 0..2 {
        assert!(out.next().await.is_some_and(|item| item.is_ok()));
    }
    assert!(cluster.segment("front").active_anchors() > 0);
    assert_eq!(cluster.segment("side").egress_in_use(), 1);
    let anchors = cluster.segment("side").active_anchors();
    let families = registry.gather();
    let exposed = families
        .iter()
        .find(|family| family.name() == "active_anchors")
        .expect("the producer exposes its anchor gauge")
        .get_metric()[0]
        .get_gauge()
        .value();
    assert_eq!(exposed, anchors as f64);
    cx.request_stop();
    assert!(collect(out).await.iter().all(Result::is_ok));
    assert!(common::settles(&cluster).await);
    assert!(
        anchors > 0,
        "the producer-side zero check starts at zero: {anchors}; it exercises no anchor release"
    );
}
