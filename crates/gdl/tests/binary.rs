//! The lifecycle every segment binary runs, held once in the runtime:
//! `gdl::run_segment` driven by hand-written `SegmentBinary`s, as a
//! generated segment crate implements it. A signal reaches the whole
//! process, so this binary runs its cases one after another in one test,
//! the one that sends a signal first: until a process takes SIGTERM, the
//! default disposition ends it, so that case fails loudly if the signals
//! are taken only after the nodes are built.
#![cfg(unix)]

use std::ffi::OsString;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use gdl::{Builder, Cx, DeploymentSpec, Failure, Placement, Roster, RuntimeConfig, Serving};
use serde::Deserialize;

mod adder {
    gdl::graph! {
        name = "adder", schema = 1;
        source Ask: u32 -> stream u32;
        sink Add: u32 -> stream u32;
        topology: Ask => Add;
    }
}

/// The longest any case waits for something it expects.
const WAIT: Duration = Duration::from_secs(10);

/// The graph's own value for `Add`, which a table may restate but not
/// change.
const GRAPH_CONFIG: &str = "[Add]\noffset = 10\n";

/// Adds `amount`, from its table, and `offset`, from the graph.
struct Add {
    by: u32,
}

#[derive(Deserialize)]
struct AddConfig {
    amount: u32,
    offset: u32,
}

impl gdl::Build for Add {
    type Config = AddConfig;
    async fn build(config: AddConfig) -> gdl::Result<Self> {
        Ok(Self {
            by: config.amount + config.offset,
        })
    }
}

impl gdl::Sink for Add {
    type Req = u32;
    type Item = u32;
    async fn generate(&self, _cx: Cx, req: u32) -> gdl::Result<gdl::Items<u32>> {
        Ok(gdl::items(futures::stream::iter([Ok(req + self.by)])))
    }
}

/// What both test binaries take: `[node.Add]`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    #[serde(default)]
    node: NodeSettings,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {
    #[serde(rename = "Add", default)]
    add: Option<toml::Table>,
}

impl gdl::Hosted for Settings {
    const KEYS: &'static [&'static str] = &["node"];
    const NAMES: &'static [&'static str] = &["Add"];
}

fn deployment() -> gdl::Result<DeploymentSpec> {
    Ok(DeploymentSpec::new(
        adder::manifest(),
        Placement::new().segment("only", ["Ask", "Add"]),
    )?)
}

fn start_adder(
    add: Add,
    node: gdl::velo::Velo,
    roster: &Roster,
    runtime: RuntimeConfig,
) -> gdl::Result<gdl::Segment<impl gdl::Next<u32, u32> + gdl::Register + use<>>> {
    let placement = Placement::new().segment("only", ["Ask", "Add"]);
    let manifest = adder::manifest();
    let deployment = gdl::Deployment::new(node, &manifest, &placement, "only", roster, runtime)?;
    let root = adder::assemble(&deployment, adder::Bindings::new().add(add))?;
    deployment.start(root)
}

/// Set by `Adder::ready`; the serving body checks that it ran first.
static READY: AtomicBool = AtomicBool::new(false);

/// A segment binary hosting `Add`, whose serving body asks for `1 + 15`
/// once and returns.
struct Adder;

impl gdl::SegmentBinary for Adder {
    const PACKAGE: &'static str = "adder-only";
    const SEGMENT: &'static str = "only";
    const SEGMENTS: &'static [&'static str] = &["only"];
    const GRAPH_CONFIG: &'static str = GRAPH_CONFIG;
    type Settings = Settings;
    type Hosts = Add;

    async fn build(settings: Settings, builder: &Builder) -> Result<Add, Failure> {
        builder.build::<Add>("Add", settings.node.add).await
    }

    async fn ready(_add: &Add) -> gdl::Result<()> {
        READY.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        deployment()
    }

    fn start(
        add: Add,
        node: gdl::velo::Velo,
        roster: &Roster,
        runtime: RuntimeConfig,
    ) -> gdl::Result<Serving> {
        let segment = start_adder(add, node, roster, runtime)?;
        Ok(Serving::new(segment, |segment, _shutdown| async move {
            assert!(READY.load(Ordering::SeqCst), "served before ready");
            let items = segment.call::<u32, u32>(Cx::new(), 1).await?;
            let items: Vec<u32> = items.map(|item| item.expect("an item")).collect().await;
            assert_eq!(items, [16], "the table's amount and the graph's offset");
            Ok(())
        }))
    }
}

/// A segment binary that stops itself while its node builds, and whose
/// node never becomes ready: only the stop can end its launch.
struct Stopping;

impl gdl::SegmentBinary for Stopping {
    const PACKAGE: &'static str = "adder-stopping";
    const SEGMENT: &'static str = "only";
    const SEGMENTS: &'static [&'static str] = &["only"];
    const GRAPH_CONFIG: &'static str = GRAPH_CONFIG;
    type Settings = Settings;
    type Hosts = Add;

    async fn build(settings: Settings, builder: &Builder) -> Result<Add, Failure> {
        let status = std::process::Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .expect("kill");
        assert!(status.success());
        builder.build::<Add>("Add", settings.node.add).await
    }

    async fn ready(_add: &Add) -> gdl::Result<()> {
        std::future::pending().await
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        deployment()
    }

    fn start(
        _add: Add,
        _node: gdl::velo::Velo,
        _roster: &Roster,
        _runtime: RuntimeConfig,
    ) -> gdl::Result<Serving> {
        unreachable!("a stopped launch starts nothing")
    }
}

/// A directory with a roster directory in it and `settings.toml`, which
/// names it, then `extra`.
fn settings(extra: &str) -> (tempfile::TempDir, OsString) {
    let dir = tempfile::Builder::new()
        .prefix("gdl-binary-")
        .tempdir()
        .expect("settings directory");
    let roster = dir.path().join("roster");
    std::fs::create_dir(&roster).expect("roster directory");
    let file = dir.path().join("settings.toml");
    std::fs::write(
        &file,
        format!(
            "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n{extra}",
            roster.display()
        ),
    )
    .expect("settings");
    (dir, file.into_os_string())
}

async fn run<B: gdl::SegmentBinary>(args: &[OsString]) -> Result<(), String> {
    tokio::time::timeout(WAIT, gdl::run_segment::<B>(args.to_vec()))
        .await
        .expect("the binary ended in time")
        .map_err(|e| e.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_binary_runs_its_lifecycle() {
    a_stop_while_the_nodes_build_ends_the_launch().await;
    usage_is_named_by_the_package().await;
    a_health_check_reads_the_settings_after_its_flag().await;
    a_conflicting_table_fails_before_the_instance_meets_its_peers().await;
    a_built_segment_serves_and_drains().await;
}

/// The signals are taken before any node is built: a stop then ends the
/// launch, rather than the process the default way.
async fn a_stop_while_the_nodes_build_ends_the_launch() {
    let (_dir, file) = settings("[node.Add]\namount = 5\n");
    let error = run::<Stopping>(&[file]).await.expect_err("stopped");
    assert!(error.contains("stopped while starting"), "{error}");
}

async fn usage_is_named_by_the_package() {
    let error = run::<Adder>(&[]).await.expect_err("no settings");
    assert_eq!(
        error,
        "usage: adder-only [--healthcheck] <config.toml> [override.toml ...]"
    );
    let error = run::<Adder>(&["--healthcheck".into()])
        .await
        .expect_err("no settings");
    assert_eq!(
        error,
        "usage: adder-only --healthcheck <config.toml> [override.toml ...]"
    );
}

/// `--healthcheck` reads the files after it, and asks the listener they
/// name: a port chosen at bind time is refused before anything is dialled.
async fn a_health_check_reads_the_settings_after_its_flag() {
    let (_dir, file) = settings("[system]\nport = 0\n");
    let error = run::<Adder>(&["--healthcheck".into(), file])
        .await
        .expect_err("no fixed port");
    assert!(error.contains("the system listener's port is 0"), "{error}");
}

/// A table that changes a graph value fails its node's build, named, and
/// before the instance binds or announces anything.
async fn a_conflicting_table_fails_before_the_instance_meets_its_peers() {
    let (dir, file) = settings("[node.Add]\namount = 5\noffset = 3\n");
    let error = run::<Adder>(&[file]).await.expect_err("a conflict");
    assert_eq!(
        error,
        "building Add: node error: config `offset` conflicts with the checked graph"
    );
    let announced: Vec<_> = std::fs::read_dir(dir.path().join("roster"))
        .expect("roster directory")
        .collect();
    assert!(announced.is_empty(), "{announced:?}");
}

/// The table and the graph's values reach the node, the node is ready
/// before the body serves, and the binary ends when its body has and the
/// instance has drained.
async fn a_built_segment_serves_and_drains() {
    let (_dir, file) = settings("[node.Add]\namount = 5\n");
    run::<Adder>(&[file]).await.expect("served");
    assert!(READY.load(Ordering::SeqCst));
}
