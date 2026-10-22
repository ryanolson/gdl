//! The lifecycle every segment binary runs, held once in the runtime:
//! `gdl_runtime::run_segment` driven by hand-written `SegmentBinary`s, as a
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
use gdl::Cx;
use gdl_runtime::{Builder, DeploymentSpec, Failure, Placement, Roster, RuntimeConfig, Serving};
use serde::Deserialize;

mod adder {
    gdl_runtime::graph! {
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
    metrics: AddMetrics,
}

/// What `Add` reports.
#[derive(gdl::Metrics)]
struct AddMetrics {
    /// Numbers added.
    added_total: gdl::prometheus::IntCounter,
}

/// A node whose metric of the same name says something else. Its build
/// is refused, so it never counts.
struct Subtract {
    _metrics: SubtractMetrics,
}

/// What `Subtract` reports, under `Add`'s name.
#[derive(gdl::Metrics)]
#[expect(dead_code, reason = "a node that is never built never counts")]
struct SubtractMetrics {
    /// Numbers subtracted.
    added_total: gdl::prometheus::IntCounter,
}

impl gdl::Build for Subtract {
    type Config = toml::Table;
    type Metrics = SubtractMetrics;
    async fn build(_config: toml::Table, metrics: SubtractMetrics) -> gdl::Result<Self> {
        Ok(Self { _metrics: metrics })
    }
}

#[derive(Deserialize)]
struct AddConfig {
    amount: u32,
    offset: u32,
}

impl gdl::Build for Add {
    type Config = AddConfig;
    type Metrics = AddMetrics;
    async fn build(config: AddConfig, metrics: AddMetrics) -> gdl::Result<Self> {
        Ok(Self {
            by: config.amount + config.offset,
            metrics,
        })
    }
}

impl gdl::Sink for Add {
    type Req = u32;
    type Item = u32;
    async fn generate(&self, _cx: Cx, req: u32) -> gdl::Result<gdl::Items<u32>> {
        self.metrics.added_total.inc();
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

impl gdl_runtime::Hosted for Settings {
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
    node: gdl_runtime::velo::Velo,
    roster: &Roster,
    runtime: RuntimeConfig,
) -> gdl::Result<gdl_runtime::Segment<impl gdl::Next<u32, u32> + gdl_runtime::Register + use<>>> {
    let placement = Placement::new().segment("only", ["Ask", "Add"]);
    let manifest = adder::manifest();
    let deployment =
        gdl_runtime::Deployment::new(node, &manifest, &placement, "only", roster, runtime)?;
    let root = adder::assemble(&deployment, adder::Bindings::new().add(add))?;
    deployment.start(root)
}

/// Set by `Adder::ready`; the serving body checks that it ran first.
static READY: AtomicBool = AtomicBool::new(false);

/// A segment binary hosting `Add`, whose serving body asks for `1 + 15`
/// once and returns.
struct Adder;

impl gdl_runtime::SegmentBinary for Adder {
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
        node: gdl_runtime::velo::Velo,
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

impl gdl_runtime::SegmentBinary for Stopping {
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
        _node: gdl_runtime::velo::Velo,
        _roster: &Roster,
        _runtime: RuntimeConfig,
    ) -> gdl::Result<Serving> {
        unreachable!("a stopped launch starts nothing")
    }
}

/// A segment binary that builds `Add`, then `Subtract`, whose metric of
/// the same name disagrees with `Add`'s.
struct Conflicting;

impl gdl_runtime::SegmentBinary for Conflicting {
    const PACKAGE: &'static str = "adder-conflicting";
    const SEGMENT: &'static str = "only";
    const SEGMENTS: &'static [&'static str] = &["only"];
    const GRAPH_CONFIG: &'static str = GRAPH_CONFIG;
    type Settings = Settings;
    type Hosts = Add;

    async fn build(settings: Settings, builder: &Builder) -> Result<Add, Failure> {
        let add = builder.build::<Add>("Add", settings.node.add).await?;
        builder.build::<Subtract>("Subtract", None).await?;
        Ok(add)
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        deployment()
    }

    fn start(
        _add: Add,
        _node: gdl_runtime::velo::Velo,
        _roster: &Roster,
        _runtime: RuntimeConfig,
    ) -> gdl::Result<Serving> {
        unreachable!("a failed build starts nothing")
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

async fn run<B: gdl_runtime::SegmentBinary>(args: &[OsString]) -> Result<(), String> {
    tokio::time::timeout(WAIT, gdl_runtime::run_segment::<B>(args.to_vec()))
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
    nodes_that_define_one_metric_differently_fail_to_build().await;
    a_built_segment_serves_and_drains().await;
    gdls_own_selector_is_built_from_its_table().await;
    a_selector_table_nothing_registers_is_refused().await;
    a_registered_selector_is_built_with_its_settings_and_metrics().await;
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

/// Two nodes may share a metric's name only if they agree on what it is:
/// the build says which two disagree, where prometheus would name neither.
async fn nodes_that_define_one_metric_differently_fail_to_build() {
    let (_dir, file) = settings("[node.Add]\namount = 5\n");
    let error = run::<Conflicting>(&[file]).await.expect_err("a conflict");
    assert_eq!(
        error,
        "building Subtract: its metric `added_total` differs from node Add's: metrics of one name in a segment share their kind, help, labels, and buckets"
    );
}

/// The table and the graph's values reach the node, the node is ready
/// before the body serves, and the binary ends when its body has and the
/// instance has drained.
async fn a_built_segment_serves_and_drains() {
    let (_dir, file) = settings("[node.Add]\namount = 5\n");
    run::<Adder>(&[file]).await.expect("served");
    assert!(READY.load(Ordering::SeqCst));
}

/// Set by `Tagged::ready`; the binary's `start` checks that it ran.
static TAGGED_READY: AtomicBool = AtomicBool::new(false);

/// A selector the binary registers: its tag from `[selector.tagged]`, and
/// a metric of its own.
struct Tagged {
    tag: String,
    _metrics: TaggedMetrics,
}

/// What `Tagged` reports.
#[derive(gdl::Metrics)]
#[expect(dead_code, reason = "registered to be found, never counted")]
struct TaggedMetrics {
    /// Requests routed.
    routed_total: gdl::prometheus::IntCounter,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaggedConfig {
    tag: String,
}

impl gdl::Build for Tagged {
    type Config = TaggedConfig;
    type Metrics = TaggedMetrics;

    async fn build(config: TaggedConfig, metrics: TaggedMetrics) -> gdl::Result<Self> {
        Ok(Self {
            tag: config.tag,
            _metrics: metrics,
        })
    }

    async fn ready(&self) -> gdl::Result<()> {
        assert_eq!(self.tag, "blue", "the selector's table reached it");
        TAGGED_READY.store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl gdl::Selector for Tagged {
    type Head = gdl::Encoded;

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, gdl::Encoded>,
        instances: gdl::Instances<'a>,
    ) -> gdl::Result<gdl::Choice<'a>> {
        Ok(gdl::Choice::new(instances.get(0).expect("never empty")))
    }
}

/// A segment binary that registers `Tagged` beside `Add`, and checks in
/// `start` what reached its runtime.
struct Selecting;

impl gdl_runtime::SegmentBinary for Selecting {
    const PACKAGE: &'static str = "adder-selecting";
    const SEGMENT: &'static str = "only";
    const SEGMENTS: &'static [&'static str] = &["only"];
    const GRAPH_CONFIG: &'static str = GRAPH_CONFIG;
    type Settings = Settings;
    type Hosts = Add;

    async fn build(settings: Settings, builder: &Builder) -> Result<Add, Failure> {
        builder.selector::<Tagged>("tagged").await?;
        builder.build::<Add>("Add", settings.node.add).await
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        deployment()
    }

    fn start(
        add: Add,
        node: gdl_runtime::velo::Velo,
        roster: &Roster,
        runtime: RuntimeConfig,
    ) -> gdl::Result<Serving> {
        assert!(runtime.selectors.contains("tagged"));
        assert!(
            TAGGED_READY.load(Ordering::SeqCst),
            "started before the selector was ready"
        );
        let registry = runtime.metrics.clone().expect("a system listener");
        let routed = registry
            .gather()
            .into_iter()
            .find(|family| family.name().ends_with("routed_total"))
            .expect("the selector's metric");
        let labels = routed.get_metric()[0].get_label();
        assert!(
            labels
                .iter()
                .any(|l| l.name() == "dynamo_endpoint" && l.value() == "tagged"),
            "{labels:?}"
        );
        let segment = start_adder(add, node, roster, runtime)?;
        Ok(Serving::new(segment, |_, _| async { Ok(()) }))
    }
}

/// A binary that registers nothing, and checks which of gdl's selectors
/// its settings built.
struct Plain;

impl gdl_runtime::SegmentBinary for Plain {
    const PACKAGE: &'static str = "adder-plain";
    const SEGMENT: &'static str = "only";
    const SEGMENTS: &'static [&'static str] = &["only"];
    const GRAPH_CONFIG: &'static str = GRAPH_CONFIG;
    type Settings = Settings;
    type Hosts = Add;

    async fn build(settings: Settings, builder: &Builder) -> Result<Add, Failure> {
        builder.build::<Add>("Add", settings.node.add).await
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        deployment()
    }

    fn start(
        add: Add,
        node: gdl_runtime::velo::Velo,
        roster: &Roster,
        runtime: RuntimeConfig,
    ) -> gdl::Result<Serving> {
        assert!(
            runtime.selectors.contains("affinity"),
            "{:?}",
            runtime.selectors
        );
        assert!(!runtime.selectors.contains("least_loaded"));
        let segment = start_adder(add, node, roster, runtime)?;
        Ok(Serving::new(segment, |_, _| async { Ok(()) }))
    }
}

/// `[selector.affinity]` builds gdl's `affinity` with that table; a key it
/// does not take fails the build, named.
async fn gdls_own_selector_is_built_from_its_table() {
    let (_dir, file) = settings("[node.Add]\namount = 5\n[selector.affinity]\nkey = \"session\"\n");
    run::<Plain>(&[file]).await.expect("served");
    let (_dir, file) = settings("[node.Add]\namount = 5\n[selector.affinity]\nkye = \"session\"\n");
    let error = run::<Plain>(&[file]).await.expect_err("a misspelled key");
    assert!(error.starts_with("building selector affinity:"), "{error}");
    assert!(error.contains("kye"), "{error}");
}

/// A table for a selector that the binary does not register and gdl does
/// not have is a mistake, not something to ignore.
async fn a_selector_table_nothing_registers_is_refused() {
    let (_dir, file) = settings("[node.Add]\namount = 5\n[selector.kv]\nblock = 16\n");
    let error = run::<Plain>(&[file])
        .await
        .expect_err("an unknown selector");
    assert!(
        error.contains("`[selector.kv]`") && error.contains("registers no selector"),
        "{error}"
    );
}

/// A selector the binary registers is built through the `Builder` as a
/// node is: from its table, with its metrics under its name, and ready
/// before the segment starts.
async fn a_registered_selector_is_built_with_its_settings_and_metrics() {
    let (_dir, file) = settings(
        "[node.Add]\namount = 5\n[selector.tagged]\ntag = \"blue\"\n[system]\nhost = \"127.0.0.1\"\nport = 0\n",
    );
    run::<Selecting>(&[file]).await.expect("served");
    assert!(TAGGED_READY.load(Ordering::SeqCst));
}
