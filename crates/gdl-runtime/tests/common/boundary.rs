//! The reference graph deployed in two classes, for the tests of a
//! chooser's routing across cells: `global` runs `a` (A and G: the source,
//! whose instance chooses), and `model_x` runs `f` (B, C, E, and F: the
//! cell's entry) and `d` (D). Hop 0 crosses into `model_x` at its inner
//! end, B. Every instance is a process of its own, in this test process.

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use gdl::{Cx, Next};
use gdl_hub::{CellReadiness, CellTiming};
use gdl_model::CellSettings;
use gdl_runtime::velo::Velo;
use gdl_runtime::{
    Cells, Deployment, DeploymentSpec, Placement, Process, ProcessConfig, Register, RuntimeConfig,
    Segment,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use velo_hub::Hub;

use super::cells::{
    Nothing, REGION, Resolving, cell_hub_proxied, cell_hub_timed, model_cell, settings, timing,
};
use super::proxy::Proxy;
use super::{Chunk, Events, Plan, Prompt, WAIT, collect, expected, prompt, reference_graph};

/// Every segment of [`placement`].
pub const SEGMENTS: &[&str] = &["a", "f", "d"];

/// `a` in `global`; `f` and `d` in `model_x`.
pub fn placement() -> Placement {
    Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "C", "E", "F"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"])
}

/// The reference graph under `placement`.
pub fn spec_of(placement: Placement) -> DeploymentSpec {
    DeploymentSpec::new(super::reference_manifest(|_| true), placement).expect("valid test fixture")
}

/// The settings of the hub of the model cell `name`, reached under the
/// name `hub`: `f` needs `entries` usable instances to be ready, `d` one.
pub fn model_hub_needing(name: &str, hub: &str, entries: u32) -> CellSettings {
    toml::from_str(&format!(
        "name = '{name}'\nclass = 'model_x'\nregion = '{REGION}'\nhub = '{hub}'\n\
         directory = 'memory'\n[ready]\nf = {entries}\nd = 1\n"
    ))
    .expect("valid test fixture")
}

/// What an instance's settings add for every test here: a short setup
/// timeout and drain, so that an instance that stops leaves soon.
pub const RUNTIME: &str = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 2000\n";

/// One instance of a segment, serving.
pub struct Running<H> {
    pub process: Process,
    pub segment: Arc<Segment<H>>,
    /// The tables of a chooser.
    pub cells: Option<Cells>,
    /// The instance's velo node.
    pub node: Velo,
    /// While true, the instance's serving body, once it is told to stop,
    /// waits before it returns, as a body that finishes its own work does.
    pub body: Arc<tokio::sync::watch::Sender<bool>>,
    serving: JoinHandle<gdl::Result<()>>,
}

impl<H> Running<H> {
    /// How many forward hand-offs the instance took.
    pub fn forwards(&self) -> usize {
        self.segment.served().forwards
    }

    /// How many forward hand-offs reached the instance, taken or refused.
    pub fn offered(&self) -> usize {
        self.segment.offered()
    }

    /// Stop the instance and wait until it has left.
    pub async fn stop(self) {
        self.process.stop();
        let _ = tokio::time::timeout(WAIT * 2, self.serving)
            .await
            .expect("the instance left in time");
    }

    /// Wait until the instance has ended on its own, within `within`, and
    /// return how its serving ended.
    pub async fn ended(self, within: Duration) -> gdl::Result<()> {
        tokio::time::timeout(within, self.serving)
            .await
            .expect("the instance ended in time")
            .expect("the instance's task")
    }
}

/// Launch an instance of `segment` of `spec` with `config`, start the
/// reference graph on it with `runtime`, and serve it until its process
/// stops. Returns once it serves.
pub async fn run(
    config: &ProcessConfig<Nothing>,
    segment: &'static str,
    spec: &DeploymentSpec,
    runtime: RuntimeConfig,
    events: &Arc<Events>,
) -> gdl::Result<Running<impl Next<Prompt, Chunk> + Register + use<>>> {
    run_graph(config, segment, spec, runtime, &|deployment| {
        reference_graph(deployment, events)
    })
    .await
}

/// [`run`] for any graph of `spec`, whose root `assemble` builds.
pub async fn run_graph<H: Register + Send + Sync + 'static>(
    config: &ProcessConfig<Nothing>,
    segment: &'static str,
    spec: &DeploymentSpec,
    runtime: RuntimeConfig,
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> gdl::Result<Running<H>> {
    let process = Process::start(config).await?;
    let launch = config
        .launch(&process, segment, SEGMENTS, spec, async { Ok(()) })
        .await?;
    let cells = launch.cells();
    let node = launch.node();
    let deployment = Deployment::new(
        launch.node(),
        &spec.graph,
        &spec.placement,
        segment,
        launch.roster(),
        runtime,
    )?;
    let root = assemble(&deployment)?;
    let started = deployment.start(root)?;
    let (tx, rx) = oneshot::channel();
    let body = Arc::new(tokio::sync::watch::Sender::new(false));
    let held = body.subscribe();
    let serving = tokio::spawn(launch.serve(started, |segment, shutdown| async move {
        let _ = tx.send(segment);
        shutdown.cancelled().await;
        let mut held = held;
        let _ = held.wait_for(|held| !*held).await;
        Ok(())
    }));
    let segment = tokio::time::timeout(WAIT, rx)
        .await
        .expect("the instance served in time")
        .expect("the instance served");
    Ok(Running {
        process,
        segment,
        cells,
        node,
        body,
        serving,
    })
}

/// A model cell of `spec`: its hub, its entry instances, and its `d`.
pub struct ModelCell<H> {
    pub hub: Hub,
    /// The cell's extension on its hub.
    pub readiness: Arc<CellReadiness<Resolving>>,
    pub entries: Vec<Running<H>>,
    pub sink: Running<H>,
}

impl<H> ModelCell<H> {
    /// The forward hand-offs its entry instances took, together.
    pub fn forwards(&self) -> usize {
        self.entries.iter().map(Running::forwards).sum()
    }
}

/// Start the model cell `name`, whose hub is reached under `hub_name`
/// and needs `quorum` entry instances, with `entries` entry instances and
/// one `d`, all serving.
pub async fn model(
    spec: &DeploymentSpec,
    directory: &Resolving,
    name: &str,
    hub_name: &str,
    quorum: u32,
    entries: usize,
    events: &Arc<Events>,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    let cell = Model {
        name,
        hub: hub_name,
        quorum,
        entries,
    };
    model_of(spec, directory, cell, &|runtime| runtime, &|deployment| {
        reference_graph(deployment, events)
    })
    .await
}

/// What a model cell has: its name, the name its hub is reached under,
/// how many entry instances it needs, and how many it starts.
#[derive(Clone, Copy)]
pub struct Model<'a> {
    pub name: &'a str,
    pub hub: &'a str,
    pub quorum: u32,
    pub entries: usize,
}

/// [`model`] for any graph of `spec`, whose root `assemble` builds, with
/// each entry instance's runtime settings changed by `entry`. Its load
/// summary period is the shortest, [`SUMMARY`]: a chooser sends only to a
/// cell whose summary it has.
pub async fn model_of<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    entry: &(dyn Fn(RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelCell<H> {
    model_summarized(spec, directory, cell, RUNTIME, entry, assemble).await
}

/// [`model`] with the default load summary period, 10 s, so that its
/// hub makes no summary within a test, and a chooser has none to choose
/// it by.
pub async fn model_unsummarized(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    events: &Arc<Events>,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    let hub = model_hub_needing(cell.name, cell.hub, cell.quorum);
    let extra = format!("{}{RUNTIME}", model_cell(cell.name));
    start_model(
        spec,
        directory,
        cell,
        (hub, timing()),
        &extra,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// The load summary period of [`model_summarized`]: the shortest.
pub const SUMMARY: Duration = Duration::from_secs(1);

/// [`model_of`], with the shortest load summary period, [`SUMMARY`], for
/// the cell's hub and its instances, a heartbeat as long, so that the
/// record carries each summary soon after it is made, and `runtime` as
/// every instance's `[runtime]` table.
pub async fn model_summarized<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    runtime: &str,
    entry: &(dyn Fn(RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelCell<H> {
    let (hub, timing, extra) = summarized(cell, runtime);
    start_model(
        spec,
        directory,
        cell,
        (hub, timing),
        &extra,
        entry,
        assemble,
    )
    .await
}

/// [`model_summarized`], with the cell's hub reached through a proxy by
/// the hubs that follow it ([`cell_hub_proxied`]).
pub async fn model_proxied<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    runtime: &str,
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> (ModelCell<H>, Proxy) {
    let (settings_of_hub, timing, extra) = summarized(cell, runtime);
    let (hub, readiness, proxy) = cell_hub_proxied(spec, settings_of_hub, directory, timing).await;
    let cell = instances(spec, (hub, readiness), cell, &extra, &|_, r| r, assemble).await;
    (cell, proxy)
}

/// The settings of [`model_summarized`]'s hub, its timing, and what it
/// adds to every instance's settings.
fn summarized(cell: Model<'_>, runtime: &str) -> (CellSettings, CellTiming, String) {
    let hub = CellSettings {
        summary: SUMMARY,
        ..model_hub_needing(cell.name, cell.hub, cell.quorum)
    };
    let timing = CellTiming {
        heartbeat: SUMMARY,
        ..timing()
    };
    let extra = format!(
        "{}summary_ms = {}\n{runtime}",
        model_cell(cell.name),
        SUMMARY.as_millis()
    );
    (hub, timing, extra)
}

/// Start the model cell `cell` with its hub's settings and timing, and
/// `extra` added to every instance's settings.
async fn start_model<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    (settings_of_hub, timing): (CellSettings, CellTiming),
    extra: &str,
    entry: &(dyn Fn(RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelCell<H> {
    let hub = cell_hub_timed(spec, settings_of_hub, directory, timing).await;
    let configure = |segment: &str, runtime| match segment {
        "f" => entry(runtime),
        _ => runtime,
    };
    instances(spec, hub, cell, extra, &configure, assemble).await
}

/// [`model_summarized`] with [`RUNTIME`], each instance's runtime
/// settings changed by `configure`, given the instance's segment.
pub async fn model_configured<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    configure: &(dyn Fn(&str, RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelCell<H> {
    let (settings_of_hub, timing, extra) = summarized(cell, RUNTIME);
    let hub = cell_hub_timed(spec, settings_of_hub, directory, timing).await;
    instances(spec, hub, cell, &extra, configure, assemble).await
}

/// Start `cell`'s instances at its hub, each with `extra` added to its
/// settings.
async fn instances<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    (hub, readiness): (Hub, Arc<CellReadiness<Resolving>>),
    cell: Model<'_>,
    extra: &str,
    configure: &(dyn Fn(&str, RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelCell<H> {
    let entries = cell.entries;
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        extra,
    );
    // Each instance's launch waits for the others of its class.
    let mut segments = vec!["d"];
    segments.extend(std::iter::repeat_n("f", entries));
    let mut running = join_all(segments.into_iter().map(|segment| {
        let config = &config;
        async move {
            let runtime = configure(segment, config.runtime.clone());
            run_graph(config, segment, spec, runtime, assemble)
                .await
                .expect("a model instance serves")
        }
    }))
    .await;
    let entries = running.split_off(1);
    let sink = running.pop().expect("the sink");
    ModelCell {
        hub,
        readiness,
        entries,
        sink,
    }
}

/// A request through the source that streams three items, and its
/// answer: `Ok` when every item came, the error otherwise. The whole
/// request is bounded by [`WAIT`].
pub async fn ask<H: Next<Prompt, Chunk>>(front: &Segment<H>) -> gdl::Result<()> {
    let plan = Plan {
        tokens: 3,
        ..Plan::default()
    };
    let answer = tokio::time::timeout(WAIT, async {
        let out = front.call(Cx::new(), prompt("across", plan)).await?;
        Ok::<_, gdl::Error>(collect(out).await)
    })
    .await
    .expect("the request was answered in time")?;
    if let Some(Err(error)) = answer.iter().find(|item| item.is_err()) {
        return Err(error.clone());
    }
    assert_eq!(answer, expected(0..3));
    Ok(())
}
