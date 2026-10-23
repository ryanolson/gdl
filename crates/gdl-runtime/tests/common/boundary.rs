//! The reference graph deployed in two classes, for the tests of a
//! chooser's routing across cells: `global` runs `a` (A and G: the source,
//! whose instance chooses), and `model_x` runs `f` (B, C, E, and F: the
//! cell's entry) and `d` (D). Hop 0 crosses into `model_x` at its inner
//! end, B. Every instance is a process of its own, in this test process.

use std::sync::Arc;

use futures::future::join_all;
use gdl::{Cx, Next};
use gdl_model::CellSettings;
use gdl_runtime::velo::Velo;
use gdl_runtime::{
    Cells, Deployment, DeploymentSpec, Placement, Process, ProcessConfig, Register, RuntimeConfig,
    Segment,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use velo_hub::Hub;

use super::cells::{Nothing, REGION, Resolving, cell_hub_of, model_cell, settings};
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
    let serving = tokio::spawn(launch.serve(started, |segment, shutdown| async move {
        let _ = tx.send(segment);
        shutdown.cancelled().await;
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
        serving,
    })
}

/// A model cell of `spec`: its hub, its entry instances, and its `d`.
pub struct ModelCell<H> {
    pub hub: Hub,
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
/// each entry instance's runtime settings changed by `entry`.
pub async fn model_of<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    entry: &(dyn Fn(RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelCell<H> {
    let Model {
        name,
        hub: hub_name,
        quorum,
        entries,
    } = cell;
    let (hub, _) = cell_hub_of(spec, model_hub_needing(name, hub_name, quorum), directory).await;
    let extra = format!("{}{RUNTIME}", model_cell(name));
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    // Each instance's launch waits for the others of its class.
    let mut segments = vec!["d"];
    segments.extend(std::iter::repeat_n("f", entries));
    let mut running = join_all(segments.into_iter().map(|segment| {
        let config = &config;
        async move {
            let runtime = match segment {
                "f" => entry(config.runtime.clone()),
                _ => config.runtime.clone(),
            };
            run_graph(config, segment, spec, runtime, assemble)
                .await
                .expect("a model instance serves")
        }
    }))
    .await;
    let entries = running.split_off(1);
    let sink = running.pop().expect("the sink");
    ModelCell { hub, entries, sink }
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
