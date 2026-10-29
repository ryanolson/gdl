//! The reference graph deployed in two classes, for the tests of a
//! chooser's routing across pools: `global` runs `a` (A and G: the source,
//! whose instance chooses), and `model_x` runs `f` (B, C, E, and F: the
//! pool's entry) and `d` (D). Hop 0 crosses into `model_x` at its inner
//! end, B. Every instance is a process of its own, in this test process.

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use gdl::{Cx, Next};
use gdl_hub::{PoolReadiness, PoolTiming};
use gdl_model::PoolSettings;
use gdl_runtime::velo::Velo;
use gdl_runtime::{
    Deployment, DeploymentSpec, Placement, Pools, Process, ProcessConfig, Register, RuntimeConfig,
    Segment,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use velo_hub::Hub;

use super::pools::{
    GLOBAL, Nothing, REGION, Resolving, model_pool, pool_hub_proxied, pool_hub_timed, settings,
    timing,
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

/// [`placement`], choosing a pool of `model_x` by `home_first`, which
/// chooses by the pools' load summaries, rather than by the default,
/// `round_robin`, which reads none.
pub fn home_first() -> Placement {
    placement().pool_policy(
        "model_x",
        gdl_model::PoolPolicy {
            policy: "home_first".into(),
        },
    )
}

/// The reference graph under `placement`.
pub fn spec_of(placement: Placement) -> DeploymentSpec {
    DeploymentSpec::new(super::reference_manifest(|_| true), placement).expect("valid test fixture")
}

/// The settings of the hub of the model pool `name`, a child of
/// [`GLOBAL`], reached under the name `hub`: `f` needs `entries` usable
/// instances to be ready, `d` one.
pub fn model_hub_needing(name: &str, hub: &str, entries: u32) -> PoolSettings {
    toml::from_str(&format!(
        "name = '{name}'\nclass = 'model_x'\nregion = '{REGION}'\nhome = '{GLOBAL}'\n\
         hub = '{hub}'\ndirectory = 'memory'\n[ready]\nf = {entries}\nd = 1\n"
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
    pub pools: Option<Pools>,
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
    run_graph_in(config, segment, SEGMENTS, spec, runtime, assemble).await
}

/// [`run_graph`] for a deployment of the segments `segments`.
pub async fn run_graph_in<H: Register + Send + Sync + 'static>(
    config: &ProcessConfig<Nothing>,
    segment: &'static str,
    segments: &'static [&'static str],
    spec: &DeploymentSpec,
    runtime: RuntimeConfig,
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> gdl::Result<Running<H>> {
    let process = Process::start(config).await?;
    let launch = config
        .launch(&process, segment, segments, spec, async { Ok(()) })
        .await?;
    let pools = launch.pools();
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
        pools,
        node,
        body,
        serving,
    })
}

/// A model pool of `spec`: its hub, its entry instances, and its `d`.
pub struct ModelPool<H> {
    pub hub: Hub,
    /// The pool's extension on its hub.
    pub readiness: Arc<PoolReadiness<Resolving>>,
    pub entries: Vec<Running<H>>,
    pub sink: Running<H>,
}

impl<H> ModelPool<H> {
    /// The forward hand-offs its entry instances took, together.
    pub fn forwards(&self) -> usize {
        self.entries.iter().map(Running::forwards).sum()
    }
}

/// Start the model pool `name`, whose hub is reached under `hub_name`
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
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    let pool = Model {
        name,
        hub: hub_name,
        quorum,
        entries,
    };
    model_of(spec, directory, pool, &|runtime| runtime, &|deployment| {
        reference_graph(deployment, events)
    })
    .await
}

/// What a model pool has: its name, the name its hub is reached under,
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
/// summary period is the shortest, [`SUMMARY`]: a `home_first` chooser
/// sends only to a pool whose summary it has.
pub async fn model_of<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    entry: &(dyn Fn(RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelPool<H> {
    model_summarized(spec, directory, pool, RUNTIME, entry, assemble).await
}

/// [`model`] with the default load summary period, 10 s, so that its
/// hub makes no summary within a test, and a chooser has none to choose
/// it by.
pub async fn model_unsummarized(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    events: &Arc<Events>,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    model_changed(spec, directory, pool, &|hub| hub, events).await
}

/// [`model_unsummarized`], with its hub's settings changed by `change`: a
/// pool with another parent, or none, or in another region.
pub async fn model_changed(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    change: &dyn Fn(PoolSettings) -> PoolSettings,
    events: &Arc<Events>,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    let hub = change(model_hub_needing(pool.name, pool.hub, pool.quorum));
    let extra = format!("{}{RUNTIME}", model_pool(pool.name));
    start_model(
        spec,
        directory,
        pool,
        (hub, timing()),
        &extra,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// [`model_of`] for a pool in `region` rather than [`REGION`]: a pool in
/// another region than its parent's choosers.
pub async fn model_in(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    region: &str,
    events: &Arc<Events>,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    let (hub, timing, extra) = summarized(pool, RUNTIME);
    let hub = PoolSettings {
        region: region.into(),
        ..hub
    };
    start_model(
        spec,
        directory,
        pool,
        (hub, timing),
        &extra,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// [`model`], with its hub's timing changed by `change`.
pub async fn model_timed(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    change: &dyn Fn(PoolTiming) -> PoolTiming,
    events: &Arc<Events>,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    let (hub, timing, extra) = summarized(pool, RUNTIME);
    start_model(
        spec,
        directory,
        pool,
        (hub, change(timing)),
        &extra,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// The load summary period of [`model_summarized`]: the shortest.
pub const SUMMARY: Duration = Duration::from_secs(1);

/// [`model_of`], with the shortest load summary period, [`SUMMARY`], for
/// the pool's hub and its instances, a heartbeat as long, so that the
/// record carries each summary soon after it is made, and `runtime` as
/// every instance's `[runtime]` table.
pub async fn model_summarized<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    runtime: &str,
    entry: &(dyn Fn(RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelPool<H> {
    let (hub, timing, extra) = summarized(pool, runtime);
    start_model(
        spec,
        directory,
        pool,
        (hub, timing),
        &extra,
        entry,
        assemble,
    )
    .await
}

/// [`model_summarized`], with the pool's hub reached through a proxy by
/// the hubs that follow it ([`pool_hub_proxied`]).
pub async fn model_proxied<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    runtime: &str,
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> (ModelPool<H>, Proxy) {
    model_proxied_leased(spec, directory, pool, runtime, timing().lease, assemble).await
}

/// [`model_proxied`], whose hub's lease, its record's and its followers'
/// watches', is `lease`.
pub async fn model_proxied_leased<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    runtime: &str,
    lease: Duration,
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> (ModelPool<H>, Proxy) {
    let (settings_of_hub, timing, extra) = summarized(pool, runtime);
    let timing = PoolTiming { lease, ..timing };
    let (hub, readiness, proxy) = pool_hub_proxied(spec, settings_of_hub, directory, timing).await;
    let pool = instances(
        spec,
        (hub, readiness),
        None,
        pool,
        &extra,
        &|_, r| r,
        assemble,
    )
    .await;
    (pool, proxy)
}

/// The settings of [`model_summarized`]'s hub, its timing, and what it
/// adds to every instance's settings.
fn summarized(pool: Model<'_>, runtime: &str) -> (PoolSettings, PoolTiming, String) {
    let hub = PoolSettings {
        layout: None,
        summary: SUMMARY,
        ..model_hub_needing(pool.name, pool.hub, pool.quorum)
    };
    let timing = PoolTiming {
        heartbeat: SUMMARY,
        ..timing()
    };
    let extra = format!(
        "{}summary_ms = {}\n{runtime}",
        model_pool(pool.name),
        SUMMARY.as_millis()
    );
    (hub, timing, extra)
}

/// Start the model pool `pool` with its hub's settings and timing, and
/// `extra` added to every instance's settings.
async fn start_model<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    (settings_of_hub, timing): (PoolSettings, PoolTiming),
    extra: &str,
    entry: &(dyn Fn(RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelPool<H> {
    let hub = pool_hub_timed(spec, settings_of_hub, directory, timing).await;
    let configure = |segment: &str, runtime| match segment {
        "f" => entry(runtime),
        _ => runtime,
    };
    instances(spec, hub, None, pool, extra, &configure, assemble).await
}

/// [`model_summarized`] with [`RUNTIME`], each instance's runtime
/// settings changed by `configure`, given the instance's segment.
pub async fn model_configured<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    configure: &(dyn Fn(&str, RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelPool<H> {
    let (settings_of_hub, timing, extra) = summarized(pool, RUNTIME);
    let hub = pool_hub_timed(spec, settings_of_hub, directory, timing).await;
    instances(spec, hub, None, pool, &extra, configure, assemble).await
}

/// [`model_summarized`], whose instances reach their hub's public
/// listener through a proxy, which the hubs that follow the pool do not.
pub async fn model_fronted<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    runtime: &str,
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> (ModelPool<H>, Proxy) {
    let (settings_of_hub, timing, extra) = summarized(pool, runtime);
    let hub = pool_hub_timed(spec, settings_of_hub, directory, timing).await;
    let proxy = Proxy::new(hub.0.public_addr()).await;
    let pool = instances(
        spec,
        hub,
        Some(proxy.port),
        pool,
        &extra,
        &|_, r| r,
        assemble,
    )
    .await;
    (pool, proxy)
}

/// Start `pool`'s instances at its hub, each with `extra` added to its
/// settings, reaching the hub's public listener on `public` if given.
async fn instances<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    (hub, readiness): (Hub, Arc<PoolReadiness<Resolving>>),
    public: Option<u16>,
    pool: Model<'_>,
    extra: &str,
    configure: &(dyn Fn(&str, RuntimeConfig) -> RuntimeConfig + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> ModelPool<H> {
    let entries = pool.entries;
    let (public, control) = (
        public.unwrap_or(hub.public_addr().port()),
        hub.control_addr().port(),
    );
    // Each instance's launch waits for the others of its class.
    let mut segments = vec!["d"];
    segments.extend(std::iter::repeat_n("f", entries));
    let mut running = join_all(segments.into_iter().map(|segment| async move {
        // The launch reads the runtime the deployment does, as a segment
        // binary's does.
        let mut config = settings(public, control, 5_000, extra);
        config.runtime = configure(segment, config.runtime.clone());
        let runtime = config.runtime.clone();
        run_graph(&config, segment, spec, runtime, assemble)
            .await
            .expect("a model instance serves")
    }))
    .await;
    let entries = running.split_off(1);
    let sink = running.pop().expect("the sink");
    ModelPool {
        hub,
        readiness,
        entries,
        sink,
    }
}

/// A model pool whose class splits the model part of the reference graph
/// over the entry `f` and other segments: its hub, its entry instances,
/// and one instance of each other segment, in the order they were named.
pub struct Spread<H> {
    pub hub: Hub,
    /// The pool's extension on its hub.
    pub readiness: Arc<PoolReadiness<Resolving>>,
    pub entries: Vec<Running<H>>,
    pub interior: Vec<Running<H>>,
}

/// Start the model pool `pool` of a deployment of `segments`, with
/// `pool.entries` instances of `f` and one of each of `interior`, with
/// the shortest load summary period, [`SUMMARY`], and `runtime` as every
/// instance's `[runtime]` table. `process` changes each instance's
/// process settings, given its segment.
#[expect(
    clippy::too_many_arguments,
    reason = "the deployment, the pool, and how each instance differs"
)]
pub async fn model_spread<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    directory: &Resolving,
    pool: Model<'_>,
    segments: &'static [&'static str],
    interior: &[&'static str],
    runtime: &str,
    process: &(dyn Fn(&str, &mut ProcessConfig<Nothing>) + Sync),
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> Spread<H> {
    let (settings_of_hub, timing, extra) = summarized(pool, runtime);
    let (hub, readiness) = pool_hub_timed(spec, settings_of_hub, directory, timing).await;
    let mut all = interior.to_vec();
    all.extend(std::iter::repeat_n("f", pool.entries));
    // Each instance's launch waits for the others of its class.
    let mut running = join_all(all.into_iter().map(|segment| {
        let mut config = settings(
            hub.public_addr().port(),
            hub.control_addr().port(),
            5_000,
            &extra,
        );
        process(segment, &mut config);
        async move {
            let runtime = config.runtime.clone();
            run_graph_in(&config, segment, segments, spec, runtime, assemble)
                .await
                .expect("a model instance serves")
        }
    }))
    .await;
    let entries = running.split_off(interior.len());
    Spread {
        hub,
        readiness,
        entries,
        interior: running,
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
