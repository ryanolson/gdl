//! A segment's binary, run once here for every segment: the generated
//! crate implements [`SegmentBinary`] with what differs between segments
//! (what it hosts, how it builds them, and what it serves), and its `main`
//! is [`segment_main`]. Code here is compiled and tested once; code
//! emitted into each segment crate is checked only when that crate builds.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::future::Future;
use std::sync::{Arc, Mutex};

use figment::Figment;
use figment::providers::Serialized;
use futures::future::{BoxFuture, LocalBoxFuture};
use velo::Velo;

use crate::config::build_node;
use crate::dist::{Roster, RuntimeConfig};
use crate::process::{Drains, Hosted, Launch, Process, ProcessConfig};
use gdl::Build;
use gdl::Cx;
use gdl::Metrics;
use gdl::Result;
use gdl::{Affinity, LeastLoaded, Selector, Selectors};
use gdl_model::{DeploymentSpec, GraphConfig, MetricField};

/// What one segment's binary hosts and serves: the parts that differ
/// between segments. [`run_segment`] runs the rest, the same for every
/// segment: the arguments and `--healthcheck`, the settings, the signals
/// and the system listener before anything is built, the build, the launch
/// once every hosted implementation is ready, the start, and the serving
/// body until the stop and its drain.
pub trait SegmentBinary {
    /// The binary's name, for its usage line.
    const PACKAGE: &'static str;
    /// This segment's name in the placement.
    const SEGMENT: &'static str;
    /// Every segment of the placement: an instance waits for all of them.
    const SEGMENTS: &'static [&'static str];
    /// The graph's own config values, as TOML: a table per node, by the
    /// node's name. A node's settings table may restate them but not
    /// change them.
    const GRAPH_CONFIG: &'static str;

    /// The tables of what the segment hosts, as its settings name them.
    type Settings: Hosted;
    /// What [`build`](Self::build) makes from them: the hosted nodes, and
    /// the frontend or entry adapters of a segment hosting the source.
    type Hosts;

    /// Build what the segment hosts from its tables, each through
    /// `builder`. Runs before the instance meets its peers, so that a bad
    /// table fails before anyone waits for it.
    fn build(
        settings: Self::Settings,
        builder: &Builder,
    ) -> impl Future<Output = std::result::Result<Self::Hosts, Failure>>;

    /// Resolve once every hosted implementation can take work
    /// ([`Build::ready`]): the instance announces itself as loaded only
    /// then. At once by default, for a segment that hosts nothing to wait
    /// for.
    fn ready(hosts: &Self::Hosts) -> impl Future<Output = Result<()>> {
        let _ = hosts;
        async { Ok(()) }
    }

    /// The deployment the segment belongs to: the contract's manifest and
    /// the placement the crate was generated for.
    fn deployment() -> Result<DeploymentSpec>;

    /// Start the segment's graph on `node`, with every instance in
    /// `roster`, and say what it serves once every segment is ready.
    fn start(
        hosts: Self::Hosts,
        node: Velo,
        roster: &Roster,
        runtime: RuntimeConfig,
    ) -> Result<Serving>;
}

/// Run segment binary `B` with `args`, the arguments after the program's
/// name: `<config.toml> [override.toml ...]` to serve, or `--healthcheck`
/// before them to ask a running instance's health probe instead, as a
/// container health check does.
pub async fn run_segment<B: SegmentBinary>(
    args: impl IntoIterator<Item = OsString>,
) -> std::result::Result<(), Failure> {
    let paths: Vec<OsString> = args.into_iter().collect();
    let Some(first) = paths.first() else {
        return Err(format!(
            "usage: {} [--healthcheck] <config.toml> [override.toml ...]",
            B::PACKAGE
        )
        .into());
    };
    if first == "--healthcheck" {
        if paths.len() == 1 {
            return Err(format!(
                "usage: {} --healthcheck <config.toml> [override.toml ...]",
                B::PACKAGE
            )
            .into());
        }
        ProcessConfig::<B::Settings>::load(&paths[1..])?
            .healthcheck()
            .await?;
        return Ok(());
    }
    let config = ProcessConfig::<B::Settings>::load(&paths)?;
    // First: a stop while the nodes build is taken, and the probes say the
    // process is live while it starts.
    let process = Process::start(&config).await?;
    let (config, settings) = config.into_hosted();
    // The deployment before the build: every metric a node registers
    // carries the deployment's identity.
    let deployment = B::deployment()?;
    let registry = process.registry(&deployment, B::SEGMENT)?;
    let builder = Builder::new(
        B::GRAPH_CONFIG,
        registry.clone(),
        config.selector_tables().clone(),
    )?;
    let hosts = B::build(settings, &builder).await?;
    // gdl's own selectors, configured in the settings and not registered
    // by the binary; a table for anything else is a mistake.
    for name in config.selector_tables().keys() {
        if builder.registered(name) {
            continue;
        }
        match name.as_str() {
            "affinity" => builder.selector::<Affinity>(name).await?,
            "least_loaded" => builder.selector::<LeastLoaded>(name).await?,
            _ => {
                return Err(format!(
                    "the settings configure `[selector.{name}]`, but this binary registers no selector `{name}` and gdl has none"
                )
                .into());
            }
        }
    }
    let ready = async {
        tokio::try_join!(B::ready(&hosts), builder.ready())?;
        Ok(())
    };
    let launch = config
        .launch(&process, B::SEGMENT, B::SEGMENTS, &deployment, ready)
        .await?;
    // The runtime records the segment's figures only where a system
    // listener can serve them: with none, nobody could read them.
    let mut runtime = config.runtime;
    runtime.metrics = process.system_address().map(|_| registry);
    runtime.selectors = builder.into_selectors();
    let serving = B::start(hosts, launch.node(), launch.roster(), runtime)?;
    (serving.serve)(launch).await?;
    Ok(())
}

/// The `main` of segment binary `B`: [`run_segment`] with the process's
/// arguments, to completion on a runtime of its own, a failure printed as
/// `segment: error`, and the process ended. The runtime is shut down in the
/// background rather than waited for: a read of stdin parked on its
/// blocking pool (a JSON-lines source whose input stayed open and idle
/// while a signal stopped the graph) would otherwise hold the process after
/// the graph has stopped, and swallow the next signal.
pub fn segment_main<B: SegmentBinary>() -> std::process::ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("{}: {error}", B::SEGMENT);
            return std::process::ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(run_segment::<B>(std::env::args_os().skip(1)));
    runtime.shutdown_background();
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: {error}", B::SEGMENT);
            std::process::ExitCode::FAILURE
        }
    }
}

/// What ends a segment binary, with its message as printed: a settings,
/// build, launch, or serving error.
pub type Failure = Box<dyn std::error::Error + Send + Sync>;

/// Builds each implementation a segment hosts from its settings table,
/// with the graph's own config values filled in and checked, and its
/// metrics registered. Every hosted implementation is built through one,
/// so what the runtime hands a node at construction is added here, not to
/// every generated crate.
pub struct Builder {
    graph: BTreeMap<String, GraphConfig>,
    registry: prometheus::Registry,
    /// Each metric registered so far, by name, with the node that defined
    /// it first.
    defined: Mutex<BTreeMap<String, (String, MetricField)>>,
    /// `[selector.<name>]` tables, by name.
    tables: BTreeMap<String, toml::Table>,
    /// The routing selectors built so far, and how to wait for each to be
    /// ready.
    selectors: Mutex<(Selectors, Vec<Readiness>)>,
}

/// One built selector's [`Build::ready`].
type Readiness = Box<dyn Fn() -> BoxFuture<'static, Result<()>> + Send>;

impl Builder {
    fn new(
        graph: &str,
        registry: prometheus::Registry,
        tables: BTreeMap<String, toml::Table>,
    ) -> std::result::Result<Self, Failure> {
        Ok(Self {
            graph: toml::from_str(graph)?,
            registry,
            defined: Mutex::new(BTreeMap::new()),
            tables,
            selectors: Mutex::new((Selectors::new(), Vec::new())),
        })
    }

    /// Build `S`, the routing selector named `name` in the placement, from
    /// its settings table `[selector.<name>]`, with its metrics registered
    /// under `name`, and route the crossings that name it through it. The
    /// instance announces itself as loaded only once `S` is ready, as for a
    /// node.
    pub async fn selector<S: Build + Selector>(
        &self,
        name: &str,
    ) -> std::result::Result<(), Failure> {
        gdl_model::check_selector_name(name)
            .map_err(|why| format!("building selector {name}: {why}"))?;
        self.define(name, <S::Metrics as Metrics>::definitions())?;
        let metrics = S::Metrics::register(&self.registry, name)
            .map_err(|e| format!("building selector {name}: registering its metrics: {e}"))?;
        let table = self.tables.get(name).cloned().unwrap_or_default();
        let providers = Figment::from(Serialized::defaults(table));
        let built = build_node::<S>(&providers, &GraphConfig::new(), metrics)
            .await
            .map_err(|e| format!("building selector {name}: {e}"))?;
        let built = Arc::new(built);
        let mut selectors = self.selectors.lock().unwrap_or_else(|e| e.into_inner());
        selectors
            .0
            .add_shared(name, Arc::clone(&built))
            .map_err(|e| format!("building selector {name}: {e}"))?;
        selectors.1.push(Box::new(move || {
            let built = Arc::clone(&built);
            Box::pin(async move { built.ready().await })
        }));
        Ok(())
    }

    /// Whether a selector named `name` is registered.
    fn registered(&self, name: &str) -> bool {
        let selectors = self.selectors.lock().unwrap_or_else(|e| e.into_inner());
        selectors.0.contains(name)
    }

    /// Resolve once every registered selector is ready.
    async fn ready(&self) -> Result<()> {
        let waits: Vec<_> = {
            let selectors = self.selectors.lock().unwrap_or_else(|e| e.into_inner());
            selectors.1.iter().map(|ready| ready()).collect()
        };
        futures::future::try_join_all(waits).await?;
        Ok(())
    }

    /// The registered selectors, for the runtime.
    fn into_selectors(self) -> Selectors {
        let selectors = self
            .selectors
            .into_inner()
            .unwrap_or_else(|e| e.into_inner());
        selectors.0
    }

    /// Build `T`, the implementation of node `node` (as the graph spells
    /// it), from its settings table, with its metrics registered.
    pub async fn build<T: Build>(
        &self,
        node: &str,
        table: Option<toml::Table>,
    ) -> std::result::Result<T, Failure> {
        self.define(node, <T::Metrics as Metrics>::definitions())?;
        let metrics = T::Metrics::register(&self.registry, node)
            .map_err(|e| format!("building {node}: registering its metrics: {e}"))?;
        let none = GraphConfig::new();
        let graph = self.graph.get(node).unwrap_or(&none);
        let providers = Figment::from(Serialized::defaults(table.unwrap_or_default()));
        build_node::<T>(&providers, graph, metrics)
            .await
            .map_err(|e| format!("building {node}: {e}").into())
    }

    /// Refuse a metric that another node defined differently. Prometheus
    /// refuses a second help text or label set with a message that names
    /// neither node, and takes a second kind without a word, which breaks
    /// the scrape.
    fn define(
        &self,
        node: &str,
        definitions: Vec<MetricField>,
    ) -> std::result::Result<(), Failure> {
        let mut defined = self.defined.lock().unwrap_or_else(|e| e.into_inner());
        for definition in definitions {
            match defined.get(&definition.name) {
                Some((first, earlier)) if *earlier != definition => {
                    return Err(format!(
                        "building {node}: its metric `{}` differs from node {first}'s: metrics of one name in a segment share their kind, help, labels, and buckets",
                        definition.name
                    )
                    .into());
                }
                Some(_) => {}
                None => {
                    defined.insert(definition.name.clone(), (node.to_string(), definition));
                }
            }
        }
        Ok(())
    }
}

/// A started segment and the body that serves it once every segment is
/// ready: a frontend, a service's entries, JSON lines, or nothing until
/// the stop. The segment's type is opaque to the binary, so the two are
/// joined here rather than named in [`SegmentBinary`].
pub struct Serving {
    serve: Box<dyn FnOnce(Launch) -> LocalBoxFuture<'static, Result<()>>>,
}

impl Serving {
    /// Serve `segment` with `body`, as [`Launch::serve`] does: the body
    /// gets the segment and a context cancelled on the stop, and the
    /// segment keeps serving through the drain after the body returns.
    pub fn new<S, F, Fut>(segment: S, body: F) -> Self
    where
        S: Drains,
        F: FnOnce(Arc<S>, Cx) -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static,
    {
        Self {
            serve: Box::new(move |launch: Launch| Box::pin(launch.serve(segment, body))),
        }
    }
}
