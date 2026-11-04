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

use crate::EntryNeed;
use crate::config::build_node;
use crate::dist::{Roster, RuntimeConfig};
use crate::process::{Drains, Hosted, Launch, Process, ProcessConfig, Reserved};
use gdl::Build;
use gdl::Cx;
use gdl::Metrics;
use gdl::Result;
use gdl::{
    Affinity, FollowedPools, HubRecords, InstanceKv, InstanceProperties, LeastLoaded, Lifecycle,
    OfferGate, PoolPolicies, PoolPolicy, PropertyType, Selector, Selectors, Unary, UnaryServices,
};
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
    /// Every segment of the placement. An instance waits for all of them,
    /// or, in a pool, only for those of its class.
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

    /// Each property kind the graph declares, with the type of its keys:
    /// the contract's `property_types()`.
    fn property_types() -> Vec<PropertyType>;

    /// The property kinds the segment's entry adapters read, each with its
    /// class and its adapters: their entries' `needs`. Its nodes' needs
    /// are in the graph.
    const ENTRY_NEEDS: &'static [EntryNeed];

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
    let (mut config, settings) = config.into_hosted();
    // The deployment before the build: every metric a node registers
    // carries the deployment's identity.
    let deployment = B::deployment()?;
    let registry = process.registry(&deployment, B::SEGMENT)?;
    let builder = Builder::new(
        B::GRAPH_CONFIG,
        &deployment.graph,
        registry.clone(),
        process.reserved(),
        config.selector_tables().clone(),
        config.runtime.records.clone(),
    )?;
    // Before the build: a node that sets or reads a kind the graph does
    // not give it is refused at the call, and fails its build.
    crate::declare_properties(
        &deployment.graph,
        &deployment.placement,
        B::SEGMENT,
        &B::property_types(),
        B::ENTRY_NEEDS,
        &builder.properties,
        &builder.followed,
    )?;
    let hosts = B::build(settings, &builder).await?;
    // From here a call to a hosted unary service waits for its `ready`
    // rather than failing: every node is built.
    builder.services.built();
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
    // Before the launch: a chooser checks the policies its placement
    // names against these before it registers.
    config.runtime.pool_policies = builder.pool_policies();
    config.runtime.followed = builder.followed.clone();
    config.runtime.properties = builder.properties.clone();
    config.runtime.kv = builder.kv.clone();
    config.runtime.gate = builder.gate.clone();
    config.runtime.lifecycle = builder.lifecycle.clone();
    // The runtime records the segment's figures only where a system
    // listener can serve them: with none, nobody could read them. Before
    // the launch, where a chooser's follower registers the pools' figures.
    config.runtime.metrics = process.system_address().map(|_| registry.clone());
    let ready = async {
        tokio::try_join!(B::ready(&hosts), builder.ready())?;
        Ok(())
    };
    let launch = config
        .launch(&process, B::SEGMENT, B::SEGMENTS, &deployment, ready)
        .await?;
    let mut runtime = config.runtime;
    runtime.selectors = builder.into_selectors();
    let serving = B::start(hosts, launch.node(), launch.roster(), runtime)?;
    // Last: making the segment registers its figures, and a scrape runs
    // the nodes' closures under the lock a registration waits for.
    process.serve_metrics(registry)?;
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
    /// The names `registry` refuses under the metrics prefix, asked first
    /// so that the build names the prefix and the process figure.
    reserved: Reserved,
    /// Each metric registered so far, by name, with the node that defined
    /// it first.
    defined: Mutex<BTreeMap<String, (String, MetricField)>>,
    /// `[selector.<name>]` tables, by name.
    tables: BTreeMap<String, toml::Table>,
    /// The routing selectors built so far.
    selectors: Mutex<Selectors>,
    /// The pool policies registered so far.
    pool_policies: Mutex<PoolPolicies>,
    /// How to wait for each selector and pool policy it built to be ready.
    readiness: Mutex<Vec<Readiness>>,
    /// What every node and selector it builds reads the chooser's pools
    /// through ([`FollowedPools::current`]), bound at the launch.
    followed: FollowedPools,
    /// What every node and selector it builds takes its pool hub's record
    /// tables from ([`gdl::PoolRecords::current`]): the runtime's own,
    /// bound at the launch.
    records: HubRecords,
    /// What every node it builds says the instance's properties are through
    /// ([`InstanceProperties::current`]).
    properties: InstanceProperties,
    /// What every node it builds says the instance's KV cache holds through
    /// ([`InstanceKv::current`]).
    kv: InstanceKv,
    /// The gate every node it builds may decide offers by
    /// ([`OfferGate::current`]).
    gate: OfferGate,
    /// Where every node it builds registers its leave hooks
    /// ([`Lifecycle::current`]).
    lifecycle: Lifecycle,
    /// The unary services the segment hosts, and what every node it builds
    /// takes its clients of them from ([`gdl::UnaryClient::current`]).
    services: UnaryServices,
}

/// One built selector's or pool policy's [`Build::ready`].
type Readiness = Box<dyn Fn() -> BoxFuture<'static, Result<()>> + Send>;

impl Builder {
    fn new(
        graph: &str,
        manifest: &gdl_model::Manifest,
        registry: prometheus::Registry,
        reserved: Reserved,
        tables: BTreeMap<String, toml::Table>,
        records: HubRecords,
    ) -> std::result::Result<Self, Failure> {
        Ok(Self {
            graph: toml::from_str(graph)?,
            registry,
            reserved,
            defined: Mutex::new(BTreeMap::new()),
            tables,
            selectors: Mutex::new(Selectors::new()),
            pool_policies: Mutex::new(PoolPolicies::new()),
            readiness: Mutex::new(Vec::new()),
            followed: FollowedPools::new(),
            records,
            properties: InstanceProperties::new(),
            kv: InstanceKv::new(),
            gate: OfferGate::new(),
            lifecycle: Lifecycle::new(),
            services: UnaryServices::new(
                manifest
                    .nodes
                    .iter()
                    .map(|node| (node.name.clone(), node.calls.clone())),
            ),
        })
    }

    /// Choose the pools of each class whose `[pools.<class>] policy` is
    /// `name` by `policy`. Refuses a name a placement cannot use, gdl's
    /// own policies' names, and a name registered twice.
    pub fn pool_policy<P: PoolPolicy>(
        &self,
        name: &str,
        policy: P,
    ) -> std::result::Result<(), Failure> {
        self.pool_policies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .add(name, policy)
            .map_err(|e| format!("registering pool policy {name}: {e}").into())
    }

    /// Build `P`, the pool policy named `name` in the placement, from its
    /// settings table `table`, with its metrics registered under `name`,
    /// and choose the pools of each class whose `[pools.<class>] policy` is
    /// `name` by it. The instance announces itself as loaded only once `P`
    /// is ready, as for a selector; it registers with its hub before.
    pub async fn build_pool_policy<P: Build + PoolPolicy>(
        &self,
        name: &str,
        table: Option<toml::Table>,
    ) -> std::result::Result<(), Failure> {
        // Under the segment's handle, not a reader's: `NEEDS` name no class,
        // and a policy may share its name with an adapter.
        let built = Arc::new(self.build_under::<P>(&self.followed, name, table).await?);
        self.pool_policies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .add_shared(name, Arc::clone(&built))
            .map_err(|e| format!("registering pool policy {name}: {e}"))?;
        self.waits_for(built);
        Ok(())
    }

    /// Keep `built`'s readiness, which [`Builder::ready`] waits for.
    fn waits_for<T: Build>(&self, built: Arc<T>) {
        self.readiness
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Box::new(move || {
                let built = Arc::clone(&built);
                Box::pin(async move { built.ready().await })
            }));
    }

    /// The registered pool policies, for the launch and the runtime.
    fn pool_policies(&self) -> PoolPolicies {
        self.pool_policies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
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
        // Held to its `NEEDS`, as a pool policy is, not to its segment's
        // nodes' needs: what `cargo gdl check` holds its crossings' class
        // to is what it reads.
        let built = self
            .followed
            .limited(S::NEEDS, format!("selector `{name}`"))
            .scope(self.records.scope(self.properties.scope(self.kv.scope(
                self.gate.scope(self.lifecycle.scope(build_node::<S>(
                    &providers,
                    &GraphConfig::new(),
                    metrics,
                ))),
            ))))
            .await
            .map_err(|e| format!("building selector {name}: {e}"))?;
        let built = Arc::new(built);
        self.selectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .add_shared(name, Arc::clone(&built))
            .map_err(|e| format!("building selector {name}: {e}"))?;
        self.waits_for(built);
        Ok(())
    }

    /// Whether a selector named `name` is registered.
    fn registered(&self, name: &str) -> bool {
        let selectors = self.selectors.lock().unwrap_or_else(|e| e.into_inner());
        selectors.contains(name)
    }

    /// Resolve once every selector, pool policy, and unary service it
    /// built is ready.
    async fn ready(&self) -> Result<()> {
        let waits: Vec<_> = {
            let readiness = self.readiness.lock().unwrap_or_else(|e| e.into_inner());
            readiness.iter().map(|ready| ready()).collect()
        };
        tokio::try_join!(futures::future::try_join_all(waits), self.services.ready())?;
        Ok(())
    }

    /// The registered selectors, for the runtime.
    fn into_selectors(self) -> Selectors {
        self.selectors
            .into_inner()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Build `T`, the implementation of node `node` (as the graph spells
    /// it, or an entry adapter as its binding names it), from its settings
    /// table, with its metrics registered. Its handle reads the classes
    /// `node`'s own needs name ([`FollowedPools::needed_class`]).
    pub async fn build<T: Build>(
        &self,
        node: &str,
        table: Option<toml::Table>,
    ) -> std::result::Result<T, Failure> {
        self.build_under(&self.followed.reader(node), node, table)
            .await
    }

    /// Build `T`, the implementation of unary node `node`, as
    /// [`Builder::build`] builds a node, and host it: a node built after it
    /// whose `calls` names `node` takes its client with
    /// [`gdl::UnaryClient::current`]. Its [`Build::ready`] runs once, in
    /// the segment's readiness phase, shared with the calls that another
    /// node's `ready` makes; a call made during the build fails. A segment
    /// builds its unary nodes before the nodes that call them.
    pub async fn unary<T: Build + Unary>(
        &self,
        node: &str,
        table: Option<toml::Table>,
    ) -> std::result::Result<Arc<T>, Failure> {
        let built = Arc::new(self.build::<T>(node, table).await?);
        self.services
            .serve(node, Arc::clone(&built))
            .map_err(|e| format!("building {node}: {e}"))?;
        Ok(built)
    }

    /// [`Builder::build`], with `followed` as the build's handle.
    async fn build_under<T: Build>(
        &self,
        followed: &FollowedPools,
        node: &str,
        table: Option<toml::Table>,
    ) -> std::result::Result<T, Failure> {
        self.define(node, <T::Metrics as Metrics>::definitions())?;
        let metrics = T::Metrics::register(&self.registry, node)
            .map_err(|e| format!("building {node}: registering its metrics: {e}"))?;
        let none = GraphConfig::new();
        let graph = self.graph.get(node).unwrap_or(&none);
        let providers = Figment::from(Serialized::defaults(table.unwrap_or_default()));
        // Boxed: each scope wraps the build's future in one of its own, and
        // unboxed the future that runs the segment holds them all, on the
        // stack of whatever polls it: a test that runs segments on its own
        // thread overflowed its stack. A build runs once per node.
        let scoped = followed.scope(
            self.records.scope(
                self.properties.scope(
                    self.kv.scope(
                        self.gate.scope(
                            self.lifecycle.scope(
                                self.services
                                    .scope(node, build_node::<T>(&providers, graph, metrics)),
                            ),
                        ),
                    ),
                ),
            ),
        );
        Box::pin(scoped)
            .await
            .map_err(|e| format!("building {node}: {e}").into())
    }

    /// Refuse a metric that another node defined differently, or that the
    /// metrics prefix would serve as one of the process's own figures.
    /// Prometheus refuses a second help text or label set with a message
    /// that names neither node, and takes a second kind without a word,
    /// which breaks the scrape; the registry refuses a process figure's
    /// name too, but names neither the prefix nor the figure.
    fn define(
        &self,
        node: &str,
        definitions: Vec<MetricField>,
    ) -> std::result::Result<(), Failure> {
        let mut defined = self.defined.lock().unwrap_or_else(|e| e.into_inner());
        for definition in definitions {
            if let Some(refusal) = self.reserved.refusal(&definition.name) {
                return Err(format!("building {node}: {refusal}").into());
            }
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
