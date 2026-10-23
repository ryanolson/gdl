//! What a segment's binary does besides running its graph: read its
//! settings, find its peers, announce that it is serving, and stop when
//! told to. A generated segment crate supplies only what it hosts;
//! everything here is the same for every segment, so it is written once
//! and tested here rather than in each generated crate.

mod barrier;
mod system;

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use figment::value::{Dict, Tag, Value};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use velo::Velo;

use crate::dist::{
    Roster, RuntimeConfig, Segment, SegmentLoad, announce, announced, bind_node, follow_directory,
    shut_down,
};
use gdl::Cx;
use gdl::{Error, ErrorKind, Result};
use gdl_model::CellSettings;
use system::{Health, Phase, SystemSettings};

/// The settings of one segment process. `H` is what the segment hosts: the
/// tables of its nodes and of its frontend or entry adapters, which the
/// generated segment crate declares. An unknown key is an error at either
/// level.
///
/// ```toml
/// listen = "127.0.0.1:0"  # this instance's address
/// roster = "/run/roster"  # a directory shared by every instance, fresh for each deployment
/// # [hub]                 # or the discovery hub; exactly one of the two. An unspecified listen
/// # host = "hub"          # address then advertises what HOSTNAME resolves to. `public_port`,
///                         # `control_port`, and `heartbeat_ms` (5000) are optional
/// rendezvous_ms = 30000   # optional: how long to wait for the other instances
/// [instances]             # optional, with a roster: instances per segment, 1 by default
/// [min_replicas]          # optional, with a hub: loaded instances to wait for, 1 by default
/// [runtime]               # optional: the runtime's own settings
/// [selector.<name>]       # optional: a routing selector's settings, by its name
/// [system]                # optional: the live and health probes; off unless `port` is set
/// port = 9090             # also DYN_SYSTEM_PORT; dynamo's meaning: negative is off, 0 ephemeral
/// host = "0.0.0.0"        # also DYN_SYSTEM_HOST
/// live_path = "/live"     # also DYN_SYSTEM_LIVE_PATH
/// health_path = "/health" # also DYN_SYSTEM_HEALTH_PATH
/// [cell]                  # optional: the cell this instance belongs to (`CellSettings`)
/// name = "model-x-01-us-east-1"
/// class = "model_x"       # the launch refuses a class that is not the segment's
/// region = "us-east-1"
/// summary_ms = 10000      # optional: how often an entry instance reports its admission to its hub
/// ```
///
/// An instance in a cell waits at its launch only for the segments of its
/// own class: `[instances]` and `[min_replicas]` may name no other. Under a
/// hub, one whose segment holds a hop into another class (a chooser) also
/// follows its hub's relay of the cells the hub follows, into the tables
/// [`Launch::cells`] reads. One whose segment is an entry segment of its
/// class (it holds the inner end of a hop into the class) reports its
/// admission to its hub once every `summary_ms` while it is ready.
pub struct ProcessConfig<H> {
    listen: SocketAddr,
    roster: Option<PathBuf>,
    hub: Option<HubAddress>,
    rendezvous_ms: u64,
    instances: BTreeMap<String, usize>,
    min_replicas: BTreeMap<String, usize>,
    system: SystemSettings,
    /// Each routing selector's table, by the selector's name.
    selector: BTreeMap<String, toml::Table>,
    cell: Option<CellSettings>,
    /// The runtime's own settings.
    pub runtime: RuntimeConfig,
    /// The tables of the hosted implementations.
    pub hosted: H,
}

/// The keys every segment has. Read apart from the hosted tables, because
/// serde cannot refuse unknown keys through a flattened field, and a
/// misspelled key must not be ignored.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    listen: SocketAddr,
    roster: Option<PathBuf>,
    hub: Option<HubAddress>,
    #[serde(default = "default_rendezvous_ms")]
    rendezvous_ms: u64,
    #[serde(default)]
    instances: BTreeMap<String, usize>,
    #[serde(default)]
    min_replicas: BTreeMap<String, usize>,
    #[serde(default)]
    runtime: RuntimeConfig,
    #[serde(default)]
    system: SystemSettings,
    #[serde(default)]
    selector: BTreeMap<String, toml::Table>,
    cell: Option<CellSettings>,
}

/// What a segment hosts, as its binary declares it: the settings type's
/// top-level keys, and the names of the tables under `node` and `adapter`,
/// so that an environment variable can address them.
pub trait Hosted: DeserializeOwned {
    /// The top-level keys the type takes, such as `node` or `frontend`.
    const KEYS: &'static [&'static str];
    /// The node and adapter names, as the graph spells them.
    const NAMES: &'static [&'static str];
}

/// `Settings`' keys. Together with [`Hosted::KEYS`], these are the only
/// `GDL_` variables read: the build scripts use the same prefix
/// (`GDL_PROFILE`, `GDL_IMAGE_PREFIX`), and a variable meant for them must
/// not stop a segment from starting.
const KEYS: [&str; 10] = [
    "listen",
    "roster",
    "hub",
    "rendezvous_ms",
    "instances",
    "min_replicas",
    "runtime",
    "system",
    "selector",
    "cell",
];

fn default_rendezvous_ms() -> u64 {
    30_000
}

/// Where the discovery hub is, and how the lease with it is kept: `[hub]`
/// with its `host`, on the deployment's ports unless `public_port` and
/// `control_port` say otherwise, and a heartbeat every `heartbeat_ms`. The
/// heartbeat is a sixth of the hub's default lease, so that one slow
/// answer does not cost it; the lease is lost after five missed
/// heartbeats. One table, so that a value set in the environment
/// (`GDL_HUB__HEARTBEAT_MS`) joins the file's rather than replacing it.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
// The ports are read only when the hub feature is on; the table is parsed
// either way, so that a settings file is refused for the same reasons.
#[cfg_attr(not(feature = "hub"), allow(dead_code))]
struct HubAddress {
    host: String,
    #[serde(default = "default_public_port")]
    public_port: u16,
    #[serde(default = "default_control_port")]
    control_port: u16,
    #[serde(default = "default_heartbeat_ms")]
    heartbeat_ms: u64,
}

fn default_public_port() -> u16 {
    gdl_model::HUB_PUBLIC_PORT
}

fn default_control_port() -> u16 {
    gdl_model::HUB_CONTROL_PORT
}

fn default_heartbeat_ms() -> u64 {
    5_000
}

impl<H: Hosted> ProcessConfig<H> {
    /// Read the settings from `paths`, each file overriding the ones before
    /// it, then from `GDL_` environment variables. `__` separates nested
    /// keys: `GDL_RUNTIME__ADMISSION_LIMIT=64`, or
    /// `GDL_NODE__TRUNCATE__MAX=3` for `[node.Truncate] max = 3`. A key
    /// from the environment is matched without regard to case; a file's
    /// keys are as spelled. dynamo's `DYN_SYSTEM_HOST`, `DYN_SYSTEM_PORT`,
    /// `DYN_SYSTEM_LIVE_PATH`, and `DYN_SYSTEM_HEALTH_PATH` set `[system]`
    /// too, under the `GDL_` variables, so that the dynamo operator's
    /// probes find the listener; no other `DYN_` variable is read.
    pub fn load(paths: impl IntoIterator<Item = impl AsRef<Path>>) -> Result<Self> {
        let mut figment = Figment::new();
        for path in paths {
            figment = figment.merge(Toml::file_exact(path.as_ref()));
        }
        Self::from_figment(figment.merge(Self::dynamo_env()).merge(Self::env()))
    }

    /// dynamo's system-port variables, as `[system]` keys.
    fn dynamo_env() -> Env {
        Env::raw()
            .only(&[
                "DYN_SYSTEM_HOST",
                "DYN_SYSTEM_PORT",
                "DYN_SYSTEM_LIVE_PATH",
                "DYN_SYSTEM_HEALTH_PATH",
            ])
            .map(|key| {
                let key = key.as_str().to_ascii_lowercase();
                format!("system.{}", &key["dyn_system_".len()..]).into()
            })
    }

    /// The settings held by `figment`, checked.
    pub fn from_figment(figment: Figment) -> Result<Self> {
        let (mut settings, mut hosted) = (Dict::new(), Dict::new());
        for (key, value) in figment
            .extract::<Dict>()
            .map_err(|e| placement(e.to_string()))?
        {
            if KEYS.contains(&key.as_str()) {
                settings.insert(key, value);
            } else if H::KEYS.contains(&key.as_str()) {
                hosted.insert(key, value);
            } else {
                let expected: Vec<&str> = KEYS.iter().chain(H::KEYS).copied().collect();
                return Err(placement(format!(
                    "unknown key `{key}`; expected one of {}",
                    expected.join(", ")
                )));
            }
        }
        let settings = Settings::deserialize(&Value::Dict(Tag::Default, settings))
            .map_err(|e| placement(e.to_string()))?;
        let hosted = H::deserialize(&Value::Dict(Tag::Default, hosted))
            .map_err(|e| placement(e.to_string()))?;
        let config = Self {
            listen: settings.listen,
            roster: settings.roster,
            hub: settings.hub,
            rendezvous_ms: settings.rendezvous_ms,
            instances: settings.instances,
            min_replicas: settings.min_replicas,
            system: settings.system,
            selector: settings.selector,
            cell: settings.cell,
            runtime: settings.runtime,
            hosted,
        };
        config.discovery()?;
        config.system.validate()?;
        if config.hub.as_ref().is_some_and(|hub| hub.heartbeat_ms == 0) {
            return Err(placement(
                "`hub.heartbeat_ms` is 0; the lease needs a cadence",
            ));
        }
        Ok(config)
    }

    /// The `GDL_` environment, as this type reads it. The case flag comes
    /// last: figment's `map` and `filter` reset it.
    fn env() -> Env {
        Env::prefixed("GDL_")
            .map(|key| canonical::<H>(key.as_str()).into())
            .filter(|key| {
                let first = key.as_str().split('.').next().unwrap_or_default();
                KEYS.contains(&first) || H::KEYS.contains(&first)
            })
            .lowercase(false)
    }

    /// The settings without the hosted tables, and the tables: a binary
    /// builds its nodes from the tables and launches with the rest.
    pub(crate) fn into_hosted(self) -> (ProcessConfig<()>, H) {
        let Self {
            listen,
            roster,
            hub,
            rendezvous_ms,
            instances,
            min_replicas,
            system,
            selector,
            cell,
            runtime,
            hosted,
        } = self;
        let config = ProcessConfig {
            listen,
            roster,
            hub,
            rendezvous_ms,
            instances,
            min_replicas,
            system,
            selector,
            cell,
            runtime,
            hosted: (),
        };
        (config, hosted)
    }
}

impl<H> ProcessConfig<H> {
    /// The cell this instance belongs to, when its deployment has cells.
    pub fn cell(&self) -> Option<&CellSettings> {
        self.cell.as_ref()
    }

    /// Each routing selector's table, by the selector's name.
    pub(crate) fn selector_tables(&self) -> &BTreeMap<String, toml::Table> {
        &self.selector
    }

    fn discovery(&self) -> Result<Discovery<'_>> {
        match (&self.roster, &self.hub) {
            (Some(dir), None) => Ok(Discovery::Files(dir)),
            (None, Some(hub)) => Ok(Discovery::Hub(hub)),
            (None, None) => Err(placement(
                "set `roster` (a directory every instance shares) or `hub` (the discovery hub's host)",
            )),
            (Some(_), Some(_)) => Err(placement(
                "`roster` and `hub` are both set; a deployment finds its instances one way",
            )),
        }
    }

    /// Bind this instance's node and meet the other segments, once `ready`
    /// resolves: the hosted nodes are loaded. Through a roster directory,
    /// `[instances]` says exactly how many of each segment to wait for, so
    /// that every instance routes to the same ones. Through a hub, which
    /// admits the instance only if `registration` names the deployment it
    /// serves, the instance registers before it is ready, so the hub knows
    /// it is live, then waits for at least `[min_replicas]` loaded
    /// instances of each segment that have not retired, and takes those as
    /// its roster. An instance in a cell (`[cell]`) waits, either way,
    /// only for the segments of its own class: the cells of another class
    /// are not part of its start. How many instances a segment has under a hub is not
    /// fixed: from just before it announces `loaded`, the instance follows
    /// membership, so that an instance that joins later is routed to once
    /// it is ready, and one that retires or leaves is not. From the same
    /// moment, a chooser in a cell follows its hub's relay of the cells of
    /// the classes it sends to, for as long as the launch lives, without
    /// waiting for any of them ([`Launch::cells`]).
    ///
    /// A stop that `process` takes before the launch is done, while the
    /// nodes load or the instances meet, ends it with `Cancelled`.
    pub async fn launch(
        &self,
        process: &Process,
        segment: &'static str,
        segments: &'static [&'static str],
        deployment: &gdl_model::DeploymentSpec,
        ready: impl Future<Output = Result<()>>,
    ) -> Result<Launch> {
        process
            .starting(self.meet(process, segment, segments, deployment, ready))
            .await
    }

    /// The launch itself, which a stop may cut short.
    async fn meet(
        &self,
        process: &Process,
        segment: &'static str,
        segments: &'static [&'static str],
        deployment: &gdl_model::DeploymentSpec,
        ready: impl Future<Output = Result<()>>,
    ) -> Result<Launch> {
        // The instances this one waits for before it serves: those of the
        // segments it sends to.
        let node = process.take_node()?;
        let layout = gdl_model::layout(&deployment.graph, &deployment.placement)?;
        // Before anything registers or meets: an instance in the wrong
        // cell would otherwise be counted toward that cell's quorum.
        if let Some(cell) = &self.cell {
            cell.check(&layout, segment)
                .map_err(|e| placement(e.to_string()))?;
            // A chooser that cannot run its cell policy refuses before it
            // registers or loads.
            #[cfg(feature = "hub")]
            crate::dist::check_cell_policies(&layout, segment)?;
        }
        let barrier = barrier::Barrier::new(&layout, segment, self.cell.is_some());
        let inner: Vec<String> = layout
            .sends_to(segment)
            .into_iter()
            .filter(|other| barrier.waits_for(other))
            .collect();
        // The segments that send to this one: their instances are the ones
        // that must stop routing here before a retirement may end.
        let senders: Vec<String> = segments
            .iter()
            .filter(|from| layout.sends_to(from).contains(segment))
            .map(ToString::to_string)
            .collect();
        let (source, reaching) = sources(&layout, &deployment.graph, segment);
        let discovery = self.discovery()?;
        let counts = match discovery {
            Discovery::Files(_) => ("[instances]", &self.instances, &self.min_replicas),
            Discovery::Hub(_) => ("[min_replicas]", &self.min_replicas, &self.instances),
        };
        let (table, counts, other) = counts;
        // A segment's name is lowercase, so a count from the environment,
        // which lowercases its keys, names the same segment as the file's.
        if let Some(unknown) = counts.keys().find(|s| !segments.contains(&s.as_str())) {
            return Err(placement(format!("{table}: no segment `{unknown}`")));
        }
        barrier.check(table, counts)?;
        if !other.is_empty() {
            return Err(placement(
                "[instances] counts the instances a roster directory waits for, [min_replicas] the loaded instances a hub waits for; a deployment uses one",
            ));
        }
        // Every instance waits for the same membership.
        let expect: Vec<(&str, usize)> = segments
            .iter()
            .filter(|s| barrier.waits_for(s))
            .map(|s| (*s, counts.get(*s).copied().unwrap_or(1)))
            .collect();
        let wait = Duration::from_millis(self.rendezvous_ms);
        let drain = self.runtime.drain_bound();
        // An entry instance in a cell reports its admission to its hub.
        #[cfg(feature = "hub")]
        let reports = self
            .cell
            .as_ref()
            .filter(|cell| layout.entry_segments(&cell.class).contains(segment))
            .map(|cell| cell.summary);
        // A chooser's follower of the cells it sends to, under a hub.
        #[cfg(feature = "hub")]
        let mut chooser = None;
        let (node, roster, peers, follower) = match discovery {
            Discovery::Files(dir) => {
                ready.await?;
                let roster = Roster::rendezvous(dir, segment, &node, expect, wait).await?;
                let follower = tokio::spawn(follow_directory(
                    dir.to_path_buf(),
                    roster.clone(),
                    segment,
                    node.clone(),
                    inner.clone(),
                    drain,
                ));
                (
                    node,
                    roster,
                    Peers::Files(dir.to_path_buf()),
                    Some(Abort(follower)),
                )
            }
            #[cfg(feature = "hub")]
            Discovery::Hub(hub) => {
                use crate::dist::HubSession;
                let HubAddress {
                    host,
                    public_port,
                    control_port,
                    heartbeat_ms,
                } = hub;
                let mut session = HubSession::register(
                    host,
                    *public_port,
                    *control_port,
                    &node,
                    deployment.registration(segment)?,
                    Duration::from_millis(*heartbeat_ms),
                )
                .await?;
                // The nodes load while the lease is kept; a lease lost
                // meanwhile (the hub restarted) ends the wait, since the
                // roster taken afterwards would be refused anyway.
                tokio::select! {
                    result = ready => result?,
                    lost = session.maintain() => lost?,
                }
                // Membership is followed from just before `loaded`, into the
                // roster this instance will route with, so that a retirement
                // is acknowledged even while the rendezvous waits (ruling
                // 143); the roster read is merged into it, keeping what the
                // follower learned.
                let mut roster = Roster::new();
                roster.hold();
                let all: Vec<&str> = segments.to_vec();
                let sends: Vec<&str> = inner.iter().map(String::as_str).collect();
                // A chooser in a cell follows the cells of the classes it
                // sends to from here too, so that its tables fill while it
                // meets, and routes into those classes by them; an instance
                // without a cell follows none. The follower is ended with
                // whatever ends the launch, a failed rendezvous or a stop
                // included.
                if let Some(cell) = &self.cell {
                    chooser = crate::dist::follow_cells(
                        &layout,
                        segment,
                        &cell.region,
                        host,
                        *public_port,
                        &node,
                        roster.live(),
                        drain,
                    )?
                    .map(|(cells, task)| (cells, Abort(task)));
                }
                // The membership leaves to the chooser what its tables hold.
                let cells = chooser.as_ref().map(|(cells, _)| cells.clone());
                if let Some(cells) = &cells {
                    roster.choose_cells(cells.clone());
                }
                session.follow(
                    Arc::clone(roster.live()),
                    node.clone(),
                    &all,
                    &sends,
                    drain,
                    cells,
                );
                let met = session.roster(&expect, wait).await?;
                for (segment, peers) in met.segments() {
                    for peer in peers {
                        roster.add(segment, peer.clone());
                    }
                }
                (node, roster, Peers::Hub(Box::new(session)), None)
            }
            #[cfg(not(feature = "hub"))]
            Discovery::Hub(hub) => {
                drop(ready);
                return Err(placement(format!(
                    "`[hub] host = {:?}`, but this binary was built without hub discovery (gdl's `hub` feature)",
                    hub.host
                )));
            }
        };
        // Held until the barrier: a request that reaches this instance
        // first waits, rather than leaving for a peer that is not serving.
        roster.hold();
        let leaving = Leaving {
            dir: match &peers {
                Peers::Files(dir) => Some(dir.clone()),
                #[cfg(feature = "hub")]
                Peers::Hub(_) => None,
            },
            segment,
            node: node.clone(),
        };
        Ok(Launch {
            node,
            roster,
            peers,
            segment,
            inner,
            senders,
            source,
            reaching,
            wait,
            drain,
            setup: self.runtime.setup_timeout,
            process: process.clone(),
            _follower: follower,
            #[cfg(feature = "hub")]
            _chooser: chooser.map(|(_, task)| task),
            #[cfg(feature = "hub")]
            reports,
            _leaving: leaving,
        })
    }

    /// Bind this instance's velo node. Under a hub, a container binds and
    /// advertises the address its hostname resolves to; any other address
    /// is bound and advertised as given. Nothing is announced: peers learn
    /// the address only when the instance launches.
    async fn bind(&self) -> Result<Velo> {
        match self.discovery()? {
            #[cfg(feature = "hub")]
            Discovery::Hub(hub) => {
                crate::dist::bind_under_hub(
                    self.listen,
                    &hub.host,
                    hub.public_port,
                    hub.control_port,
                )
                .await
            }
            _ => bind_node(self.listen).await,
        }
    }

    /// Ask the system listener of a process run with these settings
    /// whether it is healthy, as a container's health check does: `Ok` for
    /// a 200 from its health path.
    pub async fn healthcheck(&self) -> Result<()> {
        system::healthcheck(&self.system).await
    }
}

#[derive(Clone, Copy)]
enum Discovery<'a> {
    Files(&'a Path),
    Hub(&'a HubAddress),
}

/// A segment instance that has met its peers and may start its graph.
pub struct Launch {
    node: Velo,
    roster: Roster,
    peers: Peers,
    segment: &'static str,
    /// The segments this one sends to.
    inner: Vec<String>,
    /// The segments that send to this one.
    senders: Vec<String>,
    /// Whether this segment hosts a source.
    source: bool,
    /// The segments hosting a source whose requests can reach this one.
    reaching: Vec<String>,
    wait: Duration,
    /// How long a stop may take to drain.
    drain: Duration,
    /// The longest a hand-off sent before a peer stopped routing here may
    /// take to arrive.
    setup: Duration,
    process: Process,
    /// The roster directory's membership follower; the hub session owns
    /// its own.
    _follower: Option<Abort>,
    /// A chooser's follower of those cells, which lives as long as the
    /// launch, through its drain.
    #[cfg(feature = "hub")]
    _chooser: Option<Abort>,
    /// For an entry instance in a cell, how often it reports its
    /// admission to its hub.
    #[cfg(feature = "hub")]
    reports: Option<Duration>,
    /// Dropped last, on every way out: says the instance has left.
    _leaving: Leaving,
}

/// Ends a task when dropped.
struct Abort(tokio::task::JoinHandle<()>);

impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Announces, when dropped, that the instance has left, so that no peer's
/// drain waits for it. In a roster directory that is a file under
/// `left/`; at the hub, `serve` unregisters on its way out, and on any other
/// way the registration ends with the lease. An instance that crashes
/// leaves nothing: under a hub its lease lapses, and in a roster directory
/// a drain that waits for it runs to its bound.
struct Leaving {
    dir: Option<PathBuf>,
    segment: &'static str,
    node: Velo,
}

impl Drop for Leaving {
    fn drop(&mut self) {
        if let Some(dir) = &self.dir {
            let _ = announce(dir, "left", self.segment, &self.node);
        }
    }
}

/// Whether `segment` hosts a source, and the segments hosting one whose
/// requests can reach it: those it is reachable from through `sends_to`,
/// itself included only when a path leaves it and returns.
fn sources(
    layout: &gdl_model::Layout,
    graph: &gdl_model::Manifest,
    segment: &str,
) -> (bool, Vec<String>) {
    let hosting: std::collections::BTreeSet<&str> = graph
        .nodes
        .iter()
        .filter(|node| node.role == gdl_model::Role::Source)
        .filter_map(|node| layout.segment_of(&node.name))
        .collect();
    let reaching = hosting
        .iter()
        .filter(|from| layout.reaches(from, segment))
        .map(|from| from.to_string())
        .collect();
    (hosting.contains(segment), reaching)
}

/// What a launch keeps serving while it drains: the started segment, whose
/// admitted requests the drain waits to see at zero, and whose entries it
/// closes before that wait.
pub trait Drains: Send + Sync + 'static {
    /// Requests the instance holds now.
    fn admitted(&self) -> usize;

    /// Refuse the requests that arrive from now on. Nothing by default,
    /// for a launch that serves no segment.
    fn close(&self) {}

    /// What its admission holds, for the load report of an entry instance
    /// in a cell. `None` by default, for a launch that serves no segment,
    /// which reports nothing.
    fn load(&self) -> Option<SegmentLoad> {
        None
    }
}

impl<H: Send + Sync + 'static> Drains for Segment<H> {
    fn admitted(&self) -> usize {
        Segment::admitted(self)
    }

    fn load(&self) -> Option<SegmentLoad> {
        Some(Segment::load(self))
    }

    fn close(&self) {
        Segment::close(self);
    }
}

enum Peers {
    Files(PathBuf),
    #[cfg(feature = "hub")]
    Hub(Box<crate::dist::HubSession>),
}

impl Launch {
    /// This instance's node.
    pub fn node(&self) -> Velo {
        self.node.clone()
    }

    /// Every instance of every segment.
    pub fn roster(&self) -> &Roster {
        &self.roster
    }

    /// The tables of the cells this instance follows and routes into:
    /// for a chooser, an instance in a cell under a hub whose segment
    /// holds a hop into another class, one per entry segment of the hops
    /// it sends there. `None` for any other instance, which follows no
    /// cell.
    #[cfg(feature = "hub")]
    pub fn cells(&self) -> Option<crate::dist::Cells> {
        self.roster.cells().cloned()
    }

    /// Announce that this instance's handlers are installed, wait until
    /// every instance it can route to has announced the same (in a cell,
    /// every instance of its own class it routes to), release the
    /// roster, then run `serve` with the started `segment` until it
    /// finishes. A request that reaches this instance before the barrier
    /// waits at the held roster.
    ///
    /// Under a hub, once the roster is released the instance announces
    /// that it is ready, and instances already serving add it then.
    ///
    /// A stop (SIGINT or SIGTERM, or [`Process::stop`]) retires the
    /// instance: it says so, so that its peers stop routing to it, makes
    /// itself not ready, and cancels the context `serve` gets; `serve` then
    /// finishes on its own terms, so a frontend drains the requests it
    /// holds. The instance keeps `segment` serving after that: a request
    /// admitted before the stop, or routed here before a peer heard of the
    /// retirement, may still cross it. It waits for whichever comes first:
    /// every instance that sends to its segment has promised to avoid it or
    /// left, and the runtime's setup timeout has passed since; or every
    /// instance of the source segments whose requests can reach it has
    /// drained (its `serve` returned) or left. A source's instance
    /// announces that it has drained before it waits, so source segments
    /// that route to each other never wait for each other. Then it refuses
    /// what arrives, waits until it holds no request (a stream holds its
    /// request until velo has handed its output on), stops velo, and
    /// leaves. A peer avoids a retired instance only while its segment has
    /// another, so stopping every instance drains as one deployment-wide
    /// stop. The drain is bounded by the runtime's `drain` (by default the
    /// request deadline), and a second stop ends it at once.
    ///
    /// A lost hub lease stops the instance the same way, skips the wait,
    /// since the hub that would answer it is gone, and gives the lease's
    /// error: the process is invisible to new peers and will be probed as
    /// dead, so the deployment must start it again.
    pub async fn serve<S, F, Fut>(mut self, segment: S, serve: F) -> Result<()>
    where
        S: Drains,
        F: FnOnce(Arc<S>, Cx) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let process = self.process.clone();
        let segment = Arc::new(segment);
        let inner: Vec<&str> = self.inner.iter().map(String::as_str).collect();
        let barrier = async {
            match &mut self.peers {
                Peers::Files(dir) => {
                    self.roster
                        .ready(
                            &dir.join("serving"),
                            self.segment,
                            &self.node,
                            &inner,
                            self.wait,
                        )
                        .await
                }
                #[cfg(feature = "hub")]
                Peers::Hub(session) => session.ready(&self.roster, &inner, self.wait).await,
            }
        };
        let passed = tokio::select! {
            biased;
            () = process.stops(1) => None,
            passed = barrier => Some(passed),
        };
        let second = || {
            Error::new(
                ErrorKind::Cancelled,
                "stopped by a second signal before the drain finished",
            )
        };
        let (result, stop, deadline) = match passed {
            Some(Err(error)) => return Err(error),
            // A stop during the barrier comes after this instance said it
            // serves, so peers may already route here: it drains as after
            // any stop, without running the body.
            None => {
                process.health.set(Phase::Draining);
                let stopped = Error::new(ErrorKind::Cancelled, "stopped while starting");
                let deadline = tokio::time::Instant::now() + self.drain;
                tokio::select! {
                    biased;
                    () = process.stops(2) => return Err(second()),
                    () = self.retire(deadline) => {}
                }
                (Err(stopped), Ok(()), deadline)
            }
            Some(Ok(())) => {
                self.roster.release();
                // The instances already serving add this one now.
                #[cfg(feature = "hub")]
                if let Peers::Hub(session) = &mut self.peers {
                    let deadline = tokio::time::Instant::now() + self.wait;
                    // A stop meanwhile goes straight on to the drain below;
                    // a lost lease ends the announcement itself.
                    let announced = tokio::select! {
                        biased;
                        () = process.stops(1) => Ok(()),
                        announced = session.announce_ready(deadline) => announced,
                    };
                    if let Err(error) = announced {
                        eprintln!(
                            "{}: the hub did not take `ready`, so instances already serving will not route here: {error}",
                            self.segment
                        );
                    }
                }
                // An entry instance in a cell reports its admission from
                // here, while it is usable, and stops before it retires.
                #[cfg(feature = "hub")]
                let reporter = match (&self.peers, self.reports) {
                    (Peers::Hub(session), Some(period)) => {
                        let segment = Arc::clone(&segment);
                        let read = move || segment.load();
                        let me = self.node.instance_id();
                        Some(Abort(session.report_load(me, period, read)))
                    }
                    _ => None,
                };
                process.health.set(Phase::Ready);
                eprintln!("{}: ready", self.segment);
                let shutdown = Cx::new();
                let body = serve(Arc::clone(&segment), shutdown.clone());
                tokio::pin!(body);
                let lease = async {
                    match &self.peers {
                        Peers::Files(_) => std::future::pending().await,
                        #[cfg(feature = "hub")]
                        Peers::Hub(session) => session.maintain().await,
                    }
                };
                let (ended, stop) = tokio::select! {
                    result = &mut body => (Some(result), Ok(())),
                    () = process.stops(1) => (None, Ok(())),
                    result = lease => (None, result),
                };
                #[cfg(feature = "hub")]
                drop(reporter);
                let deadline = tokio::time::Instant::now() + self.drain;
                process.health.set(Phase::Draining);
                shutdown.cancel();
                // The bound ends every wait, the body's too: a request with
                // a longer deadline of its own is cut off at it. A body that
                // has finished is taken first, whenever its end is seen.
                let finished = async {
                    match ended {
                        Some(result) => result,
                        None => tokio::select! {
                            biased;
                            result = &mut body => result,
                            () = tokio::time::sleep_until(deadline) => Err(Error::new(
                                ErrorKind::Cancelled,
                                "the drain's bound passed before the serving body finished",
                            )),
                        },
                    }
                };
                // Peers hear of the retirement while the body finishes; a
                // lost lease took the hub that would hear it.
                let retiring = async {
                    if stop.is_ok() {
                        self.retire(deadline).await;
                    }
                };
                let (result, ()) = tokio::select! {
                    biased;
                    () = process.stops(2) => return Err(second()),
                    both = async { tokio::join!(finished, retiring) } => both,
                };
                (result, stop, deadline)
            }
        };
        // A lost lease takes the hub's answers, not this instance's own
        // requests: those are still waited for.
        tokio::select! {
            () = self.drain(segment.as_ref(), deadline, stop.is_ok()) => {}
            () = process.stops(2) => return Err(second()),
        }
        let left = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .max(Duration::from_secs(1));
        shut_down(&self.node, left).await;
        #[cfg(feature = "hub")]
        if let Peers::Hub(session) = &self.peers {
            session.leave().await;
        }
        stop.and(result)
    }

    /// Say that this instance has retired, so that its peers stop routing
    /// to it (ruling 141): at the hub, `retired`, then `ready` and `loaded`
    /// withdrawn; in a roster directory, a file in `retired/`. Bounded by
    /// `deadline`; what does not get through leaves the drain to (B) or its
    /// bound.
    async fn retire(&mut self, deadline: tokio::time::Instant) {
        let retired = match &mut self.peers {
            Peers::Files(dir) => announce(dir, "retired", self.segment, &self.node),
            #[cfg(feature = "hub")]
            Peers::Hub(session) => session.retire(deadline).await,
        };
        if let Err(error) = retired {
            eprintln!(
                "{}: could not announce the retirement, so peers may keep routing here: {error}",
                self.segment
            );
        }
        #[cfg(not(feature = "hub"))]
        let _ = deadline;
    }

    /// The drain after the serving body has returned (ruling 140). A
    /// source's instance announces that it has drained, then the instance
    /// waits for the first of: (A) every instance of the segments that send
    /// to this one has promised to avoid it, or left, and `setup` has
    /// passed since, so that a hand-off sent before the promise has
    /// arrived; or (B) every instance of the source segments that can reach
    /// this one has drained or left. Then it closes its entries (ruling
    /// 150) and waits until it holds no request; a stream holds its share
    /// until velo has handed its output on. Each wait ends at `deadline`.
    /// With `hub` false (its lease is lost), the hub is not asked. Polls;
    /// nothing is added to a request's path.
    async fn drain(
        &mut self,
        segment: &dyn Drains,
        deadline: tokio::time::Instant,
        // Only a hub is asked; a roster directory has no lease to lose.
        #[cfg_attr(not(feature = "hub"), allow(unused_variables))] hub: bool,
    ) {
        let reaching: Vec<&str> = self.reaching.iter().map(String::as_str).collect();
        let senders: Vec<&str> = self.senders.iter().map(String::as_str).collect();
        let me = self.node.instance_id();
        let pause = |every: Duration| async move {
            if tokio::time::Instant::now() + every > deadline {
                return false;
            }
            tokio::time::sleep(every).await;
            true
        };
        // When (A) first held. With no sender, nothing routes here, and
        // (A) holds at once.
        let mut avoided: Option<tokio::time::Instant> = None;
        let (setup, unsent) = (self.setup, senders.is_empty());
        let settled = move |avoided: Option<tokio::time::Instant>| {
            unsent || avoided.is_some_and(|at| tokio::time::Instant::now() >= at + setup)
        };
        match &mut self.peers {
            Peers::Files(dir) => {
                if self.source {
                    let _ = announce(dir, "drained", self.segment, &self.node);
                }
                // A roster directory's rosters are the same everywhere, so
                // this one names every instance to wait for. Reading a
                // local directory every 50 ms blocks the runtime for
                // microseconds; a roster directory is for local runs.
                let ids = |segments: &[&str]| -> std::collections::BTreeSet<String> {
                    self.roster
                        .segments()
                        .filter(|(segment, _)| segments.contains(segment))
                        .flat_map(|(_, peers)| peers)
                        .map(|peer| peer.instance_id().to_string())
                        .filter(|id| *id != me.to_string())
                        .collect()
                };
                let (sources, sending) = (ids(&reaching), ids(&senders));
                let avoiding = format!("avoids/{me}");
                loop {
                    let left = announced(dir, "left").unwrap_or_default();
                    let done = |phase: &str, want: &std::collections::BTreeSet<String>| {
                        announced(dir, phase).is_ok_and(|mut done| {
                            done.extend(left.iter().cloned());
                            want.is_subset(&done)
                        })
                    };
                    let drained = done("drained", &sources);
                    if avoided.is_none() && done(&avoiding, &sending) {
                        avoided = Some(tokio::time::Instant::now());
                    }
                    if drained || settled(avoided) || !pause(DRAIN_POLL).await {
                        break;
                    }
                }
            }
            #[cfg(feature = "hub")]
            Peers::Hub(session) if hub => {
                if self.source {
                    let _ = session.drained(deadline).await;
                }
                // Rosters differ under a hub, so the hub's members are
                // asked, not this instance's roster: one that joined later
                // may route here too. Each question is bounded by the
                // deadline, since a hub can stop answering without closing
                // its connections; a lease lost meanwhile ends the wait.
                loop {
                    // A lost lease is looked at first, and ends a question
                    // in flight: the hub that would answer it is gone.
                    if session.is_lost() || settled(avoided) {
                        break;
                    }
                    let ask = async {
                        let drained = reaching.is_empty() || session.all_drained(&reaching).await?;
                        let avoiding = avoided.is_some() || session.all_avoid(me, &senders).await?;
                        Ok::<_, Error>((drained, avoiding))
                    };
                    let asked = tokio::select! {
                        biased;
                        _ = session.maintain() => break,
                        asked = tokio::time::timeout_at(deadline, ask) => asked,
                    };
                    let (drained, avoiding) = match asked {
                        Ok(Ok(answer)) => answer,
                        _ => (false, false),
                    };
                    if avoided.is_none() && avoiding {
                        avoided = Some(tokio::time::Instant::now());
                    }
                    let done = drained || settled(avoided);
                    if done || !pause(crate::dist::HubSession::poll()).await {
                        break;
                    }
                }
            }
            #[cfg(feature = "hub")]
            Peers::Hub(_) => {}
        }
        segment.close();
        while segment.admitted() > 0 {
            if !pause(DRAIN_POLL).await {
                break;
            }
        }
    }
}

/// How often a draining instance looks again.
const DRAIN_POLL: Duration = Duration::from_millis(50);

/// A segment process before and beyond its graph: the signals that stop it,
/// the system listener that reports on it, its velo node, and its metrics.
/// Started first, before the nodes are built: a signal while they load
/// would otherwise end the process the default way, with nothing
/// announced, and a deployment could not tell that the process is live
/// while it starts. A clone shares the same process.
#[derive(Clone)]
pub struct Process {
    /// How many stop signals have come.
    stops: tokio::sync::watch::Receiver<u32>,
    count: Arc<tokio::sync::watch::Sender<u32>>,
    health: Arc<Health>,
    system: Option<SocketAddr>,
    /// Bound at the start, so that the instance id, which every metric
    /// carries as `worker_id`, is known before the nodes build and
    /// register their metrics; taken by the one launch. A process is one
    /// instance: a second launch would share the node, and its handlers
    /// would replace the first's without a word.
    node: Arc<std::sync::Mutex<Option<Velo>>>,
    instance: velo::InstanceId,
    metrics_prefix: Option<String>,
}

impl Process {
    /// Take the stop signals, bind the system listener if `config` asks
    /// for one, then bind the instance's velo node: the listener first,
    /// so that the process is live whatever velo does.
    pub async fn start<H>(config: &ProcessConfig<H>) -> Result<Self> {
        let mut signals = Signals::new()?;
        let (count, stops) = tokio::sync::watch::channel(0u32);
        let count = Arc::new(count);
        let counting = Arc::clone(&count);
        tokio::spawn(async move {
            let count = counting;
            loop {
                tokio::select! {
                    () = signals.wait() => count.send_modify(|stops| *stops += 1),
                    () = count.closed() => return,
                }
            }
        });
        let health = Arc::new(Health::new());
        let system = system::listen(&config.system, Arc::clone(&health)).await?;
        if let Some(address) = system {
            eprintln!("system listener at {address}");
        }
        // Under a stop, as the rest of the start is: a container's bind
        // resolves its hostname, which may wait on DNS.
        let node = tokio::select! {
            biased;
            () = stopped(&stops, 1) => {
                return Err(Error::new(ErrorKind::Cancelled, "stopped while starting"));
            }
            node = config.bind() => node?,
        };
        Ok(Self {
            stops,
            count,
            health,
            system,
            instance: node.instance_id(),
            node: Arc::new(std::sync::Mutex::new(Some(node))),
            metrics_prefix: config.system.metrics_prefix(),
        })
    }

    /// The instance's velo id, which its metrics carry as `worker_id`.
    pub fn instance_id(&self) -> velo::InstanceId {
        self.instance
    }

    /// The instance's velo node, for its one launch.
    fn take_node(&self) -> Result<Velo> {
        self.node
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| {
                placement("a process launches once: it is one instance, with one velo node")
            })
    }

    /// Make the process's one metrics registry, for segment `segment` of
    /// `deployment`, and serve it on `/metrics`. Every series it serves
    /// carries the prefix and dynamo's identity labels: `dynamo_namespace`
    /// (the graph's name and the first eight hex digits of the placement
    /// hash), `dynamo_component` (the segment), and `worker_id` (the
    /// instance id). It holds `uptime_seconds` from the start.
    pub fn registry(
        &self,
        deployment: &gdl_model::DeploymentSpec,
        segment: &str,
    ) -> Result<prometheus::Registry> {
        let registration = deployment.registration(segment)?;
        let labels = [
            (
                gdl_model::NAMESPACE_LABEL,
                format!(
                    "{}-{:08x}",
                    deployment.graph.name,
                    registration.placement >> 32
                ),
            ),
            (gdl_model::COMPONENT_LABEL, segment.to_string()),
            (
                gdl_model::WORKER_LABEL,
                format!("{:032x}", self.instance.as_u128()),
            ),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
        let registry = prometheus::Registry::new_custom(self.metrics_prefix.clone(), Some(labels))
            .map_err(metrics_error)?;
        let health = Arc::clone(&self.health);
        let uptime = prometheus::PullingGauge::new(
            "uptime_seconds",
            "Seconds since the segment process started.",
            Box::new(move || health.uptime()),
        )
        .map_err(metrics_error)?;
        registry.register(Box::new(uptime)).map_err(metrics_error)?;
        if !self.health.serve_metrics(registry.clone()) {
            return Err(Error::new(
                ErrorKind::Placement,
                "the process's metrics registry is made once",
            ));
        }
        Ok(registry)
    }

    /// Stop the process as a signal does: the first stop drains, the
    /// second ends the drain. For a process that embeds a segment and stops
    /// it itself.
    pub fn stop(&self) {
        self.count.send_modify(|stops| *stops += 1);
    }

    /// The system listener's address, if it has one.
    pub fn system_address(&self) -> Option<SocketAddr> {
        self.system
    }

    /// Run `start`, the start of the process, unless a stop comes first,
    /// which ends it with `Cancelled`. A stop that came before is seen at
    /// once.
    pub async fn starting<T, E: From<Error>>(
        &self,
        start: impl Future<Output = std::result::Result<T, E>>,
    ) -> std::result::Result<T, E> {
        tokio::select! {
            biased;
            () = self.stops(1) => Err(Error::new(ErrorKind::Cancelled, "stopped while starting").into()),
            result = start => result,
        }
    }

    /// Resolves once `n` stop signals have come.
    async fn stops(&self, n: u32) {
        stopped(&self.stops, n).await;
    }
}

/// Resolves once `n` stop signals have come, as `stops` counts them.
async fn stopped(stops: &tokio::sync::watch::Receiver<u32>, n: u32) {
    let mut stops = stops.clone();
    if stops.wait_for(|stops| *stops >= n).await.is_err() {
        // The signal task is gone only with the runtime.
        std::future::pending::<()>().await;
    }
}

/// The signals that ask a process to stop. Registered on construction, so a
/// signal sent after this exists is never lost to the default disposition.
struct Signals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl Signals {
    /// Both streams exist from here: `tokio::signal::ctrl_c` would install
    /// its handler only when first polled, and a SIGINT in between would
    /// end the process the default way, with no drain.
    fn new() -> Result<Self> {
        #[cfg(unix)]
        let stream = |kind| {
            tokio::signal::unix::signal(kind)
                .map_err(|e| Error::new(ErrorKind::Transport, e.to_string()))
        };
        Ok(Self {
            #[cfg(unix)]
            interrupt: stream(tokio::signal::unix::SignalKind::interrupt())?,
            #[cfg(unix)]
            terminate: stream(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => {}
                _ = self.terminate.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

/// An environment variable's key as a settings path: `__` separates nested
/// keys, and each key is lowercased except a node or adapter name, which is
/// the name's own spelling. `[node.Truncate]` is therefore reachable as
/// `GDL_NODE__TRUNCATE__MAX`, though figment would otherwise lowercase the
/// name and the table would not match.
fn canonical<H: Hosted>(key: &str) -> String {
    let mut path = Vec::new();
    for (index, part) in key.split("__").enumerate() {
        let named = index == 1
            && path
                .first()
                .is_some_and(|first| first == "node" || first == "adapter");
        let part = match named {
            true => H::NAMES
                .iter()
                .find(|name| name.eq_ignore_ascii_case(part))
                .map_or_else(|| part.to_ascii_lowercase(), ToString::to_string),
            false => part.to_ascii_lowercase(),
        };
        path.push(part);
    }
    path.join(".")
}

fn placement(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Placement, message)
}

fn metrics_error(error: prometheus::Error) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("the metrics registry: {error}"),
    )
}
