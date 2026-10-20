//! What a segment's binary does besides running its graph: read its
//! settings, find its peers, announce that it is serving, and stop when
//! told to. A generated binary supplies only what it hosts; everything
//! here is the same for every segment, so it is written once and tested
//! here rather than in each generated crate.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use figment::value::{Dict, Tag, Value};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use velo::Velo;

use crate::cx::Cx;
use crate::dist::{Roster, RuntimeConfig, bind_node};
use crate::error::{Error, ErrorKind, Result};

/// The settings of one segment process. `H` is what the segment hosts: the
/// tables of its nodes and of its frontend or entry adapters, which the
/// generated binary declares. An unknown key is an error at either level.
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
/// ```
pub struct ProcessConfig<H> {
    listen: SocketAddr,
    roster: Option<PathBuf>,
    hub: Option<HubAddress>,
    rendezvous_ms: u64,
    instances: BTreeMap<String, usize>,
    min_replicas: BTreeMap<String, usize>,
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
const KEYS: [&str; 7] = [
    "listen",
    "roster",
    "hub",
    "rendezvous_ms",
    "instances",
    "min_replicas",
    "runtime",
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
    /// keys are as spelled.
    pub fn load(paths: impl IntoIterator<Item = impl AsRef<Path>>) -> Result<Self> {
        let mut figment = Figment::new();
        for path in paths {
            figment = figment.merge(Toml::file_exact(path.as_ref()));
        }
        Self::from_figment(figment.merge(Self::env()))
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
            runtime: settings.runtime,
            hosted,
        };
        config.discovery()?;
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
    /// instances of each segment and takes those as its roster. How many
    /// instances a segment has under a hub is not fixed.
    pub async fn launch(
        &self,
        segment: &'static str,
        segments: &'static [&'static str],
        deployment: &gdl_model::DeploymentSpec,
        ready: impl Future<Output = Result<()>>,
    ) -> Result<Launch> {
        // The instances this one waits for before it serves: those of the
        // segments it sends to.
        let inner: Vec<String> = gdl_model::layout(&deployment.graph, &deployment.placement)?
            .sends_to(segment)
            .into_iter()
            .collect();
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
        if !other.is_empty() {
            return Err(placement(
                "[instances] counts the instances a roster directory waits for, [min_replicas] the loaded instances a hub waits for; a deployment uses one",
            ));
        }
        // Every instance waits for the same membership.
        let expect: Vec<(&str, usize)> = segments
            .iter()
            .map(|s| (*s, counts.get(*s).copied().unwrap_or(1)))
            .collect();
        let wait = Duration::from_millis(self.rendezvous_ms);
        let (node, roster, peers) = match discovery {
            Discovery::Files(dir) => {
                ready.await?;
                let node = bind_node(self.listen).await?;
                let roster = Roster::rendezvous(dir, segment, &node, expect, wait).await?;
                (node, roster, Peers::Files(dir.to_path_buf()))
            }
            #[cfg(feature = "hub")]
            Discovery::Hub(hub) => {
                use crate::dist::{HubSession, bind_container_node};
                let HubAddress {
                    host,
                    public_port,
                    control_port,
                    heartbeat_ms,
                } = hub;
                // A container binds and advertises the address its hostname
                // resolves to; a concrete address is bound and advertised as
                // given.
                let node = if self.listen.ip().is_unspecified() {
                    bind_container_node(self.listen.port()).await?
                } else {
                    bind_node(self.listen).await?
                };
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
                let roster = session.roster(&expect, wait).await?;
                (node, roster, Peers::Hub(session))
            }
            #[cfg(not(feature = "hub"))]
            Discovery::Hub(hub) => {
                let _ = ready;
                return Err(placement(format!(
                    "`[hub] host = {:?}`, but this binary was built without hub discovery (gdl's `hub` feature)",
                    hub.host
                )));
            }
        };
        // Held until the barrier: a request that reaches this instance
        // first waits, rather than leaving for a peer that is not serving.
        roster.hold();
        Ok(Launch {
            node,
            roster,
            peers,
            segment,
            inner,
            wait,
        })
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
    wait: Duration,
}

enum Peers {
    Files(PathBuf),
    #[cfg(feature = "hub")]
    Hub(crate::dist::HubSession),
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

    /// Announce that this instance's handlers are installed, wait until
    /// every instance it can route to has announced the same, release the
    /// roster, then run `serve` until it finishes. Its context is cancelled by SIGINT or
    /// SIGTERM, after which `serve` is left to finish on its own terms, so a
    /// frontend drains the requests it holds; a second signal ends the wait.
    /// A lost hub lease cancels the context the same way, and the drained
    /// body's result gives way to the lease's error: the process is
    /// invisible to new peers and will be probed as dead, so the deployment
    /// must start it again.
    ///
    /// No request may leave before every instance it can be routed to has
    /// registered its handlers, so call this after the segment has started.
    /// A request that reaches this instance earlier waits at the held
    /// roster.
    pub async fn serve<F, Fut>(mut self, serve: F) -> Result<()>
    where
        F: FnOnce(Cx) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let inner: Vec<&str> = self.inner.iter().map(String::as_str).collect();
        match &mut self.peers {
            Peers::Files(dir) => {
                self.roster
                    .ready(
                        &dir.join("ready"),
                        self.segment,
                        &self.node,
                        &inner,
                        self.wait,
                    )
                    .await?
            }
            #[cfg(feature = "hub")]
            Peers::Hub(session) => session.ready(&self.roster, &inner, self.wait).await?,
        }
        self.roster.release();
        eprintln!("{}: ready", self.segment);
        let shutdown = Cx::new();
        let mut signals = Signals::new()?;
        let body = serve(shutdown.clone());
        tokio::pin!(body);
        let lease = async {
            match &self.peers {
                Peers::Files(_) => std::future::pending().await,
                #[cfg(feature = "hub")]
                Peers::Hub(session) => session.maintain().await,
            }
        };
        let stop = tokio::select! {
            result = &mut body => return result,
            () = signals.wait() => Ok(()),
            result = lease => result,
        };
        shutdown.cancel();
        tokio::select! {
            result = &mut body => stop.and(result),
            () = signals.wait() => Err(Error::new(
                ErrorKind::Cancelled,
                "stopped by a second signal before the drain finished",
            )),
        }
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

/// Run a segment binary: `run` to completion on a runtime of its own, a
/// failure printed as `segment: error`, and the process ended. The
/// runtime is shut down in the background rather than waited for: a read
/// of stdin parked on its blocking pool (a JSON-lines source whose input
/// stayed open and idle while a signal stopped the graph) would otherwise
/// hold the process after the graph has stopped, and swallow the next
/// signal.
pub fn segment_main<E: std::fmt::Display>(
    segment: &str,
    run: impl Future<Output = std::result::Result<(), E>>,
) -> std::process::ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("{segment}: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(run);
    runtime.shutdown_background();
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{segment}: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
