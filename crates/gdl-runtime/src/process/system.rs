//! The system listener: the live and health probes a deployment asks a
//! segment process, and its metrics, on dynamo's paths and port, so that
//! the dynamo operator's probes and scrapes find it. HTTP is read by hand:
//! a request line and headers, one request per connection. Three fixed
//! paths do not justify a server dependency in every segment binary.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use gdl::{Error, ErrorKind, Result};

/// `[system]`: where the listener is, and the prefix of the metrics it
/// serves. Also read from dynamo's variables, `DYN_SYSTEM_HOST`,
/// `DYN_SYSTEM_PORT`, `DYN_SYSTEM_LIVE_PATH`, and `DYN_SYSTEM_HEALTH_PATH`,
/// with dynamo's meaning: a negative port is no listener, 0 an ephemeral
/// one. Off unless a port is set: instances that share a host would
/// collide on any default.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SystemSettings {
    #[serde(default = "default_host")]
    host: IpAddr,
    #[serde(default = "default_port")]
    port: i64,
    #[serde(default = "default_live_path")]
    live_path: String,
    #[serde(default = "default_health_path")]
    health_path: String,
    /// The prefix of every metric's name, dynamo's by default, so that
    /// dynamo's scrape configuration and dashboards take gdl's metrics as
    /// they are; empty for none.
    #[serde(default = "default_metrics_prefix")]
    metrics_prefix: String,
}

impl Default for SystemSettings {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            live_path: default_live_path(),
            health_path: default_health_path(),
            metrics_prefix: default_metrics_prefix(),
        }
    }
}

fn default_host() -> IpAddr {
    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
}

fn default_port() -> i64 {
    -1
}

fn default_live_path() -> String {
    "/live".into()
}

fn default_health_path() -> String {
    "/health".into()
}

fn default_metrics_prefix() -> String {
    "dynamo_component".into()
}

/// Where the metrics are served: dynamo's path, which its scrape
/// configuration expects.
const METRICS_PATH: &str = "/metrics";

impl SystemSettings {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.port > i64::from(u16::MAX) {
            return Err(invalid(format!(
                "`system.port` {} is not a port",
                self.port
            )));
        }
        for (key, path) in [
            ("live_path", &self.live_path),
            ("health_path", &self.health_path),
        ] {
            if !path.starts_with('/') || path.contains(char::is_whitespace) {
                return Err(invalid(format!(
                    "`system.{key}` {path:?} is not a path: it starts with `/` and holds no space"
                )));
            }
        }
        if self.live_path == self.health_path {
            return Err(invalid(
                "`system.live_path` and `system.health_path` are the same path",
            ));
        }
        if self.live_path == METRICS_PATH || self.health_path == METRICS_PATH {
            return Err(invalid(
                "a probe path cannot be `/metrics`: `/metrics` is the metrics path",
            ));
        }
        let prefix = &self.metrics_prefix;
        let valid = prefix.is_empty()
            || (prefix.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_' || c == ':')
                && prefix
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':'));
        if !valid {
            return Err(invalid(format!(
                "`system.metrics_prefix` {prefix:?} is not a metric name prefix: letters, digits, `_`, and `:`, not starting with a digit"
            )));
        }
        Ok(())
    }

    /// The prefix of every metric's name, if any.
    pub(crate) fn metrics_prefix(&self) -> Option<String> {
        // prometheus refuses an empty prefix: no prefix is `None`.
        (!self.metrics_prefix.is_empty()).then(|| self.metrics_prefix.clone())
    }

    /// The address to bind, if a listener is asked for.
    fn address(&self) -> Option<SocketAddr> {
        u16::try_from(self.port)
            .ok()
            .map(|port| SocketAddr::new(self.host, port))
    }
}

/// What the probes report: a process is live from the moment it starts,
/// and healthy only while it serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Building nodes, meeting peers, waiting at the barrier.
    Starting = 0,
    /// Past the barrier, with the roster released.
    Ready = 1,
    /// Stopped or without a lease: finishing what it holds.
    Draining = 2,
}

impl Phase {
    /// Every phase, in the order a process passes through them.
    const ALL: [Self; 3] = [Self::Starting, Self::Ready, Self::Draining];

    fn name(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
        }
    }
}

/// The family of the process's phase, and its help.
pub(crate) const PHASE: &str = "instance_phase";
const PHASE_HELP: &str = "1 for the segment process's phase, 0 for the others: starting, ready, draining. Draining is final.";

/// `instance_phase`: one series for each phase, read from the process's
/// health at each scrape, so that an operator that retired the instance
/// sees it drain on the same scrape as what it still holds. It holds the
/// health weakly: the health holds the registry it is in.
pub(crate) struct Phases {
    health: Weak<Health>,
    desc: Desc,
}

impl Phases {
    pub(crate) fn new(health: &Arc<Health>) -> prometheus::Result<Self> {
        Ok(Self {
            health: Arc::downgrade(health),
            desc: Desc::new(
                PHASE.into(),
                PHASE_HELP.into(),
                vec!["phase".into()],
                Default::default(),
            )?,
        })
    }
}

impl Collector for Phases {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let Some(health) = self.health.upgrade() else {
            return Vec::new();
        };
        let now = health.phase();
        let series = Phase::ALL
            .into_iter()
            .map(|phase| {
                let mut label = proto::LabelPair::default();
                label.set_name("phase".into());
                label.set_value(phase.name().into());
                let mut gauge = proto::Gauge::default();
                gauge.set_value(f64::from(u8::from(phase == now)));
                let mut metric = proto::Metric::from_label(vec![label]);
                metric.set_gauge(gauge);
                metric
            })
            .collect();
        let mut family = MetricFamily::default();
        family.set_name(PHASE.into());
        family.set_help(PHASE_HELP.into());
        family.set_field_type(MetricType::GAUGE);
        family.set_metric(series);
        vec![family]
    }
}

/// What the listener reports, shared between the process and its
/// listener: the phase, and the process's metrics once they exist.
pub(crate) struct Health {
    phase: AtomicU8,
    /// Set while the start asks a hub that is not up yet.
    hub: AtomicBool,
    started: Instant,
    /// The process's registry, served once nothing more registers in it;
    /// until then `/metrics` answers with no metrics, not an error, which
    /// a scrape would take for a target that is down.
    registry: OnceLock<prometheus::Registry>,
    /// The process's own figures (`process_*`, `process_cgroup_*`), made
    /// with `registry` and gathered with it. A registry of their own keeps
    /// them out of `registry`'s prefix: a hub serves the same names, and
    /// dashboards know them unprefixed.
    own: OnceLock<prometheus::Registry>,
    /// How long a scrape may take before it is answered with 503.
    scrape_limit: Duration,
    /// `SCRAPES` permits: the scrapes whose gathers run.
    scrapes: Arc<tokio::sync::Semaphore>,
}

impl Health {
    pub(crate) fn new() -> Self {
        Self {
            phase: AtomicU8::new(Phase::Starting as u8),
            hub: AtomicBool::new(false),
            started: Instant::now(),
            registry: OnceLock::new(),
            own: OnceLock::new(),
            scrape_limit: SCRAPE_LIMIT,
            scrapes: Arc::new(tokio::sync::Semaphore::new(SCRAPES)),
        }
    }

    /// Seconds since the process started.
    pub(crate) fn uptime(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    /// Gather `own`, the process's own figures, with the served registry;
    /// `false` if they are set already.
    pub(crate) fn own_metrics(&self, own: prometheus::Registry) -> bool {
        self.own.set(own).is_ok()
    }

    /// Serve `registry` on `/metrics`; `false` if a registry is served
    /// already.
    pub(crate) fn serve_metrics(&self, registry: prometheus::Registry) -> bool {
        self.registry.set(registry).is_ok()
    }

    /// The status code and body of a scrape: the metrics in Prometheus's
    /// text format, or why there are none.
    ///
    /// A node's `ScrapeGaugeVec` runs its closure inside the gather, so
    /// the gather runs on a blocking thread, not on the runtime worker
    /// that answers this connection. A scrape past `scrape_limit` is
    /// answered with 503, but its gather runs on until the closure
    /// returns, holding its thread, one of the `SCRAPES` places, and the
    /// registry's lock. Once `SCRAPES` gathers never return, every scrape
    /// is answered with 503. A scrape that finds `SCRAPES` running is
    /// answered with 503 at once: waiting, it would hold a connection the
    /// probes need.
    async fn scrape(&self) -> (u16, String) {
        let Some(registry) = self.registry.get().cloned() else {
            return (200, String::new());
        };
        let own = self.own.get().cloned();
        let Ok(permit) = Arc::clone(&self.scrapes).try_acquire_owned() else {
            return (503, "a scrape is in progress".into());
        };
        let gather = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut text = String::new();
            let mut families = registry.gather();
            if let Some(own) = own {
                families.extend(own.gather());
            }
            prometheus::TextEncoder::new()
                .encode_utf8(&families, &mut text)
                .map(|()| text)
        });
        match tokio::time::timeout(self.scrape_limit, gather).await {
            Ok(Ok(Ok(text))) => (200, text),
            Ok(Ok(Err(error))) => (500, error.to_string()),
            Ok(Err(_)) => (500, "a metric's closure panicked".into()),
            Err(_) => (
                503,
                format!(
                    "the scrape took longer than {} ms",
                    self.scrape_limit.as_millis()
                ),
            ),
        }
    }

    pub(crate) fn phase(&self) -> Phase {
        match self.phase.load(Ordering::Acquire) {
            0 => Phase::Starting,
            1 => Phase::Ready,
            _ => Phase::Draining,
        }
    }

    /// Move to `phase`. Draining is final: a process that began to drain
    /// does not become ready again.
    pub(crate) fn set(&self, phase: Phase) {
        let _ = self
            .phase
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |now| {
                (now != Phase::Draining as u8).then_some(phase as u8)
            });
    }

    /// Whether the start is waiting for its hub to answer, which the
    /// health body says while it lasts.
    #[cfg_attr(not(feature = "hub"), allow(dead_code))]
    pub(crate) fn waiting_for_hub(&self, waiting: bool) {
        self.hub.store(waiting, Ordering::Release);
    }

    /// dynamo's body, with gdl's phase added, and what a start waits for.
    fn body(&self) -> (bool, String) {
        let phase = self.phase();
        let ready = phase == Phase::Ready;
        let waiting = match self.hub.load(Ordering::Acquire) {
            true => ",\"waiting\":\"hub\"",
            false => "",
        };
        let body = format!(
            "{{\"status\":\"{}\",\"uptime\":{:.3},\"phase\":\"{}\"{waiting}}}",
            if ready { "ready" } else { "notready" },
            self.uptime(),
            phase.name()
        );
        (ready, body)
    }
}

/// The most a request line and its headers may take.
const HEAD_LIMIT: usize = 8 << 10;

/// How long a client may take to send them.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The most of a refused request read and dropped before closing.
const DRAIN_LIMIT: usize = 64 << 10;

/// How long a scrape may take: a node's closures run inside it.
/// Prometheus gives up on a scrape after 10 s by default.
const SCRAPE_LIMIT: Duration = Duration::from_secs(5);

/// Scrapes gathered at once: an HA Prometheus pair and a person; few threads for stuck closures.
const SCRAPES: usize = 4;

/// How long the listener waits after an accept fails.
const ACCEPT_PAUSE: Duration = Duration::from_millis(100);

/// Connections answered at once; more wait to be accepted. A probe that
/// finds them all held by silent clients waits for their read timeout; the
/// port is the deployment's own.
const CONNECTIONS: usize = 64;

/// Bind the listener `settings` ask for, if any, and serve `health` on it
/// until the process exits. Returns the bound address.
pub(crate) async fn listen(
    settings: &SystemSettings,
    health: Arc<Health>,
) -> Result<Option<SocketAddr>> {
    let Some(address) = settings.address() else {
        return Ok(None);
    };
    let listener = TcpListener::bind(address).await.map_err(|e| {
        Error::new(
            ErrorKind::Transport,
            format!("the system listener at {address}: {e}"),
        )
    })?;
    let bound = listener
        .local_addr()
        .map_err(|e| Error::new(ErrorKind::Transport, e.to_string()))?;
    let paths = Arc::new((settings.live_path.clone(), settings.health_path.clone()));
    let slots = Arc::new(tokio::sync::Semaphore::new(CONNECTIONS));
    tokio::spawn(async move {
        loop {
            let Ok(permit) = Arc::clone(&slots).acquire_owned().await else {
                return;
            };
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                // Out of file descriptors, the listener stays readable and
                // every accept fails at once: without a pause the loop
                // would spin a worker until one frees.
                Err(_) => {
                    drop(permit);
                    tokio::time::sleep(ACCEPT_PAUSE).await;
                    continue;
                }
            };
            let (health, paths) = (Arc::clone(&health), Arc::clone(&paths));
            tokio::spawn(async move {
                let _ = answer(stream, &health, &paths.0, &paths.1).await;
                drop(permit);
            });
        }
    });
    Ok(Some(bound))
}

/// Answer one request on `stream`.
async fn answer(
    mut stream: TcpStream,
    health: &Health,
    live_path: &str,
    health_path: &str,
) -> std::io::Result<()> {
    let head = match tokio::time::timeout(READ_TIMEOUT, read_head(&mut stream)).await {
        Ok(Ok(Some(head))) => head,
        Ok(Ok(None)) => {
            respond(&mut stream, 431, JSON, "{}").await?;
            // Closing with the rest of the request unread resets the
            // connection, which may destroy the refusal before the client
            // reads it. What is left is read and dropped, within bounds.
            let mut rest = [0u8; 4096];
            let drain = async {
                let mut left = DRAIN_LIMIT;
                while left > 0 {
                    match stream.read(&mut rest).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => left = left.saturating_sub(read),
                    }
                }
            };
            let _ = tokio::time::timeout(READ_TIMEOUT, drain).await;
            return Ok(());
        }
        Ok(Err(e)) => return Err(e),
        Err(_) => return respond(&mut stream, 408, JSON, "{}").await,
    };
    let mut words = head.split(' ');
    let (method, target) = (words.next().unwrap_or(""), words.next().unwrap_or(""));
    let path = target.split('?').next().unwrap_or("");
    let (code, kind, body) = match (method, path) {
        ("GET", path) if path == live_path => (200, JSON, health.body().1),
        ("GET", path) if path == health_path => {
            let (ready, body) = health.body();
            (if ready { 200 } else { 503 }, JSON, body)
        }
        ("GET", METRICS_PATH) => {
            let (code, body) = health.scrape().await;
            (code, prometheus::TEXT_FORMAT, body)
        }
        ("GET", _) => (404, JSON, "{}".into()),
        _ => (405, JSON, "{}".into()),
    };
    respond(&mut stream, code, kind, &body).await
}

/// The probes' content type.
const JSON: &str = "application/json";

/// The request line, once the whole head has arrived; `None` past
/// `HEAD_LIMIT`.
async fn read_head(stream: &mut TcpStream) -> std::io::Result<Option<String>> {
    let mut head = Vec::with_capacity(512);
    let mut chunk = [0u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        head.extend_from_slice(&chunk[..read]);
        if let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") {
            let line_end = head[..end]
                .windows(2)
                .position(|w| w == b"\r\n")
                .unwrap_or(end);
            return Ok(Some(
                String::from_utf8_lossy(&head[..line_end]).into_owned(),
            ));
        }
        if head.len() > HEAD_LIMIT {
            return Ok(None);
        }
    }
}

async fn respond(stream: &mut TcpStream, code: u16, kind: &str, body: &str) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        500 => "Internal Server Error",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        431 => "Request Header Fields Too Large",
        _ => "Service Unavailable",
    };
    let response = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

/// Ask a running process's health probe, as `--healthcheck` does: the
/// listener `settings` name, on loopback when it binds every address.
pub(crate) async fn healthcheck(settings: &SystemSettings) -> Result<()> {
    let address = match settings.address() {
        None => {
            return Err(invalid(
                "no system listener is configured: set `system.port` or DYN_SYSTEM_PORT",
            ));
        }
        Some(address) if address.port() == 0 => {
            return Err(invalid(
                "the system listener's port is 0, chosen when it binds; a health check needs a fixed one",
            ));
        }
        Some(address) if address.ip().is_unspecified() => {
            let loopback = match address.ip() {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(_) => IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            };
            SocketAddr::new(loopback, address.port())
        }
        Some(address) => address,
    };
    let asked = async {
        let mut stream = TcpStream::connect(address).await?;
        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n",
            settings.health_path
        );
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    };
    let response = tokio::time::timeout(READ_TIMEOUT, asked)
        .await
        .map_err(|_| unhealthy(format!("{address} did not answer in time")))?
        .map_err(|e| unhealthy(format!("{address}: {e}")))?;
    let response = String::from_utf8_lossy(&response);
    let status = response.lines().next().unwrap_or("");
    if status.starts_with("HTTP/1.1 200 ") {
        Ok(())
    } else {
        let body = response.split("\r\n\r\n").nth(1).unwrap_or("");
        Err(unhealthy(format!("{address}: {status} {body}")))
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Placement, message)
}

fn unhealthy(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Transport, message)
}

#[cfg(test)]
mod tests;
