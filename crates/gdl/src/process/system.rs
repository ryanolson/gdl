//! The system listener: the live and health probes a deployment asks a
//! segment process, on dynamo's paths and port, so that the dynamo
//! operator's probes find it. HTTP is read by hand: a request line and
//! headers, one request per connection. Three fixed paths do not justify a
//! server dependency in every segment binary.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::error::{Error, ErrorKind, Result};

/// `[system]`: where the listener is. Also read from dynamo's variables,
/// `DYN_SYSTEM_HOST`, `DYN_SYSTEM_PORT`, `DYN_SYSTEM_LIVE_PATH`, and
/// `DYN_SYSTEM_HEALTH_PATH`, with dynamo's meaning: a negative port is no
/// listener, 0 an ephemeral one. Off unless a port is set: instances that
/// share a host would collide on any default.
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
}

impl Default for SystemSettings {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            live_path: default_live_path(),
            health_path: default_health_path(),
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
        Ok(())
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
    fn name(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
        }
    }
}

/// The phase, shared between the process and its listener.
pub(crate) struct Health {
    phase: AtomicU8,
    started: Instant,
}

impl Health {
    pub(crate) fn new() -> Self {
        Self {
            phase: AtomicU8::new(Phase::Starting as u8),
            started: Instant::now(),
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

    /// dynamo's body, with gdl's phase added.
    fn body(&self) -> (bool, String) {
        let phase = self.phase();
        let ready = phase == Phase::Ready;
        let body = format!(
            "{{\"status\":\"{}\",\"uptime\":{:.3},\"phase\":\"{}\"}}",
            if ready { "ready" } else { "notready" },
            self.started.elapsed().as_secs_f64(),
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
            respond(&mut stream, 431, "{}").await?;
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
        Err(_) => return respond(&mut stream, 408, "{}").await,
    };
    let mut words = head.split(' ');
    let (method, target) = (words.next().unwrap_or(""), words.next().unwrap_or(""));
    let path = target.split('?').next().unwrap_or("");
    let (code, body) = match (method, path) {
        ("GET", path) if path == live_path => (200, health.body().1),
        ("GET", path) if path == health_path => {
            let (ready, body) = health.body();
            (if ready { 200 } else { 503 }, body)
        }
        ("GET", _) => (404, "{}".into()),
        _ => (405, "{}".into()),
    };
    respond(&mut stream, code, &body).await
}

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

async fn respond(stream: &mut TcpStream, code: u16, body: &str) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        431 => "Request Header Fields Too Large",
        _ => "Service Unavailable",
    };
    let response = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
