//! What the server's host can deploy to, and applying a Compose bundle
//! there.
//!
//! ```text
//! page --GET /api/targets-------------> docker compose version, docker info,
//!                                       kubectl config current-context
//!      --POST /api/deploy/apply {draft}-> regenerate, place in <target>/deployments/<project>,
//!                                        docker compose -p <project> up --build --detach
//!      <-- one JSON object per line: {directory, project}, {line}..., {done, ok, code}
//! ```
//!
//! Detection is always on and only reads: no probe builds, starts, or
//! contacts a cluster. Applying runs image builds and publishes ports on
//! this host, so it needs `--docker`, as local imports need `--local-root`.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::AsyncBufReadExt as _;

/// The programs the server deploys with, and whether it may apply.
#[derive(Clone, Debug)]
pub struct Deploy {
    /// The `docker` CLI, with its `compose` plugin.
    pub docker: PathBuf,
    /// The `kubectl` CLI; only detected.
    pub kubectl: PathBuf,
    /// Whether the page may build and start a deployment on this host.
    pub apply: bool,
}

impl Default for Deploy {
    fn default() -> Self {
        Self {
            docker: "docker".into(),
            kubectl: "kubectl".into(),
            apply: false,
        }
    }
}

/// What this host offers for each deployment target.
#[derive(Debug, Serialize)]
pub(crate) struct Targets {
    pub(crate) compose: Compose,
    pub(crate) kubernetes: Kubernetes,
}

#[derive(Debug, Serialize)]
pub(crate) struct Compose {
    /// `docker compose version --short`.
    pub(crate) version: Option<String>,
    /// The daemon's version, from `docker info`.
    pub(crate) daemon: Option<String>,
    /// Both the CLI and the daemon answered.
    pub(crate) available: bool,
    /// Available, and the server was started with `--docker`.
    pub(crate) apply: bool,
    pub(crate) problem: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Kubernetes {
    pub(crate) kubectl: bool,
    /// The file kubectl reads: the first existing one of `KUBECONFIG`,
    /// else `~/.kube/config`.
    pub(crate) kubeconfig: Option<PathBuf>,
    pub(crate) context: Option<String>,
}

/// A wedged daemon makes `docker info` wait without end; the page asks on
/// each visit to Deploy, so each probe gets this long.
const PROBE: Duration = Duration::from_secs(if cfg!(test) { 1 } else { 5 });

/// Ask the host what it offers. The probes run at once.
pub(crate) async fn targets(deploy: &Deploy) -> Targets {
    let (version, daemon, context) = tokio::join!(
        probe(&deploy.docker, &["compose", "version", "--short"]),
        probe(&deploy.docker, &["info", "--format", "{{.ServerVersion}}"]),
        probe(&deploy.kubectl, &["config", "current-context"]),
    );
    let problem = match (&version, &daemon) {
        (Err(problem), _) => Some(problem.to_string()),
        (Ok(_), Err(Probe::Failed(problem))) => {
            Some(format!("the Docker daemon did not answer: {problem}"))
        }
        (Ok(_), Err(problem)) => Some(problem.to_string()),
        (Ok(_), Ok(_)) => None,
    };
    let available = problem.is_none();
    let home = std::env::var_os("HOME").map(PathBuf::from);
    Targets {
        compose: Compose {
            version: version.ok(),
            daemon: daemon.ok(),
            available,
            apply: available && deploy.apply,
            problem,
        },
        kubernetes: Kubernetes {
            kubectl: !matches!(context, Err(Probe::Missing(_))),
            kubeconfig: kubeconfig(std::env::var_os("KUBECONFIG").as_deref(), home.as_deref()),
            context: context.ok(),
        },
    }
}

enum Probe {
    Missing(String),
    Failed(String),
}

impl std::fmt::Display for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Probe::Missing(program) => write!(f, "`{program}` was not found"),
            Probe::Failed(problem) => f.write_str(problem),
        }
    }
}

/// `program args`'s first line of output, or why there is none.
async fn probe(program: &Path, args: &[&str]) -> Result<String, Probe> {
    let mut command = tokio::process::Command::new(program);
    command.args(args).stdin(Stdio::null()).kill_on_drop(true);
    let shown = || format!("{} {}", name(program), args.join(" "));
    let output = match tokio::time::timeout(PROBE, command.output()).await {
        Err(_) => {
            return Err(Probe::Failed(format!(
                "`{}` did not answer within {}s",
                shown(),
                PROBE.as_secs()
            )));
        }
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Probe::Missing(name(program)));
        }
        Ok(Err(e)) => return Err(Probe::Failed(format!("`{}`: {e}", shown()))),
        Ok(Ok(output)) => output,
    };
    let first = |bytes: &[u8]| {
        String::from_utf8_lossy(bytes)
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty())
            .unwrap_or_default()
            .to_string()
    };
    if output.status.success() {
        Ok(first(&output.stdout))
    } else {
        Err(Probe::Failed(first(&output.stderr)))
    }
}

fn name(program: &Path) -> String {
    program
        .file_name()
        .unwrap_or(program.as_os_str())
        .to_string_lossy()
        .into_owned()
}

/// The file kubectl reads: with `KUBECONFIG` set and not empty, the first
/// of its paths that exists, else `~/.kube/config` if it exists.
fn kubeconfig(var: Option<&OsStr>, home: Option<&Path>) -> Option<PathBuf> {
    match var.filter(|v| !v.is_empty()) {
        Some(list) => std::env::split_paths(list).find(|p| p.is_file()),
        None => home
            .map(|home| home.join(".kube/config"))
            .filter(|p| p.is_file()),
    }
}

/// The Compose project of graph `name`: lowercase letters, digits, `-`,
/// and `_`, after `gdl-`, which also gives it the leading letter Compose
/// requires. One graph is one project, so applying again updates it.
pub(crate) fn project(name: &str) -> String {
    let name: String = name
        .chars()
        .map(|c| match c.to_ascii_lowercase() {
            c @ ('a'..='z' | '0'..='9' | '_' | '-') => c,
            _ => '-',
        })
        .collect();
    format!("gdl-{name}")
}

/// Marks a directory under `deployments` as one this server placed, and
/// so one it may replace.
const PLACED: &str = ".gdl-deployment";

/// Write a deployment with `write` beside `deployments/<project>`, then put
/// it in that place, replacing the one there only if this server placed
/// it. Compose needs the files later (`logs`, `down`), so they stay.
///
/// `running` is taken and handed back so that the apply stays running for
/// as long as the files are written, on whatever thread writes them: a
/// request whose page leaves is dropped, but its blocking thread goes on.
pub(crate) fn place(
    deployments: &Path,
    project: &str,
    running: Running,
    write: impl FnOnce(&Path) -> Result<(), String>,
) -> Result<(PathBuf, Running), String> {
    let io = |what: &Path, e: std::io::Error| format!("{}: {e}", what.display());
    std::fs::create_dir_all(deployments).map_err(|e| io(deployments, e))?;
    let deployments = deployments.canonicalize().map_err(|e| io(deployments, e))?;
    let place = deployments.join(project);
    if place.exists() && !place.join(PLACED).is_file() {
        return Err(format!(
            "{} was not placed by this server; move it away to apply here",
            place.display()
        ));
    }
    let next = deployments.join(format!(".{project}.next"));
    if next.exists() {
        std::fs::remove_dir_all(&next).map_err(|e| io(&next, e))?;
    }
    write(&next)?;
    std::fs::write(next.join(PLACED), "").map_err(|e| io(&next, e))?;
    if place.exists() {
        std::fs::remove_dir_all(&place).map_err(|e| io(&place, e))?;
    }
    std::fs::rename(&next, &place).map_err(|e| io(&place, e))?;
    Ok((place, running))
}

/// Whether an apply is running. One at a time: two builds of one project
/// would race on its containers, and two of different ones on the host.
#[derive(Default)]
pub(crate) struct Applying(Arc<AtomicBool>);

impl Applying {
    pub(crate) fn start(&self) -> Option<Running> {
        (!self.0.swap(true, Ordering::AcqRel)).then(|| Running(Arc::clone(&self.0)))
    }
}

/// Held for as long as an apply runs.
#[derive(Debug)]
pub(crate) struct Running(Arc<AtomicBool>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Build and start the deployment in `dir` as `project`, detached: the
/// command ends once the containers are started, without waiting for the
/// segments to be healthy, whose start period covers loading models. Its
/// output arrives as `{line}` events, then one `{done, ok, code}` or
/// `{done, ok, problem}`. The command runs to its end, and holds `running`
/// until then, even when the receiver is dropped: a page that leaves
/// mid-build must not leave a half-started project behind.
pub(crate) fn compose_up(
    docker: &Path,
    dir: &Path,
    project: &str,
    running: Running,
) -> tokio::sync::mpsc::Receiver<Value> {
    let (events, receiver) = tokio::sync::mpsc::channel(256);
    let mut command = tokio::process::Command::new(docker);
    command
        .arg("compose")
        .args(["-p", project, "--project-directory"])
        .arg(dir)
        .arg("-f")
        .arg(dir.join("compose.yaml"))
        .args(["up", "--build", "--detach"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let docker = name(docker);
    tokio::spawn(async move {
        let _running = running;
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                let problem = if e.kind() == std::io::ErrorKind::NotFound {
                    format!("`{docker}` was not found")
                } else {
                    format!("`{docker} compose`: {e}")
                };
                let _ = events
                    .send(json!({"done": true, "ok": false, "problem": problem}))
                    .await;
                return;
            }
        };
        let stdout = child.stdout.take().map(forward(events.clone()));
        let stderr = child.stderr.take().map(forward(events.clone()));
        for reader in [stdout, stderr].into_iter().flatten() {
            let _ = reader.await;
        }
        let done = match child.wait().await {
            Ok(status) => json!({"done": true, "ok": status.success(), "code": status.code()}),
            Err(e) => json!({"done": true, "ok": false, "problem": e.to_string()}),
        };
        let _ = events.send(done).await;
    });
    receiver
}

/// Send each line `stream` writes as a `{line}` event. The stream is read
/// to its end whether or not anyone receives, so the command never blocks
/// on a full pipe.
fn forward<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    events: tokio::sync::mpsc::Sender<Value>,
) -> impl FnOnce(R) -> tokio::task::JoinHandle<()> {
    move |stream| {
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stream).split(b'\n');
            while let Ok(Some(line)) = lines.next_segment().await {
                let line = String::from_utf8_lossy(&line);
                let line = line.trim_end_matches('\r');
                if !events.is_closed() {
                    let _ = events.send(json!({"line": line})).await;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests;
