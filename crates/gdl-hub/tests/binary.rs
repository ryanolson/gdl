//! The hub binary refuses a pool's settings it cannot run before it binds
//! a port, and says what to set; a hub that serves stops on either signal
//! that stops a container; and it writes its log to stderr, at the level
//! `RUST_LOG` sets. A hub that starts here takes ports the
//! OS chooses, through `GDL_HUB__PUBLIC_PORT` and `GDL_HUB__CONTROL_PORT`.

use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use gdl_model::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};

fn spec() -> DeploymentSpec {
    let graph = Manifest {
        name: "pools".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "D".into(),
                role: Role::Sink,
                binding: Some("d".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("T", true, Default::default())],
            backward_types: vec![TypeInfo::new("T", true, Default::default())],
        }],
    };
    let placement = Placement::new()
        .segment("a", ["S"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["d"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

#[test]
fn the_binary_refuses_a_backend_it_cannot_publish_to() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-binary-")
        .tempdir()
        .expect("a directory");
    let deployment = dir.path().join("deployment.json");
    std::fs::write(
        &deployment,
        serde_json::to_vec(&spec()).expect("valid test fixture"),
    )
    .expect("written");
    // `relay` is the default; a child's hub reports to its parent's.
    for (directory, expected) in [
        (None, "GDL_POOL__PARENT_HUB: missing"),
        (
            Some("memory"),
            "[pool] directory = \"memory\" is a directory inside one process",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_gdl-hub"));
        command
            .arg(&deployment)
            .env("GDL_POOL__NAME", "model-x-01-us-east-1")
            .env("GDL_POOL__CLASS", "model_x")
            .env("GDL_POOL__REGION", "us-east-1")
            .env("GDL_POOL__HUB", "model-x-01-hub:1337");
        if let Some(directory) = directory {
            command.env("GDL_POOL__DIRECTORY", directory);
        }
        let output = command.output().expect("the binary runs");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "started with {directory:?}");
        assert!(stderr.contains(expected), "{stderr}");
        assert!(stderr.contains("GDL_POOL__PARENT_HUB"), "{stderr}");
    }
}

/// A parent pool's hub keeps its children's records in its state
/// directory: the binary refuses to start without one, or with one it
/// cannot read, rather than start without the records it kept.
#[test]
fn the_binary_refuses_a_parent_hub_with_nowhere_to_keep_its_childrens_records() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-binary-")
        .tempdir()
        .expect("a directory");
    let deployment = dir.path().join("deployment.json");
    std::fs::write(
        &deployment,
        serde_json::to_vec(&spec()).expect("valid test fixture"),
    )
    .expect("written");
    let file = dir.path().join("a-file");
    std::fs::write(&file, b"not a directory").expect("written");
    for (state_dir, expected) in [
        (None, "GDL_POOL__STATE_DIR: missing"),
        (
            Some(&file),
            "[pool] state_dir: the directory is unavailable",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_gdl-hub"));
        command
            .arg(&deployment)
            .env("GDL_POOL__NAME", "global-us")
            .env("GDL_POOL__CLASS", "global")
            .env("GDL_POOL__REGION", "us-east-1")
            .env("GDL_POOL__HUB", "global-us:1337")
            .env("GDL_HUB__PUBLIC_PORT", "0")
            .env("GDL_HUB__CONTROL_PORT", "0");
        if let Some(state_dir) = state_dir {
            command.env("GDL_POOL__STATE_DIR", state_dir);
        }
        let output = command.output().expect("the binary runs");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "started with {state_dir:?}");
        assert!(stderr.contains(expected), "{stderr}");
    }
}

/// A hub process, killed when the test ends however it ends, and each
/// line of its stderr, in order.
struct Serving {
    child: Child,
    lines: mpsc::Receiver<String>,
    /// The lines before the one that says it serves, which a task the
    /// hub starts before it serves can write.
    before: Vec<String>,
    /// The port of the control listener.
    control: u16,
}

impl Drop for Serving {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Serving {
    /// A hub for `deployment` on ports the OS chooses, once it says that it
    /// serves.
    fn start(deployment: &std::path::Path) -> Self {
        Self::start_with(deployment, |_| {})
    }

    /// As [`Serving::start`], with the command changed by `configure`
    /// first.
    fn start_with(deployment: &std::path::Path, configure: impl FnOnce(&mut Command)) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_gdl-hub"));
        command
            .arg(deployment)
            .env("GDL_HUB__PUBLIC_PORT", "0")
            .env("GDL_HUB__CONTROL_PORT", "0")
            .stderr(Stdio::piped());
        configure(&mut command);
        let mut child = command.spawn().expect("the binary runs");
        let stderr = child.stderr.take().expect("stderr");
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        let mut serving = Self {
            child,
            lines,
            before: Vec::new(),
            control: 0,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let line = serving
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|_| {
                    panic!("the hub says that it serves; it wrote {:?}", serving.before)
                });
            if line.starts_with("gdl-hub: serving") {
                serving.control = line
                    .rsplit(':')
                    .next()
                    .and_then(|port| port.parse().ok())
                    .unwrap_or_else(|| panic!("a control port in {line:?}"));
                return serving;
            }
            serving.before.push(line);
        }
    }

    /// Every line the hub has written, up to the first that `last`
    /// accepts, within `within`.
    fn lines_until(&mut self, within: Duration, last: impl Fn(&str) -> bool) -> Vec<String> {
        let mut lines = std::mem::take(&mut self.before);
        if lines.iter().any(|line| last(line)) {
            return lines;
        }
        let deadline = Instant::now() + within;
        loop {
            match self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(line) => {
                    let done = last(&line);
                    lines.push(line);
                    if done {
                        return lines;
                    }
                }
                Err(_) => panic!("no awaited line within {within:?}; the hub wrote {lines:?}"),
            }
        }
    }

    /// Send `signal` (`TERM`, `INT`), and the hub's exit status within
    /// `within`.
    fn stop(&mut self, signal: &str, within: Duration) -> ExitStatus {
        let sent = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.child.id().to_string())
            .status()
            .expect("kill");
        assert!(sent.success(), "kill -{signal}");
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("the hub's state") {
                return status;
            }
            assert!(
                started.elapsed() < within,
                "the hub runs {within:?} after SIG{signal}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn deployment(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let path = dir.path().join("deployment.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&spec()).expect("valid test fixture"),
    )
    .expect("written");
    path
}

/// Docker stops a container with SIGTERM, and the hub is the container's
/// first process, which the kernel gives no default action for a signal it
/// does not handle: a hub that stopped only on SIGINT ran until Docker
/// killed it, 10 s later. It stops as on SIGINT, and says it succeeded.
#[test]
fn the_hub_stops_on_sigterm() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-sigterm-")
        .tempdir()
        .expect("a directory");
    let mut hub = Serving::start(&deployment(&dir));
    let status = hub.stop("TERM", Duration::from_secs(5));
    assert!(
        status.success(),
        "{status:?} (signal {:?})",
        status.signal()
    );
}

/// SIGINT stops it as before: the control that shows the SIGTERM test
/// reads an exit status, not merely a process that ended.
#[test]
fn the_hub_stops_on_sigint() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-sigint-")
        .tempdir()
        .expect("a directory");
    let mut hub = Serving::start(&deployment(&dir));
    let status = hub.stop("INT", Duration::from_secs(5));
    assert!(
        status.success(),
        "{status:?} (signal {:?})",
        status.signal()
    );
}

/// The record that `path` holds: its epoch, or None while there is none.
fn epoch(path: &std::path::Path) -> Option<u64> {
    let bytes = std::fs::read(path).ok()?;
    let entry: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    entry["record"]["epoch"].as_u64()
}

/// Wait up to 10 s for `path` to hold a record that `accept` takes.
fn wait_for_record(path: &std::path::Path, accept: impl Fn(u64) -> bool) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(epoch) = epoch(path).filter(|&epoch| accept(epoch)) {
            return epoch;
        }
        assert!(Instant::now() < deadline, "no such record at {path:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The error the hub logs when another hub of its pool publishes with a
/// later epoch.
const OVERTAKEN: &str = "another hub publishes this pool with a later epoch";

/// The warn event the hub logs, just before [`OVERTAKEN`], when the
/// directory refuses its record.
const REFUSED: &str = "the directory refused the pool's record as not newer";

/// The stderr of a hub run with `rust_log` (none: the variable unset),
/// whose pool another hub has since published with a later epoch, from its
/// start up to the `error` line that says so. A drain makes the hub write
/// its record again, the directory refuses the write with a `warn` event,
/// and the hub logs the `error` event next, in the same task: so once the
/// `error` line is on stderr, the `warn` line is there too, unless the
/// filter dropped it.
fn overtaken(rust_log: Option<&str>) -> Vec<String> {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-log-")
        .tempdir()
        .expect("a directory");
    let deployment = deployment(&dir);
    let pools = dir.path().join("pools");
    let pool = |command: &mut Command| {
        command
            .env("GDL_POOL__NAME", "model-x-01-us-east-1")
            .env("GDL_POOL__CLASS", "model_x")
            .env("GDL_POOL__REGION", "us-east-1")
            .env("GDL_POOL__HUB", "model-x-01-hub:1337")
            .env("GDL_POOL__DIRECTORY", "dir")
            .env("GDL_POOL__DIR", &pools);
    };
    let record = pools.join("model_x").join("model-x-01-us-east-1.json");
    let mut first = Serving::start_with(&deployment, |command| {
        pool(command);
        // The runner's own `RUST_LOG` must not choose the level.
        match rust_log {
            Some(filter) => command.env("RUST_LOG", filter),
            None => command.env_remove("RUST_LOG"),
        };
    });
    let stored = wait_for_record(&record, |_| true);
    let _second = Serving::start_with(&deployment, pool);
    wait_for_record(&record, |epoch| epoch > stored);
    // Held open until the lines are read: the drain is answered only
    // once its record is stored, which this one never is.
    let mut drain = TcpStream::connect((Ipv4Addr::LOCALHOST, first.control)).expect("connect");
    drain
        .write_all(
            b"POST /v1/ext/gdl-pool/drain HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .expect("drain");
    first.lines_until(Duration::from_secs(10), |line| {
        line.contains("ERROR") && line.contains(OVERTAKEN)
    })
}

/// Without `RUST_LOG`, the binary writes `warn` events to stderr, without
/// colour codes.
#[test]
fn the_hub_logs_warnings_by_default() {
    let lines = overtaken(None);
    assert!(
        lines
            .iter()
            .any(|line| line.contains(" WARN ") && line.contains(REFUSED)),
        "{lines:#?}"
    );
    assert!(
        !lines.iter().any(|line| line.contains('\x1b')),
        "{lines:#?}"
    );
}

/// `RUST_LOG` sets the level: at `error`, the `warn` event is not written,
/// and the `error` that follows it is.
#[test]
fn rust_log_sets_the_hubs_level() {
    let lines = overtaken(Some("error"));
    assert!(
        !lines.iter().any(|line| line.contains(" WARN ")),
        "{lines:#?}"
    );
}
