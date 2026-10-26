//! The hub binary refuses a cell's settings it cannot run before it binds
//! a port, and says what to set; and a hub that serves stops on either
//! signal that stops a container. A hub that starts here takes ports the
//! OS chooses, through `GDL_HUB__PUBLIC_PORT` and `GDL_HUB__CONTROL_PORT`.

use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use gdl_model::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};

fn spec() -> DeploymentSpec {
    let graph = Manifest {
        name: "cells".into(),
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
    // `relay` is the default.
    for (directory, expected) in [
        (
            None,
            "[cell] directory = \"relay\", the default, is not built yet",
        ),
        (
            Some("memory"),
            "[cell] directory = \"memory\" is a directory inside one process",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_gdl-hub"));
        command
            .arg(&deployment)
            .env("GDL_CELL__NAME", "model-x-01-us-east-1")
            .env("GDL_CELL__CLASS", "model_x")
            .env("GDL_CELL__REGION", "us-east-1")
            .env("GDL_CELL__HUB", "model-x-01-hub:1337");
        if let Some(directory) = directory {
            command.env("GDL_CELL__DIRECTORY", directory);
        }
        let output = command.output().expect("the binary runs");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "started with {directory:?}");
        assert!(stderr.contains(expected), "{stderr}");
        assert!(stderr.contains("GDL_CELL__DIRECTORY=dir"), "{stderr}");
    }
}

/// A hub process, killed when the test ends however it ends.
struct Serving(Child);

impl Drop for Serving {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Serving {
    /// A hub for `deployment` on ports the OS chooses, once it says that it
    /// serves.
    fn start(deployment: &std::path::Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_gdl-hub"))
            .arg(deployment)
            .env("GDL_HUB__PUBLIC_PORT", "0")
            .env("GDL_HUB__CONTROL_PORT", "0")
            .stderr(Stdio::piped())
            .spawn()
            .expect("the binary runs");
        let stderr = child.stderr.take().expect("stderr");
        let serving = Self(child);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if line.starts_with("gdl-hub: serving") {
                    let _ = tx.send(());
                }
            }
        });
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the hub says that it serves");
        serving
    }

    /// Send `signal` (`TERM`, `INT`), and the hub's exit status within
    /// `within`.
    fn stop(&mut self, signal: &str, within: Duration) -> ExitStatus {
        let sent = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.0.id().to_string())
            .status()
            .expect("kill");
        assert!(sent.success(), "kill -{signal}");
        let started = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait().expect("the hub's state") {
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
