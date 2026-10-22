//! A segment process stops on SIGINT and SIGTERM alike: a terminal sends
//! the first, a container runtime the second, and a frontend must get to
//! drain under both. A signal reaches the whole process, so this binary
//! runs its cases one after another in one test.
#![cfg(unix)]

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::system::get;

use figment::providers::{Format, Toml};
use gdl_runtime::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use gdl_runtime::{Hosted, Launch, ProcessConfig};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NAMES: &'static [&'static str] = &[];
}

/// A deployment of one segment, `only`, holding a source and a sink.
fn deployment() -> DeploymentSpec {
    let graph = Manifest {
        name: "one".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "Source".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "Echo".into(),
                role: Role::Sink,
                binding: Some("echo".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "Source".into(),
            inner: "Echo".into(),
            forward: vec![],
            backward: vec![],
            forward_types: vec![TypeInfo::new("Request", true)],
            backward_types: vec![TypeInfo::new("Reply", true)],
        }],
    };
    DeploymentSpec::new(graph, Placement::new().segment("only", ["Source", "Echo"]))
        .expect("valid test fixture")
}

/// The settings of one instance of `only` meeting in `dir`, with a system
/// listener on an ephemeral port.
fn config(dir: &std::path::Path) -> ProcessConfig<Nothing> {
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[system]\nhost = \"127.0.0.1\"\nport = 0\n",
        dir.display()
    );
    ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings))).expect("settings")
}

/// A deployment of one segment with one instance meets at once. Returns
/// the system listener's address with it.
async fn launched(name: &str) -> (Launch, SocketAddr, tempfile::TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("gdl-signals-{name}-"))
        .tempdir()
        .expect("roster directory");
    let config = config(dir.path());
    let process = gdl_runtime::Process::start(&config).await.expect("process");
    let address = process.system_address().expect("a listener");
    let launch = config
        .launch(&process, "only", &["only"], &deployment(), async { Ok(()) })
        .await
        .expect("met itself");
    (launch, address, dir)
}

fn send(signal: &str) {
    let status = std::process::Command::new("kill")
        .args([signal, &std::process::id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
}

#[tokio::test]
async fn signals_cancel_the_serving_context_and_let_the_body_finish() {
    for signal in ["-INT", "-TERM"] {
        let (launch, address, _dir) = launched(signal.trim_start_matches('-')).await;
        let served = tokio::time::timeout(
            Duration::from_secs(5),
            launch.serve(common::Idle, |_, shutdown| async move {
                // The handlers exist before the body runs, so the signal
                // sent here cannot end the process the default way.
                send(signal);
                shutdown.cancelled().await;
                // Draining: no longer healthy, still live and answering.
                let (code, body) = get(address, "/health").await;
                assert_eq!(code, 503, "{body}");
                assert!(body.contains("\"phase\":\"draining\""), "{body}");
                assert_eq!(get(address, "/live").await.0, 200);
                // What runs after the cancel is the body's own: a drain,
                // here a delay that the result must wait for.
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(())
            }),
        )
        .await
        .expect("stopped within the timeout");
        served.expect("the body's own result");
    }

    // A body that never finishes its drain is ended by a second signal.
    let (launch, _, _dir) = launched("second").await;
    let served = tokio::time::timeout(
        Duration::from_secs(5),
        launch.serve(common::Idle, |_, shutdown| async move {
            send("-TERM");
            shutdown.cancelled().await;
            send("-TERM");
            std::future::pending::<gdl::Result<()>>().await
        }),
    )
    .await
    .expect("stopped within the timeout");
    let Err(error) = served else {
        panic!("a hung drain ended on its own");
    };
    assert!(error.to_string().contains("second signal"), "{error}");

    // A stop while the nodes load ends the launch, which would otherwise
    // wait for them.
    let dir = tempfile::Builder::new()
        .prefix("gdl-signals-loading-")
        .tempdir()
        .expect("roster directory");
    let config = config(dir.path());
    let process = gdl_runtime::Process::start(&config).await.expect("process");
    let launched = tokio::time::timeout(
        Duration::from_secs(5),
        config.launch(&process, "only", &["only"], &deployment(), async {
            send("-TERM");
            std::future::pending::<gdl::Result<()>>().await
        }),
    )
    .await
    .expect("ended within the timeout");
    let Err(error) = launched else {
        panic!("a launch went on after a stop");
    };
    assert_eq!(error.kind(), gdl::ErrorKind::Cancelled, "{error}");
}
