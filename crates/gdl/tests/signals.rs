//! A segment process stops on SIGINT and SIGTERM alike: a terminal sends
//! the first, a container runtime the second, and a frontend must get to
//! drain under both. A signal reaches the whole process, so this binary
//! runs its cases one after another in one test.
#![cfg(unix)]

use std::time::Duration;

use figment::providers::{Format, Toml};
use gdl::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use gdl::{Hosted, Launch, ProcessConfig};
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

/// A deployment of one segment with one instance meets at once.
async fn launched(name: &str) -> (Launch, tempfile::TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("gdl-signals-{name}-"))
        .tempdir()
        .expect("roster directory");
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n",
        dir.path().display()
    );
    let config: ProcessConfig<Nothing> =
        ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings)))
            .expect("settings");
    let launch = config
        .launch("only", &["only"], &deployment(), async { Ok(()) })
        .await
        .expect("met itself");
    (launch, dir)
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
        let (launch, _dir) = launched(signal.trim_start_matches('-')).await;
        let served = tokio::time::timeout(
            Duration::from_secs(5),
            launch.serve(|shutdown| async move {
                // The handlers exist before the body runs, so the signal
                // sent here cannot end the process the default way.
                send(signal);
                shutdown.cancelled().await;
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
    let (launch, _dir) = launched("second").await;
    let served = tokio::time::timeout(
        Duration::from_secs(5),
        launch.serve(|shutdown| async move {
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
}
