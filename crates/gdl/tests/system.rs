//! The system listener: the live and health probes on dynamo's paths and
//! port, so that the dynamo operator's probes find a segment process. Live
//! from the start; healthy only while the process serves.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::system::{get, send};
use figment::Jail;
use figment::providers::{Format, Toml};
use gdl::{
    DeploymentSpec, HopInfo, Hosted, Manifest, NodeInfo, Placement, Process, ProcessConfig, Role,
    TypeInfo,
};
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

fn config(roster: &std::path::Path, system: &str) -> ProcessConfig<Nothing> {
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[system]\n{system}",
        roster.display()
    );
    ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings))).expect("settings")
}

fn roster() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("gdl-system-")
        .tempdir()
        .expect("roster directory")
}

fn phase(body: &str) -> &str {
    let at = body.find("\"phase\":\"").expect("a phase") + "\"phase\":\"".len();
    &body[at..at + body[at..].find('"').expect("its end")]
}

/// Live answers 200 throughout; health answers 503 while the process
/// starts, 200 once it serves, and 503 again once its serving ends.
#[tokio::test]
async fn the_probes_follow_the_process() {
    let dir = roster();
    let config = config(dir.path(), "host = \"127.0.0.1\"\nport = 0\n");
    let process = Process::start(&config).await.expect("process");
    let address = process.system_address().expect("a listener");

    let (code, body) = get(address, "/live").await;
    assert_eq!((code, phase(&body)), (200, "starting"), "{body}");
    let (code, body) = get(address, "/health").await;
    assert_eq!((code, phase(&body)), (503, "starting"), "{body}");
    assert!(body.contains("\"status\":\"notready\""), "{body}");

    let launch = config
        .launch(&process, "only", &["only"], &deployment(), async { Ok(()) })
        .await
        .expect("met itself");
    let (code, body) = get(address, "/health").await;
    assert_eq!(
        (code, phase(&body)),
        (503, "starting"),
        "at the barrier: {body}"
    );
    launch
        .serve((), |_, _| async move {
            let (code, body) = get(address, "/health").await;
            assert_eq!((code, phase(&body)), (200, "ready"), "{body}");
            assert!(body.contains("\"status\":\"ready\""), "{body}");
            Ok(())
        })
        .await
        .expect("served");
    let (code, body) = get(address, "/health").await;
    assert_eq!((code, phase(&body)), (503, "draining"), "{body}");
    let (code, _) = get(address, "/live").await;
    assert_eq!(code, 200);
}

/// The listener answers what it serves and nothing else, and a client that
/// never finishes its request holds up no other.
#[tokio::test]
async fn the_listener_refuses_what_it_does_not_serve() {
    let dir = roster();
    let config = config(dir.path(), "host = \"127.0.0.1\"\nport = 0\n");
    let process = Process::start(&config).await.expect("process");
    let address = process.system_address().expect("a listener");

    assert_eq!(get(address, "/metrics/nope").await.0, 404);
    assert_eq!(get(address, "/live?verbose=1").await.0, 200);
    let posted = send(address, b"POST /live HTTP/1.1\r\nHost: probe\r\n\r\n").await;
    assert!(posted.starts_with("HTTP/1.1 405 "), "{posted}");
    let mut huge = b"GET /live HTTP/1.1\r\nX: ".to_vec();
    huge.extend(std::iter::repeat_n(b'x', 16 << 10));
    huge.extend_from_slice(b"\r\n\r\n");
    let refused = send(address, &huge).await;
    assert!(refused.starts_with("HTTP/1.1 431 "), "{refused}");

    // An idle connection, open and silent, while another is answered.
    let _idle = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    let answered = tokio::time::timeout(Duration::from_secs(2), get(address, "/live")).await;
    assert_eq!(answered.expect("not held up by the idle one").0, 200);
}

/// `healthcheck` asks the health path of the listener the settings name:
/// loopback for an unspecified host, and never a port chosen at binding.
#[tokio::test]
async fn the_health_check_asks_the_listener_the_settings_name() {
    let dir = roster();
    let config = config(dir.path(), "host = \"127.0.0.1\"\nport = 0\n");
    let process = Process::start(&config).await.expect("process");
    let address = process.system_address().expect("a listener");
    let named = |host: &str, port: u16| {
        self::config(dir.path(), &format!("host = \"{host}\"\nport = {port}\n"))
    };

    let error = named("127.0.0.1", 0)
        .healthcheck()
        .await
        .expect_err("port 0");
    assert!(error.to_string().contains("port is 0"), "{error}");
    let unset = self::config(dir.path(), "");
    let error = unset.healthcheck().await.expect_err("no listener");
    assert!(error.to_string().contains("no system listener"), "{error}");

    // Starting: unhealthy, through loopback for the unspecified host.
    let error = named("0.0.0.0", address.port())
        .healthcheck()
        .await
        .expect_err("still starting");
    assert!(error.to_string().contains("503"), "{error}");

    let launch = config
        .launch(&process, "only", &["only"], &deployment(), async { Ok(()) })
        .await
        .expect("met itself");
    let checked = named("0.0.0.0", address.port());
    launch
        .serve((), |_, _| async move {
            checked.healthcheck().await.expect("healthy while serving");
            Ok(())
        })
        .await
        .expect("served");
}

/// dynamo's variables set the listener, as the dynamo operator sets them,
/// and `GDL_` variables override them.
#[test]
fn dynamo_variables_set_the_listener() {
    Jail::expect_with(|jail| {
        jail.create_file(
            "config.toml",
            "listen = \"127.0.0.1:0\"\nroster = \"roster\"\n",
        )?;
        jail.set_env("DYN_SYSTEM_HOST", "127.0.0.1");
        jail.set_env("DYN_SYSTEM_PORT", "-1");
        jail.set_env("DYN_SYSTEM_HEALTH_PATH", "/ready");
        jail.set_env("GDL_SYSTEM__PORT", "0");
        let config: ProcessConfig<Nothing> =
            ProcessConfig::load(["config.toml"]).map_err(|e| e.to_string())?;
        let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        runtime.block_on(async {
            let process = Process::start(&config).await.expect("process");
            let address: SocketAddr = process.system_address().expect("a listener");
            assert!(address.ip().is_loopback(), "{address}");
            assert_eq!(get(address, "/ready").await.0, 503);
            assert_eq!(get(address, "/health").await.0, 404);
        });
        Ok(())
    });
}

/// Paths that cannot be told apart, or are not paths, are refused.
#[test]
fn the_system_paths_are_checked() {
    let dir = roster();
    for (system, wrong) in [
        (
            "live_path = \"/same\"\nhealth_path = \"/same\"\n",
            "same path",
        ),
        ("live_path = \"live\"\n", "not a path"),
        ("port = 70000\n", "not a port"),
    ] {
        let settings = format!(
            "listen = \"127.0.0.1:0\"\nroster = {:?}\n[system]\n{system}",
            dir.path().display()
        );
        let Err(error) =
            ProcessConfig::<Nothing>::from_figment(figment::Figment::from(Toml::string(&settings)))
        else {
            panic!("{system} was accepted");
        };
        assert!(error.to_string().contains(wrong), "{system}: {error}");
    }
}
