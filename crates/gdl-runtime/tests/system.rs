//! The system listener: the live and health probes on dynamo's paths and
//! port, so that the dynamo operator's probes find a segment process. Live
//! from the start; healthy only while the process serves.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::system::{get, send};
use figment::Jail;
use figment::providers::{Format, Toml};
use gdl_runtime::{
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
            forward_types: vec![TypeInfo::new("Request", true, Default::default())],
            backward_types: vec![TypeInfo::new("Reply", true, Default::default())],
        }],
        properties: Vec::new(),
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
        .serve(common::Idle, |_, _| async move {
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
        .serve(common::Idle, |_, _| async move {
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
        (
            "live_path = \"/metrics\"\n",
            "`/metrics` is the metrics path",
        ),
        (
            "health_path = \"/metrics\"\n",
            "`/metrics` is the metrics path",
        ),
        ("metrics_prefix = \"9gdl\"\n", "not a metric name prefix"),
        ("metrics_prefix = \"gdl-x\"\n", "not a metric name prefix"),
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

/// The labels of the first series named `name` in a scrape, by label name:
/// the registry adds its common labels in a hash map's order, so a line is
/// parsed, not compared.
fn series(scrape: &str, name: &str) -> Option<(std::collections::BTreeMap<String, String>, f64)> {
    let line = scrape
        .lines()
        .find(|line| line.starts_with(&format!("{name}{{")))?;
    let (labels, value) = line[name.len() + 1..].split_once("} ")?;
    let labels = labels
        .split(',')
        .map(|pair| {
            let (key, value) = pair.split_once('=').expect("a label");
            (key.to_string(), value.trim_matches('"').to_string())
        })
        .collect();
    Some((labels, value.parse().expect("a value")))
}

/// `/metrics` answers from the start, empty until the process's registry
/// is served; then every series carries the instance's identity, under the
/// prefix, and `uptime_seconds` counts from the process's start.
#[tokio::test]
async fn metrics_carry_the_identity_of_the_instance() {
    let dir = roster();
    let config = config(dir.path(), "host = \"127.0.0.1\"\nport = 0\n");
    let process = Process::start(&config).await.expect("process");
    let address = process.system_address().expect("a listener");

    let answer = send(address, b"GET /metrics HTTP/1.1\r\nHost: probe\r\n\r\n").await;
    assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
    assert!(
        answer.contains("Content-Type: text/plain; version=0.0.4"),
        "{answer}"
    );
    assert!(
        answer.ends_with("\r\n\r\n"),
        "empty before the registry: {answer}"
    );

    let deployment = deployment();
    let registry = process.registry(&deployment, "only").expect("a registry");
    let counter = gdl::prometheus::IntCounter::new("tokens_total", "Tokens.").expect("counter");
    registry
        .register(Box::new(counter.clone()))
        .expect("registered");
    counter.inc();
    let (code, scrape) = get(address, "/metrics").await;
    assert_eq!((code, scrape.as_str()), (200, ""), "made, not served");
    process.serve_metrics(registry.clone()).expect("served");
    let (code, scrape) = get(address, "/metrics").await;
    assert_eq!(code, 200);
    let hash = gdl_runtime::layout(&deployment.graph, &deployment.placement)
        .expect("layout")
        .placement_hash();
    let worker = format!("{:032x}", process.instance_id().as_u128());
    let (labels, value) = series(&scrape, "dynamo_component_tokens_total").expect(&scrape);
    assert_eq!(value, 1.0);
    assert_eq!(
        labels,
        [
            ("dynamo_component", "only".to_string()),
            ("dynamo_namespace", format!("one-{:08x}", hash >> 32)),
            ("worker_id", worker.clone()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    );
    let (labels, first) = series(&scrape, "dynamo_component_uptime_seconds").expect(&scrape);
    assert_eq!(labels["worker_id"], worker);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (_, scrape) = get(address, "/metrics").await;
    let (_, later) = series(&scrape, "dynamo_component_uptime_seconds").expect(&scrape);
    assert!(later > first, "{first} then {later}");

    let error = process
        .registry(&deployment, "only")
        .expect_err("one registry per process");
    assert!(error.to_string().contains("once"), "{error}");
    let error = process
        .serve_metrics(registry)
        .expect_err("one registry served per process");
    assert!(error.to_string().contains("once"), "{error}");
}

/// A gauge read at scrape time, which counts its closure's runs.
#[derive(gdl::Metrics)]
struct GateMetrics {
    /// 1 for each model.
    #[gdl(labels = ["model"])]
    model_status: gdl::ScrapeGaugeVec,
}

/// A scrape runs a node's closure inside the registry's gather, which
/// holds the registry's lock, and a registration waits for that lock. So
/// no scrape reaches the registry while anything can still register: a
/// scrape before the registry is served is answered empty, and runs no
/// closure.
#[tokio::test]
async fn a_scrape_runs_no_closure_until_the_registry_is_served() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let dir = roster();
    let config = config(dir.path(), "host = \"127.0.0.1\"\nport = 0\n");
    let process = Process::start(&config).await.expect("process");
    let address = process.system_address().expect("a listener");
    let registry = process.registry(&deployment(), "only").expect("a registry");
    let gate = <GateMetrics as gdl::Metrics>::register(&registry, "Gate").expect("registered");
    let runs = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&runs);
    gate.model_status.fill_with(move |gauges| {
        count.fetch_add(1, Ordering::SeqCst);
        gauges.set(&["llama"], 1.0).expect("one label");
    });
    let (code, scrape) = get(address, "/metrics").await;
    assert_eq!(code, 200);
    assert_eq!(scrape, "", "served before the registrations end");
    assert_eq!(runs.load(Ordering::SeqCst), 0, "a scrape ran the closure");

    process.serve_metrics(registry).expect("served");
    let (code, scrape) = get(address, "/metrics").await;
    assert_eq!(code, 200);
    let (labels, value) = series(&scrape, "dynamo_component_model_status").expect(&scrape);
    assert_eq!((labels["model"].as_str(), value), ("llama", 1.0));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

/// `metrics_prefix` names the prefix; empty, there is none.
#[tokio::test]
async fn the_metrics_prefix_is_a_setting() {
    let dir = roster();
    for (prefix, name) in [
        ("\"gdl\"", "gdl_uptime_seconds"),
        ("\"\"", "uptime_seconds"),
    ] {
        let config = config(
            dir.path(),
            &format!("host = \"127.0.0.1\"\nport = 0\nmetrics_prefix = {prefix}\n"),
        );
        let process = Process::start(&config).await.expect("process");
        let registry = process.registry(&deployment(), "only").expect("a registry");
        process.serve_metrics(registry).expect("served");
        let (_, scrape) = get(process.system_address().expect("a listener"), "/metrics").await;
        assert!(
            series(&scrape, name).is_some(),
            "{name} missing from:\n{scrape}"
        );
    }
}

/// A process is one instance with one velo node: a second launch on it is
/// refused, where it would have shared the node and replaced the first
/// launch's handlers without a word.
#[tokio::test]
async fn a_process_launches_once() {
    let dir = roster();
    let config = config(dir.path(), "");
    let process = Process::start(&config).await.expect("process");
    let _first = config
        .launch(&process, "only", &["only"], &deployment(), async { Ok(()) })
        .await
        .expect("the first launch");
    let Err(error) = config
        .launch(&process, "only", &["only"], &deployment(), async { Ok(()) })
        .await
    else {
        panic!("a second launch on one process")
    };
    assert!(error.to_string().contains("launches once"), "{error}");
}

/// The process's own figures (`process_*`, and `process_cgroup_*` where
/// the process has a cgroup v2) are served beside the registry's, under
/// their own names, unprefixed, with the instance's identity; no family
/// appears twice in one scrape.
#[tokio::test]
async fn the_process_figures_are_served_unprefixed() {
    let dir = roster();
    let config = config(dir.path(), "host = \"127.0.0.1\"\nport = 0\n");
    let process = Process::start(&config).await.expect("process");
    let address = process.system_address().expect("a listener");
    let registry = process.registry(&deployment(), "only").expect("a registry");
    let (_, scrape) = get(address, "/metrics").await;
    assert_eq!(scrape, "", "made, not served");
    process.serve_metrics(registry).expect("served");
    let (code, scrape) = get(address, "/metrics").await;
    assert_eq!(code, 200);

    let mut families: Vec<&str> = scrape
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .collect();
    let count = families.len();
    families.sort_unstable();
    families.dedup();
    assert_eq!(families.len(), count, "a family twice in:\n{scrape}");
    assert!(
        !scrape.contains("dynamo_component_process_"),
        "prefixed:\n{scrape}"
    );
    let identity = |name: &str| series(&scrape, name).map(|(labels, _)| labels);
    if cfg!(target_os = "linux") {
        let (labels, _) = series(&scrape, "process_cpu_seconds_total").expect(&scrape);
        let (served, _) = series(&scrape, "dynamo_component_uptime_seconds").expect(&scrape);
        assert_eq!(labels, served, "the identity labels");
        assert!(
            identity("process_resident_memory_bytes").is_some(),
            "{scrape}"
        );
    }

    // The cgroup figures, where this host has them for this process.
    let Some(own) = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("0::").map(str::to_string))
        })
    else {
        return;
    };
    let cgroup = std::path::Path::new("/sys/fs/cgroup").join(own.trim_start_matches('/'));
    let stat = std::fs::read_to_string(cgroup.join("cpu.stat")).unwrap_or_default();
    if stat.lines().any(|line| line.starts_with("usage_usec ")) {
        assert!(
            identity("process_cgroup_cpu_usage_seconds_total").is_some(),
            "{scrape}"
        );
    }
    if stat.lines().any(|line| line.starts_with("nr_periods ")) {
        for name in [
            "process_cgroup_cpu_periods_total",
            "process_cgroup_cpu_throttled_periods_total",
            "process_cgroup_cpu_throttled_seconds_total",
        ] {
            assert!(identity(name).is_some(), "{name} missing from:\n{scrape}");
        }
    }
    if cgroup.join("memory.current").exists() {
        assert!(
            identity("process_cgroup_memory_bytes").is_some(),
            "{scrape}"
        );
    }
}
