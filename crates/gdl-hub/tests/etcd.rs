//! The stock hub binary, built with its `etcd` feature, on the etcd pool
//! directory: a parent pool's hub finds a child pool through etcd, and its
//! relay lists the child; and `--resolve` reads a running pool's hub from
//! etcd.
//!
//! Outside the default suite: the target needs the `etcd-local` feature
//! and the etcd binary `GDL_ETCD` names. Run it with `scripts/etcd.sh`,
//! which fetches etcd. The test starts its own etcd on OS-assigned ports,
//! over a temporary data directory, and each hub as its own process on
//! loopback; every wait is bounded and for something observed.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};

use std::sync::mpsc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use gdl_model::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use gdl_watch::{FollowEvent, PoolWatcher};

/// How long the parent may take to list the child: the child's first
/// record, pushed by etcd's watch.
const LISTED: Duration = Duration::from_secs(15);

/// An etcd process over a temporary data directory, killed when the test
/// ends however it ends.
struct Etcd {
    child: Child,
    url: String,
    _data: tempfile::TempDir,
}

impl Drop for Etcd {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A new etcd, once it answers its health check.
async fn etcd() -> Etcd {
    let binary = std::env::var("GDL_ETCD")
        .expect("GDL_ETCD names the etcd binary: run scripts/etcd.sh, which fetches it");
    let data = tempfile::Builder::new()
        .prefix("gdl-hub-etcd-")
        .tempdir()
        .expect("a data directory");
    let (client, peer) = (free_port(), free_port());
    let url = format!("http://127.0.0.1:{client}");
    let peer_url = format!("http://127.0.0.1:{peer}");
    let child = Command::new(binary)
        .arg("--name=default")
        .arg(format!("--data-dir={}", data.path().display()))
        .arg(format!("--listen-client-urls={url}"))
        .arg(format!("--advertise-client-urls={url}"))
        .arg(format!("--listen-peer-urls={peer_url}"))
        .arg(format!("--initial-advertise-peer-urls={peer_url}"))
        .arg(format!("--initial-cluster=default={peer_url}"))
        .arg("--log-level=error")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("etcd starts");
    let etcd = Etcd {
        child,
        url,
        _data: data,
    };
    let health = format!("{}/health", etcd.url);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(answer) = reqwest::get(&health).await
            && answer.status().is_success()
        {
            return etcd;
        }
        assert!(Instant::now() < deadline, "etcd did not answer {health}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `global` runs `a` and sends to `model_x`, which runs `d`.
fn spec() -> DeploymentSpec {
    let graph = Manifest {
        name: "pools".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
                calls: Vec::new(),
                unary: None,
            },
            NodeInfo {
                name: "D".into(),
                role: Role::Sink,
                binding: Some("d".into()),
                calls: Vec::new(),
                unary: None,
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
        properties: Vec::new(),
    };
    let placement = Placement::new()
        .segment("a", ["S"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["d"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// A port that no listener holds as this returns, for a hub whose record
/// must name its address before it binds it.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("a port")
        .local_addr()
        .expect("its address")
        .port()
}

/// A hub process, killed when the test ends however it ends.
struct Serving(Child);

impl Drop for Serving {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The hub binary for `deployment` as pool `pool`, with `env`, on public
/// port `port`, once it says that it serves.
fn serve(deployment: &std::path::Path, env: &[(String, String)], port: u16) -> Serving {
    let mut child = Command::new(env!("CARGO_BIN_EXE_gdl-hub"))
        .arg(deployment)
        .envs(env.iter().map(|(name, value)| (name, value)))
        .env("GDL_HUB__PUBLIC_PORT", port.to_string())
        .env("GDL_HUB__CONTROL_PORT", "0")
        .stderr(Stdio::piped())
        .spawn()
        .expect("the binary runs");
    let stderr = child.stderr.take().expect("stderr");
    let serving = Serving(child);
    let (tx, lines) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                return;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut before = Vec::new();
    loop {
        let line = lines
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|_| panic!("the hub says that it serves; it wrote {before:?}"));
        if line.starts_with("gdl-hub: serving") {
            // Keep draining, so that the hub never blocks on a full pipe.
            std::thread::spawn(move || while lines.recv().is_ok() {});
            return serving;
        }
        before.push(line);
    }
}

/// A parent pool's stock hub finds a child pool through etcd, and lists
/// it to its choosers with the region its record names. The child has no
/// instances, so it is not ready and the parent does not follow it: what
/// is proved is that the record crossed etcd, from the child's hub to the
/// parent's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_finds_a_child_through_etcd() {
    let etcd = etcd().await;
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-etcd-")
        .tempdir()
        .expect("a directory");
    let deployment = dir.path().join("deployment.json");
    std::fs::write(
        &deployment,
        serde_json::to_vec(&spec()).expect("valid test fixture"),
    )
    .expect("written");
    let pool = |name: &str, class: &str, port: u16, home: Option<&str>| {
        let mut env: Vec<(String, String)> = [
            ("GDL_POOL__DIRECTORY", "etcd".to_string()),
            ("GDL_POOL__ENDPOINTS", format!("[\"{}\"]", etcd.url)),
            ("GDL_POOL__PREFIX", "/gdl-hub/pools".to_string()),
            ("GDL_POOL__NAME", name.to_string()),
            ("GDL_POOL__CLASS", class.to_string()),
            ("GDL_POOL__REGION", "us-east-1".to_string()),
            ("GDL_POOL__HUB", format!("127.0.0.1:{port}")),
        ]
        .map(|(name, value)| (name.to_string(), value))
        .into();
        if let Some(home) = home {
            env.push(("GDL_POOL__HOME".into(), home.into()));
        }
        env
    };
    let parent_port = free_port();
    let _parent = serve(
        &deployment,
        &pool("global-us", "global", parent_port, None),
        parent_port,
    );
    let child_port = free_port();
    let _child = serve(
        &deployment,
        &pool("model-x-use1", "model_x", child_port, Some("global-us")),
        child_port,
    );

    let watcher = PoolWatcher::new().expect("a client");
    let mut relay = tokio::time::timeout(
        Duration::from_secs(5),
        watcher.follows(&format!("127.0.0.1:{parent_port}"), None),
    )
    .await
    .expect("the parent answers in time")
    .expect("the parent serves its relay");
    let mut listed = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + LISTED;
    while listed.is_empty() {
        let item = tokio::time::timeout_at(deadline, relay.next())
            .await
            .unwrap_or_else(|_| panic!("the parent lists the child; it lists {listed:?}"))
            .expect("the relay goes on")
            .expect("the relay does not fail");
        let (class, pool, listing) = match item.event() {
            Some(FollowEvent::Listed {
                class,
                pool,
                listing,
                ..
            })
            | Some(FollowEvent::Follow {
                class,
                pool,
                listing,
                ..
            }) => (class, pool, listing),
            _ => continue,
        };
        assert_eq!(class, "model_x");
        if let Some(listing) = listing {
            listed.insert(pool, listing.region);
        }
    }
    assert_eq!(
        listed,
        BTreeMap::from([("model-x-use1".to_string(), "us-east-1".to_string())])
    );
}

/// `--resolve` reads a running pool's hub from etcd, and writes it as a
/// segment's `[hub]`; a pool the directory does not hold writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_resolver_reads_a_pools_hub_from_etcd() {
    let etcd = etcd().await;
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-etcd-")
        .tempdir()
        .expect("a directory");
    let deployment = dir.path().join("deployment.json");
    std::fs::write(
        &deployment,
        serde_json::to_vec(&spec()).expect("valid test fixture"),
    )
    .expect("written");
    let directory: Vec<(String, String)> = vec![
        ("GDL_POOL__DIRECTORY".into(), "etcd".into()),
        ("GDL_POOL__ENDPOINTS".into(), format!("[\"{}\"]", etcd.url)),
        ("GDL_POOL__PREFIX".into(), "/gdl-hub/resolve".into()),
    ];
    let port = free_port();
    let mut pool = directory.clone();
    pool.extend(
        [
            ("GDL_POOL__NAME", "model-x-use1"),
            ("GDL_POOL__CLASS", "model_x"),
            ("GDL_POOL__REGION", "us-east-1"),
            ("GDL_POOL__HOME", "global-us"),
            ("GDL_POOL__HUB", &format!("127.0.0.1:{port}")),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    let _hub = serve(&deployment, &pool, port);

    let resolve = |target: &str, file: &std::path::Path| {
        Command::new(env!("CARGO_BIN_EXE_gdl-hub"))
            .args(["--resolve", target, "--write"])
            .arg(file)
            .envs(directory.iter().map(|(name, value)| (name, value)))
            .stdin(Stdio::null())
            .output()
            .expect("the binary runs")
    };
    let file = dir.path().join("gdl-hub.toml");
    let output = resolve("model_x/model-x-use1", &file);
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{said}");
    assert_eq!(
        std::fs::read_to_string(&file).expect("the file"),
        format!("[hub]\nhost = \"127.0.0.1\"\npublic_port = {port}\n")
    );

    let missing = dir.path().join("missing.toml");
    let output = resolve("model_x/model-x-nowhere", &missing);
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{said}");
    assert!(said.contains("not in the directory"), "{said}");
    assert!(!missing.exists());
}
