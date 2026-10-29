//! The stock hub binary on the DynamoDB pool directory, against DynamoDB
//! Local: the tables step makes the tables and keeps them on a second run,
//! and a parent pool's hub finds two child pools in two regions through
//! the tables: its relay lists each, with its region.
//!
//! Outside the default suite: the target needs the `dynamodb-local`
//! feature and a DynamoDB Local at `GDL_DYNAMODB_LOCAL`, started with
//! `-sharedDb`, so that clients of two regions see one database, as the
//! regions of a global table do. Run it with `scripts/dynamodb-local.sh`.
//! Each hub runs as its own process on loopback; every wait is bounded and
//! for something observed.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use gdl_model::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use gdl_watch::{FollowEvent, PoolWatcher};

/// How long the parent may take to list both children: a child's first
/// record, and one or two of the parent's queries a second apart.
const LISTED: Duration = Duration::from_secs(15);

fn endpoint() -> String {
    std::env::var("GDL_DYNAMODB_LOCAL").unwrap_or_else(|_| {
        panic!(
            "GDL_DYNAMODB_LOCAL names no DynamoDB Local: run scripts/dynamodb-local.sh, which starts one"
        )
    })
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

/// The `dynamodb` backend's variables for tables named for this run, and
/// `region`'s copy of them.
fn directory(run: &str, region: &str) -> Vec<(String, String)> {
    [
        ("GDL_POOL__DIRECTORY", "dynamodb".to_string()),
        ("GDL_POOL__RECORDS_TABLE", format!("gdl-hub-pools-{run}")),
        ("GDL_POOL__LISTS_TABLE", format!("gdl-hub-lists-{run}")),
        ("GDL_POOL__AWS_REGION", region.to_string()),
        ("GDL_POOL__ENDPOINT", endpoint()),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value))
    .collect()
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

/// `gdl-hub --tables` with `env`: whether it succeeded, and its stderr.
fn tables(env: &[(String, String)]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_gdl-hub"))
        .arg("--tables")
        .envs(env.iter().map(|(name, value)| (name, value)))
        .output()
        .expect("the binary runs");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// The tables step makes the absent tables, then keeps them; a parent
/// pool's stock hub in one region finds two child pools, one in its region
/// and one in another, through the tables, and lists each to its choosers
/// with the region its record names. The children have no instances, so
/// they are not ready and the parent does not follow them: what is proved
/// is that each record crossed the tables, between clients of two regions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_finds_children_in_two_regions_through_dynamodb() {
    let run = std::process::id().to_string();
    let (made, said) = tables(&directory(&run, "us-east-1"));
    assert!(made, "{said}");
    assert!(
        said.contains(&format!("made table gdl-hub-pools-{run}"))
            && said.contains(&format!("made table gdl-hub-lists-{run}")),
        "{said}"
    );
    // A second region's run of the step finds the same tables.
    let (kept, said) = tables(&directory(&run, "eu-west-1"));
    assert!(kept, "{said}");
    assert!(said.contains("kept table"), "{said}");

    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-dynamodb-")
        .tempdir()
        .expect("a directory");
    let deployment = dir.path().join("deployment.json");
    std::fs::write(
        &deployment,
        serde_json::to_vec(&spec()).expect("valid test fixture"),
    )
    .expect("written");

    let pool = |name: &str, class: &str, region: &str, port: u16, home: Option<&str>| {
        let mut env = directory(&run, region);
        env.extend(
            [
                ("GDL_POOL__NAME", name),
                ("GDL_POOL__CLASS", class),
                ("GDL_POOL__REGION", region),
                ("GDL_POOL__HUB", &format!("127.0.0.1:{port}")),
            ]
            .map(|(name, value)| (name.to_string(), value.to_string())),
        );
        if let Some(home) = home {
            env.push(("GDL_POOL__HOME".into(), home.into()));
        }
        env
    };
    let parent_port = free_port();
    let _parent = serve(
        &deployment,
        &pool("global-us", "global", "us-east-1", parent_port, None),
        parent_port,
    );
    let mut children = Vec::new();
    for (name, region) in [("model-x-use1", "us-east-1"), ("model-x-euw1", "eu-west-1")] {
        let port = free_port();
        children.push(serve(
            &deployment,
            &pool(name, "model_x", region, port, Some("global-us")),
            port,
        ));
    }

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
    while listed.len() < 2 {
        let item = tokio::time::timeout_at(deadline, relay.next())
            .await
            .unwrap_or_else(|_| panic!("the parent lists both children; it lists {listed:?}"))
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
        BTreeMap::from([
            ("model-x-euw1".to_string(), "eu-west-1".to_string()),
            ("model-x-use1".to_string(), "us-east-1".to_string()),
        ])
    );
}

/// A table that exists with other keys stops the step, which names it.
#[tokio::test]
async fn the_tables_step_refuses_a_table_with_other_keys() {
    let run = format!("{}-keys", std::process::id());
    let mut env = directory(&run, "us-east-1");
    // The records table under the lists table's name: other keys.
    let records = env
        .iter()
        .find(|(name, _)| name == "GDL_POOL__RECORDS_TABLE")
        .map(|(_, value)| value.clone())
        .expect("a records table");
    let (made, said) = tables(&env);
    assert!(made, "{said}");
    for (name, value) in &mut env {
        if name == "GDL_POOL__LISTS_TABLE" {
            *value = records.clone();
        }
    }
    let (refused, said) = tables(&env);
    assert!(!refused, "{said}");
    assert!(
        said.contains(&format!(
            "table {records} has the keys class (HASH, S), pool (RANGE, S), and the directory needs owner (HASH, S), list (RANGE, S)"
        )),
        "{said}"
    );
}
