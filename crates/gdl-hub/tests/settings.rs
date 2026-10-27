//! The hub binary's settings: a cell's settings from `GDL_CELL__*`, and
//! the hub they make. Every hub here runs in process on OS-assigned ports.

use std::collections::BTreeMap;
use std::time::Duration;

use figment::Jail;
use futures::StreamExt;
use gdl_cells::{CellDirectory, CellEvent, CellState, DirDirectory};
use gdl_hub::{CellTiming, cell_from_env, cell_from_vars, hub_builder};
use gdl_model::{
    CellSettings, DeploymentSpec, DirectoryBackend, HopInfo, HubTiming, Manifest, NodeInfo,
    Placement, Role, TypeInfo, WatchCaps,
};
use velo_hub::{Hub, HubConfig};

/// `S => D` in two classes: `global` runs `a`, and `model_x` runs `d`.
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

fn config() -> HubConfig {
    let mut config = HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    config
}

/// The variables of a model cell's hub, which publishes to `dir`.
fn model_cell(jail: &mut Jail, dir: &str) {
    jail.set_env("GDL_CELL__NAME", "model-x-01-us-east-1");
    jail.set_env("GDL_CELL__CLASS", "model_x");
    jail.set_env("GDL_CELL__REGION", "us-east-1");
    jail.set_env("GDL_CELL__HUB", "model-x-01-hub:1337");
    jail.set_env("GDL_CELL__DIRECTORY", "dir");
    jail.set_env("GDL_CELL__DIR", dir);
}

#[test]
fn a_hub_without_cell_variables_has_no_cell() {
    Jail::expect_with(|jail| {
        // The build scripts' variables are not a cell's.
        jail.set_env("GDL_PROFILE", "release");
        jail.set_env("GDL_RUNTIME__ADMISSION_LIMIT", "4");
        assert_eq!(cell_from_env(&spec()).map_err(|e| e.to_string())?, None);
        Ok(())
    });
}

/// Keys are matched without regard to case, and `__` nests them.
#[test]
fn a_cells_settings_are_read_from_the_environment() {
    Jail::expect_with(|jail| {
        model_cell(jail, "/srv/cells");
        jail.set_env("GDL_CELL__READY__D", "2");
        jail.set_env("GDL_CELL__SUMMARY_MS", "2500");
        assert_eq!(
            cell_from_env(&spec()).map_err(|e| e.to_string())?,
            Some(CellSettings {
                models: Vec::new(),
                name: "model-x-01-us-east-1".into(),
                layout: None,
                class: "model_x".into(),
                region: "us-east-1".into(),
                home: None,
                directory: DirectoryBackend::Dir {
                    path: "/srv/cells".into()
                },
                ready: BTreeMap::from([("d".into(), 2)]),
                requires: BTreeMap::new(),
                hub: Some("model-x-01-hub:1337".into()),
                summary: Duration::from_millis(2500),
                timing: Default::default(),
            })
        );
        Ok(())
    });
}

/// The hub's pace and its watch caps are `GDL_CELL__*` variables too, and
/// the timing the hub runs with is theirs, each one left out at its
/// default.
#[test]
fn a_cells_hub_timing_is_read_from_the_environment() {
    Jail::expect_with(|jail| {
        model_cell(jail, "/srv/cells");
        jail.set_env("GDL_CELL__HOLD_MS", "500");
        jail.set_env("GDL_CELL__HEARTBEAT_MS", "2000");
        jail.set_env("GDL_CELL__LEASE_MS", "9000");
        jail.set_env("GDL_CELL__WATCH_BUFFER", "64");
        jail.set_env("GDL_CELL__LOST_CHOOSER_MS", "4000");
        jail.set_env("GDL_CELL__WATCH_CAPS__CELL", "3");
        jail.set_env("GDL_CELL__WATCH_CAPS__RELAY", "4");
        jail.set_env("GDL_CELL__WATCH_CAPS__FOLLOW", "5");
        jail.set_env("GDL_CELL__KEYED_CAPACITY", "10");
        let cell = cell_from_env(&spec())
            .map_err(|e| e.to_string())?
            .ok_or("a cell")?;
        assert_eq!(cell.timing.keyed_capacity, 10);
        let timing = CellTiming::from_settings(&cell.timing);
        assert_eq!(
            timing,
            CellTiming {
                hold: Duration::from_millis(500),
                heartbeat: Duration::from_secs(2),
                lease: Duration::from_secs(9),
                watch_buffer: 64,
                lost_chooser: Duration::from_secs(4),
                watch_caps: WatchCaps {
                    cell: 3,
                    relay: 4,
                    follow: 5,
                },
                ..CellTiming::default()
            }
        );
        Ok(())
    });
}

/// With no timing variable set, the hub runs at the pace it ran at when
/// the pace was fixed in the binary.
#[test]
fn with_no_timing_variable_a_cells_hub_keeps_its_pace() {
    Jail::expect_with(|jail| {
        model_cell(jail, "/srv/cells");
        let cell = cell_from_env(&spec())
            .map_err(|e| e.to_string())?
            .ok_or("a cell")?;
        assert_eq!(cell.timing, HubTiming::default());
        let timing = CellTiming::from_settings(&cell.timing);
        assert_eq!(timing.hold, Duration::from_secs(2));
        assert_eq!(timing.heartbeat, Duration::from_secs(10));
        assert_eq!(timing.lease, Duration::from_secs(30));
        assert_eq!(timing.watch_buffer, 1024);
        assert_eq!(timing.lost_chooser, Duration::from_secs(30));
        assert_eq!(
            timing.watch_caps,
            WatchCaps {
                cell: 2048,
                relay: 2048,
                follow: 16384,
            }
        );
        assert_eq!(timing.refollow, Duration::from_millis(250));
        assert_eq!(timing, CellTiming::default());
        Ok(())
    });
}

/// Variables given to the reader are read as the same variables in the
/// environment are, so that a generator can check what it writes for a
/// hub without setting them in its own process.
#[test]
fn given_variables_are_read_as_the_environment_is() {
    let given = [
        ("GDL_CELL__NAME", "model-x-01-us-east-1"),
        ("gdl_cell__class", "model_x"),
        ("GDL_CELL__REGION", "us-east-1"),
        ("GDL_CELL__HUB", "model-x-01-hub:1337"),
        ("GDL_CELL__DIRECTORY", "dir"),
        ("GDL_CELL__DIR", "/srv/cells"),
        ("GDL_CELL__READY__D", "2"),
        ("GDL_PROFILE", "release"),
    ];
    Jail::expect_with(|jail| {
        for (name, value) in given {
            jail.set_env(name, value);
        }
        let read = cell_from_vars(&spec(), given.map(|(n, v)| (n.into(), v.into())))
            .map_err(|e| e.to_string())?;
        assert!(read.is_some());
        assert_eq!(read, cell_from_env(&spec()).map_err(|e| e.to_string())?);
        Ok(())
    });
    let misspelled = [("GDL_CELL__HONE", "x")].map(|(n, v)| (n.into(), v.into()));
    let refused = cell_from_vars(&spec(), misspelled).expect_err("a misspelled key");
    assert!(refused.to_string().contains("GDL_CELL__HONE"), "{refused}");
}

/// An IPv6 hub address starts with `[`, as a list would: it is read as
/// the address it is.
#[test]
fn an_ipv6_hub_address_is_read_as_written() {
    Jail::expect_with(|jail| {
        model_cell(jail, "/srv/cells");
        jail.set_env("GDL_CELL__HUB", "[::1]:1337");
        let cell = cell_from_env(&spec()).map_err(|e| e.to_string())?;
        assert_eq!(
            cell.and_then(|cell| cell.hub).as_deref(),
            Some("[::1]:1337")
        );
        Ok(())
    });
}

/// A requirement is `GDL_CELL__REQUIRES__<CLASS>`, its class matched
/// without regard to case, as a quorum's segment is.
#[test]
fn a_cells_requirements_are_read_from_the_environment() {
    Jail::expect_with(|jail| {
        jail.set_env("GDL_CELL__NAME", "global-us-east-1");
        jail.set_env("GDL_CELL__CLASS", "global");
        jail.set_env("GDL_CELL__REGION", "us-east-1");
        jail.set_env("GDL_CELL__STATE_DIR", "/var/lib/gdl-hub");
        jail.set_env("GDL_CELL__REQUIRES__MODEL_X", "2");
        let cell = cell_from_env(&spec()).map_err(|e| e.to_string())?;
        assert_eq!(
            cell.map(|cell| cell.requires),
            Some(BTreeMap::from([("model_x".into(), 2)]))
        );
        Ok(())
    });
}

/// A variable that names no setting, or whose value does not fit its
/// setting or the deployment, is refused, and the refusal names it.
#[test]
fn a_misspelled_or_malformed_cell_variable_is_refused_by_name() {
    for (variable, value, expected) in [
        (
            "GDL_CELL__HONE",
            "global-us-east-1",
            "GDL_CELL__HONE: unknown field",
        ),
        (
            "GDL_CELL__HUB",
            "http://hub:1337",
            "GDL_CELL__HUB: \"http://hub:1337\" must be `host:port`",
        ),
        (
            "GDL_CELL__DIRECTORY",
            "store",
            "GDL_CELL__DIRECTORY: unknown variant",
        ),
        (
            "GDL_CELL__SUMMARY_MS",
            "999",
            "GDL_CELL__SUMMARY_MS: is 999; the period is at least 1000 ms",
        ),
        (
            "GDL_CELL__SUMMARY_MS",
            "soon",
            "GDL_CELL__SUMMARY_MS: invalid type",
        ),
        // Nothing reads seeds: the relay mesh among global hubs is not built.
        (
            "GDL_CELL__SEEDS",
            r#"["hub-a:1337"]"#,
            "GDL_CELL__SEEDS: unknown field",
        ),
        (
            "GDL_CELL__READY__ZZ",
            "2",
            "GDL_CELL__READY__ZZ: not a segment of class `model_x`",
        ),
        (
            "GDL_CELL__REQUIRES__GLOBAL",
            "1",
            "GDL_CELL__REQUIRES__GLOBAL: not a class that `model_x` sends to",
        ),
        (
            "GDL_CELL__CLASS",
            "model_y",
            "GDL_CELL__CLASS: the placement has no class `model_y`",
        ),
        // Against the lease's default, 30 s.
        (
            "GDL_CELL__HEARTBEAT_MS",
            "30000",
            "GDL_CELL__HEARTBEAT_MS: is 30000; it must be shorter than the lease",
        ),
        (
            "GDL_CELL__LEASE_MS",
            "10000",
            "GDL_CELL__LEASE_MS: is 10000; it must be longer than the heartbeat",
        ),
        (
            "GDL_CELL__KEYED_CAPACITY",
            "0",
            "GDL_CELL__KEYED_CAPACITY: is 0, which holds no record",
        ),
        (
            "GDL_CELL__WATCH_BUFFER",
            "0",
            "GDL_CELL__WATCH_BUFFER: is 0",
        ),
        (
            "GDL_CELL__WATCH_CAPS__RELAY",
            "0",
            "GDL_CELL__WATCH_CAPS__RELAY: is 0",
        ),
        (
            "GDL_CELL__WATCH_CAPS__RELAYS",
            "1",
            "GDL_CELL__WATCH_CAPS__RELAYS: unknown field",
        ),
    ] {
        Jail::expect_with(|jail| {
            model_cell(jail, "/srv/cells");
            jail.set_env(variable, value);
            let Err(error) = cell_from_env(&spec()) else {
                return Err(format!("{variable}={value} was accepted").into());
            };
            let error = error.to_string();
            assert!(error.starts_with(expected), "{expected}\nin: {error}");
            Ok(())
        });
    }
    // The variable that supplied the value is named: the later of two
    // that set one key, and one that set a table under a key.
    for (variables, expected) in [
        (
            &[
                ("GDL_CELL__READY__D", "1"),
                ("GDL_CELL__READY", r#"{d="many"}"#),
            ][..],
            "GDL_CELL__READY: invalid type",
        ),
        (
            &[("GDL_CELL__SUMMARY_MS__EXTRA", "1")][..],
            "GDL_CELL__SUMMARY_MS__EXTRA: invalid type",
        ),
    ] {
        Jail::expect_with(|jail| {
            model_cell(jail, "/srv/cells");
            for (name, value) in variables {
                jail.set_env(name, value);
            }
            let Err(error) = cell_from_env(&spec()) else {
                return Err(format!("accepted {variables:?}").into());
            };
            let error = error.to_string();
            assert!(error.starts_with(expected), "{expected}\nin: {error}");
            Ok(())
        });
    }
    // A table set whole in one variable is that variable's.
    Jail::expect_with(|jail| {
        model_cell(jail, "/srv/cells");
        jail.set_env("GDL_CELL__READY", r#"{d="many"}"#);
        let Err(error) = cell_from_env(&spec()) else {
            return Err("a quorum of many was accepted".into());
        };
        let error = error.to_string();
        assert!(
            error.starts_with("GDL_CELL__READY: invalid type"),
            "{error}"
        );
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.set_env("GDL_CELL__CLASS", "model_x");
        jail.set_env("GDL_CELL__REGION", "us-east-1");
        let Err(error) = cell_from_env(&spec()) else {
            return Err("a cell without a name was accepted".into());
        };
        let error = error.to_string();
        assert!(
            error.starts_with("GDL_CELL__NAME: missing field `name`"),
            "{error}"
        );
        Ok(())
    });
}

/// `GET` on a hub's public listener: the status.
async fn status(hub: &Hub, path: &str) -> u16 {
    let url = format!("http://127.0.0.1:{}{path}", hub.public_addr().port());
    tokio::time::timeout(Duration::from_secs(5), reqwest::get(&url))
        .await
        .expect("the hub answers in time")
        .expect("the hub answers")
        .status()
        .as_u16()
}

fn settings(directory: DirectoryBackend) -> CellSettings {
    CellSettings {
        models: Vec::new(),
        name: "model-x-01-us-east-1".into(),
        layout: None,
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: None,
        directory,
        ready: BTreeMap::new(),
        requires: BTreeMap::new(),
        hub: Some("model-x-01-hub:1337".into()),
        summary: CellSettings::DEFAULT_SUMMARY,
        timing: Default::default(),
    }
}

/// A lease that expires between heartbeats must be refused for settings built in Rust too.
#[test]
fn hub_builder_refuses_a_typed_lease_no_longer_than_its_heartbeat() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-timing-")
        .tempdir()
        .expect("a directory");
    let mut cell = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    assert!(hub_builder(config(), spec(), Some(cell.clone())).is_ok());
    cell.timing.lease = cell.timing.heartbeat;
    let result = hub_builder(config(), spec(), Some(cell));
    assert!(
        result.is_err(),
        "hub_builder accepted lease_ms = heartbeat_ms = 10000"
    );
    let error = result.err().expect("the invalid lease is refused");
    assert!(error.to_string().contains("lease_ms"), "{error}");
}

/// A bound of zero on a lost chooser, built in code, is refused as the
/// reader refuses it, naming its key: the hub would stop waiting for a
/// chooser the moment its watch of the relay closed.
#[test]
fn hub_builder_refuses_a_typed_lost_chooser_bound_of_zero() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-timing-")
        .tempdir()
        .expect("a directory");
    let mut cell = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    cell.timing.lost_chooser = Duration::ZERO;
    let error = hub_builder(config(), spec(), Some(cell))
        .err()
        .expect("a bound of zero is refused");
    assert!(error.to_string().contains("lost_chooser_ms"), "{error}");
}

/// A watch buffer past its ceiling, built in code, is refused as the
/// reader refuses it, naming its key, before the hub takes its slots.
#[test]
fn hub_builder_refuses_a_typed_watch_buffer_past_its_ceiling() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-timing-")
        .tempdir()
        .expect("a directory");
    let mut cell = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    cell.timing.watch_buffer = HubTiming::MAX_WATCH_BUFFER + 1;
    let Err(error) = hub_builder(config(), spec(), Some(cell)) else {
        panic!("hub_builder accepted a watch buffer past its ceiling");
    };
    assert!(error.to_string().contains("watch_buffer"), "{error}");
}

/// A keyed record capacity of zero, built in code, is refused as the
/// reader refuses it, naming its key.
#[test]
fn hub_builder_refuses_a_typed_keyed_capacity_of_zero() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-timing-")
        .tempdir()
        .expect("a directory");
    let mut cell = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    cell.timing.keyed_capacity = 0;
    let Err(error) = hub_builder(config(), spec(), Some(cell)) else {
        panic!("hub_builder accepted a keyed capacity of zero");
    };
    assert!(error.to_string().contains("keyed_capacity"), "{error}");
}

/// With `dir`, the hub runs the cell's extension, which publishes the
/// cell's record into the directory's files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_with_the_dir_backend_publishes_into_its_directory() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-cells-")
        .tempdir()
        .expect("a directory");
    let cell = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    let hub = hub_builder(config(), spec(), Some(cell))
        .expect("a dir cell")
        .serve()
        .await
        .expect("the hub serves");
    assert_eq!(status(&hub, "/v1/ext/gdl-cell/health").await, 503);
    // Read as another hub on the host reads it.
    let mut watch = DirDirectory::new(dir.path()).watch("model_x", None);
    let published = async {
        loop {
            match watch.next().await {
                Some(Ok(CellEvent::Current(record) | CellEvent::Put { record, .. })) => {
                    return record;
                }
                Some(Ok(_)) => {}
                other => panic!("the watch ended: {other:?}"),
            }
        }
    };
    let record = tokio::time::timeout(Duration::from_secs(5), published)
        .await
        .expect("the record is published in time");
    assert_eq!(record.name, "model-x-01-us-east-1");
    assert_eq!(record.state, CellState::Unavailable);
    hub.shutdown().await.expect("the hub stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_without_a_cell_runs_admission_alone() {
    let hub = hub_builder(config(), spec(), None)
        .expect("no cell")
        .serve()
        .await
        .expect("the hub serves");
    assert_eq!(status(&hub, "/v1/ext/gdl-cell/health").await, 404);
    assert_eq!(status(&hub, "/v1/ext/gdl/members").await, 200);
    hub.shutdown().await.expect("the hub stops");
}

/// The hub the binary builds runs at its settings' pace: with a cap of one
/// watch of the cell, a second watch while the first is open is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_hub_binary_serves_the_watch_caps_its_settings_give() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-caps-")
        .tempdir()
        .expect("a directory");
    let mut cell = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    cell.timing.watch_caps.cell = 1;
    let hub = hub_builder(config(), spec(), Some(cell))
        .expect("a dir cell")
        .serve()
        .await
        .expect("the hub serves");
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-cell/watch",
        hub.public_addr().port()
    );
    let held = tokio::time::timeout(Duration::from_secs(5), reqwest::get(&url))
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    assert_eq!(held.status().as_u16(), 200, "the one watch is served");
    assert_eq!(
        status(&hub, "/v1/ext/gdl-cell/watch").await,
        503,
        "a second watch is refused"
    );
    drop(held);
    hub.shutdown().await.expect("the hub stops");
}

/// `memory` is one directory inside one process: a hub binary refuses it
/// before it serves, and says what to set.
#[test]
fn the_memory_backend_is_refused() {
    let Err(error) = hub_builder(config(), spec(), Some(settings(DirectoryBackend::Memory))) else {
        panic!("accepted memory");
    };
    let error = error.to_string();
    assert!(error.contains("\"memory\""), "{error}");
    assert!(error.contains("GDL_CELL__PARENT_HUB"), "{error}");
}

/// With `relay`, the default, a child cell's hub needs its parent's hub
/// to report to, and serves once it has one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_with_the_relay_backend_needs_its_parents_hub() {
    let Err(error) = hub_builder(config(), spec(), Some(settings(DirectoryBackend::DEFAULT)))
    else {
        panic!("a child without its parent's hub");
    };
    assert!(
        error.to_string().contains("[cell] parent_hub: missing"),
        "{error}"
    );

    let parent = hub_builder(config(), spec(), None)
        .expect("no cell")
        .serve()
        .await
        .expect("the hub serves");
    let relay = DirectoryBackend::Relay {
        parent_hub: Some(format!("127.0.0.1:{}", parent.public_addr().port())),
        state_dir: None,
    };
    let hub = hub_builder(config(), spec(), Some(settings(relay)))
        .expect("a relay cell")
        .serve()
        .await
        .expect("the hub serves");
    assert_eq!(status(&hub, "/v1/ext/gdl-cell/health").await, 503);
    hub.shutdown().await.expect("the hub stops");
    parent.shutdown().await.expect("the hub stops");
}

/// A hub reads its cell's declared models from one variable, a list of
/// tables, `GDL_CELL__MODELS`; one it cannot read names the variable.
#[test]
fn a_cells_declared_models_are_read_from_the_environment() {
    Jail::expect_with(|jail| {
        model_cell(jail, "/srv/cells");
        jail.set_env(
            "GDL_CELL__MODELS",
            r#"[{model = "llama-70b"}, {model = "qwen-32b", routing_key = "a"}]"#,
        );
        let cell = cell_from_env(&spec()).map_err(|e| e.to_string())?;
        assert_eq!(
            cell.map(|cell| cell.models),
            Some(vec![
                gdl_model::ModelKey::new("llama-70b"),
                gdl_model::ModelKey {
                    model: "qwen-32b".into(),
                    routing_key: Some("a".into()),
                },
            ])
        );
        jail.set_env("GDL_CELL__MODELS", r#"[{model = ""}]"#);
        let refused = cell_from_env(&spec()).expect_err("a model with no name");
        assert!(
            refused.to_string().contains("GDL_CELL__MODELS"),
            "{refused}"
        );
        Ok(())
    });
}
