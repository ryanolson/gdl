//! The hub binary's settings: a cell's settings from `GDL_CELL__*`, and
//! the hub they make. Every hub here runs in process on OS-assigned ports.

use std::collections::BTreeMap;
use std::time::Duration;

use figment::Jail;
use futures::StreamExt;
use gdl_cells::{CellDirectory, CellEvent, CellState, DirDirectory};
use gdl_hub::{cell_from_env, hub_builder};
use gdl_model::{
    CellSettings, DeploymentSpec, DirectoryBackend, HopInfo, Manifest, NodeInfo, Placement, Role,
    TypeInfo,
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
        assert_eq!(cell_from_env().map_err(|e| e.to_string())?, None);
        Ok(())
    });
}

/// Keys are matched without regard to case, `__` nests them, and a list
/// is written as one.
#[test]
fn a_cells_settings_are_read_from_the_environment() {
    Jail::expect_with(|jail| {
        model_cell(jail, "/srv/cells");
        jail.set_env("GDL_CELL__READY__D", "2");
        jail.set_env("GDL_CELL__SEEDS", r#"["hub-a:1337"]"#);
        jail.set_env("GDL_CELL__SUMMARY_MS", "2500");
        assert_eq!(
            cell_from_env().map_err(|e| e.to_string())?,
            Some(CellSettings {
                name: "model-x-01-us-east-1".into(),
                class: "model_x".into(),
                region: "us-east-1".into(),
                home: None,
                directory: DirectoryBackend::Dir {
                    path: "/srv/cells".into()
                },
                seeds: vec!["hub-a:1337".into()],
                ready: BTreeMap::from([("d".into(), 2)]),
                requires: BTreeMap::new(),
                hub: Some("model-x-01-hub:1337".into()),
                summary: Duration::from_millis(2500),
            })
        );
        Ok(())
    });
}

/// An IPv6 hub address starts with `[`, as a list would: it is read as
/// the address it is.
#[test]
fn an_ipv6_hub_address_is_read_as_written() {
    Jail::expect_with(|jail| {
        model_cell(jail, "/srv/cells");
        jail.set_env("GDL_CELL__HUB", "[::1]:1337");
        let cell = cell_from_env().map_err(|e| e.to_string())?;
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
        jail.set_env("GDL_CELL__REQUIRES__MODEL_X", "2");
        let cell = cell_from_env().map_err(|e| e.to_string())?;
        assert_eq!(
            cell.map(|cell| cell.requires),
            Some(BTreeMap::from([("model_x".into(), 2)]))
        );
        Ok(())
    });
}

#[test]
fn a_misspelled_or_malformed_cell_variable_is_refused() {
    for (variable, value, expected) in [
        ("GDL_CELL__HONE", "global-us-east-1", "hone"),
        ("GDL_CELL__HUB", "http://hub:1337", "[cell] hub"),
        ("GDL_CELL__DIRECTORY", "store", "store"),
        ("GDL_CELL__SUMMARY_MS", "999", "summary_ms"),
    ] {
        Jail::expect_with(|jail| {
            model_cell(jail, "/srv/cells");
            jail.set_env(variable, value);
            let Err(error) = cell_from_env() else {
                return Err(format!("{variable}={value} was accepted").into());
            };
            assert!(error.to_string().contains(expected), "{error}");
            Ok(())
        });
    }
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
        name: "model-x-01-us-east-1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: None,
        directory,
        seeds: vec![],
        ready: BTreeMap::new(),
        requires: BTreeMap::new(),
        hub: Some("model-x-01-hub:1337".into()),
        summary: CellSettings::DEFAULT_SUMMARY,
    }
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

/// `memory` is one directory inside one process, and `relay` is not built:
/// a hub binary refuses both before it serves, and says what to set.
#[test]
fn the_memory_and_relay_backends_are_refused() {
    for (directory, expected) in [
        (DirectoryBackend::Memory, "\"memory\""),
        (DirectoryBackend::Relay, "\"relay\""),
    ] {
        let Err(error) = hub_builder(config(), spec(), Some(settings(directory))) else {
            panic!("accepted {expected}");
        };
        let error = error.to_string();
        assert!(error.contains(expected), "{error}");
        assert!(error.contains("GDL_CELL__DIRECTORY=dir"), "{error}");
    }
}
