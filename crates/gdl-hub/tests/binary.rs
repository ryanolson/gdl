//! The hub binary refuses a cell's settings it cannot run before it binds
//! a port, and says what to set. Only refusals are run here: a hub that
//! starts binds the deployment's fixed ports.

use std::process::Command;

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
            forward_types: vec![TypeInfo::new("T", true)],
            backward_types: vec![TypeInfo::new("T", true)],
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
