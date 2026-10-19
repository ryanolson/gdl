//! `#[gdl::node]`: a node type checked against its crate's nodes file when
//! the crate compiles. Each case is a crate of the fixture workspace in
//! `tests/fixtures/nodes`, built with cargo: the attribute reads the
//! manifest of the crate being compiled, which only a real crate has.
//!
//! The first build of the fixture workspace compiles gdl and takes a
//! while; the target directory is the one the generator's tests share, so
//! it is usually warm.

use std::path::{Path, PathBuf};
use std::process::Command;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/nodes")
}

fn target() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"))
        .join("gdl-gen-tests")
}

/// Build package `package` of the fixture workspace: its success, and what
/// the compiler said.
fn build(package: &str) -> (bool, String) {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["build", "--quiet", "-p", package, "--manifest-path"])
        .arg(fixtures().join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target())
        .output()
        .expect("cargo");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn fails_with(package: &str, expected: &[&str]) {
    let (ok, stderr) = build(package);
    assert!(!ok, "{package} built");
    for part in expected {
        assert!(stderr.contains(part), "{part:?} missing from:\n{stderr}");
    }
}

/// Every role, streamed requests with and without a head, and a type in a
/// module that the crate root re-exports.
#[test]
fn nodes_that_match_their_declarations_compile() {
    let (ok, stderr) = build("node-good");
    assert!(ok, "{stderr}");
}

#[test]
fn a_port_with_another_message_fails_to_compile() {
    fails_with(
        "node-wrong-message",
        &["node `Engine`: its `Sink::Item` is not `Piece`, as gdl/nodes.gdl declares"],
    );
}

#[test]
fn an_undeclared_node_fails_to_compile() {
    fails_with(
        "node-undeclared",
        &["`Other` is not declared in gdl/nodes.gdl, which declares `Engine`"],
    );
}

#[test]
fn a_node_the_crate_root_does_not_reach_fails_to_compile() {
    fails_with(
        "node-not-at-root",
        &["cannot find type `Engine` in the crate root"],
    );
}

#[test]
fn a_node_of_another_role_fails_to_compile() {
    fails_with(
        "node-wrong-role",
        &["the trait bound `Engine: gdl::Forward` is not satisfied"],
    );
}

#[test]
fn a_crate_without_a_nodes_file_fails_to_compile() {
    fails_with(
        "node-no-metadata",
        &["`#[gdl::node]` needs `[package.metadata.gdl] nodes = \"<file>\"` in Cargo.toml"],
    );
}
