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

/// The nodes file is part of the crate's build: an edit that makes it
/// disagree with the code fails the next build, though no Rust file
/// changed. Built from a copy, so the other tests' crate is not touched.
#[test]
fn an_edit_to_the_nodes_file_rebuilds_the_crate() {
    let dir = std::env::temp_dir().join(format!("gdl-node-edit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for krate in ["good", "protocols"] {
        copy(&fixtures().join(krate), &dir.join(krate));
    }
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    std::fs::write(
        dir.join("Cargo.toml"),
        format!(
            "[workspace]\nmembers = [\"good\", \"protocols\"]\nresolver = \"3\"\n\
             [workspace.package]\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\
             [workspace.dependencies]\nfutures = \"0.3\"\ngdl = {{ path = {:?} }}\n\
             gdl-message = {{ path = {:?} }}\nnode-protocols = {{ path = \"protocols\" }}\n\
             serde = {{ version = \"1\", features = [\"derive\"] }}\n",
            crates.join("gdl"),
            crates.join("gdl-message"),
        ),
    )
    .expect("workspace");
    let build = || {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let output = Command::new(cargo)
            .args(["build", "--quiet", "-p", "node-good", "--manifest-path"])
            .arg(dir.join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", target())
            .output()
            .expect("cargo");
        (
            output.status.success(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    };
    let (ok, stderr) = build();
    assert!(ok, "{stderr}");
    let file = dir.join("good/gdl/nodes.gdl");
    let text = std::fs::read_to_string(&file).expect("nodes file");
    std::fs::write(
        &file,
        text.replace("Words -> stream Token;", "Words -> stream Piece;"),
    )
    .expect("edit");
    let (ok, stderr) = build();
    assert!(!ok, "the edit did not rebuild the crate");
    assert!(
        stderr.contains("node `Engine`: its `Sink::Item` is not `Piece`"),
        "{stderr}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn copy(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("dir");
    for entry in std::fs::read_dir(from).expect("read") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("type").is_dir() {
            copy(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy");
        }
    }
}
