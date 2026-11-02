//! A node whose build future nests deeply still builds in a generated
//! segment. rustc lays out an `async fn` with every future it holds, and
//! fails past a query depth of 128 (`queries overflow the depth limit`).
//! The segment's `build()` awaits each node's build inside the builder's
//! scopes, so when it held the node's future itself, those scopes and the
//! node's own depth added up: a node that compiled alone failed in its
//! segment, and each scope the builder gained took depth from every node.
//!
//! The test builds the generated segment in a nested cargo run that shares
//! the examples' target directory: a cold build takes minutes, a warm one
//! seconds. The nested cargo runs on the repository's pinned toolchain,
//! which rustup passes down in `RUSTUP_TOOLCHAIN`; the fixture's nesting is
//! calibrated on it.

mod common;

use std::path::{Path, PathBuf};

use common::{cargo, root, target};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/depth")
}

/// `Deep`'s build nests nearly as deep as its own crate compiles. Its
/// segment builds: the segment adds nothing to the depth of a node's
/// build.
#[test]
fn a_node_that_builds_alone_builds_in_its_segment() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-depth-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    let spec = gdl_gen::Spec::load(&fixture().join("gdl.toml")).expect("spec");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
    std::fs::write(
        out.join("Cargo.toml"),
        "[workspace]\nmembers = [\"contract\", \"all\"]\nresolver = \"3\"\n",
    )
    .expect("workspace");
    // Resolved as the repository is, so that the build reuses its versions.
    std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
    let build = cargo()
        .arg("build")
        .arg("--manifest-path")
        .arg(out.join("Cargo.toml"))
        .args(["-p", "depth-all"])
        .env("CARGO_TARGET_DIR", target("examples"))
        .output()
        .expect("cargo build");
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        !stderr.contains("could not compile `depth-nodes`"),
        "the fixture's node no longer compiles alone on this toolchain: \
         lower its nesting until it does\n{stderr}"
    );
    assert!(build.status.success(), "{stderr}");
}
