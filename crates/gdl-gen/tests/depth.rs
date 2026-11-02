//! Deep futures build in a generated segment. rustc lays out an `async fn`
//! with every future it holds, and fails past a query depth of 128 by
//! default (`queries overflow the depth limit`).
//!
//! A segment's `build()` awaits each node's build inside the builder's
//! scopes, so when it held the node's future itself, those scopes and the
//! node's own depth added up: a node that compiled alone failed in its
//! segment, and each scope the builder gained took depth from every node.
//!
//! The request path nests in the same way, with one layer per stage, and
//! each rustc release moves its depth. So a generated crate root raises the
//! limit, and the second test proves it with a chain past the default.
//!
//! The tests build generated segments in a nested cargo run that shares the
//! examples' target directory: a cold build takes minutes, a warm one
//! seconds. The nested cargo runs on the repository's pinned toolchain,
//! which rustup passes down in `RUSTUP_TOOLCHAIN`; the fixtures' nesting is
//! calibrated on it.

mod common;

use std::path::{Path, PathBuf};
use std::process::Output;

use common::{cargo, root, target};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/depth")
}

/// The spec `file` of the fixture, generated in `out` with a workspace of
/// the contract and the segments.
fn generate(file: &str, out: &Path) {
    let spec = gdl_gen::Spec::load(&fixture().join(file)).expect("spec");
    let files = gdl_gen::render(&spec, out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(out, &files).expect("write");
    std::fs::write(
        out.join("Cargo.toml"),
        "[workspace]\nmembers = [\"*\"]\nresolver = \"3\"\n",
    )
    .expect("workspace");
    // Resolved as the repository is, so that the build reuses its versions.
    std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
}

fn build(out: &Path, package: &str) -> Output {
    cargo()
        .arg("build")
        .arg("--manifest-path")
        .arg(out.join("Cargo.toml"))
        .args(["-p", package])
        .env("CARGO_TARGET_DIR", target("examples"))
        .output()
        .expect("cargo build")
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
    generate("gdl.toml", &out);
    let built = build(&out, "depth-all");
    let stderr = String::from_utf8_lossy(&built.stderr);
    assert!(
        !stderr.contains("could not compile `depth-nodes`"),
        "the fixture's node no longer compiles alone on this toolchain: \
         lower its nesting until it does\n{stderr}"
    );
    assert!(built.status.success(), "{stderr}");
}

/// The front's request path is deeper than the default limit, as the
/// global router's is on later toolchains: the handler of its hop for an
/// arriving request lays out a `HopCore` over nested `DOperator` and `DHop`
/// layers. It builds because the generated crate roots raise the limit.
/// Taken out, the same item overflows, so the build cannot pass for want of
/// depth.
///
/// Takes about 10 s: the chain's types are long, and the crate builds twice.
#[test]
fn a_request_path_past_the_default_limit_builds_in_its_segment() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-depth-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    generate("chain.toml", &out);
    let built = build(&out, "chain-front");
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );

    for crate_root in ["lib.rs", "main.rs"] {
        let path = out.join("front/src").join(crate_root);
        let text = std::fs::read_to_string(&path).expect("crate root");
        let limit = "#![recursion_limit = \"256\"]\n";
        assert!(text.contains(limit), "{crate_root} sets no limit:\n{text}");
        std::fs::write(&path, text.replace(limit, "")).expect("crate root");
    }
    let overflowed = build(&out, "chain-front");
    let stderr = String::from_utf8_lossy(&overflowed.stderr);
    assert!(
        !overflowed.status.success()
            && stderr.contains("queries overflow the depth limit")
            && stderr.contains("accept_forward"),
        "without the attribute the hop no longer overflows on this toolchain: \
         add stages to chain.gdl until it does\n{stderr}"
    );
}
