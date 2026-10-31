//! A segment that hosts a pinned frontend or service builds. The chat
//! example serves JSON lines and asr is driven through its library, so
//! their generated segments do not reach the code that hands a frontend
//! its `next` or a service its clients; these two examples do.

mod common;

use common::{cargo, root, target};

#[test]
fn a_pinned_frontend_or_service_segment_builds() {
    for example in ["http", "api"] {
        let temp = tempfile::Builder::new()
            .prefix(&format!("gdl-gen-frontend-{example}-"))
            .tempdir()
            .expect("dir");
        let out = temp.path().join("segments");
        let generate = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .args(["generate", "--spec"])
            .arg(root().join("examples").join(example).join("gdl.toml"))
            .arg("--out")
            .arg(&out)
            .output()
            .expect("cargo gdl generate");
        assert!(
            generate.status.success(),
            "{example}: {}",
            String::from_utf8_lossy(&generate.stderr)
        );
        check_segments(temp.path(), example);
    }
}

/// The generated block that carries an entry's needs compiles: no example
/// declares one.
#[test]
fn a_service_whose_entries_need_kinds_builds() {
    let (spec, graph) = common::api_needing_properties();
    let spec = gdl_gen::Spec::from_text(&spec, &graph, &root().join("examples/api")).expect("spec");
    let temp = tempfile::Builder::new()
        .prefix("gdl-gen-frontend-needs-")
        .tempdir()
        .expect("dir");
    let out = temp.path().join("segments");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
    let front = std::fs::read_to_string(out.join("front/src/lib.rs")).expect("the front segment");
    assert!(front.contains("needs: ["), "{front}");
    check_segments(temp.path(), "needs");
}

/// `cargo check` the generated crates in `dir/segments`.
fn check_segments(dir: &std::path::Path, example: &str) {
    // The root workspace's lock, which locks the runtime's whole tree
    // (velo among it): an example's own lock covers only what its
    // crate depends on, and the api example's has no runtime.
    std::fs::write(
        dir.join("Cargo.toml"),
        "[workspace]\nresolver = \"3\"\nmembers = [\"segments/*\"]\n",
    )
    .expect("workspace");
    std::fs::copy(root().join("Cargo.lock"), dir.join("Cargo.lock")).expect("lock");
    let build = cargo()
        .arg("check")
        .arg("--offline")
        .arg("--manifest-path")
        .arg(dir.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target("examples"))
        .output()
        .expect("cargo check");
    assert!(
        build.status.success(),
        "{example}: {}",
        String::from_utf8_lossy(&build.stderr)
    );
    let locked = std::fs::read_to_string(dir.join("Cargo.lock")).expect("lock");
    assert_eq!(
        velo_source(&locked),
        velo_source(&std::fs::read_to_string(root().join("Cargo.lock")).expect("lock")),
        "{example}: the segments built against another velo"
    );
}

/// The source line of the `velo` package in a lock file.
fn velo_source(lock: &str) -> Option<&str> {
    let mut lines = lock.lines();
    lines.find(|line| *line == "name = \"velo\"")?;
    lines.find(|line| line.starts_with("source = "))
}
