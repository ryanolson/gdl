//! A segment that hosts a pinned frontend or service builds. The chat and
//! asr examples serve JSON lines, so their generated segments do not reach
//! the code that hands a frontend its `next` or a service its clients;
//! these two examples do.

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
        // The example's lock, so the build resolves the versions the
        // example is tested with.
        std::fs::write(
            temp.path().join("Cargo.toml"),
            "[workspace]\nresolver = \"3\"\nmembers = [\"segments/*\"]\n",
        )
        .expect("workspace");
        std::fs::copy(
            root().join("examples").join(example).join("Cargo.lock"),
            temp.path().join("Cargo.lock"),
        )
        .expect("lock");
        let build = cargo()
            .arg("check")
            .arg("--offline")
            .arg("--manifest-path")
            .arg(temp.path().join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("examples"))
            .output()
            .expect("cargo check");
        assert!(
            build.status.success(),
            "{example}: {}",
            String::from_utf8_lossy(&build.stderr)
        );
    }
}
