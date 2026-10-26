//! The model and the graph syntax stay portable: they build for
//! `wasm32-unknown-unknown`, so that they can run in a browser, and
//! neither they nor the generator depend on the runtime.

use std::path::{Path, PathBuf};
use std::process::Command;

fn cargo() -> Command {
    let mut command = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command.current_dir(workspace());
    command
}

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The normal dependencies of `packages`, by name.
fn dependencies(packages: &[&str]) -> Vec<String> {
    let mut command = cargo();
    command.args([
        "tree", "-e", "normal", "--prefix", "none", "--format", "{p}",
    ]);
    for package in packages {
        command.args(["-p", package]);
    }
    let output = command.output().expect("cargo tree");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().next().map(str::to_string))
        .collect()
}

/// A runtime dependency in the model would not build for wasm, and would
/// put velo and tokio into every reader of the model.
#[test]
fn the_model_and_the_generator_do_not_depend_on_the_runtime() {
    let runtime = [
        "gdl",
        "gdl-runtime",
        "velo",
        "tokio",
        "tokio-util",
        "mio",
        "socket2",
        "dashmap",
        "futures",
    ];
    // The control: the runtime's own tree holds what the others must not.
    let gdl = dependencies(&["gdl-runtime"]);
    assert!(gdl.iter().any(|p| p == "velo") && gdl.iter().any(|p| p == "gdl-model"));
    let portable = dependencies(&["gdl-model", "gdl-syntax"]);
    for name in runtime.iter().chain(&["linkme", "gdl-message"]) {
        assert!(
            !portable.iter().any(|p| p == name),
            "gdl-model or gdl-syntax depends on {name}"
        );
    }
    let generator = dependencies(&["gdl-gen"]);
    for name in runtime {
        assert!(
            !generator.iter().any(|p| p == name),
            "gdl-gen depends on {name}"
        );
    }
}

/// The rules build for wasm, so that a browser can run them. The
/// check needs the target's standard library; without it the test says how
/// to add it and passes, so a machine without wasm can run the suite. The
/// dependency test above runs everywhere.
#[test]
fn the_model_and_the_syntax_build_for_wasm() {
    let sysroot = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
        .current_dir(workspace())
        .args(["--print", "sysroot"])
        .output()
        .expect("rustc");
    let sysroot = PathBuf::from(String::from_utf8_lossy(&sysroot.stdout).trim());
    if !sysroot.join("lib/rustlib/wasm32-unknown-unknown").exists() {
        println!("skipped: run `rustup target add wasm32-unknown-unknown` to check the wasm build");
        return;
    }
    // A target directory of its own: a wasm build shares nothing with the
    // host's, and would evict its artifacts.
    let target = workspace().join("target/gdl-model-wasm");
    let output = cargo()
        .args([
            "check",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "gdl-model",
            "-p",
            "gdl-syntax",
        ])
        .arg("--target-dir")
        .arg(&target)
        .output()
        .expect("cargo check");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
