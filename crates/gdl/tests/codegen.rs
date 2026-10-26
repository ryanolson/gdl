//! Code generation guards for the cell policies.

use std::path::{Path, PathBuf};
use std::process::Command;

/// `HomeFirst` chooses at constant thresholds: its release code passes
/// no threshold object to `HomeFirstAt`'s `choose`, which loads its
/// thresholds as data. The configurable policy's own loads are the
/// control. It builds gdl's release IR, about 9 s cold and 1 s warm.
#[test]
fn home_first_keeps_its_thresholds_in_code() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the workspace contains crates/gdl");
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target"));
    let built = Command::new(env!("CARGO"))
        .current_dir(workspace)
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_BUILD_JOBS", "6")
        .args([
            "rustc",
            "-p",
            "gdl",
            "--release",
            "--lib",
            "--message-format=json",
            "--",
            "--emit=llvm-ir",
        ])
        .output()
        .expect("compile gdl's release IR");
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let library = String::from_utf8_lossy(&built.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|value| value["reason"] == "compiler-artifact" && value["target"]["name"] == "gdl")
        .find_map(|value| {
            value["filenames"].as_array()?.iter().find_map(|name| {
                let path = PathBuf::from(name.as_str()?);
                (path.extension()? == "rmeta").then_some(path)
            })
        })
        .expect("gdl's library artifact");
    let stem = library
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.strip_prefix("lib"))
        .expect("a Rust library name");
    let ir = std::fs::read_to_string(library.with_file_name(format!("{stem}.ll")))
        .expect("the emitted IR beside the library");
    let definition = |policy: &str| {
        ir.split("\ndefine ")
            .find(|item| {
                let signature = item.lines().next().unwrap_or_default();
                signature.contains(policy) && signature.contains("6choose")
            })
            .and_then(|item| item.split_once("\n}"))
            .map(|(body, _)| body)
            .expect("the policy's choose definition")
    };
    let configured = definition("home_first..HomeFirstAt$u20$");
    assert!(
        configured.contains("load i64, ptr %self"),
        "the configurable policy must provide the data-load control"
    );
    let unit = definition("home_first..HomeFirst$u20$");
    let loads_thresholds = unit.lines().any(|line| {
        line.contains("call ")
            && line.contains("HomeFirstAt$u20$")
            && line.contains("6choose")
            && line.contains("@alloc_")
    });
    assert!(
        !loads_thresholds,
        "HomeFirst passes its constant thresholds as data to a chooser that loads them:\n{unit}"
    );
}
