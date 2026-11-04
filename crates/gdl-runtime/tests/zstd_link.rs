//! libzstd is C, and a segment pays to build it only when one of its
//! edges compresses: `gdl-runtime` links it behind its `zstd` feature, and
//! `gdl-model`, which the composer builds for wasm32, never does.

use std::process::Command;

/// The packages `package`, built with `features`, links whose names start
/// with `zstd`.
fn zstd_in(package: &str, features: &[&str]) -> Vec<String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let features = features.join(",");
    let mut args = vec![
        "tree",
        "--package",
        package,
        "--edges",
        "normal",
        "--prefix",
        "none",
        "--offline",
    ];
    if !features.is_empty() {
        args.extend(["--features", &features]);
    }
    let output = Command::new(cargo)
        .args(&args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("run cargo tree");
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("utf-8")
        .lines()
        .filter(|line| line.starts_with("zstd"))
        .map(str::to_string)
        .collect()
}

#[test]
fn the_runtime_links_zstd_only_with_its_feature() {
    assert_eq!(zstd_in("gdl-runtime", &[]), Vec::<String>::new());
    assert_eq!(
        zstd_in("gdl-runtime", &["hub", "json-lines"]),
        Vec::<String>::new()
    );
    // The control: the feature links it.
    let linked = zstd_in("gdl-runtime", &["zstd"]);
    assert!(
        linked.iter().any(|line| line.starts_with("zstd-safe")),
        "{linked:?}"
    );
}

#[test]
fn the_model_never_links_zstd() {
    assert_eq!(zstd_in("gdl-model", &[]), Vec::<String>::new());
}
