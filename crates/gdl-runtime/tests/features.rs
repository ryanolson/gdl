//! The runtime's builds with optional features omitted.

use std::process::Command;

/// The tests enable `hub` through a dev dependency, so check the library separately.
#[test]
fn the_runtime_builds_without_hub_with_warnings_denied() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let output = Command::new("timeout")
        .arg("120")
        .arg(cargo)
        .args([
            "rustc",
            "--package",
            "gdl-runtime",
            "--lib",
            "--no-default-features",
            "--offline",
            "--",
            "-D",
            "warnings",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("CARGO_BUILD_JOBS", "4")
        .output()
        .expect("compile the runtime without hub");
    assert!(
        output.status.success(),
        "the runtime without hub fails with warnings denied:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
