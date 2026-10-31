//! What the operator links.

use std::process::Command;

/// The operator talks to the Kubernetes API and to a pool hub's HTTP
/// routes alone. It must not link velo, whose calls all live in the
/// runtime's velo module: it reads the hub's instance list through a
/// mirror of its own.
#[test]
fn the_operator_does_not_link_velo() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo)
        .args([
            "tree",
            "--package",
            "gdl-operator",
            "--edges",
            "normal",
            "--prefix",
            "none",
            "--offline",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("run cargo tree");
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tree = String::from_utf8(output.stdout).expect("utf-8");
    let linked: Vec<&str> = tree
        .lines()
        .filter(|line| line.starts_with("velo") || line.starts_with("gdl-watch"))
        .collect();
    assert!(linked.is_empty(), "gdl-operator links {linked:?}");
}
