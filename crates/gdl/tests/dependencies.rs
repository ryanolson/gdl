//! What a crate that defines nodes links.

use std::process::Command;

/// A node crate depends on gdl for its traits and derives. It must not link
/// the runtime's transport: a crate that only defines nodes, such as one
/// that also serves another framework, pays for velo only when it runs a
/// segment.
#[test]
fn defining_nodes_does_not_link_velo() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo)
        .args([
            "tree",
            "--package",
            "gdl",
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
    let velo: Vec<&str> = tree
        .lines()
        .filter(|line| line.starts_with("velo"))
        .collect();
    assert!(velo.is_empty(), "gdl links {velo:?}");
}
