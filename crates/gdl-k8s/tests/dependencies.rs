//! What the render library links.

use std::process::Command;

/// The operator renders a pool's objects with this crate, and must never
/// link velo, whose calls all live in the runtime's velo module: so this
/// crate links neither velo nor anything that does.
#[test]
fn the_render_library_does_not_link_velo() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo)
        .args([
            "tree",
            "--package",
            "gdl-k8s",
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
        .filter(|line| {
            ["velo", "gdl-watch", "gdl-runtime", "gdl-hub"]
                .iter()
                .any(|name| line.starts_with(name))
        })
        .collect();
    assert!(linked.is_empty(), "gdl-k8s links {linked:?}");
}
