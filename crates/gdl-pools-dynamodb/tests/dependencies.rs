//! What the DynamoDB directory links, and who links it. The AWS SDK is the
//! heaviest dependency in the workspace, so it stays in this crate: no
//! other workspace crate depends on it or on this crate, and the crate
//! links neither the transport nor a TLS stack of its own.

use std::process::Command;

fn tree(args: &[&str]) -> String {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo)
        .arg("tree")
        .args(args)
        .args(["--prefix", "none", "--offline"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("run cargo tree");
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("utf-8")
}

/// Every workspace crate that reaches the SDK, by any kind of edge, is
/// this one: the rest of the workspace builds without it.
#[test]
fn only_this_crate_links_the_aws_sdk() {
    let dependents = tree(&[
        "--workspace",
        "--invert",
        "aws-sdk-dynamodb",
        "--edges",
        "normal,build,dev",
    ]);
    let mut crates: Vec<&str> = dependents
        .lines()
        .filter(|line| line.starts_with("gdl"))
        .filter_map(|line| line.split_whitespace().next())
        .collect();
    crates.sort_unstable();
    crates.dedup();
    assert_eq!(crates, ["gdl-pools-dynamodb"], "{dependents}");
}

/// A pool directory backend needs only the trait: no velo, as for
/// `gdl-pools`, and no aws-lc, whose C build is the SDK's default TLS.
#[test]
fn the_crate_links_no_transport_and_no_tls_of_its_own() {
    let linked = tree(&["--package", "gdl-pools-dynamodb", "--edges", "normal"]);
    let unwanted: Vec<&str> = linked
        .lines()
        .filter(|line| {
            ["velo", "aws-lc", "rustls ", "ring "]
                .iter()
                .any(|name| line.starts_with(name))
        })
        .collect();
    assert!(unwanted.is_empty(), "gdl-pools-dynamodb links {unwanted:?}");
}
