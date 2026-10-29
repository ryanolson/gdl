//! What the DynamoDB directory links, and who links it. The AWS SDK is the
//! heaviest dependency in the workspace, so it stays in this crate: no
//! other workspace crate depends on it or on this crate by default, and the
//! crate links neither the transport nor a TLS stack of its own. The one
//! exception is opt-in: `gdl-hub`'s `dynamodb` feature, off by default,
//! with which the stock hub binary publishes to DynamoDB. The hub must then
//! build the client itself, with a credential chain and an HTTPS client,
//! which only a program can choose, so it links the SDK.

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

/// The workspace crates that reach `package`, by any kind of edge, with
/// the further `args`.
fn dependents(package: &str, more: &[&str]) -> (Vec<String>, String) {
    let mut args = vec![
        "--workspace",
        "--invert",
        package,
        "--edges",
        "normal,build,dev",
    ];
    args.extend(more);
    let dependents = tree(&args);
    let mut crates: Vec<String> = dependents
        .lines()
        .filter(|line| line.starts_with("gdl"))
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .collect();
    crates.sort_unstable();
    crates.dedup();
    (crates, dependents)
}

/// Every workspace crate that reaches the SDK by default is this one: the
/// rest of the workspace builds without it. With every feature on, the
/// hub's `dynamodb` feature makes the hub depend on the SDK and this crate,
/// and no other crate depends on either itself.
#[test]
fn only_this_crate_links_the_aws_sdk() {
    let (crates, tree) = dependents("aws-sdk-dynamodb", &[]);
    assert_eq!(crates, ["gdl-pools-dynamodb"], "{tree}");
    let direct = ["--all-features", "--depth", "1"];
    let (crates, tree) = dependents("aws-sdk-dynamodb", &direct);
    assert_eq!(crates, ["gdl-hub", "gdl-pools-dynamodb"], "{tree}");
    let (crates, tree) = dependents("gdl-pools-dynamodb", &direct);
    assert_eq!(crates, ["gdl-hub", "gdl-pools-dynamodb"], "{tree}");
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
