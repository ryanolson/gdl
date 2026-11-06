//! What the etcd directory links, and who links it. The etcd client brings
//! tonic and prost, and its build compiles protos with `protoc`, so it
//! stays behind this crate's `client` feature, off by default: a build of
//! the workspace needs no `protoc`. The one crate that turns it on is
//! opt-in: `gdl-hub`'s `etcd` feature, off by default, with which the stock
//! hub binary publishes to etcd. Slurm images build the hub with it.

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

/// The crates whose build or code is the etcd client's: the client, its
/// protobuf runtime and its proto compiler's build step, which runs
/// `protoc`, and the gRPC stack.
const ETCD: [&str; 6] = [
    "etcd-client ",
    "gdl-pools-etcd ",
    "prost",
    "protoc",
    "tonic",
    "tonic-prost-build ",
];

/// The lines of `linked` that name one of [`ETCD`]'s crates.
fn etcd(linked: &str) -> Vec<&str> {
    linked
        .lines()
        .filter(|line| ETCD.iter().any(|name| line.starts_with(name)))
        .collect()
}

/// A build of the workspace with its default features links nothing of
/// the etcd client, so it needs no `protoc`: this crate's `client` feature,
/// off by default, holds the client.
#[test]
fn a_default_workspace_build_needs_no_protoc() {
    let linked = tree(&["--workspace", "--edges", "normal,build,dev"]);
    let found: Vec<&str> = etcd(&linked)
        .into_iter()
        .filter(|line| !line.starts_with("gdl-pools-etcd "))
        .collect();
    assert!(
        found.is_empty(),
        "a default workspace build links {found:?}"
    );
}

/// With every feature on, the client is this crate's alone, and the hub's
/// `etcd` feature is the one crate outside it that depends on this crate.
#[test]
fn only_this_crate_links_the_etcd_client() {
    let direct = ["--all-features", "--depth", "1"];
    let (crates, tree) = dependents("etcd-client", &direct);
    assert_eq!(crates, ["gdl-pools-etcd"], "{tree}");
    let (crates, tree) = dependents("gdl-pools-etcd", &direct);
    assert_eq!(crates, ["gdl-hub", "gdl-pools-etcd"], "{tree}");
}

/// A default build of the stock hub links no etcd or protobuf code: its
/// `etcd` feature is off by default, and turns the client on.
#[test]
fn a_default_hub_links_no_etcd_or_protobuf_code() {
    let linked = tree(&["--package", "gdl-hub", "--edges", "normal,build"]);
    let found = etcd(&linked);
    assert!(found.is_empty(), "a default gdl-hub links {found:?}");
    let featured = tree(&[
        "--package",
        "gdl-hub",
        "--features",
        "etcd",
        "--edges",
        "normal",
    ]);
    assert!(
        featured
            .lines()
            .any(|line| line.starts_with("etcd-client ")),
        "the `etcd` feature links the client: {featured}"
    );
}

/// A pool directory backend needs only the trait: no velo, as for
/// `gdl-pools`, and no TLS stack, since the members are reached over plain
/// HTTP/2.
#[test]
fn the_crate_links_no_transport_and_no_tls() {
    let linked = tree(&[
        "--package",
        "gdl-pools-etcd",
        "--features",
        "client",
        "--edges",
        "normal",
    ]);
    let unwanted: Vec<&str> = linked
        .lines()
        .filter(|line| {
            [
                "velo",
                "aws-lc",
                "rustls ",
                "ring ",
                "openssl",
                "native-tls",
            ]
            .iter()
            .any(|name| line.starts_with(name))
        })
        .collect();
    assert!(unwanted.is_empty(), "gdl-pools-etcd links {unwanted:?}");
}
