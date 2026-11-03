//! An around node end to end: the reader finds its `impl Around`, a
//! placement that keeps both halves with the outer end checks, generates,
//! builds, and runs in two segments, and one that puts the node in a
//! segment of its own is refused before anything is generated.

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::mpsc;
use std::time::Duration;

use common::{Running, cargo, generated, root, target};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/around")
}

#[test]
fn the_reader_finds_an_around_nodes_role_and_ports() {
    let krate = gdl_gen::Crate::Path(fixture().join("nodes"));
    let read = gdl_gen::extract(
        &krate,
        &target("gdl-gen-tests"),
        &gdl_extract::Cargo::trusted(),
    )
    .expect("extracted");
    let keep = read
        .offer
        .nodes
        .iter()
        .find(|node| node.name == "Keep")
        .expect("Keep");
    assert_eq!(keep.role, gdl_model::Role::Around);
    let ports: Vec<&str> = keep.ports.iter().map(|port| port.port.as_str()).collect();
    assert_eq!(ports, ["UpReq", "UpItem", "DownReq", "DownItem"]);
    gdl_gen::verify(&read, &target("gdl-gen-tests")).expect("the build agrees");
}

#[test]
fn a_placement_that_splits_an_around_node_is_refused_at_check() {
    let problems = gdl_gen::check_files(
        &fixture().join("split.toml"),
        None,
        &target("gdl-gen-tests"),
    );
    let found: Vec<String> = problems.0.iter().map(ToString::to_string).collect();
    assert!(
        found.iter().any(|p| p.contains("around node `Keep`")),
        "{found:?}"
    );
}

/// Builds two segment binaries: the first run takes as long as the chat
/// example's build, whose target directory it shares, and later runs a
/// few seconds.
#[test]
fn an_around_node_keeps_its_value_across_two_generated_segments() {
    let spec_file = fixture().join("gdl.toml");
    let problems = gdl_gen::check_files(&spec_file, None, &target("gdl-gen-tests"));
    assert!(problems.is_empty(), "{:?}", problems.0);

    let temp = tempfile::Builder::new()
        .prefix("gdl-around-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    let spec = gdl_gen::Spec::load(&spec_file).expect("spec");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
    let contract = std::fs::read_to_string(out.join("contract/src/lib.rs")).expect("contract");
    assert!(
        contract.contains("topology: Front => Keep { Echo };"),
        "the contract nests the around node's hop: {contract}"
    );
    std::fs::write(
        out.join("Cargo.toml"),
        "[workspace]\nmembers = [\"back\", \"contract\", \"front\"]\nresolver = \"3\"\n",
    )
    .expect("workspace");
    std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
    let build = cargo()
        .arg("build")
        .arg("--manifest-path")
        .arg(out.join("Cargo.toml"))
        .arg("--bins")
        .env("CARGO_TARGET_DIR", target("examples"))
        .output()
        .expect("cargo build");
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );
    let bin = target("examples").join("debug");

    let roster = temp.path().join("roster");
    std::fs::create_dir_all(&roster).expect("roster");
    let config = |name: &str| {
        let path = temp.path().join(format!("{name}.toml"));
        let text = format!(
            "listen = \"127.0.0.1:0\"\nroster = {}\n",
            toml::Value::String(roster.display().to_string())
        );
        std::fs::write(&path, text).expect("config");
        path
    };
    let back = config("back");
    let front = config("front");
    let _back = Running(
        generated(bin.join("around-back"))
            .arg(&back)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("back"),
    );
    let mut front = Running(
        generated(bin.join("around-front"))
            .arg(&front)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("front"),
    );
    let mut stdin = front.0.stdin.take().expect("stdin");
    for request in [r#"{"text": "hi"}"#, r#"{"text": "yo"}"#] {
        writeln!(stdin, "{request}").expect("request");
    }
    // End of input: the front answers what it has read, then exits.
    drop(stdin);
    let lines = front.stdout().rest(Duration::from_secs(60));
    let status = front.exits_within(Duration::from_secs(10));
    let (tx, rx) = mpsc::channel();
    if let Some(mut err) = front.0.stderr.take() {
        std::thread::spawn(move || {
            let mut text = String::new();
            let _ = std::io::Read::read_to_string(&mut err, &mut text);
            let _ = tx.send(text);
        });
    }
    let stderr = rx.recv_timeout(Duration::from_secs(5)).unwrap_or_default();
    assert!(status.success(), "the front failed: {stderr}");
    assert_eq!(
        lines,
        [
            r#"{"item":{"text":"echo hi [kept hi]"}}"#,
            r#"{"done":null}"#,
            r#"{"item":{"text":"echo yo [kept yo]"}}"#,
            r#"{"done":null}"#,
        ],
        "front stderr: {stderr}"
    );
}
