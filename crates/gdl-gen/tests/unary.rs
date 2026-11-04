//! Unary nodes through the generator: `cargo gdl check` and `generate`
//! refuse each misuse of a unary node or a `calls` clause, naming the
//! node and the clause, and a graph with a unary node beside its caller
//! generates segments that build. The graph is the around fixture's, with
//! a unary node `Tok` that `Keep` calls.

mod common;

use std::path::{Path, PathBuf};

use common::{cargo, root, target};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/unary")
}

/// A spec of the fixture's graph, or of `graph` when given, with
/// `placement` after it, written to a directory of its own.
struct Files {
    dir: tempfile::TempDir,
}

impl Files {
    fn new(graph: Option<&str>, placement: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-unary-")
            .tempdir()
            .expect("a directory");
        let around = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/around");
        let graph_path = match graph {
            None => fixture().join("unary.gdl"),
            Some(text) => {
                let path = dir.path().join("unary.gdl");
                std::fs::write(&path, text).expect("the graph");
                path
            }
        };
        let quoted = |p: &Path| format!("{:?}", p.display().to_string());
        let spec = format!(
            "[graph]\npath = {graph}\n[messages]\ncrates = [{protocols}]\n\
             [nodes]\ncrates = [{nodes}]\n[gdl]\npath = {gdl}\n{placement}",
            graph = quoted(&graph_path),
            protocols = quoted(&around.join("protocols")),
            nodes = quoted(&around.join("nodes")),
            gdl = quoted(&root().join("crates/gdl-runtime")),
        );
        std::fs::write(dir.path().join("gdl.toml"), spec).expect("the spec");
        Self { dir }
    }

    fn spec(&self) -> PathBuf {
        self.dir.path().join("gdl.toml")
    }

    /// Each problem `check` finds, with its table and key.
    fn problems(&self) -> Vec<(Option<String>, Option<String>, String)> {
        gdl_gen::check_files(&self.spec(), None, &target("gdl-gen-tests"))
            .0
            .into_iter()
            .map(|problem| (problem.table, problem.key, problem.message))
            .collect()
    }
}

fn graph(change: &dyn Fn(String) -> String) -> String {
    change(std::fs::read_to_string(fixture().join("unary.gdl")).expect("the graph"))
}

const LOCAL: &str = "[segment.front]\nnodes = [\"Front\", \"Keep\", \"Tok\"]\n\
    [segment.back]\nnodes = [\"Echo\"]\n";

/// `Tok` in a segment of its own, beside the sink's.
const SPLIT: &str = "[segment.front]\nnodes = [\"Front\", \"Keep\"]\n\
    [segment.back]\nnodes = [\"Echo\"]\n[segment.tok]\nnodes = [\"Tok\"]\n";

#[test]
fn a_unary_node_beside_its_caller_or_in_its_own_segment_checks_clean() {
    for placement in [
        LOCAL.to_string(),
        SPLIT.to_string(),
        format!("{SPLIT}[routing]\nTok = \"random\"\n"),
        // A unary node may be in several segments.
        format!("{SPLIT}[segment.more]\nnodes = [\"Tok\"]\n")
            .replace("\"Front\", \"Keep\"", "\"Front\", \"Keep\", \"Tok\""),
    ] {
        let files = Files::new(None, &placement);
        assert_eq!(files.problems(), [], "{placement}");
    }
}

/// A graph to write in place of the fixture's, a placement, the table and
/// key a problem names, and what it says.
type Case<'a> = (
    Option<String>,
    &'a str,
    Option<&'a str>,
    Option<&'a str>,
    &'a str,
);

/// Each refusal, at the table and key to change. A problem of the graph
/// alone names no placement table.
#[test]
fn check_refuses_each_misuse_naming_the_node_and_the_clause() {
    let cases: [Case<'_>; 9] = [
        (
            Some(graph(&|g| g.replace("calls Tok", "calls Tok, Missing"))),
            LOCAL,
            None,
            None,
            "node `Keep` has `calls Missing`, but the graph declares no unary node `Missing`",
        ),
        (
            Some(graph(&|g| {
                g.replace(
                    "Text -> Text idempotent;",
                    "Text -> Text idempotent calls Tok;",
                )
            })),
            LOCAL,
            None,
            None,
            "the `calls` clauses make a cycle: `Tok` calls `Tok`",
        ),
        (
            Some(graph(&|g| {
                g.replace("Keep { Echo }", "Keep { Tok => Echo }")
            })),
            LOCAL,
            None,
            None,
            "unary node `Tok` is in the topology",
        ),
        (
            Some(graph(&|g| {
                g.replace("schema = 1;", "schema = 1;\nproperty model: Text;")
                    .replace(
                        "Text -> Text idempotent;",
                        "Text -> Text idempotent provides model;",
                    )
            })),
            LOCAL,
            None,
            None,
            "unary node `Tok` has `provides model`; a unary node cannot provide or need a property",
        ),
        (
            Some(graph(&|g| {
                g.replace("schema = 1;", "schema = 1;\nproperty model: Text;")
                    .replace(
                        "Text -> Text idempotent;",
                        "Text -> Text idempotent needs model from sets;",
                    )
            })),
            LOCAL,
            None,
            None,
            "unary node `Tok` has `needs model from sets`; a unary node cannot provide or need a property",
        ),
        (
            Some(graph(&|g| g.replace("calls Tok", "calls Tok, Echo"))),
            LOCAL,
            None,
            None,
            "node `Keep` has `calls Echo`, but `Echo` is a sink in the topology",
        ),
        (
            None,
            &LOCAL.replace("[\"Echo\"]", "[\"Echo\", \"Keep\"]"),
            Some("segment"),
            None,
            "node `Keep` is in several segments: [\"back\", \"front\"]; only a unary node may be",
        ),
        (
            None,
            &format!(
                "{SPLIT}[class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"back\", \"tok\"]\n"
            ),
            Some("segment.front"),
            Some("nodes"),
            "node `Keep` in segment `front` of class `global` has `calls Tok`, but the only copy of `Tok` is in class `model_x`",
        ),
        (
            None,
            &format!("{SPLIT}[segment.more]\nnodes = [\"Tok\"]\n"),
            Some("segment.front"),
            Some("nodes"),
            "node `Keep` in segment `front` has `calls Tok`, but `front` lists no copy of `Tok`, and copies sit in several segments: [\"more\", \"tok\"]",
        ),
    ];
    for (graph, placement, table, key, why) in cases {
        let files = Files::new(graph.as_deref(), placement);
        let problems = files.problems();
        let found = problems
            .iter()
            .find(|(_, _, message)| message.contains(why));
        let Some((at_table, at_key, _)) = found else {
            panic!("{why}\nnot in: {problems:#?}");
        };
        assert_eq!(
            (at_table.as_deref(), at_key.as_deref()),
            (table, key),
            "{why}"
        );
    }
}

#[test]
fn a_routing_key_names_a_remote_runs_first_node_or_a_unary_node() {
    let files = Files::new(None, &format!("{SPLIT}[routing]\nKeep = \"random\"\n"));
    let problems = files.problems();
    assert_eq!(
        problems,
        [(
            Some("routing".to_string()),
            Some("Keep".to_string()),
            "request router `Keep` must target a request boundary or a unary node".to_string()
        )]
    );
}

/// The contract crate prints the graph from the checked model: the unary
/// node with its clause, and the caller's `calls`.
#[test]
fn the_contract_prints_the_unary_node_and_its_calls() {
    let spec = gdl_gen::Spec::load(&fixture().join("gdl.toml")).expect("spec");
    let temp = tempfile::Builder::new()
        .prefix("gdl-unary-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
    let contract = std::fs::read_to_string(out.join("contract/src/lib.rs")).expect("contract");
    assert!(
        contract.contains(
            "unary    Tok: ::around_protocols::Text -> ::around_protocols::Text idempotent;"
        ),
        "{contract}"
    );
    assert!(
        contract.contains(
            "around   Keep: (::around_protocols::Text -> stream ::around_protocols::Text) over (::around_protocols::Text -> stream ::around_protocols::Text) calls Tok;"
        ),
        "{contract}"
    );
}

/// `generate` runs the same checks as `check`, and writes nothing.
#[test]
fn generate_refuses_what_check_refuses() {
    let files = Files::new(
        None,
        &format!(
            "{SPLIT}[class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"back\", \"tok\"]\n"
        ),
    );
    let out = files.dir.path().join("out");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "generate", "--spec"])
        .arg(files.spec())
        .arg("--out")
        .arg(&out)
        .arg("--target-dir")
        .arg(target("gdl-gen-tests"))
        .stdin(std::process::Stdio::null())
        .output()
        .expect("cargo gdl");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("node `Keep` in segment `front` of class `global` has `calls Tok`"),
        "{stderr}"
    );
    assert!(!out.join("front").exists(), "generate wrote a segment");
}

/// The segments of a graph with a unary node build: the unary node takes
/// no slot, and its caller's segment lists it. Shares the examples' target
/// directory with the around test, so after the first build of either it
/// takes a few seconds.
#[test]
fn a_graph_with_a_unary_node_generates_segments_that_build() {
    let spec_file = fixture().join("gdl.toml");
    let problems = gdl_gen::check_files(&spec_file, None, &target("gdl-gen-tests"));
    assert!(problems.is_empty(), "{:?}", problems.0);

    let temp = tempfile::Builder::new()
        .prefix("gdl-unary-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    let spec = gdl_gen::Spec::load(&spec_file).expect("spec");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
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
        .stdin(std::process::Stdio::null())
        .output()
        .expect("cargo build");
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );
}

/// A generated segment binds no implementation of a unary node, so a pin
/// that would choose one is refused rather than dropped.
#[test]
fn a_pin_for_a_unary_node_is_refused() {
    let files = Files::new(
        None,
        &format!("[nodes.pin]\nTok = \"around_nodes::Keep\"\n{LOCAL}"),
    );
    let problems = files.problems();
    assert!(
        problems.iter().any(|(_, _, message)| message.contains(
            "[nodes] pin.Tok: unary node `Tok` takes no implementation in a generated segment"
        )),
        "{problems:#?}"
    );
}

#[test]
fn remote_call_types_are_asserted_by_the_generated_segments() {
    let graph = graph(&|g| g.replace("Text -> Text idempotent", "CallOnly -> CallOnly idempotent"));
    let files = Files::new(Some(&graph), SPLIT);
    let protocols = files.dir.path().join("protocols");
    std::fs::create_dir_all(protocols.join("src")).expect("protocol sources");
    std::fs::write(
        protocols.join("Cargo.toml"),
        format!(
            "[package]\nname = 'call-protocols'\nversion = '0.0.0'\nedition = '2024'\n\
             [workspace]\n[dependencies]\nserde = {{ version = '1', features = ['derive'] }}\n\
             gdl-message = {{ path = {:?} }}\n",
            root().join("crates/gdl-message")
        ),
    )
    .expect("protocol manifest");
    // A remote derive provides inherent helpers, not serde trait impls.
    // The source reader still reports the non-generic message as wire.
    std::fs::write(
        protocols.join("src/lib.rs"),
        "#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]\n\
         #[serde(remote = \"Self\")]\npub struct CallOnly { pub text: String }\n",
    )
    .expect("protocol source");
    let spec = std::fs::read_to_string(files.spec()).expect("spec");
    let around = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/around/protocols");
    let spec = spec.replace(
        &format!("crates = [{:?}]", around.display().to_string()),
        &format!(
            "crates = [{:?}, {:?}]",
            around.display().to_string(),
            protocols.display().to_string()
        ),
    );
    std::fs::write(files.spec(), spec).expect("spec with call protocols");
    assert_eq!(files.problems(), [], "source check accepts the remote call");

    let out = files.dir.path().join("segments");
    let spec = gdl_gen::Spec::load(&files.spec()).expect("spec");
    let generated = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &generated).expect("write");
    std::fs::write(
        out.join("Cargo.toml"),
        "[workspace]\nmembers = ['back', 'contract', 'front', 'tok']\nresolver = '3'\n",
    )
    .expect("generated workspace");
    std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
    std::fs::create_dir_all(out.join("contract/examples")).expect("contract examples");
    std::fs::write(
        out.join("contract/examples/wire.rs"),
        "fn main() {\n\
         let graph = unary_contract::manifest();\n\
         let unary = graph.node(\"Tok\").unwrap().unary.as_ref().unwrap();\n\
         assert!(!unary.req.wire && !unary.resp.wire);\n\
         }\n",
    )
    .expect("compiled manifest control");
    let run = |args: &[&str]| {
        std::process::Command::new("timeout")
            .arg("300")
            .arg(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .args(args)
            .arg("--manifest-path")
            .arg(out.join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("examples"))
            .stdin(std::process::Stdio::null())
            .output()
            .expect("bounded cargo command")
    };
    let control = run(&["run", "-p", "unary-contract", "--example", "wire"]);
    assert!(
        control.status.success(),
        "compiled manifest control: {}",
        String::from_utf8_lossy(&control.stderr)
    );
    let built = run(&["build", "--bins"]);
    assert_ne!(built.status.code(), Some(124), "build timed out");
    assert!(
        !built.status.success(),
        "remote calls with non-wire request and response types built successfully; generated segments must assert their serde traits"
    );
    let stderr = String::from_utf8_lossy(&built.stderr);
    assert!(
        stderr.contains("Serialize") || stderr.contains("Deserialize"),
        "{stderr}"
    );
    // The segment's own assertion fails, at the alias that names the call.
    assert!(
        stderr.contains("is carried by a call to another segment but is not a wire type"),
        "{stderr}"
    );
    assert!(stderr.contains("Keep_calls_Tok_req"), "{stderr}");
}
