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

/// A pin chooses a unary node's implementation as it does any node's, and
/// is held to the node's role.
#[test]
fn a_pin_for_a_unary_node_is_held_to_its_role() {
    let files = Files::new(
        None,
        &format!("[nodes.pin]\nTok = \"around_nodes::Keep\"\n{LOCAL}"),
    );
    let problems = files.problems();
    assert!(
        problems.iter().any(|(_, _, message)| message.contains(
            "node `Tok`: the graph declares a unary, but `around_nodes::Keep` is a around"
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
    // `Tok` of the call's own types, beside the around fixture's `Tok`.
    let nodes = files.dir.path().join("nodes");
    std::fs::create_dir_all(nodes.join("src")).expect("node sources");
    std::fs::write(
        nodes.join("Cargo.toml"),
        format!(
            "[package]\nname = 'call-nodes'\nversion = '0.0.0'\nedition = '2024'\n\
             [workspace]\n[dependencies]\ncall-protocols = {{ path = {:?} }}\n\
             gdl = {{ path = {:?} }}\n",
            protocols,
            root().join("crates/gdl")
        ),
    )
    .expect("node manifest");
    std::fs::write(
        nodes.join("src/lib.rs"),
        "use call_protocols::CallOnly;\n\
         #[derive(gdl::Node)]\npub struct Tok;\n\
         impl gdl::Build for Tok {\n    type Config = ();\n    type Metrics = ();\n    \
         async fn build((): (), (): ()) -> gdl::Result<Self> { Ok(Self) }\n}\n\
         impl gdl::Unary for Tok {\n    type Req = CallOnly;\n    type Resp = CallOnly;\n    \
         async fn call(&self, _cx: &gdl::Cx, req: CallOnly) -> gdl::Result<CallOnly> { Ok(req) }\n}\n",
    )
    .expect("node source");
    let around_nodes = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/around/nodes");
    let spec = spec.replace(
        &format!("crates = [{:?}]", around_nodes.display().to_string()),
        &format!(
            "crates = [{:?}, {:?}]\npin = {{ Tok = \"call_nodes::Tok\" }}",
            around_nodes.display().to_string(),
            nodes.display().to_string()
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

/// A contract spec's binding of a unary node reaches the segment that
/// hosts it, as any node's binding does, rather than being dropped: a type
/// that does not exist fails that segment's build at the binding. As for
/// every node of a contract spec, `check` does not read the bound crate.
#[test]
fn a_contract_specs_binding_of_a_unary_node_reaches_its_segment() {
    let files = Files::new(None, LOCAL);
    let out = files.dir.path().join("segments");
    let graph_spec = gdl_gen::Spec::load(&files.spec()).expect("graph spec");
    let generated = gdl_gen::render(&graph_spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &generated).expect("write");
    std::fs::write(
        out.join("Cargo.toml"),
        "[workspace]\nmembers = ['back', 'contract', 'front']\nresolver = '3'\n",
    )
    .expect("generated workspace");
    std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");

    let nodes = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/around/nodes");
    let mut text = format!(
        "[contract]\npath = {:?}\n[gdl]\npath = {:?}\n{LOCAL}",
        out.join("contract"),
        root().join("crates/gdl-runtime")
    );
    for (node, ty) in [("Keep", "Keep"), ("Echo", "Echo"), ("Tok", "MissingUnary")] {
        text.push_str(&format!(
            "[bind.{node}]\ncrate = 'around-nodes'\npath = {nodes:?}\ntype = 'around_nodes::{ty}'\n"
        ));
    }
    let spec_path = files.dir.path().join("contract.toml");
    std::fs::write(&spec_path, text).expect("contract spec");
    let spec = gdl_gen::Spec::load(&spec_path).expect("contract spec");
    let bound = files.dir.path().join("bound");
    let rendered = gdl_gen::render(&spec, &bound, &target("examples")).expect("rendered");
    let front = &rendered[&PathBuf::from("front/src/lib.rs")];
    assert!(
        front.contains("unary Tok tok = ::around_nodes::MissingUnary;"),
        "{front}"
    );
    gdl_gen::write(&bound, &rendered).expect("contract spec output");
    std::fs::write(
        bound.join("Cargo.toml"),
        "[workspace]\nmembers = ['back', 'front']\nresolver = '3'\n",
    )
    .expect("bound workspace");
    std::fs::copy(root().join("Cargo.lock"), bound.join("Cargo.lock")).expect("lockfile");
    let built = std::process::Command::new("timeout")
        .arg("300")
        .arg(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args(["build", "--bins", "--manifest-path"])
        .arg(bound.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target("examples"))
        .stdin(std::process::Stdio::null())
        .output()
        .expect("bounded segment build");
    assert_ne!(built.status.code(), Some(124), "the build timed out");
    assert!(!built.status.success(), "the front built without `Tok`");
    let stderr = String::from_utf8_lossy(&built.stderr);
    assert!(stderr.contains("MissingUnary"), "{stderr}");
}

/// The `calls` fixture: a router that calls a tokenizer, in one spec with
/// two layouts.
fn calls() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/calls")
}

#[test]
fn the_reader_finds_a_unary_nodes_role_and_ports() {
    let krate = gdl_gen::Crate::Path(calls().join("nodes"));
    let read = gdl_gen::extract(
        &krate,
        &target("gdl-gen-tests"),
        &gdl_extract::Cargo::trusted(),
    )
    .expect("extracted");
    let tokenize = read
        .offer
        .nodes
        .iter()
        .find(|node| node.name == "Tokenize")
        .expect("Tokenize");
    assert_eq!(tokenize.role, gdl_model::Role::Unary);
    let ports: Vec<&str> = tokenize
        .ports
        .iter()
        .map(|port| port.port.as_str())
        .collect();
    assert_eq!(ports, ["Req", "Resp"]);
    gdl_gen::verify(&read, &target("gdl-gen-tests")).expect("the build agrees");
}

/// A workspace in the target directory with the `calls` fixture's
/// generated crates, every layout, and the demos that run the compact
/// layout's segment and the copies layout's two under a counting
/// allocator; and the lock that keeps
/// another test process out of it while held. Files are written only when
/// they change, so Cargo rebuilds only what changed since the last run.
/// The first build takes as long as the examples' target directory needs;
/// later ones a few seconds.
struct Workspace {
    _lock: std::fs::File,
    dir: PathBuf,
}

impl Workspace {
    fn built() -> Self {
        let dir = target("calls-workspace");
        std::fs::create_dir_all(&dir).expect("the workspace's directory");
        let lock = std::fs::File::create(target("calls-workspace.lock")).expect("the lock");
        lock.lock().expect("hold the lock");
        let spec = gdl_gen::Spec::load(&calls().join("gdl.toml")).expect("spec");
        let segments = dir.join("segments");
        let files = gdl_gen::render(&spec, &segments, &target("gdl-gen-tests")).expect("render");
        for (path, text) in &files {
            write(&segments.join(path), text);
        }
        let quoted = |p: &Path| toml::Value::String(p.display().to_string());
        for (from, to) in [
            ("main.rs", "src/main.rs"),
            ("copies-front.rs", "src/bin/copies-front.rs"),
            ("copies-pool.rs", "src/bin/copies-pool.rs"),
        ] {
            let demo = std::fs::read_to_string(calls().join("demo").join(from)).expect("a demo");
            write(&dir.join("demo").join(to), &demo);
        }
        write(
            &dir.join("demo/Cargo.toml"),
            &format!(
                "[package]\nname = \"calls-demo\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n\
                 [dependencies]\n\
                 calls-compact-front = {{ path = \"../segments/compact/front\" }}\n\
                 calls-copies-front = {{ path = \"../segments/copies/front\" }}\n\
                 calls-copies-pool = {{ path = \"../segments/copies/pool\" }}\n\
                 calls-nodes = {{ path = {} }}\n\
                 gdl-runtime = {{ path = {} }}\n",
                quoted(&calls().join("nodes")),
                quoted(&root().join("crates/gdl-runtime")),
            ),
        );
        write(
            &dir.join("Cargo.toml"),
            "[workspace]\nresolver = \"3\"\nmembers = [\"demo\", \"segments/contract\", \"segments/compact/front\", \"segments/split/front\", \"segments/split/tok\", \"segments/copies/front\", \"segments/copies/pool\"]\n",
        );
        if !dir.join("Cargo.lock").exists() {
            std::fs::copy(root().join("Cargo.lock"), dir.join("Cargo.lock")).expect("the seed");
        }
        let build = std::process::Command::new("timeout")
            .arg("600")
            .arg(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .args(["build", "--bins", "--manifest-path"])
            .arg(dir.join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("examples"))
            .stdin(std::process::Stdio::null())
            .output()
            .expect("cargo build");
        assert!(
            build.status.success(),
            "{}",
            String::from_utf8_lossy(&build.stderr)
        );
        Self { _lock: lock, dir }
    }

    /// The path of built binary `name`.
    fn bin(&self, name: &str) -> PathBuf {
        let _ = &self.dir;
        target("examples").join("debug").join(name)
    }
}

/// Write `text` to `path` unless it already holds it, so that its time,
/// and Cargo's fingerprint, stays.
fn write(path: &Path, text: &str) {
    if std::fs::read_to_string(path).is_ok_and(|existing| existing == text) {
        return;
    }
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("a directory");
    std::fs::write(path, text).expect("a file");
}

/// The requests each layout answers, and the answers.
const ASKED: [&str; 3] = [
    r#"{"text": "hi"}"#,
    r#"{"text": "hi there"}"#,
    r#"{"text": "one two three"}"#,
];
const ANSWERED: [&str; 6] = [
    r#"{"item":{"text":"echo hi [1 words]"}}"#,
    r#"{"done":null}"#,
    r#"{"item":{"text":"echo hi there [2 words]"}}"#,
    r#"{"done":null}"#,
    r#"{"item":{"text":"echo one two three [3 words]"}}"#,
    r#"{"done":null}"#,
];

/// One run of segment binary `front` with the other segments in `others`,
/// each with `settings` after its roster: the front's answer to each line
/// of `asked`, and its exit status and stderr.
struct Run {
    lines: Vec<String>,
    status: std::process::ExitStatus,
    stderr: String,
    /// The other segments, still running, each with its stderr.
    others: Vec<(common::Running, common::Lines)>,
    /// Their roster and settings, which they read until they stop.
    _temp: tempfile::TempDir,
}

fn run(front: &Path, others: &[PathBuf], settings: &str, asked: &[&str]) -> Run {
    use std::io::Write as _;
    use std::process::Stdio;
    use std::time::Duration;

    let temp = tempfile::Builder::new()
        .prefix("gdl-calls-")
        .tempdir()
        .expect("scratch");
    let roster = temp.path().join("roster");
    std::fs::create_dir_all(&roster).expect("roster");
    let config = |name: &str| {
        let path = temp.path().join(format!("{name}.toml"));
        let text = format!(
            "listen = \"127.0.0.1:0\"\nroster = {}\n{settings}",
            toml::Value::String(roster.display().to_string())
        );
        std::fs::write(&path, text).expect("config");
        path
    };
    let others: Vec<(common::Running, common::Lines)> = others
        .iter()
        .enumerate()
        .map(|(index, bin)| {
            let mut other = common::Running(
                common::generated(bin)
                    .arg(config(&format!("other-{index}")))
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("a segment"),
            );
            let stderr = other.stderr();
            (other, stderr)
        })
        .collect();
    let mut front = common::Running(
        common::generated(front)
            .arg(config("front"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the front"),
    );
    let mut stdin = front.0.stdin.take().expect("stdin");
    for request in asked {
        writeln!(stdin, "{request}").expect("request");
    }
    // End of input: the front answers what it has read, then exits.
    drop(stdin);
    let stderr = front.stderr();
    let lines = front.stdout().rest(Duration::from_secs(60));
    let status = front.exits_within(Duration::from_secs(10));
    let stderr = stderr.rest(Duration::from_secs(5)).join("\n");
    Run {
        lines,
        status,
        stderr,
        others,
        _temp: temp,
    }
}

/// The compact layout's front answers each request through the tokenizer
/// in its own segment.
#[test]
fn the_compact_layout_calls_the_service_in_the_routers_segment() {
    let workspace = Workspace::built();
    let ran = run(&workspace.bin("calls-compact-front"), &[], "", &ASKED);
    assert!(ran.status.success(), "the front failed: {}", ran.stderr);
    assert_eq!(ran.lines, ANSWERED, "front stderr: {}", ran.stderr);
}

/// The split layout's front answers the same requests the same, from the
/// same node code, through the tokenizer in a segment of its own: each
/// call is one velo call to its `gdl.call` handler. The front's barrier
/// waits for the tokenizer's segment, which it sends calls to.
#[test]
fn the_split_layout_calls_the_service_in_its_own_segment_and_answers_the_same() {
    let workspace = Workspace::built();
    let ran = run(
        &workspace.bin("calls-split-front"),
        &[workspace.bin("calls-split-tok")],
        "",
        &ASKED,
    );
    assert!(ran.status.success(), "the front failed: {}", ran.stderr);
    assert_eq!(ran.lines, ANSWERED, "front stderr: {}", ran.stderr);
}

/// A call from the router's build fails the build, whether the service is
/// in its segment, built but not ready, or in another, whose link binds
/// only when the segment starts.
#[test]
fn a_call_during_the_build_fails_the_segment_with_routing() {
    let workspace = Workspace::built();
    // The same in both layouts: a local copy, and a link to another segment.
    for (front, others) in [
        ("calls-compact-front", vec![]),
        ("calls-split-front", vec![workspace.bin("calls-split-tok")]),
    ] {
        let ran = run(
            &workspace.bin(front),
            &others,
            "[node.Router]\ncall_in_build = true\n",
            &[],
        );
        assert!(!ran.status.success(), "{front} served: {:?}", ran.lines);
        assert!(
            ran.stderr
                .contains("front: building Router: routing error: called during build"),
            "{front}: {}",
            ran.stderr
        );
    }
}

/// A call from the router's `ready` waits for the tokenizer's own `ready`,
/// which runs once: the tokenizer answers only once it has run.
#[test]
fn a_call_from_ready_waits_for_the_services_ready() {
    let workspace = Workspace::built();
    let ran = run(
        &workspace.bin("calls-compact-front"),
        &[],
        "[node.Router]\ncall_in_ready = true\n",
        &ASKED,
    );
    assert!(ran.status.success(), "the front failed: {}", ran.stderr);
    assert_eq!(ran.lines, ANSWERED, "front stderr: {}", ran.stderr);
}

/// The compact layout's segment, under an allocator that counts: a call
/// to the service in the router's own segment allocates nothing of its
/// own, through the client the router took in its build.
#[test]
fn a_local_call_through_the_generated_segment_allocates_nothing() {
    let workspace = Workspace::built();
    let ran = run(&workspace.bin("calls-demo"), &[], "", &ASKED);
    assert!(ran.status.success(), "the demo failed: {}", ran.stderr);
    assert_eq!(ran.lines, ANSWERED, "demo stderr: {}", ran.stderr);
    // The router's three calls and the sink's three, all to the one copy.
    assert!(
        ran.stderr.contains("calls 6 allocated 0 copies [6]"),
        "{}",
        ran.stderr
    );
}

/// The copies layout lists the tokenizer in the router's segment and in
/// the sink's: each calls the copy in its own segment, with no allocation
/// of its own, and each copy answers only its own segment's calls. A call
/// to the other segment's copy would show as a count of 6 in one and 0 in
/// the other.
#[test]
fn each_caller_calls_the_copy_in_its_own_segment() {
    use std::time::Duration;

    let workspace = Workspace::built();
    let mut ran = run(
        &workspace.bin("copies-front"),
        &[workspace.bin("copies-pool")],
        "",
        &ASKED,
    );
    assert!(ran.status.success(), "the front failed: {}", ran.stderr);
    assert_eq!(ran.lines, ANSWERED, "front stderr: {}", ran.stderr);
    assert!(
        ran.stderr.contains("calls 3 allocated 0 copies [3]"),
        "{}",
        ran.stderr
    );
    let (pool, stderr) = &mut ran.others[0];
    pool.signal("-TERM");
    let status = pool.exits_within(Duration::from_secs(30));
    let stderr = stderr.rest(Duration::from_secs(5)).join("\n");
    assert!(status.success(), "the pool failed: {stderr}");
    assert!(
        stderr.contains("calls 3 allocated 0 copies [3]"),
        "{stderr}"
    );
}
