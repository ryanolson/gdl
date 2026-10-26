//! The chat example end to end: the committed segment crates are what the
//! generator produces from `examples/chat/gdl.toml`; each segment builds
//! with only the implementations it hosts; and the three segment binaries,
//! run as separate processes, answer requests.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use common::{Running, cargo, example, generated, root, target};

/// The spec's placement draws as a diagram: one cluster per segment, and
/// the three crossings (Truncate to Engine, Engine to Detokenize, Format to
/// Preprocess) marked.
#[test]
fn the_placed_graph_draws_as_a_diagram() {
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let target = target("gdl-gen-tests");
    let mermaid = gdl_gen::diagram(&spec, &target, gdl_gen::Format::Mermaid).expect("mermaid");
    for line in [
        r#"subgraph s0["engine"]"#,
        r#"subgraph s1["front"]"#,
        r#"subgraph s2["post"]"#,
        "linkStyle 4,5,7 stroke:#d62728,stroke-width:3px",
    ] {
        assert!(mermaid.contains(line), "{line} in:\n{mermaid}");
    }
    let dot = gdl_gen::diagram(&spec, &target, gdl_gen::Format::Dot).expect("dot");
    assert_eq!(dot.matches("subgraph cluster_").count(), 3, "{dot}");
    assert_eq!(dot.matches("penwidth=2").count(), 3, "{dot}");
}

#[test]
fn the_committed_segments_are_current() {
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let out = example().join("segments");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    let drift = gdl_gen::drift(&out, &files).expect("drift");
    assert!(
        drift.is_empty(),
        "regenerate with `cargo run -p gdl-gen -- generate --spec examples/chat/gdl.toml --out examples/chat/segments`:\n{}",
        drift.join("\n")
    );
}

#[test]
fn only_the_engine_segment_links_the_engine_dependency() {
    let manifest = example().join("Cargo.toml");
    for (package, links) in [
        ("chat-front", false),
        ("chat-engine", true),
        ("chat-post", false),
    ] {
        let output = cargo()
            .arg("tree")
            .arg("--manifest-path")
            .arg(&manifest)
            .args([
                "--package",
                package,
                "--edges",
                "normal,build",
                "--prefix",
                "none",
            ])
            .output()
            .expect("cargo tree");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let tree = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            tree.lines().any(|line| line.starts_with("chat-sentinel ")),
            links,
            "{package}:\n{tree}"
        );
    }
}

#[test]
fn three_segment_binaries_answer_requests() {
    let build = cargo()
        .arg("build")
        .arg("--manifest-path")
        .arg(example().join("Cargo.toml"))
        .arg("--bins")
        .env("CARGO_TARGET_DIR", target("examples"))
        .output()
        .expect("cargo build");
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(build.status.success(), "{stderr}");
    // Generated code is built by people who deny warnings.
    let warned: Vec<&str> = stderr
        .lines()
        .filter(|line| line.starts_with("warning:") && line.contains("generated"))
        .collect();
    assert!(
        warned.is_empty(),
        "generated crates warned: {warned:?}\n{stderr}"
    );
    let bin = target("examples").join("debug");

    let temp = tempfile::Builder::new()
        .prefix("gdl-chat-")
        .tempdir()
        .expect("scratch");
    let dir = temp.path();
    let roster = dir.join("roster");
    std::fs::create_dir_all(&roster).expect("roster");
    let config = |name: &str, extra: &str| {
        let path = dir.join(format!("{name}.toml"));
        let text = format!(
            "listen = \"127.0.0.1:0\"\nroster = {}\n{extra}",
            toml::Value::String(roster.display().to_string())
        );
        std::fs::write(&path, text).expect("config");
        path
    };
    // The engine loads asynchronously from its own settings; post formats
    // with its own. Each reaches only the segment that hosts its node.
    let engine = config("engine", "[node.Engine]\nload_ms = 200\nuppercase = true\n");
    let post = config("post", "[node.Format]\nopen = \"<\"\nclose = \">\"\n");
    let front = config("front", "[runtime]\ndefault_deadline_ms = 20000\n");

    let spawn = |name: &str, config: &Path, piped: bool| {
        let mut command = generated(bin.join(name));
        command.arg(config).stderr(Stdio::piped());
        if piped {
            command.stdin(Stdio::piped()).stdout(Stdio::piped());
        } else {
            command.stdin(Stdio::null()).stdout(Stdio::null());
        }
        Running(command.spawn().expect(name))
    };
    let _engine = spawn("chat-engine", &engine, false);
    let _post = spawn("chat-post", &post, false);
    let mut front = spawn("chat-front", &front, true);

    let mut stdin = front.0.stdin.take().expect("stdin");
    for request in [
        r#"{"text": "hello big wide world", "max_words": 3}"#,
        r#"{"text": "", "max_words": 3}"#,
        "not json",
        r#"{"text": "one", "max_words": 5}"#,
    ] {
        writeln!(stdin, "{request}").expect("request");
    }
    // End of input: the front answers what it has read, then exits.
    drop(stdin);

    let stdout = front.0.stdout.take().expect("stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let lines: Vec<String> = BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
            .collect();
        let _ = tx.send(lines);
    });
    let lines = match rx.recv_timeout(Duration::from_secs(120)) {
        Ok(lines) => lines,
        Err(_) => {
            drop(front);
            panic!("the front segment did not answer in time");
        }
    };
    let status = front.0.wait().expect("front");
    let mut stderr = String::new();
    if let Some(mut err) = front.0.stderr.take() {
        let _ = std::io::Read::read_to_string(&mut err, &mut stderr);
    }
    assert!(status.success(), "front failed: {stderr}");
    assert_eq!(
        lines,
        [
            r#"{"item":{"index":0,"text":"<WIDE>"}}"#,
            r#"{"item":{"index":1,"text":"<BIG>"}}"#,
            r#"{"item":{"index":2,"text":"<HELLO>"}}"#,
            r#"{"done":null}"#,
            r#"{"error":"node error: echo-reversed: nothing to answer"}"#,
            r#"{"error":"bad request: expected ident at line 1 column 2"}"#,
            r#"{"item":{"index":0,"text":"<ONE>"}}"#,
            r#"{"done":null}"#,
        ],
        "front stderr: {stderr}"
    );
}

/// `cargo gdl generate`, run as cargo runs it.
fn gdl_gen(dir: &Path, args: &[&std::ffi::OsStr]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .current_dir(dir)
        .args(["gdl", "generate"])
        .args(args)
        .arg("--target-dir")
        .arg(target("gdl-gen-tests"))
        .output()
        .expect("gdl-gen")
}

fn scratch(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("gdl-gen-{name}-"))
        .tempdir()
        .expect("scratch")
}

#[test]
fn the_check_does_not_depend_on_where_it_runs() {
    // A rustfmt config that would reformat everything, where the generator
    // runs: generated code must not pick it up.
    let temp = scratch("cwd");
    let dir = temp.path();
    std::fs::write(
        dir.join("rustfmt.toml"),
        "max_width = 40\nhard_tabs = true\n",
    )
    .expect("config");
    let spec = example().join("gdl.toml");
    let out = example().join("segments");
    let output = gdl_gen(
        dir,
        &[
            "--spec".as_ref(),
            spec.as_os_str(),
            "--out".as_ref(),
            out.as_os_str(),
            "--check".as_ref(),
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn the_check_reports_a_file_added_to_a_generated_crate() {
    let temp = scratch("foreign");
    let out = temp.path();
    let spec = example().join("gdl.toml");
    let generate = gdl_gen(
        &root(),
        &[
            "--spec".as_ref(),
            spec.as_os_str(),
            "--out".as_ref(),
            out.as_os_str(),
        ],
    );
    assert!(
        generate.status.success(),
        "{}",
        String::from_utf8_lossy(&generate.stderr)
    );
    let check = || {
        gdl_gen(
            &root(),
            &[
                "--spec".as_ref(),
                spec.as_os_str(),
                "--out".as_ref(),
                out.as_os_str(),
                "--check".as_ref(),
            ],
        )
    };
    assert!(check().status.success());
    // Cargo runs a build script it finds, marked or not.
    std::fs::write(out.join("front/build.rs"), "fn main() {}\n").expect("build script");
    let output = check();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unexpected: front/build.rs"), "{stderr}");
}

/// Generate the example into a scratch directory with the CLI.
fn generate_into(out: &Path, check: bool) -> std::process::Output {
    let spec = example().join("gdl.toml");
    let mut args: Vec<&std::ffi::OsStr> = vec![
        "--spec".as_ref(),
        spec.as_os_str(),
        "--out".as_ref(),
        out.as_os_str(),
    ];
    if check {
        args.push("--check".as_ref());
    }
    gdl_gen(&root(), &args)
}

#[test]
fn a_cache_marker_does_not_hide_a_generated_crate() {
    let temp = scratch("cachedir");
    let out = temp.path();
    assert!(generate_into(out, false).status.success());
    std::fs::write(
        out.join("front/CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55\n",
    )
    .expect("tag");
    std::fs::write(out.join("front/build.rs"), "fn main() {}\n").expect("build script");
    let output = generate_into(out, true);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("unexpected: front/build.rs"), "{stderr}");
}

#[test]
fn a_standalone_build_of_a_generated_crate_is_not_drift() {
    let temp = scratch("standalone");
    let out = temp.path();
    assert!(generate_into(out, false).status.success());
    // What building one generated crate on its own leaves behind: a
    // lockfile, and a build directory.
    let lock = cargo()
        .args(["generate-lockfile", "--offline", "--manifest-path"])
        .arg(out.join("front/Cargo.toml"))
        .output()
        .expect("cargo generate-lockfile");
    assert!(
        lock.status.success(),
        "{}",
        String::from_utf8_lossy(&lock.stderr)
    );
    assert!(out.join("front/Cargo.lock").exists());
    std::fs::create_dir_all(out.join("front/target/debug")).expect("dir");
    std::fs::write(
        out.join("front/target/CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55\n",
    )
    .expect("tag");
    std::fs::write(out.join("front/target/debug/chat-front"), "binary\n").expect("output");
    let check = generate_into(out, true);
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
    let regenerate = generate_into(out, false);
    assert!(
        regenerate.status.success(),
        "{}",
        String::from_utf8_lossy(&regenerate.stderr)
    );
}

/// A node that no listed crate offers, a pin to no offered node, and a
/// placement that leaves nodes out: all reported together.
#[test]
fn every_problem_in_a_spec_is_reported_at_once() {
    let temp = scratch("problems");
    let dir = temp.path();
    let example = example();
    let path = |p: &str| toml::Value::String(example.join(p).display().to_string());
    let gdl = toml::Value::String(root().join("crates/gdl-runtime").display().to_string());
    let text = format!(
        "[graph]\npath = {graph}\n[messages]\ncrates = [{protocols}]\n\
         [nodes]\ncrates = [{front}, {engine}]\npin = {{ Engine = \"chat_engine_mock::Engin\" }}\n\
         [gdl]\npath = {gdl}\n\
         [segment.front]\nnodes = [\"Frontend\", \"Preprocess\", \"Truncate\", \"Number\"]\n\
         [segment.engine]\nnodes = [\"Engine\"]\n",
        graph = path("chat.gdl"),
        protocols = path("protocols"),
        front = path("front-nodes"),
        engine = path("engine-mock"),
    );
    std::fs::write(dir.join("gdl.toml"), text).expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("the spec parses");
    let Err(error) = gdl_gen::render(&spec, &dir.join("segments"), &target("gdl-gen-tests")) else {
        panic!("a spec with four problems rendered");
    };
    let error = error.to_string();
    for expected in [
        // A pin to a type no node crate declares.
        "[nodes] pin.Engine: no [nodes] crate offers `chat_engine_mock::Engin`",
        // Nodes whose crate is not listed.
        "node `Detokenize`: no [nodes] crate offers it",
        "node `Format`: no [nodes] crate offers it",
        // A placement that leaves nodes out.
        "node `Detokenize` is in no segment",
    ] {
        assert!(
            error.contains(expected),
            "{expected:?} missing from:\n{error}"
        );
    }
}

#[test]
fn a_cache_marker_in_a_source_directory_hides_nothing() {
    let temp = scratch("srcbin");
    let out = temp.path();
    assert!(generate_into(out, false).status.success());
    let tag = "Signature: 8a477f597d28d172789f06886806bc55\n";
    // Cargo builds every file in `src/bin` as a binary, tag or not.
    std::fs::create_dir_all(out.join("front/src/bin")).expect("dir");
    std::fs::write(out.join("front/src/bin/CACHEDIR.TAG"), tag).expect("tag");
    std::fs::write(out.join("front/src/bin/extra.rs"), "fn main() {}\n").expect("binary");
    // A segment no longer in the placement, tagged.
    std::fs::create_dir_all(out.join("gone/src")).expect("dir");
    std::fs::write(out.join("gone/CACHEDIR.TAG"), tag).expect("tag");
    std::fs::write(
        out.join("gone/src/lib.rs"),
        "// @generated by gdl-gen; do not edit.\n",
    )
    .expect("stale");
    let output = generate_into(out, true);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains("unexpected: front/src/bin/extra.rs"),
        "{stderr}"
    );
    assert!(stderr.contains("stale: gone/src/lib.rs"), "{stderr}");
}

#[test]
fn the_spec_path_may_be_given_any_way() {
    // From the example's own directory: a bare file name, a `./` path, and
    // an absolute one must all find the committed output current.
    let dir = example();
    let spec_abs = dir.join("gdl.toml");
    for spec in [
        std::ffi::OsString::from("gdl.toml"),
        std::ffi::OsString::from("./gdl.toml"),
        spec_abs.into_os_string(),
    ] {
        let output = gdl_gen(
            &dir,
            &[
                "--spec".as_ref(),
                spec.as_os_str(),
                "--out".as_ref(),
                "segments".as_ref(),
                "--check".as_ref(),
            ],
        );
        assert!(
            output.status.success(),
            "--spec {spec:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// Rendering a graph spec, as `--check` does, and drawing it build the
/// manifest in memory and write nothing in the output.
#[test]
fn rendering_a_graph_spec_writes_nothing_in_the_output() {
    let temp = scratch("render-only");
    let out = temp.path();
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let target = target("gdl-gen-tests");
    let files = gdl_gen::render(&spec, out, &target).expect("render");
    assert!(files.contains_key(Path::new("contract/src/lib.rs")));
    gdl_gen::diagram(&spec, &target, gdl_gen::Format::Dot).expect("diagram");
    let written: Vec<_> = std::fs::read_dir(out).expect("out").collect();
    assert!(written.is_empty(), "{written:?}");
}

/// A segment named `contract` would share the generated contract's
/// directory. Refused whatever the graph's name: package names fold case,
/// and a graph named `Chat` once slipped past the package check and mixed
/// the two crates' files.
#[test]
fn a_segment_cannot_take_the_contract_directory() {
    let temp = scratch("contract-segment");
    let dir = temp.path();
    let example = example();
    let graph = std::fs::read_to_string(example.join("chat.gdl"))
        .expect("graph")
        .replace("name = \"chat\"", "name = \"Chat\"");
    std::fs::write(dir.join("chat.gdl"), graph).expect("graph");
    let spec = std::fs::read_to_string(example.join("gdl.toml"))
        .expect("spec")
        .replace("[segment.front]", "[segment.contract]")
        .replace(
            "crates = [\"protocols\"]",
            &format!("crates = [{:?}]", example.join("protocols")),
        )
        .replace(
            "crates = [\"front-nodes\", \"engine-mock\", \"post-nodes\"]",
            &format!(
                "crates = [{:?}, {:?}, {:?}]",
                example.join("front-nodes"),
                example.join("engine-mock"),
                example.join("post-nodes")
            ),
        )
        .replace(
            "path = \"../../crates/gdl-runtime\"",
            &format!("path = {:?}", root().join("crates/gdl-runtime")),
        );
    let spec = spec
        .lines()
        .map(|line| {
            match line
                .strip_prefix("path = \"")
                .and_then(|p| p.strip_suffix('"'))
            {
                Some(relative) if !relative.ends_with(".gdl") && !relative.starts_with('/') => {
                    format!("path = {:?}", example.join(relative))
                }
                _ => line.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(dir.join("gdl.toml"), spec).expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("spec");
    let error = gdl_gen::render(&spec, &dir.join("segments"), &target("gdl-gen-tests"))
        .expect_err("a segment in contract/")
        .to_string();
    assert!(
        error.contains("segment `contract`: the generated contract crate is `contract/`"),
        "{error}"
    );
}

/// The book's class example is a valid classing of the chat spec: its
/// tables, with any segment tables it shows in place of the spec's, render.
#[test]
fn the_books_class_example_renders() {
    let readme =
        std::fs::read_to_string(root().join("docs/src/concepts/cells.md")).expect("chapter");
    let block = readme
        .split("```toml\n")
        .skip(1)
        .filter_map(|rest| rest.split_once("```").map(|(block, _)| block))
        .find(|block| block.contains("[class."))
        .expect("a class example");
    let spec = std::fs::read_to_string(example().join("gdl.toml")).expect("spec");
    let text = if block.contains("[segment.") {
        let (head, _) = spec.split_once("[segment.").expect("segments");
        format!("{head}{block}")
    } else {
        format!("{spec}\n{block}")
    };
    let graph = std::fs::read_to_string(example().join("chat.gdl")).expect("graph");
    let spec = gdl_gen::Spec::from_text(&text, &graph, &example()).expect("spec");
    if let Err(error) =
        gdl_gen::render(&spec, &example().join("segments"), &target("gdl-gen-tests"))
    {
        panic!("{error}\n{text}");
    }
}

/// A response that returns to its outer end through an anchor needs a wire
/// type though its ends share a segment: with `Truncate` alone in another
/// segment, `Engine`'s `Token`s reach `Detokenize` through one, and the
/// segment that holds them asserts it, as the placement check does.
#[test]
fn a_response_an_anchor_carries_is_asserted() {
    let spec = std::fs::read_to_string(example().join("gdl.toml")).expect("spec");
    let (head, _) = spec.split_once("[segment.").expect("segments");
    let text = format!(
        "{head}[segment.core]\nnodes = [\"Frontend\", \"Preprocess\", \"Engine\", \"Detokenize\", \"Format\", \"Number\"]\n\n\
         [segment.trunc]\nnodes = [\"Truncate\"]\n"
    );
    let graph = std::fs::read_to_string(example().join("chat.gdl")).expect("graph");
    let spec = gdl_gen::Spec::from_text(&text, &graph, &example()).expect("spec");
    let files = gdl_gen::render(&spec, &example().join("segments"), &target("gdl-gen-tests"))
        .expect("render");
    let lib = files
        .iter()
        .find(|(path, _)| path.ends_with("core/src/lib.rs"))
        .map(|(_, text)| text)
        .expect("the core segment");
    assert!(
        lib.contains("type Engine_to_Detokenize = <contract::nodes::Detokenize as gdl_runtime::BackwardDecl>::In;"),
        "{lib}"
    );
}
