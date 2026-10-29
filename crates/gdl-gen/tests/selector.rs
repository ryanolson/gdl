//! A selector from a crate of its own, end to end: a spec binds the
//! selector `by_text` to its crate and type, the generated segment that
//! sends through it builds it from its settings and registers it, and the
//! crossing it names routes through it. `cargo gdl check` refuses a
//! selector it cannot resolve, and one whose `Head` does not fit the
//! crossing it routes.
//!
//! The end-to-end test builds the generated segments in a nested cargo run
//! that shares the examples' target directory: a cold build takes minutes,
//! a warm one seconds.

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use common::{Running, cargo, generated, root, target};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/selector")
}

fn check(spec: &str, graph: &str) -> Result<(), String> {
    let spec = gdl_gen::Spec::from_text(spec, graph, &fixture()).map_err(|e| e.to_string())?;
    gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
        .map(drop)
        .map_err(|e| e.to_string())
}

fn fixture_text(file: &str) -> String {
    std::fs::read_to_string(fixture().join(file)).expect("fixture")
}

/// Only `front` sends through `by_text`, so only it depends on the crate
/// and registers the selector, which it builds from its settings. A
/// request goes through the selector to the model; one whose text the
/// settings name is refused by it, with their reason.
#[test]
fn a_bound_selector_routes_the_crossing_it_names() {
    let spec_file = fixture().join("gdl.toml");
    let problems = gdl_gen::check_files(&spec_file, None, &target("gdl-gen-tests"));
    assert!(problems.is_empty(), "{:?}", problems.0);

    let temp = tempfile::Builder::new()
        .prefix("gdl-selector-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    let spec = gdl_gen::Spec::load(&spec_file).expect("spec");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
    let file = |path: &str| std::fs::read_to_string(out.join(path)).expect("generated");
    assert!(file("front/Cargo.toml").contains("selector-router = "));
    assert!(
        file("front/src/lib.rs").contains("\"by_text\" = ::selector_router::ByText;"),
        "{}",
        file("front/src/lib.rs")
    );
    for path in ["model/Cargo.toml", "model/src/lib.rs"] {
        assert!(!file(path).contains("selector_router"), "{path}");
        assert!(!file(path).contains("selector-router"), "{path}");
    }

    std::fs::write(
        out.join("Cargo.toml"),
        "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
    )
    .expect("workspace");
    // Resolved as the repository is, so that the build reuses its versions.
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
    let config = |name: &str, extra: &str| {
        let path = temp.path().join(format!("{name}.toml"));
        let text = format!(
            "listen = \"127.0.0.1:0\"\nroster = {}\n{extra}",
            toml::Value::String(roster.display().to_string())
        );
        std::fs::write(&path, text).expect("config");
        path
    };
    let model = config("model", "");
    let front = config(
        "front",
        "[selector.by_text]\nrefuse = \"stop here\"\nreason = \"its settings name the text\"\n",
    );
    let _model = Running(
        generated(bin.join("selector-model"))
            .arg(&model)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("model"),
    );
    let mut front = Running(
        generated(bin.join("selector-front"))
            .arg(&front)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("front"),
    );
    let mut stdin = front.0.stdin.take().expect("stdin");
    for request in [r#"{"text": "go on"}"#, r#"{"text": "stop here"}"#] {
        writeln!(stdin, "{request}").expect("request");
    }
    // End of input: the front answers what it has read, then exits.
    drop(stdin);
    let lines = front.stdout().rest(Duration::from_secs(60));
    let status = front.exits_within(Duration::from_secs(10));
    assert!(status.success(), "{lines:?}");
    assert_eq!(lines.len(), 3, "{lines:?}");
    assert_eq!(lines[0], r#"{"item":{"text":"go on (2 words)"}}"#);
    assert_eq!(lines[1], r#"{"done":null}"#);
    assert!(
        lines[2].starts_with(r#"{"error":"#)
            && lines[2]
                .contains("by_text refused a request of 2 words: its settings name the text"),
        "{}",
        lines[2]
    );
}

/// The fixture checks; a type the crate does not offer as a selector, a
/// crate the reader cannot read, and a pool policy's type are refused, each
/// naming the table.
#[test]
fn check_refuses_a_selector_it_cannot_resolve() {
    let text = fixture_text("gdl.toml");
    let graph = fixture_text("selector.gdl");
    check(&text, &graph).expect("the fixture checks");
    let error = check(
        &text.replace("selector_router::ByText", "selector_router::ByName"),
        &graph,
    )
    .expect_err("an unknown type");
    assert!(
        error.contains(
            "[selector.by_text]: crate `selector-router` implements no selector `ByName`; it implements `ByText`"
        ),
        "{error}"
    );
    let error = check(
        &text.replace("path = \"router\"", "path = \"nowhere\""),
        &graph,
    )
    .expect_err("no crate");
    assert!(
        error.contains("[selector.by_text]: ") && error.contains("nowhere"),
        "{error}"
    );
    let error = check(
        &text.replace(
            "selector_router::ByText",
            "selector_router::missing::ByText",
        ),
        &graph,
    )
    .expect_err("a path the crate does not have");
    assert!(
        error.contains("offers selector `ByText` at `selector_router::ByText`; write that path"),
        "{error}"
    );
}

/// `by_text` reads `Prepared`, the request that enters `Model`. Routing the
/// crossing into `Prep`, whose request is `Ask`, through it is refused,
/// naming that crossing; the crossing into `Model` is not.
#[test]
fn check_refuses_a_selector_whose_head_does_not_fit_its_crossing() {
    let text = fixture_text("gdl.toml")
        .replace(
            "[segment.front]\nnodes = [\"Front\", \"Prep\"]",
            "[segment.front]\nnodes = [\"Front\"]\n\n[segment.prep]\nnodes = [\"Prep\"]",
        )
        .replace(
            "Model = { selector = \"by_text\" }",
            "Prep = { selector = \"by_text\" }\nModel = { selector = \"by_text\" }",
        );
    let error = check(&text, &fixture_text("selector.gdl")).expect_err("a misfit");
    assert!(
        error.contains(
            "[selector.by_text]: selector `by_text` of the crossing into node `Prep` reads the head as `selector_messages::Prepared` (selector-messages 0.0.0), but the request that crosses into it carries `selector_messages::Ask` (selector-messages 0.0.0)"
        ),
        "{error}"
    );
    assert!(!error.contains("into node `Model`"), "{error}");
    // Through `Model` alone, across the same three segments, it fits.
    let fits = text.replace("Prep = { selector = \"by_text\" }\n", "");
    check(&fits, &fixture_text("selector.gdl")).expect("the head fits");
}
