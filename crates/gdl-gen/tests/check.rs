//! `cargo gdl check`: a spec and a fleet file checked together, every
//! problem in both reported with its file, table, and key, as `generate`
//! and `compose` would find them, with nothing written.

mod common;

use std::path::{Path, PathBuf};
use std::process::Output;

use common::{root, target};

/// The chat graph in two classes: `global` runs every node but the
/// engine, whose hop is the class boundary.
fn spec(extra: &str) -> String {
    let chat = root().join("examples/chat");
    let path = |p: &Path| format!("{:?}", p.display().to_string());
    format!(
        "[graph]\npath = {graph}\n[messages]\ncrates = [{protocols}]\n\
         [nodes]\ncrates = [{front}, {engine}, {post}]\n[gdl]\npath = {gdl}\n\
         [segment.front]\nnodes = [\"Frontend\", \"Preprocess\", \"Truncate\", \"Number\", \"Detokenize\", \"Format\"]\n\
         [segment.engine]\nnodes = [\"Engine\"]\n\
         [class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"engine\"]\n{extra}",
        graph = path(&chat.join("chat.gdl")),
        protocols = path(&chat.join("protocols")),
        front = path(&chat.join("front-nodes")),
        engine = path(&chat.join("engine-mock")),
        post = path(&chat.join("post-nodes")),
        gdl = path(&root().join("crates/gdl-runtime")),
    )
}

const FLEET: &str = "[class.global]\nready = { front = 2 }\nrequires = { model_x = 1 }\n\
    [class.model_x]\nready = { engine = 1 }\n\
    [cell.global-us-east-1]\nclass = \"global\"\nregion = \"us-east-1\"\n\
    [cell.model-x-01-us-east-1]\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us-east-1\"\n";

/// The two files, written to a directory of their own.
struct Files {
    dir: tempfile::TempDir,
}

impl Files {
    fn new(spec: &str, fleet: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-check-")
            .tempdir()
            .expect("a directory");
        std::fs::write(dir.path().join("gdl.toml"), spec).expect("the spec");
        std::fs::write(dir.path().join("fleet.toml"), fleet).expect("the fleet");
        Self { dir }
    }

    fn spec(&self) -> PathBuf {
        self.dir.path().join("gdl.toml")
    }

    fn fleet(&self) -> PathBuf {
        self.dir.path().join("fleet.toml")
    }

    /// `cargo gdl <command> --spec gdl.toml --fleet fleet.toml`, and more.
    fn run(&self, command: &str, more: &[&std::ffi::OsStr]) -> Output {
        std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .args(["gdl", command, "--spec"])
            .arg(self.spec())
            .arg("--fleet")
            .arg(self.fleet())
            .args(more)
            .arg("--target-dir")
            .arg(target("gdl-check-tests"))
            .output()
            .expect("cargo gdl")
    }

    fn check(&self) -> Output {
        self.run("check", &[])
    }
}

/// The line of `text` that holds `needle`, from 1.
fn line_of(text: &str, needle: &str) -> usize {
    text.lines()
        .position(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("no `{needle}` in:\n{text}"))
        + 1
}

/// Each problem line of a failed check, which names its count last.
fn problems(output: &Output) -> Vec<String> {
    assert!(!output.status.success(), "a clean check: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut lines: Vec<String> = stderr.lines().map(str::to_string).collect();
    let summary = lines.pop().expect("a summary");
    let noun = if lines.len() == 1 {
        "problem"
    } else {
        "problems"
    };
    assert_eq!(
        summary,
        format!("cargo gdl: {} {noun}", lines.len()),
        "{stderr}"
    );
    lines
}

/// Every expected line is a problem, and there are no others.
fn assert_problems(found: &[String], expected: &[String]) {
    for line in expected {
        assert!(
            found.iter().any(|found| found.starts_with(line.as_str())),
            "{line}\nin:\n{}",
            found.join("\n")
        );
    }
    assert_eq!(found.len(), expected.len(), "{}", found.join("\n"));
}

#[test]
fn a_clean_spec_and_fleet_check_clean() {
    let files = Files::new(&spec(""), FLEET);
    let output = files.check();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("no problems"), "{stdout}");
}

/// A spec that does not parse is one problem, and the fleet file is still
/// checked for what needs no layout.
#[test]
fn a_spec_that_does_not_parse_leaves_the_fleet_checked() {
    let spec = spec("").replacen("nodes = [\"Engine\"]", "node = [\"Engine\"]", 1);
    let fleet = "[cell.Global]\nclass = \"global\"\nregion = \"US\"\n\
                 [cell.m]\nclass = \"model_x\"\nregion = \"r\"\nhome = \"nope\"\n";
    let files = Files::new(&spec, fleet);
    let (s, f) = (
        files.spec().display().to_string(),
        files.fleet().display().to_string(),
    );
    assert_problems(
        &problems(&files.check()),
        &[
            format!(
                "{s}:{}: [segment.engine] node: unknown field `node`",
                line_of(&spec, "node = [")
            ),
            format!("{f}: [cell.Global]: cell name `Global` must be"),
            format!("{f}: [cell.Global] region: region name `US` must be"),
            format!("{f}: [cell.m] home: the fleet has no cell `nope`"),
        ],
    );
}

/// A fleet file that does not parse is one problem, and every problem of
/// the spec is still reported.
#[test]
fn a_fleet_that_does_not_parse_leaves_the_spec_checked() {
    let spec = spec("[cells.zz]\nmax_near = 4\n[config.Nobody]\nx = 1\n").replacen(
        "\"Engine\"]",
        "\"Engine\", \"Ghost\"]",
        1,
    );
    let fleet = format!("{FLEET}zone = \"a\"\n");
    let files = Files::new(&spec, &fleet);
    let (s, f) = (
        files.spec().display().to_string(),
        files.fleet().display().to_string(),
    );
    assert_problems(
        &problems(&files.check()),
        &[
            format!("{s}: [segment.engine] nodes: segment `engine` lists unknown node `Ghost`"),
            format!("{s}: [cells.zz]: cell policy `[cells.zz]`: the placement has no class `zz`"),
            format!("{s}: [config.Nobody]: the graph has no bound node `Nobody`"),
            format!(
                "{f}:{}: [cell.model-x-01-us-east-1] zone: unknown field `zone`",
                line_of(&fleet, "zone")
            ),
        ],
    );
}

/// Problems in both files that read: the spec's, and the fleet's against
/// the spec's layout.
#[test]
fn every_problem_of_both_files_is_reported() {
    let spec = spec("[pipeline_names]\nGhost = \"ghost\"\n[config.Nobody]\nx = 1\n");
    let fleet = format!(
        "{}[cell.model-x-02-us-east-1]\nclass = \"model_x\"\nregion = \"us-east-1\"\n",
        FLEET
            .replacen(
                "ready = { engine = 1 }",
                "ready = { engine = 1, front = 1 }",
                1
            )
            .replacen(
                "requires = { model_x = 1 }",
                "requires = { model_x = 1, global = 1 }",
                1
            )
    );
    let files = Files::new(&spec, &fleet);
    let (s, f) = (
        files.spec().display().to_string(),
        files.fleet().display().to_string(),
    );
    let expected = [
        format!("{s}: [pipeline_names] Ghost: the graph has no source `Ghost`"),
        format!("{s}: [config.Nobody]: the graph has no bound node `Nobody`"),
        format!("{f}: [class.global] requires.global: not a class that `global` sends to"),
        format!("{f}: [class.model_x] ready.front: not a segment of class `model_x`"),
        format!("{f}: [cell.model-x-02-us-east-1] home: missing"),
    ];
    assert_problems(&problems(&files.check()), &expected);
    // `compose` runs the same check, and writes nothing.
    let out = files.dir.path().join("deployment");
    let compose = files.run("compose", &["--out".as_ref(), out.as_os_str()]);
    assert_problems(&problems(&compose), &expected);
    assert!(!out.exists(), "compose wrote a bundle");
}
