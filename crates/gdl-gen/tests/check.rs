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
    [pool.global-us-east-1]\nclass = \"global\"\nregion = \"us-east-1\"\n\
    [pool.model-x-01-us-east-1]\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us-east-1\"\n";

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
    let fleet = "[pool.Global]\nclass = \"global\"\nregion = \"US\"\n\
                 [pool.m]\nclass = \"model_x\"\nregion = \"r\"\nhome = \"nope\"\n";
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
            format!("{f}: [pool.Global]: pool name `Global` must be"),
            format!("{f}: [pool.Global] region: region name `US` must be"),
            format!("{f}: [pool.m] home: the fleet has no pool `nope`"),
            // Which class sends to which is the layout's, so no parent's
            // children are judged, and the check says so once.
            format!(
                "{f}: the pools' children are not checked: which class sends to which is a layout's, and the spec's layouts did not read"
            ),
        ],
    );
}

/// A fleet file that does not parse is one problem, and every problem of
/// the spec is still reported.
#[test]
fn a_fleet_that_does_not_parse_leaves_the_spec_checked() {
    let spec = spec("[pools.zz]\npolicy = 'home_first'\n[config.Nobody]\nx = 1\n").replacen(
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
            format!("{s}: [pools.zz]: pool policy `[pools.zz]`: the placement has no class `zz`"),
            format!("{s}: [config.Nobody]: the graph has no bound node `Nobody`"),
            format!(
                "{f}:{}: [pool.model-x-01-us-east-1] zone: unknown field `zone`",
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
        "{}[pool.model-x-02-us-east-1]\nclass = \"model_x\"\nregion = \"us-east-1\"\n",
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
        format!("{f}: [pool.model-x-02-us-east-1] home: missing"),
    ];
    assert_problems(&problems(&files.check()), &expected);
    // `compose` runs the same check, and writes nothing.
    let out = files.dir.path().join("deployment");
    let compose = files.run("compose", &["--out".as_ref(), out.as_os_str()]);
    assert_problems(&problems(&compose), &expected);
    assert!(!out.exists(), "compose wrote a bundle");
}

/// A parent pool without a child of a class its class sends to is one
/// problem among the others, at the parent's table: `check` reports every
/// one and fails, and `compose` refuses the same fleet.
#[test]
fn a_missing_child_is_reported_with_every_other_problem() {
    let fleet = "[class.model_x]\nready = { front = 1 }\n\
                 [pool.global-us-east-1]\nclass = \"global\"\nregion = \"us-east-1\"\n";
    let files = Files::new(&spec(""), fleet);
    let f = files.fleet().display().to_string();
    let expected = [
        format!("{f}: [class.model_x] ready.front: not a segment of class `model_x`"),
        format!("{f}: [pool.global-us-east-1]: no child of class `model_x` in layout `default`; "),
    ];
    let check = files.check();
    assert_eq!(check.status.code(), Some(1), "{check:?}");
    assert_problems(&problems(&check), &expected);
    let out = files.dir.path().join("deployment");
    let compose = files.run("compose", &["--out".as_ref(), out.as_os_str()]);
    assert_problems(&problems(&compose), &expected);
    assert!(!out.exists(), "compose wrote a bundle");
}

/// A default spelled out for a node, a class, or a segment the spec does
/// not have is refused as any other value for it is: the name is the
/// spec's to check, whatever its value.
#[test]
fn a_default_for_a_name_the_spec_lacks_is_refused() {
    // A placement's problem leaves its layout's classes unknown, so the
    // check notes that the pools' children went unjudged; a deployment's
    // does not.
    for (extra, expected, unlaid) in [
        (
            "[routing]\nGhost = \"round_robin\"\n",
            "[routing] Ghost: request router `Ghost` must target a request boundary",
            true,
        ),
        (
            "[pools.ghost]\npolicy = \"home_first\"\n",
            "[pools.ghost]: pool policy `[pools.ghost]`: the placement has no class `ghost`",
            true,
        ),
        (
            "[deployment.segment.ghost]\nports = []\n",
            "[deployment.segment.ghost]: the placement has no segment `ghost`",
            false,
        ),
    ] {
        let files = Files::new(&spec(extra), FLEET);
        let (s, f) = (
            files.spec().display().to_string(),
            files.fleet().display().to_string(),
        );
        let mut lines = vec![format!("{s}: {expected}")];
        if unlaid {
            lines.push(format!(
                "{f}: the pools' children are not checked in layout `default`: which class sends to which is a layout's, and the layout has problems"
            ));
        }
        assert_problems(&problems(&files.check()), &lines);
    }
}

/// A runtime path that names no crate is one problem among the others,
/// not the end of the check.
#[test]
fn a_bad_runtime_path_hides_no_other_problem() {
    let gdl = format!(
        "{:?}",
        root().join("crates/gdl-runtime").display().to_string()
    );
    let spec = spec("[pipeline_names]\nGhost = \"ghost\"\n")
        .replacen(&gdl, "\"/nonexistent/gdl-runtime\"", 1)
        .replacen("\"Engine\"]", "\"Engine\", \"Ghost\"]", 1);
    let files = Files::new(&spec, FLEET);
    let (s, f) = (
        files.spec().display().to_string(),
        files.fleet().display().to_string(),
    );
    assert_problems(
        &problems(&files.check()),
        &[
            format!("{s}: [gdl] path: "),
            format!("{s}: [segment.engine] nodes: segment `engine` lists unknown node `Ghost`"),
            format!("{s}: [pipeline_names] Ghost: the graph has no source `Ghost`"),
            format!(
                "{f}: the pools' children are not checked in layout `default`: which class sends to which is a layout's, and the layout has problems"
            ),
        ],
    );
}

/// A count of 0 is wrong whatever the classes are, so a fleet file whose
/// spec does not read has its counts checked too.
#[test]
fn a_fleet_without_a_layout_still_has_its_counts_checked() {
    let spec = spec("").replacen("nodes = [\"Engine\"]", "node = [\"Engine\"]", 1);
    let fleet = "[class.global]\nready = { front = 0 }\nrequires = { model_x = 0 }\n\
                 [pool.global-us-east-1]\nclass = \"global\"\nregion = \"us-east-1\"\nready = { front = 0 }\n";
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
            format!("{f}: [class.global] ready.front: is 0; a quorum is at least 1"),
            format!("{f}: [class.global] requires.model_x: is 0; a requirement is at least 1"),
            format!("{f}: [pool.global-us-east-1] ready.front: is 0; a quorum is at least 1"),
            format!(
                "{f}: the pools' children are not checked: which class sends to which is a layout's, and the spec's layouts did not read"
            ),
        ],
    );
}

/// A node crate whose source does not read is one problem among the
/// others: the placement and the pipeline names are still checked.
#[test]
fn a_node_crate_that_does_not_read_hides_no_other_problem() {
    let chat = root().join("examples/chat");
    let engine = format!("{:?}", chat.join("engine-mock").display().to_string());
    let files = Files::new("", FLEET);
    let broken = files.dir.path().join("broken-nodes");
    std::fs::create_dir_all(broken.join("src")).expect("a crate");
    std::fs::write(
        broken.join("Cargo.toml"),
        "[package]\nname = \"broken-nodes\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("a manifest");
    std::fs::write(broken.join("src/lib.rs"), "pub fn (\n").expect("a source");
    let spec = spec("[pipeline_names]\nGhost = \"ghost\"\n")
        .replacen(
            &engine,
            &format!("{engine}, {:?}", broken.display().to_string()),
            1,
        )
        .replacen("\"Engine\"]", "\"Engine\", \"Ghost\"]", 1);
    std::fs::write(files.spec(), &spec).expect("the spec");
    let s = files.spec().display().to_string();
    let found = problems(&files.check());
    assert!(
        found.iter().any(|line| line.contains("broken-nodes")),
        "the node crate's problem in:\n{}",
        found.join("\n")
    );
    for expected in [
        format!("{s}: [segment.engine] nodes: segment `engine` lists unknown node `Ghost`"),
        format!("{s}: [pipeline_names] Ghost: the graph has no source `Ghost`"),
    ] {
        assert!(
            found.iter().any(|line| line.starts_with(&expected)),
            "{expected}\nin:\n{}",
            found.join("\n")
        );
    }
}

/// Every command that reads a spec names the spec's file in a problem of
/// its own, as `check` does: here `generate`, with a contract that is not
/// there.
#[test]
fn generate_names_the_spec_in_a_problem_of_its_own() {
    let files = Files::new(
        "[contract]\npath = \"nowhere\"\n[gdl]\nversion = \"0.1\"\n[segment.front]\nnodes = [\"A\"]\n",
        FLEET,
    );
    let out = files.dir.path().join("out");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "generate", "--spec"])
        .arg(files.spec())
        .arg("--out")
        .arg(&out)
        .arg("--target-dir")
        .arg(target("gdl-check-tests"))
        .output()
        .expect("cargo gdl");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let expected = format!("{}: [contract] path: ", files.spec().display());
    assert!(stderr.contains(&expected), "{expected}\nin: {stderr}");
    // The control: `check` names it the same way.
    let check = problems(&files.check());
    assert!(
        check.iter().any(|line| line.starts_with(&expected)),
        "{check:?}"
    );
}

/// A spec whose text is not ASCII and does not parse is one problem, and
/// the fleet file is still checked: the check reports, it does not panic.
#[test]
fn a_spec_with_text_that_is_not_ascii_is_checked_whole() {
    let spec = spec("[config.Frontend]\nnote = \"café\"\nlimit = 3 x\n");
    let fleet = "[pool.Global]\nclass = \"global\"\nregion = \"r\"\n";
    let files = Files::new(&spec, fleet);
    let output = files.check();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let (s, f) = (
        files.spec().display().to_string(),
        files.fleet().display().to_string(),
    );
    assert_problems(
        &problems(&output),
        &[
            format!(
                "{s}:{}: [config.Frontend] limit: ",
                line_of(&spec, "limit = 3 x")
            ),
            format!("{f}: [pool.Global]: pool name `Global` must be"),
            format!(
                "{f}: the pools' children are not checked: which class sends to which is a layout's, and the spec's layouts did not read"
            ),
        ],
    );
}

/// A messages crate whose `msgpack_array` messages omit fields:
/// `skip_serializing_if` on a struct's field, a one-sided skip through
/// `cfg_attr` on an enum variant's, and a one-sided skip in a generic
/// message that an alias exports. A `cfg_attr` that does not apply, and a field both directions
/// skip, omit nothing.
fn omitting(codec: &str) -> String {
    format!(
        r#"#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "{codec}")]
pub struct Hinted {{
    pub id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<u32>,
    pub text: Option<String>,
}}

#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "{codec}")]
pub enum Event {{
    Done,
    Token {{
        id: u32,
        #[cfg_attr(all(), serde(skip_deserializing))]
        logprob: f32,
    }},
}}

#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "{codec}")]
pub struct Carried<P> {{
    #[serde(skip_serializing)]
    pub payload: P,
}}

#[gdl_message::export]
pub type CarriedId = Carried<u32>;

#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub struct Fixed {{
    #[cfg_attr(any(), serde(skip_serializing_if = "Option::is_none"))]
    pub hint: Option<u32>,
    #[serde(skip)]
    pub cache: Option<u32>,
}}
"#
    )
}

/// Positional MessagePack has no field names, so a field that one
/// direction can omit and the other reads shifts every field after it:
/// check refuses such a message and names the field and the attribute.
/// With a named codec the same messages check clean.
#[test]
fn check_refuses_a_positional_message_that_omits_a_field() {
    let files = Files::new("", FLEET);
    let crate_dir = files.dir.path().join("omitting-protocols");
    std::fs::create_dir_all(crate_dir.join("src")).expect("a crate");
    std::fs::write(
        crate_dir.join("Cargo.toml"),
        "[package]\nname = \"omitting-protocols\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("a manifest");
    let protocols = format!(
        "{:?}",
        root().join("examples/chat/protocols").display().to_string()
    );
    let spec = spec("").replacen(
        &protocols,
        &format!("{protocols}, {:?}", crate_dir.display().to_string()),
        1,
    );
    std::fs::write(files.spec(), &spec).expect("the spec");
    std::fs::write(crate_dir.join("src/lib.rs"), omitting("msgpack_array")).expect("a source");
    let found = problems(&files.check());
    // The fleet's children wait on a layout without problems.
    let refused: Vec<&String> = found
        .iter()
        .filter(|line| !line.contains("the pools' children are not checked"))
        .collect();
    assert_eq!(refused.len(), 3, "{}", found.join("\n"));
    for (message, field, key) in [
        ("`Hinted`", "`hint`", "`skip_serializing_if`"),
        ("`Event`", "`Token::logprob`", "`skip_deserializing`"),
        ("`CarriedId`", "`payload`", "`skip_serializing`"),
    ] {
        assert!(
            refused.iter().any(|line| line.contains(message)
                && line.contains(field)
                && line.contains(key)
                && line.contains("msgpack_array")),
            "{message} {field} {key} in:\n{}",
            found.join("\n")
        );
    }
    for codec in ["msgpack", "json"] {
        std::fs::write(crate_dir.join("src/lib.rs"), omitting(codec)).expect("a source");
        let output = files.check();
        assert!(output.status.success(), "{codec}: {output:?}");
    }
}
