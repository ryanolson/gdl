//! A spec with several layouts of one graph: each validated on its own,
//! each generating its own segments, in a directory of its own, and the
//! commands that deploy one layout refusing a spec with several.

mod common;

use std::path::{Path, PathBuf};
use std::process::Output;

use common::{root, target};

/// The spec's header: the trio graph, over the chat example's crates.
fn header() -> String {
    let chat = root().join("examples/chat");
    let path = |p: &Path| format!("{:?}", p.display().to_string());
    format!(
        "[graph]\npath = {graph}\n[messages]\ncrates = [{protocols}]\n\
         [nodes]\ncrates = [{front}, {engine}, {post}]\npin = {{ Spare = \"chat_engine_mock::Engine\" }}\n\
         [gdl]\npath = {gdl}\n",
        graph = path(&root().join("crates/gdl-gen/tests/fixtures/layouts/trio.gdl")),
        protocols = path(&chat.join("protocols")),
        front = path(&chat.join("front-nodes")),
        engine = path(&chat.join("engine-mock")),
        post = path(&chat.join("post-nodes")),
        gdl = path(&root().join("crates/gdl-runtime")),
    )
}

const SEGMENTS: &str = "[segment.front]\n\
    nodes = [\"Ask\", \"Preprocess\", \"Truncate\", \"Detokenize\", \"Format\", \"Number\"]\n\
    [segment.side]\nnodes = [\"Tell\"]\n\
    [segment.engine]\nnodes = [\"Engine\", \"Spare\"]\n";

/// Three classes: each root crosses into `model_x`, one from `global` and
/// one from `batch`.
const THREE: &str = "[class.global]\nsegments = [\"front\"]\n\
    [class.batch]\nsegments = [\"side\"]\n\
    [class.model_x]\nsegments = [\"engine\"]\n";

/// Two classes: both entries in `global`.
const TWO: &str = "[class.global]\nsegments = [\"front\", \"side\"]\n\
    [class.model_x]\nsegments = [\"engine\"]\n";

/// `text`, a placement, as the tables of layout `name`.
fn nest(name: &str, text: &str) -> String {
    text.lines()
        .map(|line| match line.strip_prefix('[') {
            Some(header) => format!("[layout.{name}.{header}\n"),
            None => format!("{line}\n"),
        })
        .collect()
}

/// The two layouts of the README: three classes in one, two in the other.
fn two_layouts() -> String {
    header()
        + &nest("regional", &format!("{SEGMENTS}{THREE}"))
        + &nest("compact", &format!("{SEGMENTS}{TWO}"))
}

/// A spec in a directory of its own, with its output beside it.
struct Dir {
    dir: tempfile::TempDir,
}

impl Dir {
    fn new(spec: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-layouts-")
            .tempdir()
            .expect("a directory");
        std::fs::write(dir.path().join("gdl.toml"), spec).expect("the spec");
        Self { dir }
    }

    fn spec(&self) -> PathBuf {
        self.dir.path().join("gdl.toml")
    }

    fn out(&self) -> PathBuf {
        self.dir.path().join("out")
    }

    /// `cargo gdl <command> --spec gdl.toml`, and more.
    fn run(&self, command: &str, more: &[&std::ffi::OsStr]) -> Output {
        std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .args(["gdl", command, "--spec"])
            .arg(self.spec())
            .args(more)
            .arg("--target-dir")
            .arg(target("gdl-layouts-tests"))
            .output()
            .expect("cargo gdl")
    }

    fn generate(&self, check: bool) -> Output {
        let out = self.out();
        let mut args = vec![std::ffi::OsStr::new("--out"), out.as_os_str()];
        if check {
            args.push("--check".as_ref());
        }
        self.run("generate", &args)
    }
}

fn succeeded(output: &Output) {
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The placement a generated segment crate embeds.
fn embedded(lib_rs: &Path) -> gdl_model::Placement {
    let text = std::fs::read_to_string(lib_rs).expect("a generated library");
    let start = text
        .find("pub const PLACEMENT: &str = r")
        .expect("the placement")
        + "pub const PLACEMENT: &str = r".len();
    let hashes = text[start..].chars().take_while(|c| *c == '#').count();
    let open = start + hashes + 1;
    let close = format!("\"{}", "#".repeat(hashes));
    let end = open + text[open..].find(&close).expect("the placement's end");
    gdl_model::Placement::from_toml(&text[open..end]).expect("a placement")
}

/// Every generated file under `dir`, by its path from there.
fn files(dir: &Path) -> std::collections::BTreeMap<PathBuf, String> {
    let mut found = std::collections::BTreeMap::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(at) = dirs.pop() {
        for entry in std::fs::read_dir(&at).expect("a directory") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                let text = std::fs::read_to_string(&path).expect("a file");
                found.insert(path.strip_prefix(dir).expect("under").to_path_buf(), text);
            }
        }
    }
    found
}

#[test]
fn each_layout_generates_its_own_segments_in_its_own_directory() {
    let dir = Dir::new(&two_layouts());
    succeeded(&dir.run("check", &[]));
    succeeded(&dir.generate(false));
    let out = dir.out();
    // One contract for the graph; each layout's crates in its directory.
    assert!(out.join("contract/Cargo.toml").exists());
    for (layout, segments) in [
        ("regional", ["engine", "front", "side"]),
        ("compact", ["engine", "front", "side"]),
    ] {
        assert!(out.join(layout).join("crossings.txt").exists(), "{layout}");
        for segment in segments {
            let manifest =
                std::fs::read_to_string(out.join(layout).join(segment).join("Cargo.toml"))
                    .unwrap_or_else(|e| panic!("{layout}/{segment}: {e}"));
            // Two layouts' crates can share a workspace: their packages
            // are named for their layout.
            assert!(
                manifest.contains(&format!("name = \"trio-{layout}-{segment}\"")),
                "{manifest}"
            );
            assert!(manifest.contains("\"../../contract\""), "{manifest}");
        }
    }
    assert!(!out.join("front").exists());
    // Each embeds its own layout's placement, with that placement's hash.
    let regional = embedded(&out.join("regional/front/src/lib.rs"));
    let compact = embedded(&out.join("compact/front/src/lib.rs"));
    let placement = |text: &str| gdl_model::Placement::from_toml(text).expect("a placement");
    assert_eq!(
        regional.hash(),
        placement(&format!("{SEGMENTS}{THREE}")).hash()
    );
    assert_eq!(
        compact.hash(),
        placement(&format!("{SEGMENTS}{TWO}")).hash()
    );
    assert_ne!(regional.hash(), compact.hash());
    assert_eq!(regional.classes().count(), 3);
    assert_eq!(compact.classes().count(), 2);
    succeeded(&dir.generate(true));
}

/// What building one layout's segment on its own leaves behind, its build
/// directory and its lock file, is not drift, as it is not for a segment
/// at the top of the output.
#[test]
fn a_standalone_build_of_a_layouts_segment_is_not_drift() {
    let dir = Dir::new(&two_layouts());
    succeeded(&dir.generate(false));
    let segment = dir.out().join("regional/front");
    std::fs::create_dir_all(segment.join("target/debug")).expect("a build directory");
    std::fs::write(
        segment.join("target/CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55\n",
    )
    .expect("a cache tag");
    std::fs::write(segment.join("target/debug/output"), "built\n").expect("output");
    std::fs::write(segment.join("Cargo.lock"), "version = 4\n").expect("a lock file");
    succeeded(&dir.generate(true));
    succeeded(&dir.generate(false));
    // A file added to a layout's segment is still found.
    std::fs::write(segment.join("build.rs"), "fn main() {}\n").expect("a build script");
    let check = dir.generate(true);
    assert!(!check.status.success());
    let stderr = String::from_utf8_lossy(&check.stderr);
    assert!(
        stderr.contains("unexpected: regional/front/build.rs"),
        "{stderr}"
    );
}

/// One layout in a `[layout]` table generates what the same placement at
/// the top of the spec does, byte for byte.
#[test]
fn one_layout_generates_as_a_spec_without_layouts_does() {
    let plain = Dir::new(&(header() + SEGMENTS + THREE));
    let tabled = Dir::new(&(header() + &nest("only", &format!("{SEGMENTS}{THREE}"))));
    succeeded(&plain.generate(false));
    succeeded(&tabled.generate(false));
    let generated = files(&plain.out());
    assert!(generated.contains_key(Path::new("front/Cargo.toml")));
    assert_eq!(files(&tabled.out()), generated);
}

/// The output of a `cargo gdl` command that failed, each problem a line.
fn problems(output: &Output) -> Vec<String> {
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut lines: Vec<String> = stderr.lines().map(str::to_string).collect();
    let summary = lines.pop().expect("a summary");
    assert!(summary.starts_with("cargo gdl: "), "{stderr}");
    lines
}

#[test]
fn check_reports_every_layouts_problems_at_once() {
    let unplaced = SEGMENTS.replace("\"Engine\", \"Spare\"", "\"Engine\"");
    let unclassed = THREE.replace("[class.batch]\nsegments = [\"side\"]\n", "");
    // Format answers Preprocess's hop into `model_x` from another segment
    // than Preprocess's: only the inner end may be elsewhere.
    let boundary = SEGMENTS.replace(", \"Format\"", "") + "[segment.post]\nnodes = [\"Format\"]\n";
    let boundary_classes = "[class.global]\nsegments = [\"front\", \"post\", \"side\"]\n\
                            [class.model_x]\nsegments = [\"engine\"]\n";
    // Ask's root crosses from `global` into `model_x`, and Tell's from
    // `model_x` into `global`.
    let cycle = SEGMENTS.replace("\"Engine\", \"Spare\"", "\"Engine\"")
        + "[segment.spare]\nnodes = [\"Spare\"]\n";
    let cycle_classes = "[class.global]\nsegments = [\"front\", \"spare\"]\n\
                         [class.model_x]\nsegments = [\"engine\", \"side\"]\n";
    // Segment `y_z` of layout `x` and segment `z` of layout `x_y` are both
    // package `trio-x-y-z`.
    let renamed = |from: &str, to: &str| {
        SEGMENTS.replace(&format!("[segment.{from}]"), &format!("[segment.{to}]"))
    };
    let spec = header()
        + "[pipeline_names]\nNobody = \"nobody\"\n"
        + &nest("good", &format!("{SEGMENTS}{THREE}"))
        + &nest("unplaced", &format!("{unplaced}{TWO}"))
        + &nest("unclassed", &format!("{SEGMENTS}{unclassed}"))
        + &nest("boundary", &format!("{boundary}{boundary_classes}"))
        + &nest("cycle", &format!("{cycle}{cycle_classes}"))
        + &nest("x", &renamed("engine", "y_z"))
        + &nest("x_y", &renamed("engine", "z"));
    let dir = Dir::new(&spec);
    let s = dir.spec().display().to_string();
    let found = problems(&dir.run("check", &[]));
    let expected = [
        format!("{s}: [pipeline_names] Nobody: the graph has no source `Nobody`"),
        format!("{s}: [layout.unplaced.segment]: node `Spare` is in no segment"),
        format!("{s}: [layout.unclassed.class]: segment `side` is in no class"),
        format!(
            "{s}: [layout.boundary.segment.post] nodes: hop `Preprocess` -> `Engine` crosses from class `global` into `model_x`, but its backward stage `Format` is in segment `post`"
        ),
        format!(
            "{s}: [layout.cycle.class]: the class boundaries make a cycle through `global`, `model_x`"
        ),
        format!(
            "{s}: [layout.x_y.segment.z]: segment `z` becomes package `trio-x-y-z`, as segment `y_z` of layout `x` does"
        ),
    ];
    for line in &expected {
        assert!(
            found.iter().any(|found| found.starts_with(line.as_str())),
            "{line}\nin:\n{}",
            found.join("\n")
        );
    }
    // The graph's problem is reported once, not once for each layout.
    assert_eq!(found.len(), expected.len(), "{}", found.join("\n"));
    // Generating finds the same, and writes nothing.
    let generate = dir.generate(false);
    assert!(!generate.status.success());
    let stderr = String::from_utf8_lossy(&generate.stderr);
    for line in &found {
        assert!(stderr.contains(line.as_str()), "{line}\nin:\n{stderr}");
    }
    assert!(!dir.out().exists());
}

/// A layout's crates go in a directory named for it, beside the graph's
/// contract crate.
#[test]
fn a_layout_is_not_named_for_the_contracts_directory() {
    let spec = two_layouts() + &nest("contract", &format!("{SEGMENTS}{TWO}"));
    let dir = Dir::new(&spec);
    let s = dir.spec().display().to_string();
    let found = problems(&dir.run("check", &[]));
    assert_eq!(
        found,
        [format!(
            "{s}: [layout.contract]: the generated contract crate is `contract/`; rename the layout"
        )]
    );
}

/// `[deployment.segment.<name>]` names a segment of some layout.
#[test]
fn deployment_settings_name_a_segment_of_some_layout() {
    let batch = SEGMENTS.replace("[segment.side]", "[segment.batch]")
        + &TWO.replace("\"side\"", "\"batch\"");
    let spec = header()
        + &nest("regional", &format!("{SEGMENTS}{THREE}"))
        + &nest("compact", &batch)
        + "[deployment.segment.batch]\nbase_image = \"debian:bookworm\"\n\
           [deployment.segment.nowhere]\nbase_image = \"debian:bookworm\"\n";
    let dir = Dir::new(&spec);
    let s = dir.spec().display().to_string();
    let found = problems(&dir.run("check", &[]));
    assert_eq!(
        found,
        [format!(
            "{s}: [deployment.segment.nowhere]: no layout has a segment `nowhere`"
        )],
        "{}",
        found.join("\n")
    );
}

/// `export` and `diagram` take one layout, and refuse a spec with several
/// rather than choose one. `compose` deploys several only by cell, and
/// without a fleet file says that it needs one.
#[test]
fn the_commands_of_one_layout_refuse_several() {
    let dir = Dir::new(&two_layouts());
    let out = dir.dir.path().join("bundle");
    let archive = dir.dir.path().join("archive.tar.gz");
    for (command, more, refusal) in [
        (
            "compose",
            vec![std::ffi::OsStr::new("--out"), out.as_os_str()],
            "`compose` deploys a spec of several layouts, `compact` and `regional`, by cell, as its fleet file says: give it `--fleet <fleet.toml>`",
        ),
        (
            "export",
            vec!["--out".as_ref(), archive.as_os_str()],
            "`export` takes a spec of one layout; this one has layouts `compact`, `regional`",
        ),
        (
            "diagram",
            vec!["--format".as_ref(), "mermaid".as_ref()],
            "`diagram` takes a spec of one layout; this one has layouts `compact`, `regional`",
        ),
    ] {
        let output = dir.run(command, &more);
        assert!(!output.status.success(), "{command}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(refusal), "{command}: {stderr}");
    }
    assert!(!out.exists());
    assert!(!archive.exists());
}

/// The README's example of two layouts is the one these tests generate.
#[test]
fn the_readme_shows_these_layouts() {
    let readme = std::fs::read_to_string(root().join("README.md")).expect("the README");
    let shown = nest("regional", &format!("{SEGMENTS}{THREE}"))
        + "\n"
        + &nest("compact", &format!("{SEGMENTS}{TWO}"));
    assert!(readme.contains(&shown), "the README lacks:\n{shown}");
}

/// `cargo gdl check --fleet` checks each cell against the layout it names.
/// Both `global` and `batch` send to `model_x` in `regional`, so a cell of
/// each has a child there.
#[test]
fn a_fleet_is_checked_against_each_cells_layout() {
    let dir = Dir::new(&two_layouts());
    let fleet = dir.dir.path().join("fleet.toml");
    let check = || dir.run("check", &["--fleet".as_ref(), fleet.as_os_str()]);
    let good = "[cell.global-us]\nlayout = \"regional\"\nclass = \"global\"\nregion = \"us-east-1\"\n\
                [cell.batch-us]\nlayout = \"regional\"\nclass = \"batch\"\nregion = \"us-east-1\"\n\
                [cell.model-x-us]\nlayout = \"regional\"\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n\
                [cell.model-x-us-2]\nlayout = \"regional\"\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"batch-us\"\n\
                [cell.global-eu]\nlayout = \"compact\"\nclass = \"global\"\nregion = \"eu-west-1\"\n\
                [cell.model-x-eu]\nlayout = \"compact\"\nclass = \"model_x\"\nregion = \"eu-west-1\"\nhome = \"global-eu\"\n";
    std::fs::write(&fleet, good).expect("the fleet");
    succeeded(&check());
    let bad = format!(
        "{good}[cell.unnamed]\nclass = \"global\"\nregion = \"r\"\n\
         [cell.nowhere]\nlayout = \"west\"\nclass = \"global\"\nregion = \"r\"\n\
         [cell.batch-eu]\nlayout = \"compact\"\nclass = \"batch\"\nregion = \"eu-west-1\"\n\
         [cell.model-x-eu-2]\nlayout = \"compact\"\nclass = \"model_x\"\nregion = \"eu-west-1\"\nhome = \"global-us\"\n"
    );
    std::fs::write(&fleet, &bad).expect("the fleet");
    let f = fleet.display().to_string();
    let found = problems(&check());
    assert_eq!(
        found,
        [
            format!("{f}: [cell.batch-eu] class: layout `compact` has no class `batch`"),
            format!(
                "{f}: [cell.model-x-eu-2] home: cell `global-us` runs layout `regional`, not `compact`; a home is a cell of the cell's own layout"
            ),
            format!(
                "{f}: [cell.nowhere] layout: the spec has no layout `west`; its layouts are `compact`, `regional`"
            ),
            format!(
                "{f}: [cell.unnamed] layout: missing; the spec has layouts `compact`, `regional`, and a cell names the one it runs"
            ),
        ],
        "{}",
        found.join("\n")
    );
}

/// `diagram` locates a layout's problems as `check` does: in the layout's
/// tables.
#[test]
fn diagram_locates_a_layouts_problems() {
    let unplaced = SEGMENTS.replace("\"Engine\", \"Spare\"", "\"Engine\"");
    let dir = Dir::new(&(header() + &nest("regional", &format!("{unplaced}{THREE}"))));
    let output = dir.run("diagram", &["--format".as_ref(), "mermaid".as_ref()]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let s = dir.spec().display().to_string();
    assert!(
        stderr.contains(&format!(
            "{s}: [layout.regional.segment]: node `Spare` is in no segment"
        )),
        "{stderr}"
    );
}
