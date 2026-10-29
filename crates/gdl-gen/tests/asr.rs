//! The ASR example: a graph whose requests stream. Each segment generated
//! from `examples/asr/gdl.toml` links only the implementations it hosts;
//! the segment that hosts the source has no JSON-lines server; and the
//! example's demo, which runs the three generated segments in one process,
//! streams audio in and transcripts back.

use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    dir.parent()
        .and_then(Path::parent)
        .expect("the crate sits two levels below the root")
        .to_path_buf()
}

fn example() -> PathBuf {
    root().join("examples/asr")
}

/// Nested cargo runs get target directories of their own: the outer
/// `cargo test` may still hold the lock on its own.
fn target(purpose: &str) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root().join("target"))
        .join(purpose)
}

fn cargo() -> Command {
    Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
}

/// The front segment hosts the source, whose request is streamed, so it
/// has no JSON-lines server; each hop's audio crosses twice.
#[test]
fn the_asr_segments_render() {
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let out = tempfile::Builder::new()
        .prefix("gdl-asr-render-")
        .tempdir()
        .expect("scratch");
    let files = gdl_gen::render(&spec, out.path(), &target("gdl-gen-tests")).expect("render");
    let front = &files[&PathBuf::from("front/src/lib.rs")];
    assert!(!front.contains("serve_json_lines"), "{front}");
    let report = &files[&PathBuf::from("crossings.txt")];
    assert!(report.contains("hop 1: 3 crossings, 3 anchors"), "{report}");
}

#[test]
fn the_asr_diagram_labels_its_streamed_edges() {
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let mermaid = gdl_gen::diagram(&spec, &target("gdl-gen-tests"), gdl_gen::Format::Mermaid)
        .expect("mermaid");
    for label in ["Session + stream Audio", "Session + stream Pcm"] {
        assert!(mermaid.contains(label), "{label} in:\n{mermaid}");
    }
}

#[test]
fn only_the_engine_segment_links_the_recognizer() {
    let (_lock, workspace) = demo_workspace();
    let manifest = workspace.join("Cargo.toml");
    for (package, links) in [
        ("asr-front", false),
        ("asr-dsp", false),
        ("asr-engine", true),
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
            tree.lines()
                .any(|line| line.starts_with("asr-engine-mock ")),
            links,
            "{package}:\n{tree}"
        );
    }
}

/// The demo links the generated segment libraries, so it builds in a
/// workspace beside them: the run of a program that deploys segments
/// through their libraries.
#[test]
fn the_generated_segments_stream_audio_in_and_transcripts_back() {
    let (_lock, workspace) = demo_workspace();
    let run = cargo()
        .arg("test")
        .arg("--manifest-path")
        .arg(workspace.join("Cargo.toml"))
        .args(["--package", "asr-demo"])
        .env("CARGO_TARGET_DIR", target("examples"))
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo test");
    assert!(
        run.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    let out = String::from_utf8_lossy(&run.stdout);
    assert!(
        out.contains("test result: ok. 3 passed"),
        "the demo's three tests did not all run:\n{out}"
    );
}

/// A workspace in the target directory that holds the generated segment
/// crates and the demo in `examples/asr/demo`, whose manifest names them,
/// and the lock that keeps another test process out of it while held.
/// Files are written only when they change, so Cargo rebuilds only what
/// changed since the last run.
fn demo_workspace() -> (std::fs::File, PathBuf) {
    let dir = target("asr-demo");
    std::fs::create_dir_all(&dir).expect("the workspace's directory");
    let lock = std::fs::File::create(target("asr-demo.lock")).expect("the lock");
    lock.lock().expect("hold the lock");

    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let segments = dir.join("segments");
    let files = gdl_gen::render(&spec, &segments, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&segments, &files).expect("the generated crates");
    let demo = example().join("demo");
    for file in ["src/lib.rs", "tests/transcribe.rs"] {
        let text = std::fs::read_to_string(demo.join(file)).expect("the demo's source");
        write(&dir.join("demo").join(file), &text);
    }
    let path = |crate_dir: &Path| toml::Value::String(crate_dir.display().to_string());
    let crates = root().join("crates");
    write(
        &dir.join("demo/Cargo.toml"),
        &format!(
            "[package]\nname = \"asr-demo\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n\
             [dependencies]\n\
             asr-dsp = {{ path = \"../segments/dsp\" }}\n\
             asr-dsp-nodes = {{ path = {} }}\n\
             asr-engine = {{ path = \"../segments/engine\" }}\n\
             asr-engine-mock = {{ path = {} }}\n\
             asr-front = {{ path = \"../segments/front\" }}\n\
             asr-front-nodes = {{ path = {} }}\n\
             asr-protocols = {{ path = {} }}\n\
             futures = \"0.3\"\n\
             gdl = {{ path = {} }}\n\
             gdl-runtime = {{ path = {} }}\n\
             tokio = {{ version = \"1\", features = [\"macros\", \"rt-multi-thread\", \"time\"] }}\n",
            path(&example().join("dsp-nodes")),
            path(&example().join("engine-mock")),
            path(&example().join("front-nodes")),
            path(&example().join("protocols")),
            path(&crates.join("gdl")),
            path(&crates.join("gdl-runtime")),
        ),
    );
    write(
        &dir.join("Cargo.toml"),
        "[workspace]\nresolver = \"3\"\nmembers = [\"demo\", \"segments/contract\", \"segments/dsp\", \"segments/engine\", \"segments/front\"]\n",
    );
    // The runtime's lock: the versions gdl is tested with, all in the
    // cache that its own build fills.
    if !dir.join("Cargo.lock").exists() {
        std::fs::copy(root().join("Cargo.lock"), dir.join("Cargo.lock")).expect("the seed");
    }
    (lock, dir)
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

/// The older form of spec, a contract crate with `graph!` and a
/// `[bind.<Node>]` table per node, still builds: the asr graph as a
/// contract crate in a scratch directory, over the example's messages and
/// node crates, through `cargo gdl build`.
#[test]
fn a_contract_spec_builds_the_asr_segments() {
    let scratch = tempfile::Builder::new()
        .prefix("gdl-asr-contract-")
        .tempdir()
        .expect("scratch");
    let dir = scratch.path();
    let path = |crate_dir: &Path| toml::Value::String(crate_dir.display().to_string());
    let crates = root().join("crates");
    std::fs::create_dir_all(dir.join("contract/src")).expect("the contract");
    std::fs::write(
        dir.join("contract/Cargo.toml"),
        format!(
            "[package]\nname = \"asr-contract\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n\
             [dependencies]\nasr-protocols = {{ path = {} }}\ngdl-runtime = {{ path = {} }}\n",
            path(&example().join("protocols")),
            path(&crates.join("gdl-runtime")),
        ),
    )
    .expect("the contract's manifest");
    std::fs::write(
        dir.join("contract/src/lib.rs"),
        "pub use asr_protocols::{Audio, Partial, Pcm, Session, Transcript};\n\n\
         gdl_runtime::graph! {\n\
             name = \"asr\", schema = 1;\n\
             source   Client: Session + stream Audio -> stream Transcript;\n\
             operator Gateway: (Session + stream Audio -> stream Transcript) over (Session + stream Audio -> stream Partial);\n\
             forward  Resample: Session + stream Audio -> Session + stream Pcm;\n\
             sink     Recognizer: Session + stream Pcm -> stream Partial;\n\
             backward Punctuate: Partial -> Partial;\n\
             topology: Client => Gateway { Resample => Recognizer => Punctuate };\n\
         }\n",
    )
    .expect("the contract's graph");
    let bind = |node: &str, package: &str, dir_name: &str| {
        format!(
            "[bind.{node}]\ncrate = \"{package}\"\npath = {}\ntype = \"{}::{node}\"\n",
            path(&example().join(dir_name)),
            package.replace('-', "_"),
        )
    };
    let spec = format!(
        "[contract]\npath = \"contract\"\n\n[gdl]\npath = {}\n\n\
         [segment.front]\nnodes = [\"Client\", \"Gateway\"]\n[segment.dsp]\nnodes = [\"Resample\"]\n\
         [segment.engine]\nnodes = [\"Recognizer\", \"Punctuate\"]\n\n{}{}{}{}",
        path(&crates.join("gdl-runtime")),
        bind("Gateway", "asr-front-nodes", "front-nodes"),
        bind("Resample", "asr-dsp-nodes", "dsp-nodes"),
        bind("Recognizer", "asr-engine-mock", "engine-mock"),
        bind("Punctuate", "asr-engine-mock", "engine-mock"),
    );
    std::fs::write(dir.join("gdl.toml"), spec).expect("the spec");
    let build = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "build", "--spec"])
        .arg(dir.join("gdl.toml"))
        .arg("--target-dir")
        .arg(target("examples"))
        .arg("--out")
        .arg(dir.join("bin"))
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo gdl build");
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );
    let binaries: Vec<String> = String::from_utf8_lossy(&build.stdout)
        .lines()
        .map(str::to_string)
        .collect();
    let expected: Vec<String> = ["asr-dsp", "asr-engine", "asr-front"]
        .iter()
        .map(|name| dir.join("bin").join(name).display().to_string())
        .collect();
    assert_eq!(binaries, expected);
}
