//! The ASR example: a graph whose requests stream. The committed segment
//! crates are what the generator produces from `examples/asr/gdl.toml`;
//! each segment links only the implementations it hosts; the segment that
//! hosts the source has no JSON-lines server; and the example's demo, which
//! runs the three generated segments in one process, streams audio in and
//! transcripts back.

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

#[test]
fn the_committed_asr_segments_are_current() {
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let out = example().join("segments");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    let drift = gdl_gen::drift(&out, &files).expect("drift");
    assert!(
        drift.is_empty(),
        "regenerate with `cargo run -p gdl-gen -- generate --spec examples/asr/gdl.toml --out examples/asr/segments`:\n{}",
        drift.join("\n")
    );
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
    let manifest = example().join("Cargo.toml");
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

#[test]
fn the_generated_segments_stream_audio_in_and_transcripts_back() {
    let run = cargo()
        .arg("test")
        .arg("--manifest-path")
        .arg(example().join("Cargo.toml"))
        .args(["--package", "asr-demo"])
        .env("CARGO_TARGET_DIR", target("examples"))
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
