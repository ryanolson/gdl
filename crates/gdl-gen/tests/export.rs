//! `cargo gdl export`: the chat example's crates as one `.tar.gz` that is
//! the same bytes every time, holds what `generate` writes plus a build
//! script, and builds on its own, outside any workspace.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// As in `chat.rs`: nested cargo runs get target directories of their own.
fn target(purpose: &str) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root().join("target"))
        .join(purpose)
}

fn export() -> Vec<u8> {
    let spec = gdl_gen::Spec::load(&root().join("examples/chat/gdl.toml")).expect("spec");
    gdl_gen::export(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read).expect("exported")
}

/// Each entry: its path, mode, modification time, and text.
fn entries(bytes: &[u8]) -> Vec<(String, u32, u64, String)> {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes));
    archive
        .entries()
        .expect("entries")
        .map(|entry| {
            let mut entry = entry.expect("entry");
            let header = entry.header().clone();
            let mut text = String::new();
            entry.read_to_string(&mut text).expect("text");
            (
                entry.path().expect("path").display().to_string(),
                header.mode().expect("mode"),
                header.mtime().expect("mtime"),
                text,
            )
        })
        .collect()
}

#[test]
fn an_export_is_the_same_bytes_every_time() {
    let bytes = export();
    assert_eq!(bytes, export());
    let entries = entries(&bytes);
    let names: Vec<&str> = entries.iter().map(|(name, ..)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "chat/contract/Cargo.toml",
            "chat/contract/src/lib.rs",
            "chat/crossings.txt",
            "chat/engine/Cargo.toml",
            "chat/engine/src/lib.rs",
            "chat/engine/src/main.rs",
            "chat/front/Cargo.toml",
            "chat/front/src/lib.rs",
            "chat/front/src/main.rs",
            "chat/post/Cargo.toml",
            "chat/post/src/lib.rs",
            "chat/post/src/main.rs",
            "chat/README.md",
            "chat/build-all.sh",
        ]
    );
    for (name, mode, mtime, text) in &entries {
        assert_eq!(*mtime, 0, "{name}");
        let executable = name.ends_with(".sh");
        assert_eq!(*mode, if executable { 0o755 } else { 0o644 }, "{name}");
        if name.ends_with("Cargo.toml") {
            // Its own workspace root, wherever it is expanded.
            assert!(text.contains("\n[workspace]\n"), "{name}:\n{text}");
            // Every crate the spec names, by its absolute path; the
            // contract, inside the archive, by a relative one.
            for line in text.lines().filter(|line| line.contains("{ path = ")) {
                let path = line.split('"').nth(1).expect("a path");
                if line.starts_with("chat-contract = ") {
                    assert_eq!(path, "../contract", "{name}");
                } else {
                    let path = Path::new(path);
                    assert!(path.is_absolute() && path.exists(), "{line}");
                }
            }
            if !name.starts_with("chat/contract") {
                assert!(
                    text.contains("chat-contract = { path = \"../contract\" }"),
                    "{text}"
                );
                // The command that made it, complete.
                let spec =
                    std::fs::canonicalize(root().join("examples/chat/gdl.toml")).expect("the spec");
                let header = format!(
                    "# Regenerate with: cargo gdl export --spec \"{}\" --out <archive.tar.gz>",
                    spec.display()
                );
                assert_eq!(text.lines().nth(1), Some(header.as_str()), "{name}");
            }
        }
    }
}

/// Expanded outside the repository, `build-all.sh` builds each segment
/// with its own `cargo build`, passing on its arguments. It shares the chat
/// test's target directory, so only the exported crates compile, and it
/// builds their libraries alone: linking three binaries took most of 12 s,
/// and `chat.rs` links and runs the same generated code. Still the slowest
/// gdl-gen test (about 5 s): an expanded archive is new to cargo each time.
#[test]
fn an_expanded_export_builds_each_segment() {
    // Canonical, because cargo prints canonical package paths and macOS's
    // temporary directory is reached through a symlink.
    let temp = tempfile::Builder::new()
        .prefix("gdl-export-")
        .tempdir()
        .expect("dir");
    let dir = std::fs::canonicalize(temp.path()).expect("temp dir");
    tar::Archive::new(flate2::read::GzDecoder::new(export().as_slice()))
        .unpack(&dir)
        .expect("expanded");
    let build = Command::new("sh")
        .arg(dir.join("chat/build-all.sh"))
        .args(["--offline", "--lib", "--verbose"])
        .env("GDL_PROFILE", "dev")
        .env("CARGO_TARGET_DIR", target("examples"))
        .output()
        .expect("sh");
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(build.status.success(), "{stderr}");
    // The script's arguments reach cargo: `--verbose` shows each rustc run.
    assert!(stderr.contains("Running `"), "{stderr}");
    // Each segment was built from the expanded archive, not found built.
    for segment in ["engine", "front", "post"] {
        let built = format!(
            "chat-{segment} v0.0.0 ({})",
            dir.join("chat").join(segment).display()
        );
        assert!(stderr.contains(&built), "{built} in:\n{stderr}");
    }
}

/// The release profile of every segment manifest in `entries`, by name,
/// and the contract's (none expected).
fn profiles(entries: &[(String, u32, u64, String)]) -> Vec<(String, Option<toml::Value>)> {
    entries
        .iter()
        .filter(|(name, ..)| name.ends_with("Cargo.toml"))
        .map(|(name, _, _, text)| {
            let manifest: toml::Table = text.parse().expect("a manifest");
            (name.clone(), manifest.get("profile").cloned())
        })
        .collect()
}

/// Each exported segment is a workspace of its own, and its `Dockerfile`
/// and `build-all.sh` build it in release, so its manifest is the one
/// place a profile reaches its binary. The contract is only a dependency:
/// a profile there would be dead text.
#[test]
fn an_exported_segment_carries_the_release_profile() {
    let release: toml::Value =
        toml::from_str("release = { codegen-units = 1, lto = \"fat\" }").expect("the profile");
    for (name, profile) in profiles(&entries(&export())) {
        if name.starts_with("chat/contract") {
            assert_eq!(profile, None, "{name}");
        } else {
            assert_eq!(profile.as_ref(), Some(&release), "{name}");
        }
    }
}

/// Node registration goes through `linkme` sections, which an optimizer
/// that sees the whole program could drop. A binary built under the
/// profile an exported segment carries still lists the front segment's
/// nodes. It builds gdl and its dependencies in release, so its first run
/// on a cold target directory takes a minute or more; later runs build
/// only the probe and repeat the link.
#[test]
fn a_binary_under_the_exported_profile_lists_its_nodes() {
    let exported = entries(&export());
    let front = &exported
        .iter()
        .find(|(name, ..)| name == "chat/front/Cargo.toml")
        .expect("the front segment")
        .3;
    let profile = &front[front.find("[profile.release]").expect("a profile")..];
    let temp = tempfile::Builder::new()
        .prefix("gdl-export-profile-")
        .tempdir()
        .expect("dir");
    let nodes = std::fs::canonicalize(root().join("examples/chat/front-nodes")).expect("nodes");
    std::fs::create_dir_all(temp.path().join("src")).expect("dir");
    std::fs::write(
        temp.path().join("Cargo.toml"),
        format!(
            "[package]\nname = \"profile-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n\
             [dependencies]\nchat-front-nodes = {{ path = {:?} }}\ngdl = {{ path = {:?} }}\n\n\
             [workspace]\n\n{profile}",
            nodes.display().to_string(),
            std::fs::canonicalize(root().join("crates/gdl"))
                .expect("gdl")
                .display()
                .to_string(),
        ),
    )
    .expect("manifest");
    std::fs::copy(
        root().join("examples/chat/Cargo.lock"),
        temp.path().join("Cargo.lock"),
    )
    .expect("lock");
    std::fs::write(
        temp.path().join("src/main.rs"),
        "use chat_front_nodes as _;\n\
         fn main() { print!(\"{}\", gdl::Catalogue::linked().to_toml()); }\n",
    )
    .expect("source");
    let run = Command::new("timeout")
        .arg("300")
        .arg(env!("CARGO"))
        .args([
            "run",
            "--release",
            "--offline",
            "--quiet",
            "--manifest-path",
        ])
        .arg(temp.path().join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target("profile-probe"))
        .output()
        .expect("cargo run");
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "{stderr}");
    // Cargo reads a profile only at a workspace's root, and warns when it
    // ignores one.
    assert!(
        !stderr.contains("profiles for the non root package"),
        "{stderr}"
    );
    let linked = gdl_message::Catalogue::from_toml(&String::from_utf8_lossy(&run.stdout))
        .expect("a catalogue");
    let names: Vec<&str> = linked.nodes.iter().map(|node| node.name.as_str()).collect();
    for node in ["Preprocess", "Truncate", "Number"] {
        assert!(names.contains(&node), "{node} in {names:?}");
    }
}
