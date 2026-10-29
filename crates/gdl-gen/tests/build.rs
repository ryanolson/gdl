//! `cargo gdl build`: every segment of a spec generated into the target
//! directory as one workspace, built, and copied out. It writes nothing in
//! the project, rewrites only what changed, so that an unchanged segment
//! is not rebuilt, and removes only what it wrote.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, SystemTime};

use common::{Chat, root, target};

/// A copy of the chat example in a scratch directory, as a user's own
/// project: its node and message crates, its graph, its spec, and its
/// lock, with no generated crates. The repository's crates are named by
/// absolute path, so the copy builds wherever it is.
struct Project {
    dir: tempfile::TempDir,
}

impl Project {
    fn chat() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-build-")
            .tempdir()
            .expect("scratch");
        let from = root().join("examples/chat");
        copy(&from, dir.path());
        let crates = format!("{}/", root().join("crates").display());
        let manifest = std::fs::read_to_string(from.join("Cargo.toml"))
            .expect("the chat workspace")
            .lines()
            .map(|line| line.replace("../../crates/", &crates) + "\n")
            .collect::<String>();
        std::fs::write(dir.path().join("Cargo.toml"), manifest).expect("workspace");
        // The spec names the runtime by a relative path, as a user's does.
        let up = dir.path().components().count() - 1;
        let relative = format!(
            "{}{}/",
            "../".repeat(up),
            root()
                .join("crates")
                .strip_prefix("/")
                .expect("absolute")
                .display()
        );
        let spec = std::fs::read_to_string(from.join("gdl.toml"))
            .expect("the chat spec")
            .replace("../../crates/", &relative);
        std::fs::write(dir.path().join("gdl.toml"), spec).expect("spec");
        Self { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn spec(&self) -> PathBuf {
        self.path().join("gdl.toml")
    }
}

/// Copy the tree `from` into `to`, without build output.
fn copy(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("directory");
    for entry in std::fs::read_dir(from).expect("directory") {
        let entry = entry.expect("entry");
        let name = entry.file_name();
        if name == "target" {
            continue;
        }
        let path = entry.path();
        if entry.file_type().expect("type").is_dir() {
            copy(&path, &to.join(&name));
        } else {
            std::fs::copy(&path, to.join(&name)).expect("copy");
        }
    }
}

/// Every file under `dir`, with its bytes and its modification time.
fn snapshot(dir: &Path) -> BTreeMap<PathBuf, (Vec<u8>, SystemTime)> {
    let mut files = BTreeMap::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(next) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries {
            let entry = entry.expect("entry");
            let path = entry.path();
            if entry.file_type().expect("type").is_dir() {
                dirs.push(path);
                continue;
            }
            let modified = entry
                .metadata()
                .expect("metadata")
                .modified()
                .expect("time");
            let bytes = std::fs::read(&path).expect("file");
            files.insert(
                path.strip_prefix(dir).expect("inside").to_path_buf(),
                (bytes, modified),
            );
        }
    }
    files
}

/// `cargo gdl build`, run as cargo runs it, offline: the project's lock and
/// the local cache hold every crate. It runs in the directory above the
/// project, with the spec's path relative to it, as a user types it.
fn build(spec: &Path, target: &Path, out: &Path) -> Output {
    let project = spec.parent().expect("the project");
    let above = project.parent().expect("the directory above it");
    Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .current_dir(above)
        .args(["gdl", "build", "--spec"])
        .arg(spec.strip_prefix(above).expect("under it"))
        .arg("--target-dir")
        .arg(target)
        .arg("--out")
        .arg(out)
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo gdl build")
}

/// What a successful build printed: the binaries on stdout, and the
/// packages cargo compiled, from its `Compiling` lines on stderr.
fn succeeded(output: &Output) -> (Vec<PathBuf>, BTreeSet<String>) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    let binaries = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(PathBuf::from)
        .collect();
    let compiled = stderr
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("Compiling "))
        .filter_map(|rest| rest.split(' ').next())
        .map(str::to_string)
        .collect();
    (binaries, compiled)
}

/// The whole life of one build directory, in order, since each step needs
/// the build before it. The first build compiles velo and the runtime in a
/// target directory that every run shares, which takes minutes on a cold
/// checkout; later runs compile only the project's crates and the segments.
#[test]
fn a_build_generates_in_the_target_rewrites_only_changes_and_removes_only_its_own() {
    let project = Project::chat();
    let target = target("gdl-build-tests");
    let scratch = tempfile::Builder::new()
        .prefix("gdl-build-out-")
        .tempdir()
        .expect("scratch");
    let out = scratch.path().join("bin");
    let generated = target.join("gdl/chat/gdl");
    let workspace = generated.join("workspace");
    // The target directory is shared across runs for its compiled crates;
    // this build's own directory starts empty. A run before left a file
    // there that the build refuses to write over.
    match std::fs::remove_dir_all(&generated) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => panic!("{e}"),
        _ => {}
    }
    let before = snapshot(project.path());

    // The first build: every segment, and nothing written in the project.
    let (binaries, _) = succeeded(&build(&project.spec(), &target, &out));
    let expected: Vec<PathBuf> = ["chat-engine", "chat-front", "chat-post"]
        .iter()
        .map(|name| out.join(name))
        .collect();
    assert_eq!(binaries, expected);
    assert_eq!(
        snapshot(project.path()),
        before,
        "the build wrote in the project"
    );
    for crate_dir in ["contract", "engine", "front", "post"] {
        assert!(
            workspace.join(crate_dir).join("Cargo.toml").exists(),
            "{crate_dir}"
        );
    }

    // The binaries run: the three segments meet in a roster and answer.
    let chat = Chat::new("gdl-build-chat-");
    let _engine = chat.spawn_from(&out, "engine", &chat.config("engine", ""), false);
    let _post = chat.spawn_from(&out, "post", &chat.config("post", ""), false);
    let mut front = chat.spawn_from(&out, "front", &chat.config("front", ""), true);
    let answers = front.stdout();
    common::ask(&mut front, r#"{"text": "hello world", "max_words": 2}"#);
    assert_eq!(
        answers.next(Duration::from_secs(60)),
        r#"{"item":{"index":0,"text":"[world]"}}"#
    );
    drop(front);

    // A second run with no change writes no file and compiles nothing.
    let unchanged = snapshot(&generated);
    let copied = snapshot(&out);
    let (binaries, compiled) = succeeded(&build(&project.spec(), &target, &out));
    assert_eq!(binaries, expected);
    assert!(compiled.is_empty(), "compiled {compiled:?}");
    assert_eq!(
        snapshot(&generated),
        unchanged,
        "the build rewrote its files"
    );
    assert_eq!(
        snapshot(&out),
        copied,
        "the build copied the binaries again"
    );

    // A change to the engine's crate rebuilds only it and its segment.
    let engine = project.path().join("engine-mock/src/lib.rs");
    let source = std::fs::read_to_string(&engine).expect("engine source");
    std::fs::write(&engine, format!("// changed\n{source}")).expect("engine source");
    let (_, compiled) = succeeded(&build(&project.spec(), &target, &out));
    assert_eq!(
        compiled,
        BTreeSet::from(["chat-engine".to_string(), "chat-engine-mock".to_string()])
    );
    let recopied = snapshot(&out);
    for name in ["chat-front", "chat-post"] {
        assert_eq!(recopied[Path::new(name)], copied[Path::new(name)], "{name}");
    }
    assert_ne!(
        recopied[Path::new("chat-engine")].1,
        copied[Path::new("chat-engine")].1,
        "chat-engine was not copied again"
    );

    // A segment the spec drops is removed with its binary; a file the
    // build did not write stays, and so does its directory.
    std::fs::write(workspace.join("engine/notes.txt"), "mine\n").expect("a file of the user's");
    let spec = std::fs::read_to_string(project.spec()).expect("spec");
    let spec = spec
        .replace("[segment.engine]\nnodes = [\"Engine\"]\n", "")
        .replace(
            "nodes = [\"Detokenize\", \"Format\"]",
            "nodes = [\"Engine\", \"Detokenize\", \"Format\"]",
        );
    std::fs::write(project.spec(), spec).expect("spec");
    let (binaries, _) = succeeded(&build(&project.spec(), &target, &out));
    assert_eq!(binaries, [out.join("chat-front"), out.join("chat-post")]);
    assert!(
        !out.join("chat-engine").exists(),
        "the dropped segment's binary"
    );
    assert!(!workspace.join("engine/Cargo.toml").exists());
    assert!(!workspace.join("engine/src").exists());
    assert_eq!(
        std::fs::read_to_string(workspace.join("engine/notes.txt")).expect("the user's file"),
        "mine\n"
    );
}

/// A generated workspace that holds files but no stamp is someone else's
/// directory: the build refuses it and writes nothing there.
#[test]
fn a_build_refuses_a_directory_it_did_not_make() {
    let project = Project::chat();
    let scratch = tempfile::Builder::new()
        .prefix("gdl-build-foreign-")
        .tempdir()
        .expect("scratch");
    let target = scratch.path().join("target");
    let workspace = target.join("gdl/chat/gdl/workspace");
    std::fs::create_dir_all(&workspace).expect("directory");
    std::fs::write(workspace.join("Cargo.toml"), "[workspace]\n").expect("a manifest");
    let before = snapshot(&workspace);
    let output = build(&project.spec(), &target, &scratch.path().join("bin"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("not made by `cargo gdl build`"), "{stderr}");
    assert_eq!(snapshot(&workspace), before);
}

/// One build at a time in a target directory, from the render to the copy:
/// two specs can build binaries of one name, which Cargo puts at one path.
/// A build that waits says so.
#[test]
fn a_build_waits_for_another_in_its_target_directory() {
    let project = Project::chat();
    let scratch = tempfile::Builder::new()
        .prefix("gdl-build-wait-")
        .tempdir()
        .expect("scratch");
    let target = scratch.path().join("target");
    std::fs::create_dir_all(target.join("gdl")).expect("directory");
    let held = std::fs::File::create(target.join("gdl/build.lock")).expect("the lock");
    held.lock().expect("hold the lock");
    let mut waiting = common::Running(
        Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .args(["gdl", "build", "--spec"])
            .arg(project.spec())
            .arg("--target-dir")
            .arg(&target)
            .env("CARGO_NET_OFFLINE", "true")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("cargo gdl build"),
    );
    let line = waiting.stderr().find(
        "waiting for another `cargo gdl build`",
        Duration::from_secs(30),
    );
    assert!(line.contains(&target.display().to_string()), "{line}");
    assert!(
        !target.join("gdl/chat").exists(),
        "the waiting build wrote before it held the lock"
    );
}

/// A symlink in a generated crate slot must not let generation write the project.
#[test]
#[cfg(unix)]
fn a_build_keeps_a_project_behind_a_generated_crate_symlink_untouched() {
    let project = Project::chat();
    let scratch = tempfile::Builder::new()
        .prefix("gdl-build-symlink-")
        .tempdir()
        .expect("scratch");
    let target = scratch.path().join("target");
    let workspace = target.join("gdl/chat/gdl/workspace");
    std::fs::create_dir_all(&workspace).expect("a generated workspace");
    std::fs::write(
        workspace.join(".gdl-generated"),
        "@generated by gdl-gen; do not edit: `cargo gdl build` made this directory.\n",
    )
    .expect("the workspace's stamp");
    std::os::unix::fs::symlink(project.path().join("engine-mock"), workspace.join("engine"))
        .expect("a symlink the build did not make");
    let manifest = project.path().join("engine-mock/Cargo.toml");
    let before = std::fs::read(&manifest).expect("the project's manifest");

    let output = build(&project.spec(), &target, &scratch.path().join("bin"));

    assert!(
        std::fs::read(&manifest).expect("the project's manifest") == before,
        "the build overwrote a source manifest through a foreign symlink; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "the foreign symlink was accepted");
}

/// A link above the workspace must not redirect generation into the project.
#[test]
#[cfg(unix)]
fn a_build_keeps_a_project_behind_a_generated_parent_symlink_untouched() {
    let project = Project::chat();
    let scratch = tempfile::Builder::new()
        .prefix("gdl-build-parent-")
        .tempdir()
        .expect("scratch");
    let target = scratch.path().join("target");
    std::fs::create_dir_all(target.join("gdl")).expect("build directory");
    std::os::unix::fs::symlink(project.path(), target.join("gdl/chat"))
        .expect("a symlink above the workspace");
    let before = snapshot(project.path());

    let output = build(&project.spec(), &target, &scratch.path().join("bin"));

    assert!(
        snapshot(project.path()) == before,
        "the build wrote in the project through a workspace ancestor; source manifest exists: {}; stderr:\n{}",
        project.path().join("gdl/workspace/Cargo.toml").exists(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "the foreign symlink was accepted");
}
