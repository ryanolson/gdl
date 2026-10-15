//! The chat example end to end: the committed segment crates are what the
//! generator produces from `examples/chat/gdl.toml`; each segment builds
//! with only the implementations it hosts; and the three segment binaries,
//! run as separate processes, answer requests.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    dir.parent()
        .and_then(Path::parent)
        .expect("the crate sits two levels below the root")
        .to_path_buf()
}

fn example() -> PathBuf {
    root().join("examples/chat")
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
fn the_committed_segments_are_current() {
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let out = example().join("segments");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    let drift = gdl_gen::drift(&out, &files).expect("drift");
    assert!(
        drift.is_empty(),
        "regenerate with `cargo run -p gdl-gen -- --spec examples/chat/gdl.toml --out examples/chat/segments`:\n{}",
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

/// A child process killed when the test ends, however it ends.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
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
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );
    let bin = target("examples").join("debug");

    let dir = std::env::temp_dir().join(format!("gdl-chat-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
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
        let mut command = Command::new(bin.join(name));
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
    let _ = std::fs::remove_dir_all(&dir);
}
