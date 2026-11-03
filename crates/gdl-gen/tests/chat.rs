//! The chat example end to end: `cargo gdl build` builds its segments from
//! `examples/chat/gdl.toml`; each segment builds with only the
//! implementations it hosts; and the three segment binaries, run as
//! separate processes, answer requests.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use common::{Running, cargo, example, generated, root, target};

/// `cargo gdl diagram --direction tb` draws the same graph top to bottom,
/// and `lr`, or no option, left to right. Any other value, or the option
/// on another command, is a usage error.
#[test]
fn the_diagram_command_takes_a_direction() {
    let spec = example().join("gdl.toml");
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .args(["diagram", "--spec"])
            .arg(&spec)
            .args(args)
            .arg("--target-dir")
            .arg(target("gdl-gen-tests"))
            .output()
            .expect("cargo-gdl")
    };
    let stdout = |args: &[&str]| {
        let output = run(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("utf-8")
    };
    let default = stdout(&["--format", "mermaid"]);
    assert!(default.contains("\nflowchart LR\n"), "{default}");
    assert_eq!(
        default,
        stdout(&["--format", "mermaid", "--direction", "lr"])
    );
    let down = stdout(&["--format", "mermaid", "--direction", "tb"]);
    assert_eq!(
        down,
        default.replace("\nflowchart LR\n", "\nflowchart TB\n")
    );
    let dot = stdout(&["--format", "dot"]);
    assert_eq!(
        stdout(&["--format", "dot", "--direction", "tb"]),
        dot.replace("rankdir=LR;", "rankdir=TB;")
    );
    let output = run(&["--format", "dot", "--direction", "up"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--direction takes lr or tb"),
        "{output:?}"
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["resolve", "--spec"])
        .arg(&spec)
        .args(["--direction", "tb"])
        .output()
        .expect("cargo-gdl");
    assert!(!output.status.success(), "{output:?}");
}

/// The spec's placement draws as a diagram: one cluster per segment, and
/// the three crossings (Truncate to Engine, Engine to Detokenize, Format to
/// Preprocess) marked.
#[test]
fn the_placed_graph_draws_as_a_diagram() {
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let target = target("gdl-gen-tests");
    let mermaid = gdl_gen::diagram(
        &spec,
        &target,
        gdl_gen::Format::Mermaid,
        gdl_gen::Direction::LeftToRight,
    )
    .expect("mermaid");
    for line in [
        r#"subgraph s0["engine"]"#,
        r#"subgraph s1["front"]"#,
        r#"subgraph s2["post"]"#,
        "linkStyle 4,5,7 stroke:#d62728,stroke-width:3px",
    ] {
        assert!(mermaid.contains(line), "{line} in:\n{mermaid}");
    }
    let dot = gdl_gen::diagram(
        &spec,
        &target,
        gdl_gen::Format::Dot,
        gdl_gen::Direction::LeftToRight,
    )
    .expect("dot");
    assert_eq!(dot.matches("subgraph cluster_").count(), 3, "{dot}");
    assert_eq!(dot.matches("penwidth=2").count(), 3, "{dot}");
}

/// `generate` writes crates into the user's workspace, where Cargo reads
/// a profile only from the workspace's root and warns about one in a
/// member: the user's root profile applies, so the crates carry none.
#[test]
fn a_generated_segment_leaves_the_profile_to_its_workspace() {
    let spec = gdl_gen::Spec::load(&example().join("gdl.toml")).expect("spec");
    let temp = scratch("profile");
    let files = gdl_gen::render(
        &spec,
        &temp.path().join("segments"),
        &target("gdl-gen-tests"),
    )
    .expect("render");
    let manifests: Vec<_> = files
        .iter()
        .filter(|(path, _)| path.ends_with("Cargo.toml"))
        .collect();
    assert_eq!(manifests.len(), 4, "{:?}", files.keys());
    for (path, text) in manifests {
        let manifest: toml::Table = text.parse().expect("a manifest");
        assert!(
            !manifest.contains_key("profile"),
            "{}:\n{text}",
            path.display()
        );
    }
}

#[test]
fn only_the_engine_segment_links_the_engine_dependency() {
    let manifest = common::chat_build().workspace.join("Cargo.toml");
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
    let build = common::chat_build();
    // Generated code is built by people who deny warnings. Cargo replays a
    // fresh crate's warnings, so a build with nothing to do shows them too.
    let warned: Vec<&str> = build
        .stderr
        .lines()
        .filter(|line| line.starts_with("warning:") && line.contains("generated"))
        .collect();
    assert!(
        warned.is_empty(),
        "generated crates warned: {warned:?}\n{}",
        build.stderr
    );
    let bin = &build.bin;

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

/// The getting-started guide's `run`, with its request piped in as the
/// guide shows: the answer is the one the guide prints, and every segment
/// that `run` started has exited when it ends.
#[cfg(target_os = "linux")]
#[test]
fn the_guides_run_answers_and_leaves_no_segment() {
    common::chat_build();
    let guide = std::fs::read_to_string(root().join("docs/src/guides/getting-started.md"))
        .expect("the guide");
    let (_, section) = guide.split_once("## Run the graph\n").expect("the section");
    let (section, _) = section
        .split_once("### Run the segments yourself")
        .expect("its end");
    let piped = section
        .lines()
        .find(|line| line.starts_with("echo '"))
        .expect("the piped command");
    let (echo, command) = piped.split_once(" | ").expect("a pipe");
    assert_eq!(
        command,
        "cargo run -p gdl-gen -- run --spec examples/chat/gdl.toml"
    );
    let request = echo
        .strip_prefix("echo '")
        .and_then(|rest| rest.strip_suffix('\''))
        .expect("one quoted request");
    let answer: Vec<&str> = section
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with(r#"{"item""#) || line.starts_with(r#"{"done""#))
        .collect();
    assert_eq!(answer.len(), 4, "{section}");

    let mut run = run_group(Command::new(env!("CARGO_BIN_EXE_cargo-gdl")));
    let stdin = run.answers(request, &answer);
    let segments = segments_of(run.id());
    // The end of the input ends `front`, and `run` stops the others.
    drop(stdin);
    run.ends_cleanly(&segments);
}

/// Ctrl-C at a terminal: the terminal sends SIGINT to its foreground
/// process group, and each segment drains and exits on it. `cargo gdl`
/// runs `cargo-gdl` in cargo's own process, so that no cargo is left to
/// take the SIGINT or pass on a second stop. The input stays open: the
/// SIGINT, not its end, stops the graph.
#[cfg(target_os = "linux")]
#[test]
fn a_terminal_interrupt_stops_every_segment() {
    common::chat_build();
    let built = Path::new(env!("CARGO_BIN_EXE_cargo-gdl"));
    // Cargo looks for `cargo-gdl` in PATH, and first in its home's `bin`
    // unless PATH lists that: this build comes first either way.
    let home = std::env::var_os("CARGO_HOME").map_or_else(
        || Path::new(&std::env::var_os("HOME").expect("HOME")).join(".cargo"),
        std::path::PathBuf::from,
    );
    let path = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        [built.parent().expect("its directory").to_path_buf()]
            .into_iter()
            .chain(std::env::split_paths(&path))
            .chain([home.join("bin")]),
    )
    .expect("PATH");
    let mut command = cargo();
    command.env("PATH", path);
    let mut run = run_group(command);
    let request = r#"{"text": "hello big wide world", "max_words": 3}"#;
    let stdin = run.answers(
        request,
        &[
            r#"{"item":{"index":0,"text":"<WIDE>"}}"#,
            r#"{"item":{"index":1,"text":"<BIG>"}}"#,
            r#"{"item":{"index":2,"text":"<HELLO>"}}"#,
            r#"{"done":null}"#,
        ],
    );
    let exe = std::fs::read_link(format!("/proc/{}/exe", run.id())).expect("its program");
    assert_eq!(
        exe,
        std::fs::canonicalize(built).expect("the build"),
        "cargo is not the process it started"
    );
    let segments = segments_of(run.id());
    let group =
        rustix::process::Pid::from_raw(i32::try_from(run.id()).expect("a pid")).expect("a pid");
    rustix::process::kill_process_group(group, rustix::process::Signal::INT).expect("SIGINT");
    run.ends_cleanly(&segments);
    drop(stdin);
}

/// A preparation that fails ends `run` with its own problem, which it
/// printed: here a spec that cannot be read. The
/// preparation's flag belongs to `run` alone, and no usage line shows it.
#[test]
fn a_failed_preparation_shows_its_own_problem() {
    let run = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .current_dir(root())
        .args([
            "gdl",
            "run",
            "--spec",
            "examples/chat/missing.toml",
            "--target-dir",
        ])
        .arg(target("gdl-gen-tests"))
        .env("CARGO_NET_OFFLINE", "true")
        .stdin(Stdio::null())
        .output()
        .expect("cargo gdl run");
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(!run.status.success(), "{stderr}");
    assert!(stderr.contains("examples/chat/missing.toml"), "{stderr}");
    assert!(!stderr.contains("plan"), "{stderr}");
    assert!(run.stdout.is_empty(), "{:?}", run.stdout);

    let build = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args([
            "gdl",
            "build",
            "--spec",
            "examples/chat/gdl.toml",
            "--prepare-into",
            "x",
        ])
        .output()
        .expect("cargo gdl build");
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(!build.status.success());
    assert!(
        stderr.contains("usage:") && !stderr.contains("prepare"),
        "{stderr}"
    );
}

/// `cargo gdl run` on the chat example, started by `command` (a program
/// that runs `cargo-gdl` as `cargo gdl`), in a process group of its own
/// that is killed if the test ends first.
#[cfg(target_os = "linux")]
fn run_group(mut command: Command) -> Group {
    use std::os::unix::process::CommandExt;
    command
        .current_dir(root())
        .args([
            "gdl",
            "run",
            "--spec",
            "examples/chat/gdl.toml",
            "--target-dir",
        ])
        .arg(target("examples"))
        .env("CARGO_NET_OFFLINE", "true")
        .env_remove("RUST_LOG")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command.spawn().expect("cargo gdl run");
    let stdout = common::Lines::of(child.stdout.take().expect("stdout"));
    let stderr = common::Lines::of(child.stderr.take().expect("stderr"));
    Group {
        child,
        stdout,
        stderr,
    }
}

/// A `cargo gdl run` and its segments, in one process group.
#[cfg(target_os = "linux")]
struct Group {
    child: std::process::Child,
    stdout: common::Lines,
    stderr: common::Lines,
}

#[cfg(target_os = "linux")]
impl Group {
    fn id(&self) -> u32 {
        self.child.id()
    }

    /// Send `request`, and see `answer`; return the input, still open.
    fn answers(&mut self, request: &str, answer: &[&str]) -> std::process::ChildStdin {
        let mut stdin = self.child.stdin.take().expect("stdin");
        writeln!(stdin, "{request}").expect("the request");
        // The first line waits for `run`'s check and for a build with
        // nothing to do: nested cargo, which waits for the package cache
        // lock that every cargo on the host shares.
        let mut lines = vec![self.stdout.next(Duration::from_secs(120))];
        while lines.len() < answer.len() {
            lines.push(self.stdout.next(Duration::from_secs(5)));
        }
        assert_eq!(lines, answer);
        stdin
    }

    /// `run` exits cleanly, with nothing more on stdout, and none of
    /// `segments` outlives it.
    fn ends_cleanly(&mut self, segments: &[(u32, String)]) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("its state") {
                break status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "run did not end; stderr: {:?}",
                self.stderr.rest(Duration::ZERO)
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let stderr = self.stderr.rest(Duration::from_secs(5));
        assert!(status.success(), "{status}: {stderr:?}");
        assert_eq!(
            self.stdout.rest(Duration::from_secs(5)),
            Vec::<String>::new()
        );
        for (pid, name) in segments {
            assert!(
                !Path::new(&format!("/proc/{pid}")).exists(),
                "{name} outlived run"
            );
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for Group {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let group = i32::try_from(self.child.id())
                .ok()
                .and_then(rustix::process::Pid::from_raw);
            if let Some(group) = group {
                let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
            }
            let _ = self.child.wait();
        }
    }
}

/// The processes whose parent is `parent`, by id and name: the chat
/// example's three segments, while `run` runs them.
#[cfg(target_os = "linux")]
fn segments_of(parent: u32) -> Vec<(u32, String)> {
    let mut children = Vec::new();
    for entry in std::fs::read_dir("/proc").expect("/proc").flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        // `pid (name) state ppid ...`: the name may hold spaces and `)`.
        let Some((head, rest)) = stat.rsplit_once(") ") else {
            continue;
        };
        let name = head.split_once(" (").map_or("", |(_, name)| name);
        let ppid = rest.split_whitespace().nth(1).and_then(|p| p.parse().ok());
        if ppid == Some(parent) {
            children.push((pid, name.to_string()));
        }
    }
    let mut names: Vec<&str> = children.iter().map(|(_, name)| name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["chat-engine", "chat-front", "chat-post"]);
    children
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
    let generated = scratch("cwd-out");
    let out = generated.path();
    let generate = generate_into(out, false);
    assert!(
        generate.status.success(),
        "{}",
        String::from_utf8_lossy(&generate.stderr)
    );
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
    // an absolute one must all find the generated output current.
    let dir = example();
    let spec_abs = dir.join("gdl.toml");
    let generated = scratch("spellings");
    let out = generated.path();
    let generate = generate_into(out, false);
    assert!(
        generate.status.success(),
        "{}",
        String::from_utf8_lossy(&generate.stderr)
    );
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
                out.as_os_str(),
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
    gdl_gen::diagram(
        &spec,
        &target,
        gdl_gen::Format::Dot,
        gdl_gen::Direction::LeftToRight,
    )
    .expect("diagram");
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
        std::fs::read_to_string(root().join("docs/src/concepts/pools.md")).expect("chapter");
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
        lib.contains("        anchors Engine_to_Detokenize = Detokenize BackwardDecl In;\n"),
        "{lib}"
    );
}
