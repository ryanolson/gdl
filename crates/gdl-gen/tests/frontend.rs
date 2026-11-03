//! `cargo gdl run` with a spec whose source is pinned to a frontend: the
//! http example serves until Ctrl-C.

#![cfg(target_os = "linux")]

mod common;

use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use rustix::process::{Pid, Signal, kill_process_group};

/// The target directory of this test alone. A build locks its target
/// directory's `gdl/` for every graph, and other tests build examples
/// for minutes on a busy host: `run`'s bounded wait must not queue
/// behind them.
fn target() -> std::path::PathBuf {
    common::target("frontend")
}

/// A `cargo gdl run` in a process group of its own, killed whole if the
/// test ends first: its group, and the group of its preparation, which
/// `run` starts in a group of its own so that a kill of `run`'s group
/// misses it.
struct Group(common::Running);

impl Drop for Group {
    fn drop(&mut self) {
        let Some(run) = i32::try_from(self.0.0.id()).ok().and_then(Pid::from_raw) else {
            return;
        };
        // Found while `run` lives: once it is reaped, its children are no
        // longer listed as its own.
        let preparations: Vec<Pid> = children(run)
            .into_iter()
            .filter(|&child| rustix::process::getpgid(Some(child)).ok() == Some(child))
            .collect();
        for &group in &preparations {
            let _ = kill_process_group(group, Signal::KILL);
        }
        let _ = kill_process_group(run, Signal::KILL);
        let _ = self.0.0.wait();
        // Reaped by whoever adopts it: this process, if it reaps orphans.
        let deadline = Instant::now() + Duration::from_secs(3);
        for group in preparations {
            while rustix::process::test_kill_process(group).is_ok() && Instant::now() < deadline {
                let _ = rustix::process::waitpid(Some(group), rustix::process::WaitOptions::NOHANG);
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// The children of process `parent`, of any of its threads.
fn children(parent: Pid) -> Vec<Pid> {
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{}/task", parent.as_raw_pid())) else {
        return Vec::new();
    };
    tasks
        .flatten()
        .filter_map(|task| std::fs::read_to_string(task.path().join("children")).ok())
        .flat_map(|text| {
            text.split_whitespace()
                .filter_map(|pid| pid.parse().ok().and_then(Pid::from_raw))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// `run` starts the http example's segment, prints where its frontend
/// listens, and serves there until the terminal's SIGINT, on which every
/// segment drains and exits.
#[test]
fn a_pinned_frontend_serves_until_an_interrupt() {
    let root = common::root();
    let example = root.join("examples/http");
    // A port the system gave out, so that no fixed port is assumed.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port();
    let text = std::fs::read_to_string(example.join("gdl.toml")).expect("the http spec");
    let (spec, _) = text.split_once("[settings.").unwrap_or((&text, ""));
    let spec =
        spec.replace(
            "path = \"http.gdl\"",
            &format!("path = {:?}", example.join("http.gdl")),
        )
        .replace("crates = [\".\"]", &format!("crates = [{:?}]", example))
        .replace(
            "\"../../crates/gdl-runtime\"",
            &format!("{:?}", root.join("crates/gdl-runtime")),
        ) + &format!("\n[settings.main.frontend]\nlisten = \"127.0.0.1:{port}\"\n");
    let scratch = tempfile::Builder::new()
        .prefix("gdl-frontend-")
        .tempdir()
        .expect("scratch");
    let file = scratch.path().join("gdl.toml");
    std::fs::write(&file, spec).expect("the spec");
    // Built first, as `cargo gdl build` builds it, so that no wait below
    // covers a cold build.
    let built = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "build", "--spec"])
        .arg(&file)
        .arg("--target-dir")
        .arg(target())
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo gdl build");
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );

    let mut run = Group(common::Running(
        Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .current_dir(&root)
            .args(["gdl", "run", "--spec"])
            .arg(&file)
            .arg("--target-dir")
            .arg(target())
            .env("CARGO_NET_OFFLINE", "true")
            .env_remove("RUST_LOG")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("cargo gdl run"),
    ));
    let stdout = run.0.stdout();
    let stderr = run.0.stderr();
    // The address comes after the check and a build with nothing to do:
    // nested cargo, which waits for the package cache lock that every
    // cargo on the host shares.
    let deadline = Instant::now() + Duration::from_secs(120);
    let line = loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match stdout.try_next(left.min(Duration::from_secs(1))) {
            Some(line) if line.contains("listens on") => break line,
            Some(_) => {}
            None if Instant::now() < deadline => {}
            None => {
                let mut seen = Vec::new();
                while let Some(line) = stderr.try_next(Duration::from_millis(100)) {
                    seen.push(line);
                }
                panic!("no address within 120 s; stderr: {seen:#?}");
            }
        }
    };
    let address = line
        .rsplit_once("listens on ")
        .map(|(_, address)| address.trim().to_string())
        .expect("an address");
    assert_eq!(address, format!("127.0.0.1:{port}"), "{line}");

    let body = r#"{"text":"hello source","stream":false}"#;
    let request = format!(
        "POST /generate HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    // The frontend serves once the segments have met.
    let deadline = Instant::now() + Duration::from_secs(10);
    let response = loop {
        match std::net::TcpStream::connect(&address) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("a timeout");
                stream.write_all(request.as_bytes()).expect("the request");
                let mut response = String::new();
                stream.read_to_string(&mut response).expect("the response");
                break response;
            }
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "the frontend did not serve: {error}"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    };
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.ends_with(r#"{"text":"hello source"}"#),
        "{response}"
    );

    let children = std::fs::read_to_string(format!("/proc/{0}/task/{0}/children", run.0.0.id()))
        .expect("run's children")
        .split_whitespace()
        .map(|pid| pid.parse::<u32>().expect("a pid"))
        .collect::<Vec<_>>();
    let names: Vec<String> = children
        .iter()
        .map(|pid| {
            std::fs::read_to_string(format!("/proc/{pid}/comm"))
                .expect("a name")
                .trim()
                .to_string()
        })
        .collect();
    assert_eq!(names, ["http-main"]);

    let group = Pid::from_raw(i32::try_from(run.0.0.id()).expect("a pid")).expect("a pid");
    kill_process_group(group, Signal::INT).expect("SIGINT to the group");
    let status = run.0.exits_within(Duration::from_secs(10));
    let errors = stderr.rest(Duration::from_secs(5));
    assert!(status.success(), "{status}: {errors:?}");
    for pid in children {
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "segment {pid} outlived run"
        );
    }
}

/// A failed wait must also stop the preparation, which has its own group.
#[test]
fn a_failed_frontend_wait_reaps_its_preparation() {
    const READY: &str = "GDL_TEST_FRONTEND_PREPARATION";
    if let Some(ready) = std::env::var_os(READY) {
        let mut preparation = Command::new("sh");
        preparation
            .args([
                "-c",
                "printf '%s\\n' \"$$\" > \"$1\" || exit 1; exec sleep 60",
                "sh",
            ])
            .arg(ready);
        let _ = gdl_gen::run(preparation);
        return;
    }

    // Adopt the preparation if the guard kills its parent, so this test
    // can kill and reap its fixture even when the assertion fails.
    struct Subreaper(Option<Pid>);
    impl Drop for Subreaper {
        fn drop(&mut self) {
            let _ = rustix::process::set_child_subreaper(self.0);
        }
    }
    let _subreaper = Subreaper(rustix::process::child_subreaper().expect("subreaper setting"));
    rustix::process::set_child_subreaper(Some(rustix::process::getpid()))
        .expect("adopt orphaned fixtures");

    struct Preparation(Option<Pid>);
    impl Drop for Preparation {
        fn drop(&mut self) {
            if let Some(pid) = self.0 {
                let _ = kill_process_group(pid, Signal::KILL);
                let deadline = Instant::now() + Duration::from_secs(3);
                while matches!(
                    rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG),
                    Ok(None)
                ) {
                    assert!(Instant::now() < deadline, "fixture did not exit");
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    let scratch = tempfile::Builder::new()
        .prefix("gdl-frontend-cleanup-")
        .tempdir()
        .expect("scratch");
    let ready = scratch.path().join("ready");
    let run = Group(common::Running(
        Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", "a_failed_frontend_wait_reaps_its_preparation"])
            .env(READY, &ready)
            .env("TMPDIR", scratch.path())
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("run fixture"),
    ));
    let deadline = Instant::now() + Duration::from_secs(3);
    let pid = loop {
        if let Ok(text) = std::fs::read_to_string(&ready)
            && let Ok(raw) = text.trim().parse()
            && let Some(pid) = Pid::from_raw(raw)
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "preparation did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut preparation = Preparation(Some(pid));
    assert!(rustix::process::test_kill_process(pid).is_ok());
    drop(run);
    let reaped = rustix::process::test_kill_process(pid).is_err();
    if reaped {
        preparation.0 = None;
    }
    assert!(
        reaped,
        "preparation {pid:?} outlived the frontend test's guard"
    );
}
