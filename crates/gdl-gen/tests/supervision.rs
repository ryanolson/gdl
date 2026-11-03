//! Supervisor death must not leave graph processes running.

#![cfg(target_os = "linux")]

mod common;

/// A killed supervisor must not leave the graph running without an owner.
#[test]
fn killing_run_does_not_orphan_its_segments() {
    use std::io::Write;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal, kill_process_group};

    // Keep cleanup independent of the supervisor whose death is tested.
    struct Group(common::Running);
    impl Drop for Group {
        fn drop(&mut self) {
            let group = Pid::from_raw(self.0.0.id() as i32).expect("group");
            let _ = kill_process_group(group, Signal::KILL);
        }
    }

    common::chat_build();
    let scratch = tempfile::Builder::new()
        .prefix("gdl-supervision-")
        .tempdir()
        .expect("scratch");
    let mut run = Group(common::Running(
        Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .current_dir(common::root())
            .args(["run", "--spec", "examples/chat/gdl.toml", "--target-dir"])
            .arg(common::target("examples"))
            .env("TMPDIR", scratch.path())
            .env("CARGO_NET_OFFLINE", "true")
            .env_remove("RUST_LOG")
            .process_group(0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("run"),
    ));
    let output = run.0.stdout();
    let _stderr = run.0.stderr();
    let mut input = run.0.0.stdin.take().expect("input");
    writeln!(input, r#"{{"text":"hello","max_words":1}}"#).expect("request");
    // The first answer waits for `run`'s check and for a build with nothing
    // to do: nested cargo, which waits for the package cache lock that
    // every cargo on the host shares.
    output.find(r#"{"done":null}"#, Duration::from_secs(120));
    let children = std::fs::read_to_string(format!("/proc/{0}/task/{0}/children", run.0.0.id()))
        .expect("children")
        .split_whitespace()
        .map(|pid| pid.parse::<u32>().expect("pid"))
        .collect::<Vec<_>>();
    assert_eq!(children.len(), 3, "the graph is running");
    run.0.0.kill().expect("SIGKILL to run alone");
    run.0.exits_within(Duration::from_secs(2));

    let deadline = Instant::now() + Duration::from_secs(3);
    let survivors = loop {
        let survivors = children
            .iter()
            .filter_map(|pid| {
                let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
                let (_, rest) = stat.rsplit_once(") ")?;
                let mut fields = rest.split_whitespace();
                let state = fields.next()?;
                let parent = fields.next()?;
                (state != "Z").then_some(format!("{pid}: state={state}, parent={parent}"))
            })
            .collect::<Vec<_>>();
        if survivors.is_empty() || Instant::now() >= deadline {
            break survivors;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        survivors.is_empty(),
        "segments still running after run was killed and reaped: {survivors:?}"
    );
}
