//! Cancellation must account for build descendants that start a session.

#![cfg(target_os = "linux")]

mod common;

#[test]
#[ignore = "F6: a compiler that calls setsid survives preparation cancellation"]
fn cancelling_preparation_stops_a_compiler_in_another_session() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal, kill_process, kill_process_group};

    struct Family {
        run: common::Running,
        preparation: Option<u32>,
        compiler: Option<u32>,
    }
    impl Drop for Family {
        fn drop(&mut self) {
            let preparation = self.preparation.or_else(|| {
                std::fs::read_to_string(format!("/proc/{0}/task/{0}/children", self.run.0.id()))
                    .ok()?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            });
            let root = Pid::from_raw(self.run.0.id() as i32).expect("run group");
            let _ = kill_process_group(root, Signal::KILL);
            for raw in preparation.into_iter().chain(self.compiler) {
                let pid = Pid::from_raw(raw as i32).expect("fixture pid");
                let _ = kill_process_group(pid, Signal::KILL);
                let _ = kill_process(pid, Signal::KILL);
            }
        }
    }

    let scratch = tempfile::Builder::new()
        .prefix("gdl-escaping-")
        .tempdir()
        .expect("scratch");
    let dir = scratch.path();
    let temporary = dir.join("temporary");
    std::fs::create_dir(&temporary).expect("temporary");
    let compiler = dir.join("compiler");
    let ready = dir.join("ready");
    std::fs::write(
        &compiler,
        r#"#!/bin/sh
exec setsid sh -c 'printf "%s\n" "$$" > "$PROBE_READY"; exec sleep 60'
"#,
    )
    .expect("compiler fixture");
    std::fs::set_permissions(&compiler, std::fs::Permissions::from_mode(0o755))
        .expect("executable");
    let mut family = Family {
        run: common::Running(
            Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
                .current_dir(common::root())
                .args(["run", "--spec", "examples/chat/gdl.toml", "--target-dir"])
                .arg(dir.join("target"))
                .env("RUSTC", compiler)
                .env("CARGO_NET_OFFLINE", "true")
                .env("TMPDIR", &temporary)
                .env("PROBE_READY", &ready)
                .process_group(0)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("run"),
        ),
        preparation: None,
        compiler: None,
    };
    let deadline = Instant::now() + Duration::from_secs(3);
    let compiler = loop {
        if let Some(pid) = std::fs::read_to_string(&ready)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "compiler did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    family.compiler = Some(compiler);
    let fields = |pid: u32| {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).expect("process state");
        stat.rsplit_once(") ")
            .expect("fields")
            .1
            .split_whitespace()
            .take(4)
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    let state = fields(compiler);
    assert_eq!(state[2], compiler.to_string(), "new process group");
    assert_eq!(state[3], compiler.to_string(), "new session");
    let cargo = state[1].parse::<u32>().expect("cargo pid");
    family.preparation = Some(fields(cargo)[1].parse().expect("preparation pid"));
    let pid = Pid::from_raw(family.run.0.id() as i32).expect("run pid");
    kill_process(pid, Signal::TERM).expect("SIGTERM to run alone");
    let status = family.run.exits_within(Duration::from_secs(4));
    let alive = std::fs::read_to_string(format!("/proc/{compiler}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(") ")
                .map(|(_, rest)| !rest.starts_with("Z "))
        })
        .unwrap_or(false);
    assert!(
        !alive,
        "after cancellation: run={status}, escaped_compiler_alive={alive}"
    );
}
