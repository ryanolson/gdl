//! Cancellation during preparation must clean up the run.

#![cfg(target_os = "linux")]

mod common;

#[test]
fn terminating_run_during_preparation_removes_its_settings() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal, kill_process, kill_process_group};

    // The fixture must be cleaned up even if its supervisor is already gone.
    struct Group(common::Running);
    impl Drop for Group {
        fn drop(&mut self) {
            let group = Pid::from_raw(self.0.0.id() as i32).expect("group");
            let _ = kill_process_group(group, Signal::KILL);
        }
    }

    let scratch = tempfile::Builder::new()
        .prefix("gdl-preparation-")
        .tempdir()
        .expect("scratch");
    let dir = scratch.path();
    let temporary = dir.join("temporary");
    std::fs::create_dir(&temporary).expect("temporary");
    let cargo = dir.join("cargo");
    let ready = dir.join("ready");
    std::fs::write(
        &cargo,
        "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$PROBE_READY\"\n\
         while [ ! -e \"$PROBE_RELEASE\" ]; do sleep 0.01; done\n",
    )
    .expect("cargo fixture");
    std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).expect("executable");
    let mut run = Group(common::Running(
        Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .current_dir(common::root())
            .args(["run", "--spec", "examples/chat/gdl.toml", "--target-dir"])
            .arg(dir.join("target"))
            .env("CARGO", cargo)
            .env("TMPDIR", &temporary)
            .env("PROBE_READY", &ready)
            .env("PROBE_RELEASE", dir.join("release"))
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("run"),
    ));
    let deadline = Instant::now() + Duration::from_secs(3);
    while !ready.exists() {
        assert!(Instant::now() < deadline, "cargo did not start");
        std::thread::sleep(Duration::from_millis(10));
    }
    let settings = std::fs::read_dir(&temporary)
        .expect("temporary")
        .map(|entry| entry.expect("directory").path())
        .collect::<Vec<_>>();
    assert_eq!(settings.len(), 1, "run created its settings directory");
    let pid = Pid::from_raw(run.0.0.id() as i32).expect("run pid");
    kill_process(pid, Signal::TERM).expect("SIGTERM to run alone");
    let status = run.0.exits_within(Duration::from_secs(2));
    let cargo_pid = std::fs::read_to_string(&ready).expect("cargo pid");
    let cargo_alive = std::path::Path::new(&format!("/proc/{}", cargo_pid.trim())).exists();
    assert!(
        settings.iter().all(|path| !path.exists()),
        "SIGTERM during preparation: run={status}, cargo_alive={cargo_alive}, settings remain: {settings:?}"
    );
}
