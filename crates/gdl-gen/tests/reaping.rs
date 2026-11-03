//! Dead preparation descendants must not hold cancellation open.

#![cfg(target_os = "linux")]

mod common;

#[test]
fn cancelled_preparation_finishes_before_its_ancestor_reaps_orphans() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal, WaitOptions, kill_process, kill_process_group};

    struct Reaper(Option<Pid>);
    impl Drop for Reaper {
        fn drop(&mut self) {
            let _ = rustix::process::set_child_subreaper(self.0);
        }
    }
    struct Family {
        run: common::Running,
        preparation: Option<Pid>,
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
                    .and_then(Pid::from_raw)
            });
            let root = Pid::from_raw(self.run.0.id() as i32).expect("run group");
            let _ = kill_process_group(root, Signal::KILL);
            if let Some(group) = preparation {
                let _ = kill_process_group(group, Signal::KILL);
                let _ = kill_process(group, Signal::KILL);
                let deadline = Instant::now() + Duration::from_secs(2);
                while Instant::now() < deadline {
                    match rustix::process::waitpgid(group, WaitOptions::NOHANG) {
                        Ok(Some(_)) => {}
                        Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                        Err(_) => break,
                    }
                }
            }
        }
    }

    let _reaper = Reaper(rustix::process::child_subreaper().expect("subreaper state"));
    rustix::process::set_child_subreaper(Some(rustix::process::getpid()))
        .expect("become subreaper");
    let scratch = tempfile::Builder::new()
        .prefix("gdl-reaping-")
        .tempdir()
        .expect("scratch");
    let dir = scratch.path();
    let temporary = dir.join("temporary");
    std::fs::create_dir(&temporary).expect("temporary");
    let ready = dir.join("ready");
    let compiler = dir.join("compiler");
    std::fs::write(
        &compiler,
        "#!/bin/sh\ntrap '' TERM\nprintf '%s\\n' \"$$\" > \"$PROBE_READY\"\nexec sleep 60\n",
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
    };
    let deadline = Instant::now() + Duration::from_secs(3);
    let orphan = loop {
        if let Some(pid) = std::fs::read_to_string(&ready)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "compiler did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    let stat = std::fs::read_to_string(format!("/proc/{orphan}/stat")).expect("compiler state");
    let (_, rest) = stat.rsplit_once(") ").expect("compiler fields");
    let group = rest.split_whitespace().nth(2).expect("preparation group");
    let group = Pid::from_raw(group.parse().expect("group pid")).expect("group");
    family.preparation = Some(group);
    let run = Pid::from_raw(family.run.0.id() as i32).expect("run pid");
    kill_process(run, Signal::TERM).expect("stop run");
    let deadline = Instant::now() + Duration::from_secs(4);
    let before_reap = loop {
        let status = family.run.0.try_wait().expect("run state");
        if status.is_some() || Instant::now() >= deadline {
            break status;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let zombie = std::fs::read_to_string(format!("/proc/{orphan}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(") ")
                .map(|(_, rest)| rest.starts_with("Z "))
        })
        .unwrap_or(false);
    // Reaping is the control: it must release the same cancellation.
    while let Ok(Some(_)) = rustix::process::waitpgid(group, WaitOptions::NOHANG) {}
    let after_reap = family.run.exits_within(Duration::from_secs(2));
    assert_eq!(std::fs::read_dir(&temporary).expect("temporary").count(), 0);
    assert!(
        before_reap.is_some(),
        "run still waited after 4 s: orphan_zombie={zombie}; reaping it released run={after_reap}"
    );
}
