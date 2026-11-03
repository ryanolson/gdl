//! A cancellation diagnostic must not hold settings cleanup open.

#![cfg(target_os = "linux")]

mod common;

/// With stderr a full pipe, `run` removes its settings directory before it
/// writes its warning, which then waits for the pipe as any write does.
/// Draining stderr lets it exit.
///
/// It takes about 7 s: the stop's SIGTERM, SIGKILL, and give-up take 3 s,
/// and the test watches for 4 s before it drains.
#[test]
#[allow(unsafe_code)]
fn a_full_stderr_pipe_does_not_hold_the_settings_cleanup() {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
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
            }
            let _ = self.run.0.wait();
            if let Some(group) = preparation {
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
        .prefix("gdl-diagnostic-")
        .tempdir()
        .expect("scratch");
    let dir = scratch.path();
    let temporary = dir.join("temporary");
    std::fs::create_dir(&temporary).expect("temporary");
    let ready = dir.join("ready");
    let cargo = dir.join("cargo");
    std::fs::write(
        &cargo,
        r#"#!/bin/sh
if [ "$1" = build ]; then
  printf '%s\n' "$$" > "$PROBE_READY"
  dd if=/dev/zero bs=1048576 count=1 >&2
  exit 1
fi
exec "$REAL_CARGO" "$@"
"#,
    )
    .expect("cargo fixture");
    std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).expect("executable");
    let real = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut family = Family {
        run: common::Running(
            Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
                .current_dir(common::root())
                .args(["run", "--spec", "examples/chat/gdl.toml", "--target-dir"])
                .arg(dir.join("target"))
                .env("CARGO", cargo)
                .env("REAL_CARGO", real)
                .env("CARGO_NET_OFFLINE", "true")
                .env("TMPDIR", &temporary)
                .env("PROBE_READY", &ready)
                .process_group(0)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .expect("run"),
        ),
        preparation: None,
    };
    // The real metadata calls precede the controlled build, and can wait
    // for the package cache lock shared by Cargo processes on the host.
    let deadline = Instant::now() + Duration::from_secs(120);
    let cargo = loop {
        if let Some(pid) = std::fs::read_to_string(&ready)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "build did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    let stat = std::fs::read_to_string(format!("/proc/{cargo}/stat")).expect("cargo state");
    let (_, fields) = stat.rsplit_once(") ").expect("fields");
    let group = fields.split_whitespace().nth(2).expect("preparation group");
    family.preparation = Pid::from_raw(group.parse().expect("group pid"));
    let stderr = family.run.0.stderr.as_ref().expect("stderr").as_raw_fd();
    let capacity = unsafe { libc::fcntl(stderr, libc::F_GETPIPE_SZ) };
    assert!(capacity > 0, "pipe capacity");
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let mut available: libc::c_int = 0;
        assert_eq!(
            unsafe { libc::ioctl(stderr, libc::FIONREAD, &mut available) },
            0
        );
        if available == capacity {
            break;
        }
        assert!(Instant::now() < deadline, "stderr did not fill");
        std::thread::sleep(Duration::from_millis(10));
    }
    let pid = Pid::from_raw(family.run.0.id() as i32).expect("run pid");
    kill_process(pid, Signal::TERM).expect("stop run");
    let deadline = Instant::now() + Duration::from_secs(4);
    let before_drain = loop {
        let status = family.run.0.try_wait().expect("run state");
        if status.is_some() || Instant::now() >= deadline {
            break status;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let settings = std::fs::read_dir(&temporary).expect("temporary").count();
    let wchan =
        std::fs::read_to_string(format!("/proc/{}/wchan", family.run.0.id())).unwrap_or_default();
    // Draining is the control: the blocked warning must release cleanup.
    let mut stderr = family.run.0.stderr.take().expect("stderr");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = stderr.read_to_end(&mut bytes);
        let _ = tx.send(bytes);
    });
    let after_drain = family.run.exits_within(Duration::from_secs(2));
    let bytes = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("closed stderr");
    let warning = String::from_utf8_lossy(&bytes).contains("which `run` cannot reap");
    assert_eq!(
        settings, 0,
        "after 4 s, before the drain: run={before_drain:?}, wchan={wchan}, settings={settings}"
    );
    assert!(warning, "the warning was written after the cleanup");
    assert!(!after_drain.success(), "run was stopped: {after_drain}");
}
