//! Preparation cancellation must reach Cargo's compiler processes.

#![cfg(target_os = "linux")]

mod common;

#[test]
fn cancelling_preparation_stops_cargos_descendants() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal, kill_process, kill_process_group};

    struct Group(common::Running);
    impl Drop for Group {
        fn drop(&mut self) {
            let group = Pid::from_raw(self.0.0.id() as i32).expect("group");
            let _ = kill_process_group(group, Signal::KILL);
        }
    }

    let scratch = tempfile::Builder::new()
        .prefix("gdl-descendants-")
        .tempdir()
        .expect("scratch");
    let dir = scratch.path();
    let temporary = dir.join("temporary");
    std::fs::create_dir(&temporary).expect("temporary");
    let compiler = dir.join("compiler");
    let ready = dir.join("ready");
    std::fs::write(
        &compiler,
        "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$PROBE_READY\"\nexec sleep 60\n",
    )
    .expect("compiler fixture");
    std::fs::set_permissions(&compiler, std::fs::Permissions::from_mode(0o755))
        .expect("executable");
    let mut run = Group(common::Running(
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
    ));
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
    let stat = std::fs::read_to_string(format!("/proc/{compiler}/stat")).expect("compiler state");
    let (_, rest) = stat.rsplit_once(") ").expect("compiler fields");
    let cargo = rest.split_whitespace().nth(1).expect("compiler's parent");
    assert_eq!(
        std::fs::read_to_string(format!("/proc/{cargo}/comm"))
            .expect("compiler parent")
            .trim(),
        "cargo",
        "real Cargo started the compiler fixture"
    );
    let pid = Pid::from_raw(run.0.0.id() as i32).expect("run pid");
    kill_process(pid, Signal::TERM).expect("SIGTERM to run alone");
    let deadline = Instant::now() + Duration::from_secs(3);
    let (status, compiler_alive, settings) = loop {
        let status = run.0.0.try_wait().expect("run state");
        let state = std::fs::read_to_string(format!("/proc/{compiler}/stat"))
            .ok()
            .and_then(|stat| stat.rsplit_once(") ").map(|(_, rest)| rest.to_string()));
        let compiler_alive = state.as_ref().is_some_and(|state| !state.starts_with("Z "));
        let settings = std::fs::read_dir(&temporary).expect("temporary").count();
        if (status.is_some() && !compiler_alive && settings == 0) || Instant::now() >= deadline {
            break (status, compiler_alive, settings);
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.is_some() && !compiler_alive && settings == 0,
        "after SIGTERM: run={status:?}, compiler_alive={compiler_alive}, settings_directories={settings}"
    );
}

/// Ctrl-C while Cargo compiles: the terminal's SIGINT reaches `run`
/// alone, since the preparation has a process group of its own, and `run`
/// stops that group. No Cargo or compiler is left, and the settings
/// directory is removed. The compiler is a stand-in that passes Cargo's
/// questions to the real one and hangs on the first crate it compiles.
#[test]
fn an_interrupt_during_the_build_stops_cargo_and_its_compilers() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal, kill_process_group};

    struct Group(common::Running);
    impl Drop for Group {
        fn drop(&mut self) {
            let group = Pid::from_raw(self.0.0.id() as i32).expect("group");
            let _ = kill_process_group(group, Signal::KILL);
        }
    }

    let sysroot = Command::new("rustc")
        .current_dir(common::root())
        .args(["--print", "sysroot"])
        .output()
        .expect("rustc");
    let real = std::path::Path::new(String::from_utf8(sysroot.stdout).expect("utf-8").trim())
        .join("bin/rustc");
    let scratch = tempfile::Builder::new()
        .prefix("gdl-descendants-")
        .tempdir()
        .expect("scratch");
    let dir = scratch.path();
    let temporary = dir.join("temporary");
    std::fs::create_dir(&temporary).expect("temporary");
    let compiler = dir.join("compiler");
    let ready = dir.join("ready");
    std::fs::write(
        &compiler,
        format!(
            "#!/bin/sh\nprev=\nfor a in \"$@\"; do\n  \
             if [ \"$prev\" = --crate-name ] && [ \"$a\" != ___ ]; then\n    \
             printf '%s\\n' \"$$\" > \"$PROBE_READY\"; exec sleep 60\n  fi\n  prev=$a\ndone\n\
             exec '{}' \"$@\"\n",
            real.display()
        ),
    )
    .expect("compiler fixture");
    std::fs::set_permissions(&compiler, std::fs::Permissions::from_mode(0o755))
        .expect("executable");
    let mut run = Group(common::Running(
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
    ));
    // The check reads the spec's crates with Cargo before the build
    // starts: nested cargo, which waits for the package cache lock that
    // every cargo on the host shares.
    let deadline = Instant::now() + Duration::from_secs(120);
    let compiler = loop {
        if let Some(pid) = std::fs::read_to_string(&ready)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "the build did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    let parent = |pid: u32| -> Option<u32> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let (_, rest) = stat.rsplit_once(") ")?;
        rest.split_whitespace().nth(1)?.parse().ok()
    };
    let cargo = parent(compiler).expect("the compiler's parent");
    assert_eq!(
        std::fs::read_to_string(format!("/proc/{cargo}/comm"))
            .expect("cargo")
            .trim(),
        "cargo",
        "real Cargo started the compiler"
    );
    let alive = |pid: u32| {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|stat| {
                stat.rsplit_once(") ")
                    .map(|(_, rest)| !rest.starts_with("Z "))
            })
            .unwrap_or(false)
    };
    let group = Pid::from_raw(run.0.0.id() as i32).expect("run's group");
    kill_process_group(group, Signal::INT).expect("SIGINT to run's group");
    let status = run.0.exits_within(Duration::from_secs(5));
    assert!(!status.success(), "run was stopped: {status}");
    let deadline = Instant::now() + Duration::from_secs(3);
    while alive(compiler) || alive(cargo) {
        assert!(
            Instant::now() < deadline,
            "after SIGINT: compiler alive {}, cargo alive {}",
            alive(compiler),
            alive(cargo)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        std::fs::read_dir(&temporary).expect("temporary").count(),
        0,
        "the settings directory is removed"
    );
}
