//! The launcher as a process: this test binary runs it again, as the
//! launcher (`launcher_entry`), its canary (`canary_entry`), and a joiner
//! that sets its own death signal (`joiner_entry`); a bash fake segment
//! counts the `TERM`s it gets. Each test waits only for what it reads,
//! a few seconds at most. One test or more for each invariant:
//!
//! - I1: each request reaches exactly one terminal record.
//! - I2: a joiner gets at most one `TERM` from any source the launcher
//!   decides for.
//! - I3: a joiner lives only as long as its launcher.
//! - I4: a joiner is alive or has exited, never taken for neither.

use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use gdl_model::launch::{self, Done, Up};

use super::*;

const LAUNCHER: &str = "launcher::linux::tests::launcher_entry";
const CANARY_ENTRY: &str = "launcher::linux::tests::canary_entry";
const JOINER: &str = "launcher::linux::tests::joiner_entry";

/// How long any one thing the launcher writes is waited for.
const WAIT: Duration = Duration::from_secs(10);

/// The launcher, when this binary runs as one: its arguments in
/// `GDL_TEST_LAUNCH`, as JSON. With `GDL_TEST_CHURN`, threads come and go
/// beside it the whole time.
#[test]
fn launcher_entry() {
    let Ok(json) = std::env::var("GDL_TEST_LAUNCH") else {
        return;
    };
    if std::env::var_os("GDL_TEST_CHURN").is_some() {
        std::thread::spawn(|| {
            loop {
                let _ = std::thread::spawn(|| std::thread::sleep(Duration::from_millis(1))).join();
            }
        });
    }
    let args: Vec<String> = serde_json::from_str(&json).expect("the launcher's arguments");
    let args: Vec<OsString> = args.into_iter().map(OsString::from).collect();
    let me = std::env::current_exe().expect("this binary");
    let canary = vec![
        me.into_os_string(),
        "--exact".into(),
        CANARY_ENTRY.into(),
        "--nocapture".into(),
    ];
    let code = crate::launcher::run_launcher(&args, canary);
    std::process::exit(if code == ExitCode::SUCCESS { 0 } else { 1 });
}

/// The canary, when this binary runs as one: its launcher's pid comes last.
#[test]
fn canary_entry() {
    if std::env::var_os("GDL_TEST_LAUNCH").is_none() {
        return;
    }
    let pid = std::env::args_os().last().expect("the launcher's pid");
    let _ = canary_main(&[pid]);
}

/// A joiner that sets its own death signal, as a segment run with
/// `--launched-by` does; then waits to be killed.
#[test]
fn joiner_entry() {
    let Ok(pid) = std::env::var("GDL_TEST_LAUNCHED_BY") else {
        return;
    };
    if launched_by(Some(std::ffi::OsStr::new(&pid))).is_err() {
        std::process::exit(3);
    }
    let out = std::env::var("FAKE_OUT").expect("its output");
    std::fs::write(
        Path::new(&out).join(format!("pid.{}", std::process::id())),
        "",
    )
    .expect("its pid");
    loop {
        std::thread::park();
    }
}

/// A segment that writes its pid; counts each `TERM` in `terms.<pid>`;
/// drains for `FAKE_DRAIN_US` after its first, and exits 0; exits at once
/// with `FAKE_EXIT`; or, with `FAKE_ORPHAN`, leaves a child that outlives
/// it and exits 7. It waits through `wait`, which a signal interrupts, so
/// that its trap runs on each `TERM` as it comes.
const SEGMENT: &str = r#"#!/bin/bash
out=$FAKE_OUT
echo $$ > "$out/pid.$$"
draining=0
trap 'echo term >> "$out/terms.$$"; draining=1' TERM
[ -n "$FAKE_ORPHAN" ] && { (sleep 0.2; exit 7) & exit 0; }
[ -n "$FAKE_EXIT" ] && exit "$FAKE_EXIT"
while [ "$draining" = 0 ]; do sleep 0.01 & wait $!; done
until=$(( ${EPOCHREALTIME/./} + ${FAKE_DRAIN_US:-200000} ))
while [ "${EPOCHREALTIME/./}" -lt "$until" ]; do sleep 0.01 & wait $!; done
exit 0
"#;

/// Runs this binary as a joiner that sets its death signal.
const JOINER_SCRIPT: &str = r#"#!/bin/bash
[ "$1" = --launched-by ] || exit 64
GDL_TEST_LAUNCHED_BY=$2 exec "$TEST_EXE" --exact launcher::linux::tests::joiner_entry --nocapture
"#;

struct Launcher {
    process: Child,
    scratch: tempfile::TempDir,
    dir: PathBuf,
}

impl Launcher {
    /// A launcher whose joiner is the fake segment.
    fn start(env: &[(&str, &str)]) -> Self {
        Self::with("segment.sh", SEGMENT, env)
    }

    /// A launcher whose joiner is `script`, named `name`.
    fn with(name: &str, script: &str, env: &[(&str, &str)]) -> Self {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-launcher-")
            .tempdir()
            .expect("a scratch directory");
        let joiner = scratch.path().join(name);
        std::fs::write(&joiner, script).expect("the joiner");
        std::fs::set_permissions(&joiner, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        let out = scratch.path().join("out");
        std::fs::create_dir(&out).expect("its output");
        let args = vec![
            "--root".to_string(),
            scratch.path().display().to_string(),
            "--node".into(),
            "node-7".into(),
            "--segment".into(),
            "engine".into(),
            "--poll-ms".into(),
            "10".into(),
            "--".into(),
            joiner.display().to_string(),
        ];
        let me = std::env::current_exe().expect("this binary");
        let mut command = Command::new(&me);
        command
            .args(["--exact", LAUNCHER, "--nocapture"])
            .env(
                "GDL_TEST_LAUNCH",
                serde_json::to_string(&args).expect("JSON"),
            )
            .env("SLURM_JOB_ID", "42")
            .env("SLURM_STEP_ID", "9")
            .env("GDL_LISTEN", "10.0.0.7:0")
            .env("FAKE_OUT", &out)
            .env("TEST_EXE", &me)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .process_group(0);
        for (name, value) in env {
            command.env(name, value);
        }
        let dir = scratch
            .path()
            .join(launch::launcher_directory("42", "node-7", "engine"));
        let launcher = Self {
            process: command.spawn().expect("the launcher"),
            scratch,
            dir,
        };
        launcher.until("it is up", || launcher.record(launch::UP).is_some());
        launcher
    }

    fn record(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(self.dir.join(name)).ok()
    }

    fn done(&self, n: u64) -> Option<Done> {
        self.record(&launch::done(n))
            .and_then(|text| Done::read(&text))
    }

    /// Write record `name` as the login side does: whole, or not at all.
    fn write(&self, name: &str, text: &str) {
        let staged = self.dir.join(format!(".{name}.test.tmp"));
        std::fs::write(&staged, text).expect("staged");
        std::fs::rename(&staged, self.dir.join(name)).expect("renamed");
    }

    fn until(&self, what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !done() {
            assert!(Instant::now() < deadline, "never: {what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn out(&self) -> PathBuf {
        self.scratch.path().join("out")
    }

    /// Request `n`'s joiner, once it has written its pid.
    fn joiner(&self, n: u64) -> i32 {
        let mut pid = 0;
        self.until("the joiner starts", || {
            let Some(started) = self.record(&launch::started(n)) else {
                return false;
            };
            pid = started.trim().parse().expect("a pid");
            self.out().join(format!("pid.{pid}")).exists()
        });
        pid
    }

    fn terms(&self, pid: i32) -> usize {
        std::fs::read_to_string(self.out().join(format!("terms.{pid}")))
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn pid(&self) -> i32 {
        i32::try_from(self.process.id()).expect("a pid")
    }

    fn signal(&self, pid: i32, signal: Signal) {
        if let Some(pid) = Pid::from_raw(pid) {
            let _ = rustix::process::kill_process(pid, signal);
        }
    }

    /// The launcher's children, by `/proc`.
    fn children(&self) -> Vec<i32> {
        processes()
            .into_iter()
            .filter(|(_, parent, _)| *parent == self.pid())
            .map(|(pid, _, _)| pid)
            .collect()
    }

    /// The canary: the launcher's child that runs `canary_entry`.
    fn canary(&self) -> i32 {
        self.children()
            .into_iter()
            .find(|pid| {
                std::fs::read_to_string(format!("/proc/{pid}/cmdline"))
                    .is_ok_and(|cmdline| cmdline.contains("canary_entry"))
            })
            .expect("the canary")
    }

    fn exit(&mut self) -> Option<i32> {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.process.try_wait().expect("a state") {
                return status.code();
            }
            assert!(Instant::now() < deadline, "the launcher did not exit");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Launcher {
    fn drop(&mut self) {
        let mut tree = vec![self.pid()];
        let all = processes();
        let mut at = 0;
        while at < tree.len() {
            let of = tree[at];
            tree.extend(
                all.iter()
                    .filter(|(_, parent, _)| *parent == of)
                    .map(|(pid, _, _)| *pid),
            );
            at += 1;
        }
        for pid in tree {
            if let Some(pid) = Pid::from_raw(pid) {
                let _ = rustix::process::kill_process(pid, Signal::KILL);
            }
        }
        let _ = self.process.wait();
    }
}

/// Every process, as `(pid, parent, state)`.
fn processes() -> Vec<(i32, i32, char)> {
    std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let pid: i32 = entry.file_name().to_str()?.parse().ok()?;
            let stat = std::fs::read_to_string(entry.path().join("stat")).ok()?;
            let mut rest = stat.rsplit_once(") ")?.1.split_whitespace();
            let state = rest.next()?.chars().next()?;
            let parent = rest.next()?.parse().ok()?;
            Some((pid, parent, state))
        })
        .collect()
}

/// Whether process `pid` runs: neither gone nor a zombie.
fn running(pid: i32) -> bool {
    processes()
        .iter()
        .any(|(each, _, state)| *each == pid && *state != 'Z')
}

/// I1: each request ends in exactly one terminal record, whatever subset
/// of `run` and `stop` its records hold, either of them cut short (only a
/// staged file, which a reader never opens) or missing; and the launcher
/// goes on to the next request.
#[test]
fn each_request_reaches_one_terminal_record() {
    #[derive(Clone, Copy, Debug)]
    enum Record {
        Missing,
        CutShort,
        Whole,
    }
    use Record::*;
    for (run, stop) in [
        (Missing, Missing),
        (Missing, CutShort),
        (Missing, Whole),
        (CutShort, Missing),
        (CutShort, Whole),
        (Whole, Missing),
        (Whole, CutShort),
        (Whole, Whole),
    ] {
        let case = format!("run {run:?}, stop {stop:?}");
        let launcher = Launcher::start(&[]);
        for (name, record) in [(launch::stop(1), stop), (launch::run(1), run)] {
            match record {
                Missing => {}
                CutShort => std::fs::write(launcher.dir.join(format!(".{name}.77.tmp")), "a")
                    .expect("a cut-short record"),
                Whole => launcher.write(&name, "a1\n"),
            }
        }
        let runs = matches!(run, Whole);
        let stops = matches!(stop, Whole);
        if runs && !stops {
            let joiner = launcher.joiner(1);
            launcher.write(&launch::stop(1), "a1\n");
            launcher.until("1 is done", || launcher.done(1).is_some());
            assert_eq!(launcher.done(1), Some(Done::Exited(0)), "{case}");
            assert_eq!(launcher.terms(joiner), 1, "{case}");
        } else if stops {
            launcher.until("1 is done", || launcher.done(1).is_some());
            assert_eq!(launcher.done(1), Some(Done::Dropped), "{case}");
            assert!(launcher.record(&launch::started(1)).is_none(), "{case}");
        } else {
            // Neither whole: nothing yet, until a stop closes it.
            std::thread::sleep(Duration::from_millis(100));
            assert!(launcher.done(1).is_none(), "{case}");
            launcher.write(&launch::stop(1), "a1\n");
            launcher.until("1 is done", || launcher.done(1).is_some());
            assert_eq!(launcher.done(1), Some(Done::Dropped), "{case}");
        }
        // On to the next request, in order.
        launcher.write(&launch::run(2), "b2\n");
        launcher.until("2 starts", || {
            launcher.record(&launch::started(2)).is_some()
        });
    }
}

/// The launcher says where its joiners listen, and its own step.
#[test]
fn the_launcher_says_its_step() {
    let launcher = Launcher::start(&[]);
    let up = Up::read(&launcher.record(launch::UP).expect("up")).expect("a step");
    assert_eq!(
        up,
        Up {
            address: "10.0.0.7".into(),
            step: "42.9".into()
        }
    );
}

/// I2: a stop, then the launcher's own `TERM`, then another: one `TERM`.
#[test]
fn a_stop_and_two_terms_send_one_term() {
    let mut launcher = Launcher::start(&[]);
    launcher.write(&launch::run(1), "a1\n");
    let joiner = launcher.joiner(1);
    launcher.write(&launch::stop(1), "a1\n");
    launcher.until("it took its TERM", || launcher.terms(joiner) == 1);
    launcher.signal(launcher.pid(), Signal::TERM);
    launcher.signal(launcher.pid(), Signal::TERM);
    assert_eq!(launcher.exit(), Some(0));
    assert_eq!(launcher.terms(joiner), 1);
    assert!(launcher.record(launch::CLOSED).is_some());
    assert_eq!(launcher.done(1), Some(Done::Exited(0)));
}

/// I2: the launcher's own `TERM` alone reaches its joiner, once.
#[test]
fn the_launchers_term_reaches_its_joiner_once() {
    let mut launcher = Launcher::start(&[]);
    launcher.write(&launch::run(1), "a1\n");
    let joiner = launcher.joiner(1);
    launcher.signal(launcher.pid(), Signal::TERM);
    assert_eq!(launcher.exit(), Some(0));
    assert_eq!(launcher.terms(joiner), 1);
}

/// I2: where Slurm signals every process of the step, the joiner gets the
/// step's `TERM`, the canary dies of it, and the launcher adds none; also
/// with the canary stopped when the `TERM` came.
#[test]
fn where_slurm_signals_every_process_the_joiner_gets_one_term() {
    for stopped in [false, true] {
        let mut launcher = Launcher::start(&[("FAKE_DRAIN_US", "1000000")]);
        launcher.write(&launch::run(1), "a1\n");
        let joiner = launcher.joiner(1);
        let canary = launcher.canary();
        if stopped {
            launcher.signal(canary, Signal::STOP);
        }
        for pid in [launcher.pid(), canary, joiner] {
            launcher.signal(pid, Signal::TERM);
        }
        assert_eq!(launcher.exit(), Some(0));
        assert_eq!(launcher.terms(joiner), 1, "stopped canary: {stopped}");
    }
}

/// I2: a step's `TERM` to every process, and then a stop: one `TERM`.
/// The stop's decision asks the canary too.
#[test]
fn a_step_term_and_then_a_stop_send_one_term() {
    let mut launcher = Launcher::start(&[("FAKE_DRAIN_US", "1000000")]);
    launcher.write(&launch::run(1), "a1\n");
    let joiner = launcher.joiner(1);
    for pid in [launcher.canary(), joiner, launcher.pid()] {
        launcher.signal(pid, Signal::TERM);
    }
    launcher.write(&launch::stop(1), "a1\n");
    assert_eq!(launcher.exit(), Some(0));
    assert_eq!(launcher.terms(joiner), 1);
}

/// I2: a request read after the launcher took a `TERM`, its own or its
/// canary's, never starts: a joiner started then would get none.
#[test]
fn a_request_after_a_term_never_starts() {
    for every in [false, true] {
        let mut launcher = Launcher::start(&[]);
        if every {
            launcher.signal(launcher.canary(), Signal::TERM);
        }
        launcher.signal(launcher.pid(), Signal::TERM);
        launcher.write(&launch::run(1), "a1\n");
        let _ = launcher.exit();
        assert!(
            launcher.record(&launch::started(1)).is_none(),
            "every: {every}"
        );
    }
}

/// I3: a joiner dies when its launcher is killed, by `SIGKILL` to the
/// launcher alone, or to its process group, as `proctrack/pgid` ends a
/// step; and a joiner whose launcher is gone before it set its death
/// signal does not start.
#[test]
fn a_joiner_dies_with_its_launcher() {
    for group in [false, true] {
        let mut launcher = Launcher::with("joiner.sh", JOINER_SCRIPT, &[]);
        launcher.write(&launch::run(1), "a1\n");
        let joiner = launcher.joiner(1);
        assert!(running(joiner));
        match group {
            true => {
                let group = Pid::from_raw(launcher.pid()).expect("a pid");
                let _ = rustix::process::kill_process_group(group, Signal::KILL);
            }
            false => launcher.signal(launcher.pid(), Signal::KILL),
        }
        let _ = launcher.exit();
        launcher.until("the joiner is gone", || !running(joiner));
    }
    // In a process of its own: the death signal it sets is its own.
    let out = tempfile::Builder::new()
        .prefix("gdl-launcher-orphaned-")
        .tempdir()
        .expect("a scratch directory");
    let status = Command::new(std::env::current_exe().expect("this binary"))
        .args(["--exact", JOINER, "--nocapture"])
        .env("GDL_TEST_LAUNCHED_BY", "1")
        .env("FAKE_OUT", out.path())
        .stdout(Stdio::null())
        .status()
        .expect("a joiner");
    assert_eq!(status.code(), Some(3), "its launcher is not its parent");
}

/// I3: the death signal follows the thread that started the joiner; the
/// launcher starts it from its loop's thread, which lives as long as the
/// launcher, so threads beside it coming and going do not end the joiner.
#[test]
fn a_joiner_outlives_the_launchers_other_threads() {
    let launcher = Launcher::with("joiner.sh", JOINER_SCRIPT, &[("GDL_TEST_CHURN", "1")]);
    launcher.write(&launch::run(1), "a1\n");
    let joiner = launcher.joiner(1);
    std::thread::sleep(Duration::from_millis(500));
    assert!(running(joiner), "threads came and went beside the launcher");
}

/// I3: an orphan its joiner leaves, the launcher's as its subreaper, is
/// reaped; and its exit code is never taken for the joiner's.
#[test]
fn an_orphan_is_reaped_and_never_taken_for_the_joiner() {
    let launcher = Launcher::start(&[("FAKE_ORPHAN", "1")]);
    launcher.write(&launch::run(1), "a1\n");
    launcher.until("1 is done", || launcher.done(1).is_some());
    assert_eq!(launcher.done(1), Some(Done::Exited(0)));
    launcher.until("no zombie child", || {
        std::thread::sleep(Duration::from_millis(300));
        processes()
            .iter()
            .all(|(_, parent, state)| *parent != launcher.pid() || *state != 'Z')
    });
}

/// I4: a stopped joiner, resumed, is never taken for one that exited: it
/// ends only when it exits.
#[test]
fn a_stopped_joiner_has_not_exited() {
    let launcher = Launcher::start(&[]);
    launcher.write(&launch::run(1), "a1\n");
    let joiner = launcher.joiner(1);
    for _ in 0..5 {
        launcher.signal(joiner, Signal::STOP);
        std::thread::sleep(Duration::from_millis(30));
        launcher.signal(joiner, Signal::CONT);
    }
    std::thread::sleep(Duration::from_millis(100));
    assert!(launcher.done(1).is_none());
    launcher.write(&launch::stop(1), "a1\n");
    launcher.until("1 is done", || launcher.done(1).is_some());
    assert_eq!(launcher.done(1), Some(Done::Exited(0)));
}

/// A joiner's own exit is its terminal record, with its code.
#[test]
fn a_joiners_exit_is_its_record() {
    let launcher = Launcher::start(&[("FAKE_EXIT", "3")]);
    launcher.write(&launch::run(1), "a1\n");
    launcher.until("1 is done", || launcher.done(1).is_some());
    assert_eq!(launcher.done(1), Some(Done::Exited(3)));
}

/// A kernel without pidfd is refused in one line that names it.
#[test]
fn a_kernel_without_pidfd_is_refused() {
    for errno in [rustix::io::Errno::NOSYS, rustix::io::Errno::INVAL] {
        let refused = floor(Err(errno)).expect_err("refused");
        assert!(
            refused.contains("Linux 5.4 or later (pidfd); this kernel is "),
            "{refused}"
        );
        assert_eq!(refused.lines().count(), 1);
    }
    assert!(floor(Ok(())).is_ok());
    // Linux 5.3 has `pidfd_open` but not `waitid` on a pidfd, which the
    // probe asks for: this kernel has both.
    assert_eq!(probe(rustix::process::getpid()), Ok(()));
}

/// The launcher's options: each required, and refused by name otherwise.
#[test]
fn the_launchers_options_are_read_or_refused() {
    let words = |text: &str| -> Vec<OsString> { text.split(' ').map(OsString::from).collect() };
    let options = Options::read(&words(
        "--root /gdl --node n1 --segment engine --poll-ms 5 -- /bin/segment /gdl/engine.toml",
    ))
    .expect("read");
    assert_eq!(options.poll, Duration::from_millis(5));
    assert_eq!(options.command, words("/bin/segment /gdl/engine.toml"));
    for (text, named) in [
        ("--root /gdl --node n1 --segment engine", "`--`"),
        ("--root /gdl --node n1 --segment engine --", "command"),
        ("--node n1 --segment engine -- x", "`--root`"),
        ("--root /gdl --segment engine -- x", "`--node`"),
        ("--root /gdl --node n1 -- x", "`--segment`"),
        ("--root /gdl --node .. --segment engine -- x", "`--node`"),
        (
            "--root /gdl --node n1 --segment engine --poll-ms 0 -- x",
            "--poll-ms 0",
        ),
        (
            "--root /gdl --node n1 --segment engine --other 1 -- x",
            "`--other`",
        ),
    ] {
        let refused = Options::read(&words(text)).expect_err(text);
        assert!(refused.contains(named), "{named} in {refused}");
    }
}

/// A wait on the joiner's pidfd that fails is an error, which ends the
/// launcher, and its joiner with it: a failed wait taken as "not exited
/// yet" would leave the joiner's request open for good.
#[test]
fn a_failed_wait_is_an_error() {
    assert!(reaped(Err(rustix::io::Errno::INVAL)).is_err());
    assert_eq!(reaped(Ok(None)), Ok(None));
}
