//! The segments of `cargo gdl run` as child processes, and the signals
//! that stop them.

use std::os::fd::AsFd;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rustix::process::{Pid, Signal, kill_process};

use super::{Plan, Segment};
use crate::Error;

/// How often the children are looked at. A segment's exit is seen within
/// this, which is far below any drain.
const POLL: Duration = Duration::from_millis(20);

/// The stops `run` has received.
#[derive(Clone, Default)]
pub(super) struct Stops {
    /// A SIGINT: the terminal sent it to every segment as well.
    pub(super) interrupted: Arc<AtomicBool>,
    /// A SIGTERM: to `run` alone.
    pub(super) terminated: Arc<AtomicBool>,
}

impl Stops {
    /// The stops this process receives from here on. A caught signal,
    /// unlike an ignored one, is not inherited across `exec`, so each
    /// segment takes its own SIGINT and drains.
    pub(super) fn caught() -> Result<Self, Error> {
        let stops = Self::default();
        let register = |signal, flag: &Arc<AtomicBool>| {
            signal_hook::flag::register(signal, Arc::clone(flag))
                .map_err(|e| Error::new(format!("catching signal {signal}: {e}")))
        };
        register(signal_hook::consts::SIGINT, &stops.interrupted)?;
        register(signal_hook::consts::SIGTERM, &stops.terminated)?;
        Ok(stops)
    }

    /// Whether any stop has come.
    fn any(&self) -> bool {
        self.interrupted.load(Ordering::Relaxed) || self.terminated.load(Ordering::Relaxed)
    }
}

/// A segment's process, killed if `run` returns before it is reaped.
pub(super) struct Running {
    name: String,
    child: Child,
    grace: Duration,
    status: Option<ExitStatus>,
    /// Whether `run` killed it, once it outlived its grace.
    killed: bool,
}

impl Running {
    pub(super) fn spawn(name: &str, command: &mut Command, grace: Duration) -> Result<Self, Error> {
        let child = command
            .spawn()
            .map_err(|e| Error::new(format!("segment `{name}`: {e}")))?;
        Ok(Self {
            name: name.to_string(),
            child,
            grace,
            status: None,
            killed: false,
        })
    }

    /// Its exit status, once it has exited and been reaped.
    fn reap(&mut self) -> Result<Option<ExitStatus>, Error> {
        if self.status.is_none() {
            self.status = self
                .child
                .try_wait()
                .map_err(|e| Error::new(format!("segment `{}`: {e}", self.name)))?;
        }
        Ok(self.status)
    }

    /// One SIGTERM, unless it has been reaped: a reaped process's id may
    /// already name another process.
    fn stop(&self) {
        if self.status.is_none() {
            let pid = i32::try_from(self.child.id()).ok().and_then(Pid::from_raw);
            if let Some(pid) = pid {
                let _ = kill_process(pid, Signal::TERM);
            }
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if self.status.is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// How long a stopped preparation's process group may take to end on its
/// SIGTERM before it is killed.
const PREPARE_GRACE: Duration = Duration::from_secs(2);

/// Run the preparation `prepare` in a process group of its own, while this
/// process watches `stops`. Its stdin is closed and its stdout goes to
/// stderr: this process's stdout carries only answers. A stop sends
/// SIGTERM to the group, and SIGKILL after [`PREPARE_GRACE`], and returns
/// once the group is empty.
///
/// The terminal's SIGINT does not reach the group: this process takes it
/// and stops the group itself, Cargo's compilers among it.
pub(super) fn preparing(stops: &Stops, mut prepare: Command) -> Result<(), Error> {
    use std::os::unix::process::CommandExt;

    let stdout = std::io::stderr()
        .as_fd()
        .try_clone_to_owned()
        .map_err(|e| Error::new(format!("the preparation: {e}")))?;
    prepare
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .process_group(0);
    let child = prepare
        .spawn()
        .map_err(|e| Error::new(format!("the preparation: {e}")))?;
    let mut group = Group::of(child)?;
    let mut stopped: Option<Instant> = None;
    loop {
        let status = group
            .leader
            .try_wait()
            .map_err(|e| Error::new(format!("the preparation: {e}")))?;
        match stopped {
            None if stops.any() => {
                group.signal(Signal::TERM);
                stopped = Some(Instant::now());
            }
            None => {
                if let Some(status) = status {
                    group.reaped = true;
                    return match status.success() {
                        true => Ok(()),
                        // It printed its problem on stderr.
                        false => Err(Error::new(format!(
                            "preparing the segments failed: {status}"
                        ))),
                    };
                }
            }
            Some(since) => {
                if status.is_some() {
                    group.reaped = true;
                }
                if group.reaped && group.is_empty() {
                    return Err(Error::new("stopped while preparing the segments"));
                }
                if since.elapsed() > PREPARE_GRACE {
                    group.signal(Signal::KILL);
                }
            }
        }
        std::thread::sleep(POLL);
    }
}

/// A preparation's process group: its leader, and whatever it started.
/// Killed whole if `run` returns before it ends.
struct Group {
    leader: Child,
    id: Pid,
    reaped: bool,
}

impl Group {
    fn of(leader: Child) -> Result<Self, Error> {
        let id = i32::try_from(leader.id())
            .ok()
            .and_then(Pid::from_raw)
            .ok_or_else(|| Error::new("the preparation has no process id"))?;
        Ok(Self {
            leader,
            id,
            reaped: false,
        })
    }

    fn signal(&self, signal: Signal) {
        let _ = rustix::process::kill_process_group(self.id, signal);
    }

    /// Whether no process is left in the group. Its id stays the group's
    /// while any member lives, so it names no other group.
    fn is_empty(&self) -> bool {
        rustix::process::test_kill_process_group(self.id).is_err()
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        if !self.reaped || !self.is_empty() {
            self.signal(Signal::KILL);
            let _ = self.leader.wait();
        }
    }
}

/// Start the segments of `plan`, and run them until each has exited; see
/// [`run`](super::run). Called from the thread that lives as long as
/// `run`: on Linux a segment's death signal follows the thread that
/// started it, not the process.
pub(super) fn supervise(plan: &Plan, stops: &Stops) -> Result<(), Error> {
    // A SIGINT from before the start reached no segment, and a wait for
    // them to drain on it would never end.
    if stops.any() {
        return Err(Error::new("stopped before the segments started"));
    }
    let others = plan
        .others
        .iter()
        .map(|segment| launch(segment, false))
        .collect::<Result<_, _>>()?;
    watch(launch(&plan.front, true)?, others, stops)
}

/// Start `segment`. The source's segment (`front`) takes this process's
/// stdin and stdout. The others read nothing, and write to its stderr,
/// so that stdout holds only the answers.
fn launch(segment: &Segment, front: bool) -> Result<Running, Error> {
    let mut command = Command::new(&segment.binary);
    command.arg(&segment.settings);
    #[cfg(target_os = "linux")]
    stop_with_run(&mut command);
    if !front {
        let stderr = std::io::stderr()
            .as_fd()
            .try_clone_to_owned()
            .map_err(|e| Error::new(format!("segment `{}`: {e}", segment.name)))?;
        command.stdin(Stdio::null()).stdout(Stdio::from(stderr));
    }
    Running::spawn(&segment.name, &mut command, segment.grace)
}

/// Run `front` and `others` until every one has exited, stopping them as
/// `stops` and their exits say; see [`run`](super::run).
pub(super) fn watch(front: Running, others: Vec<Running>, stops: &Stops) -> Result<(), Error> {
    let mut all = Vec::with_capacity(others.len() + 1);
    all.push(front);
    all.extend(others);
    let mut first = None;
    let mut stopping: Option<Instant> = None;
    loop {
        for (index, running) in all.iter_mut().enumerate() {
            if running.reap()?.is_some() && first.is_none() {
                first = Some(index);
            }
        }
        if all.iter().all(|running| running.status.is_some()) {
            break;
        }
        // Read after the reap: the terminal's SIGINT reaches this process
        // as it reaches the segments, so a segment that exited on it is
        // never sent a second stop, which would end its peers' drain at
        // once.
        let interrupted = stops.interrupted.load(Ordering::Relaxed);
        let terminated = stops.terminated.load(Ordering::Relaxed);
        match stopping {
            None if interrupted => stopping = Some(Instant::now()),
            None if first.is_some() || terminated => {
                for running in &all {
                    running.stop();
                }
                stopping = Some(Instant::now());
            }
            None => {}
            Some(since) => {
                for running in &mut all {
                    if running.status.is_none() && since.elapsed() > running.grace {
                        let _ = running.child.kill();
                        running.status = running.child.wait().ok();
                        running.killed = true;
                    }
                }
            }
        }
        std::thread::sleep(POLL);
    }
    let stopped =
        stops.interrupted.load(Ordering::Relaxed) || stops.terminated.load(Ordering::Relaxed);
    outcome(&all, first, stopped)
}

/// What `run` reports once every segment has exited, the first to exit
/// (`first`, by index) named first. Each segment that failed or was killed
/// is a problem, and so is one other than the source's (index 0) that
/// exited first when no stop (`stopped`) came: the graph could no longer
/// answer.
fn outcome(all: &[Running], first: Option<usize>, stopped: bool) -> Result<(), Error> {
    let mut problems = Vec::new();
    let order = first
        .into_iter()
        .chain((0..all.len()).filter(|&index| Some(index) != first));
    for index in order {
        let running = &all[index];
        match running.status {
            _ if running.killed => problems.push(format!(
                "segment `{}` did not stop within {} s, and was killed",
                running.name,
                running.grace.as_secs()
            )),
            Some(status) if !status.success() => {
                problems.push(format!("segment `{}` failed: {status}", running.name));
            }
            _ if Some(index) == first && index != 0 && !stopped => problems.push(format!(
                "segment `{}` exited before segment `{}` ended",
                running.name, all[0].name
            )),
            _ => {}
        }
    }
    match problems.is_empty() {
        true => Ok(()),
        false => Err(Error::new(problems.join("; "))),
    }
}

/// Have the process `command` starts get a SIGTERM when `run` dies, by
/// any signal, SIGKILL among them: no handler of `run`'s could tell it.
/// If `run` died before the death signal was set, the process does not
/// start. The child takes the default action for SIGTERM and SIGINT
/// before the death signal is set: until exec it still has `run`'s
/// handlers, which would take a death signal and let it start.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn stop_with_run(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    let run = rustix::process::getpid();
    // SAFETY: the closure runs in the child between fork and exec, where
    // only async-signal-safe calls are sound. `signal`, `sigemptyset`, and
    // `sigprocmask` are, by POSIX, and `sigemptyset` initializes the set
    // before `sigprocmask` reads it. The others are system calls through
    // rustix, which neither allocates nor takes a lock. Turning an error
    // code into an `io::Error` does not allocate. In this order (the
    // default action, no blocked signal, the death signal, the parent
    // check), a death signal from any point on ends the child, before or
    // after exec.
    unsafe {
        command.pre_exec(move || {
            for signal in [libc::SIGTERM, libc::SIGINT] {
                if libc::signal(signal, libc::SIG_DFL) == libc::SIG_ERR {
                    return Err(std::io::Error::last_os_error());
                }
            }
            // A mask inherited from the thread that started it would hold the
            // death signal pending, never delivered.
            let mut none = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
            if libc::sigemptyset(none.as_mut_ptr()) != 0
                || libc::sigprocmask(libc::SIG_SETMASK, none.as_ptr(), std::ptr::null_mut()) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            rustix::process::set_parent_process_death_signal(Some(Signal::TERM))?;
            match rustix::process::getppid() {
                Some(parent) if parent == run => Ok(()),
                _ => Err(rustix::io::Errno::SRCH.into()),
            }
        });
    }
}

#[cfg(all(test, target_os = "linux"))]
#[test]
#[allow(unsafe_code)]
fn a_parent_death_after_the_pid_check_still_stops_the_child() {
    use std::os::unix::process::CommandExt;

    const HELPER: &str = "GDL_TEST_DEATH_WINDOW";
    if std::env::var_os(HELPER).is_some() {
        let _stops = Stops::caught().expect("handlers");
        let mut command = Command::new("/bin/sleep");
        command.arg("60");
        stop_with_run(&mut command);
        // Pause after the guard to make a death before exec observable.
        unsafe {
            command.pre_exec(|| {
                kill_process(rustix::process::getpid(), Signal::STOP).map_err(std::io::Error::from)
            });
        }
        let mut child = command.spawn().expect("child");
        let _ = child.kill();
        let _ = child.wait();
        return;
    }

    struct Family(Child);
    impl Drop for Family {
        fn drop(&mut self) {
            let group = Pid::from_raw(self.0.id() as i32).expect("group");
            let _ = rustix::process::kill_process_group(group, Signal::KILL);
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut parent = Family(
        Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "run::unix::a_parent_death_after_the_pid_check_still_stops_the_child",
            ])
            .env(HELPER, "child")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("parent fixture"),
    );
    let state = |pid: u32| {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let (_, rest) = stat.rsplit_once(") ")?;
        let mut fields = rest.split_whitespace();
        Some((
            fields.next()?.to_string(),
            fields.next()?.parse::<u32>().ok()?,
        ))
    };
    let deadline = Instant::now() + Duration::from_secs(3);
    let child = loop {
        let tasks =
            std::fs::read_dir(format!("/proc/{}/task", parent.0.id())).expect("parent threads");
        let mut paused = None;
        for task in tasks.flatten() {
            if let Ok(children) = std::fs::read_to_string(task.path().join("children")) {
                paused = children
                    .split_whitespace()
                    .filter_map(|pid| pid.parse::<u32>().ok())
                    .find(|pid| state(*pid).is_some_and(|(state, _)| state == "T"));
                if paused.is_some() {
                    break;
                }
            }
        }
        if let Some(child) = paused {
            break child;
        }
        assert!(Instant::now() < deadline, "child did not reach the guard");
        std::thread::sleep(Duration::from_millis(10));
    };
    // A live member with a parent outside the group prevents an orphaned-group
    // SIGHUP from masking the death-signal race.
    let _keeper = Running::spawn(
        "keeper",
        Command::new("/bin/sleep")
            .arg("60")
            .process_group(i32::try_from(parent.0.id()).expect("process group"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
        Duration::ZERO,
    )
    .expect("group keeper");
    parent.0.kill().expect("SIGKILL to parent alone");
    parent.0.wait().expect("reap parent");
    let pending_term = std::fs::read_to_string(format!("/proc/{child}/status"))
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with("SigPnd:") || line.starts_with("ShdPnd:"))
        .filter_map(|line| u64::from_str_radix(line.split_whitespace().nth(1)?, 16).ok())
        .any(|bits| bits & (1 << 14) != 0);
    let pid = Pid::from_raw(child as i32).expect("child pid");
    let _ = kill_process(pid, Signal::CONT);
    let deadline = Instant::now() + Duration::from_secs(2);
    let remaining = loop {
        let remaining = state(child);
        if remaining.as_ref().is_none_or(|(state, _)| state == "Z") || Instant::now() >= deadline {
            break remaining;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let executed_sleep = std::fs::read_link(format!("/proc/{child}/exe")).ok()
        == std::fs::canonicalize("/bin/sleep").ok();
    assert!(
        remaining.as_ref().is_none_or(|(state, _)| state == "Z"),
        "child survived parent death before exec: pending_SIGTERM={pending_term}, executed_sleep={executed_sleep}, state={remaining:?}"
    );
}

#[cfg(all(test, target_os = "linux"))]
#[test]
#[allow(unsafe_code)]
fn a_blocked_signal_in_the_parent_does_not_orphan_the_child() {
    use std::os::unix::process::CommandExt;

    const RECORD: &str = "GDL_TEST_MASK_RECORD";
    if let Some(record) = std::env::var_os(RECORD) {
        let _stops = Stops::caught().expect("handlers");
        // The mask belongs to this fixture's spawning thread.
        unsafe {
            let mut mask = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
            assert_eq!(libc::sigemptyset(mask.as_mut_ptr()), 0);
            assert_eq!(libc::sigaddset(mask.as_mut_ptr(), libc::SIGTERM), 0);
            assert_eq!(
                libc::pthread_sigmask(libc::SIG_BLOCK, mask.as_ptr(), std::ptr::null_mut()),
                0
            );
        }
        let mut command = Command::new("/bin/sleep");
        command.arg("60");
        stop_with_run(&mut command);
        let mut child = command.spawn().expect("child");
        std::fs::write(record, child.id().to_string()).expect("record child");
        let _ = child.wait();
        return;
    }

    struct Family(Child);
    impl Drop for Family {
        fn drop(&mut self) {
            let group = Pid::from_raw(self.0.id() as i32).expect("group");
            let _ = rustix::process::kill_process_group(group, Signal::KILL);
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let scratch = tempfile::Builder::new()
        .prefix("gdl-mask-")
        .tempdir()
        .expect("scratch");
    let record = scratch.path().join("child");
    let mut parent = Family(
        Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "run::unix::a_blocked_signal_in_the_parent_does_not_orphan_the_child",
            ])
            .env(RECORD, &record)
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("parent fixture"),
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    let child = loop {
        if let Some(pid) = std::fs::read_to_string(&record)
            .ok()
            .and_then(|text| text.parse::<u32>().ok())
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "child did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    parent.0.kill().expect("SIGKILL to parent alone");
    parent.0.wait().expect("reap parent");
    let deadline = Instant::now() + Duration::from_secs(2);
    let alive = loop {
        let alive = std::fs::read_to_string(format!("/proc/{child}/stat"))
            .ok()
            .and_then(|stat| {
                stat.rsplit_once(") ")
                    .map(|(_, rest)| !rest.starts_with("Z "))
            })
            .unwrap_or(false);
        if !alive || Instant::now() >= deadline {
            break alive;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let status = std::fs::read_to_string(format!("/proc/{child}/status")).unwrap_or_default();
    let bits = |key: &str| {
        status
            .lines()
            .find(|line| line.starts_with(key))
            .and_then(|line| u64::from_str_radix(line.split_whitespace().nth(1)?, 16).ok())
            .unwrap_or(0)
    };
    let term = 1 << (libc::SIGTERM - 1);
    assert!(
        !alive,
        "child survived parent death: SIGTERM_blocked={}, SIGTERM_pending={}",
        bits("SigBlk:") & term != 0,
        (bits("SigPnd:") | bits("ShdPnd:")) & term != 0
    );
}
