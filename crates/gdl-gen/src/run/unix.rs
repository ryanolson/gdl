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

/// Run `prepare` on a thread of its own, while this one watches `stops`.
/// A stop sends SIGTERM to each process that preparation started, Cargo
/// among them, and preparation then fails.
pub(super) fn preparing<T: Send>(
    stops: &Stops,
    prepare: impl FnOnce() -> Result<T, Error> + Send,
) -> Result<T, Error> {
    std::thread::scope(|scope| {
        let worker = scope.spawn(prepare);
        let mut stopped = false;
        while !worker.is_finished() {
            if !stopped && stops.any() {
                stopped = true;
                stop_children();
            }
            std::thread::sleep(POLL);
        }
        let prepared = worker
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        match stopped || stops.any() {
            true => Err(Error::new("stopped while preparing the segments")),
            false => prepared,
        }
    })
}

/// Send SIGTERM to each child of this process, of any of its threads.
#[cfg(target_os = "linux")]
fn stop_children() {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return;
    };
    for task in tasks.flatten() {
        let Ok(children) = std::fs::read_to_string(task.path().join("children")) else {
            continue;
        };
        for child in children.split_whitespace() {
            if let Some(pid) = child.parse().ok().and_then(Pid::from_raw) {
                let _ = kill_process(pid, Signal::TERM);
            }
        }
    }
}

/// Elsewhere a process cannot list its children: the build runs on to
/// its end, unless the terminal stopped it too.
#[cfg(not(target_os = "linux"))]
fn stop_children() {}

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
/// start.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn stop_with_run(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    let run = rustix::process::getpid();
    // SAFETY: the closure runs in the child between fork and exec, where
    // only async-signal-safe calls are sound. It makes two system calls
    // through rustix, which neither allocates nor takes a lock, and turns
    // an error code into an `io::Error`, which does not allocate.
    unsafe {
        command.pre_exec(move || {
            rustix::process::set_parent_process_death_signal(Some(Signal::TERM))?;
            match rustix::process::getppid() {
                Some(parent) if parent == run => Ok(()),
                _ => Err(rustix::io::Errno::SRCH.into()),
            }
        });
    }
}
