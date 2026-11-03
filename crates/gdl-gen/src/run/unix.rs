//! The segments of `cargo gdl run` as child processes, and the signals
//! that stop them.

use std::os::fd::AsFd;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rustix::process::{Pid, Signal, kill_process};

use super::Segment;
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

/// Start `segment`. The source's segment (`front`) takes this process's
/// stdin and stdout. The others read nothing, and write to its stderr,
/// so that stdout holds only the answers.
pub(super) fn launch(segment: &Segment, front: bool) -> Result<Running, Error> {
    let mut command = Command::new(&segment.binary);
    command.arg(&segment.settings);
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
