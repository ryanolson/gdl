//! The launcher on Linux: one single-threaded event loop over its `TERM`s,
//! its children's ends (`SIGCHLD`), and a timer that reads the next
//! request's records. Every `TERM` to a joiner is decided in that loop,
//! with every input in hand, and a joiner gets at most one from it.

use std::ffi::OsString;
use std::os::fd::{AsFd, OwnedFd};
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

use gdl_model::launch::{self, Done, Up};
use rustix::process::{Pid, PidfdFlags, Signal, WaitId, WaitIdOptions};

use super::{CANARY, LAUNCHED_BY};

mod records;

use records::Records;

/// What a launcher is told on its command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Options {
    /// The pool's directory, as the job mounts it.
    root: PathBuf,
    /// srt-slurm's name for the node.
    node: String,
    segment: String,
    /// How often it reads the next request's records.
    poll: Duration,
    /// The joiner's command: the segment's own.
    command: Vec<OsString>,
}

/// How long the launcher waits for its canary to end, once it has taken a
/// `TERM`: the step's `TERM` reaches every process of the step at once,
/// where Slurm signals them all.
const CANARY_WAIT: Duration = Duration::from_secs(1);

impl Options {
    /// The options in `args`, the arguments after `--launch`.
    pub(super) fn read(args: &[OsString]) -> Result<Self, String> {
        let split = args
            .iter()
            .position(|arg| arg == "--")
            .ok_or("no `--` before the joiner's command")?;
        let (flags, command) = (&args[..split], &args[split + 1..]);
        if command.is_empty() {
            return Err("no joiner's command after `--`".into());
        }
        let mut root = None;
        let mut node = None;
        let mut segment = None;
        let mut poll = Duration::from_millis(1000);
        let mut flags = flags.iter();
        while let Some(flag) = flags.next() {
            let flag = flag.to_string_lossy().into_owned();
            let value = flags
                .next()
                .and_then(|value| value.to_str())
                .ok_or_else(|| format!("`{flag}` takes a value"))?
                .to_string();
            match flag.as_str() {
                "--root" => root = Some(PathBuf::from(value)),
                "--node" => node = Some(value),
                "--segment" => segment = Some(value),
                "--poll-ms" => {
                    let ms: u64 =
                        value.parse().ok().filter(|ms| *ms > 0).ok_or_else(|| {
                            format!("`--poll-ms {value}` is not a positive count")
                        })?;
                    poll = Duration::from_millis(ms);
                }
                other => return Err(format!("`{other}` is not a launcher's option")),
            }
        }
        let name = |text: &str| {
            !text.is_empty()
                && text
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                && text != "."
                && text != ".."
        };
        let node = node
            .filter(|node| name(node))
            .ok_or("`--node` names no node")?;
        let segment = segment
            .filter(|segment| name(segment))
            .ok_or("`--segment` names no segment")?;
        Ok(Self {
            root: root.ok_or("`--root` names no directory")?,
            node,
            segment,
            poll,
            command: command.to_vec(),
        })
    }
}

/// `segment --launch ...`: the launcher of the segment whose binary this
/// is, with this binary as its canary.
pub(super) fn launch_main(args: &[OsString]) -> Result<ExitCode, String> {
    let options = Options::read(args)?;
    let me = std::env::current_exe().map_err(|e| format!("its own binary: {e}"))?;
    launch(options, vec![me.into_os_string(), CANARY.into()])
}

/// `segment --launch-canary <pid>`: wait for a signal, and die of it. Its
/// death signal is its launcher's, so that it ends with the launcher.
pub(super) fn canary_main(args: &[OsString]) -> Result<ExitCode, String> {
    watch_parent(args.first().map(OsString::as_os_str))?;
    loop {
        std::thread::park();
    }
}

/// `segment --launched-by <pid> ...`: die with launcher `pid`, which
/// started this process: its death signal is `SIGKILL`, set first, then
/// its parent checked, so that a launcher that died before the signal was
/// set is not missed.
pub(super) fn launched_by(pid: Option<&std::ffi::OsStr>) -> Result<(), String> {
    watch_parent(pid)
}

fn watch_parent(pid: Option<&std::ffi::OsStr>) -> Result<(), String> {
    let launcher: i32 = pid
        .and_then(|pid| pid.to_str())
        .and_then(|pid| pid.parse().ok())
        .ok_or(format!("`{LAUNCHED_BY}` takes the launcher's pid"))?;
    rustix::process::set_parent_process_death_signal(Some(Signal::KILL))
        .map_err(|e| format!("setting its death signal: {e}"))?;
    let parent = rustix::process::getppid().map_or(0, |pid| pid.as_raw_nonzero().get());
    if parent != launcher {
        return Err(format!(
            "its launcher, pid {launcher}, is gone: its parent is pid {parent}"
        ));
    }
    Ok(())
}

/// Run the launcher of `options`, with `canary` as its canary's command,
/// to which its pid is added: until it has taken a `TERM` and its joiner,
/// if any, has ended. Refused, in one line, on a kernel without pidfd
/// (Linux 5.4), and where Slurm does not say the launcher's job and step.
///
/// It runs on the calling thread, which must outlive the launcher: each
/// child's death signal follows the thread that started it, not the
/// process, so a child started from a thread that ends would be killed
/// with it. The loop runs on a current-thread runtime, and nothing in it
/// uses tokio's blocking pool, whose threads come and go.
pub(super) fn launch(options: Options, canary: Vec<OsString>) -> Result<ExitCode, String> {
    let me = rustix::process::getpid();
    floor(probe(me))?;
    let job = std::env::var("SLURM_JOB_ID").map_err(|_| "Slurm sets no SLURM_JOB_ID")?;
    let step = std::env::var("SLURM_STEP_ID").map_err(|_| "Slurm sets no SLURM_STEP_ID")?;
    let address = std::env::var("GDL_LISTEN")
        .ok()
        .and_then(|listen| listen.rsplit_once(':').map(|(host, _)| host.to_string()))
        .unwrap_or_default();
    // Orphans of its joiners are its own to reap, and die with it.
    rustix::process::set_child_subreaper(Some(me)).map_err(|e| format!("subreaper: {e}"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("its runtime: {e}"))?;
    // Set as the `TERM` comes, in its handler: each decision reads it, so
    // none is made on a `TERM` that came but that the loop has not woken
    // to yet. The stream below only wakes the loop.
    let termed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, std::sync::Arc::clone(&termed))
        .map_err(|e| format!("its TERM: {e}"))?;
    let pending = move || termed.load(std::sync::atomic::Ordering::SeqCst);
    runtime.block_on(async move {
        let mut terms = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| format!("its TERM: {e}"))?;
        let mut children = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
            .map_err(|e| format!("its SIGCHLD: {e}"))?;
        let dir = options.root.join(launch::launcher_directory(
            &job,
            &options.node,
            &options.segment,
        ));
        let records = Records::make(dir)?;
        let mut launcher = Launcher {
            canary: Canary::start(&canary, me)?,
            log: |launch: &str| {
                options
                    .root
                    .join(format!("join.{job}.{}.{launch}.log", options.node))
            },
            command: &options.command,
            records,
            me,
            next: 1,
            child: None,
            term: false,
            closed: false,
        };
        launcher.records.write(
            launch::UP,
            &Up {
                address,
                step: format!("{job}.{step}"),
            }
            .text(),
        )?;
        loop {
            launcher.turn(&pending)?;
            if launcher.closed && launcher.child.is_none() {
                launcher.canary.end();
                return Ok(ExitCode::SUCCESS);
            }
            tokio::select! {
                Some(()) = terms.recv() => {}
                Some(()) = children.recv() => {}
                () = tokio::time::sleep(options.poll) => {}
            }
        }
    })
}

/// The launcher's state, which only its loop changes.
struct Launcher<'a, L: Fn(&str) -> PathBuf> {
    canary: Canary,
    /// Where request `launch`'s joiner writes what it prints.
    log: L,
    command: &'a [OsString],
    records: Records,
    me: Pid,
    /// The request it handles next, in order from 1.
    next: u64,
    child: Option<Child>,
    /// Whether it has taken a `TERM`.
    term: bool,
    /// Whether it has closed: it starts nothing more.
    closed: bool,
}

/// A joiner the launcher started.
struct Child {
    n: u64,
    pid: Pid,
    pidfd: OwnedFd,
    /// Whether it has had its one `TERM`, from the launcher or from Slurm.
    signalled: bool,
}

impl<L: Fn(&str) -> PathBuf> Launcher<'_, L> {
    /// One turn of the loop: take a `TERM`, reap what ended, and read the
    /// next request's records. `pending` says whether a `TERM` has come, as
    /// its handler set it, so that each decision has every input.
    fn turn(&mut self, pending: &impl Fn() -> bool) -> Result<(), String> {
        self.reap_orphans();
        self.term |= pending();
        if self.term && !self.closed {
            self.closed = true;
            self.records.write(launch::CLOSED, "")?;
            self.signal(true);
        }
        if let Some(child) = &self.child
            && let Some(done) = reaped(rustix::process::waitid(
                WaitId::PidFd(child.pidfd.as_fd()),
                WaitIdOptions::EXITED | WaitIdOptions::NOHANG,
            ))?
        {
            let n = child.n;
            self.child = None;
            self.records.write(&launch::done(n), &done.text())?;
            self.next = n + 1;
        }
        while self.child.is_none() {
            let n = self.next;
            if self.records.exists(&launch::done(n)) {
                self.next += 1;
                continue;
            }
            let run = self.records.read(&launch::run(n));
            let stopped = self.records.read(&launch::stop(n)).is_some();
            let Some(run) = run else {
                if stopped {
                    // A stop with no run, as from a `join` that ended
                    // before it posted: its joiner never starts.
                    self.records
                        .write(&launch::done(n), &Done::Dropped.text())?;
                    self.next += 1;
                    continue;
                }
                break;
            };
            self.term |= pending();
            let done = if stopped {
                Some(Done::Dropped)
            } else if self.closed || self.term || self.canary.ended() {
                // A `TERM` it has taken, or that its canary died of: a
                // joiner started now would get none.
                Some(Done::Refused)
            } else {
                None
            };
            if let Some(done) = done {
                self.records.write(&launch::done(n), &done.text())?;
                self.next += 1;
                continue;
            }
            let launch = launch::read_launch(&run).unwrap_or("unnamed").to_string();
            self.start(n, &launch)?;
        }
        if let Some(child) = &self.child
            && !child.signalled
            && self.records.exists(&launch::stop(child.n))
        {
            self.term |= pending();
            self.signal(self.term);
        }
        Ok(())
    }

    /// Start request `n`'s joiner, from the loop's own thread, in the
    /// launcher's process group, with its marker first among its
    /// arguments; then say so.
    fn start(&mut self, n: u64, launch: &str) -> Result<(), String> {
        let log_path = (self.log)(launch);
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| format!("{}: {e}", log_path.display()))?;
        let copy = log
            .try_clone()
            .map_err(|e| format!("{}: {e}", log_path.display()))?;
        let spawned = Command::new(&self.command[0])
            .arg(LAUNCHED_BY)
            .arg(self.me.as_raw_nonzero().to_string())
            .args(&self.command[1..])
            .stdin(Stdio::null())
            .stdout(log)
            .stderr(copy)
            .spawn();
        let child = match spawned {
            Ok(child) => child,
            Err(e) => {
                eprintln!("launch {launch}: {e}");
                self.records
                    .write(&launch::done(n), &Done::Exited(127).text())?;
                self.next = n + 1;
                return Ok(());
            }
        };
        // Its pid stays its own until it is reaped, which only its pidfd
        // does.
        let pid = Pid::from_child(&child);
        let pidfd = rustix::process::pidfd_open(pid, PidfdFlags::empty())
            .map_err(|e| format!("pidfd of its joiner: {e}"))?;
        self.records
            .write(&launch::started(n), &format!("{}\n", pid.as_raw_nonzero()))?;
        self.child = Some(Child {
            n,
            pid,
            pidfd,
            signalled: false,
        });
        Ok(())
    }

    /// Give the joiner its one `TERM`, unless it has had it: on its stop,
    /// or on the launcher's own `TERM` (`term`). Where the canary died of
    /// that `TERM`, Slurm signalled every process of the step, the joiner
    /// among them, and the launcher sends none of its own. The canary is
    /// resumed first, so that a stopped one does not hold that `TERM`.
    fn signal(&mut self, term: bool) {
        let Some(child) = &mut self.child else {
            return;
        };
        if child.signalled {
            return;
        }
        let wait = if term { CANARY_WAIT } else { Duration::ZERO };
        let direct = self.canary.died_of_term(wait);
        child.signalled = true;
        if !direct {
            let _ = rustix::process::pidfd_send_signal(child.pidfd.as_fd(), Signal::TERM);
        }
    }

    /// Reap each orphan that ended and is the launcher's to reap, as its
    /// subreaper: never its joiner or its canary, which only their pidfds
    /// reap, so that no other process's status is taken for theirs.
    fn reap_orphans(&self) {
        let mine = |pid: i32| {
            self.child
                .as_ref()
                .is_some_and(|child| child.pid.as_raw_nonzero().get() == pid)
                || self.canary.pid.as_raw_nonzero().get() == pid
        };
        let me = self.me.as_raw_nonzero().get();
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return;
        };
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<i32>().ok())
            else {
                continue;
            };
            if mine(pid) {
                continue;
            }
            let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let Some((_, rest)) = stat.rsplit_once(") ") else {
                continue;
            };
            let mut fields = rest.split_whitespace();
            let (state, parent) = (fields.next(), fields.next());
            if state == Some("Z")
                && parent.and_then(|parent| parent.parse::<i32>().ok()) == Some(me)
                && let Some(pid) = Pid::from_raw(pid)
            {
                let _ = rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG);
            }
        }
    }
}

/// The canary: a child in the same place as a joiner, which only waits for
/// a signal and dies of it.
struct Canary {
    pid: Pid,
    pidfd: OwnedFd,
    /// How it ended, once reaped: the signal it died of, if any.
    ended: Option<Option<i32>>,
}

impl Canary {
    /// Start `command`, with launcher `me`'s pid after it.
    fn start(command: &[OsString], me: Pid) -> Result<Self, String> {
        let child = Command::new(&command[0])
            .args(&command[1..])
            .arg(me.as_raw_nonzero().to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("its canary: {e}"))?;
        let pid = Pid::from_child(&child);
        let pidfd = rustix::process::pidfd_open(pid, PidfdFlags::empty())
            .map_err(|e| format!("pidfd of its canary: {e}"))?;
        Ok(Self {
            pid,
            pidfd,
            ended: None,
        })
    }

    /// Whether it has ended, reaped through its pidfd.
    fn ended(&mut self) -> bool {
        if self.ended.is_none()
            && let Ok(Some(status)) = rustix::process::waitid(
                WaitId::PidFd(self.pidfd.as_fd()),
                WaitIdOptions::EXITED | WaitIdOptions::NOHANG,
            )
        {
            self.ended = Some(status.terminating_signal());
        }
        self.ended.is_some()
    }

    /// Whether it died of a `TERM`: resumed first, then waited for up to
    /// `wait`.
    fn died_of_term(&mut self, wait: Duration) -> bool {
        let _ = rustix::process::pidfd_send_signal(self.pidfd.as_fd(), Signal::CONT);
        let deadline = Instant::now() + wait;
        while !self.ended() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        self.ended == Some(Some(Signal::TERM.as_raw()))
    }

    /// End it, as the launcher exits.
    fn end(&mut self) {
        if !self.ended() {
            let _ = rustix::process::pidfd_send_signal(self.pidfd.as_fd(), Signal::KILL);
            let _ =
                rustix::process::waitid(WaitId::PidFd(self.pidfd.as_fd()), WaitIdOptions::EXITED);
        }
    }
}

#[cfg(test)]
mod tests;

/// Ask the kernel for both pidfd calls the launcher makes, on the
/// launcher itself: `pidfd_open` (Linux 5.3), and `waitid` on a pidfd
/// (Linux 5.4), which on a process that is no child of the caller fails
/// with `ECHILD` where the kernel has it, and `EINVAL` where it does not.
fn probe(me: Pid) -> rustix::io::Result<()> {
    let pidfd = rustix::process::pidfd_open(me, PidfdFlags::empty())?;
    match rustix::process::waitid(
        WaitId::PidFd(pidfd.as_fd()),
        WaitIdOptions::EXITED | WaitIdOptions::NOHANG,
    ) {
        Ok(_) | Err(rustix::io::Errno::CHILD) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Refuse a kernel without pidfd, from what [`probe`] returned: Linux 5.4
/// or later has it. There is no fallback: a pid alone can be reused once
/// its process is reaped.
fn floor(probed: rustix::io::Result<()>) -> Result<(), String> {
    match probed {
        Ok(()) => Ok(()),
        Err(e) if e == rustix::io::Errno::NOSYS || e == rustix::io::Errno::INVAL => Err(format!(
            "the launcher needs Linux 5.4 or later (pidfd); this kernel is {}",
            rustix::system::uname().release().to_string_lossy()
        )),
        Err(e) => Err(format!("pidfd on itself: {e}")),
    }
}

/// How the joiner ended, from a wait on its pidfd: `None` while it runs.
/// A wait that fails is an error: taken as "still running", it would
/// leave the joiner's request without its `done` for good.
fn reaped(
    waited: rustix::io::Result<Option<rustix::process::WaitIdStatus>>,
) -> Result<Option<Done>, String> {
    let Some(status) = waited.map_err(|e| format!("waitid on the joiner's pidfd: {e}"))? else {
        return Ok(None);
    };
    Ok(Some(
        match (status.exit_status(), status.terminating_signal()) {
            (Some(code), _) => Done::Exited(code),
            (None, Some(signal)) => Done::Killed(signal),
            (None, None) => Done::Killed(0),
        },
    ))
}
