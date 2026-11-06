//! The launchers a pool's job runs on its spare nodes, as the fake Slurm
//! runs them: the real launcher (`gdl_runtime`'s `--launch`), run by this
//! test binary again (`launcher_entry`), with the pool's directory in
//! place of `/gdl`, and a fake segment in place of the segment's binary.
//! The fake segment records what a test asks of a joiner: its pid, each
//! `TERM` it gets, and whether it is registered with its hub, which the
//! fake hub lists.

use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use rustix::process::{Pid, Signal};

/// This test, as the launcher this binary runs again.
const LAUNCHER: &str = "deploy::slurm::launchers::launcher_entry";
/// This test, as the launcher's canary.
const CANARY: &str = "deploy::slurm::launchers::canary_entry";

/// The launcher, when this binary runs as one: its arguments in
/// `GDL_TEST_LAUNCH`, as JSON.
#[test]
fn launcher_entry() {
    let Ok(json) = std::env::var("GDL_TEST_LAUNCH") else {
        return;
    };
    let args: Vec<String> = serde_json::from_str(&json).expect("the launcher's arguments");
    let args: Vec<std::ffi::OsString> = args.into_iter().map(Into::into).collect();
    let me = std::env::current_exe().expect("this binary");
    let canary = vec![
        me.into_os_string(),
        "--exact".into(),
        CANARY.into(),
        "--nocapture".into(),
    ];
    let code = gdl_runtime::__private::run_launcher(&args, canary);
    std::process::exit(if code == std::process::ExitCode::SUCCESS {
        0
    } else {
        1
    });
}

/// The launcher's canary, when this binary runs as one: it dies with its
/// launcher, and of the first signal that reaches it.
#[test]
fn canary_entry() {
    if std::env::var_os("GDL_TEST_LAUNCH").is_none() {
        return;
    }
    let _ = rustix::process::set_parent_process_death_signal(Some(Signal::KILL));
    loop {
        std::thread::park();
    }
}

/// A segment that writes `pid.<pid>`; registers (`registered.<pid>`) at
/// once, or, with `FAKE_GATE` set, once `go` exists; counts each `TERM`
/// in `terms.<pid>`; and, after its first, drains for `FAKE_DRAIN_US`
/// microseconds, deregisters, and exits 0. One `TERM` before it registers
/// keeps it from registering. It waits through `wait`, which a signal
/// interrupts, so that its trap runs on each `TERM` as it comes. It takes
/// the launcher's marker as its arguments, and reads none of them.
const SEGMENT: &str = r#"#!/bin/bash
out=$FAKE_OUT
echo "$GDL_LISTEN" > "$out/listen.$$"
echo $$ > "$out/pid.$$"
draining=0
trap 'echo term >> "$out/terms.$$"; draining=1' TERM
while [ "$draining" = 0 ] && [ -n "$FAKE_GATE" ] && [ ! -e "$out/go" ]; do sleep 0.01 & wait $!; done
[ "$draining" = 0 ] && : > "$out/registered.$$"
while [ "$draining" = 0 ]; do sleep 0.01 & wait $!; done
until=$(( ${EPOCHREALTIME/./} + ${FAKE_DRAIN_US:-50000} ))
while [ "${EPOCHREALTIME/./}" -lt "$until" ]; do sleep 0.01 & wait $!; done
rm -f "$out/registered.$$"
exit 0
"#;

/// One launcher: the step srt-slurm would have started for it, and its
/// process.
pub(super) struct Launcher {
    pub(super) job: u64,
    /// `<job>.<step>`.
    pub(super) step: String,
    pub(super) name: String,
    pub(super) node: String,
    child: Child,
}

impl Launcher {
    /// Start the launcher of `service`, a `launch_<segment>` service of
    /// job `job`'s recipe, on `node`, as step `step` named `name`, for
    /// the pool whose directory is `dir`, at `address`. With `gate`, its
    /// joiners register only once the test lets them.
    #[allow(clippy::too_many_arguments, reason = "the parts of one step")]
    pub(super) fn start(
        job: u64,
        step: String,
        name: String,
        node: &str,
        service: &serde_json::Value,
        dir: &Path,
        address: &str,
        gate: bool,
    ) -> Self {
        let words: Vec<String> = service["command"]
            .as_array()
            .expect("a command")
            .iter()
            .map(|word| word.as_str().expect("a word").to_string())
            .collect();
        // <binary> --launch --root /gdl --node {node} --segment <segment> -- <its command>
        let [_, launch, root, gdl, at, node_word, of, segment, split, ..] = &words[..] else {
            panic!("a launcher's command: {words:?}");
        };
        assert_eq!(
            [launch, root, gdl, at, node_word, of, split].map(String::as_str),
            [
                "--launch",
                "--root",
                "/gdl",
                "--node",
                "{node}",
                "--segment",
                "--"
            ],
            "{words:?}"
        );
        let out = fake(dir);
        std::fs::create_dir_all(&out).expect("the fake segment's directory");
        let fake_segment = out.join("segment.sh");
        if !fake_segment.exists() {
            let staged = out.join(format!(".segment.{}.sh", std::process::id()));
            std::fs::write(&staged, SEGMENT).expect("the fake segment");
            std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
                .expect("executable");
            std::fs::rename(&staged, &fake_segment).expect("the fake segment");
        }
        let args = vec![
            "--root".to_string(),
            dir.display().to_string(),
            "--node".into(),
            node.to_string(),
            "--segment".into(),
            segment.clone(),
            "--poll-ms".into(),
            "10".into(),
            "--".into(),
            fake_segment.display().to_string(),
        ];
        let mut command = Command::new(std::env::current_exe().expect("this binary"));
        command
            .args(["--exact", LAUNCHER, "--nocapture"])
            .env(
                "GDL_TEST_LAUNCH",
                serde_json::to_string(&args).expect("JSON"),
            )
            .env("SLURM_JOB_ID", job.to_string())
            .env(
                "SLURM_STEP_ID",
                step.split_once('.').map_or("", |(_, id)| id),
            )
            .env("GDL_LISTEN", format!("{address}:0"))
            .env("FAKE_OUT", &out)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .process_group(0);
        if gate {
            command.env("FAKE_GATE", "1");
        }
        Self {
            job,
            step,
            name,
            node: node.to_string(),
            child: command.spawn().expect("the launcher"),
        }
    }

    /// Whether it still runs, so that Slurm lists its step.
    pub(super) fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Send its process alone `signal`, as Slurm signals a step's task.
    pub(super) fn signal(&self, signal: Signal) {
        if let Some(pid) = Pid::from_raw(self.pid()) {
            let _ = rustix::process::kill_process(pid, signal);
        }
    }

    /// Send its process and every process it started `signal`, as a site
    /// whose `cgroup.conf` sets `SignalChildrenProcesses=yes` signals a step.
    pub(super) fn signal_all(&self, signal: Signal) {
        for pid in crate::common::tree(self.child.id()) {
            if let Some(pid) = Pid::from_raw(i32::try_from(pid).expect("a pid")) {
                let _ = rustix::process::kill_process(pid, signal);
            }
        }
    }

    fn pid(&self) -> i32 {
        i32::try_from(self.child.id()).expect("a pid")
    }

    /// End it and everything it started, as Slurm ends a step: `SIGKILL`.
    pub(super) fn kill(&mut self) {
        crate::common::kill_tree(self.child.id());
        let _ = self.child.wait();
    }
}

impl Drop for Launcher {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Where the fake segments of the pool whose directory is `dir` write.
pub(super) fn fake(dir: &Path) -> PathBuf {
    dir.join("fake")
}

/// The pid of each joiner a launcher of the pool whose directory is `dir`
/// started, as its `<n>.started` record says: written before `join`
/// returns, unlike what the joiner itself writes.
pub(super) fn joiners(dir: &Path) -> Vec<i32> {
    let mut pids = Vec::new();
    let entries = |dir: &Path| {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .collect::<Vec<_>>()
    };
    for job in entries(dir)
        .into_iter()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("launch."))
    {
        for node in entries(&job.path()) {
            for segment in entries(&node.path()) {
                for record in entries(&segment.path()) {
                    let name = record.file_name().to_string_lossy().into_owned();
                    if name.ends_with(".started") && !name.starts_with('.') {
                        let text = std::fs::read_to_string(record.path()).unwrap_or_default();
                        pids.extend(text.trim().parse::<i32>().ok());
                    }
                }
            }
        }
    }
    pids.sort_unstable();
    pids
}

/// The pid of each fake segment registered with its hub.
pub(super) fn registered(dir: &Path) -> Vec<i32> {
    named(dir, "registered.")
}

/// How many `TERM`s fake segment `pid` got.
pub(super) fn terms(dir: &Path, pid: i32) -> usize {
    std::fs::read_to_string(fake(dir).join(format!("terms.{pid}")))
        .unwrap_or_default()
        .lines()
        .count()
}

/// Whether process `pid` runs.
pub(super) fn running(pid: i32) -> bool {
    Pid::from_raw(pid).is_some_and(|pid| rustix::process::test_kill_process(pid).is_ok())
        && !std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .unwrap_or_default()
            .rsplit_once(") ")
            .is_some_and(|(_, rest)| rest.starts_with('Z'))
}

/// Let the gated fake segments of the pool whose directory is `dir`
/// register.
pub(super) fn go(dir: &Path) {
    std::fs::write(fake(dir).join("go"), "").expect("the gate");
}

fn named(dir: &Path, prefix: &str) -> Vec<i32> {
    let mut pids: Vec<i32> = std::fs::read_dir(fake(dir))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()?
                .strip_prefix(prefix)?
                .parse()
                .ok()
        })
        .collect();
    pids.sort_unstable();
    pids
}

/// The address fake segment `pid` was given to listen on.
pub(super) fn listen(dir: &Path, pid: i32) -> String {
    std::fs::read_to_string(fake(dir).join(format!("listen.{pid}")))
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// The records of the launcher of `segment` on `node` of job `job`, in the
/// pool whose directory is `dir`.
pub(super) fn records(dir: &Path, job: u64, node: &str, segment: &str) -> PathBuf {
    dir.join(format!("launch.{job}/{node}/{segment}"))
}

/// What a `join` that ended before it saw its joiner start leaves: request
/// 1 of the launcher of `segment` on `node` of job `job`, in step `step`,
/// recorded in the pool's registry, and, with `run`, its `1.run`.
pub(super) fn seed(
    dir: &Path,
    job: u64,
    node: &str,
    segment: &str,
    step: &str,
    launch: &str,
    run: bool,
) {
    std::fs::write(
        dir.join("join.requests"),
        format!(
            "next {job} {node} {segment} 2\nrequest {job} {node} {segment} {step} 1 {launch}\n"
        ),
    )
    .expect("a request");
    if run {
        let records = records(dir, job, node, segment);
        let staged = records.join(".1.run.seed.tmp");
        std::fs::write(&staged, format!("{launch}\n")).expect("its run");
        std::fs::rename(&staged, records.join("1.run")).expect("its run");
    }
}
