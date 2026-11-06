//! [`Slurm`] over the commands on the `PATH`, and the command lines a
//! Slurm site gives it: `srtctl`, `squeue`, `scancel`, `sacct`, `srun`,
//! and `curl`. Every command line names its deadline, and none names a
//! credential, an account, or a partition: `srtctl` reads those from its
//! checkout's `srtslurm.yaml`, and Slurm from the user's own session.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use super::super::command;
use super::super::kubernetes::Reply;
use crate::Error;

/// Runs a Slurm site's commands. [`SlurmCli`] runs the ones on the `PATH`;
/// tests give a fake that records each command line.
pub trait Slurm {
    /// Run `command`. An error is only a command that could not start: one
    /// that ran and failed, or ran past its budget, is a [`Reply`] that did
    /// not succeed.
    fn run(&mut self, command: &SlurmCommand) -> Result<Reply, Error>;

    /// Wait `how_long` before the next command: a hub said when to ask
    /// again. A test's fake records the wait instead, so no time passes.
    fn wait(&mut self, how_long: Duration) {
        std::thread::sleep(how_long);
    }

    /// Start `command` and return without waiting for it: it outlives this
    /// process, with what it prints appended to `log`. `cargo gdl join
    /// --in-job` starts a step this way, whose `srun` lives as long as the
    /// step. The handle tells whether it has exited. An error is only a
    /// command that could not start.
    ///
    /// By default it starts nothing and says so: only `join --in-job`
    /// starts a command, and a Slurm that is given to `deploy` or `down`
    /// alone never needs to.
    fn start(&mut self, command: &SlurmCommand, log: &Path) -> Result<Box<dyn Launched>, Error> {
        Err(Error::new(format!(
            "this Slurm starts no command that outlives `cargo gdl`: `{}`, logged to {}",
            command.program,
            log.display()
        )))
    }
}

/// A command [`Slurm::start`] started, which this process does not wait
/// for.
pub trait Launched {
    /// Whether it has exited.
    fn exited(&mut self) -> bool;
}

/// A process [`SlurmCli::start`] started.
struct Started(std::process::Child);

impl Launched for Started {
    fn exited(&mut self) -> bool {
        !matches!(self.0.try_wait(), Ok(None))
    }
}

/// One command a Slurm site runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlurmCommand {
    /// The program, found on the `PATH`: `srtctl`, `squeue`, `scancel`,
    /// `sacct`, `srun`, or `curl`.
    pub program: String,
    /// Its arguments.
    pub args: Vec<String>,
    /// The directory it runs in, when it matters: `srtctl` runs in its
    /// checkout, whose `srtslurm.yaml` it reads.
    pub dir: Option<PathBuf>,
    /// How long it may run before it is killed.
    pub budget: Duration,
}

/// Runs the commands on the `PATH`, each killed at its budget. It removes
/// `RUNNER_NAME` from what each inherits: srt-slurm names a job by it
/// instead of the recipe's name, and a job not named `gdl-<fleet>-<pool>`
/// is no job of the fleet's.
#[derive(Debug, Default)]
pub struct SlurmCli;

impl Slurm for SlurmCli {
    fn run(&mut self, line: &SlurmCommand) -> Result<Reply, Error> {
        let mut process = Command::new(&line.program);
        process.args(&line.args).env_remove("RUNNER_NAME");
        if let Some(dir) = &line.dir {
            process.current_dir(dir);
        }
        let late = format!(
            "`{}` did not end within {} s, and was killed",
            line.program,
            line.budget.as_secs_f64()
        );
        command::run(
            process,
            &line.program,
            None,
            Some(Instant::now() + line.budget),
            &late,
        )
    }

    /// Start `line` in a process group of its own, so that a Ctrl-C to
    /// `cargo gdl` does not reach it, with no stdin and its output appended
    /// to `log`. Its budget does not apply: it runs as long as its step.
    fn start(&mut self, line: &SlurmCommand, log: &Path) -> Result<Box<dyn Launched>, Error> {
        use std::os::unix::process::CommandExt as _;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .map_err(|e| Error::new(format!("{}: {e}", log.display())))?;
        let copy = file
            .try_clone()
            .map_err(|e| Error::new(format!("{}: {e}", log.display())))?;
        let mut process = Command::new(&line.program);
        process
            .args(&line.args)
            .env_remove("RUNNER_NAME")
            .stdin(std::process::Stdio::null())
            .stdout(file)
            .stderr(copy)
            .process_group(0);
        if let Some(dir) = &line.dir {
            process.current_dir(dir);
        }
        // Not waited for: it outlives this process, which reaps it only
        // while it watches it.
        process
            .spawn()
            .map(|child| Box::new(Started(child)) as Box<dyn Launched>)
            .map_err(|e| Error::new(format!("starting `{}`: {e}", line.program)))
    }
}

/// The commands of one Slurm site, each bounded.
pub(super) struct Lines;

/// What `squeue` prints for each job: its id, state, name, and comment,
/// the comment last, since only it may hold the separator.
const JOB_FORMAT: &str = "--format=%i|%T|%j|%k";

/// Every state: by default `squeue` lists only pending, running, and
/// completing jobs, and a suspended job keeps its nodes and its hub. Jobs
/// that have ended are passed over by their state.
const ALL_STATES: &str = "--states=all";

/// Every partition: by default `squeue` leaves out jobs in a hidden
/// partition, or one the user cannot use, which still run the fleet's
/// pools.
const ALL_PARTITIONS: &str = "--all";

impl Lines {
    /// Every job of the user, as [`JOB_FORMAT`] prints it.
    pub(super) fn jobs(budget: Duration) -> SlurmCommand {
        slurm(
            "squeue",
            ["--me", ALL_PARTITIONS, ALL_STATES, "--noheader", JOB_FORMAT],
            budget,
        )
    }

    /// Job `id`, as [`JOB_FORMAT`] prints it.
    pub(super) fn job(id: &str, budget: Duration) -> SlurmCommand {
        slurm(
            "squeue",
            [
                &format!("--jobs={id}"),
                ALL_PARTITIONS,
                ALL_STATES,
                "--noheader",
                JOB_FORMAT,
            ],
            budget,
        )
    }

    /// The steps of job `id`: each one's id and name.
    pub(super) fn steps(id: &str, budget: Duration) -> SlurmCommand {
        slurm(
            "squeue",
            [
                "--steps",
                &format!("--jobs={id}"),
                ALL_PARTITIONS,
                "--noheader",
                "--format=%i|%j",
            ],
            budget,
        )
    }

    /// The steps of job `id`: each one's id, name, and the nodes it runs
    /// on.
    pub(super) fn placed_steps(id: &str, budget: Duration) -> SlurmCommand {
        slurm(
            "squeue",
            [
                "--steps",
                &format!("--jobs={id}"),
                ALL_PARTITIONS,
                "--noheader",
                "--format=%i|%j|%N",
            ],
            budget,
        )
    }

    /// When job `id` ends, and how long it has left, as `squeue` prints
    /// them: `2026-11-06T16:00:00|7:59:59`, or `UNLIMITED` left for a job
    /// with no time limit.
    pub(super) fn end(id: &str, budget: Duration) -> SlurmCommand {
        slurm(
            "squeue",
            [
                &format!("--jobs={id}"),
                ALL_PARTITIONS,
                ALL_STATES,
                "--noheader",
                "--format=%e|%L",
            ],
            budget,
        )
    }

    /// The IPv4 addresses of interface `interface` on node `node`, read
    /// there by a step of job `id`, as srt-slurm reads a node's address.
    pub(super) fn addresses(
        id: &str,
        node: &str,
        interface: &str,
        budget: Duration,
    ) -> SlurmCommand {
        slurm(
            "srun",
            [
                &format!("--jobid={id}"),
                "--overlap",
                "--nodes=1",
                "--ntasks=1",
                &format!("--nodelist={node}"),
                "--quiet",
                "ip",
                "-4",
                "-o",
                "addr",
                "show",
                "dev",
                interface,
            ],
            budget,
        )
    }

    /// A step of job `id` named `name` on node `node`, which runs `script`
    /// under `bash -c` in `image`, with `mounts`, as srt-slurm starts a
    /// service's process: Pyxis's flags, no entrypoint, no home mount. It
    /// makes its step within `step.immediate`, or exits: Slurm applies
    /// `--immediate` to a step's allocation too, and without it `srun`
    /// waits for a step with no bound.
    pub(super) fn launch(step: &Launch<'_>, budget: Duration) -> SlurmCommand {
        slurm(
            "srun",
            [
                &format!("--jobid={}", step.job),
                "--overlap",
                &format!("--immediate={}", step.immediate.as_secs().max(1)),
                "--nodes=1",
                "--ntasks=1",
                &format!("--nodelist={}", step.node),
                &format!("--job-name={}", step.name),
                &format!("--container-image={}", step.image),
                "--no-container-entrypoint",
                "--no-container-mount-home",
                &format!("--container-mounts={}", step.mounts.join(",")),
                "bash",
                "-c",
                step.script,
            ],
            budget,
        )
    }

    /// How job `id` ended, as Slurm's accounting keeps it.
    pub(super) fn ended(id: &str, budget: Duration) -> SlurmCommand {
        slurm(
            "sacct",
            [
                &format!("--jobs={id}"),
                "--noheader",
                "--parsable2",
                "--allocations",
                "--format=JobID,State,ExitCode",
            ],
            budget,
        )
    }

    /// Cancel `ids`, jobs: Slurm signals each step, then ends the job.
    pub(super) fn cancel(ids: &[&str], budget: Duration) -> SlurmCommand {
        slurm("scancel", ids.iter().copied(), budget)
    }

    /// The version of Slurm that `scancel` is, as it prints it.
    pub(super) fn version(budget: Duration) -> SlurmCommand {
        slurm("scancel", ["--version"], budget)
    }

    /// Cancel those of `ids`, jobs, that still wait in the queue: one that
    /// has started is left as it runs. `--ctld` has the controller apply
    /// the state filter as it cancels; without it, `scancel` filters a
    /// listing of its own, then cancels by id, and a job that starts in
    /// between is cancelled while it runs.
    pub(super) fn cancel_pending(ids: &[&str], budget: Duration) -> SlurmCommand {
        slurm(
            "scancel",
            ["--state=PENDING", "--ctld"]
                .into_iter()
                .chain(ids.iter().copied()),
            budget,
        )
    }

    /// Send `SIGTERM` to `ids`, each a job or a step `<job>.<step>`: to a
    /// job's steps, not to its batch shell, which would end the job.
    pub(super) fn terminate(ids: &[&str], budget: Duration) -> SlurmCommand {
        slurm(
            "scancel",
            std::iter::once("--signal=TERM").chain(ids.iter().copied()),
            budget,
        )
    }

    /// Submit the recipe at `recipe` with srt-slurm's checkout `checkout`,
    /// which prints what it submitted as JSON.
    pub(super) fn submit(checkout: &Path, recipe: &Path, budget: Duration) -> SlurmCommand {
        let mut line = slurm(
            "srtctl",
            ["apply", "-f", &recipe.display().to_string(), "--json"],
            budget,
        );
        line.dir = Some(checkout.to_path_buf());
        line
    }
}

/// A step that [`Lines::launch`] starts in a running job.
pub(super) struct Launch<'a> {
    pub(super) job: &'a str,
    pub(super) node: &'a str,
    pub(super) name: &'a str,
    pub(super) image: &'a str,
    pub(super) mounts: &'a [String],
    pub(super) script: &'a str,
    /// How long it may take to make its step.
    pub(super) immediate: Duration,
}

/// `program` with `args`, bounded by `budget`.
fn slurm<'a>(
    program: &str,
    args: impl IntoIterator<Item = &'a str>,
    budget: Duration,
) -> SlurmCommand {
    SlurmCommand {
        program: program.to_string(),
        args: args.into_iter().map(str::to_string).collect(),
        dir: None,
        budget,
    }
}

/// What a submission printed: the job's id and the directory srt-slurm
/// keeps its logs under. Refused when srt-slurm says it failed, or prints
/// no submission.
pub(super) fn submitted(reply: &Reply, recipe: &Path) -> Result<(String, Option<String>), Error> {
    let refused = |why: &str| {
        Error::new(format!(
            "`srtctl apply -f {} --json` submitted no job: {why}",
            recipe.display()
        ))
    };
    for line in reply.stdout.lines() {
        let Ok(record) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
            continue;
        };
        match record["status"].as_str() {
            Some("submitted") => {
                let id = match &record["slurm_job_id"] {
                    serde_json::Value::String(id) => id.clone(),
                    serde_json::Value::Number(id) => id.to_string(),
                    _ => return Err(refused("it printed a submission with no job id")),
                };
                if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(refused(&format!("it printed job id `{id}`")));
                }
                let logs = record["output_dir"].as_str().map(str::to_string);
                return Ok((id, logs));
            }
            Some("error") => {
                return Err(refused(record["error"].as_str().unwrap_or("an error")));
            }
            _ => {}
        }
    }
    let said = match reply.success {
        true => reply.stdout.trim(),
        false => reply.stderr.trim(),
    };
    Err(refused(said))
}
