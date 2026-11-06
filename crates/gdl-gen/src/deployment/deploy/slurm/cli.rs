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
