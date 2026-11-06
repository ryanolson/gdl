//! The Slurm site: a fleet's pools as jobs that srt-slurm submits, one per
//! pool, beside the job of the directory's store (DynamoDB Local or etcd), reached through `srtctl`,
//! `squeue`, `scancel`, `sacct`, and each hub's routes over `curl`. A running
//! pool grows by joiners, jobs of their own or steps on its spare nodes
//! (`join`), and shrinks as they leave (`leave`). See
//! `docs/src/guides/slurm.md`.
//!
//! No state file: `squeue` lists the fleet's jobs by the comment each
//! carries, `gdl:<fleet>:<pool>:<kind>:<hash>`. srt-slurm submits a new job
//! on every apply, and a second hub of a pool takes the pool's record from
//! the first, so a deploy submits only what has no job, keeps a job whose
//! recipe is unchanged, and replaces one whose recipe changed only when
//! told to.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use gdl_model::{Fleet, SlurmReach};
use serde_json::Value;

use super::Waits;
use super::order::Parentage;
use super::site::{
    Answer, Driven, HubRoutes, Readiness, RenderInput, Running, Scope, Site, Stopping, Which,
    Withdrawal,
};
use crate::deployment::k8s;
use crate::deployment::slurm::{SlurmRender, slurm_files};
use crate::{Error, Files};

mod cli;
mod hubs;
mod jobs;
mod join;
mod leave;
mod reservations;

pub use cli::{Launched, Slurm, SlurmCli, SlurmCommand};
use cli::{Lines, submitted};
use hubs::{Address, Hubs, Withdrawn};
use jobs::{Job, Kind, Ran};
pub use join::{JoinBy, Joiner};

/// The pointer the directory's job writes in the fleet's directory: a link
/// to its own file of the store's address.
const POINTER: &str = "directory.endpoint";

/// A fleet's Slurm site, the one that runs its jobs, reached with one
/// [`Slurm`].
pub(super) struct SlurmPlace<'s> {
    slurm: &'s mut dyn Slurm,
    fleet: String,
    home: Home,
    waits: Waits,
    /// Whether a pool whose recipe changed may be replaced: drained, then
    /// submitted again.
    replace: bool,
    /// What the render asks for: each pool's hash, and the directory's.
    wanted: Wanted,
    /// What the apply submits and keeps, as the reconcile decided.
    plan: Plan,
    /// What [`Site::live`] found and is not removed yet.
    found: Found,
    /// Each pool's job this deploy waits on, by pool.
    jobs: BTreeMap<String, String>,
    /// The directory's job this deploy waits on.
    directory: Option<String>,
    /// Where srt-slurm keeps the logs of each job this run submitted.
    logs: BTreeMap<String, String>,
    /// Each pool's hub, where it wrote that it listens.
    hubs: BTreeMap<String, Address>,
    /// How many jobs the scope's stop cancelled, for [`Site::finish`].
    cancelled: usize,
    /// Whether `scancel` was found to filter a cancel by state in Slurm's
    /// controller ([`SlurmPlace::filters_in_controller`]).
    filters: bool,
    /// What a stop said where it is given no `say`, such as that a
    /// joiner's launch was abandoned, for [`Site::finish`] to say.
    notes: Vec<String>,
}

/// The oldest Slurm whose `scancel --ctld --state=PENDING` has the
/// controller apply the filter as it cancels. Before it, `scancel` filters
/// a listing of its own, then cancels by id: a job that starts in between
/// is cancelled while it runs, undrained.
const FILTERS_IN_CONTROLLER: (u32, u32, u32) = (23, 11, 6);

/// The site that runs the fleet's jobs: the one that runs its directory's
/// store, which every Slurm pool's job runs beside.
struct Home {
    name: String,
    /// srt-slurm's checkout, where `srtctl` reads its `srtslurm.yaml`.
    srtctl: PathBuf,
    /// The fleet's directory on the shared file system:
    /// `<run_dir>/<fleet>`.
    dir: PathBuf,
    reach: SlurmReach,
    /// The compute nodes' interface for traffic between nodes, when the
    /// site names it.
    interface: Option<String>,
    /// How long before a job's time limit Slurm sends it `SIGTERM`.
    lead: u64,
}

#[derive(Default)]
struct Wanted {
    pools: BTreeMap<String, String>,
    directory: Option<String>,
}

#[derive(Default)]
struct Plan {
    /// The directory's job, to submit (`None`) or kept: its id.
    directory: Option<Option<String>>,
    /// Each pool's job, to submit (`None`) or kept: its id.
    pools: BTreeMap<String, Option<String>>,
}

#[derive(Default)]
struct Found {
    /// The job of each pool the driver stops.
    pools: BTreeMap<String, Job>,
    /// Those of them with no parent whose job runs: their segments leave
    /// before the job is cancelled, since such a pool cannot drain.
    parentless: BTreeMap<String, u64>,
    /// The jobs that join each pool.
    joins: BTreeMap<String, Vec<Job>>,
    /// What is cancelled last: the directory's job, and jobs that join a
    /// pool that has no job.
    rest: Vec<Job>,
    /// The pools whose job is gone, each with its parent, whose hub
    /// withdraws its record.
    gone: BTreeMap<String, Parentage>,
}

impl<'s> SlurmPlace<'s> {
    /// The Slurm site of `fleet`, reached with `slurm`. Refused when the
    /// fleet has no name, which names its jobs, or no site that runs them.
    pub(super) fn new(
        slurm: &'s mut dyn Slurm,
        fleet: &Fleet,
        waits: Waits,
        replace: bool,
    ) -> Result<Self, Error> {
        let name = fleet.name().ok_or_else(|| {
            Error::new("the fleet has no `[fleet] name`, which names every job it runs")
        })?;
        let sites: Vec<(&str, &gdl_model::SlurmSite)> = fleet
            .clusters()
            .filter_map(|(name, cluster)| Some((name, cluster.slurm()?)))
            .collect();
        let directory = fleet
            .directory()
            .and_then(|directory| directory.cluster.as_deref());
        let (home, site) = match sites.as_slice() {
            [only] => *only,
            _ => sites
                .iter()
                .copied()
                .find(|(site, _)| Some(*site) == directory)
                .ok_or_else(|| {
                    Error::new(
                        "the fleet has several Slurm sites and its `[directory] cluster` names none of them: every Slurm pool runs on the site of the directory's job",
                    )
                })?,
        };
        Ok(Self {
            slurm,
            fleet: name.to_string(),
            home: Home {
                name: home.to_string(),
                srtctl: PathBuf::from(&site.srtctl),
                dir: Path::new(&site.run_dir).join(name),
                reach: site.reach,
                interface: site.network_interface.clone(),
                lead: site.signal_lead_s,
            },
            waits,
            replace,
            wanted: Wanted::default(),
            plan: Plan::default(),
            found: Found::default(),
            jobs: BTreeMap::new(),
            directory: None,
            logs: BTreeMap::new(),
            hubs: BTreeMap::new(),
            cancelled: 0,
            filters: false,
            notes: Vec::new(),
        })
    }

    /// Run `line`, refused with `what` and its own words unless it
    /// succeeds.
    fn run(&mut self, line: &SlurmCommand, what: &str) -> Result<String, Error> {
        let reply = self.slurm.run(line)?;
        if !reply.success {
            return Err(Error::new(format!(
                "{what}: `{} {}` failed: {}",
                line.program,
                line.args.join(" "),
                reply.stderr.trim()
            )));
        }
        Ok(reply.stdout)
    }

    /// The fleet's jobs that have not ended.
    fn list(&mut self) -> Result<Vec<Job>, Error> {
        let text = self.run(
            &Lines::jobs(self.waits.objects),
            &format!("listing the jobs on Slurm site `{}`", self.home.name),
        )?;
        Ok(Job::list(&text, &self.fleet))
    }

    /// The state of job `id`, of `of`, when it has not ended; else why it
    /// ended, as an error, with where its logs are.
    fn job(&mut self, id: &str, of: &str) -> Result<String, Error> {
        let reply = self.slurm.run(&Lines::job(id, self.waits.objects))?;
        if reply.success
            && let Some(job) = Job::list(&reply.stdout, &self.fleet)
                .into_iter()
                .find(|job| job.id == id)
        {
            return Ok(job.state);
        }
        // Not listed: Slurm's accounting says how it ended, or that it has
        // not, when `squeue` alone failed.
        let reply = self.slurm.run(&Lines::ended(id, self.waits.objects))?;
        let ended = match jobs::ended(&reply.stdout, id) {
            Some((state, _)) if jobs::live(&state) => return Ok(state),
            Some((state, exit)) => format!("Slurm says {state}, exit code {exit}"),
            None if reply.success => "Slurm keeps no record of it".to_string(),
            None => format!("`sacct` failed: {}", reply.stderr.trim()),
        };
        Err(Error::new(format!(
            "{of}: job {id} ended: {ended}; {}",
            self.logs_of(id)
        )))
    }

    /// Where srt-slurm keeps job `id`'s logs, in words.
    fn logs_of(&self, id: &str) -> String {
        match self.logs.get(id) {
            Some(dir) => format!("its logs are in {dir}/logs"),
            None => format!(
                "its logs are in srt-slurm's output directory for job {id}: `outputs/{id}/logs` in {}, unless its `srtslurm.yaml` sets `output_dir`",
                self.home.srtctl.display()
            ),
        }
    }

    /// The fleet's live jobs, by what they are: each pool's, each pool's
    /// joining jobs, and the directory's.
    fn grouped(&mut self) -> Result<Grouped, Error> {
        let mut grouped = Grouped::default();
        for job in self.list()? {
            match &job.kind {
                Kind::Pool(pool) => grouped.pools.entry(pool.clone()).or_default().push(job),
                Kind::Join(pool) => grouped.joins.entry(pool.clone()).or_default().push(job),
                Kind::Directory => grouped.directories.push(job),
            }
        }
        Ok(grouped)
    }

    /// Keep `jobs`, each pool's one job, as what the driver stops, with the
    /// jobs that join each; and what each runs, as its recipe says.
    fn stopping(
        &mut self,
        jobs: BTreeMap<String, Job>,
        joins: &mut BTreeMap<String, Vec<Job>>,
    ) -> Result<Running, Error> {
        let mut running = Running::default();
        for (pool, job) in jobs {
            let dir = self.home.dir.join(&pool);
            let ran = match Ran::read(&dir, &pool) {
                Ok(ran) => Some(ran),
                // A job that never started, or that ends, stops by its
                // cancel alone.
                Err(_) if job.pending() || job.ending() => None,
                Err(error) => {
                    return Err(Error::all(error.0.into_iter().chain([
                        gdl_model::Problem::from(format!(
                            "so job {} of pool `{pool}` is not stopped: cancel it with `scancel {}` once its pool has drained",
                            job.id, job.id
                        )),
                    ])));
                }
            };
            if let Some(ran) = &ran {
                running.notes.push(ran.note(&pool));
            }
            let drain = match &ran {
                Some(_) if job.pending() || job.ending() => None,
                Some(ran) if ran.parent.is_some() => {
                    let address = hubs::address(&dir, &job.id)?;
                    if let Some(address) = &address {
                        self.hubs.insert(pool.clone(), address.clone());
                    }
                    Some(Stopping {
                        pool: pool.clone(),
                        class: ran.class.clone(),
                        grace: ran.grace,
                        hub: address.is_some(),
                        seen: ran.seen.clone(),
                    })
                }
                Some(ran) => {
                    // Only its hub shows its instances leave: with no
                    // address, refused before anything changes.
                    let Some(address) = hubs::address(&dir, &job.id)? else {
                        return Err(Error::new(format!(
                            "pool `{pool}` has no parent, so its instances must leave before job {} is cancelled, and its hub has not written where it listens ({}), so nothing can see them leave. Wait until its hub starts, then run this again",
                            job.id,
                            hubs::file(&dir, &job.id).display()
                        )));
                    };
                    self.hubs.insert(pool.clone(), address);
                    self.found.parentless.insert(pool.clone(), ran.grace);
                    None
                }
                _ => None,
            };
            running.driven.push(Driven {
                pool: pool.clone(),
                drain,
            });
            self.found
                .joins
                .insert(pool.clone(), joins.remove(&pool).unwrap_or_default());
            self.found.pools.insert(pool, job);
        }
        Ok(running)
    }

    /// Add to `running` each pool of the fleet's directory that `stops`
    /// takes, that has a parent and no job in `listed`: its job is gone,
    /// killed, preempted, or lost with its hub's node, and its record may
    /// still be in the directory, where its parents hold it missing. The
    /// driver stops it children first as a pool whose hub is gone, and
    /// [`Site::withdrawn_without_hub`] withdraws its record. A pool with no
    /// parent has no parent's hub to withdraw it, and is passed over.
    fn jobless(
        &mut self,
        listed: &BTreeSet<String>,
        stops: impl Fn(&str) -> bool,
        running: &mut Running,
    ) -> Result<(), Error> {
        let entries = match std::fs::read_dir(&self.home.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(Error::new(format!("{}: {e}", self.home.dir.display()))),
        };
        let mut pools: Vec<String> = entries
            .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
            .filter(|pool| !listed.contains(pool) && stops(pool))
            .filter(|pool| self.home.dir.join(pool).join("pool.yaml").is_file())
            .collect();
        pools.sort();
        for pool in pools {
            let ran = Ran::read(&self.home.dir.join(&pool), &pool)?;
            let Some(parent) = ran.parent.clone() else {
                continue;
            };
            running.notes.push(ran.note(&pool));
            running.driven.push(Driven {
                pool: pool.clone(),
                drain: Some(Stopping {
                    pool: pool.clone(),
                    class: ran.class,
                    grace: ran.grace,
                    hub: false,
                    seen: ran.seen,
                }),
            });
            self.found.gone.insert(pool, parent);
        }
        running.driven.sort_by(|a, b| a.pool.cmp(&b.pool));
        Ok(())
    }

    /// Withdraw the record of `pool`, whose job is gone, once, through the
    /// hub of a running pool of its parent's class, `parent` as its recipe
    /// names it first. The recipe is the last render: a deploy whose submit
    /// then failed may have moved the pool to another home of that class
    /// than the one its job ran under, so the others are asked too. Under
    /// DynamoDB and etcd no parent's hub holds the record: the parent's hub
    /// writes the withdrawal in the directory, at an epoch past the pool's
    /// last record, so that a late write of the pool's own cannot bring it
    /// back. A hub that holds no record of it answers 404, and the next is
    /// asked: when none holds it, as after an earlier withdrawal, nothing
    /// is left to withdraw. With no parent's hub running, nothing can
    /// withdraw it, and nothing offers it work: it is named, and passed
    /// over.
    fn withdraw_gone(
        &mut self,
        pool: &Stopping,
        parent: &Parentage,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        let (named, class) = match parent {
            Parentage::Home(home) => {
                let class = Ran::read(&self.home.dir.join(home), home).map(|ran| ran.class);
                (Some(home.as_str()), class.ok())
            }
            Parentage::Parents(class) => (None, Some(class.clone())),
        };
        let mut hubs = BTreeMap::new();
        for job in self.list()? {
            let Kind::Pool(name) = &job.kind else {
                continue;
            };
            // A job submitted for the pool since the run listed the fleet's
            // jobs publishes a record of its own, which the withdrawal,
            // written past the last record, would withdraw.
            if *name == pool.pool {
                return Err(submitted_again(&pool.pool, &job));
            }
            if job.state != "RUNNING" || hubs.contains_key(name) {
                continue;
            }
            let dir = self.home.dir.join(name);
            let of_parent = named == Some(name.as_str())
                || class.as_ref().is_some_and(|class| {
                    Ran::read(&dir, name).is_ok_and(|ran| &ran.class == class)
                });
            if of_parent && let Some(address) = hubs::address(&dir, &job.id)? {
                hubs.insert(name.clone(), address);
            }
        }
        let mut asked: Vec<String> = hubs.keys().cloned().collect();
        if let Some(at) = asked.iter().position(|name| Some(name.as_str()) == named) {
            let first = asked.remove(at);
            asked.insert(0, first);
        }
        if asked.is_empty() {
            let named = match parent {
                Parentage::Home(home) => format!("`{home}`"),
                Parentage::Parents(class) => format!("of class `{class}`"),
            };
            say(&format!(
                "withdraw: pool `{}`'s job is gone, and no hub of its parent {named} runs: nothing can withdraw its record, and no parent offers it work",
                pool.pool
            ));
            return Ok(());
        }
        for home in asked {
            let (reach, budget) = (self.home.reach, self.waits.objects);
            let withdraw = |slurm: &mut dyn Slurm| {
                Hubs {
                    slurm,
                    reach,
                    hubs: &hubs,
                }
                .withdraw(&home, &pool.class, &pool.pool, budget)
            };
            let mut answer = withdraw(&mut *self.slurm)?;
            // The parent still hears the pool's hub: a hub that has just
            // died stops being heard within one lease, so wait for it to
            // pass and ask once more. A hub heard again is live, maybe
            // under another user's job that `squeue --me` does not list.
            if let Withdrawn::Fresh(fresh) = &answer {
                say(&format!(
                    "withdraw: parent `{home}`'s hub still hears pool `{}`; waiting {} ms for its lease to pass",
                    pool.pool,
                    fresh.wait().as_millis()
                ));
                self.slurm.wait(fresh.wait());
                // The wait is long: a job submitted for the pool meanwhile,
                // whose hub has not published yet, would lose its record to
                // a withdrawal past the old one's.
                let listed = self.list()?;
                if let Some(job) = listed
                    .iter()
                    .find(|job| matches!(&job.kind, Kind::Pool(name) if *name == pool.pool))
                {
                    return Err(submitted_again(&pool.pool, job));
                }
                answer = withdraw(&mut *self.slurm)?;
            }
            let answer = match answer {
                Withdrawn::Answer(answer) => answer,
                Withdrawn::Fresh(fresh) => {
                    return Err(Error::new(format!(
                        "pool `{}` has no job of yours, but parent `{home}`'s hub still hears it, so its record is not withdrawn ({}). Its job may be another user's, or it is still running: stop it through its own drain or job, then run this again. Nothing above it stops",
                        pool.pool,
                        fresh.error.trim()
                    )));
                }
            };
            match answer {
                Answer::Body(_) => {
                    let done = match &pool.seen {
                        Withdrawal::Shared { directory } => {
                            format!("wrote its withdrawal in the {directory} directory")
                        }
                        Withdrawal::Relay { .. } => "withdrew its record".to_string(),
                    };
                    say(&format!(
                        "withdraw: pool `{}`'s job is gone; parent `{home}`'s hub {done}",
                        pool.pool
                    ));
                    return Ok(());
                }
                Answer::NotFound => continue,
                Answer::NoHub(why) | Answer::Failed(why) => {
                    return Err(Error::new(format!(
                        "pool `{}`'s job is gone, and parent `{home}`'s hub did not withdraw its record (`POST /v1/ext/gdl-pool/children/{}/{}/withdraw` on its control port): {}. Nothing above it stops: run this again",
                        pool.pool,
                        pool.class,
                        pool.pool,
                        why.trim()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Decide what the apply submits and keeps, from what runs: a pool or
    /// a directory with no job is submitted; one whose job runs the
    /// rendered recipe is kept. Refused, before anything changes, for a
    /// pool whose job runs another recipe, unless it may be replaced; for
    /// a directory whose job does; and for two jobs of one pool. A pool
    /// that is replaced is stopped as the driver stops any.
    fn reconcile(&mut self, mut grouped: Grouped) -> Result<Running, Error> {
        let mut problems = Vec::new();
        self.plan = Plan::default();
        // A job that ends cannot be kept, and its processes may still run
        // beside one submitted now.
        let ending: Vec<&Job> = grouped
            .pools
            .values()
            .flatten()
            .chain(&grouped.directories)
            .filter(|job| job.ending())
            .collect();
        if !ending.is_empty() {
            return Err(Error::new(format!(
                "job {} is ending ({}): run the deploy again once Slurm no longer lists it",
                ending
                    .iter()
                    .map(|job| job.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                ending[0].state
            )));
        }
        // Two jobs of a pool, rendered or not, are refused before anything
        // changes, not when the prune reaches them.
        for (pool, jobs) in &grouped.pools {
            if jobs.len() > 1 && !self.wanted.pools.contains_key(pool) {
                problems.push(two_jobs(pool, jobs));
            }
        }
        // Every process reads the store's address once, when it starts,
        // from the fleet's pointer. With no directory job, a pool's job,
        // whatever the fleet runs and whatever its state, runs with the
        // address of a store that is gone, or may start with it before a new
        // directory job repoints the fleet: a new directory comes only after
        // `down`.
        let orphans: Vec<String> = grouped
            .pools
            .iter()
            .flat_map(|(pool, jobs)| jobs.iter().map(move |job| (pool, job)))
            .map(|(pool, job)| format!("pool `{pool}`'s job {} ({})", job.id, job.state))
            .collect();
        if let Some(wanted) = self.wanted.directory.clone() {
            self.plan.directory = match grouped.directories.as_slice() {
                [] if !orphans.is_empty() => {
                    problems.push(format!(
                        "the directory has no job, and {} is still listed: it runs, or would start, with the address of the store that is gone, and a new directory job would not change it. Take the fleet down with `cargo gdl down`, then deploy it",
                        orphans.join(", ")
                    ));
                    None
                }
                [] => Some(None),
                [job] if job.hash == wanted => Some(Some(job.id.clone())),
                [job] => {
                    problems.push(format!(
                        "the directory's job {} runs another recipe than this deploy renders: every hub keeps the address of the directory's store it started with, so a new directory needs every pool started again. Take the fleet down with `cargo gdl down`, then deploy it",
                        job.id
                    ));
                    None
                }
                jobs => {
                    problems.push(format!(
                        "the directory has {} jobs, {}: cancel all but one with `scancel`, then deploy again",
                        jobs.len(),
                        ids(jobs)
                    ));
                    None
                }
            };
        }
        let mut replaced = BTreeMap::new();
        for (pool, wanted) in self.wanted.pools.clone() {
            let jobs = grouped.pools.remove(&pool).unwrap_or_default();
            let step = match jobs.as_slice() {
                [] => None,
                [job] if job.hash == wanted => Some(job.id.clone()),
                [job] if self.replace => {
                    replaced.insert(pool.clone(), job.clone());
                    None
                }
                [job] => {
                    problems.push(format!(
                        "pool `{pool}` runs job {} of another recipe than this deploy renders: deploy with `--replace` to drain the pool, cancel the job, and submit the new one",
                        job.id
                    ));
                    continue;
                }
                jobs => {
                    problems.push(two_jobs(&pool, jobs));
                    continue;
                }
            };
            self.plan.pools.insert(pool, step);
        }
        if !problems.is_empty() {
            return Err(Error::all(problems));
        }
        self.stopping(replaced, &mut grouped.joins)
    }

    /// Wait until the fleet's pointer names job `id`'s file of the
    /// store's address, and that file holds one: the job writes it once
    /// its store is ready. A file that another job wrote is never read. The
    /// wait ends at once when the job ends.
    fn directory_ready(&mut self, id: &str, say: &mut dyn FnMut(&str)) -> Result<(), Error> {
        let own = format!("directory.{id}.endpoint");
        let pointer = self.home.dir.join(POINTER);
        let deadline = Instant::now() + self.waits.health;
        loop {
            let named = std::fs::read_link(&pointer).ok();
            let address = std::fs::read_to_string(self.home.dir.join(&own)).unwrap_or_default();
            if named.as_deref() == Some(Path::new(&own)) && address.starts_with("http://") {
                say(&format!("directory: job {id} wrote its store's address"));
                return Ok(());
            }
            let state = self.job(id, "the directory's job")?;
            if Instant::now() >= deadline {
                let names = match named {
                    Some(named) => format!("names `{}`", named.display()),
                    None => "is not there".to_string(),
                };
                return Err(Error::new(format!(
                    "the directory's job {id} did not point {} at its address within {} s: the pointer {names}, and the job is {state}; {}",
                    pointer.display(),
                    self.waits.health.as_secs(),
                    self.logs_of(id)
                )));
            }
            std::thread::sleep(self.waits.interval);
        }
    }

    /// Submit the recipe at `recipe`: its job's id.
    fn submit(&mut self, recipe: &Path) -> Result<String, Error> {
        let reply = self.slurm.run(&Lines::submit(
            &self.home.srtctl,
            recipe,
            self.waits.objects,
        ))?;
        let (id, logs) = submitted(&reply, recipe)?;
        if let Some(logs) = logs {
            self.logs.insert(id.clone(), logs);
        }
        Ok(id)
    }

    /// Write `files`, the render's files of the home site, into the fleet's
    /// directory: each that differs, through a file beside it renamed into
    /// place. Nothing else there is touched: each job's own files, and the
    /// recipes of pools that no longer render, whose jobs read them until
    /// they stop.
    fn write(&self, files: &Files) -> Result<(), Error> {
        let io = |path: &Path, e: std::io::Error| Error::new(format!("{}: {e}", path.display()));
        for (path, text) in files {
            let Ok(path) = path.strip_prefix(&self.home.name) else {
                continue;
            };
            let path = self.home.dir.join(path);
            if std::fs::read_to_string(&path).is_ok_and(|held| &held == text) {
                continue;
            }
            let dir = path.parent().expect("a file in the fleet's directory");
            std::fs::create_dir_all(dir).map_err(|e| io(dir, e))?;
            let staged = dir.join(format!(
                ".{}.{}",
                path.file_name().expect("a file name").to_string_lossy(),
                std::process::id()
            ));
            std::fs::write(&staged, text).map_err(|e| io(&staged, e))?;
            std::fs::rename(&staged, &path).map_err(|e| io(&path, e))?;
        }
        Ok(())
    }

    /// Cancel `ids`, and wait until none is listed. A job that ended
    /// before `scancel` reached it makes `scancel` fail: the jobs are read
    /// again, and those still listed cancelled again.
    fn cancel(&mut self, ids: &[&str], what: &str) -> Result<(), Error> {
        if ids.is_empty() {
            return Ok(());
        }
        let reply = self.slurm.run(&Lines::cancel(ids, self.waits.objects))?;
        if !reply.success {
            let listed: Vec<String> = self
                .list()?
                .into_iter()
                .filter(|job| ids.contains(&job.id.as_str()))
                .map(|job| job.id)
                .collect();
            if !listed.is_empty() {
                let listed: Vec<&str> = listed.iter().map(String::as_str).collect();
                self.run(&Lines::cancel(&listed, self.waits.objects), what)?;
            }
        }
        self.cancelled += ids.len();
        self.gone(ids, what)
    }

    /// Wait until no job of `ids` is listed, `COMPLETING` included.
    fn gone(&mut self, ids: &[&str], what: &str) -> Result<(), Error> {
        let deadline = Instant::now() + self.waits.objects;
        loop {
            let live: Vec<String> = self
                .list()?
                .into_iter()
                .filter(|job| ids.contains(&job.id.as_str()))
                .map(|job| job.id)
                .collect();
            if live.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::new(format!(
                    "{what}: job {} still runs {} s after `scancel`",
                    live.join(", "),
                    self.waits.objects.as_secs()
                )));
            }
            std::thread::sleep(self.waits.interval);
        }
    }

    /// Cancel those of `ids` that still wait in the queue, and wait until
    /// each is gone or has started: the ids of those that started, which
    /// are left running. A job listed as waiting can start before
    /// `scancel` reaches it, and one that runs must not be cancelled
    /// undrained.
    fn cancel_pending(&mut self, ids: &[&str], what: &str) -> Result<Vec<String>, Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.filters_in_controller("cancelling a job that waits in the queue")?;
        // A job that started is not cancelled, and one that ended makes
        // `scancel` fail: the listing below says which.
        self.slurm
            .run(&Lines::cancel_pending(ids, self.waits.objects))?;
        let deadline = Instant::now() + self.waits.objects;
        loop {
            let listed: Vec<Job> = self
                .list()?
                .into_iter()
                .filter(|job| ids.contains(&job.id.as_str()))
                .collect();
            if listed.iter().all(|job| !job.pending()) {
                let started: Vec<String> = listed.into_iter().map(|job| job.id).collect();
                self.cancelled += ids.len() - started.len();
                return Ok(started);
            }
            if Instant::now() >= deadline {
                return Err(Error::new(format!(
                    "{what}: a job still waits in the queue {} s after `scancel`",
                    self.waits.objects.as_secs()
                )));
            }
            std::thread::sleep(self.waits.interval);
        }
    }

    /// Let the instances of `pool`, a pool with no parent whose job `job`
    /// runs, leave: its hub refuses a drain, so each of its segments' steps
    /// and each job that joined it gets `SIGTERM`, on which each instance
    /// retires and leaves; then wait until its hub lists no instance,
    /// bounded by its stop grace and the drain wait. The steps and the
    /// joining jobs are read again on each turn of the wait, and each that
    /// appears gets its signal then: a joiner's step can start after the
    /// first read, from a launch that outlived its `cargo gdl join`.
    fn leave(
        &mut self,
        pool: &str,
        job: &Job,
        grace: u64,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        let what = format!("stopping pool `{pool}`");
        let mut tried = Vec::new();
        let mut first = BTreeMap::new();
        self.signal_new(pool, job, &what, &mut tried, say)?;
        let bound = Duration::from_secs(grace) + self.waits.drain;
        let deadline = Instant::now() + bound;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let answer = self.hub(&self.home.name.clone()).instances(pool, left)?;
            // An empty list says no instance is registered, not that no
            // joiner is still starting. Read after the list, so that an
            // instance registered since has a step here: every step must
            // have had its signal, and no launch may still hold a node.
            let outstanding = self.reserved(pool, job, &tried, &mut first, deadline, say)?;
            let listed = match &answer {
                Answer::Body(body) => serde_json::from_str::<Value>(body)
                    .ok()
                    .and_then(|body| body["instances"].as_array().map(Vec::len)),
                _ => None,
            };
            if listed == Some(0) && outstanding.is_empty() {
                say(&format!("stop: pool `{pool}`'s hub lists no instance"));
                return Ok(());
            }
            if Instant::now() >= deadline {
                let last = match (&answer, listed) {
                    (_, Some(count)) => format!("its hub still lists {count} instance(s)"),
                    (Answer::NoHub(why) | Answer::Failed(why), _) => {
                        format!("its hub does not answer: {}", why.trim())
                    }
                    _ => "its hub does not list its instances".to_string(),
                };
                let last = match outstanding.is_empty() {
                    true => last,
                    false => format!(
                        "{last}, and joiners still start: {}",
                        outstanding.join(", ")
                    ),
                };
                return Err(Error::new(format!(
                    "pool `{pool}` has no parent, and its instances did not leave within {} s, so job {} is not cancelled: {last}",
                    bound.as_secs(),
                    job.id
                )));
            }
            std::thread::sleep(self.waits.interval);
            self.signal_new(pool, job, &what, &mut tried, say)?;
        }
    }

    /// The steps of pool `pool`'s job `job` that its stop signals, as Slurm
    /// lists them now, each `<job>.<step>`: the job's segment steps, a
    /// joiner's among them, and the segment steps of each job that joined
    /// the pool and runs, never a batch shell. srt-slurm may still be
    /// starting a joining job's segment, so each read lists them afresh.
    fn targets(&mut self, pool: &str, job: &Job, what: &str) -> Result<Vec<String>, Error> {
        let joining: Vec<(String, String)> = self
            .list()?
            .into_iter()
            .filter(|join| {
                join.kind == Kind::Join(pool.to_string()) && !join.pending() && !join.ending()
            })
            .filter_map(|join| Some((join.id.clone(), join.segment(&self.fleet)?.to_string())))
            .collect();
        let text = self.run(&Lines::steps(&job.id, self.waits.objects), what)?;
        let mut targets = jobs::segment_steps(&text);
        for (join, segment) in &joining {
            let text = self.run(&Lines::steps(join, self.waits.objects), what)?;
            targets.extend(jobs::joining_steps(&text, segment));
        }
        Ok(targets)
    }

    /// Send `SIGTERM` to each segment step of `pool`'s job `job`, and each
    /// job that joined it and runs, that is not in `tried`, and add it
    /// there. Each gets its own `scancel`, once: a second `SIGTERM` stops a
    /// gdl instance at once and cuts its drain, so a target is never sent
    /// one again, and one that ended before its signal is passed over.
    fn signal_new(
        &mut self,
        pool: &str,
        job: &Job,
        what: &str,
        tried: &mut Vec<String>,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        let mut targets = self.targets(pool, job, what)?;
        targets.retain(|target| !tried.contains(target));
        let mut signalled = Vec::new();
        for target in &targets {
            tried.push(target.clone());
            let line = Lines::terminate(&[target], self.waits.objects);
            let reply = self.slurm.run(&line)?;
            if reply.success {
                signalled.push(target.as_str());
                continue;
            }
            // Ended meanwhile, or not: read the steps again.
            let still = self.targets(pool, job, what)?.contains(target);
            if still {
                return Err(Error::new(format!(
                    "{what}: `scancel {}` failed: {}",
                    line.args.join(" "),
                    reply.stderr.trim()
                )));
            }
        }
        if !signalled.is_empty() {
            say(&format!(
                "stop: pool `{pool}` has no parent, so its instances leave on SIGTERM: {}",
                signalled.join(", ")
            ));
        }
        Ok(())
    }

    /// Refuse `what`, which cancels jobs that wait in the queue, unless
    /// `scancel --version` names Slurm 23.11.6 or later
    /// ([`FILTERS_IN_CONTROLLER`]): checked once, and before anything
    /// changes for `down` and `--replace`.
    fn filters_in_controller(&mut self, what: &str) -> Result<(), Error> {
        if self.filters {
            return Ok(());
        }
        let reply = self.slurm.run(&Lines::version(self.waits.objects))?;
        let said = match reply.success {
            true => reply.stdout.trim().to_string(),
            false => reply.stderr.trim().to_string(),
        };
        let (major, minor, patch) = FILTERS_IN_CONTROLLER;
        let least = format!("{major}.{minor}.{patch}");
        match slurm_version(&said) {
            Some(version) if reply.success && version >= FILTERS_IN_CONTROLLER => {
                self.filters = true;
                Ok(())
            }
            Some(_) if reply.success => Err(Error::new(format!(
                "{what} needs Slurm {least} or later, and `scancel --version` says `{said}`: before {least}, `scancel --ctld --state=PENDING` filters a listing of its own and then cancels by id, so a pool's job that starts in between is cancelled while it runs, undrained"
            ))),
            _ => Err(Error::new(format!(
                "{what} needs Slurm {least} or later, and Slurm's version cannot be read from `scancel --version`, which says `{said}`"
            ))),
        }
    }

    /// Whether `place` is the site that runs the fleet's jobs: every other
    /// Slurm site of the fleet runs none.
    fn home(&self, place: &str) -> bool {
        place == self.home.name
    }
}

/// The fleet's live jobs, by what they are.
#[derive(Default)]
struct Grouped {
    pools: BTreeMap<String, Vec<Job>>,
    joins: BTreeMap<String, Vec<Job>>,
    directories: Vec<Job>,
}

impl Grouped {
    /// Each pool's one job, of the pools `wanted` keeps; refused, naming
    /// both, for a pool with two.
    fn one_each(&mut self, wanted: impl Fn(&str) -> bool) -> Result<BTreeMap<String, Job>, Error> {
        let mut problems = Vec::new();
        let mut each = BTreeMap::new();
        for (pool, jobs) in std::mem::take(&mut self.pools) {
            if !wanted(&pool) {
                self.pools.insert(pool, jobs);
                continue;
            }
            match <[Job; 1]>::try_from(jobs) {
                Ok([job]) => {
                    each.insert(pool, job);
                }
                Err(jobs) => problems.push(two_jobs(&pool, &jobs)),
            }
        }
        match problems.is_empty() {
            true => Ok(each),
            false => Err(Error::all(problems)),
        }
    }
}

/// The ids of `jobs`, for a message.
fn ids(jobs: &[Job]) -> String {
    jobs.iter()
        .map(|job| job.id.as_str())
        .collect::<Vec<_>>()
        .join(" and ")
}

/// Why pool `pool`, which has `jobs`, is not touched.
fn two_jobs(pool: &str, jobs: &[Job]) -> String {
    format!(
        "pool `{pool}` has {} jobs, {}: a second hub of a pool takes the pool's record from the first. Cancel the one you do not want with `scancel`, then run this again",
        jobs.len(),
        ids(jobs)
    )
}

impl Site for SlurmPlace<'_> {
    /// That Slurm answers, and that srt-slurm's checkout and the run
    /// directory are directories here, on the login node.
    fn check(&mut self, _fleet: &Fleet) -> Result<(), Error> {
        self.list()?;
        if self.replace {
            self.filters_in_controller("`cargo gdl deploy --replace`")?;
        }
        let run_dir = self
            .home
            .dir
            .parent()
            .expect("the fleet's directory is in run_dir");
        for (key, dir) in [
            ("srtctl", &self.home.srtctl),
            ("run_dir", &run_dir.to_path_buf()),
        ] {
            if !dir.is_dir() {
                return Err(Error::new(format!(
                    "[cluster.{}] {key}: `{}` is not a directory on this machine: run `cargo gdl deploy` on a login node of the site, where srt-slurm's checkout and the shared file system are",
                    self.home.name,
                    dir.display()
                )));
            }
        }
        Ok(())
    }

    /// That Slurm answers, and that its `scancel` filters a cancel by state
    /// in the controller: `down` cancels jobs that wait in the queue.
    fn reach(&mut self) -> Result<(), Error> {
        self.list()?;
        self.filters_in_controller("`cargo gdl down`")
    }

    /// The recipes, as [`slurm_files`] renders them; each pool's hash, and
    /// the directory's, are kept for the reconcile.
    fn render(&mut self, input: &RenderInput<'_>) -> Result<Files, Error> {
        let files = slurm_files(
            input.spec,
            input.fleet,
            &SlurmRender {
                target: input.target,
                tag: None,
                digests: input.digests,
                sources: input.sources,
            },
        )?;
        let hash = |text: &str| -> Option<String> {
            let recipe: Value = serde_json::from_str(text).ok()?;
            let comment = recipe["sbatch_directives"]["comment"].as_str()?;
            Some(comment.rsplit(':').next()?.to_string())
        };
        let site = Path::new(&self.home.name);
        for (path, text) in &files {
            let Ok(within) = path.strip_prefix(site) else {
                continue;
            };
            let parts: Vec<_> = within.iter().collect();
            match parts.as_slice() {
                [file] if *file == "directory.yaml" => self.wanted.directory = hash(text),
                [pool, file] if *file == "pool.yaml" => {
                    let wanted = hash(text).ok_or_else(|| {
                        Error::new(format!("{}: the render wrote no comment", path.display()))
                    })?;
                    self.wanted
                        .pools
                        .insert(pool.to_string_lossy().into_owned(), wanted);
                }
                _ => {}
            }
        }
        Ok(files)
    }

    /// Write the recipes into the fleet's directory, and to `out` when
    /// given; submit the directory's job unless one runs it, and wait until
    /// it points the fleet at its store; then submit each pool
    /// that the reconcile found with no job.
    fn apply(
        &mut self,
        _fleet: &Fleet,
        files: Files,
        out: Option<&Path>,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        if let Some(out) = out {
            k8s::write_new(out, &files)?;
        }
        self.write(&files)?;
        say(&format!(
            "write: the recipes in {} on Slurm site `{}`",
            self.home.dir.display(),
            self.home.name
        ));
        if let Some(step) = self.plan.directory.clone() {
            let id = match step {
                Some(id) => id,
                None => {
                    let id = self.submit(&self.home.dir.join("directory.yaml"))?;
                    say(&format!("submit: the directory's job {id}"));
                    id
                }
            };
            self.directory_ready(&id, say)?;
            self.directory = Some(id);
        }
        for (pool, step) in self.plan.pools.clone() {
            let id = match step {
                Some(id) => {
                    say(&format!(
                        "keep: pool `{pool}`, job {id}, which runs this recipe"
                    ));
                    id
                }
                None => {
                    let id = self.submit(&self.home.dir.join(&pool).join("pool.yaml"))?;
                    say(&format!("submit: pool `{pool}` as job {id}"));
                    id
                }
            };
            self.jobs.insert(pool, id);
        }
        Ok(())
    }

    /// The fleet's jobs on the site, of `scope`: for [`Scope::Replaced`],
    /// the pools whose job runs another recipe, once the reconcile has
    /// refused what it must, and the pools the fleet no longer runs whose
    /// job is gone; for [`Scope::Prune`], the pools the fleet no longer
    /// runs; for [`Scope::Pools`], every pool, those whose job is gone
    /// included; for [`Scope::Rest`], the directory's job and any job
    /// that joins a pool with none. Each pool's joining jobs stop with it.
    fn live(&mut self, place: &str, scope: Scope) -> Result<Running, Error> {
        if !self.home(place) {
            return Ok(Running::default());
        }
        self.found = Found::default();
        self.cancelled = 0;
        let mut grouped = self.grouped()?;
        let listed: BTreeSet<String> = grouped.pools.keys().cloned().collect();
        let running = match scope {
            Scope::Replaced => {
                let wanted = self.wanted.pools.clone();
                let mut running = self.reconcile(grouped)?;
                // A pool the fleet no longer runs whose job is gone is
                // withdrawn now, before anything changes: its parents still
                // run as they ran, and `--replace` may stop one of them.
                self.jobless(&listed, |pool| !wanted.contains_key(pool), &mut running)?;
                return Ok(running);
            }
            Scope::Prune => {
                let wanted = self.wanted.pools.clone();
                let jobs = grouped.one_each(|pool| !wanted.contains_key(pool))?;
                // A pool the fleet still runs keeps the jobs that join it.
                grouped.joins.retain(|pool, _| !wanted.contains_key(pool));
                self.stopping(jobs, &mut grouped.joins)?
            }
            Scope::Pools => {
                let jobs = grouped.one_each(|_| true)?;
                let mut running = self.stopping(jobs, &mut grouped.joins)?;
                self.jobless(&listed, |_| true, &mut running)?;
                running
            }
            Scope::Rest => {
                self.found.rest.extend(grouped.directories);
                Running::default()
            }
        };
        // Jobs that join a pool with no job: their hub is gone.
        self.found
            .rest
            .extend(grouped.joins.into_values().flatten());
        Ok(running)
    }

    fn hub(&mut self, _place: &str) -> Box<dyn HubRoutes + '_> {
        Box::new(Hubs {
            slurm: &mut *self.slurm,
            reach: self.home.reach,
            hubs: &self.hubs,
        })
    }

    /// A pool whose job is gone is withdrawn through a parent's hub
    /// ([`SlurmPlace::withdraw_gone`]). A pool's job that runs but whose
    /// hub has not said where it listens cannot be drained, so it is not
    /// stopped.
    fn withdrawn_without_hub(
        &mut self,
        _place: &str,
        pool: &Stopping,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        if let Some(parent) = self.found.gone.remove(&pool.pool) {
            return self.withdraw_gone(pool, &parent, say);
        }
        let job = self
            .found
            .pools
            .get(&pool.pool)
            .map_or("its job", |job| job.id.as_str());
        Err(Error::new(format!(
            "pool `{}` is not known to have withdrawn, so job {job} is not cancelled: the job runs, and its hub has not written where it listens ({}), so it cannot drain. Wait until its hub starts, then run this again",
            pool.pool,
            hubs::file(&self.home.dir.join(&pool.pool), job).display()
        )))
    }

    /// Cancel the jobs that wait to join `pools`: a pool that stops has no
    /// hub for them.
    fn stop_joining(
        &mut self,
        place: &str,
        pools: &[&str],
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        if !self.home(place) {
            return Ok(());
        }
        for pool in pools {
            let Some(joins) = self.found.joins.get_mut(*pool) else {
                continue;
            };
            let (pending, rest): (Vec<Job>, Vec<Job>) =
                std::mem::take(joins).into_iter().partition(Job::pending);
            let ids: Vec<&str> = pending.iter().map(|job| job.id.as_str()).collect();
            let started = self.cancel_pending(
                &ids,
                &format!("cancelling the jobs that wait to join pool `{pool}`"),
            )?;
            // One that started meanwhile follows its pool's drain, and is
            // cancelled with the pool.
            let mut kept = rest;
            for mut job in pending {
                if started.contains(&job.id) {
                    job.state = "RUNNING".to_string();
                    kept.push(job);
                } else {
                    say(&format!(
                        "cancel: job {} that waited to join pool `{pool}`",
                        job.id
                    ));
                }
            }
            self.found.joins.insert(pool.to_string(), kept);
        }
        Ok(())
    }

    /// Cancel the jobs of `which`, and wait until each has ended: a pool's
    /// job, after its drain, or, for a pool with no parent, once its
    /// instances have left; then the jobs that joined it. The rest goes
    /// once every pool's job has: the directory's job, which every hub
    /// uses until it stops.
    fn remove(&mut self, place: &str, which: Which<'_>) -> Result<(), Error> {
        if !self.home(place) {
            return Ok(());
        }
        match which {
            Which::Pools(pools) => {
                for pool in pools {
                    let Some(job) = self.found.pools.remove(*pool) else {
                        continue;
                    };
                    let what = format!("cancelling pool `{pool}`");
                    let pool_job = match job.pending() {
                        // Listed as waiting, so it was not drained: one that
                        // started since is left running, for a run that
                        // drains it.
                        true => {
                            let started = self.cancel_pending(&[&job.id], &what)?;
                            if !started.is_empty() {
                                return Err(Error::new(format!(
                                    "pool `{pool}`'s job {} started after it was listed as waiting in the queue, so it is not cancelled undrained: run this again to drain it",
                                    job.id
                                )));
                            }
                            None
                        }
                        false => Some(job.id.as_str()),
                    };
                    if let Some(grace) = self.found.parentless.remove(*pool) {
                        let mut notes = Vec::new();
                        self.leave(pool, &job, grace, &mut |note| notes.push(note.to_string()))?;
                        self.notes.extend(notes);
                    }
                    let joins = self.found.joins.remove(*pool).unwrap_or_default();
                    let ids: Vec<&str> = pool_job
                        .into_iter()
                        .chain(joins.iter().map(|join| join.id.as_str()))
                        .collect();
                    self.cancel(&ids, &what)?;
                    self.hubs.remove(*pool);
                }
            }
            Which::Rest => {
                let mut rest = std::mem::take(&mut self.found.rest);
                rest.extend(std::mem::take(&mut self.found.pools).into_values());
                rest.extend(
                    std::mem::take(&mut self.found.joins)
                        .into_values()
                        .flatten(),
                );
                let ids: Vec<&str> = rest.iter().map(|job| job.id.as_str()).collect();
                self.cancel(&ids, "cancelling the fleet's other jobs")?;
            }
        }
        Ok(())
    }

    fn stop(
        &mut self,
        _place: &str,
        _pools: &[&str],
        _say: &mut dyn FnMut(&str),
    ) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }

    /// Whether `pool`'s hub's health answers 200, once its job runs and
    /// its hub has said where it listens. A job that has ended stops the
    /// wait at once, naming its id, how it ended, and its logs.
    fn ready(&mut self, place: &str, pool: &str, deadline: Instant) -> Result<Readiness, Error> {
        if !self.home(place) {
            return Ok(Readiness::Ready);
        }
        let id = self
            .jobs
            .get(pool)
            .cloned()
            .ok_or_else(|| Error::new(format!("pool `{pool}` has no job to wait on")))?;
        // Every job this deploy waits on, the directory's and each pool's
        // that is ready too, is watched on each read: one that ends while
        // another pool is not ready stops the deploy, from one listing.
        let listed: BTreeMap<String, String> = self
            .list()?
            .into_iter()
            .map(|job| (job.id, job.state))
            .collect();
        let watched: Vec<(String, String)> = self
            .directory
            .iter()
            .map(|id| ("the directory's job".to_string(), id.clone()))
            .chain(
                self.jobs
                    .iter()
                    .map(|(pool, id)| (format!("pool `{pool}`"), id.clone())),
            )
            .collect();
        let mut state = None;
        for (of, watched) in watched {
            let now = match listed.get(&watched) {
                // Ending: its processes are being stopped.
                Some(now) if now == "COMPLETING" => {
                    return Err(Error::new(format!(
                        "{of}: job {watched} is ending ({now}); {}",
                        self.logs_of(&watched)
                    )));
                }
                Some(now) => now.clone(),
                None => self.job(&watched, &of)?,
            };
            if watched == id {
                state = Some(now);
            }
        }
        let state = state.expect("the pool's job is watched");
        if state != "RUNNING" {
            return Ok(Readiness::Waiting(Some(format!("its job {id} is {state}"))));
        }
        let dir = self.home.dir.join(pool);
        let Some(address) = hubs::address(&dir, &id)? else {
            return Ok(Readiness::Waiting(Some(format!(
                "its job {id} runs, and its hub has not written {} yet",
                hubs::file(&dir, &id).display()
            ))));
        };
        self.hubs.insert(pool.to_string(), address);
        let left = deadline.saturating_duration_since(Instant::now());
        Ok(match self.hub(place).health(pool, left)? {
            true => Readiness::Ready,
            false => Readiness::Waiting(None),
        })
    }

    fn why_not_ready(&mut self, place: &str, pool: &str) -> Result<Vec<String>, Error> {
        if !self.home(place) {
            return Ok(Vec::new());
        }
        Ok(match self.jobs.get(pool) {
            Some(id) => vec![format!("job {id}: {}", self.logs_of(id))],
            None => Vec::new(),
        })
    }

    /// Say how many jobs the prune, or `down`, cancelled.
    fn finish(
        &mut self,
        place: &str,
        scope: Scope,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        if !self.home(place) {
            return Ok(());
        }
        for note in std::mem::take(&mut self.notes) {
            say(&note);
        }
        match scope {
            Scope::Prune if self.cancelled > 0 => say(&format!(
                "prune: cancelled {} job(s) on Slurm site `{place}`",
                self.cancelled
            )),
            Scope::Rest => say(&format!(
                "down: cancelled the fleet's jobs on Slurm site `{place}`; {} stays",
                self.home.dir.display()
            )),
            _ => {}
        }
        Ok(())
    }
}

/// The version in `said`, what `scancel --version` prints: `slurm 24.05.4`,
/// or `slurm-wlm 22.05.8` from a distribution's package. `None` when it
/// names none.
fn slurm_version(said: &str) -> Option<(u32, u32, u32)> {
    let word = said.split_whitespace().last()?;
    let mut parts = word.splitn(3, '.');
    let number = |part: &str| -> Option<u32> {
        let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    };
    Some((
        number(parts.next()?)?,
        number(parts.next()?)?,
        number(parts.next()?)?,
    ))
}

/// The refusal to withdraw `pool`, whose job `job` was not listed when the
/// run began: the job publishes a record of its own, which a withdrawal
/// written past the last record would withdraw.
fn submitted_again(pool: &str, job: &Job) -> Error {
    Error::new(format!(
        "pool `{pool}` has job {} ({}), which was not listed when this run began, so its record is not withdrawn. Nothing above it stops: run this again",
        job.id, job.state
    ))
}
