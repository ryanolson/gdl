//! `cargo gdl join`: more instances of a segment in a running pool on a
//! Slurm site, as a job of their own, or as the child of a launcher on a
//! spare node of the pool's job. See `docs/src/guides/slurm.md`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use gdl_model::Fleet;
use serde_json::Value;

use super::cli::{Lines, submitted};
use super::jobs::{self, Job, Kind, Step};
use super::requests::{self, Registry, Request};
use super::{SlurmPlace, two_jobs};
use crate::Error;
use crate::deployment::slurm::launcher;
use gdl_model::launch::{self, Done, Up};

/// How a segment joins a running pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinBy {
    /// A job of its own, from the pool's `join-<segment>.yaml`, with this
    /// many instances, one per node.
    Job {
        /// How many instances, one per node: at least 1.
        nodes: u32,
    },
    /// One instance in the pool's own job, on one of its spare nodes that
    /// holds no other joiner: a child of that node's launcher of the
    /// segment, in the launcher's step.
    Step,
}

/// What `join` started, which `leave` takes to remove it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Joiner {
    /// A joining job, by its id.
    Job(String),
    /// A joiner on a spare node of a pool's job, as
    /// `<job>.<step>+<launch>`: its launcher's step, and the launch that
    /// started it, which no other joiner of that launcher has.
    Step(String),
}

impl std::fmt::Display for Joiner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Job(id) | Self::Step(id) => f.write_str(id),
        }
    }
}

/// The shortest time a joining job serves before it starts to leave, in
/// seconds. Its `--time-min` is this beyond the time it takes to leave, so
/// that Slurm never starts a joiner that would leave as it starts.
const SHORTEST_SERVICE_S: u64 = 600;

impl SlurmPlace<'_> {
    /// Add instances of `segment` to running pool `pool` of `fleet`, `by` a
    /// job of their own or a step of the pool's job: what started, for
    /// `leave`. Refused, before anything is submitted, unless the pool
    /// runs on this site with one running job, and the render wrote a
    /// recipe that joins `segment` to it.
    pub(in super::super) fn join(
        &mut self,
        fleet: &Fleet,
        pool: &str,
        segment: &str,
        by: JoinBy,
        say: &mut dyn FnMut(&str),
    ) -> Result<Joiner, Error> {
        match fleet.cluster_of(pool) {
            Some((site, _)) if site == self.home.name => {}
            Some((site, _)) => {
                return Err(Error::new(format!(
                    "pool `{pool}` runs on cluster `{site}`, and the fleet's jobs run on Slurm site `{}`",
                    self.home.name
                )));
            }
            None => {
                return Err(Error::new(format!(
                    "the fleet has no pool `{pool}` on a cluster"
                )));
            }
        }
        let dir = self.home.dir.join(pool);
        let recipe = self.joining(&dir, pool, segment)?;
        let grace = grace(&dir, segment)?;
        match by {
            JoinBy::Job { nodes } => {
                if nodes == 0 {
                    return Err(Error::new("a joining job runs at least one node"));
                }
                let job = self.running(pool)?;
                self.join_job(&dir, pool, segment, recipe, &job, nodes, grace, say)
            }
            JoinBy::Step => {
                let spare = fleet.slurm(pool).map_or(0, |slurm| slurm.spare_nodes);
                if spare == 0 {
                    return Err(Error::new(format!(
                        "pool `{pool}` holds no spare node for a joiner in its job: give it `slurm = {{ spare_nodes = <n> }}` in the fleet, deploy it again with `--replace`, or join by a job of its own"
                    )));
                }
                let job = self.running(pool)?;
                self.join_step(&dir, pool, segment, &job, say)
            }
        }
    }

    /// The recipe in `dir`, pool `pool`'s directory, that joins `segment`
    /// to it.
    fn joining(&self, dir: &Path, pool: &str, segment: &str) -> Result<Value, Error> {
        let path = dir.join(format!("join-{segment}.yaml"));
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut segments: Vec<String> = std::fs::read_dir(dir)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .filter_map(|entry| {
                        let name = entry.file_name().into_string().ok()?;
                        Some(
                            name.strip_prefix("join-")?
                                .strip_suffix(".yaml")?
                                .to_string(),
                        )
                    })
                    .collect();
                segments.sort();
                return Err(Error::new(match segments.is_empty() {
                    true => format!(
                        "pool `{pool}` has no recipes in {}: deploy the fleet with `cargo gdl deploy` first",
                        dir.display()
                    ),
                    false => format!(
                        "pool `{pool}` runs no segment `{segment}`: its class runs `{}`",
                        segments.join("`, `")
                    ),
                }));
            }
            Err(e) => return Err(Error::new(format!("{}: {e}", path.display()))),
        };
        serde_json::from_str(&text).map_err(|e| Error::new(format!("{}: {e}", path.display())))
    }

    /// Pool `pool`'s one job, which runs: a joiner joins a hub that runs.
    fn running(&mut self, pool: &str) -> Result<Job, Error> {
        let jobs: Vec<Job> = self
            .list()?
            .into_iter()
            .filter(|job| job.kind == Kind::Pool(pool.to_string()))
            .collect();
        match <[Job; 1]>::try_from(jobs) {
            Ok([job]) if job.state == "RUNNING" => Ok(job),
            Ok([job]) => Err(Error::new(format!(
                "pool `{pool}`'s job {} is {}: a segment joins a pool whose job runs",
                job.id, job.state
            ))),
            Err(jobs) if jobs.is_empty() => Err(Error::new(format!(
                "pool `{pool}` has no job on Slurm site `{}`: deploy it with `cargo gdl deploy` first",
                self.home.name
            ))),
            Err(jobs) => Err(Error::new(two_jobs(pool, &jobs))),
        }
    }

    /// Submit `recipe` with `nodes` instances of `segment`, as a job that
    /// ends no later than `job`, pool `pool`'s, and runs at least long
    /// enough to serve before it leaves.
    #[allow(clippy::too_many_arguments, reason = "the parts of one submission")]
    fn join_job(
        &mut self,
        dir: &Path,
        pool: &str,
        segment: &str,
        mut recipe: Value,
        job: &Job,
        nodes: u32,
        grace: u64,
        say: &mut dyn FnMut(&str),
    ) -> Result<Joiner, Error> {
        let limit = recipe["slurm"]["time_limit"]
            .as_str()
            .and_then(gdl_model::SlurmSite::time_limit_seconds);
        let service = recipe["services"]
            .as_array_mut()
            .and_then(|services| {
                services
                    .iter_mut()
                    .find(|service| service["name"] == segment)
            })
            .ok_or_else(|| {
                Error::new(format!(
                    "{}: it runs no service `{segment}`",
                    dir.join(format!("join-{segment}.yaml")).display()
                ))
            })?;
        service["nodes"] = nodes.into();
        let deadline = self.deadline(pool, job, grace, limit)?;
        let directives = &mut recipe["sbatch_directives"];
        if let Some((end, least)) = &deadline {
            directives["deadline"] = end.clone().into();
            directives["time-min"] = least.clone().into();
        }
        // A file of this submission alone: srt-slurm copies it into the
        // job's output directory as it submits, and the job reads that
        // copy.
        let scratch = tempfile::Builder::new()
            .prefix("gdl-join-")
            .tempdir()
            .map_err(|e| Error::new(format!("a directory for the joining recipe: {e}")))?;
        let path = scratch.path().join(format!("join-{segment}.yaml"));
        let mut text = serde_json::to_string_pretty(&recipe)
            .map_err(|e| Error::new(format!("{}: {e}", path.display())))?;
        text.push('\n');
        std::fs::write(&path, text).map_err(|e| Error::new(format!("{}: {e}", path.display())))?;
        let reply = self
            .slurm
            .run(&Lines::submit(&self.home.srtctl, &path, self.waits.objects))?;
        let (id, logs) = submitted(&reply, &path)?;
        if let Some(logs) = logs {
            self.logs.insert(id.clone(), logs);
        }
        let by = match &deadline {
            Some((end, _)) => format!(
                ", to end by {end}, when pool `{pool}`'s job {} does",
                job.id
            ),
            None => String::new(),
        };
        say(&format!(
            "join: job {id} adds {nodes} instance(s) of `{segment}` to pool `{pool}` once it starts{by}"
        ));
        Ok(Joiner::Job(id))
    }

    /// When pool `pool`'s job `job` ends, as `sbatch --deadline` takes it,
    /// and the `--time-min` of a joiner of a segment with stop grace
    /// `grace`, whose own time limit is `limit` when its recipe sets one:
    /// `None` for a job with no time limit. Refused when the job ends too
    /// soon for a joiner to serve before it leaves, or the joiner's own time
    /// limit is shorter than that, which `sbatch` would refuse.
    fn deadline(
        &mut self,
        pool: &str,
        job: &Job,
        grace: u64,
        limit: Option<u64>,
    ) -> Result<Option<(String, String)>, Error> {
        let what = format!("reading when pool `{pool}`'s job {} ends", job.id);
        let text = self.run(&Lines::end(&job.id, self.waits.objects), &what)?;
        let line = text.lines().next().unwrap_or_default().trim();
        let (end, left) = line.split_once('|').unwrap_or((line, ""));
        if left == "UNLIMITED" {
            return Ok(None);
        }
        let (true, Some(left)) = (jobs::timestamp(end), jobs::left(left)) else {
            return Err(Error::new(format!(
                "{what}: `squeue` printed `{line}`, not its end and time left"
            )));
        };
        // A joiner starts to leave its signal lead, or its stop grace,
        // before its end, whichever comes first.
        let least = self.home.lead.max(grace) + SHORTEST_SERVICE_S;
        if left < least {
            return Err(Error::new(format!(
                "pool `{pool}`'s job {} ends at {end}, in {left} s, and a joiner of it needs at least {least} s: {} s to leave, and {SHORTEST_SERVICE_S} s to serve",
                job.id,
                least - SHORTEST_SERVICE_S
            )));
        }
        if let Some(limit) = limit.filter(|limit| *limit < least) {
            return Err(Error::new(format!(
                "a joiner of pool `{pool}` runs at most its time limit, {limit} s, and needs at least {least} s: {} s to leave, and {SHORTEST_SERVICE_S} s to serve. Give the pool a longer `slurm.time_limit`",
                least - SHORTEST_SERVICE_S
            )));
        }
        let minutes = least.div_ceil(60);
        Ok(Some((
            end.to_string(),
            format!("{:02}:{:02}:00", minutes / 60, minutes % 60),
        )))
    }

    /// Ask a launcher of `segment` on a spare node of pool `pool`'s job
    /// `job` for one instance of `segment`: the launcher starts it as its
    /// own child, in its own step, which no process outside the job holds.
    ///
    /// Under the pool's registry lock, it lists the fleet's jobs: it
    /// refuses a job that Slurm no longer runs, or that `down` stops, and
    /// forgets what jobs that Slurm no longer lists asked for, and the
    /// requests of its own job that have ended. It chooses a node whose
    /// launcher is up and that no live request holds, records the request
    /// with the launcher's step, and then posts it to the launcher's
    /// mailbox: `down` knows of each request before its launcher can read
    /// it. Then, with the lock let go, it waits for the launcher to say
    /// that it started it. One not started within the objects' wait is
    /// withdrawn: the launcher never starts it once it has read the
    /// withdrawal, and holds the node until it says so.
    fn join_step(
        &mut self,
        dir: &Path,
        pool: &str,
        segment: &str,
        job: &Job,
        say: &mut dyn FnMut(&str),
    ) -> Result<Joiner, Error> {
        let what = format!("listing the steps of pool `{pool}`'s job {}", job.id);
        let mut registry = Registry::open(dir, pool, &self.waits, None)?;
        // Read under the lock: a job read before it may have been replaced
        // since, and another join may have asked the new one for a joiner.
        let jobs = self.list()?;
        let live: Vec<&str> = jobs.iter().map(|listed| listed.id.as_str()).collect();
        if !jobs
            .iter()
            .any(|listed| listed.id == job.id && listed.state == "RUNNING")
        {
            return Err(Error::new(format!(
                "pool `{pool}`'s job {} no longer runs: run the join again",
                job.id
            )));
        }
        if registry.stops(&job.id) {
            return Err(Error::new(format!(
                "pool `{pool}`'s job {} is being stopped by `cargo gdl down` or a deploy, and takes no joiner",
                job.id
            )));
        }
        let steps =
            Step::list(&self.run(&Lines::placed_steps(&job.id, self.waits.objects), &what)?);
        let before = (registry.requests.len(), registry.stopping.len());
        registry.forget_ended_jobs(&live);
        let mut kept = Vec::new();
        for request in std::mem::take(&mut registry.requests) {
            if request.job != job.id || !requests::ended(dir, &request, &steps) {
                kept.push(request);
            }
        }
        registry.requests = kept;
        if (registry.requests.len(), registry.stopping.len()) != before {
            registry.save()?;
        }
        // The spare nodes, by the launchers Slurm lists on them; the
        // launcher of the segment on each says its own step once it is up.
        // A step's name is not read for its segment: segment `a` on node
        // `b` and segment `a_b` take the same one.
        let mut nodes: Vec<&str> = Vec::new();
        for step in steps.iter().filter(|step| step.launcher()) {
            if !nodes.contains(&step.nodes.as_str()) {
                nodes.push(&step.nodes);
            }
        }
        if nodes.is_empty() {
            return Err(Error::new(format!(
                "pool `{pool}`'s job {} runs no launcher on a spare node: its srt-slurm has not started them yet, or the job was submitted before gdl ran them. Deploy it again with `--replace`, or join by a job of its own",
                job.id
            )));
        }
        let mut busy = Vec::new();
        let mut chosen = None;
        for node in nodes {
            if let Some(held) = registry
                .requests
                .iter()
                .find(|request| request.job == job.id && request.node == node)
            {
                busy.push(format!(
                    "node {node} holds launch {} of `{}`",
                    held.launch, held.segment
                ));
                continue;
            }
            let records = launcher::records(dir, &job.id, node, segment);
            let up = launcher::read(&records, launch::UP).and_then(|text| Up::read(&text));
            match up {
                None => busy.push(format!(
                    "the launcher of `{segment}` on node {node} is not up yet"
                )),
                Some(_) if launcher::read(&records, launch::CLOSED).is_some() => busy.push(
                    format!("the launcher of `{segment}` on node {node} has closed"),
                ),
                Some(up) if steps.iter().any(|listed| listed.id == up.step) => {
                    chosen = Some((up.step, node.to_string(), up.address));
                    break;
                }
                Some(up) => busy.push(format!(
                    "the launcher of `{segment}` on node {node}, step {}, has ended",
                    up.step
                )),
            }
        }
        let Some((step, node, address)) = chosen else {
            return Err(Error::new(format!(
                "pool `{pool}`'s job {}: no spare node can take a joiner of `{segment}`, and each takes one whole, since it binds the default ports: {}. Remove one with `cargo gdl leave --step <job>.<step>+<launch>`, or join by a job of its own",
                job.id,
                busy.join(", ")
            )));
        };
        let launch = launch_id();
        let n = registry.take_next(&job.id, &node, segment);
        let request = Request {
            job: job.id.clone(),
            node: node.clone(),
            segment: segment.to_string(),
            step: step.clone(),
            n,
            launch: launch.clone(),
        };
        registry.requests.push(request.clone());
        registry.save()?;
        let records = request.records(dir);
        // A `run` that another launch wrote first starts that launch's
        // joiner, never this one's.
        let posted = launcher::write_once(&records, &launch::run(n), &format!("{launch}\n"))
            .and_then(|ours| {
                ours.then_some(()).ok_or_else(|| {
                    std::io::Error::other("it holds another launch's request already")
                })
            });
        if let Err(error) = posted {
            // Its `run` may have landed: withdraw it, so that its launcher
            // drops it, or stops what it started; the node stays held until
            // the launcher says it ended.
            let withdrawn = request.stop(dir);
            return Err(Error::new(format!(
                "{}: {error}. Launch {launch} is withdrawn{}; node {node} stays held, in {}, until the launcher says it ended",
                records.display(),
                match withdrawn {
                    Ok(()) => String::new(),
                    Err(also) => format!(" ({also})"),
                },
                Registry::path(dir).display()
            )));
        }
        drop(registry);
        let handle = format!("{step}+{launch}");
        let log = launcher::log(dir, &job.id, &node, &launch);
        let deadline = Instant::now() + self.waits.objects;
        loop {
            if request.started(dir) {
                say(&format!(
                    "join: step {handle} adds an instance of `{segment}` to pool `{pool}` on node {node}, at {address}; its output is in {}",
                    log.display()
                ));
                return Ok(Joiner::Step(handle));
            }
            if let Some(done) = request.done(dir) {
                let mut registry = Registry::open(dir, pool, &self.waits, None)?;
                registry.requests.retain(|kept| kept.launch != launch);
                registry.save()?;
                return Err(Error::new(format!(
                    "the launcher of `{segment}` on node {node} of pool `{pool}`'s job {} did not start launch {launch}: {}; node {node} is free again",
                    job.id,
                    match done {
                        Done::Refused => "it has closed, as its job stops".to_string(),
                        other => other.to_string(),
                    }
                )));
            }
            // A listing that fails, as a busy controller's can, says
            // nothing of the step: the wait goes on, to its bound.
            let listed = self.run(&Lines::placed_steps(&job.id, self.waits.objects), &what);
            let mut failed = None;
            match listed {
                Ok(text) if !Step::list(&text).iter().any(|listed| listed.id == step) => {
                    return Err(Error::new(format!(
                        "the launcher of `{segment}` on node {node} of pool `{pool}`'s job {}, step {step}, ended before it started launch {launch}",
                        job.id
                    )));
                }
                Ok(_) => {}
                Err(error) => failed = Some(error),
            }
            if Instant::now() >= deadline {
                // Its `run` is posted: withdraw it whatever else fails, so
                // that it never starts after `join` has failed. The stop
                // is written under the lock when it can be taken, and
                // without it otherwise: it is this request's own record.
                let registry = Registry::open(dir, pool, &self.waits, None);
                let stopped = request.stop(dir);
                let also: Vec<String> = [failed, registry.err(), stopped.err()]
                    .into_iter()
                    .flatten()
                    .map(|e| e.to_string())
                    .collect();
                return Err(Error::new(format!(
                    "the launcher of `{segment}` on node {node} of pool `{pool}`'s job {} did not start launch {launch} within {} s, so it is withdrawn. Node {node} stays held, in {}, until the launcher says it dropped it{}",
                    job.id,
                    self.waits.objects.as_secs(),
                    Registry::path(dir).display(),
                    if also.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", also.join("; "))
                    }
                )));
            }
            std::thread::sleep(self.waits.interval);
        }
    }
}

/// The stop grace of `segment`, from its settings in `dir`, its pool's
/// directory: its drain bound and a margin.
pub(super) fn grace(dir: &Path, segment: &str) -> Result<u64, Error> {
    let path: PathBuf = dir.join(format!("{segment}.toml"));
    let text = std::fs::read_to_string(&path)
        .map_err(|e| Error::new(format!("{}: {e}", path.display())))?;
    let settings = gdl_k8s::Settings::read(&text)
        .map_err(|e| Error::new(format!("{}: {e}", path.display())))?;
    Ok(settings.grace())
}

/// A name for a new launch: this process and the time, in hex, with no
/// white space or `+`. Two launches of one pool take turns under its
/// registry's lock, so the time alone tells them apart on one machine,
/// and the process on two.
fn launch_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    format!("{:x}x{nanos:x}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::super::cli::{Slurm, SlurmCommand};
    use super::*;
    use crate::{Reply, Waits};
    use std::time::Duration;

    /// A job whose class runs segments `a` and `a_b`, on spare nodes `b`
    /// and `c`: the launcher of `a` on `b` is named `service_launch_a_b`, as
    /// the launcher of `a_b` on a job's one spare node would be. The
    /// launcher of `a_b` on `b` starts each launch it reads.
    struct Ambiguous {
        dir: PathBuf,
    }

    impl Slurm for Ambiguous {
        fn run(&mut self, command: &SlurmCommand) -> Result<Reply, Error> {
            assert_eq!(command.program, "squeue");
            if command.args.iter().any(|arg| arg == "--steps") {
                let records = launcher::records(&self.dir, "42", "b", "a_b");
                if records.join("1.run").exists() {
                    std::fs::write(records.join("1.started"), "99\n").expect("its start");
                }
                return Ok(Reply::ok(
                    "42.10|service_launch_a_b|b\n42.11|service_launch_a_c|c\n42.20|service_launch_a_b_b|b\n42.21|service_launch_a_b_c|c\n",
                ));
            }
            Ok(Reply::ok("42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n"))
        }
    }

    /// A request is tied to the step its launcher says is its own, not to
    /// the step whose name fits the segment: here `a_b`'s launcher on node
    /// `b` is step 42.20, while 42.10, `a`'s, has the name `a_b`'s would
    /// have on a job with one spare node. When `a`'s step ends, `a_b`'s
    /// joiner still runs.
    #[test]
    fn a_request_keeps_the_step_its_launcher_names() {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-join-names-")
            .tempdir()
            .expect("a scratch directory");
        let dir = scratch.path();
        let records = launcher::records(dir, "42", "b", "a_b");
        std::fs::create_dir_all(&records).expect("the records");
        std::fs::write(records.join("up"), "10.0.0.2 42.20\n").expect("its up");
        let fleet = Fleet::from_toml(&format!(
            "[fleet]\nname = \"api\"\n[cluster.lab]\nkind = \"slurm\"\nregion = \"lab\"\nsrtctl = \"/unused\"\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n[class.global]\nready = {{ a = 1, a_b = 1 }}\n[pool.p]\nclass = \"global\"\ncluster = \"lab\"\nslurm = {{ spare_nodes = 2 }}\n",
            dir.display().to_string()
        ))
        .expect("the fleet");
        let waits = Waits {
            interval: Duration::from_millis(1),
            objects: Duration::from_secs(1),
            drain: Duration::from_secs(1),
            health: Duration::from_secs(1),
        };
        let mut slurm = Ambiguous {
            dir: dir.to_path_buf(),
        };
        let mut place = SlurmPlace::new(&mut slurm, &fleet, waits, false).expect("the site");
        let job = Job::list("42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n", "api").remove(0);
        let joiner = place
            .join_step(dir, "p", "a_b", &job, &mut |_| {})
            .expect("joined");
        assert!(
            matches!(&joiner, Joiner::Step(handle) if handle.starts_with("42.20+")),
            "{joiner:?}"
        );
        let registry = Registry::open(dir, "p", &waits, None).expect("the registry");
        let [request] = &registry.requests[..] else {
            panic!("one request: {:?}", registry.requests);
        };
        assert_eq!(request.step, "42.20");
        let left = Step::list("42.20|service_launch_a_b_b|b\n42.21|service_launch_a_b_c|c\n");
        assert!(!requests::ended(dir, request, &left));
    }

    /// A Slurm whose step listing fails, once `1.run` is posted, for
    /// `failures` polls, and whose launcher then starts the launch if
    /// `starts`.
    struct Flaky {
        dir: PathBuf,
        failures: u32,
        starts: bool,
    }

    impl Slurm for Flaky {
        fn run(&mut self, command: &SlurmCommand) -> Result<Reply, Error> {
            assert_eq!(command.program, "squeue");
            if command.args.iter().any(|arg| arg == "--steps") {
                let records = launcher::records(&self.dir, "42", "b", "a");
                if records.join("1.run").exists() {
                    if self.failures > 0 {
                        self.failures -= 1;
                        return Ok(Reply::failed("slurm_load_jobs error: Socket timed out"));
                    }
                    if self.starts {
                        std::fs::write(records.join("1.started"), "99\n").expect("its start");
                    }
                }
                return Ok(Reply::ok("42.10|service_launch_a_b|b\n"));
            }
            Ok(Reply::ok("42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n"))
        }
    }

    /// `join_step` for segment `a` on a job with one spare node, `b`,
    /// through `slurm`, with `dir` its pool's directory.
    fn join_flaky(dir: &Path, slurm: &mut Flaky) -> Result<Joiner, Error> {
        let records = launcher::records(dir, "42", "b", "a");
        std::fs::create_dir_all(&records).expect("the records");
        std::fs::write(records.join("up"), "10.0.0.2 42.10\n").expect("its up");
        let fleet = Fleet::from_toml(&format!(
            "[fleet]\nname = \"api\"\n[cluster.lab]\nkind = \"slurm\"\nregion = \"lab\"\nsrtctl = \"/unused\"\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n[class.global]\nready = {{ a = 1 }}\n[pool.p]\nclass = \"global\"\ncluster = \"lab\"\nslurm = {{ spare_nodes = 1 }}\n",
            dir.display().to_string()
        ))
        .expect("the fleet");
        let waits = Waits {
            interval: Duration::from_millis(1),
            objects: Duration::from_millis(300),
            drain: Duration::from_secs(1),
            health: Duration::from_secs(1),
        };
        let mut place = SlurmPlace::new(slurm, &fleet, waits, false).expect("the site");
        let job = Job::list("42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n", "api").remove(0);
        place.join_step(dir, "p", "a", &job, &mut |_| {})
    }

    /// A step listing that fails once, as a busy controller's does, does
    /// not end the wait for a launch that its launcher then starts.
    #[test]
    fn a_failed_step_listing_does_not_end_the_wait() {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-join-flaky-")
            .tempdir()
            .expect("a scratch directory");
        let mut slurm = Flaky {
            dir: scratch.path().to_path_buf(),
            failures: 1,
            starts: true,
        };
        let joiner = join_flaky(scratch.path(), &mut slurm).expect("joined");
        assert!(
            matches!(&joiner, Joiner::Step(handle) if handle.starts_with("42.10+")),
            "{joiner:?}"
        );
    }

    /// A launch posted while the step listing keeps failing is withdrawn
    /// when the wait ends, and its request kept: it must not start after
    /// `join` has failed, with no handle to stop it by.
    #[test]
    fn a_launch_whose_wait_fails_is_withdrawn() {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-join-flaky-")
            .tempdir()
            .expect("a scratch directory");
        let dir = scratch.path();
        let mut slurm = Flaky {
            dir: dir.to_path_buf(),
            failures: u32::MAX,
            starts: false,
        };
        let error = join_flaky(dir, &mut slurm)
            .expect_err("never started")
            .to_string();
        assert!(error.contains("withdrawn"), "{error}");
        let records = launcher::records(dir, "42", "b", "a");
        let posted = std::fs::read_to_string(records.join("1.run")).expect("its run");
        assert_eq!(
            std::fs::read_to_string(records.join("1.stop")).ok(),
            Some(posted)
        );
        let waits = Waits {
            interval: Duration::from_millis(1),
            objects: Duration::from_secs(1),
            drain: Duration::from_secs(1),
            health: Duration::from_secs(1),
        };
        let registry = Registry::open(dir, "p", &waits, None).expect("the registry");
        assert_eq!(registry.requests.len(), 1, "{:?}", registry.requests);
    }
}
