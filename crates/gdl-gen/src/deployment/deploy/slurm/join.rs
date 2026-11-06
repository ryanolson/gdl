//! `cargo gdl join`: more instances of a segment in a running pool on a
//! Slurm site, as a job of their own, or as a step on a spare node of the
//! pool's job. See `docs/src/guides/slurm.md`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use gdl_model::Fleet;
use serde_json::Value;

use super::cli::{Launch, Lines, submitted};
use super::jobs::{self, JOIN_STEP, Job, Kind, Step};
use super::reservations::{Registry, Reservation};
use super::{SlurmPlace, hubs, two_jobs};
use crate::Error;

/// How a segment joins a running pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinBy {
    /// A job of its own, from the pool's `join-<segment>.yaml`, with this
    /// many instances, one per node.
    Job {
        /// How many instances, one per node: at least 1.
        nodes: u32,
    },
    /// One instance, as a step of the pool's own job on one of its spare
    /// nodes that holds no other joiner.
    Step,
}

/// What `join` started, which `leave` takes to remove it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Joiner {
    /// A joining job, by its id.
    Job(String),
    /// A step on a spare node of a pool's job, as `<job>.<step>`.
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
                let interface = self.home.interface.clone().ok_or_else(|| {
                    Error::new(format!(
                        "`cargo gdl join --in-job` reads a spare node's address on the interface `[cluster.{}] network_interface` names, and the site names none",
                        self.home.name
                    ))
                })?;
                let job = self.running(pool)?;
                self.join_step(&dir, pool, segment, &recipe, &job, &interface, say)
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

    /// Start one instance of `segment`, as `recipe` runs it, in pool
    /// `pool`'s job `job`, on a spare node that holds no other joiner,
    /// listening on that node's one IPv4 address on `interface`.
    ///
    /// Under the pool's registry lock, it chooses the node, records the
    /// launch's hold on it, and starts the launch's `srun`; then, with the
    /// lock let go, it waits for the launch's step. The hold goes once the
    /// step is listed, and with the launch when its `srun` exits first, as
    /// one that gives up at `--immediate` does. A join that stops before
    /// either, on an error or killed, leaves the hold, which `down` waits
    /// out ([`super::SlurmPlace::reserved`]).
    #[allow(clippy::too_many_arguments, reason = "the parts of one step")]
    fn join_step(
        &mut self,
        dir: &Path,
        pool: &str,
        segment: &str,
        recipe: &Value,
        job: &Job,
        interface: &str,
        say: &mut dyn FnMut(&str),
    ) -> Result<Joiner, Error> {
        let hub = hubs::address(dir, &job.id)?.ok_or_else(|| {
            Error::new(format!(
                "pool `{pool}`'s job {} runs, and its hub has not written where it listens ({}) yet",
                job.id,
                hubs::file(dir, &job.id).display()
            ))
        })?;
        let what = format!("listing the steps of pool `{pool}`'s job {}", job.id);
        let mut registry = Registry::open(dir, pool, &self.waits, None)?;
        let steps =
            Step::list(&self.run(&Lines::placed_steps(&job.id, self.waits.objects), &what)?);
        // A hold whose step is listed has done its work: the step holds
        // the node now.
        let before = registry.held.len();
        registry
            .held
            .retain(|held| !steps.iter().any(|step| step.launch() == Some(&held.launch)));
        if registry.held.len() != before {
            registry.save()?;
        }
        let holds: Vec<&Reservation> = registry
            .held
            .iter()
            .filter(|held| held.job == job.id)
            .collect();
        let node = free_node(&steps, &holds, registry.path())
            .map_err(|why| Error::new(format!("pool `{pool}`'s job {}: {why}", job.id)))?;
        let address = self.address(&job.id, &node, interface)?;
        let service = recipe["services"]
            .as_array()
            .and_then(|services| services.iter().find(|service| service["name"] == segment))
            .ok_or_else(|| Error::new(format!("the joining recipe runs no service `{segment}`")))?;
        let script = script(service, &address, &hub)?;
        let image = service["container"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let mounts: Vec<String> = recipe["extra_mount"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|mount| mount.as_str().map(str::to_string))
            .collect();
        // This launch owns its step by its name, and its hold: no other
        // join or leave takes either for its own.
        let launch = launch_id();
        let name = format!("{JOIN_STEP}{segment}+{launch}");
        let log = dir.join(format!("join.{}.{node}.log", job.id));
        registry.held.push(Reservation {
            job: job.id.clone(),
            node: node.clone(),
            launch: launch.clone(),
            log: log.clone(),
        });
        registry.save()?;
        let line = Lines::launch(
            &Launch {
                job: &job.id,
                node: &node,
                name: &name,
                image: &image,
                mounts: &mounts,
                script: &script,
                immediate: self.waits.objects,
            },
            self.waits.objects,
        );
        let mut started = match self.slurm.start(&line, &log) {
            Ok(started) => started,
            Err(error) => {
                // Nothing started.
                registry.held.retain(|held| held.launch != launch);
                registry.save()?;
                return Err(error);
            }
        };
        drop(registry);
        let unseen = |why: String| {
            Error::new(format!(
                "{why}. Node {node} stays held, in {}, until step `{name}` is listed, or `cargo gdl down` finds the launch abandoned",
                dir.join("join.reservations").display()
            ))
        };
        let release = |place: &Self| -> Result<(), Error> {
            let mut registry = Registry::open(dir, pool, &place.waits, None)?;
            registry.held.retain(|held| held.launch != launch);
            registry.save()
        };
        // The step is listed once Slurm has made it, before its container
        // starts; its `srun` makes it within `--immediate`, or exits.
        let deadline = Instant::now() + self.waits.objects + self.waits.interval * 5;
        loop {
            // Read before the listing: a launch that made its step and
            // ended at once is found in the listing that follows.
            let exited = started.exited();
            let listed = Step::list(
                &self
                    .run(&Lines::placed_steps(&job.id, self.waits.objects), &what)
                    .map_err(|error| unseen(error.to_string()))?,
            );
            if let Some(step) = listed.into_iter().find(|step| step.name == name) {
                release(self)?;
                say(&format!(
                    "join: step {} adds an instance of `{segment}` to pool `{pool}` on node {node}, at {address}; its output is in {}",
                    step.id,
                    log.display()
                ));
                return Ok(Joiner::Step(step.id));
            }
            if exited {
                release(self)?;
                let said = std::fs::read_to_string(&log).unwrap_or_default();
                let tail: Vec<&str> = said.lines().rev().take(3).collect();
                let tail: Vec<&str> = tail.into_iter().rev().collect();
                return Err(Error::new(format!(
                    "`srun` on node {node} of pool `{pool}`'s job {} exited without making step `{name}`, and node {node} is free again: {}",
                    job.id,
                    match tail.is_empty() {
                        true => format!("see {}", log.display()),
                        false => format!("it said `{}` ({})", tail.join(" / "), log.display()),
                    }
                )));
            }
            if Instant::now() >= deadline {
                return Err(unseen(format!(
                    "pool `{pool}`'s job {} lists no step `{name}` on node {node} {} s after `srun` started it",
                    job.id,
                    self.waits.objects.as_secs()
                )));
            }
            std::thread::sleep(self.waits.interval);
        }
    }

    /// Node `node`'s one IPv4 address on `interface`, read there by a step
    /// of job `job`. Refused for none or several: each peer must reach the
    /// joiner at the one it advertises.
    fn address(&mut self, job: &str, node: &str, interface: &str) -> Result<String, Error> {
        let line = Lines::addresses(job, node, interface, self.waits.objects);
        let reply = self.slurm.run(&line)?;
        if !reply.success {
            return Err(Error::new(format!(
                "reading node {node}'s address on interface {interface}: `{} {}` failed: {}",
                line.program,
                line.args.join(" "),
                reply.stderr.trim()
            )));
        }
        match jobs::addresses(&reply.stdout).as_slice() {
            [one] => Ok(one.clone()),
            [] => Err(Error::new(format!(
                "node {node} has no IPv4 address on interface {interface}, so a joiner there has none to advertise: name the interface its peers reach it on in `[cluster.{}] network_interface`",
                self.home.name
            ))),
            several => Err(Error::new(format!(
                "node {node} has {} IPv4 addresses on interface {interface}, {}, and a joiner there advertises one: gdl cannot tell which its peers reach",
                several.len(),
                several.join(", ")
            ))),
        }
    }
}

/// The first spare node of a pool's job, in `steps`, that holds no joiner,
/// and no launch of one not yet seen (`holds`, in the registry at
/// `registry`).
fn free_node(steps: &[Step], holds: &[&Reservation], registry: &Path) -> Result<String, String> {
    let spare: Vec<&Step> = steps.iter().filter(|step| step.spare()).collect();
    if spare.is_empty() {
        return Err(
            "no step holds its spare nodes: srt-slurm's `spare` service is not running".into(),
        );
    }
    let held: Vec<&Step> = steps
        .iter()
        .filter(|step| step.joined().is_some())
        .collect();
    spare
        .iter()
        .map(|step| step.nodes.clone())
        .find(|node| {
            !held.iter().any(|joiner| &joiner.nodes == node)
                && !holds.iter().any(|held| &held.node == node)
        })
        .ok_or_else(|| {
            let mut holders: Vec<String> = held
                .iter()
                .map(|step| format!("step {} on {}", step.id, step.nodes))
                .collect();
            holders.extend(holds.iter().map(|held| {
                format!(
                    "a launch on {} not yet seen ({}, in {})",
                    held.node,
                    held.launch,
                    registry.display()
                )
            }));
            format!(
                "every spare node holds a joiner, and each takes a node whole, since it binds the default ports: {}. Remove one with `cargo gdl leave --step <job>.<step>`, or join by a job of its own",
                holders.join(", ")
            )
        })
}

/// What a joiner's step runs under `bash -c`, as srt-slurm runs a service:
/// its variables, its preamble, then its binary with its settings. Where
/// srt-slurm would fill in `{node_ip}` and `{head_ip}`, the node's address
/// and the hub's host go; and the hub's host and ports are set, as a
/// joining job's `resolve` would write them.
fn script(service: &Value, address: &str, hub: &hubs::Address) -> Result<String, Error> {
    let (host, public) = split(&hub.public)?;
    let (_, control) = split(&hub.control)?;
    let mut env: Vec<(String, String)> = service["env"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(name, value)| {
            let value = value
                .as_str()?
                .replace("{node_ip}", address)
                .replace("{head_ip}", host);
            Some((name.clone(), value))
        })
        .collect();
    env.retain(|(name, _)| {
        !matches!(
            name.as_str(),
            "GDL_LISTEN" | "GDL_HUB__HOST" | "GDL_HUB__PUBLIC_PORT" | "GDL_HUB__CONTROL_PORT"
        )
    });
    env.extend([
        ("GDL_LISTEN".to_string(), format!("{address}:0")),
        ("GDL_HUB__HOST".to_string(), host.to_string()),
        ("GDL_HUB__PUBLIC_PORT".to_string(), public.to_string()),
        ("GDL_HUB__CONTROL_PORT".to_string(), control.to_string()),
    ]);
    // The segment's binary and its settings: not the file a joining job's
    // `resolve` writes, which this step has none of.
    let command: Vec<&str> = service["command"]
        .as_array()
        .into_iter()
        .flatten()
        .take(2)
        .filter_map(Value::as_str)
        .collect();
    if command.len() != 2 {
        return Err(Error::new(
            "the joining recipe's segment runs no binary with its settings",
        ));
    }
    let mut parts: Vec<String> = env
        .iter()
        .map(|(name, value)| format!("export {name}={}", quote(value)))
        .collect();
    if let Some(preamble) = service["preamble"].as_str() {
        parts.push(preamble.to_string());
    }
    parts.push(format!(
        "exec {}",
        command
            .iter()
            .map(|word| quote(word))
            .collect::<Vec<_>>()
            .join(" ")
    ));
    Ok(parts.join(" && "))
}

/// `host:port` as its host, without brackets, and its port.
fn split(listener: &str) -> Result<(&str, &str), Error> {
    let (host, port) = listener
        .rsplit_once(':')
        .ok_or_else(|| Error::new(format!("the hub's address `{listener}` has no port")))?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    Ok((host, port))
}

/// `text` as one word of a POSIX shell, whatever it holds.
fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
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
