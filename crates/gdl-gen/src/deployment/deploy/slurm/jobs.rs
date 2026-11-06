//! A fleet's jobs as `squeue` lists them, told apart by their sbatch
//! comment, `gdl:<fleet>:<pool>:<kind>:<hash>`, which the render writes;
//! and what a pool's job runs, read from the recipe it was submitted from.

use std::path::Path;

use serde_json::Value;

use super::super::order::{Note, Parentage};
use super::super::site::Withdrawal;
use crate::Error;

/// One job of the fleet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Job {
    pub(super) id: String,
    /// Slurm's state for it: `PENDING`, `RUNNING`, ...
    pub(super) state: String,
    pub(super) name: String,
    pub(super) kind: Kind,
    /// The hash of its recipe and the settings it reads.
    pub(super) hash: String,
}

/// What a job of the fleet is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Kind {
    /// The job of a pool.
    Pool(String),
    /// A job that adds instances to a running pool.
    Join(String),
    /// The job of the directory's store: DynamoDB Local or etcd.
    Directory,
}

/// States in which a job's processes have ended: Slurm lists a job for a
/// while after it ends. `COMPLETING` is not one: its processes may still
/// run while its steps and epilog end.
const ENDED: [&str; 10] = [
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "TIMEOUT",
];

impl Job {
    /// The fleet `fleet`'s jobs in what `squeue` printed, one per line as
    /// `id|state|name|comment`, of those that have not ended. Another
    /// user's or another fleet's job has no comment of the fleet's, and is
    /// passed over; so is a job whose name is not the one the render gives
    /// its comment's pool, kind, and fleet, which is not one of the
    /// fleet's.
    pub(super) fn list(text: &str, fleet: &str) -> Vec<Job> {
        text.lines()
            .filter_map(|line| Self::of(line.trim(), fleet))
            .filter(Job::live)
            .collect()
    }

    fn of(line: &str, fleet: &str) -> Option<Job> {
        let mut fields = line.splitn(4, '|');
        let (id, state, name, comment) = (
            fields.next()?,
            fields.next()?,
            fields.next()?,
            fields.next()?,
        );
        let rest = comment.strip_prefix(&format!("gdl:{fleet}:"))?;
        let mut tail = rest.rsplitn(3, ':');
        let (hash, kind, pool) = (tail.next()?, tail.next()?, tail.next()?);
        let kind = match kind {
            "pool" => Kind::Pool(pool.to_string()),
            "join" => Kind::Join(pool.to_string()),
            "directory" if pool == "-" => Kind::Directory,
            _ => return None,
        };
        let named = match &kind {
            Kind::Pool(pool) => name == format!("gdl-{fleet}-{pool}"),
            Kind::Join(pool) => name.starts_with(&format!("gdl-{fleet}-{pool}-join-")),
            Kind::Directory => name == format!("gdl-{fleet}-directory"),
        };
        // A requeued job, which every gdl job refuses, would show its
        // state with a suffix; the state alone is kept.
        let state = state.split_whitespace().next().unwrap_or_default();
        (named && !id.is_empty() && !state.is_empty()).then(|| Job {
            id: id.to_string(),
            state: state.to_string(),
            name: name.to_string(),
            kind,
            hash: hash.to_string(),
        })
    }

    /// Whether its processes may still run, or have yet to start.
    pub(super) fn live(&self) -> bool {
        live(&self.state)
    }

    /// Whether it waits in Slurm's queue: its processes have not started.
    pub(super) fn pending(&self) -> bool {
        self.state == "PENDING"
    }

    /// Whether it is ending: its processes are being stopped, and it will
    /// soon end.
    pub(super) fn ending(&self) -> bool {
        self.state == "COMPLETING"
    }
}

/// Whether a job in `state` may still run, or has yet to start.
pub(super) fn live(state: &str) -> bool {
    !ENDED.contains(&state)
}

/// What a pool's job runs, as its recipe says: its class, its parent, the
/// longest stop grace of its segments, and where its withdrawal is seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Ran {
    pub(super) class: String,
    pub(super) parent: Option<Parentage>,
    pub(super) grace: u64,
    pub(super) seen: Withdrawal,
}

impl Ran {
    /// What pool `pool`'s recipe in `dir`, its directory on the shared
    /// file system, runs: its hub's `GDL_POOL__*` variables name its class
    /// and parent, and the settings each segment reads its drain bound.
    pub(super) fn read(dir: &Path, pool: &str) -> Result<Self, Error> {
        let path = dir.join("pool.yaml");
        let unread = |why: String| {
            Error::new(format!(
                "pool `{pool}`: {} does not say what its job runs: {why}",
                path.display()
            ))
        };
        let text = std::fs::read_to_string(&path).map_err(|e| unread(e.to_string()))?;
        let recipe: Value = serde_json::from_str(&text).map_err(|e| unread(e.to_string()))?;
        let services = recipe["services"]
            .as_array()
            .ok_or_else(|| unread("it has no services".into()))?;
        let hub = services
            .iter()
            .find(|service| service["name"] == "hub")
            .ok_or_else(|| unread("it has no hub".into()))?;
        let env = |name: &str| hub["env"][name].as_str().map(str::to_string);
        let class = env("GDL_POOL__CLASS").ok_or_else(|| unread("its hub has no class".into()))?;
        let seen = env("GDL_POOL__DIRECTORY")
            .as_deref()
            .and_then(Withdrawal::shared)
            .unwrap_or_else(|| Withdrawal::Relay {
                home: env("GDL_POOL__HOME"),
            });
        let parent = match (env("GDL_POOL__HOME"), env("GDL_POOL__PARENTS")) {
            (Some(home), _) => Some(Parentage::Home(home)),
            (None, Some(class)) => Some(Parentage::Parents(class)),
            (None, None) => None,
        };
        let mut grace = 0;
        for service in services {
            // A segment's command is the binary and its settings, mounted
            // at `/gdl`.
            let Some(file) = service["command"][1]
                .as_str()
                .and_then(|file| file.strip_prefix("/gdl/"))
                .filter(|file| file.ends_with(".toml"))
            else {
                continue;
            };
            let text = std::fs::read_to_string(dir.join(file))
                .map_err(|e| unread(format!("{file}: {e}")))?;
            let settings =
                gdl_k8s::Settings::read(&text).map_err(|e| unread(format!("{file}: {e}")))?;
            grace = grace.max(settings.grace());
        }
        Ok(Self {
            class,
            parent,
            grace,
            seen,
        })
    }

    /// `pool`'s note, for the stop order.
    pub(super) fn note(&self, pool: &str) -> Note {
        Note {
            pool: pool.to_string(),
            class: self.class.clone(),
            parent: self.parent.clone(),
        }
    }
}

/// The steps of a pool's job that run its segments, from what `squeue
/// --steps` printed, one per line as `id|name`: srt-slurm names each
/// service's step `service_<service>`, or `service_<service>_<node>` for
/// one instance of several, where a node's name may hold dots, and the
/// hub's `service_hub`. A step that prepares a container,
/// `<step>.container-prepare`, is passed over.
pub(super) fn segment_steps(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|line| line.trim().split_once('|'))
        .filter(|(_, name)| {
            name.starts_with("service_")
                && *name != "service_hub"
                && !name.ends_with(".container-prepare")
        })
        .map(|(id, _)| id.to_string())
        .collect()
}

/// How job `id` ended, from what `sacct` printed: `id|state|exit code`
/// lines, the job's own first. A state is its first word: `sacct` writes
/// `CANCELLED by <uid>`.
pub(super) fn ended(text: &str, id: &str) -> Option<(String, String)> {
    text.lines().find_map(|line| {
        let mut fields = line.trim().split('|');
        (fields.next()? == id).then(|| {
            let state = fields
                .next()
                .and_then(|state| state.split_whitespace().next())
                .unwrap_or_default()
                .to_string();
            let exit = fields.next().unwrap_or_default().to_string();
            (state, exit)
        })
    })
}
