//! The spare nodes of a pool's job that launches of `cargo gdl join
//! --in-job` hold before their steps are listed: one registry file in the
//! pool's directory on the shared file system, `join.reservations`, read
//! and written only under an exclusive lock on `join.lock` beside it.
//!
//! One file under a lock, not a file per launch in a directory listing:
//! the login nodes share the directory over a network file system, whose
//! clients cache a directory's listing for up to a minute, so a `down` on
//! one login node could miss a reservation another wrote seconds before.
//! An NFS client revalidates a file's data when it takes a lock on it, and
//! when it opens it after another client closed it (close-to-open), but
//! not a directory's cached listing. See `docs/src/guides/slurm.md`.

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::Waits;
use crate::Error;

/// The registry, in a pool's directory.
const REGISTRY: &str = "join.reservations";

/// The lock that every reader and writer of the registry holds.
const LOCK: &str = "join.lock";

/// One launch's hold on a spare node of a pool's job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Reservation {
    /// The pool's job.
    pub(super) job: String,
    pub(super) node: String,
    /// The launch's name, which ends its step's name: `gdl-join-<segment>+<launch>`.
    pub(super) launch: String,
    /// Where its `srun` writes what it prints.
    pub(super) log: PathBuf,
}

/// The registry of the pool whose directory is `dir`, held under its lock
/// until dropped.
pub(super) struct Registry {
    _lock: File,
    path: PathBuf,
    /// What it holds, as read when the lock was taken.
    pub(super) held: Vec<Reservation>,
}

impl Registry {
    /// Take the lock of the registry in `dir`, pool `pool`'s directory,
    /// waiting for another holder up to the objects' wait, or until `until`
    /// when it comes sooner, then read it. A file system that takes no lock
    /// is refused: nothing else keeps two login nodes from choosing one
    /// node.
    pub(super) fn open(
        dir: &Path,
        pool: &str,
        waits: &Waits,
        until: Option<Instant>,
    ) -> Result<Self, Error> {
        let path = dir.join(LOCK);
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|e| Error::new(format!("{}: {e}", path.display())))?;
        let mut deadline = Instant::now() + waits.objects;
        if let Some(until) = until {
            deadline = deadline.min(until);
        }
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(waits.interval);
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(Error::new(format!(
                        "another `cargo gdl` of pool `{pool}` still holds {} past this command's wait for it: run this again once it has let go",
                        path.display()
                    )));
                }
                Err(std::fs::TryLockError::Error(e)) => {
                    return Err(Error::new(format!(
                        "locking {}, which keeps the spare nodes that joins hold: {e}",
                        path.display()
                    )));
                }
            }
        }
        let path = dir.join(REGISTRY);
        // Opened after the lock is taken, so that a network file system
        // reads what the last holder wrote.
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(Error::new(format!("{}: {e}", path.display()))),
        };
        let held = text
            .lines()
            .filter_map(|line| {
                let mut words = line.split_whitespace();
                let (job, node, launch, log) =
                    (words.next()?, words.next()?, words.next()?, words.next()?);
                Some(Reservation {
                    job: job.to_string(),
                    node: node.to_string(),
                    launch: launch.to_string(),
                    log: PathBuf::from(log),
                })
            })
            .collect();
        Ok(Self {
            _lock: lock,
            path,
            held,
        })
    }

    /// Write [`Registry::held`] back, in place, under the lock, and flush
    /// it to the file system before the lock is let go.
    pub(super) fn save(&self) -> Result<(), Error> {
        let io = |e: std::io::Error| Error::new(format!("{}: {e}", self.path.display()));
        let mut file = File::create(&self.path).map_err(io)?;
        for held in &self.held {
            writeln!(
                file,
                "{} {} {} {}",
                held.job,
                held.node,
                held.launch,
                held.log.display()
            )
            .map_err(io)?;
        }
        file.sync_all().map_err(io)
    }

    /// Where it is, for a message.
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl super::SlurmPlace<'_> {
    /// The holds on spare nodes of pool `pool`'s job `job` that `down`
    /// must still wait for, each named for a message, before it takes its
    /// hub's empty list as every instance gone. A hold whose launch's step
    /// is listed and in `tried`, so signalled, is done, and removed. One
    /// whose step is listed and not yet signalled waits for its signal. One
    /// whose step is not listed is abandoned once a listing taken longer
    /// than the launch's own bound after `down` first saw it still shows
    /// none: its `srun` started before its hold was written, and made its
    /// step within `--immediate`, the objects' wait, or exited. `first`
    /// keeps when each was first seen, on this process's clock.
    pub(super) fn reserved(
        &mut self,
        pool: &str,
        job: &super::jobs::Job,
        tried: &[String],
        first: &mut std::collections::BTreeMap<String, Instant>,
        until: Instant,
        say: &mut dyn FnMut(&str),
    ) -> Result<Vec<String>, Error> {
        let dir = self.home.dir.join(pool);
        let what = format!("stopping pool `{pool}`");
        // Every step the stop signals, as listed now: one that started
        // since its last signal is outstanding, whether or not a hold
        // still names it, since another join may have cleared that.
        let mut outstanding: Vec<String> = self
            .targets(pool, job, &what)?
            .into_iter()
            .filter(|step| !tried.contains(step))
            .map(|step| format!("step {step}, not yet signalled"))
            .collect();
        let mut registry = Registry::open(&dir, pool, &self.waits, Some(until))?;
        let steps = super::jobs::Step::list(&self.run(
            &super::cli::Lines::placed_steps(&job.id, self.waits.objects),
            &what,
        )?);
        let bound = self.waits.objects + self.waits.interval * 5;
        let now = Instant::now();
        let mut abandoned = Vec::new();
        let before = registry.held.len();
        registry.held.retain(|held| {
            if held.job != job.id {
                return true;
            }
            match steps
                .iter()
                .find(|step| step.launch() == Some(&held.launch))
            {
                Some(step) if tried.contains(&step.id) => false,
                Some(step) => {
                    outstanding.push(format!(
                        "step {} on {}, not yet signalled",
                        step.id, held.node
                    ));
                    true
                }
                None => {
                    let since = *first.entry(held.launch.clone()).or_insert(now);
                    if now.duration_since(since) > bound {
                        abandoned.push(held.clone());
                        false
                    } else {
                        outstanding.push(format!(
                            "launch {} on {}, whose step is not listed yet",
                            held.launch, held.node
                        ));
                        true
                    }
                }
            }
        });
        if registry.held.len() != before {
            registry.save()?;
        }
        let path = registry.path().to_path_buf();
        drop(registry);
        for held in abandoned {
            say(&format!(
                "stop: launch {} on node {} of pool `{pool}`'s job {}, held in {}, made no step within {} s: abandoned; its output is in {}",
                held.launch,
                held.node,
                job.id,
                path.display(),
                bound.as_secs(),
                held.log.display()
            ));
        }
        Ok(outstanding)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::site::{Site, Which};
    use super::super::SlurmPlace;
    use super::super::cli::{Slurm, SlurmCommand};
    use super::super::hubs::Address;
    use super::super::jobs::Job;
    use super::*;
    use crate::Reply;
    use std::time::Duration;

    struct Contended {
        dir: PathBuf,
        contend: bool,
        running: bool,
        release: Option<std::thread::JoinHandle<()>>,
    }

    impl Slurm for Contended {
        fn run(&mut self, line: &SlurmCommand) -> Result<Reply, Error> {
            match line.program.as_str() {
                "squeue" if line.args.iter().any(|arg| arg == "--steps") => {
                    Ok(Reply::ok("42.1|service_engine|node-42\n"))
                }
                "squeue" => Ok(Reply::ok(if self.running {
                    "42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n"
                } else {
                    ""
                })),
                "scancel" if line.args == ["--signal=TERM", "42.1"] => {
                    if !self.contend {
                        return Ok(Reply::ok(""));
                    }
                    let file = File::options()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(self.dir.join(LOCK))
                        .expect("the registry lock");
                    file.lock().expect("another join holds the lock");
                    // Hold longer than the stop bound, but less than the objects wait.
                    self.release = Some(std::thread::spawn(move || {
                        std::thread::sleep(Duration::from_millis(300));
                        drop(file);
                    }));
                    Ok(Reply::ok(""))
                }
                "scancel" if line.args == ["42"] => {
                    self.running = false;
                    Ok(Reply::ok(""))
                }
                "curl" => Ok(Reply::ok("{\"instances\":[]}\n200")),
                _ => panic!("unexpected command: {line:?}"),
            }
        }
    }

    #[test]
    fn a_registry_wait_past_the_stop_deadline_must_not_cancel_the_pool() {
        registry_stop(true);
    }

    #[test]
    fn an_uncontended_registry_allows_an_empty_pool_to_stop() {
        registry_stop(false);
    }

    fn registry_stop(contend: bool) {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-review-bound-")
            .tempdir()
            .expect("the fixture");
        let run = scratch.path().join("runs");
        let dir = run.join("api/p");
        std::fs::create_dir_all(&dir).expect("the pool directory");
        let fleet = gdl_model::Fleet::from_toml(&format!(
            "[fleet]\nname = \"api\"\n[cluster.lab]\nkind = \"slurm\"\nregion = \"lab\"\nsrtctl = \"/unused\"\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n[class.global]\nready = {{ engine = 1 }}\n[pool.p]\nclass = \"global\"\ncluster = \"lab\"\n",
            run.display().to_string()
        )).expect("the fleet");
        let mut slurm = Contended {
            dir,
            contend,
            running: true,
            release: None,
        };
        let waits = Waits {
            interval: Duration::from_millis(1),
            objects: Duration::from_secs(2),
            drain: Duration::from_millis(20),
            health: Duration::from_secs(1),
        };
        let result = {
            let mut place = SlurmPlace::new(&mut slurm, &fleet, waits, false).expect("the site");
            let job = Job::list("42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n", "api").remove(0);
            place.hubs.insert(
                "p".into(),
                Address {
                    job: "42".into(),
                    public: "node-42:1337".into(),
                    control: "node-42:8337".into(),
                },
            );
            place.found.pools.insert("p".into(), job);
            // A short grace tests the stop loop without a thirty-second timer.
            place.found.parentless.insert("p".into(), 0);
            place.remove("lab", Which::Pools(&["p"]))
        };
        if let Some(release) = slurm.release.take() {
            release.join().expect("the holder ended");
        }
        if contend {
            assert!(
                result.is_err() && slurm.running,
                "a lock held for 300 ms must exhaust the 20 ms stop bound without cancellation: result={result:?}, running={}",
                slurm.running
            );
        } else {
            assert!(
                result.is_ok() && !slurm.running,
                "an empty pool without contention stops: result={result:?}, running={}",
                slurm.running
            );
        }
    }
}
