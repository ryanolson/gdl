//! The joiners that `cargo gdl join --in-job` asks a pool's launchers for,
//! and the pool's jobs that `down` stops: one registry file in the pool's
//! directory on the shared file system, `join.requests`, read and written
//! only under an exclusive lock on `join.lock` beside it. A request is
//! recorded here before its launcher can read it, so `down` knows each
//! joiner that runs or may still start; once `down` has marked a job
//! stopping here, no `join` asks for another.
//!
//! One file under a lock, not a file per request in a directory listing:
//! the login nodes share the directory over a network file system, whose
//! clients cache a directory's listing for up to a minute. An NFS client
//! revalidates a file's data when it takes a lock on it, and when it opens
//! it after another client closed it (close-to-open), but not a
//! directory's cached listing. See `docs/src/guides/slurm.md`.

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::Waits;
use super::jobs::{Job, Step};
use crate::Error;
use crate::deployment::slurm::launcher;
use gdl_model::launch::{self, Done};

/// The registry, in a pool's directory.
const REGISTRY: &str = "join.requests";

/// The lock that every reader and writer of the registry, and every
/// writer of a launcher's `run` and `stop` records, holds.
const LOCK: &str = "join.lock";

/// One joiner asked of the launcher of `segment` on `node` of a pool's
/// `job`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Request {
    pub(super) job: String,
    pub(super) node: String,
    pub(super) segment: String,
    /// The launcher's step, `<job>.<step>`, as the launcher said when it
    /// was up: the joiner lives in that step, and ends with it.
    pub(super) step: String,
    /// The request's number in its launcher's order, from 1: its records
    /// are `<n>.run`, `<n>.stop`, `<n>.started`, and `<n>.done`.
    pub(super) n: u64,
    /// The launch's name, which only this request has.
    pub(super) launch: String,
}

impl Request {
    /// Where its launcher's records are, in `dir`, its pool's directory.
    pub(super) fn records(&self, dir: &Path) -> PathBuf {
        launcher::records(dir, &self.job, &self.node, &self.segment)
    }

    /// How it ended, as its launcher's records in `dir`, its pool's
    /// directory, say: `None` before it has.
    pub(super) fn done(&self, dir: &Path) -> Option<Done> {
        launcher::read(&self.records(dir), &launch::done(self.n)).and_then(|text| Done::read(&text))
    }

    /// Whether its launcher has started its joiner.
    pub(super) fn started(&self, dir: &Path) -> bool {
        launcher::read(&self.records(dir), &launch::started(self.n)).is_some()
    }

    /// Ask its launcher to stop it, once: one not started yet never will
    /// be; one that has gets one `TERM`. Records not seen here are not
    /// written: the job's teardown may have removed them, or this client
    /// may not see them yet, so a stop of a job that `down` stops is posted
    /// again on each turn of its wait ([`super::SlurmPlace::joiners`]).
    pub(super) fn stop(&self, dir: &Path) -> Result<(), Error> {
        let records = self.records(dir);
        if !records.exists() || launcher::read(&records, &launch::stop(self.n)).is_some() {
            return Ok(());
        }
        launcher::write_once(
            &records,
            &launch::stop(self.n),
            &format!("{}\n", self.launch),
        )
        .map(|_| ())
        .map_err(|e| Error::new(format!("{}: {e}", records.display())))
    }
}

/// The registry of the pool whose directory is `dir`, held under its lock
/// until dropped.
pub(super) struct Registry {
    _lock: File,
    path: PathBuf,
    /// The requests, as read when the lock was taken.
    pub(super) requests: Vec<Request>,
    /// The pool's jobs that `down` stops: no `join` asks them for another
    /// joiner.
    pub(super) stopping: Vec<String>,
    /// Each launcher's next request number, as `(job, node, segment, n)`.
    next: Vec<(String, String, String, u64)>,
}

impl Registry {
    /// Take the lock of the registry in `dir`, pool `pool`'s directory,
    /// waiting for another holder up to the objects' wait, or until `until`
    /// when it comes sooner, then read it. A file system that takes no lock
    /// is refused: nothing else keeps two login nodes from asking one node
    /// for two joiners.
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
                        "locking {}, which keeps the joiners asked of the pool's launchers: {e}",
                        path.display()
                    )));
                }
            }
        }
        let path = Self::path(dir);
        // Opened after the lock is taken, so that a network file system
        // reads what the last holder wrote.
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(Error::new(format!("{}: {e}", path.display()))),
        };
        let mut registry = Self {
            _lock: lock,
            path,
            requests: Vec::new(),
            stopping: Vec::new(),
            next: Vec::new(),
        };
        for line in text.lines() {
            let words: Vec<&str> = line.split_whitespace().collect();
            match words.as_slice() {
                ["request", job, node, segment, step, n, launch] => {
                    let Ok(n) = n.parse() else { continue };
                    registry.requests.push(Request {
                        job: job.to_string(),
                        node: node.to_string(),
                        segment: segment.to_string(),
                        step: step.to_string(),
                        n,
                        launch: launch.to_string(),
                    });
                }
                ["next", job, node, segment, n] => {
                    let Ok(n) = n.parse() else { continue };
                    registry
                        .next
                        .push((job.to_string(), node.to_string(), segment.to_string(), n));
                }
                ["stopping", job] => registry.stopping.push(job.to_string()),
                _ => {}
            }
        }
        Ok(registry)
    }

    /// Whether `down` stops job `job`.
    pub(super) fn stops(&self, job: &str) -> bool {
        self.stopping.iter().any(|stopping| stopping == job)
    }

    /// The next request number of the launcher of `segment` on `node` of
    /// `job`, taken: each is asked for once, in order from 1.
    pub(super) fn take_next(&mut self, job: &str, node: &str, segment: &str) -> u64 {
        let at = self
            .next
            .iter()
            .position(|(j, n, s, _)| j == job && n == node && s == segment);
        match at {
            Some(at) => {
                let n = self.next[at].3;
                self.next[at].3 = n + 1;
                n
            }
            None => {
                self.next
                    .push((job.to_string(), node.to_string(), segment.to_string(), 2));
                1
            }
        }
    }

    /// Forget what concerns a job that is not in `live`, the pool's jobs
    /// that Slurm lists: its joiners ended with it. A job that Slurm lists
    /// keeps all it has, whatever job the caller works on, which may have
    /// been replaced since the caller read it.
    pub(super) fn forget_ended_jobs(&mut self, live: &[&str]) {
        self.requests
            .retain(|request| live.contains(&request.job.as_str()));
        self.stopping.retain(|job| live.contains(&job.as_str()));
        self.next.retain(|(job, ..)| live.contains(&job.as_str()));
    }

    /// Write the registry back under the lock: to a file beside it, flushed
    /// to the file system, then renamed over it. A write that fails, such
    /// as on a full disk, leaves the registry as it was: a truncated one
    /// would lose the requests whose joiners still run.
    pub(super) fn save(&self) -> Result<(), Error> {
        let staged = self.path.with_extension("requests.tmp");
        let io = |e: std::io::Error| Error::new(format!("{}: {e}", staged.display()));
        let mut file = File::create(&staged).map_err(io)?;
        for job in &self.stopping {
            writeln!(file, "stopping {job}").map_err(io)?;
        }
        for (job, node, segment, n) in &self.next {
            writeln!(file, "next {job} {node} {segment} {n}").map_err(io)?;
        }
        for request in &self.requests {
            writeln!(
                file,
                "request {} {} {} {} {} {}",
                request.job, request.node, request.segment, request.step, request.n, request.launch
            )
            .map_err(io)?;
        }
        file.sync_all().map_err(io)?;
        std::fs::rename(&staged, &self.path)
            .map_err(|e| Error::new(format!("{}: {e}", self.path.display())))
    }

    /// Where the registry of the pool whose directory is `dir` is, for a
    /// message.
    pub(super) fn path(dir: &Path) -> PathBuf {
        dir.join(REGISTRY)
    }
}

/// Whether request `request` has ended: its launcher says so in `dir`,
/// its pool's directory, or its launcher's step is not among `steps`, a
/// listing of its job's steps, and whatever it started ended with the
/// step.
pub(super) fn ended(dir: &Path, request: &Request, steps: &[Step]) -> bool {
    !steps.iter().any(|step| step.id == request.step) || request.done(dir).is_some()
}

impl super::SlurmPlace<'_> {
    /// Close pool `pool`'s job `job` to joiners, as its stop starts: under
    /// the registry's lock, mark it stopping, so that no `join` asks for
    /// another, and ask each launcher to stop each joiner of the job it was
    /// asked for that has not ended. One that has not started yet never
    /// will; one that has gets one `TERM`; one whose `run` never landed is
    /// dropped. What other jobs asked for stays. Its lock is waited for
    /// until `until`.
    pub(super) fn close_launches(
        &mut self,
        pool: &str,
        job: &Job,
        until: Instant,
    ) -> Result<(), Error> {
        let dir = self.home.dir.join(pool);
        // A pool with no directory has no launcher records, and no `join`
        // can ask one for a joiner.
        if !dir.exists() {
            return Ok(());
        }
        let mut registry = Registry::open(&dir, pool, &self.waits, Some(until))?;
        if !registry.stops(&job.id) {
            registry.stopping.push(job.id.clone());
        }
        registry.save()?;
        for request in registry
            .requests
            .iter()
            .filter(|request| request.job == job.id)
        {
            if request.done(&dir).is_none() {
                request.stop(&dir)?;
            }
        }
        Ok(())
    }

    /// The joiners of pool `pool`'s job `job` that its stop must still
    /// wait for, each named for a message: each request of the job whose
    /// launcher has not said that it ended, while the launcher's step is
    /// listed. Those that ended leave the registry; other jobs' stay. While
    /// `down` stops the job, each that has not ended is asked again to
    /// stop, so that a launcher whose records this client did not see when
    /// [`Self::close_launches`] asked gets its stop once it does. Its lock
    /// is waited for until `until`.
    pub(super) fn joiners(
        &mut self,
        pool: &str,
        job: &Job,
        until: Instant,
    ) -> Result<Vec<String>, Error> {
        let dir = self.home.dir.join(pool);
        let what = format!("stopping pool `{pool}`");
        let mut registry = Registry::open(&dir, pool, &self.waits, Some(until))?;
        let steps = self.placed(&job.id, &what)?;
        let stopping = registry.stops(&job.id);
        let mut outstanding = Vec::new();
        let before = registry.requests.len();
        let mut kept = Vec::new();
        for request in std::mem::take(&mut registry.requests) {
            if request.job == job.id {
                if ended(&dir, &request, &steps) {
                    continue;
                }
                if stopping {
                    request.stop(&dir)?;
                }
                outstanding.push(format!(
                    "the joiner of launch {} of `{}` on node {}",
                    request.launch, request.segment, request.node
                ));
            }
            kept.push(request);
        }
        registry.requests = kept;
        if registry.requests.len() != before {
            registry.save()?;
        }
        Ok(outstanding)
    }

    /// Wait until each joiner of pool `pool`'s job `job` has ended,
    /// bounded by `bound`: [`Self::close_launches`] asked each to stop, so
    /// it gets one `SIGTERM`, leaves, and exits. A pool with a parent
    /// drains through its hub, and a joiner that started as the drain did
    /// gets its signal this way, before the job is cancelled.
    pub(super) fn joiners_end(
        &mut self,
        pool: &str,
        job: &Job,
        bound: std::time::Duration,
    ) -> Result<(), Error> {
        let deadline = Instant::now() + bound;
        loop {
            let left = self.joiners(pool, job, deadline)?;
            if left.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::new(format!(
                    "pool `{pool}`'s joiners did not end within {} s of their stop, so job {} is not cancelled: still to end: {}",
                    bound.as_secs(),
                    job.id,
                    left.join(", ")
                )));
            }
            std::thread::sleep(self.waits.interval);
        }
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

    /// A save that cannot complete leaves the registry as it was: here the
    /// file it is staged in cannot be made. A registry truncated by a save
    /// that failed half way, such as on a full disk, would lose the
    /// requests whose joiners still run.
    #[test]
    fn a_save_that_fails_leaves_the_registry_as_it_was() {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-requests-save-")
            .tempdir()
            .expect("a scratch directory");
        let dir = scratch.path();
        let kept = "request 42 n1 engine 42.4 1 l1\n";
        std::fs::write(dir.join(REGISTRY), kept).expect("a registry");
        let waits = Waits {
            interval: Duration::from_millis(1),
            objects: Duration::from_secs(1),
            drain: Duration::from_millis(1),
            health: Duration::from_millis(1),
        };
        let mut registry = Registry::open(dir, "p", &waits, None).expect("the registry");
        assert_eq!(registry.requests.len(), 1);
        registry.requests.push(Request {
            job: "42".into(),
            node: "n2".into(),
            segment: "engine".into(),
            step: "42.5".into(),
            n: 1,
            launch: "l2".into(),
        });
        std::fs::create_dir(dir.join("join.requests.tmp")).expect("in the way");
        assert!(registry.save().is_err());
        assert_eq!(
            std::fs::read_to_string(dir.join(REGISTRY)).expect("the registry"),
            kept
        );
        std::fs::remove_dir(dir.join("join.requests.tmp")).expect("out of the way");
        registry.save().expect("saved");
        drop(registry);
        let again = Registry::open(dir, "p", &waits, None).expect("the registry");
        assert_eq!(again.requests.len(), 2);
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
            "[fleet]\nname = \"api\"\n[cluster.lab]\nkind = \"slurm\"\nregion = \"lab\"\nsrtctl = \"/unused\"\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n[class.global]\nready = {{ engine = 1 }}\n[pool.p]\nclass = \"global\"\ncluster = \"lab\"\nslurm = {{ spare_nodes = 1 }}\n",
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

    struct Visibility {
        records: PathBuf,
    }

    impl Slurm for Visibility {
        fn run(&mut self, command: &SlurmCommand) -> Result<Reply, Error> {
            assert_eq!(command.program, "squeue");
            assert!(command.args.iter().any(|arg| arg == "--steps"));
            std::fs::create_dir_all(&self.records).expect("records become visible");
            if self.records.join("1.stop").exists() {
                std::fs::write(self.records.join("1.done"), "exited 0\n").expect("stopped");
            }
            Ok(Reply::ok("42.9|service_launch_engine|n1\n"))
        }
    }

    /// `down` asks a launcher whose records it sees to stop its joiner.
    #[test]
    fn a_visible_launcher_receives_its_stop() {
        stop_after_visibility(true);
    }

    /// `down` asks again, as it waits, a launcher whose records it did not
    /// see when it first asked: NFS can cache that a name is missing on one
    /// client after another has made it.
    #[test]
    fn a_launcher_that_becomes_visible_receives_its_stop() {
        stop_after_visibility(false);
    }

    fn stop_after_visibility(visible: bool) {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-review-visibility-")
            .tempdir()
            .expect("scratch");
        let run = scratch.path().join("runs");
        let dir = run.join("api/p");
        std::fs::create_dir_all(&dir).expect("pool");
        let fleet = gdl_model::Fleet::from_toml(&format!(
            "[fleet]\nname = \"api\"\n[cluster.lab]\nkind = \"slurm\"\nregion = \"lab\"\nsrtctl = \"/unused\"\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n[class.global]\nready = {{ engine = 1 }}\n[pool.p]\nclass = \"global\"\ncluster = \"lab\"\nslurm = {{ spare_nodes = 1 }}\n",
            run.display().to_string()
        )).expect("fleet");
        let waits = Waits {
            interval: Duration::from_millis(1),
            objects: Duration::from_secs(1),
            drain: Duration::from_millis(20),
            health: Duration::from_secs(1),
        };
        let request = Request {
            job: "42".into(),
            node: "n1".into(),
            segment: "engine".into(),
            step: "42.9".into(),
            n: 1,
            launch: "a1".into(),
        };
        let records = request.records(&dir);
        if visible {
            std::fs::create_dir_all(&records).expect("records already visible");
        }
        let mut registry = Registry::open(&dir, "p", &waits, None).expect("registry");
        registry.requests.push(request);
        registry.save().expect("saved");
        drop(registry);
        let mut slurm = Visibility {
            records: records.clone(),
        };
        let mut place = SlurmPlace::new(&mut slurm, &fleet, waits, false).expect("place");
        let job = Job::list("42|RUNNING|gdl-api-p|gdl:api:p:pool:hash\n", "api").remove(0);
        place
            .close_launches("p", &job, Instant::now() + Duration::from_secs(1))
            .expect("closed");
        let result = place.joiners_end("p", &job, Duration::from_millis(20));
        assert!(
            result.is_ok(),
            "launcher records are visible now and its step still runs, but no stop arrived: stop_exists={}, result={result:?}",
            records.join("1.stop").exists()
        );
    }
}

#[cfg(test)]
mod ending {
    use crate::{Error, Kube, Reply, Slurm, SlurmCommand, Waits, down};
    use std::time::Duration;

    /// A pool's job that is already ending, with no recipe left to read,
    /// stops by its cancel alone, as `down` found it: its launchers' steps
    /// end with it, so it is closed to joiners and waits for none.
    #[test]
    fn an_ending_pool_job_with_no_recipe_is_cancelled() {
        struct Completing {
            cancelled: bool,
        }
        impl Slurm for Completing {
            fn run(&mut self, command: &SlurmCommand) -> Result<Reply, Error> {
                match command.program.as_str() {
                    "scancel" if command.args == ["--version"] => Ok(Reply::ok("slurm 24.05.4\n")),
                    "scancel" => {
                        assert_eq!(command.args, ["42"]);
                        self.cancelled = true;
                        Ok(Reply::ok(""))
                    }
                    "squeue" => Ok(Reply::ok(if self.cancelled {
                        ""
                    } else {
                        "42|COMPLETING|gdl-api-p|gdl:api:p:pool:hash\n"
                    })),
                    _ => panic!("unexpected: {command:?}"),
                }
            }
        }
        let scratch = tempfile::Builder::new()
            .prefix("gdl-completing-")
            .tempdir()
            .expect("scratch");
        let text = format!(
            "[fleet]\nname = \"api\"\n[cluster.lab]\nkind = \"slurm\"\nregion = \"lab\"\nsrtctl = \"/unused\"\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n[class.global]\nready = {{ engine = 1 }}\n[pool.p]\nclass = \"global\"\ncluster = \"lab\"\n",
            scratch.path().display().to_string()
        );
        let fleet = scratch.path().join("fleet.toml");
        std::fs::write(&fleet, text).expect("fleet");
        let waits = Waits {
            interval: Duration::from_millis(1),
            objects: Duration::from_secs(1),
            drain: Duration::from_millis(20),
            health: Duration::from_secs(1),
        };
        let mut slurm = Completing { cancelled: false };
        struct NoKube;
        impl Kube for NoKube {
            fn run(&mut self, args: &[String], _: Option<&[u8]>) -> Result<Reply, Error> {
                panic!("no Kubernetes command: {args:?}");
            }
        }
        let result = down(&fleet, &waits, &mut NoKube, &mut slurm, &mut |_| {});
        assert!(
            result.is_ok() && slurm.cancelled,
            "a completing job needs no drain recipe: result={result:?}, cancelled={}",
            slurm.cancelled
        );
    }
}
