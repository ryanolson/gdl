//! `deploy` and `down` of a fleet on a Slurm site, against a fake Slurm
//! held in memory: it records each command line, keeps each job as Slurm
//! would list it, and answers each hub route as a hub would. A job that
//! starts writes what its own would: the directory's job, its endpoint
//! file and the fleet's pointer to it; a pool's job, its hub's address. So
//! these tests read which jobs a deploy submits, keeps, and refuses, the
//! order in which `down` stops them, and that no command line carries a
//! credential. `crates/gdl-gen/tests/srtctl.rs` runs real hubs and
//! segments under fake Slurm commands on the `PATH`.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::*;

#[path = "slurm/accounting.rs"]
mod accounting;
#[path = "slurm/completing.rs"]
mod completing;
#[path = "slurm/directory.rs"]
mod directory;
#[path = "slurm/down.rs"]
mod down;
#[path = "slurm/gone.rs"]
mod gone;
#[path = "slurm/hostnames.rs"]
mod hostnames;
#[path = "slurm/monitoring.rs"]
mod monitoring;
#[path = "slurm/pending.rs"]
mod pending;
#[path = "slurm/reconcile.rs"]
mod reconcile;
#[path = "slurm/scheduling.rs"]
mod scheduling;
#[path = "slurm/signalling.rs"]
mod signalling;
#[path = "slurm/version.rs"]
mod version;
#[path = "slurm/visibility.rs"]
mod visibility;

/// A fleet on Slurm site `lab`: a global pool, and a pool under it, with
/// the DynamoDB Local beside them. `{srtctl}` and `{run_dir}` are the
/// test's own directories.
const FLEET: &str = "[fleet]\nname = \"api\"\n\n\
    [cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = \"{srtctl}\"\nrun_dir = \"{run_dir}\"\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n{site}\n\
    [directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n\n\
    [class.global]\nready = { front = 1 }\n[class.model_x]\nready = { engine = {engines} }\n\n\
    [pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n\n\
    [pool.model-x-01]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n";

/// A Slurm site's directories, the fleet's files, and the digests of its
/// images.
struct Site {
    files: Files,
    _dir: tempfile::TempDir,
    srtctl: PathBuf,
    /// `<run_dir>/api`: the fleet's directory.
    fleet_dir: PathBuf,
    images: PathBuf,
}

impl Site {
    /// The fleet with `engines` engines in each model pool, and `site`'s
    /// keys on the site.
    fn new(mirror: &Mirror, engines: u32, site: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-slurm-site-")
            .tempdir()
            .expect("a directory");
        let srtctl = dir.path().join("srt-slurm");
        let run_dir = dir.path().join("runs");
        for made in [&srtctl, &run_dir] {
            std::fs::create_dir_all(made).expect("a directory");
        }
        let fleet = FLEET
            .replace("{srtctl}", &srtctl.display().to_string())
            .replace("{run_dir}", &run_dir.display().to_string())
            .replace("{site}", site)
            .replace("{engines}", &engines.to_string());
        let files = files(mirror, &fleet);
        // No Slurm job is given a secret: the spec keeps the rest of its
        // `[deployment]`.
        let spec = std::fs::read_to_string(&files.spec).expect("the spec");
        let (head, secret) = spec
            .split_once("[deployment.secrets.fingerprint]\n")
            .expect("the secret");
        let (_, tail) = secret.split_once('\n').expect("its file");
        let spec = format!("{head}{tail}").replace("secrets = [\"fingerprint\"]\n", "");
        std::fs::write(&files.spec, spec).expect("the spec");
        let images = dir.path().join("images.json");
        std::fs::write(&images, digests(mirror, &files)).expect("the digests");
        Self {
            files,
            _dir: dir,
            srtctl,
            fleet_dir: run_dir.join("api"),
            images,
        }
    }

    /// The fleet file again, with `engines` engines in each model pool.
    fn change(&self, engines: u32) {
        let text = std::fs::read_to_string(&self.files.fleet).expect("the fleet");
        let text = text
            .lines()
            .map(|line| match line.starts_with("ready = { engine = ") {
                true => format!("ready = {{ engine = {engines} }}"),
                false => line.to_string(),
            })
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&self.files.fleet, text + "\n").expect("the fleet");
    }

    fn deploy(
        &self,
        mirror: &Mirror,
        slurm: &mut Fake,
        replace: bool,
    ) -> Result<Vec<String>, gdl_gen::Error> {
        self.deploy_with(mirror, slurm, replace, slurm_waits())
    }

    fn deploy_with(
        &self,
        mirror: &Mirror,
        slurm: &mut Fake,
        replace: bool,
        waits: gdl_gen::Waits,
    ) -> Result<Vec<String>, gdl_gen::Error> {
        let how = gdl_gen::Deploy {
            spec: self.files.spec.clone(),
            fleet: self.files.fleet.clone(),
            target: mirror.target(),
            out: None,
            waits,
            sources: gdl_gen::Sources::Fetched,
            allow_pool_restart: false,
            images: Some(self.images.clone()),
            replace,
        };
        let log = Log::default();
        let mut docker = Docker {
            log: Rc::clone(&log),
            fail: false,
        };
        let mut kube = Kube::new(&log);
        let mut said = Vec::new();
        let result = gdl_gen::deploy(&how, &mut docker, &mut kube, slurm, &mut |step| {
            said.push(step.to_string())
        });
        // A fleet on Slurm builds nothing, and reaches no Kubernetes.
        assert_eq!(steps(&log), Vec::<String>::new());
        result.map(|()| said)
    }

    fn down(&self, slurm: &mut Fake) -> Result<Vec<String>, gdl_gen::Error> {
        let log = Log::default();
        let mut kube = Kube::new(&log);
        let mut said = Vec::new();
        let result = gdl_gen::down(
            &self.files.fleet,
            &slurm_waits(),
            &mut kube,
            slurm,
            &mut |step| said.push(step.to_string()),
        );
        assert_eq!(steps(&log), Vec::<String>::new());
        result.map(|()| said)
    }
}

/// The digests of the fleet's images, as `cargo gdl images --push --out`
/// writes them, from a Docker that builds nothing.
fn digests(mirror: &Mirror, files: &Files) -> String {
    let spec = gdl_gen::Spec::load(&files.spec).expect("the spec");
    let fleet =
        gdl_model::Fleet::from_toml(&std::fs::read_to_string(&files.fleet).expect("the fleet"))
            .expect("a fleet");
    let dgds = gdl_gen::Dgds::read(&fleet, &files.fleet).expect("no DGD");
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let built = gdl_gen::images(
        &spec,
        &fleet,
        &dgds,
        &mirror.target(),
        true,
        gdl_gen::Sources::Fetched,
        &mut docker,
    )
    .expect("the images");
    gdl_gen::Digests::of(&built).to_json()
}

/// Waits that never sleep long: the fake answers at once.
fn slurm_waits() -> gdl_gen::Waits {
    gdl_gen::Waits {
        interval: Duration::from_millis(1),
        health: Duration::from_secs(2),
        drain: Duration::from_millis(200),
        objects: Duration::from_secs(5),
    }
}

/// One job, as the fake Slurm holds it.
#[derive(Debug, Clone)]
struct Job {
    id: u64,
    state: &'static str,
    name: String,
    comment: String,
    /// Its pool's directory, for a pool's job: where its hub writes.
    dir: Option<PathBuf>,
    /// Whether a `SIGTERM` reached its segments.
    terminated: bool,
    /// How many reads of its hub's `withdrawn` answered.
    withdrawn_reads: usize,
}

/// A Slurm in memory, with one srt-slurm checkout. Each command line it
/// runs is recorded.
struct Fake {
    srtctl: PathBuf,
    calls: Rc<RefCell<Vec<gdl_gen::SlurmCommand>>>,
    next: u64,
    jobs: Vec<Job>,
    /// Names of jobs that wait in the queue when submitted.
    hold: Vec<String>,
    /// A recipe whose submission fails, by the start of its path within
    /// the fleet's directory.
    refuse: Option<String>,
    /// How many reads of a drained hub's `withdrawn` answer false first.
    withdrawn_after: usize,
    /// Each child pool whose record a parent's hub withdrew, as `class/pool`:
    /// a second withdrawal finds no record.
    withdrawals: Vec<String>,
    /// What a parent's hub answers a withdrawal instead, when set.
    withdraw_answer: Option<(u16, &'static str)>,
    /// The job whose hub alone holds a child's record, when set: any other
    /// parent's hub answers that it holds none.
    holder: Option<u64>,
    /// What `scancel --version` prints.
    version: &'static str,
}

impl Fake {
    fn new(site: &Site) -> Self {
        Self {
            srtctl: site.srtctl.clone(),
            calls: Rc::default(),
            next: 1001,
            jobs: Vec::new(),
            hold: Vec::new(),
            refuse: None,
            withdrawn_after: 1,
            withdrawals: Vec::new(),
            withdraw_answer: None,
            holder: None,
            version: "slurm 24.05.4\n",
        }
    }

    /// Each command line run, as the program and its words.
    fn lines(&self) -> Vec<Vec<String>> {
        self.calls
            .borrow()
            .iter()
            .map(|call| {
                std::iter::once(call.program.clone())
                    .chain(call.args.clone())
                    .collect()
            })
            .collect()
    }

    /// Each recipe submitted, by its path within `fleet_dir`.
    fn submitted(&self, fleet_dir: &Path) -> Vec<String> {
        self.calls
            .borrow()
            .iter()
            .filter(|call| call.program == "srtctl")
            .map(|call| {
                Path::new(&call.args[2])
                    .strip_prefix(fleet_dir)
                    .expect("a recipe in the fleet's directory")
                    .display()
                    .to_string()
            })
            .collect()
    }

    /// The jobs Slurm lists, each `id state name`.
    fn listed(&self) -> Vec<String> {
        self.jobs
            .iter()
            .filter(|job| live(job.state))
            .map(|job| format!("{} {} {}", job.id, job.state, job.name))
            .collect()
    }

    /// Start job `id`, as Slurm and srt-slurm would: it runs, and writes
    /// what its first processes write.
    fn start(&mut self, id: u64) {
        let job = self
            .jobs
            .iter_mut()
            .find(|job| job.id == id)
            .expect("a job");
        job.state = "RUNNING";
        if job.comment.contains(":-:directory:") {
            let fleet_dir = job.dir.clone().expect("the fleet's directory");
            let own = format!("directory.{id}.endpoint");
            std::fs::write(fleet_dir.join(&own), "http://10.0.0.1:8000\n").expect("its endpoint");
            let staged = fleet_dir.join(format!("directory.{id}.ptr"));
            let _ = std::fs::remove_file(&staged);
            std::os::unix::fs::symlink(&own, &staged).expect("a link");
            std::fs::rename(&staged, fleet_dir.join("directory.endpoint")).expect("the pointer");
        } else if let Some(dir) = &job.dir {
            std::fs::write(
                dir.join(format!("hub.{id}.address")),
                format!("node-{id}:1337 node-{id}:8337\n"),
            )
            .expect("its hub's address");
        }
    }

    /// Add a job as if it were submitted before, from `recipe`, a file in
    /// `fleet_dir`, in `state`.
    fn seed(&mut self, fleet_dir: &Path, recipe: &str, state: &'static str) -> u64 {
        let id = self.enqueue(&fleet_dir.join(recipe));
        match state {
            "RUNNING" => self.start(id),
            _ => self.job(id).state = state,
        }
        id
    }

    fn job(&mut self, id: u64) -> &mut Job {
        self.jobs
            .iter_mut()
            .find(|job| job.id == id)
            .expect("a job")
    }

    /// Queue the job of `recipe`: its id.
    fn enqueue(&mut self, recipe: &Path) -> u64 {
        let text = std::fs::read_to_string(recipe).expect("the recipe");
        let recipe_json: Value = serde_json::from_str(&text).expect("JSON");
        let id = self.next;
        self.next += 1;
        let comment = recipe_json["sbatch_directives"]["comment"]
            .as_str()
            .expect("a comment")
            .to_string();
        let dir = recipe.parent().expect("its directory").to_path_buf();
        self.jobs.push(Job {
            id,
            state: "PENDING",
            name: recipe_json["name"].as_str().expect("a name").to_string(),
            dir: Some(dir),
            comment,
            terminated: false,
            withdrawn_reads: 0,
        });
        id
    }

    fn line(job: &Job) -> String {
        format!("{}|{}|{}|{}\n", job.id, job.state, job.name, job.comment)
    }

    fn squeue(&mut self, args: &[String]) -> gdl_gen::Reply {
        let flags: Vec<&str> = args.iter().map(String::as_str).collect();
        match flags.as_slice() {
            [
                "--me",
                "--all",
                "--states=all",
                "--noheader",
                "--format=%i|%T|%j|%k",
            ] => gdl_gen::Reply::ok(
                self.jobs
                    .iter()
                    .filter(|job| live(job.state))
                    .map(Self::line)
                    .collect::<String>(),
            ),
            [
                jobs,
                "--all",
                "--states=all",
                "--noheader",
                "--format=%i|%T|%j|%k",
            ] => {
                let id = number(jobs.strip_prefix("--jobs=").expect("--jobs"));
                match self.jobs.iter().find(|job| job.id == id && live(job.state)) {
                    Some(job) => gdl_gen::Reply::ok(Self::line(job)),
                    None => {
                        gdl_gen::Reply::failed("slurm_load_jobs error: Invalid job id specified\n")
                    }
                }
            }
            ["--steps", jobs, "--all", "--noheader", "--format=%i|%j"] => {
                let id = number(jobs.strip_prefix("--jobs=").expect("--jobs"));
                let job = self.job(id);
                let front = match job.name.as_str() {
                    "gdl-api-global-us" => "front",
                    _ => "engine",
                };
                gdl_gen::Reply::ok(format!(
                    "{id}.0|service_hub\n{id}.1|service_{front}\n{id}.2|service_hub.container-prepare\n"
                ))
            }
            other => panic!("squeue {other:?}"),
        }
    }

    fn scancel(&mut self, args: &[String]) -> gdl_gen::Reply {
        match args.first().map(String::as_str) {
            Some("--version") => return gdl_gen::Reply::ok(self.version),
            Some("--state=PENDING") => {
                assert_eq!(args[1], "--ctld", "the controller applies the filter");
                for target in &args[2..] {
                    let job = self.job(number(target));
                    if job.state == "PENDING" {
                        job.state = "CANCELLED";
                    }
                }
            }
            Some("--signal=TERM") => {
                for target in &args[1..] {
                    let id = number(target.split('.').next().expect("a job"));
                    self.job(id).terminated = true;
                }
            }
            _ => {
                for target in args {
                    assert!(!target.starts_with('-'), "scancel {args:?}");
                    let job = self.job(number(target));
                    job.state = "CANCELLED";
                }
            }
        }
        gdl_gen::Reply::ok("")
    }

    /// A hub's answer to `curl`, at the job that its address names.
    fn curl(&mut self, args: &[String]) -> gdl_gen::Reply {
        let url = args.last().expect("a URL");
        let rest = url.strip_prefix("http://node-").expect("a hub's address");
        let (id, rest) = rest.split_once(':').expect("a port");
        let (port, route) = rest.split_once('/').expect("a route");
        let route = format!("/{route}");
        let post = args.windows(2).any(|pair| pair == ["--request", "POST"]);
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--max-time"
                    && pair[1].parse::<u64>().is_ok_and(|secs| secs > 0)),
            "{args:?}"
        );
        assert_eq!(self.job(number(id)).state, "RUNNING", "{url}");
        // A parent's hub withdraws a child's record once: then it holds
        // none.
        if let Some(child) = route
            .strip_prefix("/v1/ext/gdl-pool/children/")
            .and_then(|rest| rest.strip_suffix("/withdraw"))
            .map(str::to_string)
        {
            assert_eq!((port, post), ("8337", true), "{url}");
            let held = !self.withdrawals.contains(&child)
                && self.holder.is_none_or(|holder| holder == number(id));
            let (status, body) = match (self.withdraw_answer, held) {
                (Some((status, body)), _) => (status, body.to_string()),
                (None, true) => {
                    self.withdrawals.push(child);
                    (204, String::new())
                }
                (None, false) => (404, "no record".to_string()),
            };
            return gdl_gen::Reply::ok(format!("{body}\n{status}"));
        }
        let withdrawn_after = self.withdrawn_after;
        let job = self.job(number(id));
        let (status, body) = match (port, route.as_str(), post) {
            ("8337", "/v1/ext/gdl-pool/drain", true) => (200, "{}".to_string()),
            ("1337", "/v1/ext/gdl-pool/health", false) => (200, "{}".to_string()),
            ("1337", "/v1/ext/gdl-pool/withdrawn", false) => {
                job.withdrawn_reads += 1;
                let withdrawn = job.withdrawn_reads > withdrawn_after;
                (200, format!("{{\"withdrawn\": {withdrawn}}}"))
            }
            ("8337", "/v1/ext/gdl-pool/instances", false) => {
                let listed = match job.terminated {
                    true => "[]",
                    false => "[{\"segment\": \"front\"}]",
                };
                (200, format!("{{\"instances\": {listed}}}"))
            }
            other => panic!("curl {other:?}"),
        };
        gdl_gen::Reply::ok(format!("{body}\n{status}"))
    }

    fn srtctl(&mut self, command: &gdl_gen::SlurmCommand) -> gdl_gen::Reply {
        assert_eq!(
            command.dir.as_deref(),
            Some(self.srtctl.as_path()),
            "{command:?}"
        );
        let [apply, f, recipe, json] = command.args.as_slice() else {
            panic!("srtctl {:?}", command.args);
        };
        assert_eq!(
            (apply.as_str(), f.as_str(), json.as_str()),
            ("apply", "-f", "--json")
        );
        if self
            .refuse
            .as_ref()
            .is_some_and(|refused| recipe.contains(refused.as_str()))
        {
            return gdl_gen::Reply {
                success: false,
                stdout:
                    "{\"status\": \"error\", \"error\": \"sbatch: error: invalid partition\"}\n"
                        .into(),
                stderr: String::new(),
            };
        }
        let id = self.enqueue(Path::new(recipe));
        let name = self.job(id).name.clone();
        if !self.hold.contains(&name) {
            self.start(id);
        }
        gdl_gen::Reply::ok(format!(
            "{{\"status\": \"submitted\", \"slurm_job_id\": \"{id}\", \"job_name\": \"{name}\", \"output_dir\": \"/outputs/{id}\"}}\n"
        ))
    }
}

impl gdl_gen::Slurm for Fake {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        self.calls.borrow_mut().push(command.clone());
        assert!(command.budget > Duration::ZERO, "{command:?}");
        Ok(match command.program.as_str() {
            "srtctl" => self.srtctl(command),
            "squeue" => self.squeue(&command.args),
            "scancel" => self.scancel(&command.args),
            "sacct" => {
                let id = number(command.args[0].strip_prefix("--jobs=").expect("--jobs"));
                let state = self.job(id).state;
                gdl_gen::Reply::ok(format!("{id}|{state}|1:0\n{id}.batch|{state}|1:0\n"))
            }
            "curl" => self.curl(&command.args),
            "srun" => {
                let at = command
                    .args
                    .iter()
                    .position(|arg| arg == "curl")
                    .expect("curl in the step");
                self.curl(&command.args[at + 1..])
            }
            other => panic!("{other} {:?}", command.args),
        })
    }
}

fn number(text: &str) -> u64 {
    text.parse().unwrap_or_else(|_| panic!("a job id: {text}"))
}

fn live(state: &str) -> bool {
    matches!(state, "PENDING" | "RUNNING")
}

/// The words of each `scancel` that cancels or signals: not the read of
/// its version.
fn cancels(lines: &[Vec<String>]) -> Vec<Vec<String>> {
    of(lines, "scancel")
        .into_iter()
        .filter(|args| args != &["--version"])
        .collect()
}

/// The words of each command line of `program`.
fn of(lines: &[Vec<String>], program: &str) -> Vec<Vec<String>> {
    lines
        .iter()
        .filter(|line| line[0] == program)
        .map(|line| line[1..].to_vec())
        .collect()
}

/// The first deploy submits the directory's job and waits until the
/// fleet's pointer names it, then submits each pool's job, and waits until
/// each pool's hub answers its health at the address it wrote. It builds
/// no image and reaches no Kubernetes.
#[test]
fn a_deploy_submits_the_directory_then_each_pool_and_waits_for_health() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    let said = site.deploy(&mirror, &mut slurm, false).expect("deployed");
    assert_eq!(
        slurm.submitted(&site.fleet_dir),
        [
            "directory.yaml",
            "global-us/pool.yaml",
            "model-x-01/pool.yaml"
        ]
    );
    assert_eq!(
        slurm.listed(),
        [
            "1001 RUNNING gdl-api-directory",
            "1002 RUNNING gdl-api-global-us",
            "1003 RUNNING gdl-api-model-x-01"
        ]
    );
    // The recipes and the settings they mount are in the fleet's
    // directory.
    for file in [
        "directory.yaml",
        "global-us/pool.yaml",
        "global-us/front.toml",
        "model-x-01/engine.toml",
        "model-x-01/join-engine.yaml",
        "model-x-01/deployment.json",
    ] {
        assert!(site.fleet_dir.join(file).is_file(), "{file}");
    }
    let lines = slurm.lines();
    let health: Vec<String> = of(&lines, "curl")
        .into_iter()
        .map(|args| args.last().expect("a URL").clone())
        .collect();
    assert_eq!(
        health,
        [
            "http://node-1002:1337/v1/ext/gdl-pool/health",
            "http://node-1003:1337/v1/ext/gdl-pool/health"
        ]
    );
    assert_eq!(
        said.iter()
            .filter(|step| step.starts_with("submit:"))
            .count(),
        3,
        "{said:?}"
    );
    assert!(
        said.contains(&"wait: every pool's health answers 200".to_string()),
        "{said:?}"
    );
    no_credential(&lines);
}

/// A second deploy of an unchanged fleet submits nothing: it keeps the
/// directory's job and each pool's.
#[test]
fn a_second_unchanged_deploy_submits_nothing() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    let before = slurm.submitted(&site.fleet_dir).len();
    let said = site
        .deploy(&mirror, &mut slurm, false)
        .expect("deployed again");
    assert_eq!(slurm.submitted(&site.fleet_dir).len(), before);
    assert_eq!(slurm.listed().len(), 3);
    assert!(cancels(&slurm.lines()).is_empty());
    assert_eq!(
        said.iter().filter(|step| step.starts_with("keep:")).count(),
        2,
        "{said:?}"
    );
}

/// No command line names a credential, an account, or a partition: srtctl
/// reads those from its checkout's `srtslurm.yaml`, and Slurm from the
/// user's own session.
fn no_credential(lines: &[Vec<String>]) {
    for line in lines {
        let text = line.join(" ").to_ascii_lowercase();
        for word in [
            "token",
            "password",
            "secret",
            "aws_",
            "--account",
            "--partition",
            "credential",
        ] {
            assert!(!text.contains(word), "{word} in {text}");
        }
    }
}

/// A fleet with a Slurm site and a Kubernetes cluster runs on neither:
/// `deploy` and `down` refuse it, naming both, before they run a command.
#[test]
fn deploy_and_down_refuse_a_fleet_with_both_kinds_of_site() {
    let mirror = Mirror::new();
    let site = "\n[cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = \"/shared/srt-slurm\"\nrun_dir = \"/shared/gdl-runs\"\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n";
    let fleet = format!("{}{site}", plain_fleet());
    let checked = gdl_model::Fleet::from_toml(&fleet)
        .expect("a fleet")
        .check(None);
    assert!(checked.is_ok(), "{checked:?}");
    let files = files(&mirror, &fleet);
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    let named = [
        "Kubernetes clusters (`euw1`, `use1`)",
        "Slurm sites (`lab`)",
        "one kind of site",
    ];
    let refusal = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("refused")
        .to_string();
    for name in named {
        assert!(refusal.contains(name), "{name} in {refusal}");
    }
    let refusal = super::down(&files, &mut kube)
        .expect_err("refused")
        .to_string();
    for name in named {
        assert!(refusal.contains(name), "{name} in {refusal}");
    }
    assert_eq!(steps(&log), Vec::<String>::new());
}

/// A fleet on Slurm takes its images' digests from `--images`, since a
/// login node has no Docker; a fleet on Kubernetes builds its own, and is
/// refused `--images` and `--replace`, as a fleet on Slurm is refused
/// `--allow-pool-restart`. Each refusal comes before any command runs.
#[test]
fn each_kind_of_site_refuses_the_other_kinds_flags() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let refused = |how: gdl_gen::Deploy| {
        let log = Log::default();
        let mut docker = Docker {
            log: Rc::clone(&log),
            fail: false,
        };
        let mut kube = Kube::new(&log);
        let mut slurm = Fake::new(&site);
        let error = gdl_gen::deploy(&how, &mut docker, &mut kube, &mut slurm, &mut |_| {})
            .expect_err("refused")
            .to_string();
        assert!(slurm.lines().is_empty(), "{error}");
        assert_eq!(steps(&log), Vec::<String>::new(), "{error}");
        error
    };
    let how = |fleet: &Path, images: Option<PathBuf>, replace, allow| gdl_gen::Deploy {
        spec: site.files.spec.clone(),
        fleet: fleet.to_path_buf(),
        target: mirror.target(),
        out: None,
        waits: slurm_waits(),
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: allow,
        images,
        replace,
    };
    let error = refused(how(&site.files.fleet, None, false, false));
    assert!(error.contains("needs `--images`"), "{error}");
    let error = refused(how(
        &site.files.fleet,
        Some(site.images.clone()),
        false,
        true,
    ));
    assert!(error.contains("`--allow-pool-restart` is for"), "{error}");
    let kubernetes = files(&mirror, &plain_fleet());
    for (images, replace, flag) in [
        (Some(site.images.clone()), false, "--images"),
        (None, true, "--replace"),
    ] {
        let error = refused(how(&kubernetes.fleet, images, replace, false));
        assert!(
            error.contains(&format!("`{flag}` is for a fleet on Slurm")),
            "{error}"
        );
    }
}

/// A job that fails while the deploy waits for health stops the deploy
/// at once, naming the job, how Slurm says it ended, and where its logs
/// are, long before the health wait would run out.
#[test]
fn a_job_that_fails_during_the_health_wait_stops_the_deploy_at_once() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let mut slurm = Fake::new(&site);
    let mut waits = slurm_waits();
    waits.health = Duration::from_secs(60);
    // The pool's job fails once the deploy first asks about it.
    slurm.hold.push("gdl-api-model-x-01".into());
    let started = Instant::now();
    let mut said = Vec::new();
    let error = {
        let mut slurm_ref = Failing {
            fake: &mut slurm,
            pool: "model-x-01",
        };
        let how = gdl_gen::Deploy {
            spec: site.files.spec.clone(),
            fleet: site.files.fleet.clone(),
            target: mirror.target(),
            out: None,
            waits,
            sources: gdl_gen::Sources::Fetched,
            allow_pool_restart: false,
            images: Some(site.images.clone()),
            replace: false,
        };
        let log = Log::default();
        let mut docker = Docker {
            log: Rc::clone(&log),
            fail: false,
        };
        let mut kube = Kube::new(&log);
        gdl_gen::deploy(&how, &mut docker, &mut kube, &mut slurm_ref, &mut |step| {
            said.push(step.to_string())
        })
        .expect_err("the job failed")
        .to_string()
    };
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "{:?}",
        started.elapsed()
    );
    for named in [
        "pool `model-x-01`",
        "job 1003 ended",
        "FAILED",
        "exit code 1:0",
        "/outputs/1003/logs",
    ] {
        assert!(error.contains(named), "{named} in {error}");
    }
}

/// A fake whose pool `pool`'s job fails once the deploy's health wait has
/// begun: when it first reads a hub's health.
struct Failing<'f> {
    fake: &'f mut Fake,
    pool: &'static str,
}

impl gdl_gen::Slurm for Failing<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        let name = format!("gdl-api-{}", self.pool);
        if command.program == "curl"
            && command
                .args
                .last()
                .is_some_and(|url| url.ends_with("/health"))
            && let Some(job) = self.fake.jobs.iter_mut().find(|job| job.name == name)
        {
            job.state = "FAILED";
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}
