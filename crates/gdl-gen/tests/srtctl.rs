//! `cargo gdl deploy` and `cargo gdl down` of a fleet on a Slurm site, run
//! as the binary, with fake `srtctl`, `squeue`, `scancel`, and `sacct` on
//! the `PATH` and real hubs and segments on this host. Each fake hands its
//! arguments to this test, which plays Slurm and srt-slurm: `srtctl
//! apply` starts a recipe's services as local processes in srt-slurm's
//! order, each in declaration order, one instance at a time, each started
//! once the one before passed its readiness probe. `curl` is the real one,
//! behind a wrapper that logs each call, so the log holds every command in
//! the order it ran.
//!
//! The fake does not keep the recipe's ports: its nodes share this host,
//! and every listener binds all addresses, so each process gets ports of
//! its own, and each hub writes those to its address file in place of the
//! ones its preamble would write. The render tests check the recipes' own
//! ports, node by node, and the preamble's file. The directory is the
//! `dir` backend in a scratch directory, in place of the DynamoDB Local
//! the directory's job runs; that job writes its endpoint file and the
//! fleet's pointer as its tables step does.
//!
//! The test builds the api example's segments and the hub in nested cargo
//! runs that share the examples' target directory: a cold build takes
//! minutes, a warm one seconds. The deploy and the down then take tens of
//! seconds, since `cargo gdl` waits 2 s between reads of a hub.

#![cfg(unix)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead as _, BufReader, Write as _};
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{Lines, Running, cargo, generated, root, target};
use serde_json::Value;

/// How long one readiness probe, or one process's exit, is waited for.
const WAIT: Duration = Duration::from_secs(30);

/// The api example in two classes: the global pool's `front`, and the
/// model pools' `engine`.
fn spec_file(dir: &Path) -> PathBuf {
    let root = root();
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("the api spec");
    let api = root.join("examples/api");
    let text = text
        .replace(
            "path = \"api.gdl\"",
            &format!("path = {:?}", api.join("api.gdl").display().to_string()),
        )
        .replace(
            "crates = [\".\"]",
            &format!("crates = [{:?}]", api.display().to_string()),
        )
        .replace(
            "path = \"../../crates/gdl-runtime\"",
            &format!(
                "path = {:?}",
                root.join("crates/gdl-runtime").display().to_string()
            ),
        );
    let file = dir.join("gdl.toml");
    std::fs::write(
        &file,
        format!(
            "{text}[deployment]\n[class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"engine\"]\n"
        ),
    )
    .expect("the spec");
    file
}

/// A global pool and a pool under it on Slurm site `lab`.
fn fleet_file(dir: &Path, srtctl: &Path, run_dir: &Path) -> PathBuf {
    let file = dir.join("fleet.toml");
    std::fs::write(
        &file,
        format!(
            "[fleet]\nname = \"api\"\n\n\
             [cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = {:?}\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n\n\
             [directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n\n\
             [class.global]\nready = {{ front = 1 }}\n[class.model_x]\nready = {{ engine = 1 }}\n\n\
             [pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n\n\
             [pool.model-x-01]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n",
            srtctl.display().to_string(),
            run_dir.display().to_string()
        ),
    )
    .expect("the fleet");
    file
}

/// A Docker that builds nothing and gives each image a digest of its own.
struct Docker(usize);

impl gdl_gen::Docker for Docker {
    fn digest(&mut self, _reference: &str) -> Result<Option<String>, gdl_gen::Error> {
        Ok(None)
    }

    fn build(
        &mut self,
        _context: &Path,
        _platform: &str,
        _reference: &str,
        _push: bool,
    ) -> Result<String, gdl_gen::Error> {
        self.0 += 1;
        Ok(format!("sha256:{:064x}", self.0))
    }
}

/// The digests file of the fleet's images, as `cargo gdl images --push
/// --out --local` writes it.
fn images(spec: &Path, fleet: &Path, out: &Path) {
    let loaded = gdl_gen::Spec::load(spec).expect("the spec");
    let read = gdl_model::Fleet::from_toml(&std::fs::read_to_string(fleet).expect("the fleet"))
        .expect("a fleet");
    let dgds = gdl_gen::Dgds::read(&read, fleet).expect("no DGD");
    let built = gdl_gen::images(
        &loaded,
        &read,
        &dgds,
        &target("examples"),
        true,
        gdl_gen::Sources::Local,
        &mut Docker(0),
    )
    .expect("the images");
    std::fs::write(out, gdl_gen::Digests::of(&built).to_json()).expect("the digests");
}

/// The api example's segments and the hub binary, built: the directory
/// that holds the segments' binaries, and the hub's.
fn build(spec: &Path, dir: &Path) -> (PathBuf, PathBuf) {
    let target = target("examples");
    let bin = dir.join("bin");
    let build = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "build", "--spec"])
        .arg(spec)
        .arg("--target-dir")
        .arg(&target)
        .arg("--out")
        .arg(&bin)
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo gdl build");
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );
    let hub = cargo()
        .args([
            "build",
            "-p",
            "gdl-hub",
            "--bin",
            "gdl-hub",
            "--manifest-path",
        ])
        .arg(root().join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", &target)
        .output()
        .expect("cargo build");
    assert!(
        hub.status.success(),
        "{}",
        String::from_utf8_lossy(&hub.stderr)
    );
    (bin, target.join("debug/gdl-hub"))
}

/// The fake Slurm commands, in `dir`: each logs its line to `<state>/calls`
/// and hands its arguments to the test through `<state>/requests`, then
/// prints the answer the test writes to `<state>/replies`. The `curl`
/// wrapper logs its line and runs the real `curl`.
fn fakes(dir: &Path, curl: &Path) {
    std::fs::create_dir_all(dir).expect("the fakes' directory");
    let log = r#"line="$(basename "$0")"; for a in "$@"; do line="$line $a"; done; printf '%s\n' "$line" >> "$GDL_FAKE_SLURM/calls""#;
    let ask = r#"id="$$"; req="$GDL_FAKE_SLURM/requests/$id"; rep="$GDL_FAKE_SLURM/replies/$id"
{ basename "$0"; pwd; for a in "$@"; do printf '%s\n' "$a"; done; } > "$req.tmp" && mv "$req.tmp" "$req"
i=0
while [ ! -e "$rep.code" ]; do
  i=$((i + 1)); if [ "$i" -gt 1200 ]; then echo "fake $(basename "$0"): no answer" >&2; exit 99; fi
  sleep 0.05
done
cat "$rep.out"; cat "$rep.err" >&2; code="$(cat "$rep.code")"; rm -f "$rep.out" "$rep.err" "$rep.code"; exit "$code""#;
    for name in ["srtctl", "squeue", "scancel", "sacct"] {
        script(&dir.join(name), &format!("#!/bin/sh\n{log}\n{ask}\n"));
    }
    script(
        &dir.join("curl"),
        &format!(
            "#!/bin/sh\n{log}\nexec {:?} \"$@\"\n",
            curl.display().to_string()
        ),
    );
}

fn script(path: &Path, text: &str) {
    std::fs::write(path, text).expect("a fake");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).expect("executable");
}

/// The real `curl`, on the `PATH`.
fn real_curl() -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("a PATH"))
        .map(|dir| dir.join("curl"))
        .find(|path| path.is_file())
        .expect("curl on the PATH")
}

/// A port no process listens on now.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a port")
        .port()
}

/// The status and body `GET path` answers at `port` on loopback, if it
/// answers.
fn get(port: u16, path: &str) -> Option<(u16, String)> {
    let address = ([127, 0, 0, 1], port).into();
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    write!(stream, "GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n").ok()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let status = line.split_whitespace().nth(1)?.parse().ok()?;
    let mut rest = String::new();
    let _ = std::io::Read::read_to_string(&mut reader, &mut rest);
    let body = rest
        .split_once("\r\n\r\n")
        .map_or(rest.clone(), |(_, body)| body.to_string());
    Some((status, body))
}

/// The port of the address at the end of `line`, `host:port`.
fn port_of(line: &str) -> u16 {
    line.trim()
        .rsplit_once(':')
        .and_then(|(_, port)| port.trim_end_matches(',').parse().ok())
        .unwrap_or_else(|| panic!("a port in {line:?}"))
}

/// One step of a fake job: one process.
struct Step {
    id: String,
    name: String,
    process: Running,
    _stderr: Lines,
}

/// One job of the fake Slurm.
struct Job {
    name: String,
    comment: String,
    state: String,
    steps: Vec<Step>,
    /// When it was cancelled: its steps are killed if they outlive [`WAIT`].
    cancelled: Option<Instant>,
}

/// The fake Slurm's jobs, and what it runs them with.
struct Cluster {
    next: u64,
    jobs: BTreeMap<u64, Job>,
    /// Names of jobs that wait in the queue.
    hold: BTreeSet<String>,
    srtctl: PathBuf,
    bin: PathBuf,
    hub: PathBuf,
    /// The `dir` pool directory.
    pools: PathBuf,
    /// What went wrong in a launch, for the test to report.
    failures: Vec<String>,
}

type Shared = Arc<Mutex<Cluster>>;

/// Answer one fake command, `program` run in `pwd` with `args`: its
/// stdout, stderr, and exit code.
fn answer(cluster: &Shared, program: &str, pwd: &str, args: &[String]) -> (String, String, i32) {
    let words: Vec<&str> = args.iter().map(String::as_str).collect();
    let mut held = cluster.lock().expect("the cluster");
    match (program, words.as_slice()) {
        ("srtctl", ["apply", "-f", recipe, "--json"]) => {
            assert_eq!(Path::new(pwd), held.srtctl, "srtctl runs in its checkout");
            drop(held);
            let id = submit(cluster, Path::new(recipe));
            let name = cluster.lock().expect("the cluster").jobs[&id].name.clone();
            (
                format!(
                    "{{\"status\": \"submitted\", \"slurm_job_id\": \"{id}\", \"job_name\": \"{name}\", \"output_dir\": \"/outputs/{id}\"}}\n"
                ),
                String::new(),
                0,
            )
        }
        (
            "squeue",
            [
                "--me",
                "--all",
                "--states=all",
                "--noheader",
                "--format=%i|%T|%j|%k",
            ],
        ) => {
            let mut out = String::new();
            for (id, job) in &held.jobs {
                if matches!(job.state.as_str(), "PENDING" | "RUNNING") {
                    out.push_str(&format!(
                        "{id}|{}|{}|{}\n",
                        job.state, job.name, job.comment
                    ));
                }
            }
            (out, String::new(), 0)
        }
        (
            "squeue",
            [
                jobs,
                "--all",
                "--states=all",
                "--noheader",
                "--format=%i|%T|%j|%k",
            ],
        ) => {
            let id: u64 = jobs
                .strip_prefix("--jobs=")
                .expect("--jobs")
                .parse()
                .expect("an id");
            match held.jobs.get(&id) {
                Some(job) if matches!(job.state.as_str(), "PENDING" | "RUNNING") => (
                    format!("{id}|{}|{}|{}\n", job.state, job.name, job.comment),
                    String::new(),
                    0,
                ),
                _ => (
                    String::new(),
                    "slurm_load_jobs error: Invalid job id specified\n".into(),
                    1,
                ),
            }
        }
        ("squeue", ["--steps", jobs, "--all", "--noheader", "--format=%i|%j"]) => {
            let id: u64 = jobs
                .strip_prefix("--jobs=")
                .expect("--jobs")
                .parse()
                .expect("an id");
            let job = held.jobs.get_mut(&id).expect("a job");
            let mut out = String::new();
            for step in &mut job.steps {
                if step.process.0.try_wait().expect("a state").is_none() {
                    out.push_str(&format!("{}|{}\n", step.id, step.name));
                }
            }
            (out, String::new(), 0)
        }
        ("sacct", [jobs, ..]) => {
            let id: u64 = jobs
                .strip_prefix("--jobs=")
                .expect("--jobs")
                .parse()
                .expect("an id");
            let state = held
                .jobs
                .get(&id)
                .map_or("UNKNOWN", |job| job.state.as_str());
            (format!("{id}|{state}|0:0\n"), String::new(), 0)
        }
        ("scancel", ["--version"]) => ("slurm 24.05.4\n".into(), String::new(), 0),
        ("scancel", ["--state=PENDING", "--ctld", targets @ ..]) => {
            for target in targets {
                let job = held
                    .jobs
                    .get_mut(&target.parse().expect("an id"))
                    .expect("a job");
                if job.state == "PENDING" {
                    job.state = "CANCELLED".into();
                }
            }
            (String::new(), String::new(), 0)
        }
        ("scancel", ["--signal=TERM", targets @ ..]) => {
            for target in targets {
                let (job, step) = match target.split_once('.') {
                    Some((job, _)) => (job, Some(*target)),
                    None => (*target, None),
                };
                let job = held.jobs.get(&job.parse().expect("an id")).expect("a job");
                for each in &job.steps {
                    if step.is_none_or(|step| step == each.id) {
                        terminate(&each.process);
                    }
                }
            }
            (String::new(), String::new(), 0)
        }
        ("scancel", targets) => {
            for target in targets {
                let job = held
                    .jobs
                    .get_mut(&target.parse().expect("an id"))
                    .expect("a job");
                match job.state.as_str() {
                    "PENDING" => job.state = "CANCELLED".into(),
                    _ => {
                        for step in &job.steps {
                            terminate(&step.process);
                        }
                        job.cancelled = Some(Instant::now());
                    }
                }
            }
            (String::new(), String::new(), 0)
        }
        _ => (
            String::new(),
            format!(
                "the fake Slurm does not take `{program} {}`\n",
                args.join(" ")
            ),
            99,
        ),
    }
}

/// Send `process` `SIGTERM`, as Slurm signals a step: one that has exited
/// takes none.
fn terminate(process: &Running) {
    let _ = Command::new("kill")
        .args(["-TERM", &process.0.id().to_string()])
        .stderr(Stdio::null())
        .status();
}

/// Queue the job of `recipe`, and start it unless it is held: its id.
fn submit(cluster: &Shared, recipe: &Path) -> u64 {
    let text = std::fs::read_to_string(recipe).expect("the recipe");
    let recipe: Value = serde_json::from_str(&text).expect("JSON");
    let mut held = cluster.lock().expect("the cluster");
    let id = held.next;
    held.next += 1;
    let name = recipe["name"].as_str().expect("a name").to_string();
    let hold = held.hold.contains(&name);
    held.jobs.insert(
        id,
        Job {
            name,
            comment: recipe["sbatch_directives"]["comment"]
                .as_str()
                .expect("a comment")
                .to_string(),
            state: "PENDING".into(),
            steps: Vec::new(),
            cancelled: None,
        },
    );
    drop(held);
    if !hold {
        let cluster = Arc::clone(cluster);
        std::thread::spawn(move || {
            if let Err(why) = launch(&cluster, id, &recipe) {
                let mut held = cluster.lock().expect("the cluster");
                held.failures.push(format!("job {id}: {why}"));
                if let Some(job) = held.jobs.get_mut(&id) {
                    job.state = "FAILED".into();
                }
            }
        });
    }
    id
}

/// Run job `id` of `recipe` as srt-slurm runs it.
fn launch(cluster: &Shared, id: u64, recipe: &Value) -> Result<(), String> {
    let (bin, hub_bin, pools) = {
        let mut held = cluster.lock().expect("the cluster");
        held.jobs.get_mut(&id).expect("a job").state = "RUNNING".into();
        (held.bin.clone(), held.hub.clone(), held.pools.clone())
    };
    let mounts: BTreeMap<String, PathBuf> = recipe["extra_mount"]
        .as_array()
        .expect("mounts")
        .iter()
        .map(|mount| {
            let mut parts = mount.as_str().expect("a mount").split(':');
            let source = PathBuf::from(parts.next().expect("a source"));
            (parts.next().expect("a target").to_string(), source)
        })
        .collect();
    let comment = recipe["sbatch_directives"]["comment"]
        .as_str()
        .expect("a comment");
    if comment.contains(":-:directory:") {
        // The tables step: the job's own endpoint file, then the fleet's
        // pointer to it, renamed into place.
        let fleet = &mounts["/fleet"];
        let own = format!("directory.{id}.endpoint");
        std::fs::write(fleet.join(&own), "http://127.0.0.1:8000\n").map_err(|e| e.to_string())?;
        let staged = fleet.join(format!("directory.{id}.ptr"));
        std::os::unix::fs::symlink(&own, &staged).map_err(|e| e.to_string())?;
        std::fs::rename(&staged, fleet.join("directory.endpoint")).map_err(|e| e.to_string())?;
        return Ok(());
    }
    let settings = &mounts["/gdl"];
    let (mut public, mut control) = (0, 0);
    let mut index = 0;
    for service in recipe["services"].as_array().expect("services") {
        let name = service["name"].as_str().expect("a name");
        let instances = service["nodes"].as_u64().unwrap_or(1);
        for node in 0..instances {
            let words: Vec<&str> = service["command"]
                .as_array()
                .expect("a command")
                .iter()
                .map(|word| word.as_str().expect("a word"))
                .collect();
            let repository = service["container"]
                .as_str()
                .expect("an image")
                .split_once('#')
                .and_then(|(_, rest)| rest.split_once('@'))
                .map(|(path, _)| path.rsplit('/').next().expect("a name"))
                .expect("a repository");
            let is_hub = name == "hub";
            let mut command = generated(match is_hub {
                true => hub_bin.clone(),
                false => bin.join(repository),
            });
            for word in &words[1..] {
                match word.strip_prefix("/gdl/") {
                    Some(file) => command.arg(settings.join(file)),
                    None => command.arg(word),
                };
            }
            for (key, value) in service["env"].as_object().expect("env") {
                let value = value
                    .as_str()
                    .expect("a value")
                    .replace("{node_ip}", "127.0.0.1")
                    .replace("{head_ip}", "127.0.0.1");
                command.env(key, value);
            }
            for key in [
                "GDL_POOL__RECORDS_TABLE",
                "GDL_POOL__LISTS_TABLE",
                "GDL_POOL__AWS_REGION",
                "GDL_POOL__ENDPOINT",
            ] {
                command.env_remove(key);
            }
            command
                .env("GDL_POOL__DIRECTORY", "dir")
                .env("GDL_POOL__DIR", &pools);
            if is_hub {
                // The ports its record and its address file name: the
                // fake's nodes share one host.
                public = free_port();
                control = free_port();
                command
                    .env("GDL_HUB__PUBLIC_PORT", public.to_string())
                    .env("GDL_HUB__CONTROL_PORT", control.to_string())
                    .env("GDL_POOL__HUB", format!("127.0.0.1:{public}"));
            } else {
                command
                    .env("GDL_HUB__PUBLIC_PORT", public.to_string())
                    .env("GDL_HUB__CONTROL_PORT", control.to_string())
                    .env("GDL_POOL__HUB", format!("127.0.0.1:{public}"))
                    .env("GDL_SYSTEM__PORT", "0")
                    // The api example's `Http` source listens on a fixed
                    // port, which a second fleet on this host would take
                    // too: a test running beside this one, for one.
                    .env("GDL_FRONTEND__LISTEN", format!("127.0.0.1:{}", free_port()));
            }
            let mut process = Running(
                command
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|e| format!("{name}: {e}"))?,
            );
            let stderr = process.stderr();
            let port = match is_hub {
                true => {
                    stderr.find("gdl-hub: serving", WAIT);
                    // What the hub's preamble writes, with the fake's ports.
                    std::fs::write(
                        settings.join(format!("hub.{id}.address")),
                        format!("127.0.0.1:{public} 127.0.0.1:{control}\n"),
                    )
                    .map_err(|e| e.to_string())?;
                    public
                }
                false => port_of(&stderr.find("system listener at", WAIT)),
            };
            let step_name = match instances {
                1 => format!("service_{name}"),
                _ => format!("service_{name}_node{node}"),
            };
            {
                let mut held = cluster.lock().expect("the cluster");
                let job = held.jobs.get_mut(&id).expect("a job");
                if job.cancelled.is_some() {
                    return Ok(());
                }
                job.steps.push(Step {
                    id: format!("{id}.{index}"),
                    name: step_name,
                    process,
                    _stderr: stderr,
                });
            }
            index += 1;
            let path = service["readiness"]["http"]["path"]
                .as_str()
                .expect("a probe");
            let deadline = Instant::now() + WAIT;
            while get(port, path).map(|(status, _)| status) != Some(200) {
                if Instant::now() > deadline {
                    return Err(format!("{name}[{node}] did not pass its probe {path}"));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
    Ok(())
}

/// End jobs as Slurm would: a job ends when its hub exits, its terminal
/// service, and a cancelled job once its steps have exited, or are
/// killed past [`WAIT`].
fn reap(cluster: &Shared) {
    let mut held = cluster.lock().expect("the cluster");
    for job in held.jobs.values_mut() {
        if job.state != "RUNNING" {
            continue;
        }
        let mut alive = 0;
        for step in &mut job.steps {
            if step.process.0.try_wait().expect("a state").is_none() {
                alive += 1;
            }
        }
        let hub_gone = job
            .steps
            .iter_mut()
            .find(|step| step.name == "service_hub")
            .is_some_and(|step| step.process.0.try_wait().expect("a state").is_some());
        match job.cancelled {
            Some(_) if alive == 0 => job.state = "CANCELLED".into(),
            Some(at) if at.elapsed() > WAIT => {
                for step in &mut job.steps {
                    let _ = step.process.0.kill();
                }
            }
            Some(_) => {}
            None if hub_gone => {
                for step in &mut job.steps {
                    let _ = step.process.0.kill();
                }
                job.state = "COMPLETED".into();
            }
            None => {}
        }
    }
}

/// The fake Slurm, answering on a thread of its own until dropped.
struct Fake {
    cluster: Shared,
    state: PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Fake {
    fn start(state: &Path, cluster: Cluster) -> Self {
        for dir in ["requests", "replies"] {
            std::fs::create_dir_all(state.join(dir)).expect("the fake's directory");
        }
        let cluster = Arc::new(Mutex::new(cluster));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (cluster, stop, state) =
                (Arc::clone(&cluster), Arc::clone(&stop), state.to_path_buf());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    serve(&cluster, &state);
                    reap(&cluster);
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        };
        Self {
            cluster,
            state: state.to_path_buf(),
            stop,
            thread: Some(thread),
        }
    }

    /// Every command line run so far, as logged.
    fn calls(&self) -> Vec<String> {
        std::fs::read_to_string(self.state.join("calls"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    /// The jobs Slurm lists, each `id state name`.
    fn listed(&self) -> Vec<String> {
        let held = self.cluster.lock().expect("the cluster");
        held.jobs
            .iter()
            .filter(|(_, job)| matches!(job.state.as_str(), "PENDING" | "RUNNING"))
            .map(|(id, job)| format!("{id} {} {}", job.state, job.name))
            .collect()
    }

    /// What went wrong, for a failed assertion: each launch failure, each
    /// hub's health, and what each process said.
    fn report(&self, fleet_dir: &Path) -> String {
        let held = self.cluster.lock().expect("the cluster");
        let mut report = format!("launch failures: {:?}\n", held.failures);
        for (id, job) in &held.jobs {
            report.push_str(&format!("job {id} {} {}\n", job.state, job.name));
            let pool = job.name.trim_start_matches("gdl-api-");
            if let Ok(address) =
                std::fs::read_to_string(fleet_dir.join(pool).join(format!("hub.{id}.address")))
            {
                let public = port_of(address.split_whitespace().next().unwrap_or(":0"));
                report.push_str(&format!(
                    "  health: {:?}\n",
                    get(public, "/v1/ext/gdl-pool/health")
                ));
            }
            for step in &job.steps {
                let said: Vec<String> =
                    std::iter::from_fn(|| step._stderr.try_next(Duration::from_millis(50)))
                        .collect();
                report.push_str(&format!("  {} {}: {said:#?}\n", step.id, step.name));
            }
        }
        report
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Answer each request the fakes have written.
fn serve(cluster: &Shared, state: &Path) {
    let Ok(entries) = std::fs::read_dir(state.join("requests")) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some() {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let _ = std::fs::remove_file(&path);
        let mut lines = text.lines().map(str::to_string);
        let program = lines.next().unwrap_or_default();
        let pwd = lines.next().unwrap_or_default();
        let args: Vec<String> = lines.collect();
        let (out, err, code) = answer(cluster, &program, &pwd, &args);
        let reply = state.join("replies").join(entry.file_name());
        let with = |suffix: &str| PathBuf::from(format!("{}.{suffix}", reply.display()));
        std::fs::write(with("out"), out).expect("the reply");
        std::fs::write(with("err"), err).expect("the reply");
        std::fs::write(with("tmp"), code.to_string()).expect("the reply");
        std::fs::rename(with("tmp"), with("code")).expect("the reply");
    }
}

/// Run `cargo gdl` with `args`, the fakes first on the `PATH`, bounded:
/// whether it succeeded, and what it said.
fn gdl(fakes: &Path, state: &Path, args: &[&std::ffi::OsStr]) -> (bool, String) {
    let path = std::env::join_paths(std::iter::once(fakes.to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").expect("a PATH")),
    ))
    .expect("a PATH");
    let said = state.join("said");
    let mut child = Running(
        Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .arg("gdl")
            .args(args)
            .env("PATH", path)
            .env("GDL_FAKE_SLURM", state)
            .env("CARGO_NET_OFFLINE", "true")
            .env_remove("RUST_LOG")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&said).expect("a log"))
            .spawn()
            .expect("cargo gdl"),
    );
    let deadline = Instant::now() + Duration::from_secs(90);
    let status = loop {
        if let Some(status) = child.0.try_wait().expect("its state") {
            break Some(status);
        }
        if Instant::now() > deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut text = std::fs::read_to_string(&said).unwrap_or_default();
    if status.is_none() {
        text.push_str("\n(cargo gdl did not exit within 90 s, and was killed)");
    }
    (status.is_some_and(|status| status.success()), text)
}

/// The index of the first call that starts with `prefix`, from `from` on.
fn find(calls: &[String], from: usize, prefix: &str) -> usize {
    calls[from..]
        .iter()
        .position(|call| call.starts_with(prefix))
        .map(|at| at + from)
        .unwrap_or_else(|| panic!("no call `{prefix}` after {from} in {calls:#?}"))
}

/// A fleet's scratch directories, its built programs, and the fake Slurm
/// that runs them.
struct Up {
    fake: Fake,
    bins: PathBuf,
    state: PathBuf,
    spec: PathBuf,
    fleet: PathBuf,
    digests: PathBuf,
    target: PathBuf,
    /// `<run_dir>/api`: the fleet's directory.
    fleet_dir: PathBuf,
    _temp: tempfile::TempDir,
}

impl Up {
    /// Build the fleet's programs and start the fake Slurm: nothing is
    /// submitted yet.
    fn new() -> Self {
        let temp = tempfile::Builder::new()
            .prefix("gdl-srtctl-")
            .tempdir()
            .expect("scratch");
        let dir = temp.path();
        let (srtctl, run_dir, pools, state, bins) = (
            dir.join("srt-slurm"),
            dir.join("runs"),
            dir.join("pools"),
            dir.join("state"),
            dir.join("fakes"),
        );
        for made in [&srtctl, &run_dir, &pools] {
            std::fs::create_dir_all(made).expect("a directory");
        }
        let spec = spec_file(dir);
        let fleet = fleet_file(dir, &srtctl, &run_dir);
        let digests = dir.join("images.json");
        images(&spec, &fleet, &digests);
        let (bin, hub) = build(&spec, dir);
        fakes(&bins, &real_curl());
        let fake = Fake::start(
            &state,
            Cluster {
                next: 1001,
                jobs: BTreeMap::new(),
                hold: BTreeSet::from(["gdl-api-model-x-01-join-engine".to_string()]),
                srtctl,
                bin,
                hub,
                pools,
                failures: Vec::new(),
            },
        );
        Self {
            fake,
            bins,
            state,
            spec,
            fleet,
            digests,
            target: target("examples"),
            fleet_dir: run_dir.join("api"),
            _temp: temp,
        }
    }

    /// `cargo gdl deploy`: whether it succeeded, and what it said.
    fn deploy(&self) -> (bool, String) {
        let deploy: Vec<&std::ffi::OsStr> = vec![
            "deploy".as_ref(),
            "--spec".as_ref(),
            self.spec.as_os_str(),
            "--fleet".as_ref(),
            self.fleet.as_os_str(),
            "--images".as_ref(),
            self.digests.as_os_str(),
            "--local".as_ref(),
            "--target-dir".as_ref(),
            self.target.as_os_str(),
        ];
        gdl(&self.bins, &self.state, &deploy)
    }

    /// `cargo gdl down`: whether it succeeded, and what it said.
    fn down(&self) -> (bool, String) {
        gdl(
            &self.bins,
            &self.state,
            &["down".as_ref(), "--fleet".as_ref(), self.fleet.as_os_str()],
        )
    }
}

#[test]
fn a_fleet_deploys_on_a_slurm_site_and_comes_down_children_first() {
    let up = Up::new();
    let (fake, fleet_dir) = (&up.fake, &up.fleet_dir);

    // The first deploy submits the directory's job, then each pool's, and
    // ends once each pool's hub answers its health.
    let (ok, said) = up.deploy();
    assert!(ok, "{said}\n{}", fake.report(fleet_dir));
    assert!(
        said.contains("wait: every pool's health answers 200"),
        "{said}"
    );
    let calls = fake.calls();
    let submitted: Vec<&String> = calls
        .iter()
        .filter(|call| call.starts_with("srtctl"))
        .collect();
    assert_eq!(
        submitted,
        [
            &format!(
                "srtctl apply -f {} --json",
                fleet_dir.join("directory.yaml").display()
            ),
            &format!(
                "srtctl apply -f {} --json",
                fleet_dir.join("global-us/pool.yaml").display()
            ),
            &format!(
                "srtctl apply -f {} --json",
                fleet_dir.join("model-x-01/pool.yaml").display()
            ),
        ]
    );
    assert_eq!(
        fake.listed(),
        [
            "1001 RUNNING gdl-api-directory",
            "1002 RUNNING gdl-api-global-us",
            "1003 RUNNING gdl-api-model-x-01"
        ]
    );
    for (pool, job) in [("global-us", 1002), ("model-x-01", 1003)] {
        let address =
            std::fs::read_to_string(fleet_dir.join(pool).join(format!("hub.{job}.address")))
                .expect("the hub's address");
        let public = port_of(
            address
                .split_whitespace()
                .next()
                .expect("its public address"),
        );
        assert_eq!(
            get(public, "/v1/ext/gdl-pool/health").map(|(status, _)| status),
            Some(200),
            "{pool}"
        );
    }

    // A second deploy of the same fleet submits nothing.
    let (ok, said) = up.deploy();
    assert!(ok, "{said}");
    let again = fake.calls();
    assert_eq!(
        again
            .iter()
            .filter(|call| call.starts_with("srtctl"))
            .count(),
        3,
        "{again:#?}"
    );

    // A job waits to join the pool under the global one.
    let waiting = submit(
        &fake.cluster,
        &fleet_dir.join("model-x-01/join-engine.yaml"),
    );

    // `down` stops the child pool, then the global pool, then the
    // directory's job.
    let from = fake.calls().len();
    let (ok, said) = up.down();
    assert!(ok, "{said}\n{:#?}", fake.calls());
    let calls = fake.calls();
    let cancel_waiting = find(
        &calls,
        from,
        &format!("scancel --state=PENDING --ctld {waiting}"),
    );
    let drain = find(&calls, cancel_waiting + 1, "curl ");
    assert!(
        calls[drain].contains("--request POST") && calls[drain].ends_with("/v1/ext/gdl-pool/drain"),
        "{}",
        calls[drain]
    );
    let withdrawn = find(&calls, drain + 1, "curl ");
    assert!(
        calls[withdrawn].ends_with("/v1/ext/gdl-pool/withdrawn"),
        "{}",
        calls[withdrawn]
    );
    let cancel_child = find(&calls, withdrawn + 1, "scancel 1003");
    let steps = find(&calls, cancel_child + 1, "squeue --steps --jobs=1002");
    let terminate = find(&calls, steps + 1, "scancel --signal=TERM");
    assert_eq!(
        calls[terminate], "scancel --signal=TERM 1002.1",
        "only the global pool's segment"
    );
    let instances = find(&calls, terminate + 1, "curl ");
    assert!(
        calls[instances].ends_with("/v1/ext/gdl-pool/instances"),
        "{}",
        calls[instances]
    );
    let cancel_global = find(&calls, instances + 1, "scancel 1002");
    find(&calls, cancel_global + 1, "scancel 1001");
    assert!(
        said.contains("drain: pool `model-x-01` has withdrawn"),
        "{said}"
    );

    // Nothing of the fleet runs: every job ended, and every process
    // exited.
    assert_eq!(fake.listed(), Vec::<String>::new());
    let mut held = fake.cluster.lock().expect("the cluster");
    for (id, job) in held.jobs.iter_mut() {
        for step in &mut job.steps {
            let status = step.process.exits_within(WAIT);
            // The segments and hubs left on their signal, not by a kill.
            assert!(
                status.code().is_some(),
                "job {id} step {}: {status:?}",
                step.name
            );
        }
    }
    assert!(held.failures.is_empty(), "{:?}", held.failures);
}

/// The child pool's job is lost with its node: its hub and segment are
/// killed, and Slurm no longer lists the job, while its record is still in
/// the directory. `down` withdraws the record once, on the global pool's
/// real hub, which writes the withdrawal in the directory and answers 204;
/// then the global pool and the directory's job go as before.
#[test]
fn down_withdraws_a_pool_whose_job_is_gone_through_its_parents_hub() {
    let up = Up::new();
    let (ok, said) = up.deploy();
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    {
        let mut held = up.fake.cluster.lock().expect("the cluster");
        let job = held.jobs.get_mut(&1003).expect("the child's job");
        for step in &mut job.steps {
            let _ = step.process.0.kill();
            let _ = step.process.exits_within(WAIT);
        }
        job.state = "NODE_FAIL".into();
    }

    let from = up.fake.calls().len();
    let (ok, said) = up.down();
    let calls = up.fake.calls();
    assert!(ok, "{said}\n{calls:#?}");
    let withdraw = find(&calls, from, "curl ");
    assert!(
        calls[withdraw].contains("--request POST")
            && calls[withdraw].ends_with("/v1/ext/gdl-pool/children/model_x/model-x-01/withdraw"),
        "{}",
        calls[withdraw]
    );
    assert_eq!(
        calls[from..]
            .iter()
            .filter(|call| call.ends_with("/withdraw"))
            .count(),
        1,
        "{calls:#?}"
    );
    let terminate = find(&calls, withdraw + 1, "scancel --signal=TERM");
    assert_eq!(calls[terminate], "scancel --signal=TERM 1002.1");
    find(&calls, terminate + 1, "scancel 1002");
    assert!(
        !calls[from..].iter().any(|call| call.contains("1003")),
        "{calls:#?}"
    );
    assert!(
        said.contains(
            "withdraw: pool `model-x-01`'s job is gone; parent `global-us`'s hub wrote its withdrawal in the DynamoDB directory"
        ),
        "{said}"
    );
    assert_eq!(up.fake.listed(), Vec::<String>::new());
    let held = up.fake.cluster.lock().expect("the cluster");
    assert!(held.failures.is_empty(), "{:?}", held.failures);
}
