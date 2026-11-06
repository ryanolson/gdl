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

/// A global pool and a pool under it on Slurm site `lab`, with `site`'s
/// keys on the site and `model`'s on the pool under it.
fn fleet_file(dir: &Path, srtctl: &Path, run_dir: &Path, site: &str, model: &str) -> PathBuf {
    let file = dir.join("fleet.toml");
    std::fs::write(
        &file,
        format!(
            "[fleet]\nname = \"api\"\n\n\
             [cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = {:?}\nrun_dir = {:?}\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n{site}\n\
             [directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n\n\
             [class.global]\nready = {{ front = 1 }}\n[class.model_x]\nready = {{ engine = 1 }}\n\n\
             [pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n\n\
             [pool.model-x-01]\nclass = \"model_x\"\ncluster = \"lab\"\nparents = \"global\"\n{model}\n",
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
    for name in ["srtctl", "squeue", "scancel", "sacct", "srun"] {
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
    /// The recipe's service it runs, or the joiner it is.
    service: String,
    /// The fake node it runs on.
    node: String,
    process: Running,
    _stderr: Lines,
    /// Its system listener's port, for a segment.
    system: Option<u16>,
}

/// One job of the fake Slurm.
struct Job {
    name: String,
    comment: String,
    state: String,
    steps: Vec<Step>,
    /// When it was cancelled: its steps are killed if they outlive [`WAIT`].
    cancelled: Option<Instant>,
    /// Its terminal services: the job ends when one of them exits.
    terminal: BTreeSet<String>,
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
    /// Where each job's logs go, `<logs>/<id>`, which srt-slurm mounts at
    /// `/logs`.
    logs: PathBuf,
    /// What went wrong in a launch, for the test to report.
    failures: Vec<String>,
    /// The port of the last front's HTTP listener: the fake's nodes share
    /// this host, so no front keeps the example's fixed port.
    front: Option<u16>,
    /// Each recipe submitted, as it was when submitted.
    recipes: Vec<Value>,
    /// Variables every process of a pool's job gets after the recipe's own,
    /// as a pool's environment would add them.
    env: Vec<(String, String)>,
}

/// When every pool's job ends, as the fake `squeue` says: its end, and the
/// time it has left.
const END: &str = "2099-01-01T00:00:00|7:59:59";

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
        ("squeue", ["--steps", jobs, "--all", "--noheader", "--format=%i|%j|%N"]) => {
            let id: u64 = jobs
                .strip_prefix("--jobs=")
                .expect("--jobs")
                .parse()
                .expect("an id");
            let job = held.jobs.get_mut(&id).expect("a job");
            let mut out = String::new();
            for step in &mut job.steps {
                if step.process.0.try_wait().expect("a state").is_none() {
                    out.push_str(&format!("{}|{}|{}\n", step.id, step.name, step.node));
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
                "--format=%e|%L",
            ],
        ) => {
            let id: u64 = jobs
                .strip_prefix("--jobs=")
                .expect("--jobs")
                .parse()
                .expect("an id");
            assert_eq!(
                held.jobs[&id].state, "RUNNING",
                "the end of a job that runs"
            );
            (format!("{END}\n"), String::new(), 0)
        }
        // Every fake node has one address, on loopback.
        ("srun", [.., "ip", "-4", "-o", "addr", "show", "dev", interface]) => (
            format!(
                "1: {interface}    inet 127.0.0.1/8 scope host {interface}\\       valid_lft forever preferred_lft forever\n"
            ),
            String::new(),
            0,
        ),
        ("srun", [.., "bash", "-c", script]) => {
            let flag = |name: &str| {
                words
                    .iter()
                    .find_map(|word| word.strip_prefix(name))
                    .unwrap_or_else(|| panic!("{name} in {words:?}"))
                    .to_string()
            };
            let id: u64 = flag("--jobid=").parse().expect("an id");
            let (name, node) = (flag("--job-name="), flag("--nodelist="));
            let image = flag("--container-image=");
            let script = script.to_string();
            drop(held);
            if let Err(why) = step(cluster, id, &name, &node, &image, &script) {
                cluster
                    .lock()
                    .expect("the cluster")
                    .failures
                    .push(format!("step {name} of job {id}: {why}"));
            }
            // A real `srun` lives as long as its step; this one hands it
            // to the fake and ends.
            (String::new(), String::new(), 0)
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
    let terminal = recipe["services"]
        .as_array()
        .expect("services")
        .iter()
        .filter(|service| service["terminal"] == true)
        .map(|service| service["name"].as_str().expect("a name").to_string())
        .collect();
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
            terminal,
        },
    );
    held.recipes.push(recipe.clone());
    drop(held);
    if !hold {
        let cluster = Arc::clone(cluster);
        std::thread::spawn(move || {
            if let Err(why) = launch(&cluster, id, &recipe) {
                let mut held = cluster.lock().expect("the cluster");
                held.failures.push(format!("job {id}: {why}"));
                if let Some(job) = held.jobs.get_mut(&id) {
                    job.state = "FAILED".into();
                    for step in &mut job.steps {
                        let _ = step.process.0.kill();
                    }
                }
            }
        });
    }
    id
}

/// The port of the control listener of the hub that wrote its address in
/// `dir`, a pool's directory: where the fake's joiners find it, since its
/// ports are not the defaults that a joiner keeps.
fn hub_control(dir: &Path) -> Option<u16> {
    std::fs::read_dir(dir).ok()?.flatten().find_map(|entry| {
        let name = entry.file_name().into_string().ok()?;
        (name.starts_with("hub.") && name.ends_with(".address")).then_some(())?;
        let text = std::fs::read_to_string(entry.path()).ok()?;
        Some(port_of(text.split_whitespace().nth(1)?))
    })
}

/// Run job `id` of `recipe` as srt-slurm runs it.
fn launch(cluster: &Shared, id: u64, recipe: &Value) -> Result<(), String> {
    let (bin, hub_bin, pools, logs, pool_env) = {
        let mut held = cluster.lock().expect("the cluster");
        held.jobs.get_mut(&id).expect("a job").state = "RUNNING".into();
        let logs = held.logs.join(id.to_string());
        let env = held.env.clone();
        (
            held.bin.clone(),
            held.hub.clone(),
            held.pools.clone(),
            logs,
            env,
        )
    };
    std::fs::create_dir_all(&logs).map_err(|e| e.to_string())?;
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
    let joining = comment.contains(":join:");
    let (mut public, mut control) = (0, 0);
    let mut index = 0;
    for service in recipe["services"].as_array().expect("services") {
        let name = service["name"].as_str().expect("a name");
        let instances = service["nodes"].as_u64().unwrap_or(1);
        let words: Vec<&str> = service["command"]
            .as_array()
            .expect("a command")
            .iter()
            .map(|word| word.as_str().expect("a word"))
            .collect();
        if name == "resolve" {
            // `gdl-hub --resolve <class>/<pool> --write <file>`, as its
            // script runs it, then a wait the fake does not need: the file
            // is what the segment reads, and its probe.
            let script = words.last().expect("a script");
            let pool = script
                .split_whitespace()
                .skip_while(|word| *word != "--resolve")
                .nth(1)
                .expect("a pool");
            let resolved = generated(&hub_bin)
                .args(["--resolve", pool, "--write"])
                .arg(logs.join("gdl-hub.toml"))
                .env("GDL_POOL__DIRECTORY", "dir")
                .env("GDL_POOL__DIR", &pools)
                .stdin(Stdio::null())
                .output()
                .map_err(|e| format!("resolve: {e}"))?;
            if !resolved.status.success() {
                return Err(format!(
                    "resolve: {}",
                    String::from_utf8_lossy(&resolved.stderr).trim()
                ));
            }
            continue;
        }
        for node in 0..instances {
            let node_name = match name {
                "spare" => format!("spare-{id}-{node}"),
                _ => format!("node-{id}-{index}"),
            };
            let step_name = match instances {
                1 => format!("service_{name}"),
                _ => format!("service_{name}_{node_name}"),
            };
            if name == "spare" {
                // A placeholder that holds its node for joiners.
                let mut process = Running(
                    Command::new("sleep")
                        .arg("3600")
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::piped())
                        .spawn()
                        .map_err(|e| format!("{name}: {e}"))?,
                );
                let stderr = process.stderr();
                let mut held = cluster.lock().expect("the cluster");
                let job = held.jobs.get_mut(&id).expect("a job");
                job.steps.push(Step {
                    id: format!("{id}.{index}"),
                    name: step_name,
                    service: name.to_string(),
                    node: node_name,
                    process,
                    _stderr: stderr,
                    system: None,
                });
                index += 1;
                continue;
            }
            let repository = repository(service["container"].as_str().expect("an image"));
            let is_hub = name == "hub";
            let mut command = generated(match is_hub {
                true => hub_bin.clone(),
                false => bin.join(repository),
            });
            for word in &words[1..] {
                match (word.strip_prefix("/gdl/"), word.strip_prefix("/logs/")) {
                    (Some(file), _) => command.arg(settings.join(file)),
                    (_, Some(file)) => command.arg(logs.join(file)),
                    _ => command.arg(word),
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
                .env("GDL_POOL__DIR", &pools)
                .envs(pool_env.iter().cloned());
            if is_hub {
                // The ports its record and its address file name: the
                // fake's nodes share one host.
                public = free_port();
                control = free_port();
                command
                    .env("GDL_HUB__PUBLIC_PORT", public.to_string())
                    .env("GDL_HUB__CONTROL_PORT", control.to_string())
                    .env("GDL_POOL__HUB", format!("127.0.0.1:{public}"));
            } else if joining {
                // Its hub's host and public port come from the file
                // `resolve` wrote, as on a cluster; its control port is
                // the default on a cluster, and the fake hub's own here.
                let control = hub_control(settings).ok_or("no hub wrote its address")?;
                command
                    .env("GDL_HUB__CONTROL_PORT", control.to_string())
                    .env("GDL_SYSTEM__PORT", "0");
            } else {
                command
                    .env("GDL_HUB__PUBLIC_PORT", public.to_string())
                    .env("GDL_HUB__CONTROL_PORT", control.to_string())
                    .env("GDL_POOL__HUB", format!("127.0.0.1:{public}"))
                    .env("GDL_SYSTEM__PORT", "0");
            }
            if name == "front" {
                // The api example's `Http` source listens on a fixed
                // port, which a second fleet on this host would take
                // too: a test running beside this one, for one. The test
                // sends its requests to the last front's.
                let port = free_port();
                command.env("GDL_FRONTEND__LISTEN", format!("127.0.0.1:{port}"));
                cluster.lock().expect("the cluster").front = Some(port);
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
            {
                let mut held = cluster.lock().expect("the cluster");
                let job = held.jobs.get_mut(&id).expect("a job");
                if job.cancelled.is_some() {
                    return Ok(());
                }
                job.steps.push(Step {
                    id: format!("{id}.{index}"),
                    name: step_name,
                    service: name.to_string(),
                    node: node_name,
                    process,
                    _stderr: stderr,
                    system: (!is_hub).then_some(port),
                });
            }
            index += 1;
            probe(port, service, name, node)?;
        }
    }
    Ok(())
}

/// The repository of `image`, as Pyxis names it: the binary the fake runs
/// for it.
fn repository(image: &str) -> &str {
    image
        .split_once('#')
        .and_then(|(_, rest)| rest.split_once('@'))
        .map(|(path, _)| path.rsplit('/').next().expect("a name"))
        .expect("a repository")
}

/// Wait until the instance of `service` on node `node`, whose listener is
/// at `port`, passes the service's readiness probe.
fn probe(
    port: u16,
    service: &Value,
    name: &str,
    node: impl std::fmt::Display,
) -> Result<(), String> {
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
    Ok(())
}

/// Start step `name` of job `id` on node `node`, which runs `script` as
/// `cargo gdl join --in-job` writes it: its variables, its preamble, then
/// its binary and its settings. The fake takes the variables and the
/// binary, and runs the binary in the directory of the pool's settings,
/// with the `dir` directory in place of the preamble's store.
fn step(
    cluster: &Shared,
    id: u64,
    name: &str,
    node: &str,
    image: &str,
    script: &str,
) -> Result<(), String> {
    let (bin, pools, settings) = {
        let held = cluster.lock().expect("the cluster");
        let job = &held
            .recipes
            .iter()
            .find(|recipe| recipe["name"] == held.jobs[&id].name.as_str());
        let settings = job
            .and_then(|recipe| recipe["extra_mount"].as_array())
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .find_map(|mount| mount.strip_suffix(":/gdl"))
            .map(PathBuf::from)
            .ok_or("the pool's settings")?;
        (held.bin.clone(), held.pools.clone(), settings)
    };
    let unquote = |word: &str| -> String {
        word.strip_prefix('\'')
            .and_then(|word| word.strip_suffix('\''))
            .unwrap_or(word)
            .replace("'\\''", "'")
    };
    let mut env = Vec::new();
    let mut words = Vec::new();
    for part in script.split(" && ") {
        if let Some((key, value)) = part
            .strip_prefix("export ")
            .and_then(|it| it.split_once('='))
        {
            env.push((key.to_string(), unquote(value)));
        } else if let Some(exec) = part.strip_prefix("exec ") {
            words = exec.split(' ').map(unquote).collect();
        }
    }
    let [binary, file] = words.as_slice() else {
        return Err(format!("no binary and settings in {script}"));
    };
    assert_eq!(
        binary.as_str(),
        "/usr/local/bin/segment",
        "the image's binary"
    );
    let repository = repository(image);
    let mut command = generated(bin.join(repository));
    command.arg(settings.join(file.strip_prefix("/gdl/").expect("a settings file")));
    command.envs(env);
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
        .env("GDL_POOL__DIR", &pools)
        .env("GDL_SYSTEM__PORT", "0");
    let mut process = Running(
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("{name}: {e}"))?,
    );
    let stderr = process.stderr();
    let port = port_of(&stderr.find("system listener at", WAIT));
    let mut held = cluster.lock().expect("the cluster");
    let job = held.jobs.get_mut(&id).expect("a job");
    let index = job.steps.len();
    job.steps.push(Step {
        id: format!("{id}.{index}"),
        name: name.to_string(),
        service: name.to_string(),
        node: node.to_string(),
        process,
        _stderr: stderr,
        system: Some(port),
    });
    Ok(())
}

/// End jobs as Slurm would: a job ends once every instance of its terminal
/// services has exited, as a pool's job does when its hub exits and a
/// joining job when its segments do, `COMPLETED` when each exited 0; and a cancelled job once its steps have exited, or
/// are killed past [`WAIT`].
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
        // srt-slurm ends the job once every instance of its terminal
        // services has exited, with the worst exit code.
        let terminal = &job.terminal;
        let exits: Vec<Option<std::process::ExitStatus>> = job
            .steps
            .iter_mut()
            .filter(|step| terminal.contains(&step.service))
            .map(|step| step.process.0.try_wait().expect("a state"))
            .collect();
        let ended = !exits.is_empty() && exits.iter().all(Option::is_some);
        let clean = exits
            .iter()
            .flatten()
            .all(std::process::ExitStatus::success);
        match job.cancelled {
            Some(_) if alive == 0 => job.state = "CANCELLED".into(),
            Some(at) if at.elapsed() > WAIT => {
                for step in &mut job.steps {
                    let _ = step.process.0.kill();
                }
            }
            Some(_) => {}
            None if ended => {
                // The others get srt-slurm's teardown: a signal, which the
                // fake makes a kill.
                for step in &mut job.steps {
                    let _ = step.process.0.kill();
                }
                job.state = match clean {
                    true => "COMPLETED".into(),
                    false => "FAILED".into(),
                };
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

/// A Slurm site under the fake, with the api example built and its images'
/// digests written: what each test deploys and drives.
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
        Self::with("", "", &["gdl-api-model-x-01-join-engine"])
    }

    /// As [`Up::new`], with `site`'s keys on the fleet's site and `model`'s
    /// on its model pool, under a fake that holds jobs named in `hold` in
    /// the queue.
    fn with(site: &str, model: &str, hold: &[&str]) -> Self {
        let temp = tempfile::Builder::new()
            .prefix("gdl-srtctl-")
            .tempdir()
            .expect("scratch");
        let dir = temp.path();
        let (srtctl, run_dir, pools, state, bins, logs) = (
            dir.join("srt-slurm"),
            dir.join("runs"),
            dir.join("pools"),
            dir.join("state"),
            dir.join("fakes"),
            dir.join("logs"),
        );
        for made in [&srtctl, &run_dir, &pools, &logs] {
            std::fs::create_dir_all(made).expect("a directory");
        }
        let spec = spec_file(dir);
        let fleet = fleet_file(dir, &srtctl, &run_dir, site, model);
        let digests = dir.join("images.json");
        images(&spec, &fleet, &digests);
        let (bin, hub) = build(&spec, dir);
        fakes(&bins, &real_curl());
        let fake = Fake::start(
            &state,
            Cluster {
                next: 1001,
                jobs: BTreeMap::new(),
                hold: hold.iter().map(|name| name.to_string()).collect(),
                srtctl,
                bin,
                hub,
                pools,
                logs,
                failures: Vec::new(),
                front: None,
                recipes: Vec::new(),
                env: Vec::new(),
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

    /// `cargo gdl` with `args`: whether it succeeded, and what it said.
    fn gdl(&self, args: &[&str]) -> (bool, String) {
        let args: Vec<&std::ffi::OsStr> = args.iter().map(std::ffi::OsStr::new).collect();
        gdl(&self.bins, &self.state, &args)
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
/// the directory. `down` withdraws it on the global pool's real hub, which
/// writes the withdrawal in the directory and answers 204; then the global
/// pool and the directory's job go as before.
///
/// The hub may still hear the child when `down` asks: the hubs run the
/// default lease, 30 s, with a heartbeat every 10 s, and no fleet key
/// shortens them. Then it refuses the first withdrawal with `409`, and
/// `down` waits out the lease and asks once more. On a slow host the lease
/// may have passed before `down` asks, and the first withdrawal is
/// stored. Either path must end withdrawn, before the global pool stops;
/// the refusal itself is proven on a clock the tests control
/// (`gdl-hub/tests/operator.rs`, `deployment/deploy/slurm/gone.rs`).
/// Slow by design: the wait for the lease is real time.
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
    let withdrawals = calls[from..]
        .iter()
        .filter(|call| call.ends_with("/withdraw"))
        .count();
    let waited =
        said.contains("withdraw: parent `global-us`'s hub still hears pool `model-x-01`; waiting");
    // Refused while the lease was fresh, then asked once more; or asked
    // once, after the lease had passed.
    let stored = if waited {
        assert_eq!(withdrawals, 2, "{said}\n{calls:#?}");
        let retried = find(&calls, withdraw + 1, "curl ");
        assert!(
            calls[retried].ends_with("/model-x-01/withdraw"),
            "{calls:#?}"
        );
        retried
    } else {
        assert_eq!(withdrawals, 1, "{said}\n{calls:#?}");
        withdraw
    };
    let terminate = find(&calls, stored + 1, "scancel --signal=TERM");
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

/// The hub's instance lease in [`a_pool_stays_up_when_one_of_its_segments_dies`]:
/// short, so that the hub drops a dead instance in a few seconds, not the
/// default 30 s and its 10 s prune.
const LEASE: Duration = Duration::from_millis(3000);
const PRUNE: Duration = Duration::from_millis(250);

/// One segment instance of a running pool dies, as a node that Slurm
/// takes out of a job under `--no-kill` takes it: SIGKILL, with no stop
/// and no leave. The pool's hub drops it once its lease lapses, the pool's
/// health says `degraded` or `unavailable`, and the hub and the job stay
/// up. The hub's lease is short ([`LEASE`]), set on every process of the
/// pool's job, so the wait is bounded by it.
#[test]
fn a_pool_stays_up_when_one_of_its_segments_dies() {
    let up = Up::new();
    // The hub and its segments read the same variables: the segments
    // check their heartbeat, a tenth of the lease, against it.
    up.fake.cluster.lock().expect("the cluster").env = [
        ("GDL_HUB__INSTANCE_LEASE_MS", LEASE.as_millis()),
        ("GDL_HUB__PRUNE_MS", PRUNE.as_millis()),
        ("GDL_HUB__HEARTBEAT_MS", LEASE.as_millis() / 10),
    ]
    .map(|(name, millis)| (name.to_string(), millis.to_string()))
    .into();
    let (ok, said) = up.deploy();
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    let model = up.fleet_dir.join("model-x-01");
    until_listed(&model, "engine", 1);

    let killed = {
        let mut held = up.fake.cluster.lock().expect("the cluster");
        let job = held.jobs.get_mut(&1003).expect("the model pool's job");
        let engine = job
            .steps
            .iter_mut()
            .find(|step| step.service == "engine")
            .expect("its engine");
        engine.process.0.kill().expect("SIGKILL");
        let status = engine.process.exits_within(WAIT);
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(9)
        );
        Instant::now()
    };

    // The lease and one prune, with room for a busy host.
    let bound = LEASE + PRUNE + Duration::from_secs(5);
    while listed(&model, "engine") != Some(0) {
        assert!(
            killed.elapsed() < bound,
            "the hub lists {:?} engines {bound:?} after one was killed\n{}",
            listed(&model, "engine"),
            up.fake.report(&up.fleet_dir)
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let address = std::fs::read_to_string(model.join("hub.1003.address")).expect("its address");
    let public = port_of(
        address
            .split_whitespace()
            .next()
            .expect("its public address"),
    );
    let deadline = Instant::now() + WAIT;
    loop {
        let health = get(public, "/v1/ext/gdl-pool/health");
        let state = health
            .as_ref()
            .and_then(|(_, body)| serde_json::from_str::<Value>(body).ok())
            .and_then(|body| body["state"].as_str().map(str::to_string));
        if let (Some((503, _)), Some("degraded" | "unavailable")) = (&health, state.as_deref()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the pool's health: {health:?}\n{}",
            up.fake.report(&up.fleet_dir)
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // The job runs on: Slurm lists it, and its hub has not exited.
    let squeue = Command::new(up.bins.join("squeue"))
        .args([
            "--jobs=1003",
            "--all",
            "--states=all",
            "--noheader",
            "--format=%i|%T|%j|%k",
        ])
        .env("GDL_FAKE_SLURM", &up.state)
        .stdin(Stdio::null())
        .output()
        .expect("squeue");
    let listed = String::from_utf8_lossy(&squeue.stdout);
    assert!(
        listed.starts_with("1003|RUNNING|gdl-api-model-x-01|"),
        "{listed}"
    );
    let mut held = up.fake.cluster.lock().expect("the cluster");
    let hub = held
        .jobs
        .get_mut(&1003)
        .expect("the job")
        .steps
        .iter_mut()
        .find(|step| step.service == "hub")
        .expect("its hub")
        .process
        .0
        .try_wait()
        .expect("a state");
    assert_eq!(hub, None, "the hub exited");
    assert!(held.failures.is_empty(), "{:?}", held.failures);
}

/// One streamed chat request of `words` words to the front's HTTP listener
/// at `port`: `Err` with why unless it is answered 200, whole, with no
/// error.
fn chat(port: u16, words: usize) -> Result<(), String> {
    let prompt: Vec<String> = (0..words).map(|word| format!("w{word}")).collect();
    let body = format!("{{\"messages\":[\"{}\"]}}", prompt.join(" "));
    let address = ([127, 0, 0, 1], port).into();
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    write!(
        stream,
        "POST /v1/chat/completions HTTP/1.0\r\nHost: 127.0.0.1\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
    .map_err(|e| format!("send: {e}"))?;
    let mut answer = String::new();
    std::io::Read::read_to_string(&mut stream, &mut answer).map_err(|e| format!("read: {e}"))?;
    let status = answer.split_whitespace().nth(1).unwrap_or_default();
    let deltas = answer.matches("\"delta\"").count();
    match (status, deltas, answer.contains("\"error\"")) {
        ("200", n, false) if n == words => Ok(()),
        _ => Err(answer),
    }
}

/// Requests sent from threads of their own until stopped: how many were
/// answered, and why each other one was not.
struct Load {
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<(usize, Vec<String>)>>,
}

impl Load {
    fn start(port: u16, workers: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let workers = (0..workers)
            .map(|_| {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let (mut answered, mut failed) = (0, Vec::new());
                    while !stop.load(Ordering::Relaxed) {
                        match chat(port, 4) {
                            Ok(()) => answered += 1,
                            Err(why) => failed.push(why),
                        }
                    }
                    (answered, failed)
                })
            })
            .collect();
        Self { stop, workers }
    }

    fn stop(self) -> (usize, Vec<String>) {
        self.stop.store(true, Ordering::Relaxed);
        let (mut answered, mut failed) = (0, Vec::new());
        for worker in self.workers {
            let (count, why) = worker.join().expect("a worker");
            answered += count;
            failed.extend(why);
        }
        (answered, failed)
    }
}

/// How many instances of `segment` the hub of the pool whose directory is
/// `dir` lists, at the control address it wrote.
fn listed(dir: &Path, segment: &str) -> Option<usize> {
    let address = std::fs::read_dir(dir).ok()?.flatten().find_map(|entry| {
        let name = entry.file_name().into_string().ok()?;
        (name.starts_with("hub.") && name.ends_with(".address")).then_some(())?;
        std::fs::read_to_string(entry.path()).ok()
    })?;
    let control = port_of(address.split_whitespace().nth(1)?);
    let (status, body) = get(control, "/v1/ext/gdl-pool/instances")?;
    (status == 200).then_some(())?;
    let body: Value = serde_json::from_str(&body).ok()?;
    Some(
        body["instances"]
            .as_array()?
            .iter()
            .filter(|instance| instance["segment"] == segment)
            .count(),
    )
}

/// Wait, bounded by [`WAIT`], until the pool's hub in `dir` lists `count`
/// instances of `segment`.
fn until_listed(dir: &Path, segment: &str, count: usize) {
    let deadline = Instant::now() + WAIT;
    loop {
        let now = listed(dir, segment);
        if now == Some(count) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the hub lists {now:?} instances of `{segment}`, not {count}, after {WAIT:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// How many requests the engine whose system listener is at `port` has
/// taken, by its metrics.
fn served(port: u16) -> u64 {
    let (status, scrape) = get(port, "/metrics").expect("its metrics");
    assert_eq!(status, 200, "{scrape}");
    scrape
        .lines()
        .filter(|line| {
            line.starts_with("dynamo_component_requests_total{")
                && line.contains("dynamo_endpoint=\"Engine\"")
        })
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum::<f64>() as u64
}

/// The system ports of job `id`'s steps of `service`, which still run.
fn systems(fake: &Fake, id: u64, service: &str) -> Vec<u16> {
    let mut held = fake.cluster.lock().expect("the cluster");
    let job = held.jobs.get_mut(&id).expect("a job");
    job.steps
        .iter_mut()
        .filter(|step| step.service == service || step.service.starts_with(&format!("{service}+")))
        .filter_map(|step| {
            let running = step.process.0.try_wait().expect("a state").is_none();
            step.system.filter(|_| running)
        })
        .collect()
}

/// The id after `prefix` in what `cargo gdl` said.
fn said_id(said: &str, prefix: &str) -> String {
    said.lines()
        .find_map(|line| line.split_once(prefix))
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("`{prefix}` in {said}"))
        .to_string()
}

/// The first check of growing a pool on Slurm: a new backend joins the
/// running pool's working set by a job of its own, serves requests, and
/// leaves on one signal with no failed request; a joiner in the pool's
/// job is refused; and a joiner queued after its pool has gone fails at
/// once. Real hubs and
/// segments run under the fake Slurm. One test, since each deploy of real
/// processes costs seconds: it runs for about a minute.
#[test]
fn a_new_backend_joins_a_running_pool_and_leaves_without_a_failed_request() {
    let up = Up::with(
        "network_interface = \"ib0\"",
        "slurm = { spare_nodes = 1 }",
        &[],
    );
    let fleet = up.fleet.to_str().expect("a path");
    let (ok, said) = up.deploy();
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    let front = up
        .fake
        .cluster
        .lock()
        .expect("the cluster")
        .front
        .expect("a front");
    let model = up.fleet_dir.join("model-x-01");
    until_listed(&model, "engine", 1);
    // The front serves once it has followed the model pool, which its
    // pool's health need not wait for.
    let deadline = Instant::now() + WAIT;
    while let Err(why) = chat(front, 4) {
        assert!(
            Instant::now() < deadline,
            "the front at {front} answers no request: {why}\n{}",
            up.fake.report(&up.fleet_dir)
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // A joining job of two engines, which ends by the pool's job's end.
    let join = [
        "join",
        "--fleet",
        fleet,
        "--pool",
        "model-x-01",
        "--segment",
        "engine",
    ];
    let (ok, said) = up.gdl(&[&join[..], &["--nodes", "2"]].concat());
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    let job = said_id(&said, "join: job ");
    let recipe = up
        .fake
        .cluster
        .lock()
        .expect("the cluster")
        .recipes
        .last()
        .cloned()
        .expect("the joiner's recipe");
    assert_eq!(recipe["name"], "gdl-api-model-x-01-join-engine");
    let (end, _) = END.split_once('|').expect("an end");
    assert_eq!(recipe["sbatch_directives"]["deadline"], end);
    assert_eq!(recipe["sbatch_directives"]["time-min"], "00:25:00");
    until_listed(&model, "engine", 3);
    let id: u64 = job.parse().expect("a job id");
    let joiners = systems(&up.fake, id, "engine");
    assert_eq!(joiners.len(), 2);
    let load = Load::start(front, 8);
    std::thread::sleep(Duration::from_secs(2));
    let (answered, failed) = load.stop();
    assert!(failed.is_empty(), "{failed:#?}");
    assert!(answered > 0);
    for port in &joiners {
        assert!(served(*port) > 0, "joiner at {port} served no request");
    }

    // It leaves under load, and its job completes.
    let load = Load::start(front, 4);
    let (ok, said) = up.gdl(&["leave", "--fleet", fleet, "--job", &job]);
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    assert!(
        said.contains(&format!("leave: job {job} ended COMPLETED")),
        "{said}"
    );
    until_listed(&model, "engine", 1);
    let (answered, failed) = load.stop();
    assert!(failed.is_empty(), "{} failed: {failed:#?}", failed.len());
    assert!(answered > 0);
    // One signal to each engine step, by its id, and none to the job.
    let signals: Vec<String> = up
        .fake
        .calls()
        .into_iter()
        .filter(|call| call.starts_with(&format!("scancel --signal=TERM {job}")))
        .collect();
    assert_eq!(signals.len(), 2, "{signals:#?}");
    assert!(
        signals
            .iter()
            .all(|call| call.starts_with(&format!("scancel --signal=TERM {job}."))),
        "{signals:#?}"
    );

    // A joiner in the pool's job is refused, before any Slurm command.
    let before = up.fake.calls().len();
    let (ok, said) = up.gdl(&[&join[..], &["--in-job"]].concat());
    assert!(!ok, "{said}");
    assert!(
        said.contains("`cargo gdl join --in-job` is not available yet"),
        "{said}"
    );
    assert_eq!(up.fake.calls().len(), before, "{:#?}", up.fake.calls());

    // The fleet comes down.
    let (ok, said) = up.gdl(&["down", "--fleet", fleet]);
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    assert_eq!(up.fake.listed(), Vec::<String>::new());

    // A joiner that starts once its pool is gone fails at once: its
    // `resolve` finds no record of the pool to join.
    let late = submit(&up.fake.cluster, &model.join("join-engine.yaml"));
    let deadline = Instant::now() + WAIT;
    loop {
        let held = up.fake.cluster.lock().expect("the cluster");
        if held.jobs[&late].state == "FAILED" {
            let why = held
                .failures
                .iter()
                .find(|failure| failure.starts_with(&format!("job {late}: resolve:")))
                .expect("resolve's failure");
            // The pool withdrew its record as it drained: a directory
            // keeps a withdrawal, or drops the record.
            assert!(
                why.contains("withdrawn") || why.contains("not in the directory"),
                "{why}"
            );
            assert_eq!(held.failures.len(), 1, "{:?}", held.failures);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "job {late} is {}",
            held.jobs[&late].state
        );
        drop(held);
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `join` takes `--nodes` or `--in-job`, not both, and a count of at least
/// 1; `leave` takes `--job` or `--step`, one of them; neither takes a spec,
/// and no other command takes their flags. Each refusal comes before any
/// command runs.
#[test]
fn join_and_leave_refuse_flags_they_do_not_take() {
    let fleet = ["--fleet", "fleet.toml"];
    let join = [&fleet[..], &["--pool", "p", "--segment", "s"]].concat();
    for (args, said) in [
        (
            [&["join"][..], &join, &["--nodes", "2", "--in-job"]].concat(),
            "give one",
        ),
        (
            [&["join"][..], &join, &["--nodes", "0"]].concat(),
            "at least 1, not `0`",
        ),
        ([&["leave"][..], &fleet].concat(), "give --job <id>"),
        (
            [&["leave"][..], &fleet, &["--job", "1", "--step", "1.2"]].concat(),
            "give --job <id>",
        ),
        (
            [&["join"][..], &join, &["--spec", "gdl.toml"]].concat(),
            "usage:",
        ),
        ([&["down"][..], &fleet, &["--job", "1"]].concat(), "usage:"),
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .arg("gdl")
            .args(&args)
            .stdin(Stdio::null())
            .output()
            .expect("cargo gdl");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{args:?}: {stderr}");
        assert!(stderr.contains(said), "{args:?}: {said} in {stderr}");
    }
}

/// A joiner on the pool job's spare node, at the node's address, serves,
/// leaves under load with no failed request, and another takes the node;
/// then `down` stops the pool with the joiner in it. Real hubs and
/// segments under the fake Slurm: it runs for about a minute.
#[test]
#[ignore = "in-job launches are refused until the in-job launcher lands"]
fn a_joiner_in_the_pools_job_serves_and_leaves_without_a_failed_request() {
    let up = Up::with(
        "network_interface = \"ib0\"",
        "slurm = { spare_nodes = 1 }",
        &[],
    );
    let fleet = up.fleet.to_str().expect("a path");
    let (ok, said) = up.deploy();
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    let front = up
        .fake
        .cluster
        .lock()
        .expect("the cluster")
        .front
        .expect("a front");
    let model = up.fleet_dir.join("model-x-01");
    until_listed(&model, "engine", 1);
    // The front serves once it has followed the model pool, which its
    // pool's health need not wait for.
    let deadline = Instant::now() + WAIT;
    while let Err(why) = chat(front, 4) {
        assert!(
            Instant::now() < deadline,
            "the front at {front} answers no request: {why}\n{}",
            up.fake.report(&up.fleet_dir)
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // A joining job of two engines, which ends by the pool's job's end.
    let join = [
        "join",
        "--fleet",
        fleet,
        "--pool",
        "model-x-01",
        "--segment",
        "engine",
    ];

    // A joiner on the pool job's spare node, at the node's address.
    let in_job = [&join[..], &["--in-job"]].concat();
    let (ok, said) = up.gdl(&in_job);
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    let step = said_id(&said, "join: step ");
    assert!(said.contains("on node spare-1003-"), "{said}");
    until_listed(&model, "engine", 2);
    let pool_job = 1003;
    let joiner = systems(&up.fake, pool_job, "gdl-join-engine");
    assert_eq!(joiner.len(), 1);
    let load = Load::start(front, 8);
    std::thread::sleep(Duration::from_secs(2));
    let (_, failed) = load.stop();
    assert!(failed.is_empty(), "{failed:#?}");
    assert!(
        served(joiner[0]) > 0,
        "the joiner in the job served no request"
    );

    // It leaves under load, and frees its node for another.
    let load = Load::start(front, 4);
    let (ok, said) = up.gdl(&["leave", "--fleet", fleet, "--step", &step]);
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    until_listed(&model, "engine", 1);
    let (answered, failed) = load.stop();
    assert!(failed.is_empty(), "{} failed: {failed:#?}", failed.len());
    assert!(answered > 0);
    let (ok, said) = up.gdl(&in_job);
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    assert_ne!(said_id(&said, "join: step "), step);
    until_listed(&model, "engine", 2);

    let (ok, said) = up.gdl(&["down", "--fleet", fleet]);
    assert!(ok, "{said}\n{}", up.fake.report(&up.fleet_dir));
    assert_eq!(up.fake.listed(), Vec::<String>::new());
}
