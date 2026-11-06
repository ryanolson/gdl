//! A pool's srt-slurm recipe started as srt-slurm starts it, by a fake
//! launcher on one host: each service in the order the recipe declares,
//! one instance at a time, and each instance's readiness probe passed
//! before the next starts. The pool's class has two segments, each of which
//! waits for the other before it is healthy: a probe of health would hold
//! the first instance forever, and the recipe's probe of liveness does
//! not. The hub is the `gdl-hub` binary, and each instance the api
//! example's generated segment, all on loopback.
//!
//! The fake keeps srt-slurm's order but not the recipe's ports: its nodes
//! share one host, so each process binds ports the OS chooses, and the
//! fake reads them from what the process prints. The render tests check
//! the recipe's own ports, node by node. The directory is the `dir`
//! backend in a scratch directory, in place of the recipe's DynamoDB.
//!
//! The test builds the generated segments and the hub binary in nested
//! cargo runs that share the examples' target directory: a cold build
//! takes minutes, a warm one seconds.

mod common;

use std::io::{BufRead as _, BufReader, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Lines, Running, cargo, generated, root, target};
use serde_json::Value;

/// The api example, its two segments in one class: an instance of either
/// is healthy only once the class has an instance of each. Its
/// `[deployment]` table builds each segment with the hub's discovery, as a
/// deployment's images are.
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
    assert_eq!(
        text.matches(&api.display().to_string()).count(),
        3,
        "{text}"
    );
    let file = dir.join("gdl.toml");
    std::fs::write(
        &file,
        format!("{text}[deployment]\n[class.global]\nsegments = [\"front\", \"engine\"]\n"),
    )
    .expect("the spec");
    file
}

/// One pool of that class on a Slurm site.
const FLEET: &str = "[fleet]\nname = \"api\"\n\n\
    [cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = \"/shared/srt-slurm\"\nrun_dir = \"/shared/gdl-runs\"\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n\n\
    [directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\ncluster = \"lab\"\n\n\
    [class.global]\nready = { front = 1, engine = 1 }\n\n\
    [pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n";

/// The recipes of [`FLEET`] for the spec in `file`, written to `out`.
fn render(file: &Path, out: &Path) {
    let spec = gdl_gen::Spec::load(file).expect("the spec");
    let fleet = gdl_model::Fleet::from_toml(FLEET).expect("the fleet");
    let entries: Vec<String> = ["api-hub", "api-front", "api-engine"]
        .iter()
        .enumerate()
        .map(|(index, repository)| {
            format!(
                "\"registry.example/gdl/{repository}:pinned\": \"sha256:{:064x}\"",
                index + 1
            )
        })
        .collect();
    let digests =
        gdl_gen::Digests::from_json(&format!("{{{}}}", entries.join(", "))).expect("digests");
    let target = target("gdl-gen-tests");
    let files = gdl_gen::slurm_files(
        &spec,
        &fleet,
        &gdl_gen::SlurmRender {
            target: &target,
            tag: Some("pinned"),
            digests: &digests,
            sources: gdl_gen::Sources::Local,
        },
    )
    .expect("the recipes");
    gdl_gen::write(out, &files).expect("written");
}

/// The api example's segments and the hub binary, built: the directory
/// that holds the segments' binaries, and the hub's.
fn build(file: &Path, dir: &Path) -> (PathBuf, PathBuf) {
    let target = target("examples");
    let bin = dir.join("bin");
    let build = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "build", "--spec"])
        .arg(file)
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

/// The status `GET path` answers at `port` on loopback, if it answers.
fn status(port: u16, path: &str) -> Option<u16> {
    get(port, path).map(|(status, _)| status)
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

/// A launcher that starts a recipe's services as srt-slurm does, each
/// process on loopback.
struct Launcher {
    /// The segments' binaries, by package.
    bin: PathBuf,
    hub: PathBuf,
    /// The pool's settings, as the recipe mounts them at `/gdl`.
    settings: PathBuf,
    /// The `dir` pool directory.
    pools: PathBuf,
    /// How long one probe is waited for.
    wait: Duration,
}

/// What the launcher started: each process, and its stderr.
struct Started {
    processes: Vec<(String, Running, Lines)>,
    hub_public: u16,
}

impl Launcher {
    /// Start `recipe`'s services in order, each instance once the one
    /// before it passed its probe. `Err` names the instance whose probe did
    /// not pass within the wait, with what was started, so that it is
    /// stopped when dropped.
    fn launch(&self, recipe: &Value) -> Result<Started, (String, Started)> {
        let mut started = Started {
            processes: Vec::new(),
            hub_public: 0,
        };
        let mut hub_control = 0;
        for service in recipe["services"].as_array().expect("services") {
            let name = service["name"].as_str().expect("a name").to_string();
            let instances = service["nodes"].as_u64().unwrap_or(1);
            for index in 0..instances {
                let words: Vec<&str> = service["command"]
                    .as_array()
                    .expect("a command")
                    .iter()
                    .map(|word| word.as_str().expect("a word"))
                    .collect();
                let container = service["container"].as_str().expect("an image");
                let repository = container
                    .split_once('#')
                    .and_then(|(_, rest)| rest.split_once('@'))
                    .map(|(path, _)| path.rsplit('/').next().expect("a name"))
                    .expect("a repository");
                let is_hub = repository == "api-hub";
                let program = match is_hub {
                    true => self.hub.clone(),
                    false => self.bin.join(repository),
                };
                assert_eq!(words[0], "/usr/local/bin/segment", "{service}");
                let mut command = generated(&program);
                for word in &words[1..] {
                    match word.strip_prefix("/gdl/") {
                        Some(file) => command.arg(self.settings.join(file)),
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
                    .env("GDL_POOL__DIR", &self.pools);
                // One host holds every fake node: each listener takes the
                // port the OS gives it.
                if is_hub {
                    command
                        .env("GDL_HUB__PUBLIC_PORT", "0")
                        .env("GDL_HUB__CONTROL_PORT", "0")
                        .env("GDL_POOL__HUB", "127.0.0.1:9");
                } else {
                    command
                        .env("GDL_HUB__PUBLIC_PORT", started.hub_public.to_string())
                        .env("GDL_HUB__CONTROL_PORT", hub_control.to_string())
                        .env("GDL_SYSTEM__PORT", "0");
                }
                let mut running = Running(
                    command
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::piped())
                        .spawn()
                        .unwrap_or_else(|e| panic!("{}: {e}", program.display())),
                );
                let stderr = running.stderr();
                let instance = format!("{name}[{index}]");
                let probe = &service["readiness"]["http"];
                let path = probe["path"].as_str().expect("a path").to_string();
                let port = match is_hub {
                    true => {
                        let line = stderr.find("gdl-hub: serving", self.wait);
                        let (public, control) =
                            line.split_once(", control ").expect("the hub's addresses");
                        started.hub_public = port_of(public);
                        hub_control = port_of(control);
                        started.hub_public
                    }
                    false => port_of(&stderr.find("system listener at", self.wait)),
                };
                started.processes.push((instance.clone(), running, stderr));
                let deadline = Instant::now() + self.wait;
                while status(port, &path) != Some(200) {
                    if Instant::now() > deadline {
                        return Err((instance, started));
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
        Ok(started)
    }
}

#[test]
fn a_class_of_two_segments_starts_in_srt_slurms_order() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-slurm-launch-")
        .tempdir()
        .expect("scratch");
    let file = spec_file(temp.path());
    let out = temp.path().join("out");
    render(&file, &out);
    let (bin, hub) = build(&file, temp.path());
    let recipe: Value = serde_json::from_str(
        &std::fs::read_to_string(out.join("lab/global-us/pool.yaml")).expect("the recipe"),
    )
    .expect("JSON");
    let names: Vec<&str> = recipe["services"]
        .as_array()
        .expect("services")
        .iter()
        .map(|service| service["name"].as_str().expect("a name"))
        .collect();
    // The placement's segments, in name order.
    assert_eq!(names, ["hub", "engine", "front"]);
    let launcher = |pools: &str, wait: Duration| {
        let pools = temp.path().join(pools);
        std::fs::create_dir_all(&pools).expect("the pool directory");
        Launcher {
            bin: bin.clone(),
            hub: hub.clone(),
            settings: out.join("lab/global-us"),
            pools,
            wait,
        }
    };

    // Each instance is ready once it is live, so the second segment starts
    // after the first, and the pool becomes healthy.
    let started = launcher("live", Duration::from_secs(10))
        .launch(&recipe)
        .unwrap_or_else(|(instance, started)| {
            let said: Vec<String> = started
                .processes
                .iter()
                .flat_map(|(name, _, stderr)| {
                    std::iter::from_fn(|| stderr.try_next(Duration::from_millis(100)))
                        .map(move |line| format!("{name}: {line}"))
                })
                .collect();
            panic!("{instance} was not ready: {said:#?}")
        });
    let launched: Vec<&str> = started
        .processes
        .iter()
        .map(|(name, _, _)| name.as_str())
        .collect();
    assert_eq!(launched, ["hub[0]", "engine[0]", "front[0]"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while status(started.hub_public, "/v1/ext/gdl-pool/health") != Some(200) {
        if Instant::now() > deadline {
            let said: Vec<String> = started
                .processes
                .iter()
                .flat_map(|(name, _, stderr)| {
                    std::iter::from_fn(|| stderr.try_next(Duration::from_millis(100)))
                        .map(move |line| format!("{name}: {line}"))
                })
                .collect();
            panic!(
                "the pool never became healthy: {:?}; {said:#?}",
                get(started.hub_public, "/v1/ext/gdl-pool/health")
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(started);

    // The control: probed on health, the first instance waits for the
    // second segment, which a launcher that waits for it never starts. The
    // wait is short, since what it shows is that nothing happens; the
    // instance would wait for its rendezvous, a minute.
    let mut health = recipe.clone();
    for service in health["services"].as_array_mut().expect("services") {
        if service["name"] != "hub" {
            service["readiness"]["http"]["path"] = "/health".into();
        }
    }
    match launcher("health", Duration::from_secs(3)).launch(&health) {
        Ok(_) => panic!("a class of two segments started on health probes"),
        Err((instance, started)) => {
            assert_eq!(instance, "engine[0]");
            assert_eq!(started.processes.len(), 2);
        }
    }
}
