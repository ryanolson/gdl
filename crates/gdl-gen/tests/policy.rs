//! A pool policy from a crate of its own, end to end: a spec binds the
//! policy `by_model` to its crate and type, the generated chooser builds it
//! from its settings and registers it, and every request reaches the model
//! pool the policy names for its model. Each pool's hub is the `gdl-hub`
//! binary, publishing to a `dir` pool directory in a scratch directory, and
//! every instance runs on loopback.
//!
//! The test builds the generated segments and the hub binary in nested
//! cargo runs that share the examples' target directory: a cold build takes
//! minutes, a warm one seconds. The run itself waits a few seconds for the
//! hubs to follow each other.

mod common;

use std::io::Write as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Lines, Running, cargo, generated, root, target};

const REGION: &str = "us-east-1";
const GLOBAL: &str = "global-01-us-east-1";
const LLAMA: &str = "llama-01-us-east-1";
const QWEN: &str = "qwen-01-us-east-1";

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/policy")
}

/// The generated segments and the hub binary, built: the directory that
/// holds the binaries.
fn build(out: &Path) -> PathBuf {
    std::fs::write(
        out.join("Cargo.toml"),
        "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
    )
    .expect("workspace");
    // Resolved as the repository is, so that the build reuses its versions.
    std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
    for manifest in [out.join("Cargo.toml"), root().join("Cargo.toml")] {
        let mut command = cargo();
        command
            .arg("build")
            .arg("--manifest-path")
            .arg(&manifest)
            .arg("--bins")
            .env("CARGO_TARGET_DIR", target("examples"));
        if manifest.starts_with(root().join("Cargo.toml")) {
            command.args(["-p", "gdl-hub"]);
        }
        let build = command.output().expect("cargo build");
        assert!(
            build.status.success(),
            "{}: {}",
            manifest.display(),
            String::from_utf8_lossy(&build.stderr)
        );
    }
    target("examples").join("debug")
}

/// A port the OS had free a moment ago. A pool's hub must know the address
/// it publishes before it serves, so its ports are chosen first.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port()
}

/// One pool: its settings as `GDL_POOL__*` variables, and its hub's ports.
struct Pool {
    name: &'static str,
    class: &'static str,
    home: Option<&'static str>,
    public: u16,
    control: u16,
}

impl Pool {
    fn new(name: &'static str, class: &'static str, home: Option<&'static str>) -> Self {
        Self {
            name,
            class,
            home,
            public: free_port(),
            control: free_port(),
        }
    }

    /// What the pool's hub and its instances both read, as a Compose
    /// bundle gives it, publishing to `pools`.
    fn variables(&self, pools: &Path) -> Vec<(String, String)> {
        let mut variables = vec![
            ("GDL_POOL__NAME".into(), self.name.into()),
            ("GDL_POOL__CLASS".into(), self.class.into()),
            ("GDL_POOL__REGION".into(), REGION.into()),
            ("GDL_POOL__DIRECTORY".into(), "dir".into()),
            ("GDL_POOL__DIR".into(), pools.display().to_string()),
            ("GDL_POOL__HUB".into(), format!("127.0.0.1:{}", self.public)),
            ("GDL_POOL__SUMMARY_MS".into(), "1000".into()),
            ("GDL_HUB__PUBLIC_PORT".into(), self.public.to_string()),
            ("GDL_HUB__CONTROL_PORT".into(), self.control.to_string()),
        ];
        if let Some(home) = self.home {
            variables.push(("GDL_POOL__HOME".into(), home.into()));
        }
        variables
    }

    /// Start the pool's hub, once it says that it serves: the hub, and its
    /// stderr, read for as long as it runs. Its ports were free when they
    /// were chosen, but another process can bind one before the hub does:
    /// then the hub starts again on ports chosen afresh, a few times.
    fn hub(&mut self, bin: &Path, deployment: &Path, pools: &Path) -> (Running, Lines) {
        let mut said = Vec::new();
        for _ in 0..5 {
            let mut hub = Running(
                Command::new(bin.join("gdl-hub"))
                    .arg(deployment)
                    .envs(self.variables(pools))
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("a hub"),
            );
            let stderr = hub.stderr();
            said.clear();
            while let Some(line) = stderr.try_next(Duration::from_secs(10)) {
                if line.starts_with("gdl-hub: serving") {
                    return (hub, stderr);
                }
                said.push(line);
            }
            if !said
                .iter()
                .any(|line| line.contains("Address already in use"))
            {
                break;
            }
            self.public = free_port();
            self.control = free_port();
        }
        panic!("hub {} does not serve: {said:?}", self.name);
    }

    /// Start an instance of `segment` in this pool, with `extra` in its
    /// settings file and `RUST_LOG` set to `log` when it names a filter:
    /// the instance, with its stdin piped, and its stdout and stderr, each
    /// read as it writes them. `name` tells apart the instances of one
    /// segment in one pool.
    #[allow(clippy::too_many_arguments)]
    fn instance(
        &self,
        bin: &Path,
        dir: &Path,
        pools: &Path,
        segment: &str,
        name: &str,
        extra: &str,
        log: Option<&str>,
    ) -> Instance {
        let config = dir.join(format!("{}-{name}.toml", self.name));
        std::fs::write(
            &config,
            format!(
                "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", heartbeat_ms = 200 }}\n{extra}"
            ),
        )
        .expect("settings");
        let mut command = generated(bin.join(format!("policy-{segment}")));
        command.envs(log.map(|filter| ("RUST_LOG", filter)));
        let mut running = Running(
            command
                .arg(&config)
                .envs(self.variables(pools))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect(segment),
        );
        let (stdout, stderr) = (running.stdout(), running.stderr());
        Instance {
            running,
            stdout,
            stderr,
        }
    }
}

/// A running instance and what it writes.
struct Instance {
    running: Running,
    stdout: Lines,
    stderr: Lines,
}

impl Instance {
    /// What it has written on stderr so far.
    fn errors(&self) -> Vec<String> {
        std::iter::from_fn(|| self.stderr.try_next(Duration::from_millis(100))).collect()
    }
}

/// Ask the source for `model`: the pool of the answer's one item, or the
/// line that ended it otherwise.
fn ask(front: &mut Instance, model: &str) -> Result<String, String> {
    let stdin = front.running.0.stdin.as_mut().expect("piped stdin");
    if let Err(error) = writeln!(stdin, r#"{{"model": "{model}", "text": "hi"}}"#) {
        return Err(format!("{error}; stderr: {:?}", front.errors()));
    }
    let mut pool = None;
    loop {
        let Some(line) = front.stdout.try_next(Duration::from_secs(10)) else {
            return Err(format!("no answer; stderr: {:?}", front.errors()));
        };
        let value: serde_json::Value = serde_json::from_str(&line).expect("a JSON line");
        if let Some(item) = value.get("item") {
            pool = item["pool"].as_str().map(str::to_string);
        } else if value.get("done").is_some() {
            return pool.ok_or_else(|| format!("no item: {line}"));
        } else {
            return Err(line);
        }
    }
}

/// The policy the spec binds is what chooses: each request reaches the pool
/// its settings name for the request's model, which no gdl policy would
/// do. Without the binding the chooser refuses to launch, since nothing
/// registers `by_model`.
#[test]
fn a_bound_policy_chooses_the_pool_of_each_request() {
    let spec_file = fixture().join("gdl.toml");
    let problems = gdl_gen::check_files(&spec_file, None, &target("gdl-gen-tests"));
    assert!(problems.is_empty(), "{:?}", problems.0);

    let temp = tempfile::Builder::new()
        .prefix("gdl-policy-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    let spec = gdl_gen::Spec::load(&spec_file).expect("spec");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
    let bin = build(&out);

    let checked =
        gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read).expect("checked");
    let text = std::fs::read_to_string(&spec_file).expect("spec text");
    let placement = gdl_model::Placements::from_toml(&text)
        .expect("placements")
        .get(gdl_model::Placements::DEFAULT)
        .expect("one layout")
        .clone();
    let deployment = temp.path().join("deployment.json");
    std::fs::write(
        &deployment,
        serde_json::to_vec(
            &gdl_model::DeploymentSpec::new(checked.manifest, placement).expect("deployment"),
        )
        .expect("JSON"),
    )
    .expect("deployment");
    let pools = temp.path().join("pools");
    std::fs::create_dir_all(&pools).expect("pool directory");

    let mut global = Pool::new(GLOBAL, "global", None);
    let mut models = [
        Pool::new(LLAMA, "model", Some(GLOBAL)),
        Pool::new(QWEN, "model", Some(GLOBAL)),
    ];
    // Another process holds the global hub's public port, as one can take
    // a chosen port on a busy host: the hub starts on others. The port is
    // one the OS gave the listener, so that holding it cannot race as well.
    let taken = TcpListener::bind("127.0.0.1:0").expect("a port");
    global.public = taken.local_addr().expect("its address").port();
    let _hubs: Vec<(Running, Lines)> = std::iter::once(&mut global)
        .chain(&mut models)
        .map(|pool| pool.hub(&bin, &deployment, &pools))
        .collect();
    let _models: Vec<Instance> = models
        .iter()
        .map(|pool| {
            let settings = format!("[node.Model]\npool = \"{}\"\n", pool.name);
            pool.instance(&bin, temp.path(), &pools, "model", "model", &settings, None)
        })
        .collect();
    // The policy's settings: `mistral` names no pool, so no pool may take it.
    let settings = format!(
        "[pool_policy.by_model.models]\nllama = \"{LLAMA}\"\nqwen = \"{QWEN}\"\nmistral = \"none\"\n"
    );
    let mut front = global.instance(&bin, temp.path(), &pools, "front", "front", &settings, None);

    // Until the global hub follows both model pools, a request may find no
    // pool: ask until each model's pool answers.
    let deadline = Instant::now() + Duration::from_secs(30);
    for model in ["llama", "qwen"] {
        let mut errors = Vec::new();
        while let Err(error) = ask(&mut front, model) {
            errors.push(error);
            assert!(
                Instant::now() < deadline,
                "{model} found no pool within 30 s: {:?}",
                errors.first()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    for model in ["llama", "qwen", "qwen", "llama", "llama", "qwen"] {
        let expected = if model == "llama" { LLAMA } else { QWEN };
        assert_eq!(ask(&mut front, model).as_deref(), Ok(expected), "{model}");
    }
    let refused = ask(&mut front, "mistral").expect_err("no pool serves mistral");
    assert!(refused.contains("error"), "{refused}");

    // The policy says why on stderr, at `warn`, the binary's default
    // level. Its `info` for each choice is below that level: the six
    // choices came before the refusal on the same stream, so they would
    // be read first.
    let mut before = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let warned = loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match front.stderr.try_next(left) {
            // While the hubs followed each other, other models found no
            // pool too.
            Some(line)
                if line.contains("no pool serves the model")
                    && line.contains(r#"model="mistral""#) =>
            {
                break line;
            }
            Some(line) => before.push(line),
            None => panic!("no warning on stderr within 10 s: {before:?}"),
        }
    };
    assert!(
        warned.contains("WARN") && warned.contains("mistral"),
        "{warned}"
    );
    assert!(
        !before.iter().any(|line| line.contains("chose a pool")),
        "info under the default level: {before:?}"
    );

    // `RUST_LOG` sets the level: a second source instance with `info`
    // logs each choice.
    let mut verbose = global.instance(
        &bin,
        temp.path(),
        &pools,
        "front",
        "front-verbose",
        &settings,
        Some("info"),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut errors = Vec::new();
    while let Err(error) = ask(&mut verbose, "llama") {
        errors.push(error);
        assert!(
            Instant::now() < deadline,
            "llama found no pool within 30 s: {:?}",
            errors.first()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let chose = verbose.stderr.find("chose a pool", Duration::from_secs(10));
    assert!(chose.contains(" INFO policy_router: "), "{chose}");
    assert!(chose.contains(&format!(r#"pool="{LLAMA}""#)), "{chose}");
}

/// `cargo gdl check` reads each policy's crate as it reads a node crate,
/// and refuses a crate it cannot read and a type that is not a pool policy
/// the crate offers, naming the table.
#[test]
fn check_refuses_a_policy_it_cannot_resolve() {
    let text = std::fs::read_to_string(fixture().join("gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(fixture().join("policy.gdl")).expect("graph");
    let refused = |spec: &str| {
        let spec = gdl_gen::Spec::from_text(spec, &graph, &fixture()).expect("a spec");
        gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
            .expect_err("refused")
            .to_string()
    };
    let error = refused(&text.replace("policy_router::ByModel", "policy_router::ByName"));
    assert!(
        error.contains(
            "[pool_policy.by_model]: crate `policy-router` implements no pool policy `ByName`; it implements `ByModel`"
        ),
        "{error}"
    );
    let error = refused(&text.replace("path = \"router\"", "path = \"nowhere\""));
    assert!(
        error.contains("[pool_policy.by_model]: reading ") && error.contains("nowhere"),
        "{error}"
    );
}

/// Two policy names that one crate implements are one dependency of the
/// segment that chooses by both.
#[test]
fn two_bound_policy_names_can_share_one_crate() {
    let text = std::fs::read_to_string(fixture().join("gdl.toml")).expect("spec");
    let text = text
        .replace(
            "crates = [\"nodes\"]",
            "crates = [\"nodes\"]\npin = { Another = \"policy_nodes::Model\" }",
        )
        .replace("nodes = [\"Front\"]", "nodes = [\"Front\", \"Other\"]");
    let text = format!(
        "{text}\n[segment.another]\nnodes = [\"Another\"]\n\
         [class.another]\nsegments = [\"another\"]\n\
         [pools.another]\npolicy = \"by_kind\"\n"
    );
    let graph = "name = \"policy\", schema = 1;\n\
        source Front: Ask -> stream Answer;\n\
        source Other: Ask -> stream Answer;\n\
        sink Model: Ask -> stream Answer;\n\
        sink Another: Ask -> stream Answer;\n\
        topology: Front => Model;\ntopology: Other => Another;\n";
    let spec = gdl_gen::Spec::from_text(&text, graph, &fixture()).expect("spec");
    gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
        .expect("the placement works with one bound policy and one registered in code");

    let text = format!(
        "{text}\n[pool_policy.by_kind]\ncrate = \"policy-router\"\n\
         path = \"router\"\ntype = \"policy_router::ByModel\"\n"
    );
    let spec = gdl_gen::Spec::from_text(&text, graph, &fixture()).expect("spec");
    gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
        .expect("two separately configured policy names share one Cargo dependency");
}

/// The type must be written at the crate root, where the reader found it:
/// a path through a module that the crate does not have is refused by
/// check, not by the generated crate's build.
#[test]
fn check_refuses_a_policy_path_with_a_missing_module() {
    let text = std::fs::read_to_string(fixture().join("gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(fixture().join("policy.gdl")).expect("graph");
    let check = |text: &str| {
        let spec = gdl_gen::Spec::from_text(text, &graph, &fixture()).expect("spec");
        gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
    };
    check(&text).expect("the crate root offers ByModel");
    let text = text.replace("policy_router::ByModel", "policy_router::missing::ByModel");
    assert!(
        check(&text).is_err(),
        "check accepts policy_router::missing::ByModel, but the router crate has no missing module"
    );
}

/// A spec given as text reads a policy crate's library name from its
/// manifest, as a spec loaded from a file does.
#[test]
fn a_policy_library_name_is_preserved_when_loading_spec_text() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-policy-library-")
        .tempdir()
        .expect("scratch");
    let router = temp.path().join("router");
    std::fs::create_dir_all(router.join("src")).expect("source directory");
    std::fs::copy(
        fixture().join("router/src/lib.rs"),
        router.join("src/lib.rs"),
    )
    .expect("policy source");
    let string = |path: PathBuf| toml::Value::String(path.display().to_string());
    let mut manifest: toml::Table = std::fs::read_to_string(fixture().join("router/Cargo.toml"))
        .expect("manifest")
        .parse()
        .expect("manifest table");
    manifest.insert("workspace".into(), toml::Table::new().into());
    manifest.insert(
        "lib".into(),
        toml::Table::from_iter([("name".into(), "router_library".into())]).into(),
    );
    manifest["dependencies"]["gdl"]["path"] = string(root().join("crates/gdl"));
    manifest["dependencies"]["policy-messages"]["path"] = string(fixture().join("messages"));
    std::fs::write(
        router.join("Cargo.toml"),
        toml::to_string(&manifest).expect("manifest text"),
    )
    .expect("manifest");
    std::fs::copy(fixture().join("Cargo.lock"), router.join("Cargo.lock")).expect("lockfile");

    let mut text: toml::Table = std::fs::read_to_string(fixture().join("gdl.toml"))
        .expect("spec")
        .parse()
        .expect("spec table");
    text["gdl"]["path"] = string(root().join("crates/gdl-runtime"));
    text["graph"]["path"] = string(fixture().join("policy.gdl"));
    text["messages"]["crates"] = vec![string(fixture().join("messages"))].into();
    text["nodes"]["crates"] = vec![string(fixture().join("nodes"))].into();
    text["pool_policy"]["by_model"]["path"] = string(router);
    text["pool_policy"]["by_model"]["type"] = "router_library::ByModel".into();
    let text = toml::to_string(&text).expect("spec text");
    let file = temp.path().join("gdl.toml");
    std::fs::write(&file, &text).expect("spec file");
    let spec = gdl_gen::Spec::load(&file).expect("spec from file");
    gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
        .expect("the policy's custom library name works when loaded from a file");

    let graph = std::fs::read_to_string(fixture().join("policy.gdl")).expect("graph");
    let spec = gdl_gen::Spec::from_text(&text, &graph, temp.path()).expect("spec from text");
    gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
        .expect("loading the same spec from text preserves the policy's library name");
}

/// The end-to-end policy test still passes when another process binds the
/// first port it frees, at once, as one can on a busy host: no bind of a
/// port the test picked can fail it before the hub's retry. An
/// `LD_PRELOAD` library takes the port in the test's own process, since
/// nothing else can act between the free and the next bind. It runs the
/// end-to-end test again, so it takes as long.
#[cfg(target_os = "linux")]
#[test]
fn a_competing_binder_does_not_bypass_the_hub_retry() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-port-race-")
        .tempdir()
        .expect("scratch");
    let source = temp.path().join("intercept.c");
    let library = temp.path().join("intercept.so");
    // Run the actual policy test with a socket that takes its first port
    // as soon as free_port releases it. Other executables are unaffected.
    std::fs::write(
        &source,
        r#"
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int enabled, picked_fd = -1, taken_fd = -1;
static struct sockaddr_in picked;

__attribute__((constructor)) static void enable(void) {
    char exe[4096] = {0};
    readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    enabled = strstr(exe, "/deps/policy-") != NULL;
}

int bind(int fd, const struct sockaddr *addr, socklen_t len) {
    int (*real_bind)(int, const struct sockaddr *, socklen_t) = dlsym(RTLD_NEXT, "bind");
    int result = real_bind(fd, addr, len);
    if (enabled && picked_fd == -1 && taken_fd == -1 && result == 0 &&
        addr->sa_family == AF_INET && ((const struct sockaddr_in *)addr)->sin_port == 0) {
        socklen_t size = sizeof(picked);
        if (getsockname(fd, (struct sockaddr *)&picked, &size) != 0) abort();
        picked_fd = fd;
    }
    return result;
}

int close(int fd) {
    int (*real_close)(int) = dlsym(RTLD_NEXT, "close");
    int result = real_close(fd);
    if (enabled && fd == picked_fd && taken_fd == -1) {
        int (*real_bind)(int, const struct sockaddr *, socklen_t) = dlsym(RTLD_NEXT, "bind");
        taken_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (real_bind(taken_fd, (const struct sockaddr *)&picked, sizeof(picked)) != 0) abort();
        fprintf(stderr, "review: competing binder took freed port %u\n", ntohs(picked.sin_port));
    }
    return result;
}
"#,
    )
    .expect("interceptor source");
    let compiled = Command::new("cc")
        .args(["-shared", "-fPIC", "-o"])
        .arg(&library)
        .arg(&source)
        .arg("-ldl")
        .output()
        .expect("compile interceptor");
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    // No limit of its own: the run includes the nested builds, which take
    // minutes when cold, and the test it runs bounds each of its waits.
    let result = Command::new(std::env::current_exe().expect("policy test binary"))
        .args([
            "--exact",
            "a_bound_policy_chooses_the_pool_of_each_request",
            "--nocapture",
        ])
        .env("LD_PRELOAD", &library)
        .output()
        .expect("policy test with a competing binder");
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("review: competing binder took freed port"),
        "the competing binder did not run: {stderr}"
    );
    assert!(
        result.status.success(),
        "the policy test failed after its freed port was taken:\n{stderr}\n{}",
        String::from_utf8_lossy(&result.stdout)
    );
}
