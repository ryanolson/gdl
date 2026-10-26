//! A cell policy from a crate of its own, end to end: a spec binds the
//! policy `by_model` to its crate and type, the generated chooser builds it
//! from its settings and registers it, and every request reaches the model
//! cell the policy names for its model. Each cell's hub is the `gdl-hub`
//! binary, publishing to a `dir` cell directory in a scratch directory, and
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

/// A port the OS had free a moment ago. A cell's hub must know the address
/// it publishes before it serves, so its ports are chosen first.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port()
}

/// One cell: its settings as `GDL_CELL__*` variables, and its hub's ports.
struct Cell {
    name: &'static str,
    class: &'static str,
    home: Option<&'static str>,
    public: u16,
    control: u16,
}

impl Cell {
    fn new(name: &'static str, class: &'static str, home: Option<&'static str>) -> Self {
        Self {
            name,
            class,
            home,
            public: free_port(),
            control: free_port(),
        }
    }

    /// What the cell's hub and its instances both read, as a Compose
    /// bundle gives it, publishing to `cells`.
    fn variables(&self, cells: &Path) -> Vec<(String, String)> {
        let mut variables = vec![
            ("GDL_CELL__NAME".into(), self.name.into()),
            ("GDL_CELL__CLASS".into(), self.class.into()),
            ("GDL_CELL__REGION".into(), REGION.into()),
            ("GDL_CELL__DIRECTORY".into(), "dir".into()),
            ("GDL_CELL__DIR".into(), cells.display().to_string()),
            ("GDL_CELL__HUB".into(), format!("127.0.0.1:{}", self.public)),
            ("GDL_CELL__SUMMARY_MS".into(), "1000".into()),
            ("GDL_HUB__PUBLIC_PORT".into(), self.public.to_string()),
            ("GDL_HUB__CONTROL_PORT".into(), self.control.to_string()),
        ];
        if let Some(home) = self.home {
            variables.push(("GDL_CELL__HOME".into(), home.into()));
        }
        variables
    }

    /// Start the cell's hub, once it says that it serves: the hub, and its
    /// stderr, read for as long as it runs.
    fn hub(&self, bin: &Path, deployment: &Path, cells: &Path) -> (Running, Lines) {
        let mut hub = Running(
            Command::new(bin.join("gdl-hub"))
                .arg(deployment)
                .envs(self.variables(cells))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .expect("a hub"),
        );
        let stderr = hub.stderr();
        let mut said = Vec::new();
        loop {
            match stderr.try_next(Duration::from_secs(10)) {
                Some(line) if line.starts_with("gdl-hub: serving") => break,
                Some(line) => said.push(line),
                None => panic!("hub {} does not serve: {said:?}", self.name),
            }
        }
        (hub, stderr)
    }

    /// Start an instance of `segment` in this cell, with `extra` in its
    /// settings file and `RUST_LOG` set to `log` when it names a filter:
    /// the instance, with its stdin piped, and its stdout and stderr, each
    /// read as it writes them. `name` tells apart the instances of one
    /// segment in one cell.
    #[allow(clippy::too_many_arguments)]
    fn instance(
        &self,
        bin: &Path,
        dir: &Path,
        cells: &Path,
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
                .envs(self.variables(cells))
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

/// Ask the source for `model`: the cell of the answer's one item, or the
/// line that ended it otherwise.
fn ask(front: &mut Instance, model: &str) -> Result<String, String> {
    let stdin = front.running.0.stdin.as_mut().expect("piped stdin");
    if let Err(error) = writeln!(stdin, r#"{{"model": "{model}", "text": "hi"}}"#) {
        return Err(format!("{error}; stderr: {:?}", front.errors()));
    }
    let mut cell = None;
    loop {
        let Some(line) = front.stdout.try_next(Duration::from_secs(10)) else {
            return Err(format!("no answer; stderr: {:?}", front.errors()));
        };
        let value: serde_json::Value = serde_json::from_str(&line).expect("a JSON line");
        if let Some(item) = value.get("item") {
            cell = item["cell"].as_str().map(str::to_string);
        } else if value.get("done").is_some() {
            return cell.ok_or_else(|| format!("no item: {line}"));
        } else {
            return Err(line);
        }
    }
}

/// The policy the spec binds is what chooses: each request reaches the cell
/// its settings name for the request's model, which no gdl policy would
/// do. Without the binding the chooser refuses to launch, since nothing
/// registers `by_model`.
#[test]
fn a_bound_policy_chooses_the_cell_of_each_request() {
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
    let cells = temp.path().join("cells");
    std::fs::create_dir_all(&cells).expect("cell directory");

    let global = Cell::new(GLOBAL, "global", None);
    let models = [
        Cell::new(LLAMA, "model", Some(GLOBAL)),
        Cell::new(QWEN, "model", Some(GLOBAL)),
    ];
    let _hubs: Vec<(Running, Lines)> = std::iter::once(&global)
        .chain(&models)
        .map(|cell| cell.hub(&bin, &deployment, &cells))
        .collect();
    let _models: Vec<Instance> = models
        .iter()
        .map(|cell| {
            let settings = format!("[node.Model]\ncell = \"{}\"\n", cell.name);
            cell.instance(&bin, temp.path(), &cells, "model", "model", &settings, None)
        })
        .collect();
    // The policy's settings: `mistral` names no cell, so no cell may take it.
    let settings = format!(
        "[cell_policy.by_model.models]\nllama = \"{LLAMA}\"\nqwen = \"{QWEN}\"\nmistral = \"none\"\n"
    );
    let mut front = global.instance(&bin, temp.path(), &cells, "front", "front", &settings, None);

    // Until the global hub follows both model cells, a request may find no
    // cell: ask until each model's cell answers.
    let deadline = Instant::now() + Duration::from_secs(30);
    for model in ["llama", "qwen"] {
        let mut errors = Vec::new();
        while let Err(error) = ask(&mut front, model) {
            errors.push(error);
            assert!(
                Instant::now() < deadline,
                "{model} found no cell within 30 s: {:?}",
                errors.first()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    for model in ["llama", "qwen", "qwen", "llama", "llama", "qwen"] {
        let expected = if model == "llama" { LLAMA } else { QWEN };
        assert_eq!(ask(&mut front, model).as_deref(), Ok(expected), "{model}");
    }
    let refused = ask(&mut front, "mistral").expect_err("no cell serves mistral");
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
            // cell too.
            Some(line)
                if line.contains("no cell serves the model")
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
        !before.iter().any(|line| line.contains("chose a cell")),
        "info under the default level: {before:?}"
    );

    // `RUST_LOG` sets the level: a second source instance with `info`
    // logs each choice.
    let mut verbose = global.instance(
        &bin,
        temp.path(),
        &cells,
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
            "llama found no cell within 30 s: {:?}",
            errors.first()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let chose = verbose.stderr.find("chose a cell", Duration::from_secs(10));
    assert!(chose.contains(" INFO policy_router: "), "{chose}");
    assert!(chose.contains(&format!(r#"cell="{LLAMA}""#)), "{chose}");
}

/// `cargo gdl check` reads each policy's crate as it reads a node crate,
/// and refuses a crate it cannot read and a type that is not a cell policy
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
            "[cell_policy.by_model]: crate `policy-router` implements no cell policy `ByName`; it implements `ByModel`"
        ),
        "{error}"
    );
    let error = refused(&text.replace("path = \"router\"", "path = \"nowhere\""));
    assert!(
        error.contains("[cell_policy.by_model]: reading ") && error.contains("nowhere"),
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
         [cells.another]\npolicy = \"by_kind\"\n"
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
        "{text}\n[cell_policy.by_kind]\ncrate = \"policy-router\"\n\
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
    text["cell_policy"]["by_model"]["path"] = string(router);
    text["cell_policy"]["by_model"]["type"] = "router_library::ByModel".into();
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
