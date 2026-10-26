//! `cargo gdl compose --local`: a bundle whose images build the crates the
//! spec names by path, and gdl and its hub from `[gdl] path`, from a build
//! context that holds them and the bundle. Its files, alone and with a
//! fleet, the specs it refuses, and, with Docker, the http example
//! answering from it.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::{root, target};

fn spec(path: &str) -> gdl_gen::Spec {
    gdl_gen::Spec::load(&root().join(path)).expect("a spec")
}

fn local(spec: &gdl_gen::Spec, out: &Path) -> Result<gdl_gen::Files, gdl_gen::Error> {
    gdl_gen::compose_files(
        spec,
        None,
        out,
        &target("gdl-gen-tests"),
        gdl_gen::Sources::Local,
    )
}

/// The http example's bundle: every file but the generated Rust, pinned.
/// Paths are relative to the bundle or to the context, so no file names
/// this machine's directories, and the pin holds on any checkout.
#[test]
fn a_local_bundle_builds_from_a_context_that_holds_its_crates() {
    let files = local(
        &spec("examples/http/gdl.toml"),
        &root().join("target/gdl-local-golden"),
    )
    .expect("a bundle");
    let found: String = [
        "README.md",
        "compose.yaml",
        "contract/Cargo.toml",
        "hub/Dockerfile",
        "hub/Dockerfile.dockerignore",
        "main/Cargo.toml",
        "main/Dockerfile",
        "main/Dockerfile.dockerignore",
        "main/config.toml",
    ]
    .iter()
    .map(|name| format!("==> {name} <==\n{}\n", files[Path::new(name)]))
    .collect();
    let expected = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bundle/http-local.txt"),
    )
    .expect("the pinned bundle");
    for (found, expected) in found.lines().zip(expected.lines()) {
        assert_eq!(found, expected, "the bundle differs from the pin");
    }
    assert_eq!(found, expected, "the bundle differs from the pin");
    let machine = root().display().to_string();
    for (path, text) in &files {
        assert!(
            !text.contains(&machine),
            "{} names {machine}",
            path.display()
        );
    }
}

/// The api example in two classes: `global` runs the frontend, and
/// `model_x` the engine.
const SPLIT: &str = "[segment.front]\nnodes = [\"Http\"]\n[segment.engine]\nnodes = [\"Engine\"]\n\
    [class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"engine\"]\n";

/// A global cell and two model cells of [`SPLIT`].
const FLEET: &str = "[class.global]\nready = { front = 1 }\n[class.model_x]\nready = { engine = 1 }\n\
    [cell.global-us]\nclass = \"global\"\nregion = \"us-east-1\"\n\
    [cell.model-x-01]\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n\
    [cell.model-x-02]\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n";

/// `--local` with `--fleet`: each cell has its own hub, its segments wait
/// only for it, and every image builds from the local context, the hub
/// from `[gdl] path` and the segments from the crates the spec names by
/// path.
#[test]
fn a_local_fleet_builds_each_cells_images_from_local_sources() {
    let dir = root().join("examples/api");
    let text = std::fs::read_to_string(dir.join("gdl.toml")).expect("the spec");
    let graph = std::fs::read_to_string(dir.join("api.gdl")).expect("the graph");
    let segments = "[segment.front]\nnodes = [\"Http\"]\n[segment.engine]\nnodes = [\"Engine\"]\n";
    assert!(text.contains(segments), "{text}");
    let spec = gdl_gen::Spec::from_text(&text.replace(segments, SPLIT), &graph, &dir)
        .expect("a classed spec");
    let fleet = gdl_model::Fleet::from_toml(FLEET).expect("a fleet");
    let files = gdl_gen::compose_files(
        &spec,
        Some(&fleet),
        &root().join("target/gdl-local-fleet"),
        &target("gdl-gen-tests"),
        gdl_gen::Sources::Local,
    )
    .expect("a bundle");
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("compose.yaml");
    let services = compose["services"].as_object().expect("services");
    assert_eq!(
        services.keys().collect::<Vec<_>>(),
        [
            "global-us",
            "global-us_front",
            "model-x-01",
            "model-x-01_engine",
            "model-x-02",
            "model-x-02_engine"
        ]
    );
    for cell in fleet.cells() {
        assert_eq!(services[&cell.name]["image"], "api-hub");
        let segment = match cell.class.as_str() {
            "global" => "front",
            _ => "engine",
        };
        let service = &services[&format!("{}_{segment}", cell.name)];
        let depends = service["depends_on"].as_object().expect("depends_on");
        assert_eq!(depends.keys().collect::<Vec<_>>(), [&cell.name]);
        assert_eq!(service["environment"]["GDL_HUB__HOST"], cell.name.as_str());
    }
    // One service builds each image, from the context that holds the
    // repository, with the Dockerfile's own ignore file.
    let mut built: Vec<&str> = services
        .values()
        .filter_map(|service| service.get("build"))
        .map(|build| {
            assert_eq!(build["context"], "../..", "{build}");
            build["dockerfile"].as_str().expect("a Dockerfile")
        })
        .collect();
    built.sort();
    assert_eq!(
        built,
        [
            "target/gdl-local-fleet/engine/Dockerfile",
            "target/gdl-local-fleet/front/Dockerfile",
            "target/gdl-local-fleet/hub/Dockerfile"
        ]
    );
    for name in ["hub", "front", "engine"] {
        assert!(
            files.contains_key(&Path::new(name).join("Dockerfile.dockerignore")),
            "{name}"
        );
    }
    assert!(
        files[Path::new("hub/Dockerfile")].contains("cargo install --path '/app/crates/gdl-hub'"),
        "{}",
        files[Path::new("hub/Dockerfile")]
    );
    for segment in ["front", "engine"] {
        let dockerfile = &files[&Path::new(segment).join("Dockerfile")];
        assert!(
            dockerfile.contains(&format!(
                "--manifest-path '/app/target/gdl-local-fleet/{segment}/Cargo.toml'"
            )),
            "{dockerfile}"
        );
        let manifest = &files[&Path::new(segment).join("Cargo.toml")];
        assert!(
            manifest.contains("api-frontend = { path = \"../../../examples/api\" }"),
            "{manifest}"
        );
    }
    assert!(compose["x-gdl-local-sources"].is_string(), "{compose}");
    assert!(files[Path::new("README.md")].contains("This bundle builds local sources"));
    let machine = root().display().to_string();
    for (path, text) in &files {
        assert!(
            !text.contains("git = "),
            "{} names a Git source",
            path.display()
        );
        assert!(
            !text.contains(&machine),
            "{} names {machine}",
            path.display()
        );
    }
}

/// A bundle and crates that share no directory but the root have no build
/// context: sending the whole disk to Docker is none.
#[test]
fn a_bundle_that_shares_only_the_root_with_its_crates_is_refused() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-local-elsewhere-")
        .tempdir()
        .expect("a directory");
    let shared = temp
        .path()
        .components()
        .zip(root().components())
        .take_while(|(a, b)| a == b)
        .count();
    if shared > 1 {
        eprintln!("skipped: the temporary directory and the repository share a directory");
        return;
    }
    let error = local(&spec("examples/http/gdl.toml"), &temp.path().join("bundle"))
        .expect_err("no build context")
        .to_string();
    assert!(
        error.starts_with("`--local`: the build context would be `/`"),
        "{error}"
    );
}

/// `--local` builds gdl from its `[gdl] path`: a spec that names gdl by Git
/// builds without it. A contract spec is refused in either mode.
#[test]
fn local_needs_a_gdl_path_and_a_graph_spec() {
    let out = root().join("target/gdl-local-refused");
    let error = local(&spec("examples/api/deploy.toml"), &out)
        .expect_err("gdl from Git")
        .to_string();
    assert!(error.contains("names gdl by Git or version"), "{error}");
    let error = local(&spec("examples/asr/gdl.toml"), &out)
        .expect_err("a contract spec")
        .to_string();
    assert!(error.contains("graph definition"), "{error}");
}

/// A relative output directory gives the same build as the absolute one:
/// the context is found from the directory resolved, as the manifests are
/// written from it.
#[test]
fn a_local_preview_accepts_a_relative_output_directory() {
    let spec = spec("examples/http/gdl.toml");
    let relative = Path::new("target/gdl-local-relative");
    let absolute = std::env::current_dir()
        .expect("the working directory")
        .join(relative);
    let expected = local(&spec, &absolute).expect("an absolute output works");
    let found = local(&spec, relative).expect("the same output works by relative path");
    let parse = |files: &gdl_gen::Files| {
        serde_json::from_str::<serde_json::Value>(&files[Path::new("compose.yaml")])
            .expect("compose.yaml")
    };
    assert_eq!(
        parse(&found)["services"]["main"]["build"],
        parse(&expected)["services"]["main"]["build"],
        "the build must resolve to the same context and Dockerfile"
    );
}

/// Each image copies its settings by one path, though a local bundle's
/// path from its context can hold spaces: the `COPY` takes its JSON form,
/// which Docker does not split.
#[test]
fn a_local_bundle_copies_its_settings_by_one_path() {
    let out = root().join("target/gdl-local-spaced/bundle with spaces");
    let files = local(&spec("examples/http/gdl.toml"), &out).expect("a bundle");
    for (dockerfile, config) in [
        ("hub/Dockerfile", "deployment.json"),
        ("main/Dockerfile", "main/config.toml"),
    ] {
        let text = &files[Path::new(dockerfile)];
        let copy = text
            .lines()
            .find(|line| line.starts_with("COPY ") && line.contains("/etc/gdl/config"))
            .expect("a copy of the settings");
        let args: Vec<String> = serde_json::from_str(&copy["COPY ".len()..])
            .unwrap_or_else(|e| panic!("{dockerfile}: `{copy}` is not the JSON form: {e}"));
        assert_eq!(
            args,
            [
                format!("target/gdl-local-spaced/bundle with spaces/{config}"),
                "/etc/gdl/config".into()
            ],
            "{dockerfile}"
        );
    }
}

/// The same, with Docker: the hub's generated `COPY` copies its settings
/// from a bundle path with spaces, as a `COPY` in JSON form does.
#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_local_bundle_copies_configuration_from_a_path_with_spaces() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-local-copy-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let out = holder.path().join("bundle with spaces");
    let files = local(&spec("examples/http/gdl.toml"), &out).expect("a bundle");
    let context = tempfile::Builder::new()
        .prefix("gdl-copy-context-")
        .tempdir()
        .expect("a context for the COPY instruction");
    let deployment = out
        .strip_prefix(root())
        .expect("the bundle is under the repository")
        .join("deployment.json");
    let source = context.path().join(&deployment);
    std::fs::create_dir_all(source.parent().expect("a parent")).expect("the source directory");
    std::fs::write(&source, &files[Path::new("deployment.json")]).expect("the config");
    let generated = files[Path::new("hub/Dockerfile")]
        .lines()
        .find(|line| line.starts_with("COPY ") && line.contains("/etc/gdl/config"))
        .expect("the hub copies its config");
    let control = format!(
        "COPY {}",
        serde_json::to_string(&[deployment.to_str().expect("a path"), "/etc/gdl/config"])
            .expect("COPY arguments")
    );
    // Test only COPY, so no Rust image, compilation, or running service is needed.
    for (case, copy) in [("control", control.as_str()), ("generated", generated)] {
        let dockerfile = context.path().join("Dockerfile");
        std::fs::write(&dockerfile, format!("FROM scratch\n{copy}\n")).expect("a Dockerfile");
        let destination = context.path().join(case);
        let result = Command::new("timeout")
            .args([
                "30",
                "docker",
                "build",
                "--network=none",
                "--progress=plain",
                "--output",
            ])
            .arg(format!("type=local,dest={}", destination.display()))
            .arg("--file")
            .arg(&dockerfile)
            .arg(context.path())
            .output()
            .expect("docker build");
        assert!(
            result.status.success(),
            "{case}: {copy}\n{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(destination.join("etc/gdl/config")).expect("the copied config"),
            files[Path::new("deployment.json")]
        );
    }
}

/// Removes what one run of a bundle made, its containers, network, volume,
/// and images, and the bundle, however the test ends.
struct Project {
    name: String,
    dir: PathBuf,
}

impl Project {
    fn compose(&self) -> Command {
        let mut command = Command::new("docker");
        command
            .current_dir(&self.dir)
            .args(["compose", "--project-name", &self.name]);
        command
    }
}

impl Drop for Project {
    fn drop(&mut self) {
        let _ = self
            .compose()
            .args(["down", "--volumes", "--rmi", "all", "--timeout", "15"])
            .output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The http example, from the sources in this checkout, under Docker
/// Compose: the hub and the segment build from the repository, and the
/// frontend answers on a port Docker chooses. Building the images takes a
/// minute or more.
#[test]
#[ignore = "needs docker; run with --ignored"]
fn the_http_example_answers_from_a_local_bundle() {
    let version = Command::new("docker").args(["compose", "version"]).output();
    if !version.is_ok_and(|output| output.status.success()) {
        eprintln!("skipped: `docker compose version` fails");
        return;
    }
    let example = root().join("examples/http");
    let text = std::fs::read_to_string(example.join("gdl.toml")).expect("the spec")
        + "\n[deployment.segment.main]\nports = [\"0:8081\"]\n\
           [deployment.segment.main.config.frontend]\nlisten = \"0.0.0.0:8081\"\n";
    let graph = std::fs::read_to_string(example.join("http.gdl")).expect("the graph");
    let spec = gdl_gen::Spec::from_text(&text, &graph, &example).expect("a spec");
    let id = std::process::id();
    let project = Project {
        name: format!("gdl-local-test-{id}"),
        dir: root().join(format!("target/gdl-local-test-{id}")),
    };
    let _ = std::fs::remove_dir_all(&project.dir);
    gdl_gen::compose(
        &spec,
        None,
        &project.dir,
        &target("gdl-gen-tests"),
        gdl_gen::Sources::Local,
    )
    .expect("a bundle");

    let started = Instant::now();
    let up = project
        .compose()
        .args(["up", "--build", "--detach", "--wait"])
        .output()
        .expect("docker compose up");
    assert!(
        up.status.success(),
        "{}",
        String::from_utf8_lossy(&up.stderr)
    );
    let built = started.elapsed();
    let port = project
        .compose()
        .args(["port", "main", "8081"])
        .output()
        .expect("docker compose port");
    let address = String::from_utf8_lossy(&port.stdout).trim().to_string();
    let port = address.rsplit_once(':').expect("an address").1;

    let body = "{\"text\":\"hello source\"}";
    let mut stream = TcpStream::connect(("127.0.0.1", port.parse::<u16>().expect("a port")))
        .expect("the frontend");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a timeout");
    write!(
        stream,
        "POST /generate HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("the request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("the response");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    for item in [
        "{\"item\":{\"text\":\"hello\"}}",
        "{\"item\":{\"text\":\"source\"}}",
    ] {
        assert!(response.contains(item), "{item} in:\n{response}");
    }
    eprintln!(
        "built and started in {:.1} s, answered {:.1} s later",
        built.as_secs_f64(),
        (started.elapsed() - built).as_secs_f64()
    );
}
