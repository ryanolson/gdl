//! Container deployment output contains an ordered hub and one image per segment.
use std::path::Path;

mod common;

#[path = "deployment/relay.rs"]
mod relay;

#[test]
fn compose_starts_segments_only_after_hub_health() {
    let mirror = Mirror::new();
    let spec = portable_spec(&mirror).expect("valid test fixture");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-test-")
        .tempdir()
        .expect("valid test fixture");
    let files = gdl_gen::compose_files(
        &spec,
        None,
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("valid test fixture");
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("valid test fixture");
    for name in ["front", "engine"] {
        assert_eq!(
            compose["services"][name]["depends_on"]["hub"]["condition"],
            "service_healthy"
        );
        assert!(files.contains_key(&Path::new(name).join("Dockerfile")));
        // Each image builds its segment in release, from the segment's own
        // workspace, so the profile there is the one that applies.
        let manifest: toml::Table = files[&Path::new(name).join("Cargo.toml")]
            .parse()
            .expect("a manifest");
        assert_eq!(
            manifest["profile"],
            toml::from_str::<toml::Value>("release = { codegen-units = 1, lto = \"fat\" }")
                .expect("the profile"),
            "{name}"
        );
        let config = &files[&Path::new(name).join("config.toml")];
        assert!(config.contains("[hub]\nhost = \"hub\""));
        // The dynamo operator's port; the probe asks the binary itself, so
        // an image needs no HTTP client.
        assert!(config.contains("[system]\nport = 9090"), "{config}");
        let check = &compose["services"][name]["healthcheck"];
        assert_eq!(
            check["test"],
            serde_json::json!([
                "CMD",
                "/usr/local/bin/segment",
                "--healthcheck",
                "/etc/gdl/config"
            ])
        );
        assert!(check["start_period"].is_string(), "{check}");
        // The drain's bound, the request deadline by default, and a margin:
        // Docker otherwise kills a container 10 s after stopping it.
        assert_eq!(compose["services"][name]["stop_grace_period"], "630s");
        // Health is reported, never waited on between segments.
        let depends = compose["services"][name]["depends_on"]
            .as_object()
            .expect("depends_on");
        assert_eq!(depends.keys().collect::<Vec<_>>(), ["hub"]);
    }
    assert!(
        files[Path::new("hub/Dockerfile")]
            .contains(&format!("cargo install --git '{}'", mirror.url))
    );
    assert!(files.values().all(|text| !text.contains("COPY sources")));
    assert!(files[Path::new("front/Cargo.toml")].contains(&format!("git = \"{}\"", mirror.url)));
    assert!(files.contains_key(Path::new("deployment.json")));
    // Instances join and retire while the others serve, and the README
    // says when Compose can add them.
    let readme = &files[Path::new("README.md")];
    assert!(
        readme.contains("docker compose up --scale <segment>=N"),
        "{readme}"
    );
    assert!(
        readme.contains("Scaling works only for a segment that publishes no host port"),
        "{readme}"
    );
    assert!(!readme.contains("not routed to"), "{readme}");
}

#[test]
fn each_image_can_override_the_runtime_base_without_changing_the_builder() {
    let mirror = Mirror::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text =
        std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("valid test fixture");
    let graph =
        std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("valid test fixture");
    let text = mirror.portable(&text);
    let text = format!(
        "{text}\n[deployment]\nbase_image = \"ubuntu:24.04\"\nbuild_image = \"rust:1.93-bookworm\"\nhub_image = \"debian:bookworm-slim\"\n[deployment.segment.engine]\nbase_image = \"nvcr.io/nvidia/cuda:12.8.1-runtime-ubuntu24.04\"\n[deployment.segment.engine.config.runtime]\ndrain_ms = 90000\n"
    );
    let spec = gdl_gen::Spec::from_text(&text, &graph, &root.join("examples/api"))
        .expect("valid test fixture");
    let files = gdl_gen::compose_files(
        &spec,
        None,
        &root.join("target/deployment-test"),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("valid test fixture");
    assert!(files[Path::new("front/Dockerfile")].contains("FROM ubuntu:24.04\n"));
    assert!(
        files[Path::new("engine/Dockerfile")]
            .contains("FROM nvcr.io/nvidia/cuda:12.8.1-runtime-ubuntu24.04\n")
    );
    assert!(files[Path::new("engine/Dockerfile")].contains("FROM rust:1.93-bookworm AS build"));
    // A drain bound set in the deployment's settings sets the grace.
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("valid test fixture");
    assert_eq!(compose["services"]["engine"]["stop_grace_period"], "120s");
    assert_eq!(compose["services"]["front"]["stop_grace_period"], "630s");
}

/// Without image settings, a segment and the hub run in a distroless image
/// as a user that is not root, and are built in the toolchain that
/// `rust-toolchain.toml` pins, on the Debian release the distroless image
/// is built from, so that the binary finds the glibc it was linked against.
#[test]
fn the_default_images_are_distroless_and_the_pinned_toolchain() {
    let mirror = Mirror::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let toolchain: toml::Table = toml::from_str(
        &std::fs::read_to_string(root.join("rust-toolchain.toml")).expect("the toolchain"),
    )
    .expect("the toolchain");
    let channel = toolchain["toolchain"]["channel"]
        .as_str()
        .expect("a channel");
    let spec = portable_spec(&mirror).expect("valid test fixture");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-defaults-")
        .tempdir()
        .expect("a directory");
    let files = gdl_gen::compose_files(
        &spec,
        None,
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    for dockerfile in ["front/Dockerfile", "engine/Dockerfile", "hub/Dockerfile"] {
        let text = &files[Path::new(dockerfile)];
        assert!(
            text.contains(&format!("FROM rust:{channel}-slim-bookworm AS build\n")),
            "{dockerfile}:\n{text}"
        );
        assert!(
            text.contains("FROM gcr.io/distroless/cc-debian12:nonroot\n"),
            "{dockerfile}:\n{text}"
        );
    }
}

#[test]
fn preview_files_are_the_exact_downloaded_snapshot() {
    let mirror = Mirror::new();
    use std::io::Read;
    let spec = portable_spec(&mirror).expect("valid deployment fixture");
    let bundle =
        gdl_gen::compose_bundle(&spec, &mirror.target()).expect("valid deployment fixture");
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bundle.archive.as_slice()));
    let mut matched = 0;
    for entry in archive.entries().expect("valid deployment fixture") {
        let mut entry = entry.expect("valid deployment fixture");
        let path = entry
            .path()
            .expect("valid deployment fixture")
            .strip_prefix("deployment")
            .expect("valid deployment fixture")
            .to_path_buf();
        if let Some(expected) = bundle.files.get(&path) {
            let mut actual = String::new();
            entry
                .read_to_string(&mut actual)
                .expect("valid deployment fixture");
            assert_eq!(&actual, expected, "{}", path.display());
            matched += 1;
        }
    }
    assert_eq!(matched, bundle.files.len());
    assert!(bundle.files.contains_key(Path::new("compose.yaml")));
    assert!(bundle.files.contains_key(Path::new("engine/Dockerfile")));
}

#[test]
fn deployment_rejects_local_node_and_message_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let spec = gdl_gen::Spec::load(&root.join("examples/api/gdl.toml")).expect("fixture");
    let result = gdl_gen::compose_files(
        &spec,
        None,
        &root.join("target/rejected"),
        &root.join("target"),
        gdl_gen::Sources::Fetched,
    );
    assert!(result.is_err(), "local packages must not be deployable");
    assert!(
        result
            .expect_err("rejection")
            .to_string()
            .contains("api-frontend")
    );
}

/// The chat example with its crates from the mirror at the pinned
/// commit: a source with no pinned frontend, whose binary reads JSON
/// requests on stdin.
fn chat_spec(mirror: &Mirror) -> gdl_gen::Spec {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/chat/gdl.toml"))
        .expect("the chat spec")
        .replace(
            "crates = [\"protocols\"]",
            &format!("crates = [{}]", mirror.dep("chat-protocols")),
        )
        .replace(
            "crates = [\"front-nodes\", \"engine-mock\", \"post-nodes\"]",
            &format!(
                "crates = [{}, {}, {}]",
                mirror.dep("chat-front-nodes"),
                mirror.dep("chat-engine-mock"),
                mirror.dep("chat-post-nodes")
            ),
        )
        .replace("path = \"../../crates/gdl-runtime\"", &mirror.gdl());
    let graph = std::fs::read_to_string(root.join("examples/chat/chat.gdl")).expect("the graph");
    gdl_gen::Spec::from_text(&text, &graph, &root.join("examples/chat")).expect("a spec")
}

/// A source with no pinned frontend reads JSON requests on stdin, which
/// `docker compose run -T <source>` gives it. Its service keeps stdin open,
/// so that it also waits for requests under `up`, rather than reading the
/// end of its input and stopping. It still waits for the hub alone.
#[test]
fn a_source_that_reads_stdin_keeps_it_open() {
    let mirror = Mirror::new();
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-stdin-")
        .tempdir()
        .expect("a directory");
    let files = gdl_gen::compose_files(
        &chat_spec(&mirror),
        None,
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("compose.yaml");
    let services = &compose["services"];
    assert_eq!(services["front"]["stdin_open"], true, "{compose}");
    for other in ["engine", "post", "hub"] {
        assert!(
            services[other].get("stdin_open").is_none(),
            "{other}: {compose}"
        );
    }
    assert_liveness(&compose, |_| "hub".into());
    // Each segment starts without the source, which comes with each run.
    for segment in ["front", "engine", "post"] {
        let config = &files[&Path::new(segment).join("config.toml")];
        assert!(
            config.contains("\n[min_replicas]\nfront = 0\n"),
            "{segment}: {config}"
        );
    }
    let readme = &files[Path::new("README.md")];
    assert!(
        readme.contains("docker compose run --rm -T front < requests.jsonl"),
        "{readme}"
    );
}

/// The mirror commit that `examples/api/deploy.toml` pins gdl and the
/// example package to, read from that file so that a pin is moved in one
/// place.
fn pinned_rev() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/deploy.toml")).expect("deploy spec");
    let spec: toml::Table = toml::from_str(&text).expect("deploy spec");
    spec["gdl"]["rev"].as_str().expect("[gdl] rev").to_string()
}

/// A stand-in for the mirror that `examples/api/deploy.toml` names, so
/// that no test reaches GitHub (`docs/src/development/testing.md`): a Git
/// repository built from this repository's history, and a target
/// directory of the test's own for the fetches from it.
///
/// The repository is built once per pinned commit, under the cargo target
/// directory ([`mirror_repo`]), and every test fetches from it. Cargo keeps
/// a checkout in its home for each repository URL and commit it fetches,
/// and the checkout outlives the test: with a repository of its own, each
/// test would add one, 34 MiB per run of these tests.
struct Mirror {
    dir: tempfile::TempDir,
    url: String,
    rev: String,
}

impl Mirror {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let repo = mirror_repo(&common::target("gdl-test-mirrors"), &pinned_rev());
        let mirror = Self {
            url: format!("file://{}", repo.display()),
            rev: head(&repo),
            dir: tempfile::Builder::new()
                .prefix("gdl-mirror-target-")
                .tempdir()
                .expect("a directory"),
        };
        // A fetch resolves its package in a manifest of its own, which has
        // no lock, so Cargo would read crates.io's index. Each starts from
        // its example's lock, which pins every crate the package needs:
        // the fetch keeps a lock it finds.
        for (packages, lock) in [
            (&["api-frontend"][..], "examples/api/Cargo.lock"),
            (
                &[
                    "chat-protocols",
                    "chat-front-nodes",
                    "chat-engine-mock",
                    "chat-post-nodes",
                ],
                "examples/chat/Cargo.lock",
            ),
        ] {
            for package in packages {
                let scratch = gdl_extract::Workspace::git_scratch(
                    package,
                    &mirror.url,
                    &mirror.rev,
                    &mirror.target(),
                );
                std::fs::create_dir_all(&scratch).expect("the fetch's directory");
                std::fs::copy(root.join(lock), scratch.join("Cargo.lock"))
                    .expect("the example's lock");
            }
        }
        mirror
    }

    /// The target directory the tests fetch into.
    fn target(&self) -> std::path::PathBuf {
        self.dir.path().join("target")
    }

    /// `package` from the mirror, as a spec lists a crate.
    fn dep(&self, package: &str) -> String {
        format!(
            "{{ package = \"{package}\", git = \"{}\", rev = \"{}\" }}",
            self.url, self.rev
        )
    }

    /// gdl from the mirror, as `[gdl]` names it.
    fn gdl(&self) -> String {
        format!("git = \"{}\"\nrev = \"{}\"", self.url, self.rev)
    }

    /// `text`, the api example's spec, with its crate and gdl from the
    /// mirror, as `examples/api/deploy.toml` takes them from GitHub.
    fn portable(&self, text: &str) -> String {
        text.replace(
            "crates = [\".\"]",
            &format!("crates = [{}]", self.dep("api-frontend")),
        )
        .replace("path = \"../../crates/gdl-runtime\"", &self.gdl())
    }
}

/// A relative `CARGO_TARGET_DIR`, which Cargo accepts, still gives the
/// mirror an absolute `file://` URL, which Cargo can fetch. The child runs
/// the hub-health test with the suite's target directory named relative
/// to its working directory.
#[test]
fn a_relative_cargo_target_directory_can_fetch_the_mirror() {
    let target = common::target("")
        .canonicalize()
        .expect("the Cargo target directory");
    // Reuse the suite's target so this check adds no repository URL to Cargo's cache.
    let output = std::process::Command::new("timeout")
        .arg(common::CARGO_LIMIT)
        .arg(std::env::current_exe().expect("the test binary"))
        .args([
            "--exact",
            "compose_starts_segments_only_after_hub_health",
            "--nocapture",
        ])
        .current_dir(target.parent().expect("the target's parent"))
        .env(
            "CARGO_TARGET_DIR",
            target.file_name().expect("the target's name"),
        )
        .env("CARGO_NET_RETRY", "0")
        .output()
        .expect("the deployment test");
    assert!(
        output.status.success(),
        "relative CARGO_TARGET_DIR could not fetch the mirror:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed;"),
        "the child must run the deployment test: {output:?}"
    );
}

/// Test processes that start together build the mirror once between
/// them: each builds its own copy beside the mirror's path and renames it
/// into place, and a process whose rename finds the mirror there uses
/// that one. The commit has a fixed date, so a mirror built again, after
/// the target directory is cleaned, has the same commit, of which Cargo
/// already keeps a checkout.
#[test]
fn processes_that_start_together_share_one_mirror() {
    let pinned = pinned_rev();
    let parent = tempfile::Builder::new()
        .prefix("gdl-mirrors-")
        .tempdir()
        .expect("a directory");
    let built: Vec<(std::path::PathBuf, String)> = std::thread::scope(|scope| {
        let builders: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    let repo = mirror_repo(parent.path(), &pinned);
                    let rev = head(&repo);
                    (repo, rev)
                })
            })
            .collect();
        builders
            .into_iter()
            .map(|builder| builder.join().expect("a builder"))
            .collect()
    });
    assert!(built.iter().all(|one| *one == built[0]), "{built:?}");
    let left: Vec<_> = std::fs::read_dir(parent.path())
        .expect("the mirrors")
        .map(|entry| entry.expect("an entry").path())
        .collect();
    assert_eq!(left, [built[0].0.clone()], "one mirror and no copy left");

    // Git dates a commit to the second: two builds a second apart would
    // differ without the fixed date.
    assert_eq!(
        run_git(
            &built[0].0,
            &["show", "--no-patch", "--format=%at %ct", "HEAD"]
        ),
        "1767225600 1767225600"
    );
}

/// The paths of the pinned commit that the mirror holds.
const MIRRORED: [&str; 8] = [
    "Cargo.toml",
    "crates/gdl",
    "crates/gdl-macros",
    "crates/gdl-message",
    "crates/gdl-model",
    "crates/gdl-syntax",
    "examples/api",
    "examples/chat",
];

/// The mirror's repository for the pinned commit `pinned`, in `parent`.
/// Its one commit holds what a fetch of the examples' packages reads from
/// the pinned commit ([`MIRRORED`]): not the whole pinned tree, 9 MB, of
/// which Cargo would keep a checkout. It is built beside its path and
/// renamed into place, so a test never reads one half built, and tests
/// that build it at once agree on one.
fn mirror_repo(parent: &Path, pinned: &str) -> std::path::PathBuf {
    use std::hash::{Hash, Hasher};
    // Named for what it holds, so that a change to it builds a new one.
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    MIRRORED.hash(&mut hash);
    // The mirror's URL names its path, which must be absolute, where
    // `CARGO_TARGET_DIR` may be relative.
    let parent = std::path::absolute(parent).expect("an absolute path");
    let repo = parent.join(format!("{pinned}-{:016x}", hash.finish()));
    if repo.exists() {
        return repo;
    }
    std::fs::create_dir_all(&parent).expect("the mirrors' directory");
    let building = tempfile::Builder::new()
        .prefix(".building-")
        .tempdir_in(&parent)
        .expect("a directory");
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let git = |args: &[&str]| run_git(building.path(), args);
    git(&["init", "--quiet"]);
    // The pinned commit alone, from this repository's history.
    let origin = format!("file://{}", root.display());
    git(&["fetch", "--quiet", "--depth", "1", &origin, pinned]);
    git(&["checkout", "--quiet", pinned, "--"]
        .into_iter()
        .chain(MIRRORED)
        .collect::<Vec<_>>());
    git(&["commit", "--quiet", "--message", "the pinned examples"]);
    match std::fs::rename(building.path(), &repo) {
        // Its path is gone: nothing is left to remove.
        Ok(()) => {
            let _ = building.keep();
        }
        // Another test renamed its copy into place first.
        Err(_) if repo.exists() => {}
        Err(e) => panic!("{}: {e}", repo.display()),
    }
    repo
}

/// The commit `repo` has checked out.
fn head(repo: &Path) -> String {
    run_git(repo, &["rev-parse", "HEAD"])
}

/// Run `git args` in `dir`, and return what it printed. Its commits have a
/// fixed author and date, so one tree always makes one commit.
fn run_git(dir: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .current_dir(dir)
        .args(["-c", "user.name=gdl", "-c", "user.email=gdl@localhost"])
        .args(["-c", "commit.gpgsign=false"])
        .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
        .args(args)
        .output()
        .expect("git");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// The pinned commit carries what generated crates depend on, and the
/// example package comes from the same commit. A pin that predates
/// `gdl-runtime` compiles here, where only text is compared, and fails
/// when cargo resolves the generated manifests in the image build. This
/// reads the local repository. No test fetches from the mirror itself
/// ([`Mirror`]), so none fails for a pin that was never pushed to it.
#[test]
fn the_deployment_pin_carries_the_runtime() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let rev = pinned_rev();
    let text = std::fs::read_to_string(root.join("examples/api/deploy.toml")).expect("deploy spec");
    assert_eq!(
        text.matches(&format!("rev = \"{rev}\"")).count(),
        3,
        "{text}"
    );
    let show = std::process::Command::new("git")
        .arg("-C")
        .arg(&root)
        .args([
            "cat-file",
            "-e",
            &format!("{rev}:crates/gdl-runtime/Cargo.toml"),
        ])
        .status()
        .expect("git");
    assert!(show.success(), "{rev} has no crates/gdl-runtime");
}

/// The pin is on the public mirror that `examples/api/deploy.toml` names,
/// which every other test here replaces with [`Mirror`]: the example
/// package and `gdl-runtime`, which the generated segments take from it,
/// resolve there at the pin. Run it after moving the pin, with
/// `cargo nextest run -p gdl-gen --test deployment --run-ignored only -E 'test(=the_pin_is_on_the_public_mirror)'`.
#[test]
#[ignore = "needs network"]
fn the_pin_is_on_the_public_mirror() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/deploy.toml")).expect("deploy spec");
    let spec: toml::Table = toml::from_str(&text).expect("deploy spec");
    let url = spec["gdl"]["git"].as_str().expect("[gdl] git");
    let rev = pinned_rev();
    let target = tempfile::Builder::new()
        .prefix("gdl-public-mirror-")
        .tempdir()
        .expect("a directory");
    for package in ["api-frontend", "gdl-runtime"] {
        let workspace = gdl_extract::Workspace::fetch_git(
            package,
            url,
            &rev,
            target.path(),
            &gdl_extract::Cargo::trusted(),
        )
        .unwrap_or_else(|e| panic!("{package} at {rev} from {url}: {e}"));
        let located = workspace.locate(package).expect("the package");
        assert!(
            located.source.starts_with(&format!("git+{url}"))
                && located.source.ends_with(&format!("#{rev}")),
            "{package}: {}",
            located.source
        );
    }
}

fn portable_spec(mirror: &Mirror) -> Result<gdl_gen::Spec, gdl_gen::Error> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
    gdl_gen::Spec::from_text(&mirror.portable(&text), &graph, &root.join("examples/api"))
}

/// Only a spec that deploys asks for `gdl-runtime`'s `hub` feature, which
/// brings the hub client and its HTTP stack: a spec without `[deployment]`
/// generates segments that build without them.
#[test]
fn only_a_spec_with_deployment_turns_on_the_hub_feature() {
    let mirror = Mirror::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
    let text = mirror.portable(&text);
    assert!(!text.contains("[deployment]"), "{text}");
    for (text, hub) in [(text.clone(), false), (text + "[deployment]\n", true)] {
        let spec =
            gdl_gen::Spec::from_text(&text, &graph, &root.join("examples/api")).expect("a spec");
        let temp = tempfile::Builder::new()
            .prefix("gdl-hub-feature-")
            .tempdir()
            .expect("a directory");
        let files = gdl_gen::render(&spec, temp.path(), &mirror.target()).expect("rendered");
        for segment in ["front", "engine"] {
            let manifest: toml::Table =
                toml::from_str(&files[&Path::new(segment).join("Cargo.toml")]).expect("toml");
            let runtime = &manifest["dependencies"]["gdl-runtime"];
            let features: Vec<&str> = runtime
                .get("features")
                .and_then(|features| features.as_array())
                .map_or(Vec::new(), |features| {
                    features.iter().filter_map(|f| f.as_str()).collect()
                });
            assert_eq!(features.contains(&"hub"), hub, "{segment}: {runtime}");
        }
    }
}

/// A spec's classes reach the hubs' `deployment.json` and every segment's
/// embedded placement, so all of them agree on the classed hash. A spec
/// with classes is composed with its fleet.
#[test]
fn a_classed_bundle_agrees_on_the_placement_hash() {
    let mirror = Mirror::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
    let classes =
        "\n[class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"engine\"]\n";
    let spec = gdl_gen::Spec::from_text(
        &format!("{}{classes}", mirror.portable(&text)),
        &graph,
        &root.join("examples/api"),
    )
    .expect("a classed spec");
    let expected = gdl_model::Placement::from_toml(&format!(
        "[segment.front]\nnodes = [\"Http\"]\n[segment.engine]\nnodes = [\"Engine\"]\n{classes}"
    ))
    .expect("a placement")
    .hash();
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-classes-")
        .tempdir()
        .expect("a directory");
    let fleet = gdl_model::Fleet::from_toml(ONE_FLEET).expect("a fleet");
    let files = gdl_gen::compose_files(
        &spec,
        Some(&fleet),
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    let deployment: gdl_model::DeploymentSpec =
        serde_json::from_str(&files[Path::new("deployment.json")]).expect("the hubs' spec");
    assert_eq!(deployment.placement.hash(), expected);
    let mut embedded = 0;
    for (path, text) in &files {
        let Some((_, rest)) = text.split_once("    placement: r#\"") else {
            continue;
        };
        let (placement, _) = rest.split_once("\"#;").expect("a raw string");
        let placement = gdl_model::Placement::from_toml(placement).expect("an embedded placement");
        assert_eq!(placement.hash(), expected, "{}", path.display());
        embedded += 1;
    }
    assert_eq!(embedded, 2, "one embedded placement per segment");
}

/// `files`, every file of a bundle, as one text: each file under a line
/// that names it, with the mirror's URL and commit, which move, as `<url>`
/// and `<rev>`.
fn pinned(mirror: &Mirror, files: &gdl_gen::Files) -> String {
    files
        .iter()
        .map(|(path, text)| {
            format!(
                "==> {} <==\n{}\n",
                path.display(),
                text.replace(&mirror.url, "<url>")
                    .replace(&mirror.rev, "<rev>")
            )
        })
        .collect()
}

/// A spec without classes composes as it always has, byte for byte: every
/// file of the bundle is pinned, so that a bundle for cells changes
/// nothing of one without them.
#[test]
fn a_bundle_without_classes_is_pinned() {
    let mirror = Mirror::new();
    let spec = portable_spec(&mirror).expect("valid test fixture");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-pin-")
        .tempdir()
        .expect("a directory");
    let files = gdl_gen::compose_files(
        &spec,
        None,
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    let expected = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bundle/api.txt"),
    )
    .expect("the pinned bundle");
    let found = pinned(&mirror, &files);
    if std::env::var_os("GDL_UPDATE_FIXTURES").is_some() {
        let pin = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bundle/api.txt");
        std::fs::write(pin, &found).expect("the pinned bundle");
        return;
    }
    for (found, expected) in found.lines().zip(expected.lines()) {
        assert_eq!(found, expected, "the bundle differs from the pin");
    }
    assert_eq!(found, expected, "the bundle differs from the pin");
}

/// Every hub's health check is its own liveness, never the cell health
/// endpoint, and every service waits only for its own cell's hub, given by
/// `hub_of`, which names a hub for itself. A parent cell's health answers 503 until its children are
/// ready, so a wait on it, or on another cell, would deadlock a fleet that
/// starts together.
fn assert_liveness(compose: &serde_json::Value, hub_of: impl Fn(&str) -> String) {
    let text = compose.to_string();
    assert!(!text.contains("/v1/ext/gdl-cell/health"), "{text}");
    let services = compose["services"].as_object().expect("services");
    // A hub waits for nothing: it is its own cell's hub.
    let hubs: Vec<&String> = services
        .keys()
        .filter(|name| hub_of(name) == **name)
        .collect();
    for (name, service) in services {
        if hubs.contains(&name) {
            assert_eq!(
                service["healthcheck"]["test"],
                serde_json::json!(["CMD", "/usr/local/bin/segment", "--healthcheck"]),
                "{name}"
            );
            assert!(service.get("depends_on").is_none(), "{name}: {service}");
            continue;
        }
        let depends = service["depends_on"].as_object().expect("depends_on");
        assert_eq!(
            depends.keys().collect::<Vec<_>>(),
            [&hub_of(name)],
            "{name} waits only for its own cell's hub"
        );
        assert_eq!(depends[&hub_of(name)]["condition"], "service_healthy");
    }
}

#[test]
fn a_bundle_without_classes_waits_only_on_liveness() {
    let mirror = Mirror::new();
    let spec = portable_spec(&mirror).expect("valid test fixture");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-live-")
        .tempdir()
        .expect("a directory");
    let files = gdl_gen::compose_files(
        &spec,
        None,
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("compose.yaml");
    assert_liveness(&compose, |_| "hub".into());
}

/// The api graph's two segments, as `examples/api/gdl.toml` places them.
const SEGMENTS: &str =
    "[segment.front]\nnodes = [\"Http\"]\n[segment.engine]\nnodes = [\"Engine\"]\n";

/// The api graph in two classes: `global` runs the frontend, and sends to
/// `model_x`, which runs the engine.
const SPLIT: &str = "[segment.front]\nnodes = [\"Http\"]\n[segment.engine]\nnodes = [\"Engine\"]\n\
    [class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"engine\"]\n";

/// The api graph in one segment and one class, which sends to none.
const WHOLE: &str = "[segment.front]\nnodes = [\"Http\", \"Engine\"]\n\
    [class.global]\nsegments = [\"front\"]\n";

/// A global cell and two model cells of [`SPLIT`], the second larger.
const ONE_FLEET: &str = "[class.global]\nready = { front = 1 }\n[class.model_x]\nready = { engine = 1 }\n\
    [cell.global-us]\nclass = \"global\"\nregion = \"us-east-1\"\n\
    [cell.model-x-01]\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n\
    [cell.model-x-02]\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\nready = { engine = 2 }\n";

/// Three cells of layout `split` and one of `whole`.
const TWO_FLEET: &str = "[cell.global-us]\nlayout = \"split\"\nclass = \"global\"\nregion = \"us-east-1\"\n\
    [cell.model-x-us]\nlayout = \"split\"\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n\
    [cell.model-x-us-2]\nlayout = \"split\"\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\nready = { engine = 3 }\n\
    [cell.global-eu]\nlayout = \"whole\"\nclass = \"global\"\nregion = \"eu-west-1\"\n";

/// `text`, a placement, as the tables of layout `name`.
fn nest(name: &str, text: &str) -> String {
    text.lines()
        .map(|line| match line.strip_prefix('[') {
            Some(header) => format!("[layout.{name}.{header}\n"),
            None => format!("{line}\n"),
        })
        .collect()
}

/// The api example on the pinned commit, placed as `placement` says.
fn placed(mirror: &Mirror, placement: &str) -> gdl_gen::Spec {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
    let text = mirror.portable(&text);
    assert!(text.contains(SEGMENTS), "{text}");
    gdl_gen::Spec::from_text(
        &text.replace(SEGMENTS, placement),
        &graph,
        &root.join("examples/api"),
    )
    .expect("a spec")
}

/// [`SPLIT`] as layout `split` and [`WHOLE`] as layout `whole`.
fn two_layouts(mirror: &Mirror) -> gdl_gen::Spec {
    placed(mirror, &(nest("split", SPLIT) + &nest("whole", WHOLE)))
}

/// The bundle of `spec` for `fleet`: its files and its `compose.yaml`.
fn fleet_bundle(
    mirror: &Mirror,
    spec: &gdl_gen::Spec,
    fleet: &gdl_model::Fleet,
) -> (gdl_gen::Files, serde_json::Value) {
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-fleet-")
        .tempdir()
        .expect("a directory");
    let files = gdl_gen::compose_files(
        spec,
        Some(fleet),
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    let compose = serde_json::from_str(&files[Path::new("compose.yaml")]).expect("compose.yaml");
    (files, compose)
}

/// The hub that `service` of a fleet's bundle waits for: its cell's, the
/// service named for the cell.
fn cell_hub(service: &str) -> String {
    service
        .split_once('_')
        .map_or(service, |(cell, _)| cell)
        .into()
}

/// The paths of a bundle's Dockerfiles.
fn dockerfiles(files: &gdl_gen::Files) -> Vec<String> {
    files
        .keys()
        .filter(|path| path.ends_with("Dockerfile"))
        .map(|path| path.display().to_string())
        .collect()
}

/// The services of `compose` that run `image`, and those of them that
/// build it.
fn running<'a>(compose: &'a serde_json::Value, image: &str) -> (Vec<&'a str>, Vec<&'a str>) {
    let services = compose["services"].as_object().expect("services");
    let run: Vec<&str> = services
        .iter()
        .filter(|(_, service)| service["image"] == image)
        .map(|(name, _)| name.as_str())
        .collect();
    let build = run
        .iter()
        .copied()
        .filter(|name| services[*name].get("build").is_some())
        .collect();
    (run, build)
}

/// A fleet's bundle shares no volume: each child cell's hub reports its
/// record to its parent's hub, so its cells may run on several hosts. Each
/// parent cell's hub, and only a parent's, mounts a volume of its own,
/// where it keeps its children's records across its restart.
#[test]
fn each_parent_hub_keeps_its_childrens_records_in_a_volume_of_its_own() {
    let mirror = Mirror::new();
    for (spec, fleet, parents) in [
        (placed(&mirror, SPLIT), ONE_FLEET, &["global-us"][..]),
        // `global-eu` runs layout `whole`, in which no class sends to
        // another, so it has no children.
        (two_layouts(&mirror), TWO_FLEET, &["global-us"][..]),
    ] {
        let fleet = gdl_model::Fleet::from_toml(fleet).expect("a fleet");
        let (files, compose) = fleet_bundle(&mirror, &spec, &fleet);
        // The hub's image has the directory, writable by any user, so
        // that a new volume mounted on it is too
        // (`relay::a_parent_can_store_a_record_in_its_new_volume`).
        for (path, text) in &files {
            if path.ends_with("hub/Dockerfile") {
                assert!(
                    text.contains(
                        "mkdir -p /build-tools/state/gdl-hub && chmod 1777 /build-tools/state/gdl-hub"
                    ) && text.ends_with("COPY --from=build /build-tools/state/ /var/lib/\n"),
                    "{}: {text}",
                    path.display()
                );
            }
        }
        let volumes: Vec<String> = parents.iter().map(|cell| format!("{cell}-state")).collect();
        let declared: Vec<&String> = compose["volumes"]
            .as_object()
            .expect("the volumes")
            .keys()
            .collect();
        assert_eq!(declared, volumes.iter().collect::<Vec<_>>(), "{compose}");
        for (name, service) in compose["services"].as_object().expect("services") {
            let state = &service["environment"]["GDL_CELL__STATE_DIR"];
            if parents.contains(&name.as_str()) {
                assert_eq!(
                    service["volumes"],
                    serde_json::json!([format!("{name}-state:/var/lib/gdl-hub")]),
                    "{name}"
                );
                assert_eq!(state, "/var/lib/gdl-hub", "{name}");
            } else {
                assert!(service.get("volumes").is_none(), "{name}: {service}");
                // A parent's segments read the hub's `[cell]`, and nothing
                // else names a state directory.
                let parents_segment = parents
                    .iter()
                    .any(|parent| name.starts_with(&format!("{parent}_")));
                assert_eq!(state.is_string(), parents_segment, "{name}: {service}");
            }
        }
    }
}

/// A fleet of one layout: a hub per cell, and each cell's segments of its
/// class, configured as that cell and waiting only for its hub; each
/// child reporting to its parent's hub; one image per segment, which both
/// model cells run.
#[test]
fn a_fleet_of_one_layout_deploys_each_cell_with_its_own_hub() {
    let mirror = Mirror::new();
    let fleet = gdl_model::Fleet::from_toml(ONE_FLEET).expect("a fleet");
    let (files, compose) = fleet_bundle(&mirror, &placed(&mirror, SPLIT), &fleet);
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
        let hub = &services[&cell.name];
        assert_eq!(hub["image"], "api-hub", "{}", cell.name);
        let env = hub["environment"].as_object().expect("the hub's variables");
        assert_eq!(env["GDL_CELL__NAME"], cell.name.as_str());
        assert_eq!(env["GDL_CELL__CLASS"], cell.class.as_str());
        assert_eq!(env["GDL_CELL__DIRECTORY"], "relay");
        assert!(env.get("GDL_CELL__DIR").is_none(), "{env:?}");
        assert_eq!(env["GDL_CELL__HUB"], format!("{}:1337", cell.name));
        let segment = if cell.class == "global" {
            "front"
        } else {
            "engine"
        };
        let service = &services[&format!("{}_{segment}", cell.name)];
        assert_eq!(service["image"], format!("api-{segment}"));
        assert!(service.get("volumes").is_none(), "{service}");
        let own = service["environment"]
            .as_object()
            .expect("the segment's variables");
        assert_eq!(own["GDL_HUB__HOST"], cell.name.as_str());
        // A segment's `[cell]` is its hub's.
        let cell_vars = |vars: &serde_json::Map<String, serde_json::Value>| {
            vars.iter()
                .filter(|(name, _)| name.starts_with("GDL_CELL__"))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(cell_vars(own), cell_vars(env), "{}", cell.name);
    }
    let env = |cell: &str, key: &str| services[cell]["environment"][key].clone();
    assert_eq!(env("model-x-01", "GDL_CELL__READY__ENGINE"), "1");
    assert_eq!(env("model-x-02", "GDL_CELL__READY__ENGINE"), "2");
    assert_eq!(env("model-x-02", "GDL_CELL__HOME"), "global-us");
    assert_eq!(env("global-us", "GDL_CELL__HOME"), serde_json::Value::Null);
    // A child reports to its parent's hub, under the name of its service;
    // the global cell reports nowhere.
    assert_eq!(env("model-x-02", "GDL_CELL__PARENT_HUB"), "global-us:1337");
    assert_eq!(
        env("global-us", "GDL_CELL__PARENT_HUB"),
        serde_json::Value::Null
    );
    // One image per segment, built once, which both model cells run.
    assert_eq!(
        dockerfiles(&files),
        ["engine/Dockerfile", "front/Dockerfile", "hub/Dockerfile"]
    );
    assert_eq!(
        running(&compose, "api-engine"),
        (
            vec!["model-x-01_engine", "model-x-02_engine"],
            vec!["model-x-01_engine"]
        )
    );
    assert_eq!(
        services["model-x-01_engine"]["build"]["dockerfile"],
        "engine/Dockerfile"
    );
    assert_eq!(running(&compose, "api-hub").1, ["global-us"]);
    assert!(files.contains_key(Path::new("deployment.json")));
    // An image's settings are every cell's: its cell comes from the
    // service's variables.
    for segment in ["front", "engine"] {
        let config = &files[&Path::new(segment).join("config.toml")];
        assert!(
            !config.contains("[hub]") && !config.contains("[cell]"),
            "{config}"
        );
    }
    assert_liveness(&compose, cell_hub);
}

/// A fleet whose cells run two layouts builds each layout's images once,
/// each from its own layout's crates, and each cell runs its own layout's
/// segments and its own layout's hub.
#[test]
fn a_fleet_of_two_layouts_builds_each_layouts_images_once() {
    let mirror = Mirror::new();
    let fleet = gdl_model::Fleet::from_toml(TWO_FLEET).expect("a fleet");
    let (files, compose) = fleet_bundle(&mirror, &two_layouts(&mirror), &fleet);
    let services = compose["services"].as_object().expect("services");
    assert_eq!(
        services.keys().collect::<Vec<_>>(),
        [
            "global-eu",
            "global-eu_front",
            "global-us",
            "global-us_front",
            "model-x-us",
            "model-x-us-2",
            "model-x-us-2_engine",
            "model-x-us_engine"
        ]
    );
    assert_eq!(
        dockerfiles(&files),
        [
            "split/engine/Dockerfile",
            "split/front/Dockerfile",
            "split/hub/Dockerfile",
            "whole/front/Dockerfile",
            "whole/hub/Dockerfile"
        ]
    );
    for (image, run, dockerfile) in [
        (
            "api-hub:split",
            &["global-us", "model-x-us", "model-x-us-2"][..],
            "split/hub/Dockerfile",
        ),
        ("api-hub:whole", &["global-eu"], "whole/hub/Dockerfile"),
        (
            "api-split-front",
            &["global-us_front"],
            "split/front/Dockerfile",
        ),
        (
            "api-split-engine",
            &["model-x-us-2_engine", "model-x-us_engine"],
            "split/engine/Dockerfile",
        ),
        (
            "api-whole-front",
            &["global-eu_front"],
            "whole/front/Dockerfile",
        ),
    ] {
        let (running, building) = running(&compose, image);
        assert_eq!(running, run, "{image}");
        assert_eq!(building.len(), 1, "{image} is built once");
        assert_eq!(services[building[0]]["build"]["dockerfile"], dockerfile);
    }
    // Each image builds its own layout's crate, with its own settings.
    for (layout, segment) in [("split", "front"), ("split", "engine"), ("whole", "front")] {
        let dockerfile = &files[&Path::new(layout).join(segment).join("Dockerfile")];
        for needle in [
            format!("--manifest-path '/app/{layout}/{segment}/Cargo.toml'"),
            format!("COPY --from=build /build/release/api-{layout}-{segment} "),
            format!("COPY [\"./{layout}/{segment}/config.toml\", \"/etc/gdl/config\"]"),
        ] {
            assert!(dockerfile.contains(&needle), "{needle}\nin:\n{dockerfile}");
        }
    }
    // Each layout's hub admits its own layout's placement.
    for (layout, placement) in [("split", SPLIT), ("whole", WHOLE)] {
        let hub = &files[&Path::new(layout).join("hub/Dockerfile")];
        assert!(
            hub.contains(&format!(
                "COPY [\"./{layout}/deployment.json\", \"/etc/gdl/config\"]"
            )),
            "{hub}"
        );
        let deployment: gdl_model::DeploymentSpec =
            serde_json::from_str(&files[&Path::new(layout).join("deployment.json")])
                .expect("the hub's spec");
        let expected = gdl_model::Placement::from_toml(placement)
            .expect("a placement")
            .hash();
        assert_eq!(deployment.placement.hash(), expected, "{layout}");
    }
    assert!(!files.contains_key(Path::new("deployment.json")));
    assert_eq!(
        services["global-eu"]["environment"]["GDL_CELL__LAYOUT"],
        "whole"
    );
    assert_liveness(&compose, cell_hub);
}

/// No hub can be scaled, and the bundle's README says why: a cell's record
/// has one writer.
#[test]
fn no_hub_can_be_scaled() {
    let mirror = Mirror::new();
    for (spec, fleet) in [
        (placed(&mirror, SPLIT), ONE_FLEET),
        (two_layouts(&mirror), TWO_FLEET),
    ] {
        let fleet = gdl_model::Fleet::from_toml(fleet).expect("a fleet");
        let (files, compose) = fleet_bundle(&mirror, &spec, &fleet);
        let services = compose["services"].as_object().expect("services");
        let mut names = Vec::new();
        for (name, service) in services {
            let fixed = service.get("container_name");
            if fleet.cell(name).is_some() {
                // Compose refuses to scale a service whose container is named.
                names.push(fixed.expect("a hub's container name").clone());
                assert!(service.get("deploy").is_none() && service.get("scale").is_none());
            } else {
                assert!(fixed.is_none(), "{name} can be scaled");
            }
        }
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count, "{names:?}");
        assert_eq!(count, fleet.cells().count());
        let readme = &files[Path::new("README.md")];
        for needle in [
            "must never run as two at once",
            "a cell's record has one writer",
            "stops the old container before it starts the new one",
            "share no volume",
            "`relay`",
            "still holds them after it restarts",
        ] {
            assert!(readme.contains(needle), "{needle}\nin:\n{readme}");
        }
    }
}

/// The fleet guide reads a cell's health and drains it from a helper
/// container on the bundle's network (`docs/src/guides/fleet.md`). That
/// works only while no hub publishes a host port, and while every service
/// is on Compose's default network, which Compose names `<project>_default`
/// with the project named for the bundle's directory: no top-level `name`,
/// no network of the bundle's own, and no service on another.
#[test]
fn a_fleets_hubs_publish_no_host_port_and_share_the_default_network() {
    let mirror = Mirror::new();
    for (spec, fleet) in [
        (placed(&mirror, SPLIT), ONE_FLEET),
        (two_layouts(&mirror), TWO_FLEET),
    ] {
        let fleet = gdl_model::Fleet::from_toml(fleet).expect("a fleet");
        let (_, compose) = fleet_bundle(&mirror, &spec, &fleet);
        // Only these change the project's name or its network; other
        // top-level keys, such as a local bundle's `x-gdl-local-sources`,
        // do not.
        for key in ["name", "networks"] {
            assert!(compose.get(key).is_none(), "{key}: {compose}");
        }
        let services = compose["services"].as_object().expect("services");
        let mut hubs = 0;
        for (name, service) in services {
            for key in ["networks", "network_mode"] {
                assert!(service.get(key).is_none(), "{name}: {service}");
            }
            if fleet.cell(name).is_some() {
                hubs += 1;
                assert!(service.get("ports").is_none(), "{name}: {service}");
            }
        }
        assert_eq!(hubs, fleet.cells().count());
    }
}

/// Each hub's `GDL_CELL__*` variables, read as the hub reads them against
/// its layout's deployment, are its cell's settings as the fleet gives
/// them, with the address other hubs reach it at and, for a child, its
/// parent's: the `relay` backend, which needs no shared volume.
#[test]
fn each_hub_reads_its_cells_settings_as_the_fleet_gives_them() {
    let mirror = Mirror::new();
    assert_each_hub_reads_its_cell(&mirror, &placed(&mirror, SPLIT), ONE_FLEET);
    assert_each_hub_reads_its_cell(&mirror, &two_layouts(&mirror), TWO_FLEET);
}

/// A segment's name may hold `__`, which the environment's keys use
/// between nested keys: its quorum still reads back as the fleet gives it.
#[test]
fn a_quorum_of_a_segment_whose_name_holds_a_separator_reads_back() {
    let mirror = Mirror::new();
    let split = SPLIT
        .replace("[segment.engine]", "[segment.engine__work]")
        .replace("\"engine\"", "\"engine__work\"");
    let fleet = ONE_FLEET.replace("engine =", "engine__work =");
    assert!(split.contains("engine__work") && fleet.contains("engine__work"));
    assert_each_hub_reads_its_cell(&mirror, &placed(&mirror, &split), &fleet);
}

/// A name that reads as another type when it is written bare, such as a
/// cell named `true`, reads back as the name it is, in the hub's
/// variables and in its segments' `GDL_HUB__HOST`.
#[test]
fn a_name_that_reads_as_another_type_reads_back_as_written() {
    let mirror = Mirror::new();
    let fleet = ONE_FLEET.replace("global-us", "true");
    assert!(fleet.contains("[cell.true]") && fleet.contains("home = \"true\""));
    assert_each_hub_reads_its_cell(&mirror, &placed(&mirror, SPLIT), &fleet);
}

/// The check of [`each_hub_reads_its_cells_settings_as_the_fleet_gives_them`]
/// for `spec` and the fleet `fleet`. A segment's `GDL_HUB__HOST` is its
/// hub's name, written as the hub's own `GDL_CELL__NAME` is.
fn assert_each_hub_reads_its_cell(mirror: &Mirror, spec: &gdl_gen::Spec, fleet: &str) {
    let fleet = gdl_model::Fleet::from_toml(fleet).expect("a fleet");
    let (files, compose) = fleet_bundle(mirror, spec, &fleet);
    for cell in fleet.cells() {
        let deployment = cell
            .layout
            .as_ref()
            .map_or("deployment.json".into(), |layout| {
                format!("{layout}/deployment.json")
            });
        let deployment: gdl_model::DeploymentSpec =
            serde_json::from_str(&files[Path::new(&deployment)]).expect("the hub's spec");
        let env = compose["services"][&cell.name]["environment"]
            .as_object()
            .expect("the hub's variables");
        let vars = env
            .iter()
            .map(|(name, value)| (name.into(), value.as_str().expect("a string").into()));
        let read = gdl_hub::cell_from_vars(&deployment, vars)
            .unwrap_or_else(|error| panic!("{}: {error}", cell.name))
            .expect("a cell");
        let layout =
            gdl_model::layout(&deployment.graph, &deployment.placement).expect("the layout");
        let parent = layout
            .boundaries()
            .iter()
            .any(|boundary| boundary.from == cell.class);
        let mut expected = cell.clone();
        expected.directory = gdl_model::DirectoryBackend::Relay {
            parent_hub: cell.home.as_ref().map(|home| format!("{home}:1337")),
            state_dir: parent.then(|| "/var/lib/gdl-hub".into()),
        };
        expected.hub = Some(format!("{}:1337", cell.name));
        assert_eq!(read, expected);
        // Each of its segments reads the same `[cell]`, which fits an
        // instance of the segment, as the instance checks at its launch.
        let services = compose["services"].as_object().expect("services");
        let mut segments = 0;
        for (name, service) in services {
            let Some(segment) = name.strip_prefix(&format!("{}_", cell.name)) else {
                continue;
            };
            read.check(&layout, segment)
                .unwrap_or_else(|problems| panic!("{name}: {problems}"));
            assert_eq!(
                service["environment"]["GDL_HUB__HOST"], env["GDL_CELL__NAME"],
                "{name}"
            );
            segments += 1;
        }
        assert!(segments > 0, "{}", cell.name);
    }
}

/// `compose` without a fleet refuses a spec that deploys by cell, one with
/// classes or with several layouts, and says that it needs the fleet.
#[test]
fn compose_without_a_fleet_refuses_a_spec_that_deploys_by_cell() {
    let mirror = Mirror::new();
    for (spec, why) in [
        (placed(&mirror, SPLIT), "with classes"),
        (
            two_layouts(&mirror),
            "of several layouts, `split` and `whole`,",
        ),
    ] {
        let temp = tempfile::Builder::new()
            .prefix("gdl-compose-unfleeted-")
            .tempdir()
            .expect("a directory");
        let out = temp.path().join("bundle");
        let error = gdl_gen::compose(
            &spec,
            None,
            &out,
            &mirror.target(),
            gdl_gen::Sources::Fetched,
        )
        .expect_err("a spec that deploys by cell")
        .to_string();
        assert_eq!(
            error,
            format!(
                "`compose` deploys a spec {why} by cell, as its fleet file says: give it `--fleet <fleet.toml>`"
            )
        );
        assert!(!out.exists());
    }
}

/// A fleet that `check` refuses, or that has no cells, deploys nothing,
/// and `compose` writes nothing.
#[test]
fn compose_refuses_a_fleet_that_check_refuses_and_writes_nothing() {
    let mirror = Mirror::new();
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-refused-")
        .tempdir()
        .expect("a directory");
    let out = temp.path().join("bundle");
    let childless = "[cell.global-us]\nclass = \"global\"\nregion = \"us-east-1\"\n";
    for (fleet, expected) in [
        (
            childless,
            "[cell.global-us]: no child of class `model_x` in layout `default`",
        ),
        ("", "the fleet has no cells, so there is nothing to deploy"),
    ] {
        let fleet = gdl_model::Fleet::from_toml(fleet).expect("a fleet");
        let error = gdl_gen::compose(
            &placed(&mirror, SPLIT),
            Some(&fleet),
            &out,
            &mirror.target(),
            gdl_gen::Sources::Fetched,
        )
        .expect_err("a refused fleet")
        .to_string();
        assert!(error.contains(expected), "{error}");
        assert!(!out.exists());
    }
}

/// A segment that publishes host ports cannot run in two cells on one
/// host: the second container could not bind them.
#[test]
fn a_segment_that_publishes_ports_runs_in_one_cell() {
    let mirror = Mirror::new();
    let ports = "[deployment.segment.front]\nports = [\"8082:8082\"]\n";
    let spec = placed(
        &mirror,
        &(nest("split", SPLIT) + &nest("whole", WHOLE) + ports),
    );
    let fleet = gdl_model::Fleet::from_toml(TWO_FLEET).expect("a fleet");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-ports-")
        .tempdir()
        .expect("a directory");
    let error = gdl_gen::compose_files(
        &spec,
        Some(&fleet),
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect_err("two cells publish one port")
    .to_string();
    assert!(
        error.contains(
            "segment `front` publishes host port `8082:8082`, and runs in cells `global-eu`, `global-us`"
        ),
        "{error}"
    );
    // In one cell it publishes them.
    let one = gdl_model::Fleet::from_toml(
        "[cell.global-eu]\nlayout = \"whole\"\nclass = \"global\"\nregion = \"eu-west-1\"\n",
    )
    .expect("a fleet");
    let (_, compose) = fleet_bundle(&mirror, &spec, &one);
    assert_eq!(
        compose["services"]["global-eu_front"]["ports"],
        serde_json::json!(["8082:8082"])
    );
}

/// A port that Docker gives a host port of its own choosing binds nothing
/// fixed, so a segment that publishes only such ports runs in several
/// cells, each container with its own host port.
#[test]
fn a_segment_that_publishes_only_dynamic_host_ports_runs_in_several_cells() {
    let mirror = Mirror::new();
    let ports =
        "[deployment.segment.engine]\nports = [\"9000\", \"127.0.0.1::9001\", \"9002/udp\"]\n";
    let spec = placed(&mirror, &format!("{SPLIT}{ports}"));
    let fleet = gdl_model::Fleet::from_toml(ONE_FLEET).expect("a fleet");
    let (_, compose) = fleet_bundle(&mirror, &spec, &fleet);
    for service in ["model-x-01_engine", "model-x-02_engine"] {
        assert_eq!(
            compose["services"][service]["ports"],
            serde_json::json!(["9000", "127.0.0.1::9001", "9002/udp"]),
            "{service}"
        );
    }
    // A fixed host port among them is still refused.
    for fixed in ["9000:9000", "127.0.0.1:9000:9000", "[::1]:9000:9000/tcp"] {
        let ports = format!("[deployment.segment.engine]\nports = [\"9001\", \"{fixed}\"]\n");
        let spec = placed(&mirror, &format!("{SPLIT}{ports}"));
        let temp = tempfile::Builder::new()
            .prefix("gdl-compose-ports-")
            .tempdir()
            .expect("a directory");
        let error = gdl_gen::compose_files(
            &spec,
            Some(&fleet),
            temp.path(),
            &mirror.target(),
            gdl_gen::Sources::Fetched,
        )
        .expect_err("two cells bind one host port")
        .to_string();
        assert!(
            error.contains(&format!(
                "segment `engine` publishes host port `{fixed}`, and runs in cells `model-x-01`, `model-x-02`"
            )),
            "{error}"
        );
    }
}

/// A host port of 0 asks Docker for a free one, as a port without a host
/// port does, so it too runs in several cells.
#[test]
fn a_host_port_of_zero_lets_docker_choose() {
    let mirror = Mirror::new();
    let ports = "[deployment.segment.engine]\nports = [\"0:9000\", \"127.0.0.1:0:9000\", \"[::1]:0:9000/tcp\"]\n";
    let spec = placed(&mirror, &format!("{SPLIT}{ports}"));
    let fleet = gdl_model::Fleet::from_toml(ONE_FLEET).expect("a fleet");
    let (_, compose) = fleet_bundle(&mirror, &spec, &fleet);
    for service in ["model-x-01_engine", "model-x-02_engine"] {
        assert_eq!(
            compose["services"][service]["ports"],
            serde_json::json!(["0:9000", "127.0.0.1:0:9000", "[::1]:0:9000/tcp"]),
            "{service}"
        );
    }
}

/// `cargo gdl compose --fleet` checks the fleet, then writes its bundle.
#[test]
fn the_cli_composes_a_fleet() {
    let mirror = Mirror::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-cli-")
        .tempdir()
        .expect("a directory");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let text = mirror.portable(&text).replace(SEGMENTS, SPLIT);
    std::fs::write(temp.path().join("gdl.toml"), text).expect("the spec");
    std::fs::copy(
        root.join("examples/api/api.gdl"),
        temp.path().join("api.gdl"),
    )
    .expect("graph");
    std::fs::write(temp.path().join("fleet.toml"), ONE_FLEET).expect("the fleet");
    let out = temp.path().join("bundle");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "compose", "--spec"])
        .arg(temp.path().join("gdl.toml"))
        .arg("--fleet")
        .arg(temp.path().join("fleet.toml"))
        .arg("--out")
        .arg(&out)
        .arg("--target-dir")
        .arg(mirror.target())
        .output()
        .expect("cargo gdl");
    assert!(output.status.success(), "{output:?}");
    let compose: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.join("compose.yaml")).expect("compose.yaml"),
    )
    .expect("compose.yaml");
    for cell in ["global-us", "model-x-01", "model-x-02"] {
        assert!(compose["services"][cell].is_object(), "{compose}");
    }
}

/// What a secret's file holds in these tests: no generated file, and no
/// output of the generator, may hold it.
const SENTINEL: &str = "gdl-secret-sentinel-5c0d";

/// `path` as a TOML string.
fn quoted(path: &Path) -> String {
    serde_json::to_string(path.to_str().expect("a UTF-8 path")).expect("a string")
}

/// Secrets `fingerprint`, from `fingerprint`, and `other`, from `other`,
/// both mounted in segment `segment`.
fn secrets(segment: &str, fingerprint: &Path, other: &Path) -> String {
    format!(
        "[deployment.secrets.fingerprint]\nfile = {}\n[deployment.secrets.other]\nfile = {}\n\
         [deployment.segment.{segment}]\nsecrets = [\"fingerprint\", \"other\"]\n",
        quoted(fingerprint),
        quoted(other)
    )
}

/// Every generated file that holds `text`.
fn holding(files: &gdl_gen::Files, text: &str) -> Vec<String> {
    files
        .iter()
        .filter(|(_, body)| body.contains(text))
        .map(|(path, _)| path.display().to_string())
        .collect()
}

/// A segment mounts each secret it names at `/run/secrets/<name>`, from
/// the file on the Docker host that the spec names. The generator never
/// opens the file: one that does not exist is written all the same, and
/// no generated file holds what one does hold. Compose reads `$` in a path
/// as a variable, so the path writes it `$$`.
#[test]
fn a_segment_mounts_the_secrets_it_names() {
    let mirror = Mirror::new();
    let keys = tempfile::Builder::new()
        .prefix("gdl-secrets-")
        .tempdir()
        .expect("a directory");
    let fingerprint = keys.path().join("fingerprint");
    std::fs::write(&fingerprint, SENTINEL).expect("the secret");
    let other = keys.path().join("absent$HOME");
    // The guide's recipe: the node reads the file where the secret is
    // mounted.
    let setting = "[deployment.segment.front.config.node.Fingerprint]\nsecret_file = \"/run/secrets/fingerprint\"\n";
    let spec = placed(
        &mirror,
        &format!(
            "{SEGMENTS}{}{setting}",
            secrets("front", &fingerprint, &other)
        ),
    );
    let out = tempfile::Builder::new()
        .prefix("gdl-secrets-out-")
        .tempdir()
        .expect("a directory");
    let files = gdl_gen::compose_files(
        &spec,
        None,
        &out.path().join("bundle"),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("compose.yaml");
    let other = other.to_str().expect("UTF-8").replace('$', "$$");
    assert_eq!(
        compose["secrets"],
        serde_json::json!({
            "fingerprint": {"file": fingerprint},
            "other": {"file": other},
        })
    );
    assert_eq!(
        compose["services"]["front"]["secrets"],
        serde_json::json!(["fingerprint", "other"])
    );
    assert!(
        compose["services"]["engine"].get("secrets").is_none(),
        "{compose}"
    );
    let config: toml::Table =
        toml::from_str(&files[Path::new("front/config.toml")]).expect("the settings");
    assert_eq!(
        config["node"]["Fingerprint"]["secret_file"].as_str(),
        Some("/run/secrets/fingerprint")
    );
    assert_eq!(holding(&files, SENTINEL), Vec::<String>::new());
    let readme = &files[Path::new("README.md")];
    assert!(readme.contains("/run/secrets/fingerprint"), "{readme}");
}

/// In a fleet's bundle, each cell's service of a segment mounts the
/// segment's secrets, and the bundle declares them beside its volumes.
#[test]
fn each_cells_segment_mounts_its_secrets() {
    let mirror = Mirror::new();
    let keys = tempfile::Builder::new()
        .prefix("gdl-secrets-")
        .tempdir()
        .expect("a directory");
    let spec = placed(
        &mirror,
        &format!(
            "{SPLIT}{}",
            secrets(
                "engine",
                &keys.path().join("fingerprint"),
                &keys.path().join("other")
            )
        ),
    );
    let fleet = gdl_model::Fleet::from_toml(ONE_FLEET).expect("a fleet");
    let (_, compose) = fleet_bundle(&mirror, &spec, &fleet);
    for service in ["model-x-01_engine", "model-x-02_engine"] {
        assert_eq!(
            compose["services"][service]["secrets"],
            serde_json::json!(["fingerprint", "other"]),
            "{service}"
        );
    }
    for service in ["global-us", "global-us_front", "model-x-01"] {
        assert!(
            compose["services"][service].get("secrets").is_none(),
            "{service}"
        );
    }
    assert_eq!(
        compose["secrets"]["fingerprint"]["file"],
        keys.path().join("fingerprint").to_str().expect("UTF-8")
    );
    // A secret is no volume: the only one is the parent's state.
    assert_eq!(
        compose["volumes"],
        serde_json::json!({"global-us-state": {}}),
        "{compose}"
    );
}

/// A secret is refused, before anything is written, when Compose cannot
/// mount it as one file at `/run/secrets/<name>`, when its segment names
/// one the spec does not declare, or when its file is relative or lies
/// in the bundle: the bundle is the build context of its images, so every
/// build would copy the file into an image. Each case but the names
/// checks the whole spec, as the other bundle tests do, so the test takes
/// as long as several of them.
#[test]
fn a_secret_that_cannot_be_mounted_safely_is_refused() {
    let mirror = Mirror::new();
    let keys = tempfile::Builder::new()
        .prefix("gdl-secrets-")
        .tempdir()
        .expect("a directory");
    let out = tempfile::Builder::new()
        .prefix("gdl-secrets-out-")
        .tempdir()
        .expect("a directory");
    let bundle = out.path().join("bundle");
    let file = quoted(&keys.path().join("key"));
    let cases = [
        (
            format!("[deployment.secrets.\"..\"]\nfile = {file}\n"),
            "[deployment.secrets...]",
        ),
        (
            format!("[deployment.secrets.\"a/b\"]\nfile = {file}\n"),
            "[deployment.secrets.a/b]",
        ),
        (
            "[deployment.secrets.key]\nfile = \"keys/key\"\n".to_string(),
            "[deployment.secrets.key]: file `keys/key` is not an absolute path",
        ),
        (
            format!(
                "[deployment.secrets.key]\nfile = {file}\n[deployment.segment.front]\nsecrets = [\"missing\"]\n"
            ),
            "[deployment.segment.front]: secret `missing` is not declared",
        ),
        (
            format!(
                "[deployment.secrets.key]\nfile = {}\n[deployment.segment.front]\nsecrets = [\"key\"]\n",
                quoted(&bundle.join("secrets/key"))
            ),
            "[deployment.secrets.key]: file",
        ),
    ];
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for (secrets, expected) in cases {
        let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
        let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
        let spec = gdl_gen::Spec::from_text(
            &format!("{}{secrets}", mirror.portable(&text)),
            &graph,
            &root.join("examples/api"),
        );
        let error = match spec {
            Err(error) => error,
            Ok(spec) => gdl_gen::compose(
                &spec,
                None,
                &bundle,
                &mirror.target(),
                gdl_gen::Sources::Fetched,
            )
            .expect_err(&format!("refused: {secrets}")),
        };
        assert!(error.to_string().contains(expected), "{secrets}: {error}");
        assert!(!bundle.exists(), "refusal must precede writing");
    }
}

/// `cargo gdl compose` neither prints a secret nor writes it: it never
/// opens the file.
#[test]
fn the_cli_never_prints_or_writes_a_secret() {
    let mirror = Mirror::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-cli-")
        .tempdir()
        .expect("a directory");
    let fingerprint = temp.path().join("fingerprint");
    std::fs::write(&fingerprint, SENTINEL).expect("the secret");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let text = format!(
        "{}{}",
        mirror.portable(&text),
        secrets("front", &fingerprint, &temp.path().join("other"))
    );
    std::fs::write(temp.path().join("gdl.toml"), text).expect("the spec");
    std::fs::copy(
        root.join("examples/api/api.gdl"),
        temp.path().join("api.gdl"),
    )
    .expect("graph");
    let out = temp.path().join("bundle");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "compose", "--spec"])
        .arg(temp.path().join("gdl.toml"))
        .arg("--out")
        .arg(&out)
        .arg("--target-dir")
        .arg(mirror.target())
        .output()
        .expect("cargo gdl");
    assert!(output.status.success(), "{output:?}");
    for stream in [&output.stdout, &output.stderr] {
        assert!(!String::from_utf8_lossy(stream).contains(SENTINEL));
    }
    let mut pending = vec![out.clone()];
    let mut read = 0;
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).expect("the bundle") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let bytes = std::fs::read(&path).expect("a file");
                assert!(
                    !String::from_utf8_lossy(&bytes).contains(SENTINEL),
                    "{}",
                    path.display()
                );
                read += 1;
            }
        }
    }
    assert!(read > 0);
    let compose = std::fs::read_to_string(out.join("compose.yaml")).expect("compose.yaml");
    assert!(compose.contains("\"secrets\""), "{compose}");
}

/// With Docker: a segment's container reads its secret at
/// `/run/secrets/<name>`, from a path that holds `$`. Compose mounts the
/// file as it is on the host, with its owner and mode, and ignores `uid`
/// and `mode` outside Swarm: so a file only its owner can read is
/// unreadable to the default image's user, 65532, until an ACL lets that
/// user read it. The override swaps only the image, user, and command:
/// the secrets are the bundle's as generated.
#[cfg(unix)]
#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_container_reads_its_secret() {
    let mirror = Mirror::new();
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    struct Project(String, std::path::PathBuf);
    impl Drop for Project {
        fn drop(&mut self) {
            let _ = Command::new("timeout")
                .args(["30", "docker", "compose", "--project-name", &self.0])
                .args(["down", "--remove-orphans"])
                .current_dir(&self.1)
                .output();
        }
    }

    let keys = tempfile::Builder::new()
        .prefix("gdl-secrets-")
        .tempdir()
        .expect("a directory");
    let fingerprint = keys.path().join("finger$print");
    std::fs::write(&fingerprint, SENTINEL).expect("the secret");
    std::fs::set_permissions(&fingerprint, std::fs::Permissions::from_mode(0o600))
        .expect("owner only");
    let spec = placed(
        &mirror,
        &format!(
            "{SEGMENTS}{}",
            secrets("front", &fingerprint, &keys.path().join("other"))
        ),
    );
    let temp = tempfile::Builder::new()
        .prefix("gdl-secrets-out-")
        .tempdir()
        .expect("a directory");
    let out = temp.path().join("bundle");
    gdl_gen::compose(
        &spec,
        None,
        &out,
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle");
    // `other` is not read here; Compose wants each mounted file to exist.
    std::fs::write(keys.path().join("other"), "").expect("the other secret");
    std::fs::write(
        out.join("compose.override.yaml"),
        "services:\n  front:\n    build: !reset null\n    depends_on: !reset {}\n    image: busybox:1.36\n    user: \"65532\"\n    entrypoint: [\"sh\", \"-c\", \"test \\\"$$(cat /run/secrets/fingerprint)\\\" = \\\"$$EXPECTED\\\"\"]\n",
    )
    .expect("the override");
    let project = Project(format!("gdl-secrets-{}", std::process::id()), out.clone());
    let run = || {
        Command::new("timeout")
            .args(["60", "docker", "compose", "--project-name", &project.0])
            .args(["run", "--rm", "--no-deps", "-e"])
            .arg(format!("EXPECTED={SENTINEL}"))
            .arg("front")
            .current_dir(&out)
            .output()
            .expect("docker compose run")
    };
    let denied = run();
    assert!(
        !denied.status.success(),
        "a file only its owner reads is unreadable to uid 65532"
    );
    let acl = Command::new("setfacl")
        .args(["-m", "u:65532:r"])
        .arg(&fingerprint)
        .output()
        .expect("setfacl");
    assert!(acl.status.success(), "{acl:?}");
    let read = run();
    assert!(
        read.status.success(),
        "the container reads its secret:\n{}",
        String::from_utf8_lossy(&read.stderr)
    );
}
