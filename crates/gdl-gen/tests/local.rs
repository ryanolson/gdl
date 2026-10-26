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
/// The api example split into classes, deployed by [`FLEET`] into `out`
/// from local sources.
fn local_fleet(out: &Path) -> (gdl_model::Fleet, gdl_gen::Files) {
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
        out,
        &target("gdl-gen-tests"),
        gdl_gen::Sources::Local,
    )
    .expect("a bundle");
    (fleet, files)
}

#[test]
fn a_local_fleet_builds_each_cells_images_from_local_sources() {
    let (fleet, files) = local_fleet(&root().join("target/gdl-local-fleet"));
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
/// which Docker does not split, and writes each space as a class, which
/// Docker does not trim.
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
                format!("./target/gdl-local-spaced/bundle[ ]with[ ]spaces/{config}"),
                "/etc/gdl/config".into()
            ],
            "{dockerfile}"
        );
    }
}

/// The name of a bundle directory that holds each character Compose or
/// Docker reads as syntax in a path: `$` for Compose, `*`, `?`, `[`, `]`,
/// and `\` for Docker's patterns, `^`, `!`, `+`, `(`, `)`, `{`, `}`, and
/// `|` for the regular expressions it builds from them, quotes and `$` for
/// the Dockerfile, and spaces, which Docker trims at the ends of a pattern.
const SYNTAX: &str = r#" bundle $X [dev] *?\ '" ^!+(a){2}| "#;

/// A bundle whose path holds Compose and Docker syntax names it literally
/// in each file that names it: `compose.yaml` doubles `$`, each `COPY`
/// and the ignore file write Docker's pattern characters as classes of
/// one escaped character and whitespace as a class, and the `COPY` also
/// escapes what the Dockerfile reads in a word. Paths that hold none of
/// them are written as they are
/// ([`a_local_bundle_builds_from_a_context_that_holds_its_crates`]).
#[test]
fn a_local_bundle_writes_compose_and_docker_syntax_in_its_path_literally() {
    let parent = "target/gdl-local-syntax";
    let out = root().join(parent).join(SYNTAX);
    let compose_path = format!("{parent}/{}", r#" bundle $$X [dev] *?\ '" ^!+(a){2}| "#);
    let copy_path = format!(
        "./{parent}/{}",
        r#"[ ]bundle[ ]\$X[ ][\\[]dev[\\]][ ][\\*][\\?][\\\\][ ]\'\"[ ][\\^][\\!][\\+][\\(]a[\\)][\\{]2[\\}][\\|][ ]"#
    );
    let ignore = format!(
        "!{parent}/{}\n",
        r#"[ ]bundle[ ]$X[ ][\[]dev[\]][ ][\*][\?][\\][ ]'"[ ][\^][\!][\+][\(]a[\)][\{]2[\}][\|][ ]"#
    );
    let copy = |files: &gdl_gen::Files, dockerfile: &str| -> String {
        let line = files[Path::new(dockerfile)]
            .lines()
            .find(|line| line.starts_with("COPY [") && line.contains("/etc/gdl/config"))
            .unwrap_or_else(|| panic!("{dockerfile} copies its settings"))
            .to_owned();
        let args: Vec<String> =
            serde_json::from_str(&line["COPY ".len()..]).expect("the JSON form");
        args[0].clone()
    };
    let dockerfiles = |files: &gdl_gen::Files| -> Vec<String> {
        let compose: serde_json::Value =
            serde_json::from_str(&files[Path::new("compose.yaml")]).expect("compose.yaml");
        let mut found: Vec<String> = compose["services"]
            .as_object()
            .expect("services")
            .values()
            .filter_map(|service| service["build"]["dockerfile"].as_str())
            .map(str::to_owned)
            .collect();
        found.sort();
        found
    };

    let files = local(&spec("examples/http/gdl.toml"), &out).expect("a bundle");
    assert_eq!(
        dockerfiles(&files),
        [
            format!("{compose_path}/hub/Dockerfile"),
            format!("{compose_path}/main/Dockerfile")
        ]
    );
    assert_eq!(
        copy(&files, "hub/Dockerfile"),
        format!("{copy_path}/deployment.json")
    );
    assert_eq!(
        copy(&files, "main/Dockerfile"),
        format!("{copy_path}/main/config.toml")
    );
    for name in ["hub", "main"] {
        let text = &files[&Path::new(name).join("Dockerfile.dockerignore")];
        assert!(text.ends_with(&ignore), "{name}: {text}");
    }

    let (_, files) = local_fleet(&out);
    assert_eq!(
        dockerfiles(&files),
        [
            format!("{compose_path}/engine/Dockerfile"),
            format!("{compose_path}/front/Dockerfile"),
            format!("{compose_path}/hub/Dockerfile")
        ]
    );
    assert_eq!(
        copy(&files, "engine/Dockerfile"),
        format!("{copy_path}/engine/config.toml")
    );
    assert!(
        files[Path::new("hub/Dockerfile.dockerignore")].ends_with(&ignore),
        "{}",
        files[Path::new("hub/Dockerfile.dockerignore")]
    );
}

/// A line break cannot be written in a Dockerfile or an ignore file, and a
/// path that is not UTF-8 cannot be written in any of the bundle's files:
/// `--local` refuses either rather than write another path.
#[cfg(unix)]
#[test]
fn a_local_bundle_refuses_a_path_it_cannot_write() {
    use std::os::unix::ffi::OsStrExt;
    let error = local(
        &spec("examples/http/gdl.toml"),
        &root().join("target/gdl-local-line\nbreak"),
    )
    .expect_err("a line break")
    .to_string();
    assert!(
        error.starts_with("`--local`: the bundle's path from its build context, \"target/gdl-local-line\\nbreak\", holds a line break"),
        "{error}"
    );
    let error = local(
        &spec("examples/http/gdl.toml"),
        &root()
            .join("target")
            .join(std::ffi::OsStr::from_bytes(b"gdl-local-\xff")),
    )
    .expect_err("not UTF-8")
    .to_string();
    assert!(
        error.starts_with("`--local`: the bundle's path from its build context, \"target/gdl-local-\\xFF\", is not UTF-8"),
        "{error}"
    );
}

/// A bundle that Docker Compose cannot run from where it is written is
/// refused before anything is written, in either mode: a path that is not
/// UTF-8 or not in Unicode normal form C, a control character, a `%` that
/// does not start an escape, or `${` anywhere in its directory's path, as
/// given or with symlinks resolved, and a name that gives an empty project
/// name. A `%` with two hex digits is accepted.
#[test]
fn a_bundle_that_compose_cannot_run_is_refused_before_it_is_written() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-local-unloadable-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let spec = spec("examples/http/gdl.toml");
    let compose =
        |out: &Path, sources| gdl_gen::compose(&spec, None, out, &target("gdl-gen-tests"), sources);
    for (path, why) in [
        ("bundle%", "holds a `%` that two hex digits do not follow"),
        ("bundle%GG", "holds a `%` that two hex digits do not follow"),
        ("bundle\t", "holds a control character"),
        ("bundle\u{1b}", "holds a control character"),
        ("bundle\u{7f}", "holds a control character"),
        ("a${X}/bundle", "holds `${`"),
        (
            "a%{X}/bundle",
            "holds a `%` that two hex digits do not follow",
        ),
        ("部署", "has a name with no ASCII letter or digit"),
        ("---", "has a name with no ASCII letter or digit"),
        ("cafe\u{301}/bundle", "is not in Unicode normal form C"),
        ("bundle-\u{212a}", "is not in Unicode normal form C"),
    ] {
        for sources in [gdl_gen::Sources::Local, gdl_gen::Sources::Fetched] {
            let out = holder.path().join(path);
            let error = compose(&out, sources).expect_err(path).to_string();
            assert!(
                error.starts_with(&format!("Compose output {:?} {why}", out.as_os_str())),
                "{path:?}: {error}"
            );
            let first = path.split('/').next().expect("a name");
            assert!(!holder.path().join(first).exists(), "{path:?} is written");
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let out = holder
            .path()
            .join(std::ffi::OsStr::from_bytes(b"bundle-\xff"));
        let error = compose(&out, gdl_gen::Sources::Fetched)
            .expect_err("not UTF-8")
            .to_string();
        assert!(
            error.starts_with(&format!(
                "Compose output {:?} is not UTF-8",
                out.as_os_str()
            )),
            "{error}"
        );
        assert!(!out.exists());
        // Compose loads the directory as the shell names it, through a
        // symlink's name.
        let real = holder.path().join("real");
        std::fs::create_dir(&real).expect("a directory");
        std::os::unix::fs::symlink(&real, holder.path().join("alias%")).expect("a symlink");
        let out = holder.path().join("alias%/bundle");
        let error = compose(&out, gdl_gen::Sources::Local)
            .expect_err("a `%` in the symlink's name")
            .to_string();
        assert!(
            error.starts_with(&format!(
                "Compose output {:?} holds a `%` that two hex digits do not follow",
                out.as_os_str()
            )),
            "{error}"
        );
        assert!(!real.join("bundle").exists());
    }
    let out = holder.path().join("bundle%41");
    compose(&out, gdl_gen::Sources::Local).expect("an escape that Compose reads");
    assert!(out.join("compose.yaml").exists());
}

/// `compose.yaml` is JSON, and Compose reads it as YAML, which folds U+0085
/// into a space and refuses U+FFFE: each is written as a `\u` escape,
/// which JSON and YAML read alike. Other characters stay as they are.
#[test]
fn a_local_bundle_escapes_in_compose_yaml_what_yaml_reads_otherwise() {
    let name = "bundle\u{85}\u{fffe}\u{2028}\u{e9}\u{1f600}";
    let files = local(
        &spec("examples/http/gdl.toml"),
        &root().join("target/gdl-local-yaml").join(name),
    )
    .expect("a bundle");
    let text = &files[Path::new("compose.yaml")];
    assert!(
        text.contains("bundle\\u0085\\ufffe\\u2028\u{e9}\u{1f600}/hub/Dockerfile"),
        "{text}"
    );
    let compose: serde_json::Value = serde_json::from_str(text).expect("JSON");
    assert_eq!(
        compose["services"]["hub"]["build"]["dockerfile"],
        format!("target/gdl-local-yaml/{name}/hub/Dockerfile")
    );
}

/// A bundle under a `.git` directory, which the ignore file leaves out of
/// the context as it does `target`, is let back in the same way.
#[test]
fn a_local_bundle_under_a_git_directory_keeps_itself_in_its_context() {
    let files = local(
        &spec("examples/http/gdl.toml"),
        &root().join("gdl-local-git/.git/bundle"),
    )
    .expect("a bundle");
    let ignore = &files[Path::new("hub/Dockerfile.dockerignore")];
    assert!(ignore.ends_with("!gdl-local-git/.git/bundle\n"), "{ignore}");
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

/// The same, with Docker: Compose finds each Dockerfile, and the image
/// build copies the settings, from a bundle whose path holds [`SYNTAX`].
/// `docker compose config` cannot show this: it writes `$` back as `$$`,
/// and Compose builds through `buildx bake`, which reads the Dockerfile's
/// path again. So the hub builds for real, its Dockerfile cut to the copy
/// of its settings. A decoy directory, which each pattern character would
/// match unescaped, holds other settings.
#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_local_bundle_builds_from_a_path_with_compose_and_docker_syntax() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-local-syntax-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let out = holder.path().join(SYNTAX);
    gdl_gen::compose(
        &spec("examples/http/gdl.toml"),
        None,
        &out,
        &target("gdl-gen-tests"),
        gdl_gen::Sources::Local,
    )
    .expect("a bundle")
    .expect("a local context");
    let decoy = holder.path().join(
        SYNTAX
            .replace('*', "A")
            .replace('?', "B")
            .replace("[dev]", "d")
            .replace("\\ ", " "),
    );
    std::fs::create_dir(&decoy).expect("the decoy");
    std::fs::write(decoy.join("deployment.json"), "decoy").expect("the decoy settings");
    let dockerfile = out.join("hub/Dockerfile");
    let text = std::fs::read_to_string(&dockerfile).expect("the hub's Dockerfile");
    let copy = text
        .lines()
        .find(|line| line.starts_with("COPY [") && line.contains("/etc/gdl/config"))
        .expect("the hub copies its settings");
    std::fs::write(&dockerfile, format!("FROM scratch\n{copy}\n")).expect("a Dockerfile");

    let project = holder
        .path()
        .file_name()
        .expect("a name")
        .to_string_lossy()
        .to_lowercase();
    let image = format!("{project}-hub");
    let docker = |args: &[&str], dir: &Path| {
        let result = Command::new("timeout")
            .arg("120")
            .arg("docker")
            .args(args)
            .current_dir(dir)
            .env_remove("X")
            .output()
            .expect("docker");
        assert!(
            result.status.success(),
            "docker {args:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        String::from_utf8_lossy(&result.stdout).trim().to_owned()
    };
    // The image goes however the test ends.
    struct Image(String);
    impl Drop for Image {
        fn drop(&mut self) {
            let _ = Command::new("docker")
                .args(["image", "rm", "--force", &self.0])
                .output();
        }
    }
    let _image = Image(image.clone());
    docker(
        &["compose", "--project-name", &project, "build", "hub"],
        &out,
    );
    let container = docker(&["create", &image, "none"], &out);
    let copied = holder.path().join("copied");
    let result = Command::new("timeout")
        .arg("30")
        .args(["docker", "cp"])
        .arg(format!("{container}:/etc/gdl/config"))
        .arg(&copied)
        .output()
        .expect("docker cp");
    let _ = Command::new("docker")
        .args(["rm", "--force", &container])
        .output();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(copied).expect("the copied settings"),
        std::fs::read_to_string(out.join("deployment.json")).expect("the settings")
    );
}

/// Docker treats brackets as patterns even in a JSON-form COPY and in
/// the ignore file's exception for a bundle under target.
#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_local_bundle_copies_configuration_from_a_bracketed_path() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-local-bracket-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let out = holder.path().join("bundle[dev]");
    let files = local(&spec("examples/http/gdl.toml"), &out).expect("a bundle");
    let context = tempfile::Builder::new()
        .prefix("gdl-bracket-context-")
        .tempdir()
        .expect("a context");
    let exported = tempfile::Builder::new()
        .prefix("gdl-bracket-export-")
        .tempdir()
        .expect("the exported files");
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
    let generated_ignore = &files[Path::new("hub/Dockerfile.dockerignore")];
    let escaped = deployment.to_str().expect("a path").replace('[', "[[]");
    let control = format!(
        "COPY {}",
        serde_json::to_string(&[escaped, "/etc/gdl/config".into()]).expect("COPY arguments")
    );
    let mut failures = Vec::new();
    for (case, copy, ignore) in [
        ("control", control.as_str(), ""),
        ("generated-copy", generated, ""),
        (
            "generated-ignore",
            control.as_str(),
            generated_ignore.as_str(),
        ),
    ] {
        let dockerfile = context.path().join("Dockerfile");
        std::fs::write(&dockerfile, format!("FROM scratch\n{copy}\n")).expect("a Dockerfile");
        std::fs::write(context.path().join("Dockerfile.dockerignore"), ignore)
            .expect("an ignore file");
        let destination = exported.path().join(case);
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
        if !result.status.success() {
            failures.push(format!(
                "{case}: {copy}\n{ignore}\n{}",
                String::from_utf8_lossy(&result.stderr)
            ));
            continue;
        }
        assert_eq!(
            std::fs::read_to_string(destination.join("etc/gdl/config")).expect("the copied config"),
            files[Path::new("deployment.json")]
        );
        eprintln!("{case}: copied the expected settings");
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
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
/// frontend answers on a port Docker chooses. The bundle's path holds
/// [`SYNTAX`], so each file that names it, and each command the image
/// builds run on it, is proved. Building the images takes a minute or more.
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
    // Dropped after the project, which stops it from the bundle.
    let holder = tempfile::Builder::new()
        .prefix("gdl-local-test-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let project = Project {
        name: format!("gdl-local-test-{id}"),
        dir: holder.path().join(SYNTAX),
    };
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

/// A spec that binds a cell policy by path: with `--local`, the chooser's
/// image builds the policy's crate from the context, and only the chooser
/// depends on it; without, the bundle is refused and names the crate, as
/// a node crate by path is.
#[test]
fn a_local_bundle_builds_a_bound_policy_into_its_chooser() {
    let spec = spec("crates/gdl-gen/tests/fixtures/policy/gdl.toml");
    let fleet = gdl_model::Fleet::from_toml(
        "[class.global]\nready = { front = 1 }\n[class.model]\nready = { model = 1 }\n\
         [cell.global-us]\nclass = \"global\"\nregion = \"us-east-1\"\n\
         [cell.llama-01]\nclass = \"model\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n",
    )
    .expect("a fleet");
    let compose = |sources| {
        gdl_gen::compose_files(
            &spec,
            Some(&fleet),
            &root().join("target/gdl-local-policy"),
            &target("gdl-gen-tests"),
            sources,
        )
    };
    let files = compose(gdl_gen::Sources::Local).expect("a bundle");
    let manifest = &files[Path::new("front/Cargo.toml")];
    assert!(
        manifest.contains(
            "policy-router = { path = \"../../../crates/gdl-gen/tests/fixtures/policy/router\" }"
        ),
        "{manifest}"
    );
    assert!(
        files[Path::new("front/src/lib.rs")]
            .contains("build_cell_policy::<::policy_router::ByModel>(")
    );
    assert!(!files[Path::new("model/Cargo.toml")].contains("policy-router"));
    let refused = compose(gdl_gen::Sources::Fetched)
        .expect_err("a crate by path")
        .to_string();
    assert!(refused.contains("policy-router"), "{refused}");
}

/// A local bundle mounts its secrets as any bundle does, and refuses a
/// secret whose file lies in its build context, which is wider than the
/// bundle: every image build would copy the file into an image.
#[test]
fn a_local_bundle_refuses_a_secret_in_its_context() {
    let dir = root().join("examples/http");
    let text = std::fs::read_to_string(dir.join("gdl.toml")).expect("the spec");
    let graph = std::fs::read_to_string(dir.join("http.gdl")).expect("the graph");
    let outside = tempfile::Builder::new()
        .prefix("gdl-secrets-")
        .tempdir()
        .expect("a directory");
    let bundle = tempfile::Builder::new()
        .prefix("gdl-local-secrets-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let with = |file: &Path| {
        let file = serde_json::to_string(file.to_str().expect("UTF-8")).expect("a string");
        let secrets = format!(
            "[deployment.secrets.key]\nfile = {file}\n[deployment.segment.main]\nsecrets = [\"key\"]\n"
        );
        gdl_gen::Spec::from_text(&format!("{text}{secrets}"), &graph, &dir).expect("a spec")
    };
    let out = bundle.path().join("bundle");
    let files = local(&with(&outside.path().join("key")), &out).expect("a bundle");
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("compose.yaml");
    assert_eq!(
        compose["services"]["main"]["secrets"],
        serde_json::json!(["key"])
    );
    assert!(compose["x-gdl-local-sources"].is_string(), "{compose}");
    for inside in [root().join("examples/key"), root().join("target/x/../key")] {
        let error = local(&with(&inside), &out).expect_err("a secret in the context");
        assert!(
            error.to_string().contains("lies in the build context"),
            "{inside:?}: {error}"
        );
    }
}
