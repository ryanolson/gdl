use super::*;

/// A Docker that checks, in each context, that every manifest the
/// generator wrote names each path relatively, and within the context.
#[derive(Default)]
struct Generated {
    manifests: usize,
}

impl gdl_gen::Docker for Generated {
    fn digest(&mut self, _: &str) -> Result<Option<String>, gdl_gen::Error> {
        Ok(None)
    }

    fn build(
        &mut self,
        context: &Path,
        _: &str,
        reference: &str,
        _: bool,
    ) -> Result<String, gdl_gen::Error> {
        let bundle = context.join("target/gdl-images");
        let mut stack = vec![bundle];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("a directory") {
                let path = entry.expect("an entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.file_name().is_none_or(|name| name != "Cargo.toml") {
                    continue;
                }
                self.manifests += 1;
                let text = std::fs::read_to_string(&path).expect("a manifest");
                let table: toml::Table = text.parse().expect("a manifest");
                let mut named = Vec::new();
                paths(&table, &mut named);
                for name in named {
                    assert!(
                        Path::new(&name).is_relative(),
                        "{reference}: {} names {name}",
                        path.display()
                    );
                    let real = path
                        .parent()
                        .expect("a directory")
                        .join(&name)
                        .canonicalize()
                        .unwrap_or_else(|e| panic!("{}: {name}: {e}", path.display()));
                    assert!(
                        real.starts_with(context.canonicalize().expect("the context")),
                        "{reference}: {} names {name}, outside the context",
                        path.display()
                    );
                }
            }
        }
        Ok(format!("sha256:{}", "0".repeat(64)))
    }
}

/// Each `path` string in `table`, at any depth.
fn paths(table: &toml::Table, named: &mut Vec<String>) {
    for (key, value) in table {
        match value {
            toml::Value::String(text) if key == "path" => named.push(text.clone()),
            toml::Value::Table(inner) => paths(inner, named),
            toml::Value::Array(items) => {
                for item in items.iter().filter_map(toml::Value::as_table) {
                    paths(item, named);
                }
            }
            _ => {}
        }
    }
}

/// The manifests the generator writes, each segment's patched to build
/// against gdl's sources, are the generator's, not archived: each names
/// every path relative to itself, and within the context, so that no
/// image depends on where the sources are on this machine.
#[test]
fn the_generated_manifests_name_paths_within_the_tree() {
    let copy = Copy::new();
    let mut docker = Generated::default();
    gdl_gen::images(
        &copy.spec(),
        &fleet(),
        &gdl_gen::Dgds::default(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    )
    .expect("local images");
    assert!(docker.manifests >= 3, "{}", docker.manifests);
}

/// Add a dependency of the api example on `support`, a crate of its own,
/// named by `path`.
fn depend(copy: &Copy, path: &str) {
    let support = copy.path().join("examples/api/support");
    std::fs::create_dir_all(support.join("src")).expect("a dependency directory");
    std::fs::write(
        support.join("Cargo.toml"),
        "[package]\nname = \"image-support\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("a dependency manifest");
    std::fs::write(support.join("src/lib.rs"), "pub const VALUE: u8 = 1;\n")
        .expect("a dependency source");
    let manifest = copy.path().join("examples/api/Cargo.toml");
    let mut text = std::fs::read_to_string(&manifest).expect("the node manifest");
    text.push_str(&format!(
        "\n[dependencies.image-support]\npath = {}\n",
        toml::Value::String(path.to_string())
    ));
    std::fs::write(&manifest, text).expect("a path dependency");
}

/// Build the copy's images with each builder, and return the refusal,
/// which no context precedes: one rule for both.
fn refused(copy: &Copy) -> String {
    let errors: Vec<String> = Builder::ALL
        .iter()
        .map(|builder| match builder.contexts(&copy.spec()) {
            Err(error) => error.to_string(),
            Ok(contexts) => panic!("{builder:?} built {} contexts", contexts.len()),
        })
        .collect();
    assert_eq!(errors[0], errors[1], "one rule refuses for both");
    errors[0].clone()
}

/// A manifest that names a dependency by an absolute path is refused,
/// naming the manifest and the path, even when the path is within the
/// sources: Cargo would read the host's source there, and the image would
/// not hold it. No host source can reach an image.
#[test]
fn an_absolute_path_dependency_is_refused() {
    let copy = Copy::new();
    let support = copy.path().join("examples/api/support");
    depend(&copy, &support.display().to_string());
    copy.add();
    let error = refused(&copy);
    for named in [copy.path().join("examples/api/Cargo.toml"), support.clone()] {
        assert!(
            error.contains(&named.display().to_string()),
            "{} in: {error}",
            named.display()
        );
    }
}

/// A relative path that leaves the sources is refused, naming the
/// manifest and the path, in every manifest the sources hold: here one
/// that no build reads, which Cargo would still read if a later change
/// made it a dependency.
#[test]
fn a_path_that_leaves_the_sources_is_refused() {
    let copy = Copy::new();
    let named = "../../../../gdl-outside";
    let extra = copy.path().join("examples/api/extra");
    std::fs::create_dir_all(&extra).expect("a directory");
    std::fs::write(
        extra.join("Cargo.toml"),
        format!(
            "[package]\nname = \"extra\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [dependencies.outside]\npath = \"{named}\"\n"
        ),
    )
    .expect("a manifest");
    copy.add();
    let error = refused(&copy);
    assert!(error.contains("examples/api/extra/Cargo.toml"), "{error}");
    assert!(error.contains(named), "{error}");
}

/// Parent components cannot disguise an absolute dependency outside the
/// copied sources as a path within their root.
#[test]
fn parent_components_cannot_admit_an_absolute_host_dependency() {
    let copy = Copy::new();
    let host = tempfile::Builder::new()
        .prefix("gdl-host-dependency-")
        .tempdir()
        .expect("a dependency outside the copied sources");
    std::fs::create_dir(host.path().join("src")).expect("a source directory");
    std::fs::write(
        host.path().join("Cargo.toml"),
        "[package]\nname = \"image-support\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("a dependency manifest");
    std::fs::write(host.path().join("src/lib.rs"), "pub const VALUE: u8 = 1;\n")
        .expect("a dependency source");
    let manifest = copy.path().join("examples/api/Cargo.toml");
    let original = std::fs::read_to_string(&manifest).expect("the node manifest");
    let set_path = |named: &Path| {
        std::fs::write(
            &manifest,
            format!(
                "{original}\n[dependencies.image-support]\npath = {}\n",
                toml::Value::String(named.display().to_string())
            ),
        )
        .expect("an absolute dependency");
    };
    set_path(host.path());
    copy.add();
    let error = refused(&copy);
    assert!(error.contains("outside the sources"), "{error}");
    let disguised = copy
        .path()
        .join("..")
        .join(host.path().file_name().expect("a host directory name"));
    assert_eq!(
        disguised.canonicalize().expect("the same dependency"),
        host.path()
    );
    set_path(&disguised);
    copy.add();
    let error = refused(&copy);
    assert!(error.contains("outside the sources"), "{error}");
}

/// A manifest reached through a link within the sources is checked as
/// the manifest Cargo reads: its absolute dependency is refused, naming
/// the link and the path, so no host source reaches an image through it.
#[cfg(unix)]
#[test]
fn a_linked_manifest_with_an_absolute_dependency_is_refused() {
    let copy = Copy::new();
    let api = copy.path().join("examples/api");
    let support = api.join("support");
    depend(&copy, &support.display().to_string());
    let manifest = api.join("Cargo.toml");
    std::fs::rename(&manifest, api.join("Manifest.toml")).expect("move the manifest text");
    std::os::unix::fs::symlink("Manifest.toml", &manifest).expect("an internal manifest link");
    copy.add();
    let error = refused(&copy);
    for named in [manifest, support] {
        assert!(
            error.contains(&named.display().to_string()),
            "{} in: {error}",
            named.display()
        );
    }
}

/// Any Cargo configuration a build of the sources reads is refused,
/// naming the file, whatever it holds: committed at the root or below it,
/// or untracked in a directory between a source and the root, which Cargo
/// on this machine reads. The image build reads none, so a build with one
/// would quietly differ from this checkout's.
#[test]
fn any_cargo_configuration_is_refused() {
    let net = "[build]\njobs = 1\n";
    for (at, staged) in [
        (".cargo/config.toml", true),
        ("examples/api/.cargo/config", true),
        (".cargo/config.toml", false),
    ] {
        let copy = Copy::new();
        let file = copy.path().join(at);
        std::fs::create_dir_all(file.parent().expect("a directory")).expect("a directory");
        std::fs::write(&file, net).expect("a configuration");
        if staged {
            copy.add();
        }
        let error = refused(&copy);
        assert!(error.contains(&file.display().to_string()), "{at}: {error}");
    }
}

/// The configuration `dynamo-endpoints` holds, as it is: only `[net]
/// git-fetch-with-cli`.
const FETCH_WITH_CLI: &str =
    "# gdl is a private GitHub repository: let cargo fetch it with the system git
# and its SSH agent rather than libgit2, which has no agent support.
[net]
git-fetch-with-cli = true
";

/// A Cargo configuration whose only key is `[net] git-fetch-with-cli` is
/// accepted, committed or untracked, and kept out of the tree: a local
/// build fetches no gdl over Git, so the key changes nothing it builds.
#[test]
fn a_configuration_of_git_fetch_with_cli_alone_is_accepted() {
    for (text, at, staged) in [
        (FETCH_WITH_CLI, ".cargo/config.toml", true),
        (
            "[net]\ngit-fetch-with-cli = false\n",
            "examples/api/.cargo/config",
            true,
        ),
        (FETCH_WITH_CLI, ".cargo/config.toml", false),
    ] {
        let copy = Copy::new();
        let file = copy.path().join(at);
        std::fs::create_dir_all(file.parent().expect("a directory")).expect("a directory");
        std::fs::write(&file, text).expect("a configuration");
        if staged {
            copy.add();
        }
        for builder in Builder::ALL {
            let contexts = builder.contexts(&copy.spec()).expect("local images");
            for (reference, context) in &contexts {
                assert!(
                    !context.keys().any(|path| path.starts_with(".cargo")
                        || path.starts_with("examples/api/.cargo")),
                    "{builder:?}: {reference}: {at} is in the tree"
                );
            }
        }
    }
}

/// Any other key beside `[net] git-fetch-with-cli`, or a file that does not
/// parse, is refused, naming the file and the key. The file that does not
/// parse is in a directory Cargo does not read here, so that only the
/// check reads it.
#[test]
fn a_configuration_with_another_key_is_refused() {
    let root = ".cargo/config.toml";
    for (text, key, at) in [
        (
            format!("{FETCH_WITH_CLI}[source.x]\ndirectory = \"/host\"\n"),
            "`source`",
            root,
        ),
        (
            "[net]\ngit-fetch-with-cli = true\noffline = true\n".to_string(),
            "`net.offline`",
            root,
        ),
        (
            "[net]\ngit-fetch-with-cli = \"yes\"\n".to_string(),
            "`net.git-fetch-with-cli`",
            root,
        ),
        (
            "[net\n".to_string(),
            "does not parse",
            "crates/gdl/.cargo/config.toml",
        ),
    ] {
        let copy = Copy::new();
        let file = copy.path().join(at);
        std::fs::create_dir_all(file.parent().expect("a directory")).expect("a directory");
        std::fs::write(&file, &text).expect("a configuration");
        copy.add();
        let error = refused(&copy);
        assert!(
            error.contains(&file.display().to_string()),
            "{text}: {error}"
        );
        assert!(error.contains(key), "{text}: {error}");
    }
}
