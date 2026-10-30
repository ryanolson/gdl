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

/// Build the copy's images, and return the refusal.
fn refused(copy: &Copy) -> String {
    let mut docker = Fake::default();
    let error = gdl_gen::images(
        &copy.spec(),
        &fleet(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    )
    .expect_err("refused");
    assert!(docker.contexts.is_empty());
    error.to_string()
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

/// A relative path dependency that leaves the sources, to a crate of this
/// machine, is refused, naming the manifest and the path.
#[test]
fn a_path_dependency_that_leaves_the_sources_is_refused() {
    let copy = Copy::new();
    let outside = tempfile::Builder::new()
        .prefix("gdl-outside-")
        .tempdir_in(copy.path().parent().expect("a parent"))
        .expect("a directory beside the sources");
    std::fs::create_dir(outside.path().join("src")).expect("a source directory");
    std::fs::write(
        outside.path().join("Cargo.toml"),
        "[package]\nname = \"image-support\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("a manifest");
    std::fs::write(outside.path().join("src/lib.rs"), "").expect("a source");
    let named = format!(
        "../../../{}",
        outside
            .path()
            .file_name()
            .expect("a name")
            .to_string_lossy()
    );
    let manifest = copy.path().join("examples/api/Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("the node manifest")
        + &format!("\n[dependencies.image-support]\npath = \"{named}\"\n");
    std::fs::write(&manifest, text).expect("a path dependency");
    copy.add();
    let error = refused(&copy);
    assert!(error.contains("examples/api/Cargo.toml"), "{error}");
    assert!(error.contains(&named), "{error}");
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
    let mut docker = Fake::default();
    let refused = gdl_gen::images(
        &copy.spec(),
        &fleet(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    )
    .expect_err("an absolute dependency outside the sources is refused");
    assert!(
        refused.to_string().contains("outside the sources"),
        "{refused}"
    );
    assert!(docker.contexts.is_empty());
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
    let result = gdl_gen::images(
        &copy.spec(),
        &fleet(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    );
    assert!(
        result.is_err(),
        "parent components admitted an absolute host dependency into {} contexts: {:?}",
        docker.contexts.len(),
        docker
            .contexts
            .values()
            .filter_map(|context| context.get(Path::new("examples/api/Cargo.toml")))
            .collect::<Vec<_>>()
    );
    assert!(docker.contexts.is_empty());
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
