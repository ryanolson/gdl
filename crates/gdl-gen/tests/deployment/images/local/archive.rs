use super::*;

/// A generated lock must not copy data from a lock outside the Git snapshot.
#[test]
fn an_ignored_lock_cannot_supply_bytes_to_the_context() {
    let copy = Copy::new();
    git(copy.path(), &["rm", "--cached", "Cargo.lock"]);
    std::fs::write(copy.path().join(".gitignore"), "/Cargo.lock\n").expect("an ignore rule");
    copy.add();
    git(
        copy.path(),
        &["commit", "-q", "-m", "ignore workspace lock"],
    );
    git(copy.path(), &["check-ignore", "--quiet", "Cargo.lock"]);
    let listed = std::process::Command::new("git")
        .arg("-C")
        .arg(copy.path())
        .args(["ls-files", "--", "Cargo.lock"])
        .output()
        .expect("Git lists tracked files");
    assert!(listed.status.success() && listed.stdout.is_empty());
    let marker = ["untracked", "lock", "credential", "marker"].join("-");
    let lock = copy.path().join("Cargo.lock");
    let text = std::fs::read_to_string(&lock).expect("the ignored lock");
    std::fs::write(
        &lock,
        format!("{text}\n[metadata]\nreview-marker = {marker:?}\n"),
    )
    .expect("synthetic host-only metadata");

    let mut leaked = Vec::new();
    for builder in Builder::ALL {
        let contexts = builder.contexts(&copy.spec()).expect("local images");
        for (reference, context) in contexts {
            assert!(
                !context.contains_key(Path::new("Cargo.lock")),
                "the ignored workspace lock was archived"
            );
            for (path, text) in context {
                if text.contains(&marker) {
                    assert!(
                        path.starts_with("target/gdl-images") && path.ends_with("Cargo.lock"),
                        "{}",
                        path.display()
                    );
                    leaked.push(format!("{builder:?}: {reference}: {}", path.display()));
                }
            }
        }
    }
    assert!(
        leaked.is_empty(),
        "ignored lock metadata reached the contexts: {leaked:?}"
    );
}

/// A source replacement must not resolve packages from an absolute host directory.
#[test]
fn a_cargo_source_cannot_resolve_from_an_absolute_host_directory() {
    let copy = Copy::new();
    let host = tempfile::Builder::new()
        .prefix("gdl-host-vendor-")
        .tempdir()
        .expect("a vendor directory outside the checkout");
    let package = host.path().join("image-support-0.1.77");
    std::fs::create_dir_all(package.join("src")).expect("a vendor package");
    std::fs::write(
        package.join("Cargo.toml"),
        "[package]\nname = \"image-support\"\nversion = \"0.1.77\"\nedition = \"2024\"\n",
    )
    .expect("a vendor manifest");
    std::fs::write(package.join("src/lib.rs"), "pub const VALUE: u8 = 1;\n")
        .expect("a vendor library");
    std::fs::write(
        package.join(".cargo-checksum.json"),
        serde_json::json!({"files": {}, "package": "0".repeat(64)}).to_string(),
    )
    .expect("vendor checksums");
    let config = format!(
        "[registries.review]\nindex = \"https://example.invalid/review-index\"\n[source.review]\nregistry = \"https://example.invalid/review-index\"\nreplace-with = \"review-host\"\n[source.review-host]\ndirectory = {}\n",
        toml::Value::String(host.path().display().to_string())
    );
    std::fs::create_dir(copy.path().join(".cargo")).expect("a configuration directory");
    std::fs::write(copy.path().join(".cargo/config.toml"), &config)
        .expect("an absolute source directory");
    let manifest = copy.path().join("examples/api/Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("the node manifest");
    std::fs::write(
        &manifest,
        format!("{text}\n[dependencies.image-support]\nversion = \"0.1\"\nregistry = \"review\"\n"),
    )
    .expect("a dependency from the host vendor source");
    copy.add();

    let mut accepted = Vec::new();
    for builder in Builder::ALL {
        match builder.contexts(&copy.spec()) {
            // Refused as Cargo configuration, naming the file.
            Err(error) => assert!(
                error.to_string().contains(".cargo/config.toml"),
                "{builder:?}: {error}"
            ),
            Ok(contexts) => {
                for (reference, context) in contexts {
                    assert_eq!(context[Path::new(".cargo/config.toml")], config);
                    assert!(
                        context
                            .iter()
                            .any(|(path, text)| path.starts_with("target/gdl-images")
                                && path.ends_with("Cargo.lock")
                                && text.contains("name = \"image-support\"\nversion = \"0.1.77\"")),
                        "the segment did not resolve the host package"
                    );
                    assert!(
                        context
                            .keys()
                            .all(|path| !path.starts_with("image-support-0.1.77")),
                        "the host source was archived"
                    );
                    accepted.push(format!("{builder:?}: {reference}"));
                }
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "the generated contexts require the host's absolute vendor directory: {accepted:?}"
    );
}

/// Generating the bundle must not activate a link to unchecked host configuration.
#[cfg(unix)]
#[test]
fn generating_the_bundle_cannot_activate_an_external_config_link() {
    struct Probe {
        config: String,
        reads: usize,
    }

    impl gdl_gen::Docker for Probe {
        fn digest(&mut self, _: &str) -> Result<Option<String>, gdl_gen::Error> {
            Ok(None)
        }

        fn build(
            &mut self,
            context: &Path,
            _: &str,
            _: &str,
            _: bool,
        ) -> Result<String, gdl_gen::Error> {
            let path = context.join(".cargo/config.toml");
            let real = path
                .canonicalize()
                .expect("the activated configuration link");
            assert!(!real.starts_with(context.canonicalize().expect("the tree")));
            assert_eq!(
                std::fs::read_to_string(path).expect("the host configuration"),
                self.config
            );
            let lock: toml::Table =
                std::fs::read_to_string(context.join("target/gdl-images/front/Cargo.lock"))
                    .expect("the generated lock")
                    .parse()
                    .expect("a lock");
            let itoa = lock["package"]
                .as_array()
                .expect("packages")
                .iter()
                .find(|package| package["name"].as_str() == Some("itoa"))
                .expect("itoa in the segment");
            assert!(
                itoa.get("source").is_none(),
                "the lock did not select the unchecked host patch"
            );
            self.reads += 1;
            Ok(format!("sha256:{}", "0".repeat(64)))
        }
    }

    let copy = Copy::new();
    let host = tempfile::Builder::new()
        .prefix("gdl-host-config-")
        .tempdir()
        .expect("a host directory");
    let lock: toml::Table = std::fs::read_to_string(copy.path().join("Cargo.lock"))
        .expect("the workspace lock")
        .parse()
        .expect("a lock");
    let version = lock["package"]
        .as_array()
        .expect("packages")
        .iter()
        .find(|package| package["name"].as_str() == Some("itoa"))
        .expect("the locked itoa")["version"]
        .as_str()
        .expect("a version");
    let patch = host.path().join("itoa");
    std::fs::create_dir_all(patch.join("src")).expect("a host patch");
    std::fs::write(
        patch.join("Cargo.toml"),
        format!("[package]\nname = \"itoa\"\nversion = {version:?}\nedition = \"2024\"\n"),
    )
    .expect("a patch manifest");
    std::fs::write(patch.join("src/lib.rs"), "pub const VALUE: u8 = 1;\n").expect("a host library");
    let config = format!(
        "[patch.crates-io]\nitoa = {{ path = {} }}\n",
        toml::Value::String(patch.display().to_string())
    );
    std::fs::write(host.path().join("config.toml"), &config).expect("host-only configuration");
    std::fs::create_dir_all(copy.path().join(".cargo/deep")).expect("a configuration directory");
    std::os::unix::fs::symlink("../../target/gdl-images", copy.path().join(".cargo/deep/x"))
        .expect("a dangling link to the generated bundle");
    let link_target = format!(
        "deep/x/../../../{}/config.toml",
        host.path()
            .file_name()
            .expect("a host directory name")
            .to_string_lossy()
    );
    let linked = copy.path().join(".cargo/config.toml");
    std::os::unix::fs::symlink(&link_target, &linked)
        .expect("a lexically internal configuration link");
    assert!(
        !linked.exists(),
        "the configuration was active before generation"
    );
    copy.add();

    let mut docker = Probe { config, reads: 0 };
    let images = gdl_gen::images(
        &copy.spec(),
        &fleet(),
        &gdl_gen::Dgds::default(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    );
    let bundle = compose(&copy.spec());
    assert!(
        images.is_err() && bundle.is_err(),
        "a link to unchecked host configuration was accepted: images={}, compose={}, host reads={}",
        images.is_ok(),
        bundle.is_ok(),
        docker.reads
    );
}

/// A link that is dangling until the bundle is generated, and then
/// resolves outside the tree through the bundle's place, is refused by
/// name: links are checked on the tree that is built, after generation.
/// Here no Cargo configuration is involved: in `examples/api`, `deep/x`
/// names the bundle's place, which does not exist until generation, and
/// `notes.txt -> deep/x/../../../<host>/secret.txt` reads as within the
/// tree.
#[cfg(unix)]
#[test]
fn a_link_that_generation_activates_is_refused() {
    let copy = Copy::new();
    let host = tempfile::Builder::new()
        .prefix("gdl-host-secret-")
        .tempdir_in(copy.path().parent().expect("a parent"))
        .expect("a host directory beside the sources");
    std::fs::write(host.path().join("secret.txt"), "host-only\n").expect("a host file");
    let api = copy.path().join("examples/api");
    std::fs::create_dir(api.join("deep")).expect("a directory");
    std::os::unix::fs::symlink("../../../target/gdl-images", api.join("deep/x"))
        .expect("a link to the bundle's place");
    let target = format!(
        "deep/x/../../../{}/secret.txt",
        host.path().file_name().expect("a name").to_string_lossy()
    );
    std::os::unix::fs::symlink(&target, api.join("notes.txt")).expect("a link");
    assert!(
        !api.join("notes.txt").exists(),
        "dangling before generation"
    );
    copy.add();
    for builder in Builder::ALL {
        let error = builder
            .contexts(&copy.spec())
            .expect_err("a link that resolves outside the built tree")
            .to_string();
        assert!(
            error.contains("examples/api/notes.txt"),
            "{builder:?}: {error}"
        );
    }
}
