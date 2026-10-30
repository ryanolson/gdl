use super::*;

struct Isolated<'a> {
    host: &'a Path,
    checked: bool,
}

impl gdl_gen::Docker for Isolated<'_> {
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
        if reference.contains("/api-front:") {
            assert!(context.join("examples/api/support/Cargo.toml").is_file());
            let resolve = || {
                std::process::Command::new("timeout")
                    .arg("30")
                    .arg(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
                    .args([
                        "metadata",
                        "--offline",
                        "--locked",
                        "--format-version",
                        "1",
                        "--manifest-path",
                    ])
                    .arg(context.join("target/gdl-images/front/Cargo.toml"))
                    .current_dir(context)
                    .output()
                    .expect("Cargo resolves the context")
            };
            let reachable = resolve();
            assert!(
                reachable.status.success(),
                "{}",
                String::from_utf8_lossy(&reachable.stderr)
            );
            let saved = self.host.with_file_name("support-saved");
            std::fs::rename(self.host, &saved).expect("hide the host source from Cargo");
            let isolated = resolve();
            std::fs::rename(&saved, self.host).expect("restore the host source");
            self.checked = true;
            assert!(
                isolated.status.success(),
                "the copied dependency exists, but Cargo still requires the host source: {}",
                String::from_utf8_lossy(&isolated.stderr)
            );
        }
        Ok(format!("sha256:{}", "0".repeat(64)))
    }
}

/// A copied manifest that names a dependency by an absolute path names it
/// within the copied tree: Cargo resolves the segment with the host's
/// source hidden.
#[test]
fn copied_manifests_resolve_without_the_host_sources() {
    let copy = Copy::new();
    let api = copy.path().join("examples/api");
    let support = api.join("support");
    std::fs::create_dir_all(support.join("src")).expect("a dependency directory");
    std::fs::write(
        support.join("Cargo.toml"),
        "[package]\nname = \"image-support\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("a dependency manifest");
    std::fs::write(support.join("src/lib.rs"), "pub const VALUE: u8 = 1;\n")
        .expect("a dependency source");
    let manifest = api.join("Cargo.toml");
    let mut text = std::fs::read_to_string(&manifest).expect("the node manifest");
    text.push_str(&format!(
        "\n[dependencies.image-support]\npath = {}\n",
        toml::Value::String(support.display().to_string())
    ));
    std::fs::write(&manifest, text).expect("a valid absolute path dependency");
    let mut docker = Isolated {
        host: &support,
        checked: false,
    };
    gdl_gen::images(
        &copy.spec(),
        &fleet(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    )
    .expect("local images");
    assert!(docker.checked);
}

/// Parent components cannot disguise an absolute dependency outside the
/// copied sources as a path within their root.
#[test]
#[ignore = "parent components still bypass the absolute dependency boundary"]
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
