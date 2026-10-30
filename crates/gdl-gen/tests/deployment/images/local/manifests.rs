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
