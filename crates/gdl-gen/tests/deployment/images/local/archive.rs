use super::*;

/// A generated lock must not copy data from a lock outside the Git snapshot.
#[test]
#[ignore = "F1: an ignored host Cargo.lock supplies metadata to generated locks"]
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
#[ignore = "F2: a Cargo source directory bypasses the root configuration path check"]
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
            Err(error) => assert!(
                error.to_string().contains("outside the sources"),
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
