use super::*;

/// Refusing a link must precede writes through it to the host.
#[cfg(unix)]
#[test]
#[ignore = "major: bundle generation writes through an escaping link before refusal"]
fn an_external_bundle_link_is_refused_before_writing() {
    let original = Copy::new();
    let mut leaked = Vec::new();
    for builder in Builder::ALL {
        // The link is dangling here, but resolves beside the temporary build tree.
        let parent = tempfile::Builder::new()
            .prefix("gdl-nested-checkout-")
            .tempdir()
            .expect("a checkout parent");
        let dir = tempfile::Builder::new()
            .prefix("checkout-")
            .tempdir_in(parent.path())
            .expect("a nested checkout");
        copy_tree(original.path(), dir.path());
        let copy = Copy { dir };
        git(copy.path(), &["init", "-q"]);
        copy.add();
        git(copy.path(), &["commit", "-q", "-m", "copy"]);
        let host = tempfile::Builder::new()
            .prefix("gdl-external-bundle-")
            .tempdir()
            .expect("a host directory beside temporary build trees");
        std::fs::create_dir(copy.path().join("deep")).expect("a directory");
        std::os::unix::fs::symlink("..", copy.path().join("deep/x"))
            .expect("a link to the sources' root");
        let target = format!(
            "deep/x/../{}",
            host.path().file_name().expect("a name").to_string_lossy()
        );
        std::os::unix::fs::symlink(target, copy.path().join("target"))
            .expect("a lexically internal link to a host directory");
        assert!(
            !copy.path().join("target").exists(),
            "dangling in the checkout"
        );
        copy.add();
        git(copy.path(), &["add", "-f", "target"]);

        let result = builder.contexts(&copy.spec());
        assert!(result.is_err(), "{builder:?} accepted the external link");
        let error = result.expect_err("the link's refusal").to_string();
        assert!(
            error.contains("target") && error.contains("outside"),
            "{error}"
        );
        let written = host.path().join("gdl-images/front/Cargo.toml");
        if written.exists() {
            leaked.push(format!("{builder:?}: {}", written.display()));
        }
    }
    assert!(
        leaked.is_empty(),
        "the link was refused after writing outside the temporary tree: {leaked:?}"
    );
}
