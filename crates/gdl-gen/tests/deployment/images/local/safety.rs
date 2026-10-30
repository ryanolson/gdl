use super::*;

/// A tracked file, or a dangling link, at each level of the bundle's path
/// is refused, naming it, and nothing is generated. A tracked `target ->
/// crates` that resolves in the checkout is no such case: the bundle's
/// place is the output directory resolved, so the bundle goes to
/// `crates/gdl-images`, inside the tree, through no link
/// ([`a_resolving_target_link_places_the_bundle_in_the_tree`]).
///
/// The five cases share one copy, set back to its commit after each, since
/// a copy takes seconds to make; each case builds with both builders.
#[cfg(unix)]
#[test]
fn every_bundle_level_refuses_a_tracked_file_or_link() {
    let copy = Copy::new();
    let spec = copy.spec();
    for path in ["target", "target/gdl-images"] {
        for link in [None, Some("missing"), Some("crates")] {
            if (path, link) == ("target", Some("crates")) {
                continue;
            }
            let at = copy.path().join(path);
            std::fs::create_dir_all(at.parent().expect("a parent")).expect("a directory");
            match link {
                Some(target) => std::os::unix::fs::symlink(target, &at).expect("a link"),
                None => std::fs::write(&at, "tracked\n").expect("a file"),
            }
            // Forced: a global ignore file commonly names `target`.
            git(copy.path(), &["add", "-f", path]);
            for builder in Builder::ALL {
                let error = builder
                    .contexts(&spec)
                    .expect_err("a file or link on the bundle's path")
                    .to_string();
                assert!(
                    error.contains(path),
                    "{builder:?}: {path}: {link:?}: {error}"
                );
                assert!(error.contains("bundle's path"), "{error}");
                assert!(!copy.path().join("crates/front/Cargo.toml").exists());
            }
            git(copy.path(), &["reset", "-q", "--hard"]);
            let _ = std::fs::remove_dir_all(copy.path().join("target"));
            let _ = std::fs::remove_file(copy.path().join("target"));
        }
    }
}

/// What a tree holds at `target` and at `crates/gdl-images`, read without
/// following a link: whether `target` is a link, and whether the bundle's
/// segment manifest is a file under `crates/gdl-images`.
fn placed(tree: &Path) -> (bool, bool) {
    let link = std::fs::symlink_metadata(tree.join("target"))
        .is_ok_and(|found| found.file_type().is_symlink());
    let manifest = std::fs::symlink_metadata(tree.join("crates/gdl-images/front/Cargo.toml"))
        .is_ok_and(|found| found.is_file());
    (link, manifest)
}

/// A Docker that reads where each context's bundle is, without following
/// a link.
#[derive(Default)]
struct Placed {
    seen: Vec<(bool, bool)>,
}

impl gdl_gen::Docker for Placed {
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
        self.seen.push(placed(context));
        Ok(format!("sha256:{}", "0".repeat(64)))
    }
}

/// A tracked `target -> crates` that resolves in the checkout: the bundle
/// is written at `crates/gdl-images`, as a file of the tree, and `target`
/// stays the link it is, so nothing lands at `target/gdl-images` but
/// through it.
#[cfg(unix)]
#[test]
fn a_resolving_target_link_places_the_bundle_in_the_tree() {
    let copy = Copy::new();
    std::os::unix::fs::symlink("crates", copy.path().join("target")).expect("a link");
    copy.add();
    git(copy.path(), &["add", "-f", "target"]);
    let mut docker = Placed::default();
    gdl_gen::images(
        &copy.spec(),
        &fleet(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    )
    .expect("local images");
    assert_eq!(docker.seen.len(), 3);
    assert!(
        docker.seen.iter().all(|seen| *seen == (true, true)),
        "{:?}",
        docker.seen
    );
    let (_bundle, tree) = compose(&copy.spec()).expect("a compose bundle");
    assert_eq!(placed(&tree), (true, true));
}

/// Real directories on the bundle's path, which an archive holds when a
/// file under them is left out of it, do not stop generation.
#[test]
fn real_bundle_directories_allow_generation() {
    let copy = Copy::new();
    std::fs::create_dir_all(copy.path().join("target/gdl-images")).expect("bundle directories");
    std::fs::write(copy.path().join("target/gdl-images/.keep"), "\n").expect("a tracked file");
    std::fs::write(
        copy.path().join(".gitattributes"),
        "target/gdl-images/.keep export-ignore\n",
    )
    .expect("an excluded file leaves directories in the archive");
    copy.add();
    git(copy.path(), &["add", "-f", "target/gdl-images/.keep"]);
    for builder in Builder::ALL {
        let contexts = builder.contexts(&copy.spec()).expect("real directories");
        assert!(contexts.values().all(|context| {
            context.contains_key(Path::new("target/gdl-images/front/Cargo.toml"))
        }));
    }
}

/// A link that one archive unpacks cannot be the parent of what a later
/// archive writes: `examples`, committed as a link in gdl's repository,
/// with the api example a repository of its own below it, is refused,
/// naming it, and nothing is written through it.
#[test]
fn an_earlier_archive_link_cannot_parent_a_later_source() {
    for target in ["crates", "zz"] {
        let copy = Copy::new();
        let api = copy.path().join("examples/api");
        for args in [
            &["init", "-q"][..],
            &["add", "-A"][..],
            &["commit", "-q", "-m", "api"][..],
        ] {
            git(&api, args);
        }
        std::fs::create_dir(copy.path().join("zz")).expect("a later sibling directory");
        std::fs::write(copy.path().join("zz/marker"), "sibling\n").expect("a sibling file");
        git(copy.path(), &["add", "zz"]);
        git(copy.path(), &["rm", "-qr", "--cached", "examples"]);
        let link = copy.path().join("link-body");
        std::fs::write(&link, target).expect("a link target");
        let object = std::process::Command::new("git")
            .arg("-C")
            .arg(copy.path())
            .args(["hash-object", "-w"])
            .arg(&link)
            .output()
            .expect("a link blob");
        assert!(object.status.success(), "{object:?}");
        let object = String::from_utf8(object.stdout).expect("a hex object id");
        git(
            copy.path(),
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("120000,{},examples", object.trim()),
            ],
        );
        git(
            copy.path(),
            &["update-index", "--skip-worktree", "examples"],
        );
        git(copy.path(), &["commit", "-q", "-m", "archive link"]);
        for builder in Builder::ALL {
            let error = builder
                .contexts(&copy.spec())
                .expect_err("a parent link")
                .to_string();
            assert!(
                error.contains("examples") && error.contains("not a directory"),
                "{builder:?}: {target}: {error}"
            );
            assert!(!copy.path().join("zz/api/Cargo.toml").exists());
        }
    }
}
