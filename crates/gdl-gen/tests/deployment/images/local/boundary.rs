use super::*;

fn tracked(copy: &Copy) {
    for args in [&["init", "-q"][..], &["add", "-A"][..]] {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(copy.path())
            .args(args)
            .output()
            .expect("git runs");
        assert!(output.status.success(), "{output:?}");
    }
}

/// A link to a file outside the copied tree is never followed: no byte of
/// its target reaches a context, and the build is refused, naming it.
#[cfg(unix)]
#[test]
fn external_symlinks_do_not_copy_host_credentials() {
    let copy = Copy::new();
    tracked(&copy);
    let host = tempfile::Builder::new()
        .prefix("gdl-host-secret-")
        .tempdir()
        .expect("a host directory outside the copied workspace");
    let marker = ["external", "credential", "marker"].join("-");
    let secret = host.path().join(".env");
    std::fs::write(&secret, &marker).expect("a synthetic credential");
    std::os::unix::fs::symlink(&secret, copy.path().join("credentials.txt"))
        .expect("a visible untracked symlink");
    kept_out(&copy.spec(), "credentials.txt", &marker);
    // Committed, the link is refused by name.
    copy.add();
    kept_out(&copy.spec(), "credentials.txt", &marker);
}

/// A crate named by path below a hidden directory of its checkout: its
/// tracked sources are copied, but no untracked file below that directory,
/// since hidden is judged from the checkout's root, not the crate's.
#[test]
fn a_hidden_crate_directory_does_not_admit_untracked_credentials() {
    let copy = Copy::new();
    let dir = copy.path().join("examples/.private");
    std::fs::rename(copy.path().join("examples/api"), &dir).expect("a hidden crate directory");
    tracked(&copy);
    let text = std::fs::read_to_string(dir.join("gdl.toml")).expect("a spec");
    let graph = std::fs::read_to_string(dir.join("api.gdl")).expect("a graph");
    let spec = gdl_gen::Spec::from_text(&text.replace(SEGMENTS, SPLIT), &graph, &dir)
        .expect("a spec naming the hidden crate by path");
    let marker = ["hidden", "credential", "marker"].join("-");
    std::fs::write(dir.join("credentials.txt"), &marker).expect("an untracked credential");
    for builder in Builder::ALL {
        let contexts = builder.contexts(&spec).expect("local images");
        for (reference, context) in &contexts {
            assert!(
                context.contains_key(Path::new("examples/.private/src/lib.rs")),
                "{builder:?}: {reference}"
            );
            assert!(
                context.values().all(|text| !text.contains(&marker)),
                "{builder:?}: {reference}: an untracked credential below .private reached the build context"
            );
        }
    }
}

/// A Docker that reads what a link in each context is.
#[derive(Default)]
struct Links {
    seen: Vec<Option<PathBuf>>,
}

impl gdl_gen::Docker for Links {
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
        self.seen
            .push(std::fs::read_link(context.join("examples/api/LINKED.md")).ok());
        Ok(format!("sha256:{}", "0".repeat(64)))
    }
}

/// A link whose target is inside the copied sources reaches each context
/// as the same link, not as a copy of what it names.
#[cfg(unix)]
#[test]
fn a_link_within_the_sources_is_copied_as_a_link() {
    let copy = Copy::new();
    std::fs::write(copy.path().join("examples/api/NOTES.md"), "notes\n").expect("a file");
    std::os::unix::fs::symlink("NOTES.md", copy.path().join("examples/api/LINKED.md"))
        .expect("a link within the sources");
    copy.add();
    let mut docker = Links::default();
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
    for seen in &docker.seen {
        assert_eq!(seen.as_deref(), Some(Path::new("NOTES.md")));
    }
    let (_bundle, tree) = compose(&copy.spec()).expect("a compose bundle");
    assert_eq!(
        std::fs::read_link(tree.join("examples/api/LINKED.md")).expect("a link"),
        Path::new("NOTES.md")
    );
}

/// A tracked `.dockerignore` at the root of the sources, which would leave
/// `target/` and so the bundle out of the build, never reaches the context:
/// the context's root holds only the Dockerfile `--local` writes, so no
/// ignore file of the checkout applies to the build.
#[test]
fn a_tracked_root_dockerignore_does_not_drop_the_bundle() {
    let copy = Copy::new();
    std::fs::write(copy.path().join(".dockerignore"), "target/\n").expect("an ignore file");
    std::fs::write(copy.path().join("Dockerfile.dockerignore"), "*\n").expect("an ignore file");
    std::fs::write(copy.path().join("Dockerfile"), "FROM scratch\n").expect("a Dockerfile");
    tracked(&copy);
    for builder in Builder::ALL {
        let contexts = builder.contexts(&copy.spec()).expect("local images");
        for (reference, context) in &contexts {
            for ignore in [".dockerignore", "Dockerfile.dockerignore"] {
                assert!(
                    !context.contains_key(Path::new(ignore)),
                    "{builder:?}: {reference}: {ignore} is in the context"
                );
            }
            // `images` writes its own Dockerfile there; `compose` none.
            assert!(
                context
                    .get(Path::new("Dockerfile"))
                    .is_none_or(|text| !text.starts_with("FROM scratch")),
                "{builder:?}: {reference}: the checkout's Dockerfile is in the context"
            );
            assert!(
                context
                    .keys()
                    .any(|path| path.starts_with("target/gdl-images")),
                "{builder:?}: {reference}: the bundle is not in the context"
            );
        }
    }
}

/// Git still lists tracked descendants when their directory becomes a
/// link. Checking only the leaf must not copy that link's host target.
#[cfg(unix)]
#[test]
fn a_directory_link_cannot_redirect_tracked_files_to_host_credentials() {
    let copy = Copy::new();
    let assets = copy.path().join("assets");
    std::fs::create_dir(&assets).expect("a tracked directory");
    std::fs::write(assets.join("credentials.txt"), "public fixture\n").expect("a tracked file");
    tracked(&copy);
    std::fs::write(copy.path().join(".gitignore"), "/assets\n").expect("an ignore rule");
    local(&copy, false, &mut Fake::default());

    let host = tempfile::Builder::new()
        .prefix("gdl-host-credentials-")
        .tempdir()
        .expect("a host directory outside the sources");
    let marker = ["ancestor", "credential", "marker"].join("-");
    std::fs::write(host.path().join("credentials.txt"), &marker).expect("a synthetic secret");
    std::fs::remove_dir_all(&assets).expect("remove the tracked directory");
    std::os::unix::fs::symlink(host.path(), &assets).expect("redirect the tracked directory");
    let listed = std::process::Command::new("git")
        .arg("-C")
        .arg(copy.path())
        .args(["ls-files", "--cached", "--full-name"])
        .output()
        .expect("Git lists the tracked descendants");
    assert!(listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stdout).contains("assets/credentials.txt"));
    let untracked = std::process::Command::new("git")
        .arg("-C")
        .arg(copy.path())
        .args(["ls-files", "--others", "--exclude-standard"])
        .output()
        .expect("Git lists the untracked files");
    assert!(untracked.status.success());
    assert!(!String::from_utf8_lossy(&untracked.stdout).contains("assets"));

    kept_out(&copy.spec(), "assets", &marker);
}

/// Build the copy's images with each builder, and return the refusal of
/// each, which no context precedes: one rule for both.
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

/// A committed link to an absolute path is refused, naming it: its text
/// would put a path of this machine into the image.
#[cfg(unix)]
#[test]
fn a_tracked_link_to_an_absolute_path_is_refused() {
    let copy = Copy::new();
    std::os::unix::fs::symlink("/etc/hostname", copy.path().join("examples/api/host.txt"))
        .expect("a link");
    copy.add();
    let error = refused(&copy);
    assert!(error.contains("examples/api/host.txt"), "{error}");
    assert!(error.contains("absolute"), "{error}");
}

/// A committed link whose target leaves the sources is refused, naming it.
#[cfg(unix)]
#[test]
fn a_tracked_link_that_leaves_the_sources_is_refused() {
    let copy = Copy::new();
    std::os::unix::fs::symlink("../../../x", copy.path().join("examples/api/out.txt"))
        .expect("a link");
    copy.add();
    let error = refused(&copy);
    assert!(error.contains("examples/api/out.txt"), "{error}");
}

/// Links whose targets each stay within the sources, but which resolve
/// outside them through each other, are refused: in `examples/api/a/b`,
/// `y -> x/../..` reads as `examples/api/a`, and resolves two levels above
/// the root, through `x -> ../../../..`, the root.
#[cfg(unix)]
#[test]
fn a_link_that_resolves_outside_through_another_is_refused() {
    let copy = Copy::new();
    let dir = copy.path().join("examples/api/a/b");
    std::fs::create_dir_all(&dir).expect("a directory");
    std::os::unix::fs::symlink("../../../..", dir.join("x")).expect("a link");
    std::os::unix::fs::symlink("x/../..", dir.join("y")).expect("a link");
    copy.add();
    let error = refused(&copy);
    assert!(error.contains("examples/api/a/b/y"), "{error}");
}

/// A submodule is another repository, which `git archive` leaves out: it
/// is refused, naming it, rather than built without its files.
#[test]
fn a_submodule_is_refused() {
    let copy = Copy::new();
    let inner = tempfile::Builder::new()
        .prefix("gdl-submodule-")
        .tempdir()
        .expect("a repository");
    std::fs::write(inner.path().join("README.md"), "inner\n").expect("a file");
    for args in [
        &["init", "-q"][..],
        &["add", "-A"][..],
        &["commit", "-q", "-m", "inner"][..],
    ] {
        super::git(inner.path(), args);
    }
    super::git(
        copy.path(),
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            &inner.path().display().to_string(),
            "examples/api/vendored",
        ],
    );
    let error = refused(&copy);
    assert!(error.contains("examples/api/vendored"), "{error}");
    assert!(error.contains("submodule"), "{error}");
}
