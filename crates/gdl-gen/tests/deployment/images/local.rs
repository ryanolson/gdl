//! `images --local`: the images build gdl and its hub from `[gdl] path`,
//! and the node crates the spec names by path, from a context that holds
//! them, with no fetch of gdl. Each test works on a copy of this checkout's
//! gdl workspace and the api example in a directory of its own, so that it
//! can change a source file, or move the copy, without touching the
//! checkout that other tests build. Each copy is resolved with `cargo
//! metadata` and its segments' locks settled, which takes a few seconds.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::{Fake, run_with};
use crate::common::root;
use crate::{SEGMENTS, SPLIT};

#[path = "local/boundary.rs"]
mod boundary;
#[path = "local/git.rs"]
mod git;
#[path = "local/manifests.rs"]
mod manifests;
#[path = "local/tags.rs"]
mod tags;

/// A copy of the gdl workspace and the api example, with no build output.
struct Copy {
    dir: tempfile::TempDir,
}

impl Copy {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-images-local-")
            .tempdir()
            .expect("a directory");
        let from = root();
        for file in ["Cargo.toml", "Cargo.lock"] {
            std::fs::copy(from.join(file), dir.path().join(file)).expect("a workspace file");
        }
        for tree in ["crates", "examples/api"] {
            copy_tree(&from.join(tree), &dir.path().join(tree));
        }
        // `--local` builds what Git holds: the copy is a checkout, with
        // every file committed.
        for args in [
            &["init", "-q"][..],
            &["add", "-A"][..],
            &["commit", "-q", "-m", "copy"][..],
        ] {
            git(dir.path(), args);
        }
        Self { dir }
    }

    /// Stage every change in the copy, as a developer does before a build
    /// with `--local`.
    fn add(&self) {
        git(self.path(), &["add", "-A"]);
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    /// The api example's spec in the copy, in two classes, with gdl and
    /// its crates by path.
    fn spec(&self) -> gdl_gen::Spec {
        let dir = self.path().join("examples/api");
        let text = std::fs::read_to_string(dir.join("gdl.toml")).expect("the spec");
        assert!(text.contains(SEGMENTS), "{text}");
        let graph = std::fs::read_to_string(dir.join("api.gdl")).expect("the graph");
        gdl_gen::Spec::from_text(&text.replace(SEGMENTS, SPLIT), &graph, &dir).expect("a spec")
    }
}

/// Run `git <args>` in `dir`, as the fixture's owner, whatever a case asks
/// of Git afterwards, under a name of its own and without signing.
fn git(dir: &Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "user.name=gdl", "-c", "user.email=gdl@example.com"])
        .args(["-c", "commit.gpgsign=false"])
        .args(args)
        .env_remove("GIT_TEST_ASSUME_DIFFERENT_OWNER")
        .output()
        .expect("git runs");
    assert!(output.status.success(), "git {args:?}: {output:?}");
}

/// Copy `from` into `to`, all but build output.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("a directory");
    for entry in std::fs::read_dir(from).expect("a directory") {
        let entry = entry.expect("an entry");
        let path = entry.path();
        let name = entry.file_name();
        if name == "target" || name == ".git" {
            continue;
        }
        if entry.file_type().expect("a type").is_dir() {
            copy_tree(&path, &to.join(&name));
        } else {
            std::fs::copy(&path, to.join(&name)).expect("a file");
        }
    }
}

/// A fleet of one cluster: a global pool and its child.
fn fleet() -> gdl_model::Fleet {
    gdl_model::Fleet::from_toml(
        "[fleet]\nname = \"api\"\n\
         [cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-api\"\nregion = \"us-east-1\"\n\
         registry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n\
         [class.global]\nready = { front = 1 }\n[class.model_x]\nready = { engine = 1 }\n\
         [pool.global-us]\nclass = \"global\"\ncluster = \"use1\"\n\
         [pool.model-x-us]\nclass = \"model_x\"\ncluster = \"use1\"\nhome = \"global-us\"\n",
    )
    .expect("a fleet")
}

fn target() -> PathBuf {
    crate::common::target("gdl-images-local")
}

/// Build the copy's images, locally, through `docker`.
fn local(copy: &Copy, push: bool, docker: &mut Fake) -> Vec<gdl_gen::Built> {
    run_with(
        &copy.spec(),
        &fleet(),
        &target(),
        push,
        gdl_gen::Sources::Local,
        docker,
    )
}

/// Each image's tag, by its repository.
fn tags(built: &[gdl_gen::Built]) -> BTreeMap<String, String> {
    built
        .iter()
        .map(|image| {
            let (rest, tag) = image.reference.rsplit_once(':').expect("a tag");
            let repository = rest.rsplit_once('/').expect("a repository").1;
            (repository.to_string(), tag.to_string())
        })
        .collect()
}

/// The hub installs from the copy's `gdl-hub`, and each segment builds
/// from its crate in the bundle, by its lock, against the copy's gdl: each
/// context holds those sources, and each Dockerfile copies the context.
#[test]
fn images_build_gdl_and_the_hub_from_the_gdl_path() {
    let copy = Copy::new();
    let mut docker = Fake::default();
    let built = local(&copy, false, &mut docker);
    assert_eq!(
        tags(&built).into_keys().collect::<Vec<_>>(),
        ["api-engine", "api-front", "api-hub"]
    );
    for (reference, context) in &docker.contexts {
        let dockerfile = &context[Path::new("Dockerfile")];
        assert!(
            dockerfile.contains("COPY . /app"),
            "{reference}: {dockerfile}"
        );
        for source in [
            "Cargo.toml",
            "Cargo.lock",
            "crates/gdl-runtime/src/lib.rs",
            "crates/gdl-hub/Cargo.toml",
        ] {
            assert!(
                context.contains_key(Path::new(source)),
                "{reference}: {source} is not in the context"
            );
        }
        if reference.contains("/api-hub:") {
            assert!(
                dockerfile.contains("cargo install --path '/app/crates/gdl-hub'"),
                "{dockerfile}"
            );
            assert!(!dockerfile.contains("--git"), "{dockerfile}");
        } else {
            assert!(dockerfile.contains("cargo build --locked"), "{dockerfile}");
            let lock = context
                .keys()
                .find(|path| path.starts_with("target/gdl-images") && path.ends_with("Cargo.lock"))
                .expect("the segment's lock");
            assert!(
                context[lock].contains("name = \"gdl-runtime\""),
                "{}",
                lock.display()
            );
        }
    }
}

/// A change to one gdl source file moves every tag, since every image
/// builds from the context that holds it; an unchanged source, built and
/// pushed again, pushes nothing.
#[test]
fn a_gdl_source_change_moves_the_tags_and_no_change_pushes_nothing() {
    let copy = Copy::new();
    let mut docker = Fake::default();
    let first = local(&copy, true, &mut docker);
    let pushed = docker.builds().len();
    assert_eq!(pushed, 3);
    let again = local(&copy, true, &mut docker);
    assert_eq!(
        docker.builds().len(),
        pushed,
        "an unchanged source was pushed"
    );
    assert!(
        again
            .iter()
            .all(|image| image.outcome == gdl_gen::Outcome::Present),
        "{again:?}"
    );
    assert_eq!(tags(&first), tags(&again));

    let lib = copy.path().join("crates/gdl/src/lib.rs");
    let mut text = std::fs::read_to_string(&lib).expect("gdl's lib.rs");
    text.push_str("\n// A change that is not published.\n");
    std::fs::write(&lib, text).expect("written");
    let changed = local(&copy, true, &mut docker);
    let (before, after) = (tags(&first), tags(&changed));
    for repository in ["api-engine", "api-front", "api-hub"] {
        assert_ne!(before[repository], after[repository], "{repository}");
    }
}

/// The same sources at two places give the same tags, and no file of any
/// context, nor any object, names where they are: no absolute path leaks
/// into a manifest, a patch, a lock, or a Dockerfile.
#[test]
fn the_tags_and_the_objects_do_not_depend_on_where_the_sources_are() {
    let (one, two) = (Copy::new(), Copy::new());
    let (mut docker_one, mut docker_two) = (Fake::default(), Fake::default());
    let built_one = local(&one, true, &mut docker_one);
    let built_two = local(&two, true, &mut docker_two);
    assert_eq!(tags(&built_one), tags(&built_two));
    for (copy, docker) in [(&one, &docker_one), (&two, &docker_two)] {
        let place = copy.path().display().to_string();
        for (reference, context) in &docker.contexts {
            for (path, text) in context {
                assert!(
                    !text.contains(&place),
                    "{reference}: {} names {place}",
                    path.display()
                );
            }
        }
    }
    let objects = gdl_gen::k8s_files(
        &one.spec(),
        &fleet(),
        &target(),
        None,
        Some(&gdl_gen::Digests::of(&built_one)),
        gdl_gen::Sources::Local,
    )
    .expect("the objects");
    let place = one.path().display().to_string();
    for (path, text) in &objects {
        assert!(!text.contains(&place), "{} names {place}", path.display());
    }
    // The objects name the images that were pushed, by digest.
    let text: String = objects.values().cloned().collect();
    for image in &built_one {
        assert!(
            text.contains(&format!("{}@{}", image.reference, image.digest)),
            "{}",
            image.reference
        );
    }
}

/// The context holds what Git tracks, and nothing it does not: no
/// untracked file, a dotfile such as an `.env` of credentials or a new
/// source, and nothing the checkout ignores.
#[test]
fn an_untracked_file_never_reaches_an_image() {
    let copy = Copy::new();
    std::fs::write(copy.path().join(".gitignore"), "/ignored.txt\n").expect("written");
    git(copy.path(), &["add", "-A"]);
    git(copy.path(), &["commit", "-q", "-m", "ignore"]);
    // Made here, so that no source file, this one in the copy included,
    // holds it.
    let marker = ["gdl-local", "credential", "marker"].join("-");
    std::fs::write(copy.path().join(".env"), format!("TOKEN={marker}\n")).expect("written");
    std::fs::write(copy.path().join("crates/gdl/.env"), &marker).expect("written");
    std::fs::write(copy.path().join("ignored.txt"), &marker).expect("written");
    std::fs::write(
        copy.path().join("crates/gdl/src/unpublished.rs"),
        "// A new source, not committed.\n",
    )
    .expect("written");

    let mut docker = Fake::default();
    local(&copy, false, &mut docker);
    for (reference, context) in &docker.contexts {
        for (path, text) in context {
            assert!(
                !text.contains(&marker),
                "{reference}: {} holds the marker",
                path.display()
            );
        }
        assert!(
            !context.contains_key(Path::new("crates/gdl/src/unpublished.rs")),
            "{reference}: an untracked source is in the context"
        );
        assert!(context.contains_key(Path::new(".gitignore")), "{reference}");
    }
}

/// A new source that is staged, and a change to a tracked file that is
/// not, are both built, from the commit `git stash create` makes of them;
/// the checkout, its index, and its refs are as they were.
#[test]
fn a_staged_source_and_an_unstaged_change_are_built() {
    let copy = Copy::new();
    std::fs::write(
        copy.path().join("crates/gdl/src/unpublished.rs"),
        "// A new source, staged.\n",
    )
    .expect("written");
    git(copy.path(), &["add", "crates/gdl/src/unpublished.rs"]);
    let lib = copy.path().join("crates/gdl/src/lib.rs");
    let text = std::fs::read_to_string(&lib).expect("gdl's lib.rs") + "// Not staged.\n";
    std::fs::write(&lib, &text).expect("written");
    let state = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(copy.path())
            .args(args)
            .output()
            .expect("git runs");
        assert!(output.status.success(), "git {args:?}: {output:?}");
        output.stdout
    };
    let before = (
        state(&["status", "--porcelain"]),
        state(&["show-ref"]),
        state(&["stash", "list"]),
    );
    let mut docker = Fake::default();
    local(&copy, false, &mut docker);
    assert_eq!(docker.contexts.len(), 3);
    for (reference, context) in &docker.contexts {
        assert!(
            context.contains_key(Path::new("crates/gdl/src/unpublished.rs")),
            "{reference}: the staged source is not in the context"
        );
        assert_eq!(
            context[Path::new("crates/gdl/src/lib.rs")],
            text,
            "{reference}: the unstaged change is not in the context"
        );
    }
    let after = (
        state(&["status", "--porcelain"]),
        state(&["show-ref"]),
        state(&["stash", "list"]),
    );
    assert_eq!(before, after, "the build changed the checkout's state");
    assert_eq!(std::fs::read_to_string(&lib).expect("gdl's lib.rs"), text);
}
