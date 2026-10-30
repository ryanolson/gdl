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
    local(&copy, false, &mut Fake::default());
    let host = tempfile::Builder::new()
        .prefix("gdl-host-secret-")
        .tempdir()
        .expect("a host directory outside the copied workspace");
    let marker = ["external", "credential", "marker"].join("-");
    let secret = host.path().join(".env");
    std::fs::write(&secret, &marker).expect("a synthetic credential");
    std::os::unix::fs::symlink(&secret, copy.path().join("credentials.txt"))
        .expect("a visible untracked symlink");

    let mut docker = Fake::default();
    let result = gdl_gen::images(
        &copy.spec(),
        &fleet(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    );
    if let Err(error) = result {
        assert!(error.to_string().contains("credentials.txt"), "{error}");
        return;
    }
    assert_eq!(docker.contexts.len(), 3);
    for (reference, context) in &docker.contexts {
        assert!(
            context.values().all(|text| !text.contains(&marker)),
            "{reference}: the external credential reached the Docker build context"
        );
    }
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
    let mut docker = Fake::default();
    run_with(
        &spec,
        &fleet(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut docker,
    );
    assert_eq!(docker.contexts.len(), 3);
    for (reference, context) in &docker.contexts {
        assert!(context.contains_key(Path::new("examples/.private/src/lib.rs")));
        assert!(
            context.values().all(|text| !text.contains(&marker)),
            "{reference}: an untracked credential below .private reached the build context"
        );
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
}
