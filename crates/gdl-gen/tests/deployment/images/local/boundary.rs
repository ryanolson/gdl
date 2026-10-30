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

#[cfg(unix)]
#[test]
#[ignore = "F1: a visible symlink copies credentials from outside the selected sources"]
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

#[test]
/// A crate named by path below a hidden directory of its checkout: its
/// tracked sources are copied, but no untracked file below that directory,
/// since hidden is judged from the checkout's root, not the crate's.
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
