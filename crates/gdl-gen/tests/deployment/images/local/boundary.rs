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
