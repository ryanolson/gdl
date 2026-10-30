use super::*;

/// A Git checkout that Git refuses to list, here for its ownership, stops
/// the build with Git's error: nothing falls back to a plain copy, which
/// would admit what the checkout ignores.
#[test]
fn a_git_error_does_not_admit_ignored_credentials() {
    if std::env::var_os("GDL_REVIEW_GIT_WORKER").is_none() {
        let output = std::process::Command::new("timeout")
            .arg("30")
            .arg(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "images::local::git::a_git_error_does_not_admit_ignored_credentials",
                "--nocapture",
            ])
            .env("GDL_REVIEW_GIT_WORKER", "1")
            .env("GIT_TEST_ASSUME_DIFFERENT_OWNER", "1")
            .output()
            .expect("an isolated Git ownership check");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // The child ran the case itself, not none.
        assert!(stdout.contains("1 passed"), "{stdout}");
        return;
    }
    let copy = Copy::new();
    std::fs::write(copy.path().join(".gitignore"), "/ignored-credentials.txt\n")
        .expect("an ignore rule");
    for args in [&["init", "-q"][..], &["add", "-A"][..]] {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(copy.path())
            .args(args)
            .env_remove("GIT_TEST_ASSUME_DIFFERENT_OWNER")
            .output()
            .expect("Git sets up an owned fixture");
        assert!(output.status.success(), "{output:?}");
    }
    let listed = std::process::Command::new("git")
        .arg("-C")
        .arg(copy.path())
        .args(["ls-files", "--others", "--exclude-standard"])
        .output()
        .expect("Git runs its ownership check");
    assert!(!listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stderr).contains("dubious ownership"));
    let marker = ["ignored", "credential", "marker"].join("-");
    std::fs::write(copy.path().join("ignored-credentials.txt"), &marker)
        .expect("an ignored synthetic credential");
    let ignored = std::process::Command::new("git")
        .arg("-C")
        .arg(copy.path())
        .args(["check-ignore", "--quiet", "ignored-credentials.txt"])
        .env_remove("GIT_TEST_ASSUME_DIFFERENT_OWNER")
        .status()
        .expect("Git checks the ignore rule without the ownership error");
    assert!(ignored.success());
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
        assert!(error.to_string().contains("git"), "{error}");
        return;
    }
    assert_eq!(docker.contexts.len(), 3);
    for (reference, context) in &docker.contexts {
        assert!(
            context.values().all(|text| !text.contains(&marker)),
            "{reference}: a Git ownership error admitted the ignored credential"
        );
    }
}

/// Lossy decoding can turn a tracked byte name into a different, ignored
/// UTF-8 name. The latter must never supply bytes for the former.
#[cfg(unix)]
#[test]
fn a_non_utf8_tracked_name_cannot_select_an_ignored_secret() {
    use std::os::unix::ffi::OsStringExt;

    let copy = Copy::new();
    let raw = std::ffi::OsString::from_vec(b"credential-\xff.txt".to_vec());
    std::fs::write(copy.path().join(&raw), "public fixture\n").expect("a byte-named file");
    let replacement = "credential-\u{fffd}.txt";
    std::fs::write(copy.path().join(".gitignore"), format!("{replacement}\n"))
        .expect("an ignore rule for a distinct UTF-8 name");
    let added = std::process::Command::new("git")
        .arg("-C")
        .arg(copy.path())
        .args(["add", "-A"])
        .output()
        .expect("Git tracks the byte-named file");
    assert!(added.status.success(), "{added:?}");
    let listed = std::process::Command::new("git")
        .arg("-C")
        .arg(copy.path())
        .args(["ls-files", "-z", "--cached"])
        .output()
        .expect("Git lists byte names");
    assert!(listed.status.success());
    assert!(
        listed
            .stdout
            .split(|byte| *byte == 0)
            .any(|name| name == b"credential-\xff.txt")
    );
    let marker = ["ignored", "unicode", "credential", "marker"].join("-");
    std::fs::write(copy.path().join(replacement), &marker).expect("an ignored synthetic secret");
    let ignored = std::process::Command::new("git")
        .arg("-C")
        .arg(copy.path())
        .args(["check-ignore", "--quiet", replacement])
        .status()
        .expect("Git checks the distinct UTF-8 name");
    assert!(ignored.success());

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
        let text = error.to_string();
        assert!(
            text.contains("UTF-8") || text.contains("utf-8") || text.contains("credential-"),
            "{error}"
        );
        assert!(docker.contexts.is_empty());
        return;
    }
    assert_eq!(docker.contexts.len(), 3);
    for (reference, context) in &docker.contexts {
        assert!(
            context.values().all(|text| !text.contains(&marker)),
            "{reference}: lossy Git name decoding copied an ignored secret into the context"
        );
    }
}
