use super::*;

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

    kept_out(&copy.spec(), "credential-", &marker);
}
