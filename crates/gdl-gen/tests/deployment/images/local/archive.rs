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
