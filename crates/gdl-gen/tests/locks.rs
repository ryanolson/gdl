//! The test fixtures' lock files pin only crates that the repository's own
//! workspaces fetch.
//!
//! Tests read and build their fixtures with nested cargo runs that must not
//! reach the network. They are checked with a `CARGO_HOME` that holds only
//! `cargo fetch --locked` of the root workspace and of each `examples/*`
//! workspace (`docs/src/development/testing.md`). A fixture lock that pins
//! another version of a crate, as a `cargo update` in the fixture can,
//! makes every test that reads the fixture download that version.

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use common::root;

#[test]
fn every_fixture_lock_pins_only_what_the_workspaces_fetch() {
    let root = root();
    let mut fetched = registry_packages(&root.join("Cargo.lock"));
    let examples = std::fs::read_dir(root.join("examples")).expect("the examples");
    for example in examples {
        let lock = example.expect("an example").path().join("Cargo.lock");
        if lock.exists() {
            fetched.extend(registry_packages(&lock));
        }
    }

    let mut locks = Vec::new();
    for krate in std::fs::read_dir(root.join("crates")).expect("the crates") {
        collect(
            &krate.expect("a crate").path().join("tests/fixtures"),
            &mut locks,
        );
    }
    assert!(
        locks
            .iter()
            .any(|l| l.ends_with("fixtures/generic/Cargo.lock")),
        "the walk finds the fixture locks: {locks:?}"
    );

    let mut missing = Vec::new();
    for lock in &locks {
        for package in registry_packages(lock).difference(&fetched) {
            let relative = lock.strip_prefix(&root).expect("under the root");
            missing.push(format!(
                "{}: {} {}",
                relative.display(),
                package.0,
                package.1
            ));
        }
    }
    assert!(
        missing.is_empty(),
        "fixture locks pin versions that no workspace lock pins; pin the workspace's \
         version with `cargo update --offline -p <crate> --precise <version>` in the \
         fixture:\n{}",
        missing.join("\n")
    );
}

/// The name and version of each package that `lock` takes from a registry.
fn registry_packages(lock: &Path) -> BTreeSet<(String, String)> {
    let text = std::fs::read_to_string(lock).expect("a lock file");
    let lock: toml::Table = toml::from_str(&text).expect("a lock file in TOML");
    let field = |package: &toml::Value, key: &str| {
        package
            .get(key)
            .and_then(toml::Value::as_str)
            .map(str::to_string)
    };
    lock.get("package")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|p| field(p, "source").is_some_and(|s| s.starts_with("registry+")))
        .map(|p| {
            (
                field(p, "name").expect("a name"),
                field(p, "version").expect("a version"),
            )
        })
        .collect()
}

/// Every `Cargo.lock` under `dir`, outside build output.
fn collect(dir: &Path, locks: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n != "target") {
                collect(&path, locks);
            }
        } else if path.file_name().is_some_and(|n| n == "Cargo.lock") {
            locks.push(path);
        }
    }
}
