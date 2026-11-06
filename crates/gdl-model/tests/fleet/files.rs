//! Every fleet file the repository holds reads and writes as it did when
//! the pin was made: the fixtures, each `toml` block of the book that
//! reads as a fleet, the fleets the scripts write, and these tests' own.
//! A change to the fleet's types must leave each byte of their TOML and
//! JSON as it was. Run with `GDL_UPDATE_FIXTURES=1` to pin them again.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use gdl_model::Fleet;

/// The pin, beside this file's crate.
const PIN: &str = "tests/fixtures/fleets.txt";

#[test]
fn every_fleet_file_reads_and_writes_as_pinned() {
    let mut pinned = String::new();
    for (source, text) in corpus() {
        let fleet = Fleet::from_toml(&text)
            .unwrap_or_else(|problems| panic!("{source} does not read: {problems:?}"));
        let toml = toml::to_string(&fleet).expect("toml");
        let json = serde_json::to_string(&fleet).expect("json");
        // What it writes reads back as the same fleet, in both forms.
        assert_eq!(Fleet::from_toml(&toml).expect("toml read back"), fleet);
        let back: Fleet = serde_json::from_str(&json).expect("json read back");
        assert_eq!(back, fleet, "{source}");
        write!(pinned, "## {source}\n{toml}\n{json}\n\n").expect("a string");
    }
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(PIN);
    if std::env::var_os("GDL_UPDATE_FIXTURES").is_some() {
        std::fs::create_dir_all(path.parent().expect("a directory")).expect("mkdir");
        std::fs::write(&path, &pinned).expect("write the pin");
    }
    let expected = std::fs::read_to_string(&path).expect("the pin");
    assert!(
        expected == pinned,
        "a fleet file reads or writes differently; run with GDL_UPDATE_FIXTURES=1 to pin it again"
    );
}

/// Each fleet file, by where it is, in a fixed order.
fn corpus() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    for fixture in [
        "crates/gdl-gen/tests/fixtures/k8s/fleet.toml",
        "crates/gdl-gen/tests/fixtures/k8s/dynamodb/fleet.toml",
        "crates/gdl-gen/tests/fixtures/k8s/dgd/fleet.toml",
        "crates/gdl-gen/tests/fixtures/k8s/etcd/fleet.toml",
    ] {
        let text = std::fs::read_to_string(root.join(fixture)).expect(fixture);
        files.push((fixture.to_string(), text));
    }
    let mut chapters = Vec::new();
    markdown(&root.join("docs/src"), &mut chapters);
    chapters.sort();
    for chapter in chapters {
        let text = std::fs::read_to_string(&chapter).expect("a chapter");
        let name = chapter
            .strip_prefix(&root)
            .expect("under the root")
            .display()
            .to_string();
        for (index, block) in toml_blocks(&text).into_iter().enumerate() {
            // Only a block that reads as a fleet with something in it: the
            // book's other blocks are specs and settings.
            let Ok(fleet) = Fleet::from_toml(&block) else {
                continue;
            };
            if fleet != Fleet::default() {
                files.push((format!("{name} block {index}"), block));
            }
        }
    }
    for script in [
        "scripts/kind-fleet.sh",
        "scripts/kind-operator.sh",
        "scripts/images-local.sh",
    ] {
        let text = std::fs::read_to_string(root.join(script)).expect(script);
        files.push((script.to_string(), script_fleet(&text)));
    }
    for (name, text) in [
        ("tests/fleet.rs FLEET", super::FLEET),
        ("tests/fleet.rs SHARED", super::SHARED),
        (
            "tests/fleet/clusters.rs CLUSTERED",
            super::clusters::CLUSTERED,
        ),
        ("tests/fleet/placement.rs PLACED", super::placement::PLACED),
    ] {
        files.push((name.to_string(), text.to_string()));
    }
    files
}

/// Every Markdown file under `dir`.
fn markdown(dir: &Path, into: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("a directory") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            markdown(&path, into);
        } else if path.extension().is_some_and(|e| e == "md") {
            into.push(path);
        }
    }
}

/// The text of each block fenced as ```` ```toml ````.
fn toml_blocks(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut open: Option<String> = None;
    for line in text.lines() {
        match &mut open {
            None if line.trim_start().starts_with("```toml") => open = Some(String::new()),
            Some(block) if line.trim_start().starts_with("```") => {
                blocks.push(std::mem::take(block));
                open = None;
            }
            Some(block) => {
                block.push_str(line);
                block.push('\n');
            }
            None => {}
        }
    }
    blocks
}

/// The fleet a script writes: each here-document it writes or appends to
/// `fleet.toml`, in order, with each shell variable as `x`.
fn script_fleet(text: &str) -> String {
    let mut fleet = String::new();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        if !(trimmed.starts_with("cat >") && trimmed.contains("/fleet.toml\" <<")) {
            continue;
        }
        for body in lines.by_ref() {
            if body == "EOF" {
                break;
            }
            fleet.push_str(&unvariable(body));
            fleet.push('\n');
        }
    }
    assert!(!fleet.is_empty(), "the script writes no fleet");
    fleet
}

/// `line` with each `${...}` as `x`.
fn unvariable(line: &str) -> String {
    let mut out = String::new();
    let mut rest = line;
    while let Some(at) = rest.find("${") {
        out.push_str(&rest[..at]);
        let end = rest[at..].find('}').expect("a closed variable");
        out.push('x');
        rest = &rest[at + end + 1..];
    }
    out.push_str(rest);
    out
}
