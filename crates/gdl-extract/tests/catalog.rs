//! The working catalogue: entries from the source reader's fixtures, added
//! once with their provenance, and the index a load builds from them.

use std::path::Path;

use gdl_extract::{Cargo, Catalog, Entry, Workspace};

fn workspace() -> Workspace {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/read/Cargo.toml");
    Workspace::load(&manifest, &Cargo::trusted()).expect("cargo metadata")
}

/// `entry`, as if it came from crates.io: a published version, which is
/// written once.
fn published(mut entry: Entry) -> Entry {
    entry.offer.source = "registry+https://github.com/rust-lang/crates.io-index".into();
    entry
}

fn entry(workspace: &Workspace, package: &str, added: &str) -> Entry {
    let extraction = workspace.extract(package).expect("package");
    Entry {
        added: added.to_string(),
        checksum: workspace.locate(package).expect("package").checksum,
        offer: extraction.offer,
    }
}

fn scratch(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("gdl-catalog-{name}-"))
        .tempdir()
        .expect("temporary directory")
}

/// An entry is written once, as `<name>/<version>.toml`, with when it was
/// added; adding it again keeps the file as it was.
#[test]
fn an_entry_is_written_once() {
    let temp = scratch("once");
    let dir = temp.path();
    let workspace = workspace();
    let mut catalog = Catalog::load(dir).expect("an empty catalogue");
    let (_, file) = catalog
        .add(published(entry(
            &workspace,
            "read-protocols",
            "2026-10-18T21:04:05Z",
        )))
        .expect("added");
    assert_eq!(file, dir.join("read-protocols/0.0.0.toml"));
    let text = std::fs::read_to_string(&file).expect("written");
    assert!(
        text.starts_with("added = \"2026-10-18T21:04:05Z\"\n"),
        "{text}"
    );
    // The fixture's checksum is none: it was read from a directory.
    assert!(!text.contains("checksum"), "{text}");
    let (kept, _) = catalog
        .add(published(entry(
            &workspace,
            "read-protocols",
            "2027-01-01T00:00:00Z",
        )))
        .expect("kept");
    assert_eq!(kept.added, "2026-10-18T21:04:05Z");
    assert_eq!(std::fs::read_to_string(&file).expect("kept"), text);
    // A second load reads the same entry back.
    let loaded = Catalog::load(dir).expect("loaded");
    let entries: Vec<String> = loaded.entries().map(Entry::key).collect();
    assert_eq!(entries, ["read-protocols@0.0.0"]);
}

/// Loading builds the index, which the catalogue's text form prints:
/// message names to their messages, and node names to their nodes with
/// the messages at their ports.
#[test]
fn a_load_indexes_messages_and_nodes() {
    let temp = scratch("index");
    let dir = temp.path();
    let workspace = workspace();
    let mut catalog = Catalog::load(dir).expect("an empty catalogue");
    for package in ["read-protocols", "read-facade", "read-quirks"] {
        catalog
            .add(entry(&workspace, package, "2026-10-18T21:04:05Z"))
            .expect("added");
    }
    let catalog = Catalog::load(dir).expect("loaded");
    let text = catalog.to_string();
    for line in [
        // Defined in one crate and exported by another: one message.
        "  Token: read_protocols::engine::Token (read-protocols 0.0.0)\n",
        // Exported under another name.
        "  Prompt: read_protocols::Words (read-protocols 0.0.0)\n",
        // Two messages of one name.
        "  Payload: read_quirks::hidden::Payload (read-quirks 0.0.0), read_quirks::visible::Payload (read-quirks 0.0.0)\n",
    ] {
        assert!(text.contains(line), "{line} in:\n{text}");
    }
    let text = catalog.to_string();
    assert!(
        text.contains("  Renamed: forward read_quirks::Renamed (read-quirks@0.0.0)\n    In: read_quirks::Actual (read-quirks 0.0.0) + stream read_quirks::Actual (read-quirks 0.0.0)\n"),
        "{text}"
    );
}

/// A file in the wrong place is refused: the directory is the index of
/// the entries' names.
#[test]
fn an_entry_in_the_wrong_file_is_refused() {
    let temp = scratch("misplaced");
    let dir = temp.path();
    let mut catalog = Catalog::load(dir).expect("an empty catalogue");
    let (_, file) = catalog
        .add(entry(
            &workspace(),
            "read-protocols",
            "2026-10-18T21:04:05Z",
        ))
        .expect("added");
    std::fs::rename(&file, dir.join("read-protocols/9.9.9.toml")).expect("moved");
    let error = Catalog::load(dir).expect_err("misplaced").to_string();
    assert!(error.contains("holds read-protocols@0.0.0"), "{error}");
}

/// Written once, whoever writes first: a second catalogue loaded before
/// the first added the version keeps the first one's entry, file and all.
#[test]
fn the_first_add_wins_across_catalogues() {
    let temp = scratch("first");
    let dir = temp.path();
    let workspace = workspace();
    let mut one = Catalog::load(dir).expect("one");
    let mut two = Catalog::load(dir).expect("two");
    let (_, file) = one
        .add(published(entry(
            &workspace,
            "read-protocols",
            "2026-10-18T21:04:05Z",
        )))
        .expect("first");
    let text = std::fs::read_to_string(&file).expect("written");
    let (kept, _) = two
        .add(published(entry(
            &workspace,
            "read-protocols",
            "2027-01-01T00:00:00Z",
        )))
        .expect("second");
    assert_eq!(kept.added, "2026-10-18T21:04:05Z");
    assert_eq!(std::fs::read_to_string(&file).expect("kept"), text);
}

/// Adders at once, each with a catalogue of its own: every add succeeds,
/// one entry is written whole, and every adder holds that one.
#[test]
fn adders_at_once_agree_on_one_entry() {
    let temp = scratch("racing");
    let dir = temp.path();
    let workspace = workspace();
    let barrier = std::sync::Barrier::new(16);
    let kept: Vec<String> = std::thread::scope(|scope| {
        let adders: Vec<_> = (0..16)
            .map(|i| {
                let entry = published(entry(
                    &workspace,
                    "read-protocols",
                    &format!("2026-10-18T21:04:{i:02}Z"),
                ));
                let (dir, barrier) = (&dir, &barrier);
                scope.spawn(move || {
                    let mut catalog = Catalog::load(dir).expect("load");
                    barrier.wait();
                    catalog.add(entry).expect("add").0.added.clone()
                })
            })
            .collect();
        adders
            .into_iter()
            .map(|a| a.join().expect("adder"))
            .collect()
    });
    let file = dir.join("read-protocols/0.0.0.toml");
    let written =
        Entry::from_toml(&std::fs::read_to_string(&file).expect("written")).expect("whole");
    assert!(
        kept.iter().all(|added| *added == written.added),
        "{kept:?} vs {}",
        written.added
    );
    // Nothing else is left beside it.
    let left: Vec<_> = std::fs::read_dir(dir.join("read-protocols"))
        .expect("dir")
        .map(|e| e.expect("entry").file_name())
        .collect();
    assert_eq!(left, ["0.0.0.toml"]);
}

/// A version already there is kept when it comes from the same source; one
/// from another source is another crate, and the add says so instead of
/// keeping the first as if it were the second.
#[test]
fn a_version_from_another_source_is_refused() {
    let temp = scratch("sources");
    let dir = temp.path();
    let workspace = workspace();
    let mut catalog = Catalog::load(dir).expect("catalogue");
    let mut registry = entry(&workspace, "read-protocols", "2026-10-18T21:04:05Z");
    let path_source = registry.offer.source.clone();
    registry.offer.source = "registry+https://github.com/rust-lang/crates.io-index".into();
    catalog.add(registry).expect("added");
    let error = catalog
        .add(entry(&workspace, "read-protocols", "2026-10-18T21:04:06Z"))
        .expect_err("another source")
        .to_string();
    assert_eq!(
        error,
        format!(
            "read-protocols@0.0.0 is already in the catalogue from registry+https://github.com/rust-lang/crates.io-index, not from {path_source}"
        )
    );
}

/// A crate from a directory is not a published version: it changes as it
/// is edited, so adding it again replaces its entry, file and all.
#[test]
fn a_directory_crate_added_again_is_replaced() {
    let temp = scratch("edited");
    let dir = temp.path();
    let workspace = workspace();
    let mut catalog = Catalog::load(dir).expect("catalogue");
    let first = entry(&workspace, "read-protocols", "2026-10-18T21:04:05Z");
    assert!(first.offer.source.starts_with("path+"));
    catalog.add(first.clone()).expect("added");
    let mut edited = entry(&workspace, "read-protocols", "2026-10-18T21:05:00Z");
    edited.offer.messages.retain(|m| m.name == "Words");
    let (held, file) = catalog.add(edited).expect("replaced");
    assert_eq!(held.added, "2026-10-18T21:05:00Z");
    assert_eq!(held.offer.messages.len(), 1);
    let loaded = Catalog::load(dir).expect("loaded");
    let reread = loaded.entries().next().expect("one entry");
    assert_eq!(
        reread.offer.messages.len(),
        1,
        "{}",
        std::fs::read_to_string(file).expect("file")
    );
}

/// A directory is one package, whatever version it says: adding it at a
/// new version replaces the entry of its old one, file and all, so a
/// catalogue never offers two readings of one directory.
#[test]
fn a_directory_crate_at_a_new_version_replaces_its_old_entry() {
    let temp = scratch("bumped");
    let dir = temp.path();
    let workspace = workspace();
    let mut catalog = Catalog::load(dir).expect("catalogue");
    catalog
        .add(entry(&workspace, "read-protocols", "2026-10-18T21:04:05Z"))
        .expect("added");
    let mut bumped = entry(&workspace, "read-protocols", "2026-10-18T21:05:00Z");
    bumped.offer.version = "0.2.0".into();
    catalog.add(bumped).expect("bumped");
    let keys: Vec<String> = catalog.entries().map(Entry::key).collect();
    assert_eq!(keys, ["read-protocols@0.2.0"]);
    assert!(!dir.join("read-protocols/0.0.0.toml").exists());
    let loaded: Vec<String> = Catalog::load(dir)
        .expect("loaded")
        .entries()
        .map(Entry::key)
        .collect();
    assert_eq!(loaded, ["read-protocols@0.2.0"]);
}

/// A catalogue written before a directory's entries were kept to one may
/// hold several readings of it: a load keeps the one added last, and of
/// two added in the same second, the one read last (the higher version).
#[test]
fn a_load_keeps_one_reading_of_each_directory() {
    let temp = scratch("readings");
    let dir = temp.path();
    let workspace = workspace();
    let write = |version: &str, added: &str| {
        let mut entry = entry(&workspace, "read-protocols", added);
        entry.offer.version = version.into();
        let file = dir.join(format!("read-protocols/{version}.toml"));
        std::fs::create_dir_all(file.parent().expect("dir")).expect("dir");
        std::fs::write(file, entry.to_toml()).expect("written");
    };
    write("0.0.0", "2026-10-18T21:04:05Z");
    write("0.2.0", "2026-10-18T21:05:00Z");
    // Added before 0.2.0, though its version is higher.
    write("0.3.0", "2026-10-18T21:04:30Z");
    let keys = |dir: &Path| -> Vec<String> {
        Catalog::load(dir)
            .expect("loaded")
            .entries()
            .map(Entry::key)
            .collect()
    };
    assert_eq!(keys(dir), ["read-protocols@0.2.0"]);
    // A tie goes to the file read last.
    write("0.4.0", "2026-10-18T21:05:00Z");
    assert_eq!(keys(dir), ["read-protocols@0.4.0"]);
}
