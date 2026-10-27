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

#[test]
fn entries_without_encodings_require_reextraction_with_a_recovery_path() {
    let workspace = workspace();
    for (package, source) in [
        ("read-protocols", None),
        (
            "read-facade",
            Some("registry+https://github.com/rust-lang/crates.io-index"),
        ),
        (
            "read-protocols",
            Some("git+https://example.com/protocols?rev=abc#abc"),
        ),
    ] {
        let temp = scratch("encoding");
        let dir = temp.path();
        let mut extracted = entry(&workspace, package, "2026-10-18T21:04:05Z");
        if let Some(source) = source {
            extracted.offer.source = source.into();
        }
        let mut catalog = Catalog::load(dir).expect("empty catalogue");
        let (_, file) = catalog.add(extracted.clone()).expect("added");
        let file = file.clone();
        let text = std::fs::read_to_string(&file).expect("entry");
        assert!(text.contains("encoding ="), "{text}");
        let incomplete = text
            .lines()
            .filter(|line| !line.starts_with("encoding ="))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&file, incomplete).expect("entry without encoding");

        let error = Catalog::load(dir)
            .expect_err("no inferred encoding")
            .to_string();
        for expected in [
            "missing field `encoding`",
            file.to_str().expect("UTF-8"),
            "Remove this entry",
            "current cargo-gdl",
            "cargo gdl extract",
            "--catalog",
            dir.to_str().expect("UTF-8"),
            "re-import a Git crate in the composer",
        ] {
            assert!(error.contains(expected), "missing {expected:?}: {error}");
        }

        std::fs::remove_file(&file).expect("remove the entry named by the error");
        let mut catalog = Catalog::load(dir).expect("recovered catalogue");
        catalog.add(extracted.clone()).expect("re-extract and add");
        let loaded = Catalog::load(dir).expect("current entry loads");
        assert_eq!(loaded.entries().next(), Some(&extracted));
    }
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

/// `entry` with the wire capability a build reported, `wire`, recorded on
/// each instantiation it exports, as `cargo gdl extract --probe` records
/// it.
fn probed(mut entry: Entry, wire: bool) -> Entry {
    for export in &mut entry.offer.exports {
        if export.is_instantiation() {
            export.probed = Some(wire);
        }
    }
    entry
}

/// An entry without probe data reads and writes as it did before probes
/// existed, and each export's wire capability is the reader's.
#[test]
fn an_entry_without_probe_data_reads_as_before() {
    let workspace = workspace();
    let read = entry(&workspace, "read-generic", "2026-10-18T21:04:05Z");
    let text = read.to_toml();
    assert!(!text.contains("probed"), "{text}");
    let loaded = Entry::from_toml(&text).expect("entry");
    assert_eq!(loaded, read);
    assert_eq!(loaded.to_toml(), text);
    assert!(loaded.offer.exports.iter().any(|e| e.is_instantiation()));
    for export in &loaded.offer.exports {
        assert_eq!(export.probed, None, "{}", export.name);
    }
}

/// A build's reading of each instantiation is written beside the
/// reader's, and read back.
#[test]
fn probe_data_round_trips() {
    let workspace = workspace();
    let read = entry(&workspace, "read-generic", "2026-10-18T21:04:05Z");
    let local = probed(read.clone(), false);
    let wire = probed(read, true);
    let instantiations = local
        .offer
        .exports
        .iter()
        .zip(&wire.offer.exports)
        .filter(|(e, _)| e.is_instantiation());
    let mut seen = 0;
    for (local, wire) in instantiations {
        assert_eq!((local.probed, wire.probed), (Some(false), Some(true)));
        seen += 1;
    }
    assert!(seen >= 2, "the fixture exports instantiations");
    let text = wire.to_toml();
    assert!(text.contains("probed = true"), "{text}");
    assert_eq!(Entry::from_toml(&text).expect("entry"), wire);
}

/// A published version is probed once: an add with probe data fills in a
/// held entry that has none, and is kept after that, whatever a later add
/// brings. The catalogue then says the version needs no probe.
#[test]
fn a_published_version_is_probed_once() {
    let temp = scratch("probed-once");
    let dir = temp.path();
    let workspace = workspace();
    let read = published(entry(&workspace, "read-generic", "2026-10-18T21:04:05Z"));
    let mut catalog = Catalog::load(dir).expect("an empty catalogue");
    let (_, file) = catalog.add(read.clone()).expect("added");
    assert!(!catalog.probed(&read.offer), "added without a probe");

    // A later add with a probe fills in the held entry, which keeps its
    // time.
    let later = Entry {
        added: "2027-01-01T00:00:00Z".into(),
        ..read.clone()
    };
    let (held, _) = catalog.add(probed(later.clone(), true)).expect("probed");
    assert_eq!(held, &probed(read.clone(), true));
    assert!(catalog.probed(&read.offer));
    let loaded = Catalog::load(dir).expect("loaded");
    assert_eq!(loaded.entries().next(), Some(&probed(read.clone(), true)));
    assert!(loaded.probed(&read.offer));
    let text = std::fs::read_to_string(&file).expect("entry");

    // After that, the probe is kept: an add without one, or with another
    // reading, leaves the file as it is.
    let mut catalog = loaded;
    catalog.add(later.clone()).expect("kept");
    catalog.add(probed(later, false)).expect("kept");
    assert_eq!(std::fs::read_to_string(&file).expect("entry"), text);
    assert!(Catalog::load(dir).expect("loaded").probed(&read.offer));
}

/// A crate from a directory changes as it is edited, so the catalogue
/// never says it needs no probe; an add without one drops the last
/// probe's data, which may describe code that is gone.
#[test]
fn a_directory_crate_is_always_probed_again() {
    let temp = scratch("probed-directory");
    let dir = temp.path();
    let workspace = workspace();
    let read = entry(&workspace, "read-generic", "2026-10-18T21:04:05Z");
    assert!(read.offer.source.starts_with("path+"));
    let mut catalog = Catalog::load(dir).expect("an empty catalogue");
    catalog.add(probed(read.clone(), true)).expect("added");
    assert!(!catalog.probed(&read.offer));
    let (held, _) = catalog.add(read.clone()).expect("replaced");
    assert_eq!(held, &read);
}

/// A probe's data is kept when another adder wrote the version's entry
/// first, after this catalogue was loaded: the composer can add a version
/// while a probe of it builds.
#[test]
fn a_probe_fills_an_entry_another_adder_wrote_first() {
    let temp = scratch("probed-race");
    let dir = temp.path();
    let workspace = workspace();
    let read = published(entry(&workspace, "read-generic", "2026-10-18T21:04:05Z"));
    let mut probing = Catalog::load(dir).expect("an empty catalogue");
    let mut other = Catalog::load(dir).expect("an empty catalogue");
    other.add(read.clone()).expect("added first");
    let (held, _) = probing.add(probed(read.clone(), true)).expect("probed");
    assert_eq!(held, &probed(read.clone(), true));
    assert!(Catalog::load(dir).expect("loaded").probed(&read.offer));
}

/// Two probes that each fill another instantiation, from catalogues
/// loaded before either, keep both readings: each fills the file as it is.
#[test]
fn probe_fills_from_stale_catalogues_preserve_each_others_readings() {
    let temp = scratch("probe-fills");
    let dir = temp.path();
    let workspace = workspace();
    let read = published(entry(&workspace, "read-generic", "2026-10-18T21:04:05Z"));
    let indices: Vec<_> = read
        .offer
        .exports
        .iter()
        .enumerate()
        .filter(|(_, e)| e.is_instantiation())
        .map(|(i, _)| i)
        .collect();
    assert!(indices.len() >= 2, "the fixture offers two instantiations");
    let mut seed = Catalog::load(dir).expect("empty catalogue");
    seed.add(read.clone()).expect("unprobed entry");
    let mut first = Catalog::load(dir).expect("first adder");
    let mut second = Catalog::load(dir).expect("second adder before any fill");
    let mut first_probe = read.clone();
    first_probe.offer.exports[indices[0]].probed = Some(true);
    let mut second_probe = read;
    second_probe.offer.exports[indices[1]].probed = Some(true);

    first
        .add(first_probe)
        .expect("first probe fills one instantiation");
    let after_first = Catalog::load(dir).expect("first probe persisted");
    assert_eq!(
        after_first.entries().next().expect("entry").offer.exports[indices[0]].probed,
        Some(true)
    );
    second
        .add(second_probe)
        .expect("second probe fills another instantiation");

    let loaded = Catalog::load(dir).expect("both probes persisted");
    let held = loaded.entries().next().expect("entry");
    assert_eq!(
        held.offer.exports[indices[0]].probed,
        Some(true),
        "the second fill must preserve the first build's reading"
    );
    assert_eq!(held.offer.exports[indices[1]].probed, Some(true));
}

/// Re-importing a published crate refreshes a composer's cached entry
/// after an external probe fills its file.
#[test]
fn an_unprobed_reimport_sees_another_adders_probe() {
    let temp = scratch("probe-reimport");
    let dir = temp.path();
    let workspace = workspace();
    let read = published(entry(&workspace, "read-generic", "2026-10-18T21:04:05Z"));
    let mut composer = Catalog::load(dir).expect("empty catalogue");
    composer.add(read.clone()).expect("initial import");
    assert!(!composer.probed(&read.offer));

    let mut external = Catalog::load(dir).expect("external probe loads the entry");
    external
        .add(probed(read.clone(), true))
        .expect("probe fills the file");
    assert!(
        Catalog::load(dir)
            .expect("probe persisted")
            .probed(&read.offer)
    );

    let (held, _) = composer.add(read).expect("re-import without a build");
    let export = held
        .offer
        .exports
        .iter()
        .find(|e| e.is_instantiation())
        .expect("an instantiation");
    assert_eq!(
        export.probed,
        Some(true),
        "re-import must return the entry with the persisted probe"
    );
}
