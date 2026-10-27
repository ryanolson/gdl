//! The reader's spelling of an instantiation of a generic message against
//! the compiler's. A build names an instantiation with
//! `core::any::type_name`, which the derive records, and the reader writes
//! the same type from source; `extract --verify` compares the two. The
//! standard library documents `type_name` as not stable across compiler
//! versions, so a toolchain that spells a type another way would show up
//! only as a `--verify` difference on some crate. Here it shows up as a row.
//!
//! One source is both: this test compiles `fixtures/spelling/lib.rs` at its
//! own root, so its crate is `spelling`, and the reader reads the same file
//! as the library of a package named `spelling`.

// The fixture's types are read, never used.
#![allow(dead_code)]

include!("fixtures/spelling/lib.rs");

// `BTreeMap` comes from the fixture's own `use`: a second would clash.
use gdl_extract::{Cargo, Workspace};

/// Each alias the fixture exports, and the instantiation it names, as both
/// sides must write it.
const ROWS: &[(&str, &str)] = &[
    ("OfUnit", "spelling::carrier::Carried<()>"),
    ("OfPrimitive", "spelling::carrier::Carried<u64>"),
    (
        "OfString",
        "spelling::carrier::Carried<alloc::string::String>",
    ),
    (
        "OfWords",
        "spelling::carrier::Carried<spelling::words::Words>",
    ),
    ("OfAlias", "spelling::carrier::Carried<alloc::vec::Vec<u8>>"),
    (
        "OfStd",
        "spelling::carrier::Carried<core::option::Option<alloc::vec::Vec<alloc::boxed::Box<u8>>>>",
    ),
    (
        "OfUnitInside",
        "spelling::carrier::Carried<core::option::Option<()>>",
    ),
    (
        "OfQueue",
        "spelling::carrier::Carried<alloc::collections::vec_deque::VecDeque<spelling::words::Words>>",
    ),
    (
        "OfHashMap",
        "spelling::carrier::Carried<std::collections::hash::map::HashMap<alloc::string::String, u8>>",
    ),
    (
        "OfBTreeMap",
        "spelling::carrier::Carried<alloc::collections::btree::map::BTreeMap<alloc::string::String, alloc::vec::Vec<u8>>>",
    ),
    (
        "OfCarried",
        "spelling::carrier::Carried<spelling::carrier::Carried<spelling::words::Words>>",
    ),
    ("OfPair", "spelling::Pair<spelling::words::Words, ()>"),
    (
        "OfPairs",
        "spelling::Pair<spelling::carrier::Carried<u8>, spelling::Pair<(), alloc::string::String>>",
    ),
];

/// What the derive records for each alias this test links.
fn built() -> BTreeMap<String, String> {
    gdl_message::Catalogue::linked()
        .exports
        .into_iter()
        .filter(|e| e.package == "gdl-extract")
        .map(|e| (e.name, e.origin.path))
        .collect()
}

/// What the reader writes for each alias of the fixture, read as a
/// package of its own, and the aliases it leaves out.
fn read() -> (BTreeMap<String, String>, Vec<String>) {
    let temp = tempfile::Builder::new()
        .prefix("gdl-spelling-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"spelling\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        include_str!("fixtures/spelling/lib.rs"),
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("spelling").expect("the package");
    assert!(
        extraction.problems.is_empty(),
        "{}",
        extraction.problems.join("\n")
    );
    let exports = extraction
        .offer
        .exports
        .into_iter()
        .map(|e| (e.name, e.origin.path))
        .collect();
    let skipped = extraction
        .offer
        .skipped
        .into_iter()
        .map(|s| s.name)
        .collect();
    (exports, skipped)
}

/// Each row is written alike by the build and by the reader, and as the
/// row writes it, so the failure names the side that changed.
#[test]
fn the_reader_spells_an_instantiation_as_the_build_does() {
    let built = built();
    let (read, _) = read();
    for (alias, spelling) in ROWS {
        assert_eq!(
            built.get(*alias).map(String::as_str),
            Some(*spelling),
            "the build's spelling of `{alias}`"
        );
        assert_eq!(
            read.get(*alias).map(String::as_str),
            Some(*spelling),
            "the reader's spelling of `{alias}`"
        );
    }
}

/// A reference is not a type the reader names, so its spelling is not
/// pinned: the reader leaves the alias out, which `--verify` reports as
/// left out rather than as a difference. The build's spelling drops the
/// lifetime, which the reader would have to match if it ever read one.
#[test]
fn a_reference_is_left_out_by_the_reader() {
    assert_eq!(
        built().get("OfReference").map(String::as_str),
        Some("spelling::carrier::Carried<&str>")
    );
    let (read, skipped) = read();
    assert!(!read.contains_key("OfReference"), "{read:?}");
    assert_eq!(skipped, ["OfReference"]);
}

/// The table covers every alias the fixture exports, so a row cannot be
/// added to the fixture and left unchecked.
#[test]
fn every_alias_has_a_row() {
    let mut names: Vec<&str> = ROWS.iter().map(|(alias, _)| *alias).collect();
    names.push("OfReference");
    names.sort_unstable();
    let built = built();
    let linked: Vec<&str> = built.keys().map(String::as_str).collect();
    assert_eq!(linked, names);
}
