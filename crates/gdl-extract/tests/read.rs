//! The source reader against the crates of `tests/fixtures/read`: what each
//! crate offers, read from source. Nothing is built, and the engine crate's
//! build script, which fails, never runs.

use std::path::Path;

use gdl_extract::{Extraction, Origin, Workspace};

fn workspace() -> Workspace {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/read/Cargo.toml");
    Workspace::load(&manifest).expect("cargo metadata")
}

fn origin(origin: &Origin) -> String {
    format!("{} {} {}", origin.package, origin.version, origin.path)
}

fn clean(extraction: &Extraction) {
    assert!(
        extraction.problems.is_empty(),
        "{}",
        extraction.problems.join("\n")
    );
}

/// Messages at the root, in a module file, and in a private module; a
/// feature that is off and a `cfg` that is false hide theirs; wire
/// capability comes from serde derives and from hand-written serde impls.
#[test]
fn a_protocols_crate_offers_its_messages_and_exports() {
    let extraction = workspace().extract("read-protocols").expect("package");
    clean(&extraction);
    let offer = &extraction.offer;
    assert_eq!(
        (offer.package.as_str(), offer.version.as_str()),
        ("read-protocols", "0.0.0")
    );
    assert!(offer.source.starts_with("path+file://"), "{}", offer.source);
    let messages: Vec<(&str, &str, bool)> = offer
        .messages
        .iter()
        .map(|m| (m.name.as_str(), m.path.as_str(), m.wire))
        .collect();
    assert_eq!(
        messages,
        [
            ("Piece", "read_protocols::hidden::Piece", false),
            ("Session", "read_protocols::hidden::Session", true),
            ("Token", "read_protocols::engine::Token", true),
            ("Words", "read_protocols::Words", true),
        ]
    );
    let exports: Vec<(&str, &str, String)> = offer
        .exports
        .iter()
        .map(|e| (e.name.as_str(), e.path.as_str(), origin(&e.origin)))
        .collect();
    assert_eq!(
        exports,
        [
            (
                "Piece",
                "read_protocols::Piece",
                "read-protocols 0.0.0 read_protocols::hidden::Piece".to_string()
            ),
            (
                "Session",
                "read_protocols::Session",
                "read-protocols 0.0.0 read_protocols::hidden::Session".to_string()
            ),
        ]
    );
}

/// An export resolves to the message's origin, through a renamed
/// dependency and through the other crate's own re-export.
#[test]
fn an_export_names_its_origin() {
    let extraction = workspace().extract("read-facade").expect("package");
    clean(&extraction);
    let exports: Vec<(&str, &str, String)> = extraction
        .offer
        .exports
        .iter()
        .map(|e| (e.name.as_str(), e.path.as_str(), origin(&e.origin)))
        .collect();
    assert_eq!(
        exports,
        [
            (
                "Prompt",
                "read_facade::Prompt",
                "read-protocols 0.0.0 read_protocols::Words".to_string()
            ),
            (
                "Token",
                "read_facade::Token",
                "read-protocols 0.0.0 read_protocols::engine::Token".to_string()
            ),
        ]
    );
    assert!(extraction.offer.messages.is_empty());
}

/// Nodes in module files, reached through a root re-export and a glob,
/// with their role impl in their own module or another; ports through
/// imports, a module alias, `super`, `crate`, streamed requests with and
/// without a head, and a message offered only through an export.
#[test]
fn a_node_crate_offers_its_nodes_by_message_origin() {
    let extraction = workspace().extract("read-engine").expect("package");
    let nodes: Vec<String> = extraction
        .offer
        .nodes
        .iter()
        .map(|n| {
            let ports: Vec<String> = n
                .ports
                .iter()
                .map(|p| {
                    let side = |o: &Option<Origin>| {
                        o.as_ref().map_or("()".to_string(), |o| o.path.clone())
                    };
                    match &p.item {
                        None => format!("{}={}", p.port, side(&p.head)),
                        Some(item) => {
                            format!("{}={} + stream {}", p.port, side(&p.head), item.path)
                        }
                    }
                })
                .collect();
            format!("{} {} {}: {}", n.role, n.name, n.ty, ports.join(", "))
        })
        .collect();
    assert_eq!(
        nodes,
        [
            "backward Detokenize read_engine::Detokenize: In=read_protocols::engine::Token, Out=read_protocols::hidden::Piece",
            "sink Engine read_engine::Engine: Req=read_protocols::Words, Item=read_protocols::engine::Token",
            "forward Resample read_engine::Resample: In=read_protocols::Words + stream read_protocols::engine::Token, Out=() + stream read_protocols::engine::Token",
        ]
    );
    let problems = extraction.problems.join("\n");
    for expected in [
        "node `Bad`: port `Req`: `String` names no type that derives `Message`",
        "node `Buried`: the crate root does not reach it as `read_engine::Buried`",
        "node `Twice` implements more than one role trait: sink, backward",
        "node `Idle`: the crate implements no role trait for it",
    ] {
        assert!(
            problems.contains(expected),
            "{expected:?} missing from:\n{problems}"
        );
    }
    assert_eq!(extraction.problems.len(), 4, "{problems}");
    assert!(problems.contains("engine/src/lib.rs:"), "{problems}");
}

#[test]
fn an_unknown_package_is_an_error() {
    let error = workspace().extract("no-such-crate").expect_err("unknown");
    assert!(error.to_string().contains("no-such-crate"), "{error}");
}

/// An export carries its message's wire capability, read where the message
/// is defined, in whatever crate.
#[test]
fn an_export_carries_its_messages_wire_capability() {
    let workspace = workspace();
    let wire = |package: &str| -> Vec<(String, bool)> {
        workspace
            .extract(package)
            .expect("package")
            .offer
            .exports
            .iter()
            .map(|e| (e.name.clone(), e.wire))
            .collect()
    };
    assert_eq!(
        wire("read-protocols"),
        [("Piece".to_string(), false), ("Session".to_string(), true)]
    );
    assert_eq!(
        wire("read-facade"),
        [("Prompt".to_string(), true), ("Token".to_string(), true)]
    );
}

/// A spec names a crate by its directory; a catalogue, by name and version.
#[test]
fn a_package_is_found_by_directory_or_by_name_and_version() {
    let workspace = workspace();
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/read/facade");
    let located = workspace.locate_in(&dir).expect("found");
    assert_eq!(
        (
            located.name.as_str(),
            located.version.as_str(),
            located.path
        ),
        ("read-facade", "0.0.0", true)
    );
    let offer = workspace.extract_in(&dir).expect("found").offer;
    assert_eq!(offer.package, "read-facade");
    assert!(workspace.locate_in(Path::new("/nowhere")).is_none());
    assert!(workspace.extract_in(Path::new("/nowhere")).is_none());
    let offer = workspace
        .extract("read-protocols@0.0.0")
        .expect("package")
        .offer;
    assert_eq!(offer.messages.len(), 4);
    let error = workspace
        .extract("read-protocols@9.9.9")
        .expect_err("no such version");
    assert!(
        error.to_string().contains("read-protocols@9.9.9"),
        "{error}"
    );
}

/// A working-catalogue entry is the offer as TOML, and reads back the same.
#[test]
fn an_offer_round_trips_as_a_catalogue_entry() {
    let offer = workspace().extract("read-engine").expect("package").offer;
    assert_eq!(
        gdl_extract::Offer::from_toml(&offer.to_toml()).expect("entry"),
        offer
    );
}

/// Rust read as the compiler reads it: a file's own `cfg`, `#[path]` in
/// an inline module, globs that import only what their module can see,
/// renamed imports of the markers, role traits, and `Streamed`, `cfg` on
/// associated types, serde impls that borrow, raw identifiers, `extern
/// crate self`, and cycles of globs. `cargo gdl extract --verify` checks
/// the same crate against its build.
#[test]
fn rust_is_read_as_the_compiler_reads_it() {
    // A cycle of globs once took exponential time: the reader runs on a
    // thread, and a slow read fails the test instead of hanging it.
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = send.send(workspace().extract("read-quirks").expect("package"));
    });
    let extraction = receive
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the read ends");
    clean(&extraction);
    let offer = &extraction.offer;
    let messages: Vec<(&str, &str, bool)> = offer
        .messages
        .iter()
        .map(|m| (m.name.as_str(), m.path.as_str(), m.wire))
        .collect();
    assert_eq!(
        messages,
        [
            ("Actual", "read_quirks::Actual", false),
            ("Aliased", "read_quirks::defs::Aliased", false),
            ("Borrowed", "read_quirks::Borrowed", false),
            ("Configured", "read_quirks::Configured", true),
            ("Correct", "read_quirks::inline::nested::Correct", false),
            ("Gated", "read_quirks::Gated", true),
            ("Leaf", "read_quirks::paths::nested::leaf::Leaf", false),
            ("Payload", "read_quirks::hidden::Payload", false),
            ("Payload", "read_quirks::visible::Payload", false),
            ("Skipping", "read_quirks::Skipping", true),
            ("Wrong", "read_quirks::Wrong", false),
            ("match", "read_quirks::type::match", false),
        ]
    );
    let exports: Vec<(&str, &str, String)> = offer
        .exports
        .iter()
        .map(|e| (e.name.as_str(), e.path.as_str(), origin(&e.origin)))
        .collect();
    assert_eq!(
        exports,
        [
            (
                "Aliased",
                "read_quirks::Aliased",
                "read-quirks 0.0.0 read_quirks::defs::Aliased".to_string()
            ),
            (
                "Chosen",
                "read_quirks::Chosen",
                "read-quirks 0.0.0 read_quirks::visible::Payload".to_string()
            ),
            (
                "Cycled",
                "read_quirks::cycle::Cycled",
                "read-protocols 0.0.0 read_protocols::Words".to_string()
            ),
            (
                "Many",
                "read_quirks::many::Many",
                "read-protocols 0.0.0 read_protocols::Words".to_string()
            ),
            (
                "Matched",
                "read_quirks::Matched",
                "read-quirks 0.0.0 read_quirks::type::match".to_string()
            ),
            (
                "Passed",
                "read_quirks::Passed",
                "read-quirks 0.0.0 read_quirks::visible::Payload".to_string()
            ),
            (
                "SelfAlias",
                "read_quirks::SelfAlias",
                "read-quirks 0.0.0 read_quirks::visible::Payload".to_string()
            ),
        ]
    );
    let nodes: Vec<String> = offer
        .nodes
        .iter()
        .map(|n| {
            let ports: Vec<String> = n
                .ports
                .iter()
                .map(|p| {
                    let path = |o: &Option<Origin>| {
                        o.as_ref().map_or("()", |o| o.path.as_str()).to_string()
                    };
                    match &p.item {
                        None => format!("{}={}", p.port, path(&p.head)),
                        Some(_) => {
                            format!("{}={} + stream {}", p.port, path(&p.head), path(&p.item))
                        }
                    }
                })
                .collect();
            format!("{} {}: {}", n.role, n.name, ports.join(", "))
        })
        .collect();
    assert_eq!(
        nodes,
        [
            "forward Renamed: In=read_quirks::Actual + stream read_quirks::Actual, Out=read_quirks::Actual",
            "forward Selected: In=read_quirks::Actual, Out=read_quirks::Actual",
        ]
    );
}

/// A definition path is public when the type and every module above it
/// are `pub`; an export's, when every module above the `pub use` is.
#[test]
fn the_reader_knows_which_paths_are_public() {
    let offer = workspace()
        .extract("read-protocols")
        .expect("package")
        .offer;
    let public: Vec<(&str, bool)> = offer
        .messages
        .iter()
        .map(|m| (m.path.as_str(), m.public))
        .chain(offer.exports.iter().map(|e| (e.path.as_str(), e.public)))
        .collect();
    assert_eq!(
        public,
        [
            ("read_protocols::hidden::Piece", false),
            ("read_protocols::hidden::Session", false),
            ("read_protocols::engine::Token", true),
            ("read_protocols::Words", true),
            ("read_protocols::Piece", true),
            ("read_protocols::Session", true),
        ]
    );
}
