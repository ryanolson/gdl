//! The source reader against the crates of `tests/fixtures/read`: what each
//! crate offers, read from source. Nothing is built, and the engine crate's
//! build script, which fails, never runs.

use std::path::Path;

use gdl_extract::{Cargo, Extraction, Origin, Workspace};

fn workspace() -> Workspace {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/read/Cargo.toml");
    Workspace::load(&manifest, &Cargo::trusted()).expect("cargo metadata")
}

fn origin(origin: &Origin) -> String {
    format!("{} {} {}", origin.package, origin.version, origin.path)
}

#[test]
fn message_codecs_survive_source_reading_and_exports() {
    let workspace = workspace();
    let protocols = workspace.extract("read-protocols").expect("package");
    clean(&protocols);
    let encoding = |name: &str| {
        protocols
            .offer
            .messages
            .iter()
            .find(|m| m.name == name)
            .expect("message found")
            .encoding
    };
    assert_eq!(encoding("Words"), gdl_model::Encoding::MsgpackArray);
    assert_eq!(encoding("Session"), gdl_model::Encoding::Json);
    assert_eq!(encoding("Token"), gdl_model::Encoding::Msgpack);
    assert_eq!(
        protocols
            .offer
            .exports
            .iter()
            .find(|e| e.name == "Session")
            .expect("message found")
            .encoding,
        gdl_model::Encoding::Json
    );
    let facade = workspace.extract("read-facade").expect("package");
    clean(&facade);
    assert_eq!(
        facade
            .offer
            .exports
            .iter()
            .find(|e| e.name == "Prompt")
            .expect("message found")
            .encoding,
        gdl_model::Encoding::MsgpackArray
    );
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
        "node `Buried`: the crate root does not reach it publicly as `read_engine::Buried`",
        "node `Shy`: the crate root does not reach it publicly as `read_engine::Shy`",
        "node `Twice` implements more than one role trait: sink, backward",
        "node `Idle`: the crate implements no role trait for it",
        "metrics type must derive Metrics",
    ] {
        assert!(
            problems.contains(expected),
            "{expected:?} missing from:\n{problems}"
        );
    }
    assert_eq!(extraction.problems.len(), 6, "{problems}");
    // The engine's metrics, read from its metrics type's derive.
    let engine = extraction
        .offer
        .nodes
        .iter()
        .find(|n| n.name == "Engine")
        .expect("Engine");
    let metrics: Vec<(&str, gdl_model::MetricKind, &[f64])> = engine
        .metrics
        .iter()
        .map(|m| (m.name.as_str(), m.kind, m.buckets.as_slice()))
        .collect();
    assert_eq!(
        metrics,
        [
            ("tokens_total", gdl_model::MetricKind::Counter, &[][..]),
            (
                "time_to_first_token_seconds",
                gdl_model::MetricKind::Histogram,
                &[0.01, 1.0][..]
            ),
        ]
    );
    assert_eq!(engine.metrics[0].help, "Tokens generated.");
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
            "forward Measured: In=read_quirks::Actual, Out=read_quirks::Actual",
            "forward Renamed: In=read_quirks::Actual + stream read_quirks::Actual, Out=read_quirks::Actual",
            "forward Selected: In=read_quirks::Actual, Out=read_quirks::Actual",
        ]
    );
    let measured = &offer.nodes[0].metrics;
    assert_eq!(measured[0].name, "passed_total");
    assert_eq!(measured[0].labels, ["kind"]);
    assert_eq!(measured[0].help, "Items passed, by kind.");
    assert_eq!(measured[1].buckets, [0.5, 1.0, 2.5]);
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

/// A module file that names itself with `#[path]` is a cycle, read once
/// and reported, rather than read until the stack runs out.
#[test]
fn a_module_path_cycle_is_a_problem_not_an_overflow() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-cycle-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"cycle\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[path = \"lib.rs\"]\npub mod again;\n#[derive(gdl_message::Message)]\npub struct Words;\n",
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("cycle").expect("the package");
    assert!(
        extraction.problems.iter().any(|p| p.contains("cycle")),
        "{:?}",
        extraction.problems
    );
    assert_eq!(extraction.offer.messages.len(), 1);
}

/// A crate is read from under its own directory, once per file, each file
/// a regular file of bounded size, within one budget: a `#[path]` to a
/// device or to a file elsewhere, a pipe, a file too large, or a chain of
/// files each naming the next twice (bounded by the budget, since each
/// read costs at least a few kilobytes) is a problem rather than a process
/// that never ends. The
/// extraction runs on its own thread, so that a reader that hangs fails
/// the test instead of hanging it.
#[test]
fn the_reader_is_bounded_by_the_crate() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-bounded-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"bounded\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    // Thirty files, each naming the next twice: doubled at every level.
    for level in 0..30 {
        let next = level + 1;
        std::fs::write(
            dir.join(format!("src/m{level}.rs")),
            format!("#[path = \"m{next}.rs\"]\npub mod a;\n#[path = \"m{next}.rs\"]\npub mod b;\n"),
        )
        .expect("module");
    }
    std::fs::write(dir.join("src/m30.rs"), "").expect("module");
    // A pipe inside the crate, and a sparse file past the limit.
    #[cfg(unix)]
    assert!(
        std::process::Command::new("mkfifo")
            .arg(dir.join("src/pipe.rs"))
            .status()
            .expect("mkfifo")
            .success()
    );
    std::fs::File::create(dir.join("src/huge.rs"))
        .expect("huge")
        .set_len(17 << 20)
        .expect("sparse");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[path = \"/dev/zero\"]\npub mod zero;\n#[path = \"pipe.rs\"]\npub mod pipe;\n#[path = \"huge.rs\"]\npub mod huge;\n#[path = \"m0.rs\"]\npub mod chain;\n#[derive(gdl_message::Message)]\npub struct Words;\n",
    )
    .expect("source");
    let manifest = dir.join("Cargo.toml");
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let workspace = Workspace::load(&manifest, &Cargo::trusted()).expect("cargo metadata");
        let _ = done.send(workspace.extract("bounded").expect("the package"));
    });
    let extraction = finished
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("the reader finished");
    let problems = extraction.problems.join("\n");
    assert!(
        problems.contains("/dev/zero") && problems.contains("outside"),
        "{problems}"
    );
    assert!(
        problems.contains("pipe.rs") && problems.contains("not a regular file"),
        "{problems}"
    );
    assert!(
        problems.contains("huge.rs") && problems.contains("larger than"),
        "{problems}"
    );
    assert!(problems.contains("in all"), "{problems}");
    assert_eq!(extraction.offer.messages.len(), 1);
}

/// The budget is for the extraction in all: many files backed by one blob
/// cost a repository nothing and would cost the reader everything.
#[test]
fn the_reader_is_bounded_in_total() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-total-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"total\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    // Eight modules of four megabytes of valid Rust each: more than the
    // budget in all, well under the limit each.
    let big: String = (0..250_000)
        .map(|i| format!("pub struct S{i};\n"))
        .collect();
    let mut lib = String::new();
    for module in 0..8 {
        std::fs::write(dir.join(format!("src/m{module}.rs")), &big).expect("module");
        lib.push_str(&format!("pub mod m{module};\n"));
    }
    lib.push_str("#[derive(gdl_message::Message)]\npub struct Words;\n");
    std::fs::write(dir.join("src/lib.rs"), lib).expect("source");
    let manifest = dir.join("Cargo.toml");
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let workspace = Workspace::load(&manifest, &Cargo::trusted()).expect("cargo metadata");
        let _ = done.send(workspace.extract("total").expect("the package"));
    });
    let extraction = finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the reader finished");
    let problems = extraction.problems.join("\n");
    assert!(problems.contains("in all"), "{problems}");
    assert_eq!(extraction.offer.messages.len(), 1);
}

/// One file may back two modules, as rustc allows: the budget, not a
/// read-once rule, bounds what a repository can make the reader read.
#[test]
fn one_file_may_back_two_modules() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-twice-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"twice\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/shared.rs"),
        "#[derive(gdl_message::Message)]\npub struct Words;\n",
    )
    .expect("shared");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[path = \"shared.rs\"]\npub mod a;\n#[path = \"shared.rs\"]\npub mod b;\n",
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("twice").expect("the package");
    assert!(extraction.problems.is_empty(), "{:?}", extraction.problems);
    assert_eq!(extraction.offer.messages.len(), 2);
}

/// The budget counts files as well as bytes: a crate of thousands of tiny
/// modules is refused by their number, since each costs at least a few
/// kilobytes of the budget.
#[test]
fn many_tiny_files_are_bounded_by_their_number() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-many-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"many\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    let mut lib = String::new();
    for module in 0..5000 {
        std::fs::write(dir.join(format!("src/m{module}.rs")), "").expect("module");
        lib.push_str(&format!("pub mod m{module};\n"));
    }
    lib.push_str("#[derive(gdl_message::Message)]\npub struct Words;\n");
    std::fs::write(dir.join("src/lib.rs"), lib).expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("many").expect("the package");
    let problems = extraction.problems.join("\n");
    assert!(problems.contains("in all"), "{problems}");
}
