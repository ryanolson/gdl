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

/// A crate is read from under its own directory, each file a regular
/// file of bounded size, within one budget: a `#[path]` to a
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

/// One line per node: its role, name, type, and each port's messages by
/// definition path.
fn node_lines(extraction: &Extraction) -> Vec<String> {
    extraction
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
                    format!("{}={}", p.port, side(&p.head))
                })
                .collect();
            format!("{} {} {}: {}", n.role, n.name, n.ty, ports.join(", "))
        })
        .collect()
}

/// A generic carrier and a generic node offer only the instantiations an
/// exported alias names: a message instantiation as an export under the
/// alias's path, identified by its type, with the generic type's codec and
/// its own wire capability; a node instantiation as a node of the alias's
/// name, whose ports carry the instantiations its impl names with the
/// alias's arguments.
#[test]
fn an_alias_offers_an_instantiation_of_a_generic_type() {
    let extraction = workspace().extract("read-generic").expect("package");
    let offer = &extraction.offer;
    assert!(offer.messages.is_empty(), "{:?}", offer.messages);
    let plain = "read_generic::carrier::Carried<read_protocols::Words, read_generic::Plain>";
    let opaque = "read_generic::carrier::Carried<read_protocols::Words, read_generic::Opaque>";
    let tagged = "read_generic::Tagged<read_protocols::Words, read_generic::Opaque>";
    let exports: Vec<(&str, &str, String, bool, gdl_model::Encoding, bool)> = offer
        .exports
        .iter()
        .map(|e| {
            (
                e.name.as_str(),
                e.path.as_str(),
                origin(&e.origin),
                e.wire,
                e.encoding,
                e.public,
            )
        })
        .collect();
    let json = gdl_model::Encoding::Json;
    let at = |path: &str| format!("read-generic 0.0.0 {path}");
    assert_eq!(
        exports,
        [
            (
                "OpaqueCarried",
                "read_generic::OpaqueCarried",
                at(opaque),
                false,
                json,
                true
            ),
            (
                "OpaqueTagged",
                "read_generic::OpaqueTagged",
                at(tagged),
                true,
                gdl_model::Encoding::Msgpack,
                true
            ),
            (
                "PlainCarried",
                "read_generic::PlainCarried",
                at(plain),
                true,
                json,
                true
            ),
            (
                "Reused",
                "read_generic::Reused",
                at(plain),
                true,
                json,
                true
            ),
        ]
    );
    assert_eq!(
        node_lines(&extraction),
        [
            format!("forward OpaqueRelay read_generic::OpaqueRelay: In={opaque}, Out={opaque}"),
            format!("forward PlainRelay read_generic::PlainRelay: In={plain}, Out={plain}"),
            format!(
                "sink PlainStore read_generic::PlainStore: Req={plain}, Item=read_protocols::Words"
            ),
            format!("forward Unwrap read_generic::Unwrap: In={plain}, Out=read_protocols::Words"),
        ]
    );
    let relay = offer
        .nodes
        .iter()
        .find(|n| n.name == "PlainRelay")
        .expect("PlainRelay");
    assert_eq!(relay.config[0].name, "enabled");
    assert_eq!(relay.metrics[0].name, "relayed_total");
    let problems = extraction.problems.join("\n");
    for expected in [
        "node `OpaqueStore`: the crate implements no role trait for it",
        "node `Hidden`: the crate root does not reach it publicly as `read_generic::Hidden`",
    ] {
        assert!(
            problems.contains(expected),
            "{expected:?} missing from:\n{problems}"
        );
    }
    assert_eq!(extraction.problems.len(), 2, "{problems}");
    let skipped: Vec<&str> = offer.skipped.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(skipped, ["Shared"]);
}

/// An instantiation is one message by its type, not its spelling: another
/// crate names it through renames and is offering the same message, and a
/// node instantiated with that crate's own builder carries that crate's
/// instantiation of the carrier.
#[test]
fn an_instantiation_is_one_message_however_it_is_spelled() {
    let workspace = workspace();
    let generic = workspace.extract("read-generic").expect("package");
    let deployment = workspace.extract("read-deployment").expect("package");
    clean(&deployment);
    let export = |extraction: &Extraction, name: &str| {
        extraction
            .offer
            .exports
            .iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("{name}"))
            .clone()
    };
    assert_eq!(
        export(&deployment, "Again").origin,
        export(&generic, "PlainCarried").origin
    );
    let custom = export(&deployment, "CustomCarried");
    assert_eq!(
        origin(&custom.origin),
        "read-generic 0.0.0 read_generic::carrier::Carried<read_protocols::Words, read_deployment::Custom>"
    );
    assert!(custom.wire);
    let relay = deployment
        .offer
        .nodes
        .iter()
        .find(|n| n.name == "CustomRelay")
        .expect("CustomRelay");
    assert_eq!(relay.ty, "read_deployment::CustomRelay");
    assert!(
        relay
            .ports
            .iter()
            .all(|p| p.head.as_ref() == Some(&custom.origin)),
        "{:?}",
        relay.ports
    );
}

/// An impl for an alias is an impl for the type it names, and an alias
/// that names itself, directly or through an argument, ends the reader's
/// search instead of hanging it: the node it was for has no role.
#[test]
fn an_alias_that_names_itself_ends() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-alias-loop-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"looped\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Words;\n\
         #[derive(gdl::Node)]\npub struct Hold<B>(B);\n\
         pub type Again = Again;\n\
         impl gdl::Sink for Again { type Req = Words; type Item = Words; }\n\
         pub type Nested<B> = Hold<Nested<B>>;\n\
         impl<B> gdl::Sink for Nested<B> { type Req = Words; type Item = Words; }\n\
         #[gdl::export]\npub type Held = Hold<u8>;\n",
    )
    .expect("source");
    let manifest = dir.join("Cargo.toml");
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let workspace = Workspace::load(&manifest, &Cargo::trusted()).expect("cargo metadata");
        let _ = done.send(workspace.extract("looped").expect("the package"));
    });
    let extraction = finished
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("the reader finished");
    let problems = extraction.problems.join("\n");
    assert!(
        problems.contains("node `Held`: the crate implements no role trait for it"),
        "{problems}"
    );
}

/// An instantiation's identity holds the package of each argument: two
/// versions of one package, or one version from two sources, define
/// `api::Request` twice, and the instantiations that carry each are two
/// messages, though `type_name` writes them alike.
#[test]
fn an_instantiation_is_told_apart_by_its_arguments_packages() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-argument-packages-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let write = |path: &str, text: &str| {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("dir");
        std::fs::write(path, text).expect("file");
    };
    // Each its own workspace, so that the path dependencies do not meet
    // as members of one.
    for (at, version) in [("one", "1.0.0"), ("two", "2.0.0")] {
        write(
            &format!("{at}/Cargo.toml"),
            &format!(
                "[package]\nname = \"api\"\nversion = \"{version}\"\nedition = \"2024\"\n[workspace]\n"
            ),
        );
        write(&format!("{at}/src/lib.rs"), "pub struct Request;\n");
    }
    // One version from two sources: a path, and the registry, which a
    // vendored directory stands in for, offline and with nothing written
    // outside this directory. Two paths of one version cannot share a
    // lockfile.
    write(
        "app/vendor/api/Cargo.toml",
        "[package]\nname = \"api\"\nversion = \"1.0.0\"\nedition = \"2024\"\n",
    );
    write("app/vendor/api/src/lib.rs", "pub struct Request;\n");
    write(
        "app/vendor/api/.cargo-checksum.json",
        "{\"files\":{},\"package\":null}",
    );
    write(
        "app/.cargo/config.toml",
        "[source.crates-io]\nreplace-with = \"vendored\"\n\
         [source.vendored]\ndirectory = \"vendor\"\n\
         [net]\noffline = true\n",
    );
    write(
        "app/Cargo.toml",
        "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
         [workspace]\nexclude = [\"vendor\"]\n\
         [dependencies]\n\
         api_one = { package = \"api\", path = \"../one\" }\n\
         api_two = { package = \"api\", path = \"../two\" }\n\
         api_copy = { package = \"api\", version = \"=1.0.0\" }\n",
    );
    write(
        "app/src/lib.rs",
        "#[derive(gdl_message::Message)]\npub struct Carried<P>(pub P);\n\
         #[gdl_message::export]\npub type One = Carried<api_one::Request>;\n\
         #[gdl_message::export]\npub type OneAgain = Carried<api_one::Request>;\n\
         #[gdl_message::export]\npub type Two = Carried<api_two::Request>;\n\
         #[gdl_message::export]\npub type Copy = Carried<api_copy::Request>;\n",
    );
    let workspace =
        Workspace::load(&dir.join("app/Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let app = workspace.extract("app").expect("the package");
    clean(&app);
    let origin = |name: &str| {
        app.offer
            .exports
            .iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("{name}"))
            .origin
            .clone()
    };
    assert_eq!(origin("One").path, "app::Carried<api::Request>");
    assert_eq!(origin("One"), origin("OneAgain"));
    assert_ne!(origin("One"), origin("Two"), "two versions");
    assert_ne!(origin("One"), origin("Copy"), "two sources");
    assert_ne!(origin("Two"), origin("Copy"));
    let source = |name: &str| origin(name).arguments[0].source.clone();
    assert!(source("One").starts_with("path+"), "{}", source("One"));
    assert!(
        source("Copy").starts_with("registry+"),
        "{}",
        source("Copy")
    );
}

/// A qualified path, `<T as Trait>::Out`, names an associated type, not
/// the type at the path `Trait::Out`: an impl for one, or for a type with
/// one as its argument, is no impl of the type a module of that name
/// holds.
#[test]
fn a_qualified_self_type_is_not_read_as_a_plain_path() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-qualified-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"qualified\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Words;\n\
         pub mod tr {\n    #[derive(gdl::Node)]\n    pub struct Out;\n    pub struct Plain;\n}\n\
         pub use tr::Out;\n\
         impl gdl::Sink for <u8 as tr>::Out { type Req = Words; type Item = Words; }\n\
         #[derive(gdl::Node)]\npub struct Store<B>(B);\n\
         impl gdl::Sink for Store<<u8 as tr>::Plain> { type Req = Words; type Item = Words; }\n\
         #[gdl::export]\npub type Held = Store<tr::Plain>;\n",
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("qualified").expect("the package");
    let problems = extraction.problems.join("\n");
    for node in ["Out", "Held"] {
        assert!(
            problems.contains(&format!(
                "node `{node}`: the crate implements no role trait for it"
            )),
            "{problems}"
        );
    }
}

/// An alias's parameter stands for the argument written for it, through
/// aliases of aliases: an impl for `Twice<Hold<u8>>`, where `Twice<T>` is
/// `Identity<Identity<T>>`, is an impl for `Hold<u8>`.
#[test]
fn an_alias_of_an_alias_binds_its_arguments() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-alias-arguments-")
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
        dir.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Words;\n\
         #[derive(gdl::Node)]\npub struct Hold<B>(B);\n\
         pub type Identity<T> = T;\n\
         pub type Twice<T> = Identity<Identity<T>>;\n\
         impl gdl::Sink for Twice<Hold<u8>> { type Req = Words; type Item = Words; }\n\
         #[gdl::export]\npub type Held = Hold<u8>;\n\
         #[gdl::export]\npub type Other = Hold<u16>;\n",
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("twice").expect("the package");
    let roles: Vec<(&str, String)> = extraction
        .offer
        .nodes
        .iter()
        .map(|n| (n.name.as_str(), n.role.to_string()))
        .collect();
    assert_eq!(roles, [("Held", "sink".to_string())]);
    let problems = extraction.problems.join("\n");
    assert!(
        problems.contains("node `Other`: the crate implements no role trait for it"),
        "{problems}"
    );
}

/// A `pub use` marked `#[export]` that names a generic message names no
/// one message: the problem says to export an instantiation by an alias.
#[test]
fn an_export_of_a_generic_type_says_to_export_an_instantiation() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-export-generic-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"reexport\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "mod inner {\n    #[derive(gdl_message::Message)]\n    pub struct Carried<P>(pub P);\n}\n\
         #[gdl_message::export]\npub use inner::Carried as Again;\n",
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("reexport").expect("the package");
    let problems = extraction.problems.join("\n");
    assert!(
        problems.contains(
            "export `Again`: `inner::Carried` names a generic type: export an instantiation of it with `#[export] pub type`"
        ),
        "{problems}"
    );
}

/// What the wire check cannot judge makes an instantiation local: a trait
/// or a derive of serde's name that is not serde's, and a serde attribute
/// the reader does not read, such as `from`. Serde's own, and attributes
/// it reads, still make wire types.
#[test]
fn the_wire_check_fails_closed() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-fails-closed-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"closed\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "pub mod fake {\n    pub trait Serialize {}\n    pub trait Deserialize<'de> {}\n}\n\
         #[derive(gdl_message::Message)]\npub struct ByHand<T>(pub T);\n\
         impl fake::Serialize for ByHand<u8> {}\n\
         impl<'de> fake::Deserialize<'de> for ByHand<u8> {}\n\
         impl serde::Serialize for ByHand<u16> {}\n\
         impl<'de> serde::Deserialize<'de> for ByHand<u16> {}\n\
         #[gdl_message::export]\npub type Fake = ByHand<u8>;\n\
         #[gdl_message::export]\npub type Real = ByHand<u16>;\n\
         #[derive(fake::Serialize, fake::Deserialize, gdl_message::Message)]\npub struct Mimic<T>(pub T);\n\
         #[gdl_message::export]\npub type Mimicked = Mimic<u8>;\n\
         #[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]\n\
         #[serde(from = \"u8\")]\npub struct Converted<T>(pub T);\n\
         #[gdl_message::export]\npub type FromByte = Converted<u8>;\n\
         #[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]\n\
         #[serde(rename_all = \"camelCase\")]\npub struct Renamed<T> {\n    pub value: T,\n}\n\
         #[gdl_message::export]\npub type RenamedByte = Renamed<u8>;\n",
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("closed").expect("the package");
    clean(&extraction);
    let wire: Vec<(&str, bool)> = extraction
        .offer
        .exports
        .iter()
        .map(|e| (e.name.as_str(), e.wire))
        .collect();
    assert_eq!(
        wire,
        [
            ("Fake", false),
            ("FromByte", false),
            ("Mimicked", false),
            ("Real", true),
            ("RenamedByte", true),
        ]
    );
}

/// A `Deserialize` that holds for one input lifetime alone is not
/// `DeserializeOwned`: an impl for `'static`, or for a `'de` that a bound
/// makes outlive another lifetime, in its parameters, its `where` clause,
/// or a derive's own `#[serde(bound)]`, is local. One for every input
/// lifetime, whatever it is named and whatever its parameters'
/// `Deserialize` lifetimes, is a wire type.
/// `cargo gdl extract --verify` checks the buildable forms (the `wire`
/// fixture of gdl-gen); the bounded ones make `#[export]` fail to build.
#[test]
fn a_deserialize_for_one_input_lifetime_is_local() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-one-lifetime-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"lifetimes\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    let by_hand = |name: &str, header: &str| {
        format!(
            "#[derive(gdl_message::Message)]\npub struct {name}<T>(pub T);\n\
             impl<T: serde::Serialize> serde::Serialize for {name}<T> {{}}\n\
             {header} {{}}\n\
             #[gdl_message::export]\npub type Byte{name} = {name}<u8>;\n"
        )
    };
    let source = [
        by_hand(
            "Fixed",
            "impl<T: serde::Deserialize<'static>> serde::Deserialize<'static> for Fixed<T>",
        ),
        by_hand(
            "Outlived",
            "impl<'de: 'static, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Outlived<T>",
        ),
        by_hand(
            "Where",
            "impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Where<T> where 'de: 'static",
        ),
        by_hand(
            "Named",
            "impl<'a, T: serde::Deserialize<'a>> serde::Deserialize<'a> for Named<T>",
        ),
        by_hand(
            "Anchored",
            "impl<'de, T: serde::Deserialize<'static>> serde::Deserialize<'de> for Anchored<T>",
        ),
        by_hand(
            "Every",
            "impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Every<T>",
        ),
        "#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]\n\
         #[serde(bound(deserialize = \"'de: 'static, T: serde::Deserialize<'de>\"))]\n\
         pub struct Lasting<T>(pub T);\n\
         #[gdl_message::export]\npub type ByteLasting = Lasting<u8>;\n"
            .to_string(),
    ];
    std::fs::write(dir.join("src/lib.rs"), source.concat()).expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("lifetimes").expect("the package");
    clean(&extraction);
    let wire: Vec<(&str, bool)> = extraction
        .offer
        .exports
        .iter()
        .map(|e| (e.name.as_str(), e.wire))
        .collect();
    assert_eq!(
        wire,
        [
            ("ByteAnchored", true),
            ("ByteEvery", true),
            ("ByteFixed", false),
            ("ByteLasting", false),
            ("ByteNamed", true),
            ("ByteOutlived", false),
            ("ByteWhere", false),
        ]
    );
}

/// A crate's own type named `Streamed` is not gdl's: a port of it carries
/// that type, an instantiation of a generic message here, not a streamed
/// request's head and item.
#[test]
fn a_type_named_streamed_of_ones_own_is_a_message() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-own-streamed-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"own\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Words;\n\
         #[derive(gdl_message::Message)]\npub struct Streamed<H, I>(pub H, pub I);\n\
         #[derive(gdl::Node)]\npub struct Take;\n\
         impl gdl::Sink for Take { type Req = Streamed<Words, Words>; type Item = Words; }\n",
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("own").expect("the package");
    clean(&extraction);
    assert_eq!(
        node_lines(&extraction),
        ["sink Take own::Take: Req=own::Streamed<own::Words, own::Words>, Item=own::Words"]
    );
    let take = &extraction.offer.nodes[0];
    assert!(
        take.ports.iter().all(|p| p.item.is_none()),
        "{:?}",
        take.ports
    );
}

/// A hand-written impl bounded by a type that is not one of its
/// parameters, here the instantiation itself, is beyond the reader: the
/// instantiation is local, and the check ends rather than following the
/// bound back to the type it began with.
#[test]
fn an_impl_bounded_by_its_own_type_is_local_and_ends() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-self-bound-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"looped\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Msg<T>(pub std::marker::PhantomData<T>);\n\
         impl<'de, T> serde::Deserialize<'de> for Msg<T> {}\n\
         impl<T> serde::Serialize for Msg<T> where Msg<T>: serde::de::DeserializeOwned {}\n\
         #[gdl_message::export]\npub type Looped = Msg<u8>;\n",
    )
    .expect("source");
    let manifest = dir.join("Cargo.toml");
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let workspace = Workspace::load(&manifest, &Cargo::trusted()).expect("cargo metadata");
        let _ = done.send(workspace.extract("looped").expect("the package"));
    });
    let extraction = finished
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("the reader finished");
    clean(&extraction);
    let looped = &extraction.offer.exports[0];
    assert_eq!(looped.name, "Looped");
    assert!(!looped.wire);
}

/// `Default` alone is the prelude's only where nothing in scope binds the
/// name: a trait of that name a glob of the crate's own module brings in
/// hides it, and a glob of another crate's module, which the reader does
/// not read, might, so either makes the instantiation local.
#[test]
fn a_default_a_glob_may_bring_is_not_the_preludes() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-glob-default-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"globbed\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    let hidden = "#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]\n\
                  pub struct Hidden<T> { pub value: u8, #[serde(skip)] pub hidden: T }\n";
    std::fs::write(
        dir.join("src/lib.rs"),
        format!(
            "{hidden}\
             pub mod traits {{ pub trait Default {{}} }}\n\
             pub mod own {{\n    use super::traits::*;\n    pub struct Plain;\n    impl Default for Plain {{}}\n}}\n\
             pub mod other {{\n    use elsewhere::*;\n    pub struct Plain;\n    impl Default for Plain {{ fn default() -> Self {{ Plain }} }}\n}}\n\
             pub mod prelude {{\n    pub struct Plain;\n    impl Default for Plain {{ fn default() -> Self {{ Plain }} }}\n}}\n\
             #[gdl_message::export]\npub type Own = Hidden<own::Plain>;\n\
             #[gdl_message::export]\npub type Other = Hidden<other::Plain>;\n\
             #[gdl_message::export]\npub type Prelude = Hidden<prelude::Plain>;\n"
        ),
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("globbed").expect("the package");
    clean(&extraction);
    let wire: Vec<(&str, bool)> = extraction
        .offer
        .exports
        .iter()
        .map(|e| (e.name.as_str(), e.wire))
        .collect();
    assert_eq!(wire, [("Other", false), ("Own", false), ("Prelude", true)]);
}

/// An alias whose argument the reader cannot name is left out, with why,
/// and the rest of the crate is read: it is no problem of the crate's.
#[test]
fn an_alias_of_an_argument_the_reader_cannot_name_is_left_out_alone() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-unnamed-argument-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"unnamed\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Words;\n\
         #[derive(gdl_message::Message)]\npub struct Plain<T>(pub T);\n\
         #[gdl_message::export]\npub type Shared = Plain<std::sync::Arc<Words>>;\n\
         #[gdl_message::export]\npub type Fine = Plain<Words>;\n",
    )
    .expect("source");
    let workspace =
        Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("cargo metadata");
    let extraction = workspace.extract("unnamed").expect("the package");
    clean(&extraction);
    let names: Vec<&str> = extraction
        .offer
        .exports
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(names, ["Fine"]);
    assert_eq!(extraction.offer.messages[0].name, "Words");
    let skipped: Vec<(&str, &str)> = extraction
        .offer
        .skipped
        .iter()
        .map(|s| (s.name.as_str(), s.problem.as_str()))
        .collect();
    assert_eq!(skipped.len(), 1, "{skipped:?}");
    assert_eq!(skipped[0].0, "Shared");
    assert!(
        skipped[0].1.contains(
            "the reader names a crate's own types, primitives, `()`, and of the standard library's `String`, `Vec`, `VecDeque`, `Option`, `Box`, `HashMap`, and `BTreeMap`"
        ),
        "{}",
        skipped[0].1
    );
}
