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

/// Nodes in module files, reached through a root re-export and a glob;
/// ports through imports, a module alias, `super`, streamed requests with
/// and without a head, and a message offered only through an export.
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
    assert_eq!(extraction.problems.len(), 2, "{:#?}", extraction.problems);
    assert!(
        extraction.problems[0]
            .contains("node `Bad`: port `Req`: `String` names no type that derives `Message`"),
        "{}",
        extraction.problems[0]
    );
    assert!(
        extraction.problems[1]
            .contains("node `Buried`: the crate root does not reach it as `read_engine::Buried`"),
        "{}",
        extraction.problems[1]
    );
    assert!(
        extraction.problems[0].contains("engine/src/lib.rs:"),
        "{}",
        extraction.problems[0]
    );
}

#[test]
fn an_unknown_package_is_an_error() {
    let error = workspace().extract("no-such-crate").expect_err("unknown");
    assert!(error.to_string().contains("no-such-crate"), "{error}");
}
