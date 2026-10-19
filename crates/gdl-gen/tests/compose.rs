//! Composition end to end: a graph file's message names resolve against the
//! catalogue that real protocols crates register with `derive(gdl_message::Message)`,
//! read by compiling and running a program that links them.

use std::path::{Path, PathBuf};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compose")
}

/// Nested cargo runs get a target directory of their own: the outer
/// `cargo test` may still hold the lock on its own.
fn target() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"))
        .join("gdl-gen-tests")
}

fn resolve(spec: &str) -> Result<gdl_gen::Resolution, String> {
    let spec = gdl_gen::Spec::load(&fixtures().join(spec)).map_err(|e| e.to_string())?;
    gdl_gen::resolve(&spec, &target()).map_err(|e| e.to_string())
}

/// One protocols crate holds every message. A type in a module resolves to
/// its definition, though the crate also re-exports it at the root; a type
/// without serde resolves too, as a local type.
#[test]
fn one_protocols_crate_resolves_the_whole_graph() {
    let resolution = resolve("one.toml").expect("resolved");
    assert_eq!(
        resolution.to_string(),
        "\
Chunk   chat_protocols::Chunk          wire
Piece   chat_protocols::Piece          local
Prompt  chat_protocols::Prompt         wire
Text    chat_protocols::Text           wire
Token   chat_protocols::engine::Token  wire
Words   chat_protocols::engine::Words  wire
"
    );
}

/// Two listed crates define `Chunk`: ambiguous until a pin chooses. The
/// second crate re-exports the first's `Token`, which stays one message.
#[test]
fn a_name_two_crates_define_resolves_only_when_pinned() {
    let error = resolve("two.toml").expect_err("ambiguous");
    assert_eq!(
        error,
        "message `Chunk` (Frontend, Preprocess, Number) is in more than one listed crate: \
         `chat_protocols::Chunk`, `other_protocols::Chunk`; choose one under [messages] pin"
    );
    let resolution = resolve("pinned.toml").expect("pinned");
    assert_eq!(resolution.messages["Chunk"].path, "chat_protocols::Chunk");
    assert_eq!(
        resolution.messages["Token"].path,
        "chat_protocols::engine::Token"
    );
}

/// The empty crate's library is named `gen`, a Rust 2024 keyword: the
/// catalogue program must still link it.
#[test]
fn every_resolution_problem_is_reported_at_once() {
    let error = resolve("bad.toml").expect_err("problems");
    assert_eq!(
        error,
        "[messages] crate `empty-protocols` offers no messages: derive `gdl_message::Message` on its \
         message types\n\
         message `Txet` (Preprocess, Format, Pad) is in no listed crate; did you mean `Text`?"
    );
}

/// A listed crate is one package: its name and its version. Another version
/// of it in the program, here linked through `vbridge`, offers nothing: its
/// `Hidden` does not resolve, and its `Chunk` neither makes the name
/// ambiguous nor answers a pin.
#[test]
fn only_the_listed_version_of_a_package_offers_messages() {
    let error = resolve("versions.toml").expect_err("Hidden is unlisted");
    assert_eq!(
        error,
        "message `Hidden` (A, B) is in no listed crate; `vproto 0.1.0` defines it, but is not \
         in [messages] crates"
    );
    let resolution = resolve("versions-pinned.toml").expect("pinned");
    let chunk = &resolution.messages["Chunk"];
    assert_eq!((chunk.version.as_str(), chunk.wire), ("0.2.0", true));
}

/// Only a `gdl-message` linked into the catalogue program conflicts: one that a
/// message crate reaches through a proc macro, takes for another platform,
/// or takes for its build script does not.
#[test]
fn a_gdl_that_is_not_linked_is_no_conflict() {
    let resolution = resolve("platform.toml").expect("resolved");
    assert_eq!(
        resolution.messages["Chunk"].path,
        "platform_protocols::Chunk"
    );
}

/// A build that fails is reported as the compiler reports it, even when
/// the failure is a duplicate catalogue inside a proc macro: only the
/// catalogue program itself, once it runs, can link two catalogues.
#[test]
fn a_build_failure_is_not_taken_for_two_catalogues() {
    let error = resolve("macro-panic.toml").expect_err("the build fails");
    assert!(error.contains("proc macro panicked"), "{error}");
    assert!(!error.contains("links two message catalogues"), "{error}");
}

/// A program holds one message catalogue. A message crate that links
/// another gdl-message's catalogue into it is refused, and the error names it
/// and the crates that use it.
#[test]
fn a_message_crate_on_another_gdl_is_refused() {
    let error = resolve("foreign.toml").expect_err("two runtimes");
    for part in [
        "the catalogue program links two message catalogues",
        "`gdl-message 0.0.1`",
        "used by `foreign-protocols`",
        "depend on the gdl-message of the gdl that [gdl] names",
    ] {
        assert!(error.contains(part), "{part} in:\n{error}");
    }
}

/// Names are checked before anything is compiled: a graph whose edges
/// disagree fails with the file's positions, and builds no catalogue.
#[test]
fn a_graph_whose_names_disagree_fails_before_any_build() {
    let dir = std::env::temp_dir().join(format!("gdl-gen-compose-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    let graph = std::fs::read_to_string(fixtures().join("chat.gdl"))
        .expect("graph")
        .replace("sink     Engine: Words", "sink     Engine: Tokens");
    std::fs::write(dir.join("chat.gdl"), graph).expect("graph");
    let spec = format!(
        "[graph]\npath = \"chat.gdl\"\n[messages]\ncrates = [{}]\n[gdl]\nversion = \"0.1\"\n",
        toml_string(&fixtures().join("chat-protocols"))
    );
    std::fs::write(dir.join("gdl.toml"), spec).expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("spec");
    // An empty target directory: a build there would take far longer than
    // this test allows, and there is nothing in it to reuse.
    let error = gdl_gen::resolve(&spec, &dir.join("target"))
        .expect_err("names differ")
        .to_string();
    assert_eq!(
        error,
        format!(
            "{}:6:18: `Engine` takes `Tokens`, but `Truncate` sends `Words`",
            dir.join("chat.gdl").display()
        )
    );
    assert!(!dir.join("target").exists(), "a catalogue was built");
    let _ = std::fs::remove_dir_all(&dir);
}

fn toml_string(path: &Path) -> String {
    format!("{:?}", path.to_string_lossy())
}

/// A generated contract names each message where it is defined, so a
/// message in a private module cannot be named, and the error says why.
#[test]
fn a_message_must_be_public_where_it_is_defined() {
    let spec = gdl_gen::Spec::load(&fixtures().join("private.toml")).expect("spec");
    let error = gdl_gen::diagram(&spec, &target(), gdl_gen::Format::Mermaid)
        .expect_err("private")
        .to_string();
    assert!(
        error.contains(
            "a message must be public at the path where it is defined: `private_protocols::inner::Prompt`"
        ),
        "{error}"
    );
}
