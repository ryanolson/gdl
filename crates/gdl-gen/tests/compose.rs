//! Composition end to end: a graph file's message names resolve against
//! what real crates offer, read from their source by `gdl-extract`, and a
//! contract generated from the resolution compiles.

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
    gdl_gen::resolve(&spec).map_err(|e| e.to_string())
}

/// One protocols crate holds every message. A type in a module resolves to
/// its definition, though the crate also re-exports it at the root without
/// `export`; a type without serde resolves too, as a local type.
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
/// second crate re-exports the first's `Token` without `export`, so it
/// offers only its own `Chunk`.
#[test]
fn a_name_two_crates_define_resolves_only_when_pinned() {
    let error = resolve("two.toml").expect_err("ambiguous");
    assert_eq!(
        error,
        "message `Chunk` (Frontend, Preprocess, Number) is more than one message in the \
         listed crates: `chat_protocols::Chunk` (chat-protocols 0.0.0), \
         `other_protocols::Chunk` (other-protocols 0.0.0); choose one under [messages] pin"
    );
    let resolution = resolve("pinned.toml").expect("pinned");
    assert_eq!(resolution.messages["Chunk"].path, "chat_protocols::Chunk");
    assert_eq!(
        resolution.messages["Token"].path,
        "chat_protocols::engine::Token"
    );
}

/// The empty crate's library is named `gen`, a Rust 2024 keyword, which the
/// reader reads like any other.
#[test]
fn every_resolution_problem_is_reported_at_once() {
    let error = resolve("bad.toml").expect_err("problems");
    assert_eq!(
        error,
        "[messages] crate `empty-protocols` offers no messages: derive `Message` on its \
         message types, or export them\n\
         message `Txet` (Preprocess, Format, Pad) is in no listed crate; did you mean `Text`?"
    );
}

/// A listed crate offers what it defines and what it exports, nothing
/// else: `vbridge` re-exports `Hidden` from `vproto` 0.1.0 without
/// `export`, so it is not offered; and `vproto` 0.2.0's `Chunk` is the one
/// a pin to `vproto::Chunk` takes.
#[test]
fn a_listed_crate_offers_only_what_it_defines_and_exports() {
    let error = resolve("versions.toml").expect_err("Hidden is not offered");
    assert_eq!(
        error,
        "message `Hidden` (A, B) is in no listed crate; the listed crates offer `Bridge`, `Chunk`"
    );
    let resolution = resolve("versions-pinned.toml").expect("pinned");
    let chunk = &resolution.messages["Chunk"];
    assert_eq!((chunk.origin.version.as_str(), chunk.wire), ("0.2.0", true));
}

/// A listed directory is one package, whichever workspace read it first:
/// the bridge's workspace holds both versions of `vproto`, and listing it
/// first still takes the listed directory's version.
#[test]
fn a_listed_directory_names_one_package_in_any_order() {
    for spec in ["versions-pinned.toml", "versions-bridge-first.toml"] {
        let resolution = resolve(spec).expect(spec);
        let chunk = &resolution.messages["Chunk"];
        assert_eq!(chunk.origin.version, "0.2.0", "{spec}");
    }
}

/// A crate that only exports a message offers it under its own path, and
/// the name resolves to the message where it is defined; listed with the
/// defining crate, it is one message, not two.
#[test]
fn an_export_resolves_to_its_origin() {
    for spec in ["facade.toml", "facade-both.toml"] {
        let resolution = resolve(spec).expect("resolved");
        let chunk = &resolution.messages["Chunk"];
        assert_eq!(chunk.path, "facade_protocols::Chunk", "{spec}");
        assert_eq!(chunk.origin.path, "other_protocols::Chunk", "{spec}");
        assert_eq!(chunk.origin.package, "other-protocols", "{spec}");
    }
}

/// Names are checked before any crate is read: a graph whose edges
/// disagree fails with the file's positions.
#[test]
fn a_graph_whose_names_disagree_fails_before_any_crate_is_read() {
    let dir = std::env::temp_dir().join(format!("gdl-gen-compose-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    let graph = std::fs::read_to_string(fixtures().join("chat.gdl"))
        .expect("graph")
        .replace("sink     Engine: Words", "sink     Engine: Tokens");
    std::fs::write(dir.join("chat.gdl"), graph).expect("graph");
    // A crate that does not exist: reading it would fail another way.
    let spec = format!(
        "[graph]\npath = \"chat.gdl\"\n[messages]\ncrates = [{}]\n[gdl]\nversion = \"0.1\"\n",
        toml_string(&fixtures().join("chat-protocols"))
    );
    std::fs::write(dir.join("gdl.toml"), spec).expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("spec");
    let error = gdl_gen::resolve(&spec)
        .expect_err("names differ")
        .to_string();
    assert_eq!(
        error,
        format!(
            "{}:6:18: `Engine` takes `Tokens`, but `Truncate` sends `Words`",
            dir.join("chat.gdl").display()
        )
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn toml_string(path: &Path) -> String {
    format!("{:?}", path.to_string_lossy())
}

/// A generated contract names a message by a public path. A message
/// defined in a private module and not exported has none, and the error
/// says why; exported, it is named by the export's path and compiles.
#[test]
fn a_message_in_a_private_module_needs_an_export() {
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
    let spec = gdl_gen::Spec::load(&fixtures().join("exported.toml")).expect("spec");
    let resolution = gdl_gen::resolve(&spec).expect("resolved");
    assert_eq!(
        resolution.messages["Prompt"].path,
        "exported_protocols::Prompt"
    );
    gdl_gen::diagram(&spec, &target(), gdl_gen::Format::Mermaid).expect("diagram");
}

/// The manifest program is run by cargo, as it was built: a target named
/// explicitly puts it in another directory, and `prefer-dynamic` makes it
/// load the standard library at run time. Both settings at once, in a
/// target directory of their own, so neither reaches the other tests'
/// build. Slow on its first run: it builds gdl for that target.
#[test]
fn the_manifest_program_runs_as_cargo_built_it() {
    let host = String::from_utf8(
        std::process::Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .arg("-vV")
            .output()
            .expect("rustc")
            .stdout,
    )
    .expect("utf-8")
    .lines()
    .find_map(|line| line.strip_prefix("host: ").map(str::to_string))
    .expect("host");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["diagram", "--spec"])
        .arg(fixtures().join("exported.toml"))
        .args(["--format", "dot"])
        .arg("--target-dir")
        .arg(target().with_file_name("gdl-gen-tests-target"))
        .env("CARGO_BUILD_TARGET", host)
        .env("RUSTFLAGS", "-C prefer-dynamic")
        .output()
        .expect("gdl-gen");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("digraph"));
}

/// `cargo gdl extract` writes a crate's offer, read from source, as a
/// working-catalogue entry, `<name>/<version>.toml`.
#[test]
fn extract_writes_a_working_catalogue_entry() {
    let catalogue = std::env::temp_dir().join(format!("gdl-catalogue-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&catalogue);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "extract"])
        .arg(fixtures().join("chat-protocols"))
        .arg("--catalog")
        .arg(&catalogue)
        .output()
        .expect("cargo gdl");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let entry = catalogue.join("chat-protocols/0.0.0.toml");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        entry.display().to_string()
    );
    let offer = gdl_extract_offer(&entry);
    assert_eq!(offer.messages.len(), 6);
    let _ = std::fs::remove_dir_all(&catalogue);
}

fn gdl_extract_offer(entry: &Path) -> gdl_gen::Offer {
    gdl_gen::Offer::from_toml(&std::fs::read_to_string(entry).expect("entry")).expect("offer")
}

/// `--verify` builds the crate and compares what its markers register with
/// what was read: an export agrees; a message a macro makes is registered
/// and not read, which it reports.
#[test]
fn verify_compares_the_build_with_the_source() {
    let read = |name: &str| {
        let krate = gdl_gen::Crate::Path(fixtures().join(name));
        let extracted = gdl_gen::extract(&krate, &target()).expect("extracted");
        gdl_gen::verify(&extracted, &target())
    };
    read("exported-protocols").expect("an export agrees");
    // The source reader's own fixture of Rust read as the compiler reads
    // it: the build agrees with every case.
    let quirks = gdl_gen::Crate::Path(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-extract/tests/fixtures/read/quirks"),
    );
    let extracted = gdl_gen::extract(&quirks, &target()).expect("extracted");
    gdl_gen::verify(&extracted, &target()).expect("the build agrees with the reader");
    let error = read("macro-made")
        .expect_err("a macro made a message")
        .to_string();
    assert_eq!(
        error,
        "registered by the build, not read from source: message `Hidden` at `macro_made::Hidden`, local"
    );
}
