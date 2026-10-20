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
    gdl_gen::resolve(&spec, &target()).map_err(|e| e.to_string())
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
    let error = gdl_gen::resolve(&spec, &target())
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
/// defined in a private module and not exported has none: resolution says
/// so, read from source, before anything is generated. Exported, it is
/// named by the export's path, and its contract compiles.
#[test]
fn a_message_in_a_private_module_needs_an_export() {
    let spec = gdl_gen::Spec::load(&fixtures().join("private.toml")).expect("spec");
    let error = gdl_gen::resolve(&spec, &target())
        .expect_err("private")
        .to_string();
    assert_eq!(
        error,
        "message `Prompt` (A, B): a message must be public at the path where it is defined, or exported with `#[export]`: `private_protocols::inner::Prompt` is not"
    );
    let spec = gdl_gen::Spec::load(&fixtures().join("exported.toml")).expect("spec");
    let resolution = gdl_gen::resolve(&spec, &target()).expect("resolved");
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
        // A contract spec: a graph spec's manifest is built without a
        // program.
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/asr/gdl.toml"))
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

/// `cargo gdl extract --catalog` adds a crate's offer, read from source, to
/// the working catalogue as `<name>/<version>.toml`, and `cargo gdl catalog`
/// prints the catalogue's index.
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
    let entry =
        gdl_gen::Entry::from_toml(&std::fs::read_to_string(&entry).expect("entry")).expect("entry");
    assert_eq!(entry.offer.messages.len(), 6);
    // A path package: when it was added, and no registry checksum.
    assert_eq!(
        entry.added.len(),
        "2026-10-18T21:04:05Z".len(),
        "{}",
        entry.added
    );
    assert_eq!(entry.checksum, None);
    // `cargo gdl catalog` prints the index of what was added.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "catalog"])
        .arg(&catalogue)
        .output()
        .expect("cargo gdl");
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.starts_with("entries:\n  chat-protocols@0.0.0  path+file://"),
        "{text}"
    );
    assert!(
        text.contains("  Words: chat_protocols::engine::Words (chat-protocols 0.0.0)\n"),
        "{text}"
    );
    let _ = std::fs::remove_dir_all(&catalogue);
}

/// `--verify` builds the crate and compares what its markers register with
/// what was read: an export agrees; a message a macro makes is registered
/// and not read, which it reports.
#[test]
fn verify_compares_the_build_with_the_source() {
    let read = |name: &str| {
        let krate = gdl_gen::Crate::Path(fixtures().join(name));
        let extracted =
            gdl_gen::extract(&krate, &target(), &gdl_extract::Cargo::trusted()).expect("extracted");
        gdl_gen::verify(&extracted, &target())
    };
    read("exported-protocols").expect("an export agrees");
    // The source reader's own fixture of Rust read as the compiler reads
    // it: the build agrees with every case.
    let quirks = gdl_gen::Crate::Path(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-extract/tests/fixtures/read/quirks"),
    );
    let extracted =
        gdl_gen::extract(&quirks, &target(), &gdl_extract::Cargo::trusted()).expect("extracted");
    gdl_gen::verify(&extracted, &target()).expect("the build agrees with the reader");
    let error = read("macro-made")
        .expect_err("a macro made a message")
        .to_string();
    assert_eq!(
        error,
        "registered by the build, not read from source: message `Hidden` at `macro_made::Hidden`, local"
    );
}

/// A directory whose `.cargo/config.toml` takes crates.io from
/// `tests/fixtures/registry`, offline: a vendored registry of crates that
/// are read, never built. Fetches under it see that registry, with its
/// source id and checksums, as they would see crates.io.
fn offline_registry(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gdl-registry-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join(".cargo")).expect("dir");
    let registry = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/registry");
    std::fs::write(
        dir.join(".cargo/config.toml"),
        format!(
            "[net]\noffline = true\n\n[source.crates-io]\nreplace-with = \"fixture\"\n\n[source.fixture]\ndirectory = {}\n",
            toml_string(&registry)
        ),
    )
    .expect("config");
    dir
}

/// A spec lists crates by name and version: the generator fetches them,
/// reads their source, and resolves names to messages from the registry.
#[test]
fn a_spec_lists_crates_by_version() {
    let dir = offline_registry("spec");
    std::fs::write(
        dir.join("graph.gdl"),
        "name = \"versioned\", schema = 1;\nsource A: Words -> stream Token;\nsink   Engine: Words -> stream Token;\ntopology: A => Engine;\n",
    )
    .expect("graph");
    std::fs::write(
        dir.join("gdl.toml"),
        format!(
            "[graph]\npath = \"graph.gdl\"\n[messages]\ncrates = [\"regproto@0.1.0\"]\n[nodes]\ncrates = [\"regnodes@0.1.0\"]\n[gdl]\npath = {}\n[segment.all]\nnodes = [\"A\", \"Engine\"]\n",
            toml_string(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl"))
        ),
    )
    .expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("spec");
    let resolution = gdl_gen::resolve(&spec, &dir.join("target")).expect("resolved");
    let words = &resolution.messages["Words"];
    assert_eq!(words.path, "regproto::Words");
    assert_eq!(
        (
            words.origin.package.as_str(),
            words.origin.version.as_str(),
            words.origin.source.as_str()
        ),
        (
            "regproto",
            "0.1.0",
            "registry+https://github.com/rust-lang/crates.io-index"
        )
    );
    assert!(words.wire);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Generated code names a crate by its library, and a spec that lists a
/// crate by version names the library for the package: a crate whose
/// library is named otherwise is refused, by name.
#[test]
fn a_crate_listed_by_version_names_its_library_for_its_package() {
    let dir = offline_registry("renamed");
    std::fs::write(
        dir.join("graph.gdl"),
        "name = \"renamed\", schema = 1;\nsource A: Words -> stream Words;\nsink   B: Words -> stream Words;\ntopology: A => B;\n",
    )
    .expect("graph");
    std::fs::write(
        dir.join("gdl.toml"),
        "[graph]\npath = \"graph.gdl\"\n[messages]\ncrates = [\"regrenamed@0.1.0\"]\n[gdl]\nversion = \"0.1\"\n",
    )
    .expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("spec");
    let error = gdl_gen::resolve(&spec, &dir.join("target"))
        .expect_err("renamed")
        .to_string();
    assert_eq!(
        error,
        "regrenamed@0.1.0: its library is `renamed`, not `regrenamed`; a crate listed by version must name its library for its package"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// `cargo gdl extract name@version --catalog` fetches the crate, reads it,
/// and adds it to the catalogue with the registry's checksum.
#[test]
fn a_registry_crate_enters_the_catalogue_with_its_checksum() {
    let dir = offline_registry("catalogue");
    let catalogue = dir.join("catalogue");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "extract", "regnodes@0.1.0", "--catalog"])
        .arg(&catalogue)
        .arg("--target-dir")
        .arg(dir.join("target"))
        .output()
        .expect("cargo gdl");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = std::fs::read_to_string(catalogue.join("regnodes/0.1.0.toml")).expect("entry");
    let entry = gdl_gen::Entry::from_toml(&text).expect("entry");
    // The fixture's `.cargo-checksum.json`, as Cargo locked it.
    assert_eq!(
        entry.checksum.as_deref(),
        Some("677ebc6731d74ce2567ec1e7a5928b902f6f5d8a3ad7b57a332fdb6475142189")
    );
    let engine = &entry.offer.nodes[0];
    assert_eq!(
        (engine.name.as_str(), engine.role.name()),
        ("Engine", "sink")
    );
    let req = engine.ports[0].head.as_ref().expect("Req");
    assert_eq!(
        (req.package.as_str(), req.path.as_str()),
        ("regproto", "regproto::Words")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// What each listed crate of the chat example offers, as a catalogue would
/// hold it: read once, from source.
fn chat_offers() -> Vec<gdl_gen::Offer> {
    let chat = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/chat");
    let workspace =
        gdl_extract::Workspace::load(&chat.join("Cargo.toml"), &gdl_extract::Cargo::trusted())
            .expect("metadata");
    ["protocols", "front-nodes", "engine-mock", "post-nodes"]
        .iter()
        .map(|dir| {
            let extraction = workspace.extract_in(&chat.join(dir)).expect("a package");
            assert!(extraction.problems.is_empty(), "{:?}", extraction.problems);
            extraction.offer
        })
        .collect()
}

/// `check` validates a spec as generation does and writes nothing: from
/// source, or from offers a catalogue holds, to the same manifest and the
/// committed crossing report. A crate the catalogue lacks is named.
#[test]
fn a_check_agrees_with_generation() {
    let chat = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/chat");
    let spec = gdl_gen::Spec::load(&chat.join("gdl.toml")).expect("spec");
    let read = gdl_gen::check(&spec, &target(), gdl_gen::Offers::Read).expect("read");
    let committed =
        std::fs::read_to_string(chat.join("segments/crossings.txt")).expect("crossings");
    assert!(
        committed.contains(&read.crossings),
        "{}\nvs\n{committed}",
        read.crossings
    );
    assert!(
        read.mermaid.contains("\nflowchart LR\n"),
        "{}",
        read.mermaid
    );
    let offers = chat_offers();
    let lent: Vec<&gdl_gen::Offer> = offers.iter().collect();
    let given = gdl_gen::check(&spec, &target(), gdl_gen::Offers::Given(&lent)).expect("given");
    assert_eq!(given.manifest, read.manifest);
    assert_eq!(given.crossings, read.crossings);
    let without_engine: Vec<&gdl_gen::Offer> = offers
        .iter()
        .filter(|o| o.package != "chat-engine-mock")
        .collect();
    let error = gdl_gen::check(&spec, &target(), gdl_gen::Offers::Given(&without_engine))
        .expect_err("no engine")
        .to_string();
    assert!(
        error.starts_with("`chat-engine-mock` (")
            && error.ends_with(") is not in the catalogue: add it first"),
        "{error}"
    );
}

/// An archive whose crates all come from a registry or git builds images
/// too: a `Dockerfile` per segment, from the archive alone, and a script
/// that builds each. An archive naming a directory has neither (`export.rs`
/// lists the chat example's entries exactly).
#[test]
fn an_archive_of_fetchable_crates_builds_images() {
    use std::io::Read as _;
    let dir = offline_registry("images");
    std::fs::write(
        dir.join("graph.gdl"),
        "name = \"imaged\", schema = 1;\nsource A: Words -> stream Token;\nsink   Engine: Words -> stream Token;\ntopology: A => Engine;\n",
    )
    .expect("graph");
    std::fs::write(
        dir.join("gdl.toml"),
        "[graph]\npath = \"graph.gdl\"\n[messages]\ncrates = [\"regproto@0.1.0\"]\n[nodes]\ncrates = [\"regnodes@0.1.0\"]\n[gdl]\ngit = \"https://example.com/gdl\"\nrev = \"abc123\"\n[segment.front]\nnodes = [\"A\"]\n[segment.engine]\nnodes = [\"Engine\"]\n",
    )
    .expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("spec");
    let bytes =
        gdl_gen::export(&spec, &dir.join("target"), gdl_gen::Offers::Read).expect("exported");
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let mut files = std::collections::BTreeMap::new();
    for entry in archive.entries().expect("entries") {
        let mut entry = entry.expect("entry");
        let name = entry.path().expect("path").display().to_string();
        let mut text = String::new();
        entry.read_to_string(&mut text).expect("text");
        files.insert(name, text);
    }
    let dockerfile = &files["imaged/engine/Dockerfile"];
    for line in [
        "FROM rust:${RUST_VERSION}-slim-bookworm AS build\n",
        "COPY [\"contract\", \"contract\"]\n",
        "COPY [\"engine\", \"engine\"]\n",
        "RUN [\"cargo\", \"build\", \"--release\", \"--manifest-path\", \"./engine/Cargo.toml\"]\n",
        "COPY --from=build [\"/src/engine/target/release/imaged-engine\", \"/usr/local/bin/imaged-engine\"]\n",
    ] {
        assert!(dockerfile.contains(line), "{line} in:\n{dockerfile}");
    }
    assert!(files.contains_key("imaged/front/Dockerfile"));
    let script = &files["imaged/build-images.sh"];
    assert!(
        script.contains("docker build -f './engine/Dockerfile' -t \"${GDL_IMAGE_PREFIX:-}\"'imaged-engine' \"$@\" .\n"),
        "{script}"
    );
    assert!(
        files["imaged/engine/Cargo.toml"]
            .contains("gdl = { git = \"https://example.com/gdl\", rev = \"abc123\" }")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A Compose deployment needs a frontend: a source without one takes its
/// requests on stdin, and a container gives it none, so the segment would
/// stop at once, ready.
#[test]
fn compose_refuses_a_source_without_a_frontend() {
    let dir = offline_registry("compose-no-frontend");
    std::fs::write(
        dir.join("graph.gdl"),
        "name = \"imaged\", schema = 1;\nsource A: Words -> stream Token;\nsink   Engine: Words -> stream Token;\ntopology: A => Engine;\n",
    )
    .expect("graph");
    std::fs::write(
        dir.join("gdl.toml"),
        "[graph]\npath = \"graph.gdl\"\n[messages]\ncrates = [\"regproto@0.1.0\"]\n[nodes]\ncrates = [\"regnodes@0.1.0\"]\n[gdl]\ngit = \"https://example.com/gdl\"\nrev = \"abc123\"\n[segment.front]\nnodes = [\"A\"]\n[segment.engine]\nnodes = [\"Engine\"]\n",
    )
    .expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("spec");
    let Err(error) = gdl_gen::compose_files(&spec, &dir.join("out"), &dir.join("target")) else {
        panic!("a deployment whose source reads stdin was accepted");
    };
    assert!(error.to_string().contains("frontend"), "{error}");
    let _ = std::fs::remove_dir_all(&dir);
}
