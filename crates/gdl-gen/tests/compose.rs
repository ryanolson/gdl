//! Composition end to end: a graph file's message names resolve against
//! what real crates offer, read from their source by `gdl-extract`, and a
//! contract generated from the resolution compiles.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::CARGO_LIMIT;

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
    let temp = tempfile::Builder::new()
        .prefix("gdl-gen-compose-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
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
        // The manifest program has no lock, so online Cargo would read
        // crates.io's index to resolve it.
        .env("CARGO_NET_OFFLINE", "true")
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
    let temp = tempfile::Builder::new()
        .prefix("gdl-catalogue-")
        .tempdir()
        .expect("dir");
    let catalogue = temp.path();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
        .args(["gdl", "extract"])
        .arg(fixtures().join("chat-protocols"))
        .arg("--catalog")
        .arg(catalogue)
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
        .arg(catalogue)
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
        "registered by the build, not read from source: message `Hidden` at `macro_made::Hidden`, local (codec msgpack)"
    );
}

/// A directory whose `.cargo/config.toml` takes crates.io from
/// `tests/fixtures/registry`, offline: a vendored registry of crates that
/// are read, never built. Fetches under it see that registry, with its
/// source id and checksums, as they would see crates.io.
fn offline_registry(name: &str) -> tempfile::TempDir {
    let temp = tempfile::Builder::new()
        .prefix(&format!("gdl-registry-{name}-"))
        .tempdir()
        .expect("dir");
    let dir = temp.path();
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
    temp
}

/// A spec lists crates by name and version: the generator fetches them,
/// reads their source, and resolves names to messages from the registry.
#[test]
fn a_spec_lists_crates_by_version() {
    let temp = offline_registry("spec");
    let dir = temp.path();
    std::fs::write(
        dir.join("graph.gdl"),
        "name = \"versioned\", schema = 1;\nsource A: Words -> stream Token;\nsink   Engine: Words -> stream Token;\ntopology: A => Engine;\n",
    )
    .expect("graph");
    std::fs::write(
        dir.join("gdl.toml"),
        format!(
            "[graph]\npath = \"graph.gdl\"\n[messages]\ncrates = [\"regproto@0.1.0\"]\n[nodes]\ncrates = [\"regnodes@0.1.0\"]\n[gdl]\npath = {}\n[segment.all]\nnodes = [\"A\", \"Engine\"]\n",
            toml_string(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-runtime"))
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
}

/// Generated code names a crate by its library, and a spec that lists a
/// crate by version names the library for the package: a crate whose
/// library is named otherwise is refused, by name.
#[test]
fn a_crate_listed_by_version_names_its_library_for_its_package() {
    let temp = offline_registry("renamed");
    let dir = temp.path();
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
}

/// `cargo gdl extract name@version --catalog` fetches the crate, reads it,
/// and adds it to the catalogue with the registry's checksum.
#[test]
fn a_registry_crate_enters_the_catalogue_with_its_checksum() {
    let temp = offline_registry("catalogue");
    let dir = temp.path();
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
}

/// `cargo gdl extract name@version --verify` on a fetched crate that does
/// not depend on gdl-message says so and builds nothing: a build of it
/// could register nothing. The registry fixture's crates name gdl-message
/// only in their markers, which the reader reads as text.
#[test]
fn verify_of_a_fetched_crate_without_gdl_message_says_so() {
    let temp = offline_registry("verify");
    let dir = temp.path();
    let output = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO_BIN_EXE_cargo-gdl"),
            "gdl",
            "extract",
            "regproto@0.1.0",
            "--verify",
        ])
        .arg("--target-dir")
        .arg(dir.join("target"))
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo gdl");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr
            .contains("regproto does not depend on gdl-message, so it registers nothing to verify"),
        "{stderr}"
    );
}

/// A Git crate can build without having a release in the registry.
#[test]
fn verify_keeps_a_git_crates_source() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-git-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let write = |path: &str, text: &str| {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
        std::fs::write(path, text).expect("file");
    };
    write(
        ".cargo/config.toml",
        &format!(
            "[net]\noffline = false\n\n[source.crates-io]\nreplace-with = \"fixture\"\n\n[source.fixture]\ndirectory = {}\n",
            toml_string(&dir.join("vendor"))
        ),
    );
    // Only dependency resolution matters here, so the catalogue is empty.
    write(
        "vendor/gdl-message/Cargo.toml",
        "[package]\nname = \"gdl-message\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    );
    write(
        "vendor/gdl-message/src/lib.rs",
        "pub struct Catalogue;\n\
         impl Catalogue {\n\
         pub fn linked() -> Self { Self }\n\
         pub fn to_toml(&self) -> &'static str { \"\" }\n\
         }\n",
    );
    write(
        "vendor/gdl-message/.cargo-checksum.json",
        &format!(r#"{{"files":{{}},"package":"{}"}}"#, "a".repeat(64)),
    );
    write(
        "repo/Cargo.toml",
        "[package]\nname = \"verify-input\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[dependencies]\ngdl-message = \"=0.1.0\"\n",
    );
    write("repo/src/lib.rs", "pub fn ready() -> bool { true }\n");
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .current_dir(dir.join("repo"))
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .output()
            .expect("git");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("text")
            .trim()
            .to_string()
    };
    git(&["init", "--quiet"]);
    git(&["add", "."]);
    git(&["commit", "--quiet", "-m", "fixture"]);
    let rev = git(&["rev-parse", "HEAD"]);
    let url = format!("file://{}", dir.join("repo").display());
    write(
        "probe/Cargo.toml",
        &format!(
            "[package]\nname = \"verify-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n\n[dependencies]\ngdl-message = \"=0.1.0\"\nverify_input = {{ package = \"verify-input\", git = {url:?}, rev = {rev:?} }}\n"
        ),
    );
    write(
        "probe/src/main.rs",
        "fn main() {\nassert!(verify_input::ready());\nprint!(\"{}\", gdl_message::Catalogue::linked().to_toml());\n}\n",
    );
    let target = dir.join("target");
    let probe = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO"),
            "run",
            "--quiet",
            "--manifest-path",
        ])
        .arg(dir.join("probe/Cargo.toml"))
        .arg("--target-dir")
        .arg(&target)
        .current_dir(dir.join("probe"))
        .output()
        .expect("probe");
    assert!(
        probe.status.success(),
        "{}",
        String::from_utf8_lossy(&probe.stderr)
    );
    gdl_message::Catalogue::from_toml(&String::from_utf8_lossy(&probe.stdout))
        .expect("the compiled probe prints a catalogue");
    let extracted = gdl_gen::extract(
        &gdl_gen::Crate::Git {
            name: "verify-input".into(),
            url,
            rev,
            path: PathBuf::new(),
        },
        &target,
        &gdl_extract::Cargo::trusted(),
    )
    .expect("Git extraction");
    gdl_gen::verify(&extracted, &target).expect("verification keeps the Git source");
}

/// A crate that takes `gdl-message` from Git verifies: the program
/// `--verify` builds takes it from the same Git source, under the same
/// reference, as the crate's lock does. From the registry, the program
/// finds no `gdl-message`; under any other reference, it links a second
/// one, and the two stop it at start-up.
///
/// The repository holds the workspace's own `gdl-message` and the crates it
/// needs, and the crate takes its default branch. Cargo's HTTP goes to a
/// dead proxy, so the run reads no registry: the workspace lock pins every
/// registry crate, and Git reads the repository from the file system.
#[test]
fn verify_takes_gdl_message_from_the_crates_git_source() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-git-message-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = dir.join("repo");
    let members = ["gdl-message", "gdl-model", "gdl-macros", "gdl-syntax"];
    for member in members {
        copy_dir(
            &root.join("crates").join(member),
            &repo.join("crates").join(member),
        );
    }
    let mut manifest: toml::Table = std::fs::read_to_string(root.join("Cargo.toml"))
        .expect("the workspace manifest")
        .parse()
        .expect("TOML");
    let workspace = manifest["workspace"].as_table_mut().expect("a workspace");
    workspace.remove("exclude");
    workspace.insert(
        "members".into(),
        members.map(|m| format!("crates/{m}")).to_vec().into(),
    );
    std::fs::write(repo.join("Cargo.toml"), manifest.to_string()).expect("manifest");
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .current_dir(&repo)
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .output()
            .expect("git");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "--quiet"]);
    git(&["add", "."]);
    git(&["commit", "--quiet", "-m", "fixture"]);
    let url = format!("file://{}", repo.display());

    let app = dir.join("app");
    std::fs::create_dir_all(app.join("src")).expect("dir");
    std::fs::write(
        app.join("Cargo.toml"),
        format!(
            "[package]\nname = \"verify-git-message\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [workspace]\n[dependencies]\ngdl-message = {{ git = {url:?} }}\n"
        ),
    )
    .expect("manifest");
    std::fs::write(
        app.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Words;\n",
    )
    .expect("source");
    std::fs::copy(root.join("Cargo.lock"), app.join("Cargo.lock")).expect("the workspace lock");

    let output = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO_BIN_EXE_cargo-gdl"),
            "gdl",
            "extract",
        ])
        .arg(&app)
        .arg("--verify")
        .arg("--target-dir")
        .arg(target())
        .env("CARGO_HTTP_PROXY", "127.0.0.1:9")
        .env("CARGO_NET_RETRY", "0")
        .output()
        .expect("cargo gdl");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    let lock = std::fs::read_to_string(app.join("Cargo.lock")).expect("the crate's lock");
    assert!(
        lock.contains(&format!("source = \"git+{url}#")),
        "the crate's lock takes gdl-message from the repository:\n{lock}"
    );
    let offer =
        gdl_gen::Offer::from_toml(&String::from_utf8_lossy(&output.stdout)).expect("an offer");
    let messages: Vec<&str> = offer.messages.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(messages, ["Words"]);
}

/// Copy the directory `from` to `to`, with every file below it.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("dir");
    for entry in std::fs::read_dir(from).expect("a directory") {
        let entry = entry.expect("an entry");
        let path = entry.path();
        if entry.file_type().expect("a type").is_dir() {
            copy_dir(&path, &to.join(entry.file_name()));
        } else {
            std::fs::copy(&path, to.join(entry.file_name())).expect("a file");
        }
    }
}

/// A workspace's Git patch supplies the same package to its build and verification.
#[test]
fn verify_keeps_the_crates_git_patch() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-git-patch-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let write = |path: &str, text: &str| {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
        std::fs::write(path, text).expect("file");
    };
    std::fs::create_dir_all(dir.join("vendor")).expect("vendor");
    write(
        ".cargo/config.toml",
        &format!(
            "[net]\noffline = false\n\n[source.crates-io]\nreplace-with = \"fixture\"\n\n[source.fixture]\ndirectory = {}\n",
            toml_string(&dir.join("vendor"))
        ),
    );
    write(
        "repo/Cargo.toml",
        "[package]\nname = \"gdl-message\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    );
    write(
        "repo/src/lib.rs",
        "pub struct Catalogue;\nimpl Catalogue {\n\
         pub fn linked() -> Self { Self }\n\
         pub fn to_toml(&self) -> &'static str { \"\" }\n}\n",
    );
    for args in [
        vec!["init", "--quiet"],
        vec!["add", "."],
        vec!["commit", "--quiet", "-m", "fixture"],
    ] {
        let output = Command::new("git")
            .current_dir(dir.join("repo"))
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .output()
            .expect("git");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let url = format!("file://{}", dir.join("repo").display());
    write(
        "app/Cargo.toml",
        &format!(
            "[package]\nname = \"verify-patched-input\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [workspace]\n[dependencies]\ngdl-message = \"=0.1.0\"\n\
             [patch.crates-io]\ngdl-message = {{ git = {url:?} }}\n"
        ),
    );
    write(
        "app/src/lib.rs",
        "pub fn ready() -> bool { let _ = gdl_message::Catalogue::linked(); true }\n",
    );
    let target = dir.join("target");
    let control = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO"),
            "build",
            "--quiet",
            "--manifest-path",
        ])
        .arg(dir.join("app/Cargo.toml"))
        .arg("--target-dir")
        .arg(&target)
        .current_dir(dir.join("app"))
        .output()
        .expect("the crate's own build");
    assert!(
        control.status.success(),
        "{}",
        String::from_utf8_lossy(&control.stderr)
    );
    let lock = std::fs::read_to_string(dir.join("app/Cargo.lock")).expect("lock");
    assert!(lock.contains(&format!("source = \"git+{url}#")), "{lock}");
    let extracted = gdl_gen::extract(
        &gdl_gen::Crate::Path(dir.join("app")),
        &target,
        &gdl_extract::Cargo::trusted(),
    )
    .expect("the patched crate extracts");
    gdl_gen::verify(&extracted, &target).expect("verification keeps the crate's Git patch");
}

/// A workspace member uses its own Git dependency even when the workspace contains another copy.
/// This compiles both copies of gdl-message, so its first run takes several seconds.
#[test]
fn verify_uses_the_members_gdl_message_dependency() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-member-message-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let members = ["gdl-message", "gdl-model", "gdl-macros", "gdl-syntax"];
    let repo = dir.join("repo");
    let workspace = dir.join("workspace");
    let mut manifest: toml::Table = std::fs::read_to_string(root.join("Cargo.toml"))
        .expect("the workspace manifest")
        .parse()
        .expect("TOML");
    let table = manifest["workspace"].as_table_mut().expect("workspace");
    table.remove("exclude");
    table.insert(
        "members".into(),
        members.map(|m| format!("crates/{m}")).to_vec().into(),
    );
    for to in [&repo, &workspace] {
        for member in members {
            copy_dir(
                &root.join("crates").join(member),
                &to.join("crates").join(member),
            );
        }
        std::fs::write(to.join("Cargo.toml"), manifest.to_string()).expect("manifest");
    }
    for args in [
        vec!["init", "--quiet"],
        vec!["add", "."],
        vec!["commit", "--quiet", "-m", "fixture"],
    ] {
        let output = Command::new("git")
            .current_dir(&repo)
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .output()
            .expect("git");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    manifest["workspace"]["members"]
        .as_array_mut()
        .expect("members")
        .push("app".into());
    std::fs::write(workspace.join("Cargo.toml"), manifest.to_string()).expect("manifest");
    std::fs::copy(root.join("Cargo.lock"), workspace.join("Cargo.lock")).expect("lock");
    let url = format!("file://{}", repo.display());
    let app = workspace.join("app");
    std::fs::create_dir_all(app.join("src")).expect("dir");
    std::fs::write(
        app.join("Cargo.toml"),
        format!(
            "[package]\nname = \"verify-workspace-input\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [dependencies]\ngdl-message = {{ git = {url:?} }}\n"
        ),
    )
    .expect("manifest");
    std::fs::write(
        app.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Words;\n",
    )
    .expect("source");
    std::fs::write(
        app.join("src/main.rs"),
        "use verify_workspace_input as _;\n\
         fn main() { print!(\"{}\", gdl_message::Catalogue::linked().to_toml()); }\n",
    )
    .expect("source");
    let control = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO"),
            "run",
            "--quiet",
            "--manifest-path",
        ])
        .arg(app.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(target())
        .env("CARGO_HTTP_PROXY", "127.0.0.1:9")
        .env("CARGO_NET_RETRY", "0")
        .output()
        .expect("the member's own build");
    assert!(
        control.status.success(),
        "{}",
        String::from_utf8_lossy(&control.stderr)
    );
    let linked = gdl_message::Catalogue::from_toml(&String::from_utf8_lossy(&control.stdout))
        .expect("a catalogue");
    assert_eq!(linked.messages.len(), 1, "{linked:?}");
    assert_eq!(linked.messages[0].name, "Words");
    let output = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO_BIN_EXE_cargo-gdl"),
            "gdl",
            "extract",
        ])
        .arg(&app)
        .arg("--verify")
        .arg("--target-dir")
        .arg(target())
        .env("CARGO_HTTP_PROXY", "127.0.0.1:9")
        .env("CARGO_NET_RETRY", "0")
        .output()
        .expect("cargo gdl");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A dependency for another target does not add a linker section to the host program.
/// The control compiles the copied workspace, so its first run takes several seconds.
#[test]
#[cfg(unix)]
fn verify_ignores_gdl_message_for_another_target() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-target-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workspace = dir.join("workspace");
    let members = ["gdl-message", "gdl-model", "gdl-macros", "gdl-syntax"];
    for member in members {
        copy_dir(
            &root.join("crates").join(member),
            &workspace.join("crates").join(member),
        );
    }
    let mut manifest: toml::Table = std::fs::read_to_string(root.join("Cargo.toml"))
        .expect("manifest")
        .parse()
        .expect("TOML");
    let table = manifest["workspace"].as_table_mut().expect("workspace");
    table.remove("exclude");
    let mut paths = members.map(|m| format!("crates/{m}")).to_vec();
    paths.push("app".into());
    table.insert("members".into(), paths.into());
    std::fs::write(workspace.join("Cargo.toml"), manifest.to_string()).expect("manifest");
    let write = |path: &str, text: &str| {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
        std::fs::write(path, text).expect("file");
    };
    write(
        "message/Cargo.toml",
        "[package]\nname = \"gdl-message\"\nversion = \"0.2.0\"\nedition = \"2024\"\n",
    );
    write("message/src/lib.rs", "");
    write(
        "windows/Cargo.toml",
        "[package]\nname = \"windows-protocols\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
         [dependencies]\ngdl-message = { path = \"../message\" }\n",
    );
    write(
        "windows/src/lib.rs",
        "compile_error!(\"the host must not build the Windows dependency\");\n",
    );
    write(
        "workspace/app/Cargo.toml",
        "[package]\nname = \"verify-target-input\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
         [dependencies]\ngdl-message = { path = \"../crates/gdl-message\" }\n\
         [target.'cfg(windows)'.dependencies]\nwindows-protocols = { path = \"../../windows\" }\n",
    );
    write(
        "workspace/app/src/lib.rs",
        "#[derive(gdl_message::Message)]\npub struct Words;\n",
    );
    write(
        "workspace/app/src/main.rs",
        "use verify_target_input as _;\n\
         fn main() { print!(\"{}\", gdl_message::Catalogue::linked().to_toml()); }\n",
    );
    std::fs::copy(root.join("Cargo.lock"), workspace.join("Cargo.lock")).expect("lock");
    let control = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO"),
            "run",
            "--offline",
            "--quiet",
            "--manifest-path",
        ])
        .arg(workspace.join("app/Cargo.toml"))
        .arg("--target-dir")
        .arg(target())
        .output()
        .expect("the crate's own build");
    assert!(
        control.status.success(),
        "{}",
        String::from_utf8_lossy(&control.stderr)
    );
    let linked = gdl_message::Catalogue::from_toml(&String::from_utf8_lossy(&control.stdout))
        .expect("one linked catalogue");
    assert_eq!(linked.messages.len(), 1, "{linked:?}");
    assert_eq!(linked.messages[0].name, "Words");
    let output = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO_BIN_EXE_cargo-gdl"),
            "gdl",
            "extract",
        ])
        .arg(workspace.join("app"))
        .arg("--verify")
        .arg("--target-dir")
        .arg(target())
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo gdl");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `--verify` compares only the verified crate's registrations, though the
/// program it builds links its dependencies' too. The build tells them
/// apart by the directory each was built from (`CARGO_MANIFEST_DIR`), not
/// by a prediction from `cargo metadata`: here a dependency of another name
/// and another version of the crate itself both register messages, and
/// neither is a difference.
#[test]
fn verify_leaves_out_what_the_crates_dependencies_register() {
    verify_dependencies("0.1.0", "0.2.0");
}

/// [`verify_leaves_out_what_the_crates_dependencies_register`], with the
/// other version of the crate first in `cargo metadata`'s order: taking
/// the first package of the crate's name would take the dependency's
/// registrations for the crate's own.
#[test]
fn verify_keeps_its_directory_when_a_dependency_sorts_first() {
    verify_dependencies("0.2.0", "0.1.0");
}

/// An alias the reader leaves out is matched by its path, not its name:
/// `a::Req` is read and agrees with the build, and `b::Req`, of the same
/// name, is left out with its reason. Neither is a difference.
#[test]
fn verify_matches_a_left_out_alias_by_its_path() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-aliases-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let message = Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-message");
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        format!(
            "[package]\nname = \"verify-aliases\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [workspace]\n[dependencies]\ngdl-message = {{ path = {} }}\n",
            toml_string(&message)
        ),
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Token;\n\
         #[derive(gdl_message::Message)]\npub struct Carried<P>(pub P);\n\
         pub mod a {\n\
             #[gdl_message::export]\n\
             pub type Req = super::Carried<super::Token>;\n\
         }\n\
         pub mod b {\n\
             #[gdl_message::export]\n\
             pub type Req = super::Carried<std::sync::Arc<super::Token>>;\n\
         }\n",
    )
    .expect("source");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
        dir.join("Cargo.lock"),
    )
    .expect("the workspace lock");

    let output = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO_BIN_EXE_cargo-gdl"),
            "gdl",
            "extract",
        ])
        .arg(dir)
        .arg("--verify")
        .arg("--target-dir")
        .arg(target())
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo gdl");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    let offer =
        gdl_gen::Offer::from_toml(&String::from_utf8_lossy(&output.stdout)).expect("an offer");
    let exports: Vec<&str> = offer.exports.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(exports, ["verify_aliases::a::Req"]);
    let skipped: Vec<&str> = offer.skipped.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(skipped, ["Req"]);
    assert_eq!(
        stderr
            .matches("left out by the reader on purpose, registered by the build: `Req`")
            .count(),
        1,
        "{stderr}"
    );
}

/// A node alias re-exported at the root registers its root path, even
/// when the reader leaves out its argument in a private module.
/// Building and linking the fixture can take several seconds.
#[test]
fn verify_matches_a_left_out_node_alias_at_its_registered_path() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-node-alias-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let gdl = Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl");
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        format!(
            "[package]\nname = \"verify-nodes\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [workspace]\n[dependencies]\ngdl = {{ path = {} }}\n",
            toml_string(&gdl)
        ),
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        r#"#[derive(gdl::Message)]
pub struct Token;

#[derive(gdl::Node)]
pub struct Relay<B>(std::marker::PhantomData<B>);

impl<B: Send + Sync + 'static> gdl::Build for Relay<B> {
    type Config = ();
    type Metrics = ();
    async fn build(_: (), _: ()) -> gdl::Result<Self> {
        Ok(Self(std::marker::PhantomData))
    }
}

impl<B: Send + Sync + 'static> gdl::Forward for Relay<B> {
    type In = Token;
    type Out = Token;
    async fn forward(&self, _: &gdl::Cx, req: Token) -> gdl::Result<Token> {
        Ok(req)
    }
}

mod nodes {
    #[gdl::export]
    pub type Read = super::Relay<super::Token>;
    #[gdl::export]
    pub type Hidden = super::Relay<std::sync::Arc<super::Token>>;
}
pub use nodes::{Hidden, Read};

#[gdl::export]
pub type Visible = Relay<std::sync::Arc<Token>>;
"#,
    )
    .expect("source");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
        dir.join("Cargo.lock"),
    )
    .expect("the workspace lock");

    let extract = |verify: bool| {
        let mut command = Command::new("timeout");
        command
            .args([
                CARGO_LIMIT,
                env!("CARGO_BIN_EXE_cargo-gdl"),
                "gdl",
                "extract",
            ])
            .arg(dir)
            .arg("--target-dir")
            .arg(target())
            .env("CARGO_NET_OFFLINE", "true");
        if verify {
            command.arg("--verify");
        }
        command.output().expect("cargo gdl")
    };
    let read = extract(false);
    assert!(
        read.status.success(),
        "{}",
        String::from_utf8_lossy(&read.stderr)
    );
    let offer =
        gdl_gen::Offer::from_toml(&String::from_utf8_lossy(&read.stdout)).expect("an offer");
    assert_eq!(offer.nodes.len(), 1);
    assert_eq!(offer.nodes[0].ty, "verify_nodes::Read");
    assert_eq!(offer.skipped.len(), 2);

    let verified = extract(true);
    let stderr = String::from_utf8_lossy(&verified.stderr);
    assert!(verified.status.success(), "{stderr}");
    for name in ["Hidden", "Visible"] {
        assert_eq!(
            stderr
                .matches(&format!(
                    "left out by the reader on purpose, registered by the build: `{name}`"
                ))
                .count(),
            1,
            "{stderr}"
        );
    }
}

/// A skipped message alias must not hide a node of the same name.
/// Building and linking the fixture can take several seconds.
#[test]
fn verify_does_not_hide_a_node_behind_a_skipped_message_alias() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-alias-collision-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let gdl = Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl");
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        format!(
            "[package]\nname = \"verify-alias-collision\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [workspace]\n[dependencies]\ngdl = {{ path = {} }}\n",
            toml_string(&gdl)
        ),
    )
    .expect("manifest");
    let source = r#"#[derive(gdl::Message)]
pub struct Token;

#[derive(gdl::Message)]
pub struct Carried<P>(pub P);

macro_rules! handler {
    () => {
        #[derive(gdl::Node)]
        pub struct Handler;

        impl gdl::Build for Handler {
            type Config = ();
            type Metrics = ();
            async fn build(_: (), _: ()) -> gdl::Result<Self> {
                Ok(Self)
            }
        }

        impl gdl::Forward for Handler {
            type In = Token;
            type Out = Token;
            async fn forward(&self, _: &gdl::Cx, req: Token) -> gdl::Result<Token> {
                Ok(req)
            }
        }
    };
}
handler!();
"#;
    std::fs::write(dir.join("src/lib.rs"), source).expect("source");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
        dir.join("Cargo.lock"),
    )
    .expect("the workspace lock");

    let extract = |verify: bool| {
        let mut command = Command::new("timeout");
        command
            .args([
                CARGO_LIMIT,
                env!("CARGO_BIN_EXE_cargo-gdl"),
                "gdl",
                "extract",
            ])
            .arg(dir)
            .arg("--target-dir")
            .arg(target())
            .env("CARGO_NET_OFFLINE", "true");
        if verify {
            command.arg("--verify");
        }
        command.output().expect("cargo gdl")
    };
    let difference = "registered by the build, not read from source: node `Handler` at `verify_alias_collision::Handler`";
    let control = extract(true);
    let stderr = String::from_utf8_lossy(&control.stderr);
    assert_eq!(control.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains(difference), "{stderr}");

    std::fs::write(
        dir.join("src/lib.rs"),
        format!(
            "{source}\nmod messages {{\n\
             #[gdl::export]\n\
             pub type Handler = super::Carried<std::sync::Arc<super::Token>>;\n\
             }}\n"
        ),
    )
    .expect("source");
    let read = extract(false);
    assert!(
        read.status.success(),
        "{}",
        String::from_utf8_lossy(&read.stderr)
    );
    let offer =
        gdl_gen::Offer::from_toml(&String::from_utf8_lossy(&read.stdout)).expect("an offer");
    assert!(offer.nodes.is_empty());
    assert_eq!(offer.skipped.len(), 1);
    assert_eq!(
        offer.skipped[0].path.as_deref(),
        Some("verify_alias_collision::messages::Handler")
    );

    let verified = extract(true);
    let stderr = String::from_utf8_lossy(&verified.stderr);
    assert_eq!(
        verified.status.code(),
        Some(1),
        "a skipped message alias must not hide the build-only node: {stderr}"
    );
    assert!(stderr.contains(difference), "{stderr}");
}

/// A local type alias of a generic node still registers each exported
/// instantiation at the crate root when the reader leaves out an argument.
/// Building and linking the fixture can take several seconds.
#[test]
fn verify_matches_a_left_out_node_through_a_type_alias() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-node-chain-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let gdl = Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl");
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        format!(
            "[package]\nname = \"verify-node-chain\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
             [workspace]\n[dependencies]\ngdl = {{ path = {} }}\n",
            toml_string(&gdl)
        ),
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        r#"#[derive(gdl::Message)]
pub struct Token;

#[derive(gdl::Node)]
pub struct Relay<B>(std::marker::PhantomData<B>);

impl<B: Send + Sync + 'static> gdl::Build for Relay<B> {
    type Config = ();
    type Metrics = ();
    async fn build(_: (), _: ()) -> gdl::Result<Self> {
        Ok(Self(std::marker::PhantomData))
    }
}

impl<B: Send + Sync + 'static> gdl::Forward for Relay<B> {
    type In = Token;
    type Out = Token;
    async fn forward(&self, _: &gdl::Cx, req: Token) -> gdl::Result<Token> {
        Ok(req)
    }
}

pub type NamedRelay<B> = Relay<B>;

mod nodes {
    #[gdl::export]
    pub type Read = super::NamedRelay<super::Token>;
    #[gdl::export]
    pub type Hidden = super::NamedRelay<std::sync::Arc<super::Token>>;
}
pub use nodes::{Hidden, Read};
"#,
    )
    .expect("source");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
        dir.join("Cargo.lock"),
    )
    .expect("the workspace lock");

    let extract = |verify: bool| {
        let mut command = Command::new("timeout");
        command
            .args([
                CARGO_LIMIT,
                env!("CARGO_BIN_EXE_cargo-gdl"),
                "gdl",
                "extract",
            ])
            .arg(dir)
            .arg("--target-dir")
            .arg(target())
            .env("CARGO_NET_OFFLINE", "true");
        if verify {
            command.arg("--verify");
        }
        command.output().expect("cargo gdl")
    };
    let read = extract(false);
    assert!(
        read.status.success(),
        "{}",
        String::from_utf8_lossy(&read.stderr)
    );
    let offer =
        gdl_gen::Offer::from_toml(&String::from_utf8_lossy(&read.stdout)).expect("an offer");
    assert_eq!(offer.nodes.len(), 1);
    assert_eq!(offer.nodes[0].ty, "verify_node_chain::Read");
    assert_eq!(offer.skipped.len(), 1);
    assert_eq!(offer.skipped[0].name, "Hidden");
    assert!(offer.skipped[0].problem.contains("Arc"));

    let verified = extract(true);
    let stderr = String::from_utf8_lossy(&verified.stderr);
    assert!(verified.status.success(), "{stderr}");
    assert_eq!(
        stderr
            .matches("left out by the reader on purpose, registered by the build: `Hidden`")
            .count(),
        1,
        "{stderr}"
    );
}

/// Verify the crate `verify-app` at `version`, which depends on
/// `verify-shared` and on `verify-app` at `dependency_version`, each of
/// which registers messages.
///
/// Every cargo here is offline, with the workspace's lock, and shares the
/// other verify tests' target directory, where gdl-message is built.
fn verify_dependencies(version: &str, dependency_version: &str) {
    let temp = tempfile::Builder::new()
        .prefix("gdl-verify-dependencies-")
        .tempdir()
        .expect("dir");
    let dir = temp.path();
    let message = Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-message");
    let write = |path: &str, text: &str| {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("dir");
        std::fs::write(path, text).expect("file");
    };
    let manifest = |name: &str, version: &str, dependencies: &str| {
        format!(
            "[package]\nname = \"{name}\"\nversion = \"{version}\"\nedition = \"2024\"\n\
             [workspace]\n[dependencies]\ngdl-message = {{ path = {} }}\n{dependencies}",
            toml_string(&message)
        )
    };
    write("shared/Cargo.toml", &manifest("verify-shared", "0.1.0", ""));
    write(
        "shared/src/lib.rs",
        "#[derive(gdl_message::Message)]\npub struct Token;\n\
         #[derive(gdl_message::Message)]\npub struct Carried<P>(pub P);\n\
         #[gdl_message::export]\npub type CarriedToken = Carried<Token>;\n",
    );
    write(
        "two/Cargo.toml",
        &manifest("verify-app", dependency_version, ""),
    );
    write(
        "two/src/lib.rs",
        "#[derive(gdl_message::Message)]\npub struct Extra;\n",
    );
    write(
        "app/Cargo.toml",
        &manifest(
            "verify-app",
            version,
            "verify-shared = { path = \"../shared\" }\n\
             app_two = { package = \"verify-app\", path = \"../two\" }\n",
        ),
    );
    // Each dependency is named, so that the program links it.
    write(
        "app/src/lib.rs",
        "#[derive(gdl_message::Message)]\npub struct Words;\n\
         #[gdl_message::export]\npub use verify_shared::Token;\n\
         #[gdl_message::export]\npub use app_two::Extra;\n",
    );
    // A program that links the crate as `--verify`'s does, and prints what
    // it registers. Each case names its own: the cases share a target
    // directory, and `cargo run` runs the program after it lets go of the
    // build lock, so a program of one name could be the other case's.
    write(
        "probe/Cargo.toml",
        &manifest(
            &format!("verify-probe-{}", version.replace('.', "-")),
            "0.0.0",
            "verify-app = { path = \"../app\" }\n",
        ),
    );
    write(
        "probe/src/main.rs",
        "use verify_app as _;\n\n\
         fn main() {\n    print!(\"{}\", gdl_message::Catalogue::linked().to_toml());\n}\n",
    );
    for at in ["app", "probe"] {
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
            dir.join(at).join("Cargo.lock"),
        )
        .expect("the workspace lock");
    }

    // The control: the program links what the dependencies register, so
    // only the filter keeps it out of the comparison.
    let probe = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO"),
            "run",
            "--quiet",
            "--manifest-path",
        ])
        .arg(dir.join("probe/Cargo.toml"))
        .arg("--target-dir")
        .arg(target())
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo run");
    assert!(
        probe.status.success(),
        "{}",
        String::from_utf8_lossy(&probe.stderr)
    );
    let linked = gdl_message::Catalogue::from_toml(&String::from_utf8_lossy(&probe.stdout))
        .expect("a catalogue");
    let built_in = |name: &str| {
        let message = linked
            .messages
            .iter()
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("`{name}` is linked: {linked:?}"));
        std::fs::canonicalize(&message.dir).expect("a directory")
    };
    let canonical = |at: &str| std::fs::canonicalize(dir.join(at)).expect("a directory");
    assert_eq!(built_in("Token"), canonical("shared"));
    assert_eq!(built_in("Extra"), canonical("two"));
    assert!(
        linked.exports.iter().any(|e| e.name == "CarriedToken"),
        "{linked:?}"
    );

    let output = Command::new("timeout")
        .args([
            CARGO_LIMIT,
            env!("CARGO_BIN_EXE_cargo-gdl"),
            "gdl",
            "extract",
        ])
        .arg(dir.join("app"))
        .arg("--verify")
        .arg("--target-dir")
        .arg(target())
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo gdl");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    let offer =
        gdl_gen::Offer::from_toml(&String::from_utf8_lossy(&output.stdout)).expect("an offer");
    let messages: Vec<&str> = offer.messages.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(messages, ["Words"]);
    let exports: Vec<&str> = offer.exports.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(exports, ["Extra", "Token"]);
}

/// The `Texts` export of the entry of `package` at `version` in `catalogue`.
fn texts(catalogue: &Path, package: &str, version: &str) -> gdl_extract::ExportEntry {
    let file = catalogue.join(package).join(format!("{version}.toml"));
    let entry = gdl_gen::Entry::from_toml(&std::fs::read_to_string(&file).expect("an entry"))
        .expect("an entry");
    entry
        .offer
        .exports
        .into_iter()
        .find(|e| e.name == "Texts")
        .expect("`Texts` is exported")
}

/// `cargo gdl extract <crate> --catalog <dir> [--probe]`, offline, with
/// `args` after it.
fn add(
    krate: &str,
    catalogue: &Path,
    probe: bool,
    args: &[&std::ffi::OsStr],
) -> std::process::Output {
    let mut command = Command::new("timeout");
    command
        .args([
            CARGO_LIMIT,
            env!("CARGO_BIN_EXE_cargo-gdl"),
            "gdl",
            "extract",
            krate,
            "--catalog",
        ])
        .arg(catalogue)
        .args(args)
        .env("CARGO_HTTP_PROXY", "127.0.0.1:9")
        .env("CARGO_NET_RETRY", "0");
    if probe {
        command.arg("--probe");
    }
    command.output().expect("cargo gdl")
}

/// `--probe` builds the crate's registrations, as `--verify` does, and
/// records on each instantiation the wire capability the build reports,
/// which takes precedence over the reader's. The fixture's serde impls
/// bound their parameter on `Clone`, which the reader does not judge: it
/// reads `Wrap<String>` as local, and the build as wire. A crate from a
/// directory is probed again at each add, so an edit shows; an add without
/// `--probe` drops the data, which may describe code that is gone.
///
/// One cold build of the fixture's dependencies, in the target directory
/// the other `--verify` tests share; each probe after it builds only the
/// fixture.
#[test]
fn probe_records_the_builds_wire_capability() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-probe-")
        .tempdir()
        .expect("dir");
    let krate = temp.path().join("probed");
    copy_dir(&fixtures().join("probed-protocols"), &krate);
    // The copy takes gdl-message from where the fixture does.
    let manifest = krate.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)
        .expect("manifest")
        .replace(
            "\"../../../../../gdl-message\"",
            &toml_string(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-message")),
        );
    std::fs::write(&manifest, text).expect("manifest");
    let catalogue = temp.path().join("catalogue");
    let target = target();
    let at = krate.to_str().expect("UTF-8");
    let add = |probe: bool| {
        let output = add(
            at,
            &catalogue,
            probe,
            &["--target-dir".as_ref(), target.as_os_str()],
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        texts(&catalogue, "probed-protocols", "0.0.0")
    };

    let read = add(false);
    assert_eq!((read.wire, read.probed), (false, None));
    let probed = add(true);
    assert_eq!((probed.wire, probed.probed), (false, Some(true)));

    // `String` is not `Copy`: the build now reads `Wrap<String>` as local.
    let lib = krate.join("src/lib.rs");
    let source = std::fs::read_to_string(&lib).expect("source");
    std::fs::write(&lib, source.replace("T: Clone +", "T: Copy +")).expect("source");
    let edited = add(true);
    assert_eq!(edited.probed, Some(false));

    let dropped = add(false);
    assert_eq!(dropped.probed, None);
}

/// A published version is probed once and kept, since it never changes.
/// The fixture registry's crate does not depend on gdl-message, so a
/// probe of it fails: the first `--probe` tries one, and once the
/// catalogue holds the build's reading, a `--probe` builds nothing and
/// keeps it.
#[test]
fn a_published_version_is_probed_once() {
    let temp = offline_registry("probe");
    let dir = temp.path();
    let catalogue = dir.join("catalogue");
    let target = |name: &str| dir.join(name).into_os_string();
    let first = target("first");
    let output = add(
        "regprobe@0.1.0",
        &catalogue,
        true,
        &["--target-dir".as_ref(), &first],
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "a probe was tried");
    assert!(
        stderr
            .contains("regprobe does not depend on gdl-message, so it registers nothing to probe"),
        "{stderr}"
    );
    assert!(!catalogue.exists(), "a failed probe adds nothing");

    // The catalogue takes the build's reading, as a probe that worked
    // would have added it.
    let output = add(
        "regprobe@0.1.0",
        &catalogue,
        false,
        &["--target-dir".as_ref(), &first],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(texts(&catalogue, "regprobe", "0.1.0").probed, None);
    let mut catalog = gdl_gen::Catalog::load(&catalogue).expect("catalogue");
    let mut entry = catalog.entries().next().expect("an entry").clone();
    for export in &mut entry.offer.exports {
        export.probed = Some(true);
    }
    catalog.add(entry).expect("probed");
    assert_eq!(texts(&catalogue, "regprobe", "0.1.0").probed, Some(true));

    let kept = target("kept");
    let output = add(
        "regprobe@0.1.0",
        &catalogue,
        true,
        &["--target-dir".as_ref(), &kept],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(texts(&catalogue, "regprobe", "0.1.0").probed, Some(true));
    let built: Vec<_> = std::fs::read_dir(Path::new(&kept).join("gdl-gen"))
        .expect("the fetch's scratch")
        .map(|e| e.expect("an entry").file_name())
        .filter(|name| name.to_string_lossy().starts_with("gdl-verify-"))
        .collect();
    assert_eq!(built, Vec::<std::ffi::OsString>::new(), "nothing is built");
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
/// crossing report that generation writes. A crate the catalogue lacks is
/// named.
#[test]
fn a_check_agrees_with_generation() {
    let chat = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/chat");
    let spec = gdl_gen::Spec::load(&chat.join("gdl.toml")).expect("spec");
    let read = gdl_gen::check(&spec, &target(), gdl_gen::Offers::Read).expect("read");
    let out = tempfile::Builder::new()
        .prefix("gdl-check-agrees-")
        .tempdir()
        .expect("scratch");
    let files = gdl_gen::render(&spec, out.path(), &target()).expect("render");
    let committed = &files[Path::new("crossings.txt")];
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

/// The archive `export` writes for a two-segment graph whose crates come
/// from the fixture registry and gdl from git, by path in the archive.
fn fetchable_archive() -> std::collections::BTreeMap<String, String> {
    use std::io::Read as _;
    let temp = offline_registry("images");
    let dir = temp.path();
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
    files
}

/// An archive whose crates all come from a registry or git builds images
/// too: a `Dockerfile` per segment, from the archive alone, and a script
/// that builds each. An archive naming a directory has neither (`export.rs`
/// lists the chat example's entries exactly).
#[test]
fn an_archive_of_fetchable_crates_builds_images() {
    let files = fetchable_archive();
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
            .contains("gdl-runtime = { git = \"https://example.com/gdl\", rev = \"abc123\" }")
    );
}

/// An exported image builds in `rust:<rust-version>`, the oldest Rust that
/// gdl builds with, not the toolchain `rust-toolchain.toml` pins. A compose
/// bundle's images build in the pinned toolchain instead
/// (`deployment.rs`, `the_default_images_are_distroless_and_the_pinned_toolchain`).
#[test]
fn an_exported_image_builds_with_gdls_rust_version() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workspace: toml::Table =
        toml::from_str(&std::fs::read_to_string(root.join("Cargo.toml")).expect("the workspace"))
            .expect("the workspace");
    let version = workspace["workspace"]["package"]["rust-version"]
        .as_str()
        .expect("a rust-version");
    let files = fetchable_archive();
    for segment in ["front", "engine"] {
        let dockerfile = &files[&format!("imaged/{segment}/Dockerfile")];
        assert!(
            dockerfile.contains(&format!("ARG RUST_VERSION={version}\n")),
            "{segment}: {dockerfile}"
        );
    }
}

/// A source without a frontend takes its requests on stdin, which
/// `docker compose run -T` gives it: its service keeps stdin open, and the
/// other segments start without it.
#[test]
fn compose_serves_a_source_without_a_frontend_on_stdin() {
    let temp = offline_registry("compose-no-frontend");
    let dir = temp.path();
    let spec = front_and_engine(dir);
    let files = gdl_gen::compose_files(
        &spec,
        None,
        &dir.join("out"),
        &dir.join("target"),
        gdl_gen::Sources::Fetched,
    )
    .expect("a deployment whose source reads stdin");
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("compose.yaml");
    assert_eq!(
        compose["services"]["front"]["stdin_open"], true,
        "{compose}"
    );
    assert!(compose["services"]["engine"].get("stdin_open").is_none());
    assert!(
        files[Path::new("engine/config.toml")].contains("[min_replicas]\nfront = 0\n"),
        "{}",
        files[Path::new("engine/config.toml")]
    );
}

/// A spec in `dir`, which [`offline_registry`] made, of a source `front`
/// without a frontend and a sink `engine`, both from the registry.
fn front_and_engine(dir: &Path) -> gdl_gen::Spec {
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
    gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("spec")
}

/// Bundle directories, under a fresh directory, and whether Docker Compose
/// loads a project from each. Compose turns its working directory into a
/// `file://` URL, and the parse fails on a `%` that two hex digits do not
/// follow, anywhere in the path. A `%` with two hex digits, in either case,
/// loads.
const PERCENT: &[(&str, bool)] = &[
    ("p%x", false),
    ("p%", false),
    ("p%4", false),
    ("a%41b%", false),
    ("up%x/bundle", false),
    ("p%41", true),
    ("p%25", true),
    ("u%e9", true),
];

/// `compose` refuses to write a bundle where Docker Compose cannot load
/// it, and writes nothing there. Each directory is a fresh one, as
/// `compose` needs.
#[test]
fn compose_refuses_a_directory_compose_cannot_load() {
    let temp = offline_registry("compose-percent");
    let spec = front_and_engine(temp.path());
    let holder = tempfile::Builder::new()
        .prefix("gdl-compose-percent-")
        .tempdir()
        .expect("dir");
    for (name, loads) in PERCENT {
        let out = holder.path().join(name);
        let result = gdl_gen::compose(
            &spec,
            None,
            &out,
            &temp.path().join("target"),
            gdl_gen::Sources::Fetched,
        );
        if *loads {
            result.unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(out.join("compose.yaml").is_file(), "{name}");
        } else {
            let error = result.expect_err(name).to_string();
            assert!(
                error.contains("holds a `%` that two hex digits do not follow"),
                "{name}: {error}"
            );
            assert!(
                error.contains(&format!("{:?}", out.as_os_str())),
                "{name}: {error}"
            );
            assert!(!out.exists(), "{name}");
        }
    }
}

/// Compose reads its working directory as the shell gives it, through any
/// symlink, so a bundle is refused when either the path as given or the
/// path the file system resolves holds such a `%`.
#[cfg(unix)]
#[test]
fn compose_refuses_a_percent_on_either_side_of_a_symlink() {
    let temp = offline_registry("compose-percent-link");
    let spec = front_and_engine(temp.path());
    let holder = tempfile::Builder::new()
        .prefix("gdl-compose-percent-link-")
        .tempdir()
        .expect("dir");
    let plain = holder.path().join("plain");
    let percent = holder.path().join("p%x");
    std::fs::create_dir(&plain).expect("dir");
    std::fs::create_dir(&percent).expect("dir");
    std::os::unix::fs::symlink(&plain, holder.path().join("to%x")).expect("link");
    std::os::unix::fs::symlink(&percent, holder.path().join("to")).expect("link");
    for link in ["to%x", "to"] {
        let out = holder.path().join(link).join("bundle");
        let error = gdl_gen::compose(
            &spec,
            None,
            &out,
            &temp.path().join("target"),
            gdl_gen::Sources::Fetched,
        )
        .expect_err(link)
        .to_string();
        assert!(
            error.contains("holds a `%` that two hex digits do not follow"),
            "{link}: {error}"
        );
        assert!(!out.exists(), "{link}");
    }
}

/// A missing component followed by `..` can hide an existing symlink
/// from a single resolution pass.
#[cfg(unix)]
#[test]
fn compose_refuses_a_percent_symlink_after_a_cancelled_component() {
    let temp = offline_registry("compose-percent-cancelled");
    let spec = front_and_engine(temp.path());
    let holder = tempfile::Builder::new()
        .prefix("gdl-compose-percent-cancelled-")
        .tempdir()
        .expect("dir");
    for (name, link, accepted) in [("plain", "safe", true), ("p%x", "to", false)] {
        let destination = holder.path().join(name);
        std::fs::create_dir(&destination).expect("dir");
        std::os::unix::fs::symlink(&destination, holder.path().join(link)).expect("link");
        let out = holder
            .path()
            .join("missing")
            .join("..")
            .join(link)
            .join("bundle");
        let result = gdl_gen::compose(
            &spec,
            None,
            &out,
            &temp.path().join("target"),
            gdl_gen::Sources::Fetched,
        );
        if accepted {
            result.expect("a plain destination is accepted");
            assert!(destination.join("bundle/compose.yaml").is_file());
        } else {
            assert!(
                result.is_err(),
                "compose accepted {out:?}; compose.yaml written under p%x={}",
                destination.join("bundle/compose.yaml").is_file()
            );
            assert!(
                result
                    .expect_err("a percent-bearing destination is refused")
                    .to_string()
                    .contains("holds a `%` that two hex digits do not follow")
            );
            assert!(!destination.join("bundle").exists());
        }
    }
}

/// A shell keeps the path it took through a symlink in `PWD`, and Compose
/// reads `PWD`, so a relative output is checked from there too.
#[cfg(unix)]
#[test]
fn compose_refuses_a_relative_output_under_a_percent_pwd() {
    let temp = offline_registry("compose-percent-pwd");
    front_and_engine(temp.path());
    let holder = tempfile::Builder::new()
        .prefix("gdl-compose-percent-pwd-")
        .tempdir()
        .expect("dir");
    let plain = holder.path().join("plain");
    let link = holder.path().join("work%x");
    std::fs::create_dir(&plain).expect("dir");
    std::os::unix::fs::symlink(&plain, &link).expect("link");
    for (cwd, name, accepted) in [(&plain, "control", true), (&link, "bundle", false)] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .args(["gdl", "compose", "--spec"])
            .arg(temp.path().join("gdl.toml"))
            .arg("--target-dir")
            .arg(temp.path().join("target"))
            .args(["--out", name])
            .current_dir(cwd)
            .env("PWD", cwd)
            .output()
            .expect("cargo gdl compose");
        let out = cwd.join(name);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.success(),
            accepted,
            "PWD={}: --out {name}; compose.yaml written={}; stderr={stderr}",
            cwd.display(),
            out.join("compose.yaml").is_file()
        );
        if accepted {
            assert!(out.join("compose.yaml").is_file());
        } else {
            assert!(
                stderr.contains("holds a `%` that two hex digits do not follow"),
                "{stderr}"
            );
            assert!(!out.exists());
        }
    }
}

/// [`PERCENT`] agrees with Docker Compose: `docker compose config` loads a
/// project from each directory that `compose` writes, and fails in each
/// that it refuses. When a Compose release loads them all, the limit can
/// be lifted (`docs/src/operations/limitations.md`).
#[test]
#[ignore = "needs docker; run with --ignored"]
fn the_percent_rule_matches_docker_compose() {
    let temp = offline_registry("compose-percent-docker");
    let spec = front_and_engine(temp.path());
    let holder = tempfile::Builder::new()
        .prefix("gdl-compose-percent-docker-")
        .tempdir()
        .expect("dir");
    for (name, loads) in PERCENT {
        let out = holder.path().join(name);
        if *loads {
            gdl_gen::compose(
                &spec,
                None,
                &out,
                &temp.path().join("target"),
                gdl_gen::Sources::Fetched,
            )
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        } else {
            std::fs::create_dir_all(&out).expect("dir");
            std::fs::write(
                out.join("compose.yaml"),
                "{\"services\":{\"a\":{\"image\":\"scratch\"}}}\n",
            )
            .expect("compose.yaml");
        }
        let result = std::process::Command::new("timeout")
            .args(["30", "docker", "compose", "config", "--quiet"])
            .current_dir(&out)
            // Compose prefers `PWD` to the directory the process runs in.
            .env("PWD", &out)
            .output()
            .expect("docker compose");
        assert_eq!(
            result.status.success(),
            *loads,
            "{name}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
}

/// Generation, a check, and an export each write the `[gdl]` path into
/// manifests that name `gdl-runtime`, so each refuses a path to the
/// definitions crate. Resolving and drawing write nothing, and read the
/// path as text.
#[test]
fn every_step_that_writes_a_manifest_refuses_a_gdl_path_to_the_definitions() {
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/http");
    let text = std::fs::read_to_string(example.join("gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(example.join("http.gdl")).expect("graph");
    let text = text.replace("../../crates/gdl-runtime", "../../crates/gdl");
    let spec = gdl_gen::Spec::from_text(&text, &graph, &example).expect("loads as text");
    let target = target();
    let refused = |result: Result<(), gdl_gen::Error>, step: &str| {
        let error = result.expect_err(step).to_string();
        assert!(error.contains("must name `gdl-runtime`"), "{step}: {error}");
    };
    refused(
        gdl_gen::render(&spec, &target.join("out"), &target).map(drop),
        "render",
    );
    refused(
        gdl_gen::check(&spec, &target, gdl_gen::Offers::Read).map(drop),
        "check",
    );
    refused(
        gdl_gen::export(&spec, &target, gdl_gen::Offers::Read).map(drop),
        "export",
    );
    gdl_gen::resolve(&spec, &target).expect("resolve");
    gdl_gen::diagram(&spec, &target, gdl_gen::Format::Dot).expect("diagram");
}

/// A Git repository in `dir`, committed: a pool policy crate `regpolicy`
/// whose `ByWords` reads `regproto::Words`, beside a stand-in `gdl` that
/// declares the trait, so that it resolves from the offline registry. The
/// repository's `file://` URL and its commit.
fn policy_repository(dir: &Path) -> (String, String) {
    let repo = dir.join("policy-repo");
    let write = |path: &str, text: &str| {
        let path = repo.join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("dir");
        std::fs::write(path, text).expect("fixture");
    };
    write(
        "Cargo.toml",
        "[workspace]\nmembers = [\"gdl\", \"regpolicy\"]\nresolver = \"3\"\n",
    );
    write(
        "gdl/Cargo.toml",
        "[package]\nname = \"gdl\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    );
    write(
        "gdl/src/lib.rs",
        "pub trait PoolPolicy {\n    type Head;\n}\n",
    );
    write(
        "regpolicy/Cargo.toml",
        "[package]\nname = \"regpolicy\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[dependencies]\ngdl = { path = \"../gdl\" }\nregproto = \"0.1.0\"\n",
    );
    write(
        "regpolicy/src/lib.rs",
        "pub struct ByWords;\n\nimpl gdl::PoolPolicy for ByWords {\n    type Head = regproto::Words;\n}\n",
    );
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .current_dir(&repo)
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .output()
            .expect("git");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("text")
            .trim()
            .to_string()
    };
    git(&["init", "--quiet"]);
    git(&["add", "."]);
    git(&["commit", "--quiet", "-m", "fixture"]);
    let rev = git(&["rev-parse", "HEAD"]);
    let url = format!("file://{}", repo.canonicalize().expect("repo").display());
    (url, rev)
}

/// A spec whose pool policy crate is a Git package is fetched, so Compose
/// builds it without `--local`: only the chooser depends on the package,
/// at its commit, and registers the policy.
#[test]
fn compose_fetches_a_pool_policy_crate_from_git() {
    let temp = offline_registry("policy-git");
    let dir = temp.path();
    // Offline, cargo checks out no Git repository, not even a `file://`
    // one; the registry stays replaced, so nothing reaches the network.
    let config = dir.join(".cargo/config.toml");
    let text = std::fs::read_to_string(&config).expect("config");
    std::fs::write(&config, text.replace("[net]\noffline = true\n", "")).expect("config");
    let (url, rev) = policy_repository(dir);
    std::fs::write(
        dir.join("graph.gdl"),
        "name = \"routed\", schema = 1;\nsource A: Words -> stream Token;\nsink   Engine: Words -> stream Token;\ntopology: A => Engine;\n",
    )
    .expect("graph");
    std::fs::write(
        dir.join("gdl.toml"),
        format!(
            "[graph]\npath = \"graph.gdl\"\n[messages]\ncrates = [\"regproto@0.1.0\"]\n[nodes]\ncrates = [\"regnodes@0.1.0\"]\n[gdl]\ngit = \"https://example.com/gdl\"\nrev = \"abc123\"\n\
             [segment.front]\nnodes = [\"A\"]\n[segment.engine]\nnodes = [\"Engine\"]\n\
             [class.global]\nsegments = [\"front\"]\n[class.model]\nsegments = [\"engine\"]\n[pools.model]\npolicy = \"by_words\"\n\
             [pool_policy.by_words]\ncrate = \"regpolicy\"\ngit = {url:?}\nrev = {rev:?}\ntype = \"regpolicy::ByWords\"\n"
        ),
    )
    .expect("spec");
    let spec = gdl_gen::Spec::load(&dir.join("gdl.toml")).expect("a policy crate from Git");
    let fleet = gdl_model::Fleet::from_toml(
        "[class.global]\nready = { front = 1 }\n[class.model]\nready = { engine = 1 }\n\
         [pool.global-us]\nclass = \"global\"\nregion = \"us-east-1\"\n\
         [pool.model-01]\nclass = \"model\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n",
    )
    .expect("a fleet");
    let files = gdl_gen::compose_files(
        &spec,
        Some(&fleet),
        &dir.join("out"),
        &dir.join("target"),
        gdl_gen::Sources::Fetched,
    )
    .expect("a bundle without --local");
    let chooser = &files[Path::new("front/Cargo.toml")];
    assert!(
        chooser.contains(&format!("regpolicy = {{ git = {url:?}, rev = {rev:?} }}")),
        "{chooser}"
    );
    assert!(
        files[Path::new("front/src/lib.rs")].contains("ByWords"),
        "{}",
        files[Path::new("front/src/lib.rs")]
    );
    assert!(!files[Path::new("engine/Cargo.toml")].contains("regpolicy"));
    // The archive builds anywhere, so it carries the images' Dockerfiles.
    use std::io::Read as _;
    let bytes =
        gdl_gen::export(&spec, &dir.join("target"), gdl_gen::Offers::Read).expect("exported");
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let mut names = Vec::new();
    for entry in archive.entries().expect("entries") {
        let mut entry = entry.expect("entry");
        let name = entry.path().expect("path").display().to_string();
        let mut text = String::new();
        entry.read_to_string(&mut text).expect("text");
        if name == "routed/front/Cargo.toml" {
            assert!(
                text.contains(&format!("regpolicy = {{ git = {url:?}, rev = {rev:?} }}")),
                "{text}"
            );
        }
        names.push(name);
    }
    for name in ["routed/front/Dockerfile", "routed/front/Cargo.toml"] {
        assert!(names.iter().any(|n| n == name), "{name} in {names:?}");
    }
}
