//! Selector bindings checked against crates that build on their own.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use common::{root, target};

fn fixture() -> PathBuf {
    root().join("crates/gdl-gen/tests/fixtures/selector")
}

fn fixture_text(file: &str) -> String {
    std::fs::read_to_string(fixture().join(file)).expect("fixture")
}

fn cargo_check(manifest: &Path) -> Output {
    Command::new("timeout")
        .args(["300", "cargo", "check", "--offline", "--manifest-path"])
        .arg(manifest)
        .env("CARGO_TARGET_DIR", target("binding-proof"))
        .output()
        .expect("cargo check")
}

fn router(source: &str, lib: &str) -> tempfile::TempDir {
    let temp = tempfile::Builder::new()
        .prefix("gdl-binding-")
        .tempdir()
        .expect("scratch");
    std::fs::create_dir_all(temp.path().join("src")).expect("source directory");
    let path = |path: PathBuf| toml::Value::String(path.display().to_string());
    let mut manifest: toml::Table = fixture_text("router/Cargo.toml")
        .parse()
        .expect("manifest table");
    manifest.insert("workspace".into(), toml::Table::new().into());
    manifest.insert("lib".into(), toml::toml! { name = lib }.into());
    manifest["dependencies"]["gdl"]["path"] = path(root().join("crates/gdl"));
    manifest["dependencies"]["selector-messages"]["path"] = path(fixture().join("messages"));
    std::fs::write(
        temp.path().join("Cargo.toml"),
        toml::to_string(&manifest).expect("manifest text"),
    )
    .expect("manifest");
    std::fs::write(temp.path().join("src/lib.rs"), source).expect("selector source");
    std::fs::copy(fixture().join("Cargo.lock"), temp.path().join("Cargo.lock")).expect("lockfile");
    let checked = cargo_check(&temp.path().join("Cargo.toml"));
    assert!(
        checked.status.success(),
        "the selector crate must build: {}",
        String::from_utf8_lossy(&checked.stderr)
    );
    temp
}

fn spec(router: &Path, ty: &str) -> gdl_gen::Spec {
    let mut text: toml::Table = fixture_text("gdl.toml").parse().expect("spec table");
    text["selector"]["by_text"]["path"] = toml::Value::String(router.display().to_string());
    text["selector"]["by_text"]["type"] = ty.into();
    gdl_gen::Spec::from_text(
        &toml::to_string(&text).expect("spec text"),
        &fixture_text("selector.gdl"),
        &fixture(),
    )
    .expect("spec")
}

#[test]
#[ignore = "F1: selector library names bypass the generated segment's crate-name checks"]
fn check_refuses_a_selector_library_that_shadows_the_runtime() {
    let router = router(&fixture_text("router/src/lib.rs"), "gdl_runtime");
    let spec = spec(router.path(), "gdl_runtime::ByText");
    let checked = gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read);
    if checked.is_ok() {
        let out = router.path().join("generated");
        let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests"))
            .expect("the accepted spec renders");
        gdl_gen::write(&out, &files).expect("generated files");
        std::fs::write(
            out.join("Cargo.toml"),
            "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
        )
        .expect("generated workspace");
        std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("segment lockfile");
        let built = cargo_check(&out.join("front/Cargo.toml"));
        let stderr = String::from_utf8_lossy(&built.stderr);
        assert!(
            !built.status.success()
                && stderr.contains("error[E0464]")
                && stderr.contains("dependency `gdl_runtime`"),
            "the generated crate must expose the library collision: {stderr}"
        );
        panic!(
            "check accepted selector library gdl_runtime, but its sender cannot build:\n{stderr}"
        );
    }
    assert!(
        checked
            .expect_err("a library collision")
            .to_string()
            .contains("crate name `gdl_runtime`"),
        "the diagnostic must name the colliding library"
    );
}
