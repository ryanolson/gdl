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

#[test]
fn the_book_explains_which_router_reexports_can_be_bound() {
    let policy = r#"
impl gdl::PoolPolicy for ByText {
    type Head = gdl::Encoded;
    fn choose(&self, _: gdl::PoolRequest<'_, Self::Head>, pools: &[gdl::Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
        pools.iter().position(gdl::Pool::takes_work).map(gdl::PoolChoice::new).ok_or_else(gdl::NoPool::new)
    }
}
"#;
    for name in ["ByText", "PublicSelector"] {
        let source = format!(
            "mod implementation {{\n{}\n{policy}\n}}\npub use implementation::ByText as {name};\n",
            fixture_text("router/src/lib.rs")
        );
        let router = router(&source, "selector_router");
        let workspace = gdl_extract::Workspace::load(
            &router.path().join("Cargo.toml"),
            &gdl_extract::Cargo::trusted(),
        )
        .expect("the compiled crate can be read");
        let extracted = workspace.extract("selector-router").expect("the package");
        assert!(extracted.problems.is_empty(), "{:?}", extracted.problems);
        for entries in [&extracted.offer.policies, &extracted.offer.selectors] {
            assert_eq!(
                entries.iter().any(|entry| entry.name == name),
                name == "ByText"
            );
        }
    }

    let book = std::fs::read_to_string(root().join("docs/src/operations/limitations.md"))
        .expect("limitations chapter");
    let explained = book.split("\n\n").any(|paragraph| {
        paragraph.contains("pool polic")
            && paragraph.contains("selector")
            && paragraph.contains("root")
            && (paragraph.contains("re-export") || paragraph.contains("reexport"))
    });
    assert!(
        explained,
        "both renamed root re-exports build and are intentionally refused, but the limitations chapter does not explain this type-name restriction for pool policies and selectors"
    );
}

#[test]
fn a_catalogue_selector_must_have_the_bound_library() {
    let router = router(
        r#"
pub struct ByText;

impl gdl::Build for ByText {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();
    async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> {
        Ok(Self)
    }
}


impl gdl::Selector for ByText {
    type Head = gdl::Encoded;
    async fn select<'a>(&'a self, _: gdl::Request<'a, Self::Head>, instances: gdl::Instances<'a>) -> gdl::Result<gdl::Choice<'a>> {
        Ok(gdl::Choice::new(instances.get(0).expect("never empty")))
    }
}
"#,
        "router_library",
    );
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .arg("-C")
            .arg(router.path())
            .args(args)
            .output()
            .expect("git");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("git output")
            .trim()
            .to_string()
    };
    git(&["init", "--quiet"]);
    git(&["add", "Cargo.toml", "Cargo.lock", "src"]);
    git(&[
        "-c",
        "commit.gpgsign=false",
        "-c",
        "user.name=gdl-test",
        "-c",
        "user.email=gdl@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    ]);
    let rev = git(&["rev-parse", "HEAD"]);
    let url = format!("file://{}", router.path().display());
    let fetched = gdl_extract::Workspace::fetch_git(
        "selector-router",
        &url,
        &rev,
        &target("gdl-gen-tests"),
        &gdl_extract::Cargo::trusted(),
    )
    .expect("Git selector workspace");
    let selector = fetched.extract("selector-router").expect("selector").offer;
    assert!(
        selector.messages.is_empty() && selector.exports.is_empty() && selector.nodes.is_empty()
    );
    assert_eq!(selector.selectors[0].ty, "router_library::ByText");
    let workspace = gdl_extract::Workspace::load(
        &fixture().join("Cargo.toml"),
        &gdl_extract::Cargo::trusted(),
    )
    .expect("fixture workspace");
    let messages = workspace
        .extract("selector-messages")
        .expect("messages")
        .offer;
    let nodes = workspace.extract("selector-nodes").expect("nodes").offer;
    let offers = [&messages, &nodes, &selector];
    let text = fixture_text("gdl.toml")
        .replace(
            "path = \"router\"",
            &format!("git = {url:?}\nrev = {rev:?}\nlib = \"router_library\""),
        )
        .replace("selector_router::ByText", "router_library::ByText");
    let checked = |text: &str, from| {
        let spec = gdl_gen::Spec::from_text(text, &fixture_text("selector.gdl"), &fixture())
            .expect("spec");
        gdl_gen::check(&spec, &target("gdl-gen-tests"), from)
    };
    checked(&text, gdl_gen::Offers::Read).expect("the real library matches");
    checked(&text, gdl_gen::Offers::Given(&offers)).expect("the catalogue library matches");
    let wrong = text
        .replace("lib = \"router_library\"", "lib = \"selector_router\"")
        .replace("router_library::ByText", "selector_router::ByText");
    let error = checked(&wrong, gdl_gen::Offers::Read)
        .expect_err("source reader refuses the wrong library");
    assert!(
        error.to_string().contains("library does not match"),
        "{error}"
    );
    if checked(&wrong, gdl_gen::Offers::Given(&offers)).is_ok() {
        for (text, matches) in [(&text, true), (&wrong, false)] {
            let out = router
                .path()
                .join(if matches { "matching" } else { "mismatched" });
            let spec = gdl_gen::Spec::from_text(text, &fixture_text("selector.gdl"), &fixture())
                .expect("spec");
            let archive = gdl_gen::export(
                &spec,
                &target("gdl-gen-tests"),
                gdl_gen::Offers::Given(&offers),
            )
            .expect("the accepted spec exports");
            tar::Archive::new(flate2::read::GzDecoder::new(archive.as_slice()))
                .unpack(&out)
                .expect("exported files");
            let manifest = out.join("selector/front/Cargo.toml");
            let patch = format!(
                "\n[patch.{url:?}]\ngdl = {{ path = {} }}\n",
                toml::Value::String(root().join("crates/gdl").display().to_string())
            );
            std::fs::write(
                &manifest,
                format!(
                    "{}{patch}",
                    std::fs::read_to_string(&manifest).expect("manifest")
                ),
            )
            .expect("use the runtime's copy of gdl");
            std::fs::copy(
                root().join("Cargo.lock"),
                out.join("selector/front/Cargo.lock"),
            )
            .expect("segment lockfile");
            let built = cargo_check(&manifest);
            let stderr = String::from_utf8_lossy(&built.stderr);
            if matches {
                assert!(
                    built.status.success(),
                    "the matching library must build: {stderr}"
                );
            } else {
                assert!(
                    !built.status.success()
                        && stderr.contains("error[E0433]")
                        && stderr.contains("selector_router"),
                    "the generated crate must expose the wrong library: {stderr}"
                );
                panic!(
                    "catalogue check accepted selector_router::ByText, but its sender cannot build:\n{stderr}"
                );
            }
        }
    }
}
