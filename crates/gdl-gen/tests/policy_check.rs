//! Policy bindings checked against crates that compile on their own.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::{root, target};

fn fixture() -> PathBuf {
    root().join("crates/gdl-gen/tests/fixtures/policy")
}

fn router(source: &str) -> tempfile::TempDir {
    let temp = tempfile::Builder::new()
        .prefix("gdl-policy-check-")
        .tempdir()
        .expect("scratch");
    std::fs::create_dir_all(temp.path().join("src")).expect("source directory");
    let path = |path: PathBuf| toml::Value::String(path.display().to_string());
    let mut manifest: toml::Table = std::fs::read_to_string(fixture().join("router/Cargo.toml"))
        .expect("manifest")
        .parse()
        .expect("manifest table");
    manifest.insert("workspace".into(), toml::Table::new().into());
    manifest["dependencies"]["gdl"]["path"] = path(root().join("crates/gdl"));
    manifest["dependencies"]["policy-messages"]["path"] = path(fixture().join("messages"));
    std::fs::write(
        temp.path().join("Cargo.toml"),
        toml::to_string(&manifest).expect("manifest text"),
    )
    .expect("manifest");
    std::fs::write(temp.path().join("src/lib.rs"), source).expect("policy source");
    std::fs::copy(fixture().join("Cargo.lock"), temp.path().join("Cargo.lock")).expect("lockfile");
    cargo_check(&temp.path().join("Cargo.toml"));
    temp
}

fn cargo_check(manifest: &Path) {
    let checked = Command::new("timeout")
        .args(["120", "cargo", "check", "--offline", "--manifest-path"])
        .arg(manifest)
        .env("CARGO_TARGET_DIR", target("policy-proof"))
        .output()
        .expect("cargo check");
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
}

fn spec(router: &Path) -> gdl_gen::Spec {
    let mut text: toml::Table = std::fs::read_to_string(fixture().join("gdl.toml"))
        .expect("spec")
        .parse()
        .expect("spec table");
    text["cell_policy"]["by_model"]["path"] = toml::Value::String(router.display().to_string());
    let graph = std::fs::read_to_string(fixture().join("policy.gdl")).expect("graph");
    gdl_gen::Spec::from_text(
        &toml::to_string(&text).expect("spec text"),
        &graph,
        &fixture(),
    )
    .expect("spec")
}

/// A trait of the same name does not make a type a gdl cell policy.
#[test]
fn check_refuses_an_unrelated_trait_named_cell_policy() {
    let router = router(
        r#"
pub trait CellPolicy {
    type Head;
}

pub struct ByModel;

impl CellPolicy for ByModel {
    type Head = gdl::Encoded;
}

impl gdl::Build for ByModel {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();

    async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> {
        Ok(Self)
    }
}
"#,
    );
    let spec = spec(router.path());
    let checked = gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read);
    assert!(
        checked.is_err(),
        "check accepts policy_router::ByModel, which implements a local CellPolicy trait but does not implement gdl::CellPolicy"
    );
}

fn unit_case() -> (tempfile::TempDir, String, String) {
    let router = router(
        r#"
pub struct ByModel;

impl gdl::CellPolicy for ByModel {
    type Head = ();

    fn choose(&self, _: gdl::CellRequest<'_, ()>, cells: &[gdl::Cell]) -> Option<gdl::CellChoice> {
        cells.iter().position(gdl::Cell::takes_work).map(gdl::CellChoice::new)
    }
}

impl gdl::Build for ByModel {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();

    async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> {
        Ok(Self)
    }
}

#[derive(gdl::Node)]
pub struct Model;

impl gdl::Build for Model {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();

    async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> {
        Ok(Self)
    }
}

impl gdl::Sink for Model {
    type Req = ();
    type Item = policy_messages::Answer;

    async fn generate(&self, _: gdl::Cx, (): ()) -> gdl::Result<gdl::Items<Self::Item>> {
        Err(gdl::Error::node("fixture"))
    }
}
"#,
    );
    let mut text: toml::Table = std::fs::read_to_string(fixture().join("gdl.toml"))
        .expect("spec")
        .parse()
        .expect("spec table");
    let path = toml::Value::String(router.path().display().to_string());
    text["nodes"]["crates"] = vec![path.clone()].into();
    text["cell_policy"]["by_model"]["path"] = path;
    let graph = std::fs::read_to_string(fixture().join("policy.gdl"))
        .expect("graph")
        .replace("Ask", "()");
    (router, toml::to_string(&text).expect("spec text"), graph)
}

/// Unit requests and policies registered in code already form a valid graph.
#[test]
fn check_accepts_a_unit_request_without_a_policy_binding() {
    let (_router, text, graph) = unit_case();
    let mut text: toml::Table = text.parse().expect("spec table");
    text.remove("cell_policy");
    let spec = gdl_gen::Spec::from_text(
        &toml::to_string(&text).expect("spec text"),
        &graph,
        &fixture(),
    )
    .expect("spec");
    gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
        .expect("unit requests work when the policy is registered in code");
}

/// Binding a policy must also work when the request's head is unit.
#[test]
fn check_accepts_a_bound_policy_for_a_unit_request() {
    let (_router, text, graph) = unit_case();
    let spec = gdl_gen::Spec::from_text(&text, &graph, &fixture()).expect("spec");
    gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
        .expect("a nongeneric public CellPolicy<Head = ()> can be bound for a unit request");
}

fn entry_case(policy: &str) -> (tempfile::TempDir, gdl_gen::Spec) {
    let router = router(
        r#"
use policy_messages::{Answer, Ask};

#[derive(serde::Deserialize, gdl::Config)]
pub struct Hosted {
    #[gdl(graph, default = true)]
    pub enabled: bool,
}

#[derive(gdl::Endpoints)]
pub struct Clients {
    #[gdl(endpoint = "/ask", enabled = "enabled")]
    pub ask: Option<gdl::Client<Ask, Answer>>,
}

#[derive(gdl::Node)]
#[gdl(config = Hosted)]
pub struct Server;

impl gdl::Build for Server {
    type Config = Hosted;
    type Metrics = ();
    async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> { Ok(Self) }
}

impl gdl::Service for Server {
    type Clients = Clients;
    async fn serve(self, _: Clients, _: impl std::future::Future<Output = ()> + Send + 'static) -> gdl::Result<()> {
        Ok(())
    }
}

#[derive(serde::Deserialize, gdl::Config)]
pub struct StampSettings {
    #[gdl(graph, default = "!")]
    pub suffix: String,
}

#[derive(gdl::Node)]
#[gdl(config = StampSettings)]
pub struct Stamp;

impl gdl::Build for Stamp {
    type Config = StampSettings;
    type Metrics = ();
    async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> { Ok(Self) }
}

impl gdl::Forward for Stamp {
    type In = Ask;
    type Out = Ask;
    async fn forward(&self, _: &gdl::Cx, request: Ask) -> gdl::Result<Ask> { Ok(request) }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySettings {}

pub struct ByModel;

impl gdl::Build for ByModel {
    type Config = PolicySettings;
    type Metrics = ();
    async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> { Ok(Self) }
}

impl gdl::CellPolicy for ByModel {
    type Head = gdl::Encoded;
    fn choose(&self, _: gdl::CellRequest<'_, gdl::Encoded>, cells: &[gdl::Cell]) -> Option<gdl::CellChoice> {
        cells.iter().position(gdl::Cell::takes_work).map(gdl::CellChoice::new)
    }
}
"#,
    );
    let mut text: toml::Table = std::fs::read_to_string(fixture().join("gdl.toml"))
        .expect("spec")
        .parse()
        .expect("spec table");
    let path = toml::Value::String(router.path().display().to_string());
    text["nodes"]["crates"] = vec![toml::Value::String("nodes".into()), path.clone()].into();
    text["nodes"].as_table_mut().expect("nodes table").insert(
        "pin".into(),
        toml::Table::from_iter([("front".into(), "policy_router::Server".into())]).into(),
    );
    text["segment"]["front"]["nodes"] = vec![toml::Value::String("front".into())].into();
    text["cells"]["model"]["policy"] = policy.into();
    let table = text["cell_policy"]
        .as_table_mut()
        .expect("policy table")
        .remove("by_model")
        .expect("binding");
    text["cell_policy"]
        .as_table_mut()
        .expect("policy table")
        .insert(policy.into(), table);
    text["cell_policy"][policy]["path"] = path;
    text.insert(
        "entry".into(),
        toml::Table::from_iter([(
            "ask".into(),
            toml::Table::from_iter([
                ("target".into(), "front".into()),
                (
                    "forward".into(),
                    vec![toml::Value::String("policy_router::Stamp".into())].into(),
                ),
            ])
            .into(),
        )])
        .into(),
    );
    let graph = std::fs::read_to_string(fixture().join("policy.gdl"))
        .expect("graph")
        .replace("Front", "front");
    let spec = gdl_gen::Spec::from_text(
        &toml::to_string(&text).expect("spec text"),
        &graph,
        &fixture(),
    )
    .expect("spec");
    (router, spec)
}

/// A separate policy name leaves the entry adapter's config in its own table.
#[test]
fn check_accepts_a_policy_named_apart_from_entry_adapters() {
    let (_router, spec) = entry_case("by_model");
    gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read)
        .expect("the entry adapter and policy have different build names");
}

/// The entry adapter's graph config must not be given to a cell policy.
/// Building the generated binary takes seconds on a warm target.
#[test]
fn check_refuses_a_policy_named_for_an_entry_adapter() {
    let (router, spec) = entry_case("front_ask_f0");
    let checked = gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read);
    if checked.is_ok() {
        let out = router.path().join("generated");
        let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
        gdl_gen::write(&out, &files).expect("write");
        std::fs::write(
            out.join("Cargo.toml"),
            "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
        )
        .expect("workspace");
        std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
        let built = Command::new("timeout")
            .args(["180", "cargo", "build", "--offline", "--manifest-path"])
            .arg(out.join("front/Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("examples"))
            .output()
            .expect("cargo build");
        assert!(
            built.status.success(),
            "{}",
            String::from_utf8_lossy(&built.stderr)
        );
        let roster = router.path().join("roster");
        std::fs::create_dir_all(&roster).expect("roster");
        let settings = router.path().join("settings.toml");
        std::fs::write(
            &settings,
            format!(
                "listen = \"127.0.0.1:0\"\nroster = {}\nrendezvous_ms = 1000\n",
                toml::Value::String(roster.display().to_string())
            ),
        )
        .expect("settings");
        let run = Command::new("timeout")
            .arg("5")
            .arg(target("examples").join("debug/policy-front"))
            .arg(settings)
            .output()
            .expect("generated binary");
        let stderr = String::from_utf8_lossy(&run.stderr);
        assert_eq!(run.status.code(), Some(1), "{stderr}");
        assert!(
            stderr.contains("building front_ask_f0")
                && stderr.contains("unknown field")
                && stderr.contains("`suffix`"),
            "{stderr}"
        );
        eprintln!("generated binary: {stderr}");
    }
    assert!(
        checked.is_err(),
        "check accepts a cell policy and a service entry adapter with the same build name; the adapter's suffix is injected into the policy's config"
    );
}

/// An explicit parent import names the same trait as a direct gdl import.
#[test]
fn check_accepts_a_policy_imported_from_its_parent() {
    let source = r#"
use gdl::CellPolicy;

mod implementation {
    use super::CellPolicy;

    pub struct ByModel;

    impl CellPolicy for ByModel {
        type Head = gdl::Encoded;

        fn choose(&self, _: gdl::CellRequest<'_, Self::Head>, _: &[gdl::Cell]) -> Option<gdl::CellChoice> {
            None
        }
    }

    impl gdl::Build for ByModel {
        type Config = std::collections::BTreeMap<String, String>;
        type Metrics = ();

        async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> {
            Ok(Self)
        }
    }
}

pub use implementation::ByModel;
"#;
    let direct = router(&source.replace("use super::CellPolicy;", "use gdl::CellPolicy;"));
    gdl_gen::check(
        &spec(direct.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("the direct import works");

    let parent = router(source);
    gdl_gen::check(
        &spec(parent.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("the public policy implements gdl::CellPolicy through a parent import");
}

/// A local module can shadow gdl in a glob without implementing gdl's trait.
/// Compiling the generated chooser proves the mismatch and takes seconds.
#[test]
fn check_refuses_a_policy_from_a_shadowed_gdl_glob() {
    let router = router(
        r#"
mod gdl {
    pub trait CellPolicy {
        type Head;
    }
}

use gdl::*;

pub struct ByModel;

impl CellPolicy for ByModel {
    type Head = ::gdl::Encoded;
}

impl ::gdl::Build for ByModel {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();

    async fn build(_: Self::Config, (): ()) -> ::gdl::Result<Self> {
        Ok(Self)
    }
}
"#,
    );
    let spec = spec(router.path());
    let checked = gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read);
    if checked.is_ok() {
        let out = router.path().join("generated");
        let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
        gdl_gen::write(&out, &files).expect("write");
        std::fs::write(
            out.join("Cargo.toml"),
            "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
        )
        .expect("workspace");
        std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
        let compiled = Command::new("timeout")
            .args(["120", "cargo", "check", "--offline", "--manifest-path"])
            .arg(out.join("front/Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("policy-proof"))
            .output()
            .expect("cargo check");
        let stderr = String::from_utf8_lossy(&compiled.stderr);
        assert!(!compiled.status.success(), "{stderr}");
        assert!(
            stderr.contains("E0277") && stderr.contains("CellPolicy") && stderr.contains("ByModel"),
            "{stderr}"
        );
        eprintln!("generated chooser: {stderr}");
    }
    assert!(
        checked.is_err(),
        "check accepts a local trait imported through a module named gdl; the generated chooser cannot register ByModel as gdl::CellPolicy"
    );
}

#[test]
fn check_accepts_a_policy_imported_by_a_parent_alias() {
    let source = r#"
use gdl::CellPolicy as Routing;

mod implementation {
    use super::Routing;

    pub struct ByModel;

    impl Routing for ByModel {
        type Head = gdl::Encoded;

        fn choose(&self, _: gdl::CellRequest<'_, Self::Head>, _: &[gdl::Cell]) -> Option<gdl::CellChoice> {
            None
        }
    }

    impl gdl::Build for ByModel {
        type Config = std::collections::BTreeMap<String, String>;
        type Metrics = ();

        async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> {
            Ok(Self)
        }
    }
}

pub use implementation::ByModel;
"#;
    let direct = router(&source.replace("use super::Routing;", "use gdl::CellPolicy as Routing;"));
    gdl_gen::check(
        &spec(direct.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("a direct alias names gdl's trait");

    let parent = router(source);
    gdl_gen::check(
        &spec(parent.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("a parent alias names the same gdl trait");
}

#[test]
fn check_refuses_a_shadowed_gdl_after_an_external_glob() {
    let source = r#"
mod facade {
    pub mod gdl {
        pub trait CellPolicy {
            type Head;
        }
    }
}

use ::gdl::*;
use facade::*;

pub struct ByModel;

impl gdl::CellPolicy for ByModel {
    type Head = ::gdl::Encoded;
}

impl ::gdl::Build for ByModel {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();

    async fn build(_: Self::Config, (): ()) -> ::gdl::Result<Self> {
        Ok(Self)
    }
}
"#;
    let direct = router(&source.replace("use ::gdl::*;\n", ""));
    gdl_gen::check(
        &spec(direct.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect_err("the local gdl module holds an unrelated trait");

    let router = router(source);
    let spec = spec(router.path());
    let checked = gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read);
    if checked.is_ok() {
        let out = router.path().join("generated");
        let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
        gdl_gen::write(&out, &files).expect("write");
        std::fs::write(
            out.join("Cargo.toml"),
            "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
        )
        .expect("workspace");
        std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
        let compiled = Command::new("timeout")
            .args(["120", "cargo", "check", "--offline", "--manifest-path"])
            .arg(out.join("front/Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("policy-proof"))
            .output()
            .expect("cargo check");
        let stderr = String::from_utf8_lossy(&compiled.stderr);
        assert!(!compiled.status.success(), "{stderr}");
        assert!(
            stderr.contains("E0277") && stderr.contains("CellPolicy") && stderr.contains("ByModel"),
            "{stderr}"
        );
        eprintln!("generated chooser: {stderr}");
    }
    assert!(
        checked.is_err(),
        "an external glob must not hide a local gdl module imported by another glob"
    );
}

#[test]
fn check_accepts_an_explicit_import_of_the_gdl_crate() {
    let source =
        std::fs::read_to_string(fixture().join("router/src/lib.rs")).expect("policy source");
    let direct = router(&source);
    gdl_gen::check(
        &spec(direct.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("the policy implements gdl's trait");

    let imported = source.replace("use gdl::{Build,", "use gdl::{self, Build,");
    assert_ne!(source, imported, "the control and explicit import differ");
    let imported = router(&imported);
    gdl_gen::check(
        &spec(imported.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("explicitly importing the gdl crate keeps the same trait");
}

fn dependency_rename(names_trait: bool) -> tempfile::TempDir {
    let source =
        std::fs::read_to_string(fixture().join("router/src/lib.rs")).expect("policy source");
    let router = router(&source);
    let facade = router.path().join("facade");
    std::fs::create_dir_all(facade.join("src")).expect("facade source directory");
    std::fs::write(
        facade.join("Cargo.toml"),
        format!(
            "[package]\nname = \"policy-facade\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[dependencies]\ngdl = {{ path = {} }}\n",
            toml::Value::String(root().join("crates/gdl").display().to_string())
        ),
    )
    .expect("facade manifest");
    std::fs::write(
        facade.join("src/lib.rs"),
        "pub use gdl::CellPolicy as Routing;\n",
    )
    .expect("facade source");
    let manifest_path = router.path().join("Cargo.toml");
    let mut manifest: toml::Table = std::fs::read_to_string(&manifest_path)
        .expect("manifest")
        .parse()
        .expect("manifest table");
    manifest["dependencies"]
        .as_table_mut()
        .expect("dependencies")
        .insert(
            "policy-facade".into(),
            toml::Table::from_iter([("path".into(), "facade".into())]).into(),
        );
    std::fs::write(
        &manifest_path,
        toml::to_string(&manifest).expect("manifest text"),
    )
    .expect("manifest");
    let source = source
        .replace(
            "Build, Cell, CellChoice, CellPolicy, CellRequest",
            "Build, Cell, CellChoice, CellRequest",
        )
        .replace("impl CellPolicy for", "impl Routing for");
    let source = format!("use policy_facade::Routing;\n{source}").replace("//!", "//");
    let source = match names_trait {
        true => format!("use gdl::CellPolicy;\n{source}"),
        false => source,
    };
    std::fs::write(router.path().join("src/lib.rs"), source).expect("policy source");
    cargo_check(&manifest_path);
    router
}

#[test]
fn check_accepts_a_dependency_trait_alias_when_cell_policy_is_also_named() {
    let router = dependency_rename(true);
    gdl_gen::check(
        &spec(router.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("the resolver follows a dependency's rename when CellPolicy is also named");
}

#[test]
fn check_accepts_a_policy_trait_renamed_by_a_dependency() {
    let router = dependency_rename(false);
    gdl_gen::check(
        &spec(router.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("removing an unused import keeps the same gdl::CellPolicy implementation");
}

fn unrelated_dependency(padding: usize) -> tempfile::TempDir {
    let source =
        std::fs::read_to_string(fixture().join("router/src/lib.rs")).expect("policy source");
    let router = router(&source);
    let helper = router.path().join("helper");
    std::fs::create_dir_all(helper.join("src")).expect("helper source directory");
    std::fs::write(
        helper.join("Cargo.toml"),
        "[package]\nname = \"policy-helper\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("helper manifest");
    std::fs::write(
        helper.join("src/lib.rs"),
        format!("pub trait Helper {{}}\n// {}\n", "x".repeat(padding)),
    )
    .expect("helper source");
    let manifest_path = router.path().join("Cargo.toml");
    let mut manifest: toml::Table = std::fs::read_to_string(&manifest_path)
        .expect("manifest")
        .parse()
        .expect("manifest table");
    manifest["dependencies"]
        .as_table_mut()
        .expect("dependencies")
        .insert(
            "policy-helper".into(),
            toml::Table::from_iter([("path".into(), "helper".into())]).into(),
        );
    std::fs::write(
        &manifest_path,
        toml::to_string(&manifest).expect("manifest text"),
    )
    .expect("manifest");
    std::fs::write(
        router.path().join("src/lib.rs"),
        format!("{source}\npub struct Utility;\nimpl policy_helper::Helper for Utility {{}}\n"),
    )
    .expect("policy source");
    cargo_check(&manifest_path);
    router
}

#[test]
fn check_accepts_a_policy_with_an_unrelated_dependency_trait() {
    let router = unrelated_dependency(0);
    gdl_gen::check(
        &spec(router.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("the helper's trait is unrelated to the cell policy");
}

#[test]
fn checking_a_policy_does_not_read_unrelated_trait_dependencies() {
    let router = unrelated_dependency(16 << 20);
    gdl_gen::check(
        &spec(router.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("a comment in an unrelated trait's crate cannot change the offered policy");
}

/// An import can shadow gdl even when its crate does not depend on gdl.
#[test]
fn check_refuses_a_dependency_that_shadows_the_gdl_crate() {
    let router = unrelated_dependency(0);
    gdl_gen::check(
        &spec(router.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("the real policy works with the unrelated dependency");
    std::fs::write(
        router.path().join("helper/src/lib.rs"),
        "pub trait CellPolicy { type Head; }\n",
    )
    .expect("unrelated trait");
    std::fs::write(
        router.path().join("src/lib.rs"),
        r#"
use policy_helper as gdl;

pub struct ByModel;

impl gdl::CellPolicy for ByModel {
    type Head = ::gdl::Encoded;
}

impl ::gdl::Build for ByModel {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();

    async fn build(_: Self::Config, (): ()) -> ::gdl::Result<Self> {
        Ok(Self)
    }
}
"#,
    )
    .expect("policy source");
    cargo_check(&router.path().join("Cargo.toml"));
    let spec = spec(router.path());
    let checked = gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read);
    if checked.is_ok() {
        let out = router.path().join("generated");
        let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
        gdl_gen::write(&out, &files).expect("write");
        std::fs::write(
            out.join("Cargo.toml"),
            "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
        )
        .expect("workspace");
        std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
        let compiled = Command::new("timeout")
            .args(["120", "cargo", "check", "--offline", "--manifest-path"])
            .arg(out.join("front/Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("policy-proof"))
            .output()
            .expect("cargo check");
        let stderr = String::from_utf8_lossy(&compiled.stderr);
        assert!(!compiled.status.success(), "{stderr}");
        assert!(
            stderr.contains("E0277") && stderr.contains("CellPolicy") && stderr.contains("ByModel"),
            "{stderr}"
        );
        eprintln!("generated chooser: {stderr}");
    }
    assert!(
        checked.is_err(),
        "check accepts policy_helper::CellPolicy as gdl::CellPolicy after the dependency gate skips the explicit import that shadows gdl"
    );
}

/// A disabled trait cannot hide the imported trait that Rust uses.
#[test]
fn check_accepts_a_policy_when_a_shadowing_trait_is_disabled() {
    let source =
        std::fs::read_to_string(fixture().join("router/src/lib.rs")).expect("policy source");
    let direct = router(&source);
    gdl_gen::check(
        &spec(direct.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("the policy implements gdl's trait");

    let source = format!("{source}\n#[cfg(any())]\npub trait CellPolicy {{}}\n");
    let router = router(&source);
    gdl_gen::check(
        &spec(router.path()),
        &target("gdl-gen-tests"),
        gdl_gen::Offers::Read,
    )
    .expect("a cfg-disabled trait cannot hide the imported gdl::CellPolicy");
}

/// A name `gdl` that a crate without gdl binds hides the gdl crate.
/// The reader does not enter such a crate, so it cannot see that binding:
/// a documented limitation, kept here as its failing case.
#[test]
#[ignore = "known limitation: a `gdl` that a module of a crate without gdl binds is not seen (\"The reader finds a cell policy only where it can follow the trait's path\" in docs/src/operations/limitations.md)"]
fn check_refuses_a_dependency_module_import_that_shadows_gdl() {
    let router = unrelated_dependency(0);
    std::fs::write(
        router.path().join("helper/src/lib.rs"),
        "pub mod routing { pub trait CellPolicy { type Head; } }\n",
    )
    .expect("unrelated trait");
    std::fs::write(
        router.path().join("src/lib.rs"),
        r#"
use policy_helper::routing as gdl;

pub struct ByModel;

impl gdl::CellPolicy for ByModel {
    type Head = ::gdl::Encoded;
}

impl ::gdl::Build for ByModel {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();

    async fn build(_: Self::Config, (): ()) -> ::gdl::Result<Self> {
        Ok(Self)
    }
}
"#,
    )
    .expect("policy source");
    cargo_check(&router.path().join("Cargo.toml"));
    let spec = spec(router.path());
    let checked = gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read);
    if checked.is_ok() {
        let out = router.path().join("generated");
        let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
        gdl_gen::write(&out, &files).expect("write");
        std::fs::write(
            out.join("Cargo.toml"),
            "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
        )
        .expect("workspace");
        std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
        let compiled = Command::new("timeout")
            .args(["120", "cargo", "check", "--offline", "--manifest-path"])
            .arg(out.join("front/Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("policy-proof"))
            .output()
            .expect("cargo check");
        let stderr = String::from_utf8_lossy(&compiled.stderr);
        assert!(!compiled.status.success(), "{stderr}");
        assert!(
            stderr.contains("E0277") && stderr.contains("CellPolicy") && stderr.contains("ByModel"),
            "{stderr}"
        );
        eprintln!("generated chooser: {stderr}");
    }
    assert!(
        checked.is_err(),
        "check accepts policy_helper::routing::CellPolicy as gdl::CellPolicy when a qualified import shadows gdl"
    );
}

/// A name `gdl` that a crate without gdl binds hides the gdl crate.
/// The reader does not enter such a crate, so it cannot see that binding:
/// a documented limitation, kept here as its failing case.
#[test]
#[ignore = "known limitation: a `gdl` that a glob of a crate without gdl binds is not seen (\"The reader finds a cell policy only where it can follow the trait's path\" in docs/src/operations/limitations.md)"]
fn check_refuses_a_dependency_glob_that_shadows_gdl() {
    let router = unrelated_dependency(0);
    std::fs::write(
        router.path().join("helper/src/lib.rs"),
        "pub mod gdl { pub trait CellPolicy { type Head; } }\n",
    )
    .expect("unrelated trait");
    std::fs::write(
        router.path().join("src/lib.rs"),
        r#"
use policy_helper::*;

pub struct ByModel;

impl gdl::CellPolicy for ByModel {
    type Head = ::gdl::Encoded;
}

impl ::gdl::Build for ByModel {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();

    async fn build(_: Self::Config, (): ()) -> ::gdl::Result<Self> {
        Ok(Self)
    }
}
"#,
    )
    .expect("policy source");
    cargo_check(&router.path().join("Cargo.toml"));
    let spec = spec(router.path());
    let checked = gdl_gen::check(&spec, &target("gdl-gen-tests"), gdl_gen::Offers::Read);
    if checked.is_ok() {
        let out = router.path().join("generated");
        let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
        gdl_gen::write(&out, &files).expect("write");
        std::fs::write(
            out.join("Cargo.toml"),
            "[workspace]\nmembers = [\"contract\", \"front\", \"model\"]\nresolver = \"3\"\n",
        )
        .expect("workspace");
        std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
        let compiled = Command::new("timeout")
            .args(["120", "cargo", "check", "--offline", "--manifest-path"])
            .arg(out.join("front/Cargo.toml"))
            .env("CARGO_TARGET_DIR", target("policy-proof"))
            .output()
            .expect("cargo check");
        let stderr = String::from_utf8_lossy(&compiled.stderr);
        assert!(!compiled.status.success(), "{stderr}");
        assert!(
            stderr.contains("E0277") && stderr.contains("CellPolicy") && stderr.contains("ByModel"),
            "{stderr}"
        );
        eprintln!("generated chooser: {stderr}");
    }
    assert!(
        checked.is_err(),
        "check ignores the gdl module imported by an unrelated dependency's glob and accepts the unrelated CellPolicy trait"
    );
}
