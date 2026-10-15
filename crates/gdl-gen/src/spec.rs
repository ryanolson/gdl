//! The generator's input: where the contract and the node implementations
//! live, and which segment hosts which nodes.
//!
//! ```toml
//! [contract]
//! path = "contract"              # the contract crate, relative to this file
//! module = "chat_contract"       # optional: where `graph!` was invoked
//!
//! [gdl]
//! path = "../../crates/gdl"      # or: version = "0.1"
//!
//! [segment.front]
//! nodes = ["A", "B", "C", "G"]
//!
//! [bind.B]
//! crate = "front-nodes"          # the implementation's package
//! path = "front-nodes"           # relative to this file
//! type = "front_nodes::Preprocess"
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::Error;

/// A parsed and resolved generator input.
#[derive(Debug, Clone)]
pub struct Spec {
    /// The spec file's own path, as given.
    pub(crate) file: PathBuf,
    /// The spec file's directory; relative paths in it resolve from here.
    pub(crate) dir: PathBuf,
    /// The contract crate.
    pub(crate) contract: Contract,
    /// How generated crates depend on gdl.
    pub(crate) gdl: GdlDep,
    /// The placement: the `[segment.*]` tables.
    pub(crate) placement: gdl::Placement,
    /// Node name to implementation.
    pub(crate) bind: BTreeMap<String, Bind>,
    /// What inspecting the bound crates found wrong. Reported with every
    /// other problem when the spec is validated, not one at a time here.
    pub(crate) problems: Vec<String>,
}

/// The contract crate.
#[derive(Debug, Clone)]
pub(crate) struct Contract {
    /// The package name.
    pub(crate) package: String,
    /// Its directory, resolved.
    pub(crate) path: PathBuf,
    /// The Rust path of the module holding the `graph!` expansion.
    pub(crate) module: String,
}

/// How generated crates depend on gdl.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GdlDep {
    /// A path dependency, resolved.
    Path(PathBuf),
    /// A registry version requirement.
    Version(String),
}

/// One node's implementation.
#[derive(Debug, Clone)]
pub(crate) struct Bind {
    /// The implementation's package name.
    pub(crate) package: String,
    /// Its directory, resolved.
    pub(crate) path: PathBuf,
    /// The implementing type's Rust path.
    pub(crate) ty: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    contract: RawContract,
    gdl: RawGdl,
    #[serde(default)]
    segment: toml::Table,
    #[serde(default)]
    bind: BTreeMap<String, RawBind>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawContract {
    path: PathBuf,
    module: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGdl {
    path: Option<PathBuf>,
    version: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBind {
    #[serde(rename = "crate")]
    package: String,
    path: PathBuf,
    #[serde(rename = "type")]
    ty: String,
}

impl Spec {
    /// The spec file's directory; relative paths in it resolve from here.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Read and resolve the spec at `file`. Each bound crate's manifest
    /// must name the package the spec gives for it; what is wrong with the
    /// bound crates is reported when the spec is validated, with every other
    /// problem.
    pub fn load(file: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(file)
            .map_err(|e| Error::new(format!("reading {}: {e}", file.display())))?;
        let dir = file.parent().map(Path::to_path_buf).unwrap_or_default();
        let mut spec = Self::parse(&text, file, &dir)?;
        for (node, bind) in &spec.bind {
            match package_name(&bind.path) {
                Err(error) => spec.problems.push(format!("[bind.{node}]: {error}")),
                Ok(found) if found != bind.package => spec.problems.push(format!(
                    "[bind.{node}]: {} holds package `{found}`, not `{}`",
                    bind.path.display(),
                    bind.package
                )),
                Ok(_) => {}
            }
        }
        Ok(spec)
    }

    fn parse(text: &str, file: &Path, dir: &Path) -> Result<Self, Error> {
        let raw: Raw =
            toml::from_str(text).map_err(|e| Error::new(format!("{}: {e}", file.display())))?;
        let contract_path = dir.join(&raw.contract.path);
        let package = package_name(&contract_path)?;
        let module = raw
            .contract
            .module
            .unwrap_or_else(|| package.replace('-', "_"));
        let gdl = match (raw.gdl.path, raw.gdl.version) {
            (Some(path), None) => GdlDep::Path(dir.join(path)),
            (None, Some(version)) => GdlDep::Version(version),
            _ => {
                return Err(Error::new(
                    "[gdl] needs exactly one of `path` and `version`",
                ));
            }
        };
        let mut placement_table = toml::Table::new();
        placement_table.insert("segment".into(), toml::Value::Table(raw.segment));
        let placement = gdl::Placement::from_toml(&placement_table.to_string())
            .map_err(|e| Error::new(format!("{}: [segment]: {e}", file.display())))?;
        let bind = raw
            .bind
            .into_iter()
            .map(|(node, b)| {
                let bind = Bind {
                    package: b.package,
                    path: dir.join(b.path),
                    ty: b.ty,
                };
                (node, bind)
            })
            .collect();
        Ok(Self {
            file: file.to_path_buf(),
            dir: dir.to_path_buf(),
            contract: Contract {
                package,
                path: contract_path,
                module,
            },
            gdl,
            placement,
            bind,
            problems: Vec::new(),
        })
    }
}

/// A crate's package name, from its `Cargo.toml`.
fn package_name(dir: &Path) -> Result<String, Error> {
    let manifest = dir.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)
        .map_err(|e| Error::new(format!("reading {}: {e}", manifest.display())))?;
    let table: toml::Table =
        toml::from_str(&text).map_err(|e| Error::new(format!("{}: {e}", manifest.display())))?;
    table
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(toml::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Error::new(format!("{} has no [package] name", manifest.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("gdl-gen-spec-{name}-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("contract")).expect("dir");
        std::fs::write(
            dir.join("contract/Cargo.toml"),
            "[package]\nname = \"chat-contract\"\n",
        )
        .expect("write");
        dir
    }

    #[test]
    fn a_spec_resolves_paths_from_its_own_directory() {
        let dir = contract_dir("resolve");
        let text = r#"
            [contract]
            path = "contract"
            [gdl]
            path = "../gdl"
            [segment.front]
            nodes = ["A", "B"]
            [bind.B]
            crate = "front-nodes"
            path = "front"
            type = "front_nodes::B"
        "#;
        let spec = Spec::parse(text, &dir.join("gdl.toml"), &dir).expect("spec");
        assert_eq!(spec.contract.package, "chat-contract");
        assert_eq!(spec.contract.module, "chat_contract");
        assert_eq!(spec.contract.path, dir.join("contract"));
        assert_eq!(spec.gdl, GdlDep::Path(dir.join("../gdl")));
        assert_eq!(spec.bind["B"].path, dir.join("front"));
        assert_eq!(
            spec.placement,
            gdl::Placement::new().segment("front", ["A", "B"])
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_misspelled_key_is_an_error() {
        let dir = contract_dir("misspelled");
        let text = r#"
            [contract]
            path = "contract"
            [gdl]
            version = "0.1"
            [bind.B]
            crate = "x"
            path = "x"
            typ = "x::B"
        "#;
        let Err(error) = Spec::parse(text, &dir.join("gdl.toml"), &dir) else {
            panic!("a misspelled key parsed");
        };
        assert!(error.to_string().contains("typ"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn every_bad_binding_is_found() {
        let dir = contract_dir("package");
        std::fs::create_dir_all(dir.join("front")).expect("dir");
        std::fs::write(
            dir.join("front/Cargo.toml"),
            "[package]\nname = \"other-nodes\"\n",
        )
        .expect("write");
        std::fs::write(
            dir.join("gdl.toml"),
            "[contract]\npath = \"contract\"\n[gdl]\nversion = \"0.1\"\n\
             [bind.B]\ncrate = \"front-nodes\"\npath = \"front\"\ntype = \"front_nodes::B\"\n\
             [bind.C]\ncrate = \"gone-nodes\"\npath = \"gone\"\ntype = \"gone_nodes::C\"\n",
        )
        .expect("write");
        let spec = Spec::load(&dir.join("gdl.toml")).expect("the spec itself parses");
        assert_eq!(spec.problems.len(), 2, "{:#?}", spec.problems);
        assert!(spec.problems[0].contains("holds package `other-nodes`, not `front-nodes`"));
        assert!(
            spec.problems[1].starts_with("[bind.C]: reading"),
            "{}",
            spec.problems[1]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gdl_needs_one_source() {
        let dir = contract_dir("gdl");
        let text = "[contract]\npath = \"contract\"\n[gdl]\n";
        assert!(Spec::parse(text, &dir.join("gdl.toml"), &dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
