//! The generator's input: where the contract and the node implementations
//! live, and which segment hosts which nodes.
//!
//! ```toml
//! [contract]
//! path = "contract"              # the contract crate, relative to this file
//! module = "chat_contract"       # optional: where `graph!` was invoked
//!
//! # Or, instead of [contract], a graph file and the crates that define
//! # its messages:
//! [graph]
//! path = "chat.gdl"              # relative to this file
//!
//! [messages]
//! crates = ["protocols"]         # directories, relative to this file, or name@version
//! pin = { Chunk = "chat_protocols::Chunk" }   # optional: a name's type
//!
//! [nodes]                        # with [graph], instead of [bind.*]
//! crates = ["engine-mock"]       # node crates, read from their source
//! pin = { Engine = "chat_engine_mock::Engine" }   # optional
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
    pub(crate) deployment: Option<crate::deployment::Settings>,
    pub(crate) pipeline_names: BTreeMap<String, String>,
    pub(crate) entry: BTreeMap<String, crate::service::EntrySpec>,
    pub(crate) service: Option<crate::service::ServiceBinding>,
    /// Resolved graph-affecting values, keyed by node instance.
    pub(crate) config: BTreeMap<String, gdl_model::GraphConfig>,
    /// The spec file's own path, as given.
    pub(crate) file: PathBuf,
    /// The spec's text, when it was given as text rather than a file: an
    /// export holds it, so the archive can export itself again.
    pub(crate) text: Option<String>,
    /// The spec file's directory; relative paths in it resolve from here.
    pub(crate) dir: PathBuf,
    /// Where the graph is defined.
    pub(crate) source: Source,
    /// How generated crates depend on gdl.
    pub(crate) gdl: GdlDep,
    /// Segment placement and request-boundary routing policies.
    pub(crate) placement: gdl_model::Placement,
    /// Node name to implementation.
    pub(crate) bind: BTreeMap<String, Bind>,
    /// What inspecting the bound crates found wrong. Reported with every
    /// other problem when the spec is validated, not one at a time here.
    pub(crate) problems: Vec<String>,
}

/// Where the graph is defined.
#[derive(Debug, Clone)]
pub(crate) enum Source {
    /// A contract crate with a `graph!` invocation: `[contract]`.
    Contract(Contract),
    /// A graph file whose message names resolve against crates:
    /// `[graph]` and `[messages]`.
    Graph(GraphSource),
}

/// A graph file and the crates that define its messages.
#[derive(Debug, Clone)]
pub(crate) struct GraphSource {
    /// The graph file, resolved.
    pub(crate) path: PathBuf,
    /// The graph file's text, when the spec was given it rather than a
    /// file to read: a composer's draft is never written to disk.
    pub(crate) text: Option<String>,
    /// The crates whose messages the graph's names resolve against.
    pub(crate) crates: Vec<ListedCrate>,
    /// Message name to the path of the type it takes, for a name that
    /// would otherwise be ambiguous or that differs from its type's.
    pub(crate) pins: BTreeMap<String, String>,
    /// The crates whose declared nodes the graph's nodes bind to.
    pub(crate) nodes: Vec<ListedCrate>,
    /// Graph node name to the type of the declared node it binds to, for
    /// a name that would otherwise be ambiguous or that differs from the
    /// declared node's.
    pub(crate) node_pins: BTreeMap<String, String>,
}

/// A crate a spec lists: under `[messages]`, for the messages it offers,
/// or under `[nodes]`, for its nodes.
#[derive(Debug, Clone)]
pub(crate) struct ListedCrate {
    /// The package name.
    pub(crate) package: String,
    /// Its library's crate name, as Rust code names it.
    pub(crate) lib: String,
    /// Where it comes from.
    pub(crate) source: CrateSource,
}

/// A listed package source: a directory, an exact crates.io version, or a
/// Git revision. Generated dependencies retain that source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CrateSource {
    /// Its directory, resolved.
    Path(PathBuf),
    /// An exact version on crates.io.
    Version(String),
    /// A package from a Git repository at a revision.
    Git { url: String, rev: String },
}

impl CrateSource {
    /// A `[messages]` or `[nodes]` entry, relative to the spec's directory
    /// `dir`: `name@version` that is not a directory names a version.
    fn parse(dir: &Path, entry: &str) -> Result<(CrateNames, Self), Error> {
        match crate::Crate::parse_in(dir, entry)? {
            crate::Crate::Registry { name, version } => {
                // The library is named for the package; `offers` checks
                // this when it reads the crate.
                let names = CrateNames {
                    lib: name.replace('-', "_"),
                    package: name,
                };
                Ok((names, Self::Version(version)))
            }
            crate::Crate::Path(path) => Ok((crate_names(&path)?, Self::Path(path))),
            // A spec names a Git package in a table, parsed as `Git` above,
            // never in an entry's text.
            crate::Crate::Git { .. } => {
                unreachable!("an entry's text is a directory or name@version")
            }
        }
    }

    /// The source, as the inside of an inline table, with a path written
    /// as `path` writes it.
    pub(crate) fn source(&self, path: &dyn Fn(&Path) -> String) -> String {
        match self {
            Self::Git { url, rev } => format!(
                "git = {}, rev = {}",
                crate::dump::toml_string(url),
                crate::dump::toml_string(rev)
            ),
            Self::Path(p) => format!("path = {}", path(p)),
            Self::Version(v) => format!("version = {}", crate::dump::toml_string(&format!("={v}"))),
        }
    }
}

impl std::fmt::Display for CrateSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Git { url, rev } => write!(f, "{url} at {rev}"),
            Self::Path(p) => write!(f, "{}", p.display()),
            Self::Version(v) => write!(f, "version {v}"),
        }
    }
}

/// The contract crate.
#[derive(Debug, Clone)]
pub(crate) struct Contract {
    /// The package name.
    pub(crate) package: String,
    /// Its library's crate name, as Rust code names it.
    pub(crate) lib: String,
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
    /// A git repository at one revision: gdl is not published, and an
    /// exported tarball must name it from any machine.
    Git {
        /// The repository.
        url: String,
        /// The commit, tag, or branch.
        rev: String,
    },
}

impl GdlDep {
    /// The dependency's source, as the inside of an inline table, with a
    /// path written as `path` writes it.
    pub(crate) fn source(&self, path: &dyn Fn(&Path) -> String) -> String {
        use crate::dump::toml_string;
        match self {
            Self::Path(p) => format!("path = {}", path(p)),
            Self::Version(v) => format!("version = {}", toml_string(v)),
            Self::Git { url, rev } => {
                format!("git = {}, rev = {}", toml_string(url), toml_string(rev))
            }
        }
    }
}

/// One node's implementation.
#[derive(Debug, Clone)]
pub(crate) struct Bind {
    /// The implementation's package name.
    pub(crate) package: String,
    /// Its library's crate name, as Rust code names it: `[lib] name` in its
    /// manifest, or the package name with `-` as `_`.
    pub(crate) lib: String,
    /// Where it comes from: a `[bind.*]` table's directory, or a `[nodes]`
    /// crate's source.
    pub(crate) source: CrateSource,
    /// The implementing type's Rust path.
    pub(crate) ty: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    #[serde(default)]
    deployment: Option<crate::deployment::Settings>,
    #[serde(default)]
    pipeline_names: BTreeMap<String, String>,
    #[serde(default)]
    entry: BTreeMap<String, crate::service::EntrySpec>,
    #[serde(default)]
    config: BTreeMap<String, gdl_model::GraphConfig>,
    contract: Option<RawContract>,
    graph: Option<RawGraph>,
    messages: Option<RawMessages>,
    nodes: Option<RawNodes>,
    gdl: RawGdl,
    #[serde(default)]
    segment: toml::Table,
    #[serde(default)]
    routing: toml::Table,
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
struct RawGraph {
    path: PathBuf,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawCrate {
    Named(String),
    Git {
        package: String,
        git: String,
        rev: String,
        lib: Option<String>,
    },
}

impl RawCrate {
    fn resolve(self, dir: &Path) -> Result<(CrateNames, CrateSource), Error> {
        match self {
            Self::Named(entry) => CrateSource::parse(dir, &entry),
            Self::Git {
                package,
                git,
                rev,
                lib,
            } => {
                if package.is_empty() || git.is_empty() || rev.is_empty() {
                    return Err(Error::new("Git entries require package, git, and rev"));
                }
                Ok((
                    CrateNames {
                        lib: lib.unwrap_or_else(|| package.replace('-', "_")),
                        package,
                    },
                    CrateSource::Git { url: git, rev },
                ))
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMessages {
    crates: Vec<RawCrate>,
    #[serde(default)]
    pin: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNodes {
    crates: Vec<RawCrate>,
    #[serde(default)]
    pin: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGdl {
    path: Option<PathBuf>,
    version: Option<String>,
    git: Option<String>,
    rev: Option<String>,
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

    /// The contract crate a `[contract]` spec names.
    #[cfg(test)]
    pub(crate) fn contract(&self) -> Option<&Contract> {
        match &self.source {
            Source::Contract(contract) => Some(contract),
            Source::Graph(_) => None,
        }
    }

    /// Read and resolve the spec at `file`. Each bound crate's manifest
    /// must name the package the spec gives for it; what is wrong with the
    /// bound crates is reported when the spec is validated, with every other
    /// problem.
    pub fn load(file: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(file)
            .map_err(|e| Error::new(format!("reading {}: {e}", file.display())))?;
        // A bare file name has an empty parent: the current directory.
        let dir = match file.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let mut spec = Self::parse(&text, file, &dir)?;
        let mut problems = Vec::new();
        for (node, bind) in &mut spec.bind {
            let CrateSource::Path(path) = &bind.source else {
                continue;
            };
            match crate_names(path) {
                Err(error) => problems.push(format!("[bind.{node}]: {error}")),
                Ok(names) if names.package != bind.package => problems.push(format!(
                    "[bind.{node}]: {} holds package `{}`, not `{}`",
                    path.display(),
                    names.package,
                    bind.package
                )),
                Ok(names) => bind.lib = names.lib,
            }
        }
        spec.problems = problems;
        Ok(spec)
    }

    /// A graph spec given as text, with its graph file's text: the spec as
    /// if it were `gdl.toml` in `dir`, which paths in it are relative to.
    /// Nothing is read from disk but the listed crates.
    pub fn from_text(spec: &str, graph: &str, dir: &Path) -> Result<Self, Error> {
        let text = spec.to_string();
        let mut spec = Self::parse(spec, &dir.join("gdl.toml"), dir)?;
        spec.text = Some(text);
        match &mut spec.source {
            Source::Graph(source) => source.text = Some(graph.to_string()),
            Source::Contract(_) => {
                return Err(Error::new(
                    "a spec given as text names a [graph], not a [contract]",
                ));
            }
        }
        Ok(spec)
    }

    fn parse(text: &str, file: &Path, dir: &Path) -> Result<Self, Error> {
        let raw: Raw =
            toml::from_str(text).map_err(|e| Error::new(format!("{}: {e}", file.display())))?;
        let nodes = raw.nodes;
        let source = match (raw.contract, raw.graph, raw.messages) {
            (Some(_), None, None) if nodes.is_some() => {
                return Err(Error::new(
                    "[nodes] goes with [graph]; a contract spec binds its nodes with [bind.*]",
                ));
            }
            (Some(contract), None, None) => {
                let path = dir.join(&contract.path);
                let CrateNames { package, lib } = crate_names(&path)?;
                let module = contract.module.unwrap_or_else(|| default_module(&lib));
                Source::Contract(Contract {
                    package,
                    lib,
                    path,
                    module,
                })
            }
            (None, Some(_), Some(_)) if !raw.bind.is_empty() => {
                return Err(Error::new(
                    "[bind.*] goes with [contract]; a graph spec binds its nodes through [nodes]",
                ));
            }
            (None, Some(graph), Some(messages)) => {
                Source::Graph(graph_source(dir, graph, messages, nodes)?)
            }
            (Some(_), Some(_), _) => {
                return Err(Error::new("a spec has [contract] or [graph], not both"));
            }
            (Some(_), None, Some(_)) => {
                return Err(Error::new(
                    "[messages] goes with [graph]; a contract crate names its own types",
                ));
            }
            (None, Some(_), None) => {
                return Err(Error::new(
                    "[graph] needs [messages]: the crates that define its messages",
                ));
            }
            (None, None, _) => {
                return Err(Error::new("a spec needs [contract] or [graph]"));
            }
        };
        let gdl = match (raw.gdl.path, raw.gdl.version, raw.gdl.git, raw.gdl.rev) {
            (Some(path), None, None, None) => GdlDep::Path(dir.join(path)),
            (None, Some(version), None, None) => GdlDep::Version(version),
            (None, None, Some(url), Some(rev)) => GdlDep::Git { url, rev },
            _ => {
                return Err(Error::new(
                    "[gdl] needs exactly one of `path`, `version`, and `git` with `rev`",
                ));
            }
        };
        let mut placement_table = toml::Table::new();
        placement_table.insert("segment".into(), toml::Value::Table(raw.segment));
        placement_table.insert("routing".into(), toml::Value::Table(raw.routing));
        let placement = gdl_model::Placement::from_toml(&placement_table.to_string())
            .map_err(|e| Error::new(format!("{}: [segment]: {e}", file.display())))?;
        let bind = raw
            .bind
            .into_iter()
            .map(|(node, b)| {
                let bind = Bind {
                    // Read from the crate's manifest when the spec is loaded.
                    lib: b.package.replace('-', "_"),
                    package: b.package,
                    source: CrateSource::Path(dir.join(b.path)),
                    ty: b.ty,
                };
                (node, bind)
            })
            .collect();
        Ok(Self {
            deployment: raw.deployment,
            pipeline_names: raw.pipeline_names,
            entry: raw.entry,
            service: None,
            config: raw.config,
            file: file.to_path_buf(),
            text: None,
            dir: dir.to_path_buf(),
            source,
            gdl,
            placement,
            bind,
            problems: Vec::new(),
        })
    }
}

fn graph_source(
    dir: &Path,
    graph: RawGraph,
    messages: RawMessages,
    nodes: Option<RawNodes>,
) -> Result<GraphSource, Error> {
    if messages.crates.is_empty() {
        return Err(Error::new("[messages] crates lists no crate"));
    }
    let mut crates: Vec<ListedCrate> = Vec::with_capacity(messages.crates.len());
    for entry in messages.crates {
        let (CrateNames { package, lib }, source) = entry
            .resolve(dir)
            .map_err(|e| Error::new(format!("[messages] crates: {e}")))?;
        // Catalogue entries name a package, not a path: two listed packages
        // of one name could not be told apart by a pin.
        if let Some(first) = crates.iter().find(|c| c.package == package) {
            return Err(Error::new(format!(
                "[messages] crates: {} and {source} are both package `{package}`",
                first.source
            )));
        }
        // The generated contract names each message by its library's path.
        if let Some(first) = crates.iter().find(|c| c.lib == lib) {
            return Err(Error::new(format!(
                "[messages] crates: {} and {source} are both library `{lib}`",
                first.source
            )));
        }
        crates.push(ListedCrate {
            package,
            lib,
            source,
        });
    }
    let RawNodes {
        crates: node_dirs,
        pin: node_pins,
    } = nodes.unwrap_or(RawNodes {
        crates: Vec::new(),
        pin: BTreeMap::new(),
    });
    let mut nodes: Vec<ListedCrate> = Vec::with_capacity(node_dirs.len());
    for entry in node_dirs {
        let (CrateNames { package, lib }, source) = entry
            .resolve(dir)
            .map_err(|e| Error::new(format!("[nodes] crates: {e}")))?;
        if let Some(first) = nodes.iter().find(|c| c.package == package) {
            return Err(Error::new(format!(
                "[nodes] crates: {} and {source} are both package `{package}`",
                first.source
            )));
        }
        nodes.push(ListedCrate {
            package,
            lib,
            source,
        });
    }
    Ok(GraphSource {
        path: dir.join(graph.path),
        text: None,
        crates,
        pins: messages.pin,
        nodes,
        node_pins,
    })
}

/// What a crate is called: by Cargo, and by Rust code.
#[derive(Debug)]
struct CrateNames {
    package: String,
    lib: String,
}

/// A crate's names, from its `Cargo.toml`.
fn crate_names(dir: &Path) -> Result<CrateNames, Error> {
    let manifest = dir.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)
        .map_err(|e| Error::new(format!("reading {}: {e}", manifest.display())))?;
    let table: toml::Table =
        toml::from_str(&text).map_err(|e| Error::new(format!("{}: {e}", manifest.display())))?;
    let package = table
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(toml::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Error::new(format!("{} has no [package] name", manifest.display())))?;
    let lib = table
        .get("lib")
        .and_then(|l| l.get("name"))
        .and_then(toml::Value::as_str)
        .map_or_else(|| package.replace('-', "_"), str::to_string);
    Ok(CrateNames { package, lib })
}

/// The Rust path of a library's root: its name, or `r#name` when the name
/// is a keyword (cargo accepts a library named `async` or `gen`, and Rust
/// reaches it only as `r#async` or `r#gen`).
pub(crate) fn default_module(lib: &str) -> String {
    gdl_syntax::identifier(lib)
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
        let contract = spec.contract().expect("contract");
        assert_eq!(contract.package, "chat-contract");
        assert_eq!(contract.module, "chat_contract");
        assert_eq!(contract.path, dir.join("contract"));
        assert_eq!(spec.gdl, GdlDep::Path(dir.join("../gdl")));
        assert_eq!(spec.bind["B"].source, CrateSource::Path(dir.join("front")));
        assert_eq!(
            spec.placement,
            gdl_model::Placement::new().segment("front", ["A", "B"])
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// gdl is not published: an exported tarball names it by a git
    /// revision, which builds anywhere. A git source needs its revision,
    /// and `[gdl]` takes one source.
    #[test]
    fn gdl_comes_from_a_path_a_version_or_a_git_revision() {
        let dir = contract_dir("git");
        let parse = |gdl: &str| {
            let text = format!(
                "[contract]\npath = \"contract\"\n[gdl]\n{gdl}\n[segment.front]\nnodes = [\"A\"]\n"
            );
            Spec::parse(&text, &dir.join("gdl.toml"), &dir).map(|spec| spec.gdl)
        };
        let git = parse("git = \"https://example.com/gdl\"\nrev = \"abc123\"").expect("git");
        assert_eq!(
            git.source(&|_| unreachable!("no path")),
            r#"git = "https://example.com/gdl", rev = "abc123""#
        );
        for wrong in [
            "git = \"https://example.com/gdl\"",
            "rev = \"abc123\"",
            "version = \"0.1\"\ngit = \"https://example.com/gdl\"\nrev = \"abc123\"",
        ] {
            assert_eq!(
                parse(wrong).expect_err(wrong).to_string(),
                "[gdl] needs exactly one of `path`, `version`, and `git` with `rev`"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A listed crate is a directory, or `name@version` that is not one.
    #[test]
    fn a_listed_crate_is_a_directory_or_a_version() {
        let dir = contract_dir("listed");
        let (names, source) = CrateSource::parse(&dir, "contract").expect("a directory");
        assert_eq!(names.package, "chat-contract");
        assert_eq!(source, CrateSource::Path(dir.join("contract")));
        let (names, source) = CrateSource::parse(&dir, "chat-protocols@1.2.3").expect("a version");
        assert_eq!(
            (names.package.as_str(), names.lib.as_str()),
            ("chat-protocols", "chat_protocols")
        );
        assert_eq!(source, CrateSource::Version("1.2.3".into()));
        let error = CrateSource::parse(&dir, "@1.2.3")
            .expect_err("no name")
            .to_string();
        assert_eq!(error, "`@1.2.3` is neither a directory nor `name@version`");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A graph spec names its graph file and the crates that define its
    /// messages, each read for its package and library names.
    #[test]
    fn a_graph_spec_reads_its_message_crates() {
        let dir = contract_dir("graph");
        std::fs::create_dir_all(dir.join("protocols")).expect("dir");
        std::fs::write(
            dir.join("protocols/Cargo.toml"),
            "[package]\nname = \"chat-protocols\"\n",
        )
        .expect("write");
        let text = r#"
            [graph]
            path = "chat.gdl"
            [messages]
            crates = ["protocols"]
            pin = { Chunk = "chat_protocols::Chunk" }
            [gdl]
            version = "0.1"
        "#;
        let spec = Spec::parse(text, &dir.join("gdl.toml"), &dir).expect("spec");
        let Source::Graph(graph) = &spec.source else {
            panic!("not a graph spec");
        };
        assert_eq!(graph.path, dir.join("chat.gdl"));
        assert_eq!(graph.crates.len(), 1);
        assert_eq!(graph.crates[0].package, "chat-protocols");
        assert_eq!(
            graph.crates[0].source,
            CrateSource::Path(dir.join("protocols"))
        );
        assert_eq!(graph.pins["Chunk"], "chat_protocols::Chunk");
        assert!(spec.contract().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A graph spec lists its node crates, each read for its names; pins
    /// choose an offered node by its type's path.
    #[test]
    fn a_graph_spec_reads_its_node_crates() {
        let dir = contract_dir("nodes");
        for name in ["protocols", "engine"] {
            std::fs::create_dir_all(dir.join(name)).expect("dir");
            std::fs::write(
                dir.join(name).join("Cargo.toml"),
                format!("[package]\nname = \"chat-{name}\"\n"),
            )
            .expect("write");
        }
        let head = "[graph]\npath = \"chat.gdl\"\n[messages]\ncrates = [\"protocols\"]\n[gdl]\nversion = \"0.1\"\n";
        let text = format!(
            "{head}[nodes]\ncrates = [\"engine\"]\npin = {{ Engine = \"chat_engine::Engine\" }}\n"
        );
        let spec = Spec::parse(&text, &dir.join("gdl.toml"), &dir).expect("spec");
        let Source::Graph(graph) = &spec.source else {
            panic!("not a graph spec");
        };
        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(graph.nodes[0].package, "chat-engine");
        assert_eq!(graph.nodes[0].lib, "chat_engine");
        assert_eq!(graph.node_pins["Engine"], "chat_engine::Engine");

        let error = Spec::parse(
            &format!("{head}[bind.Engine]\ncrate = \"chat-engine\"\npath = \"engine\"\ntype = \"chat_engine::Engine\"\n"),
            &dir.join("gdl.toml"),
            &dir,
        )
        .expect_err("a binding table")
        .to_string();
        assert!(
            error.contains("a graph spec binds its nodes through [nodes]"),
            "{error}"
        );
        let error = Spec::parse(
            "[contract]\npath = \"contract\"\n[gdl]\nversion = \"0.1\"\n[nodes]\ncrates = [\"engine\"]\n",
            &dir.join("gdl.toml"),
            &dir,
        )
        .expect_err("nodes with a contract")
        .to_string();
        assert!(error.contains("[nodes] goes with [graph]"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_spec_has_a_contract_or_a_graph_with_its_messages() {
        let dir = contract_dir("sources");
        std::fs::create_dir_all(dir.join("twin")).expect("dir");
        std::fs::write(
            dir.join("twin/Cargo.toml"),
            "[package]\nname = \"twin\"\n[lib]\nname = \"chat_contract\"\n",
        )
        .expect("write");
        let gdl = "[gdl]\nversion = \"0.1\"\n";
        let contract = "[contract]\npath = \"contract\"\n";
        let graph = "[graph]\npath = \"g.gdl\"\n";
        let messages = "[messages]\ncrates = [\"contract\"]\n";
        for (text, expected) in [
            (format!("{contract}{graph}{messages}{gdl}"), "not both"),
            (
                format!("{contract}{messages}{gdl}"),
                "[messages] goes with [graph]",
            ),
            (format!("{graph}{gdl}"), "[graph] needs [messages]"),
            (gdl.to_string(), "needs [contract] or [graph]"),
            (
                format!("{graph}[messages]\ncrates = []\n{gdl}"),
                "lists no crate",
            ),
            (
                format!("{graph}[messages]\ncrates = [\"contract\", \"./contract\"]\n{gdl}"),
                "are both package `chat-contract`",
            ),
            // The generated contract names each message crate by its
            // library: two of one library name cannot both be named.
            (
                format!("{graph}[messages]\ncrates = [\"contract\", \"twin\"]\n{gdl}"),
                "are both library `chat_contract`",
            ),
        ] {
            let error = Spec::parse(&text, &dir.join("gdl.toml"), &dir)
                .err()
                .unwrap_or_else(|| panic!("parsed:\n{text}"))
                .to_string();
            assert!(error.contains(expected), "{error}");
        }
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

    /// A contract library named like a keyword is reached as `r#name`.
    #[test]
    fn the_default_module_escapes_a_keyword_library() {
        assert_eq!(default_module("chat_contract"), "chat_contract");
        assert_eq!(default_module("async"), "r#async");
        // A keyword only from edition 2024 on; syn takes it for a name.
        assert_eq!(default_module("gen"), "r#gen");
    }
}

#[cfg(test)]
mod git_tests {
    use super::*;

    #[test]
    fn git_packages_keep_their_commit_in_generated_dependencies() {
        let text = r#"
[graph]
path = "graph.gdl"
[messages]
crates = [{ package = "protocols", git = "https://example.com/repo", rev = "0123456789abcdef0123456789abcdef01234567ab" }]
[gdl]
git = "https://example.com/gdl"
rev = "main"
[segment.main]
nodes = ["Source", "Engine"]
"#;
        let spec = Spec::parse(text, Path::new("gdl.toml"), Path::new(".")).expect("git package");
        let Source::Graph(graph) = spec.source else {
            panic!("graph")
        };
        assert_eq!(graph.crates[0].package, "protocols");
        assert_eq!(
            graph.crates[0].source.source(&|_| panic!("no local path")),
            "git = \"https://example.com/repo\", rev = \"0123456789abcdef0123456789abcdef01234567ab\""
        );
    }
}
