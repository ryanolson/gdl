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
//! path = "../../crates/gdl-runtime"   # or: version = "0.1"
//!
//! [segment.front]
//! nodes = ["A", "B", "C", "G"]
//!
//! [class.global]                 # optional: segments that deploy together;
//! segments = ["front"]           # with classes, each segment is in one
//!
//! # Or, instead of [segment.*], [routing], [class.*], and [cells.*], one
//! # [layout.<name>] table of them for each way the graph is placed:
//! # [layout.compact.segment.front], [layout.compact.class.global], ...
//!
//! [bind.B]
//! crate = "front-nodes"          # the implementation's package
//! path = "front-nodes"           # relative to this file
//! type = "front_nodes::Preprocess"
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

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
    /// Each layout's placement: its segments, request-boundary routing
    /// policies, classes, and cell policies.
    pub(crate) placements: gdl_model::Placements,
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
    /// Refuse a path whose package is not `gdl-runtime`, which every
    /// generated manifest names. A version or a revision is not fetched
    /// here.
    pub(crate) fn check(&self) -> Result<(), Error> {
        match self {
            Self::Path(path) => {
                runtime_at(path).map_err(|e| Error::new(format!("[gdl] path: {e}")))
            }
            Self::Version(_) | Self::Git { .. } => Ok(()),
        }
    }

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

/// The spec file as written: read with every unknown key refused, and
/// written back as it was read, with what is not set left unsaid.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deployment: Option<crate::deployment::Settings>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pipeline_names: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    entry: BTreeMap<String, crate::service::EntrySpec>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    config: BTreeMap<String, gdl_model::GraphConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contract: Option<RawContract>,
    #[serde(skip_serializing_if = "Option::is_none")]
    graph: Option<RawGraph>,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages: Option<RawMessages>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nodes: Option<RawNodes>,
    gdl: RawGdl,
    // The placement's tables, or the layouts', kept as written. The
    // placements themselves are read by `Placements::from_toml` from the
    // same text, so that their problems have their lines in the spec.
    #[serde(default, skip_serializing_if = "toml::Table::is_empty")]
    layout: toml::Table,
    #[serde(default, skip_serializing_if = "toml::Table::is_empty")]
    segment: toml::Table,
    #[serde(default, skip_serializing_if = "toml::Table::is_empty")]
    routing: toml::Table,
    #[serde(default, skip_serializing_if = "toml::Table::is_empty")]
    class: toml::Table,
    #[serde(default, skip_serializing_if = "toml::Table::is_empty")]
    cells: toml::Table,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    bind: BTreeMap<String, RawBind>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawContract {
    path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    module: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGraph {
    path: PathBuf,
}

/// A `[messages]` or `[nodes]` entry: a directory or `name@version`, or a
/// Git package's table.
#[derive(Debug, PartialEq, Serialize)]
#[serde(untagged)]
enum RawCrate {
    Named(String),
    Git(RawGit),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGit {
    package: String,
    git: String,
    rev: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    lib: Option<String>,
}

// Read by hand rather than as an untagged enum, which would refuse a
// table with an unknown key only as matching no variant, without the key.
impl<'de> Deserialize<'de> for RawCrate {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Entry;
        impl<'de> serde::de::Visitor<'de> for Entry {
            type Value = RawCrate;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a directory, `name@version`, or { package, git, rev }")
            }

            fn visit_str<E: serde::de::Error>(self, entry: &str) -> Result<RawCrate, E> {
                Ok(RawCrate::Named(entry.to_string()))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<RawCrate, A::Error> {
                RawGit::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(RawCrate::Git)
            }
        }
        deserializer.deserialize_any(Entry)
    }
}

impl RawCrate {
    fn resolve(self, dir: &Path) -> Result<(CrateNames, CrateSource), Error> {
        match self {
            Self::Named(entry) => CrateSource::parse(dir, &entry),
            Self::Git(RawGit {
                package,
                git,
                rev,
                lib,
            }) => {
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMessages {
    crates: Vec<RawCrate>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pin: BTreeMap<String, String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNodes {
    crates: Vec<RawCrate>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pin: BTreeMap<String, String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGdl {
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    git: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rev: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

    /// The names of the spec's layouts, in name order: `default` alone for
    /// a spec without `[layout]` tables.
    pub(crate) fn layout_names(&self) -> impl Iterator<Item = &str> {
        self.placements.names()
    }

    /// The placement of a spec of one layout.
    #[cfg(test)]
    pub(crate) fn placement(&self) -> &gdl_model::Placement {
        self.only_layout("a test").expect("one layout").placement
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
        let text = std::fs::read_to_string(file).map_err(|e| {
            Error::from(
                gdl_model::Problems::from(gdl_model::Problem::from(format!("cannot be read: {e}")))
                    .in_file(file.display()),
            )
        })?;
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
        let located = |problems: gdl_model::Problems| Error::from(problems.in_file(file.display()));
        let raw: Raw = gdl_model::read_toml(text).map_err(located)?;
        let nodes = raw.nodes;
        let source = match (raw.contract, raw.graph, raw.messages) {
            (Some(_), None, None) if nodes.is_some() => {
                return Err(Error::new(
                    "[nodes] goes with [graph]; a contract spec binds its nodes with [bind.*]",
                ));
            }
            (Some(contract), None, None) => {
                let path = dir.join(&contract.path);
                let CrateNames { package, lib } =
                    crate_names(&path).map_err(|e| Error::new(format!("[contract] path: {e}")))?;
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
        let placements = gdl_model::Placements::from_toml(text).map_err(located)?;
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
            placements,
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

/// Refuse `dir` unless it holds the package `gdl-runtime`.
pub fn runtime_at(dir: &Path) -> Result<(), Error> {
    let CrateNames { package, .. } = crate_names(dir)?;
    if package != "gdl-runtime" {
        return Err(Error::new(format!(
            "{} is the package `{package}`; it must name `gdl-runtime`",
            dir.display()
        )));
    }
    Ok(())
}

/// The Rust path of a library's root: its name, or `r#name` when the name
/// is a keyword (cargo accepts a library named `async` or `gen`, and Rust
/// reaches it only as `r#async` or `r#gen`).
pub(crate) fn default_module(lib: &str) -> String {
    gdl_syntax::identifier(lib)
}

#[cfg(test)]
mod tests;
