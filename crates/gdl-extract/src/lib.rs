//! Reads what a crate offers to gdl graphs from its source, without
//! building it.
//!
//! A crate that needs a toolkit (CUDA, for one) does not build on a machine
//! without it, and building runs a crate's code. So the reader builds
//! nothing. `cargo metadata` downloads the crate and its dependencies and
//! resolves them, which runs no build script and no proc macro; the reader
//! then reads source with `syn`, from each library's root through its
//! module tree, and finds three markers:
//!
//! - `#[derive(Message)]` on a type: a message, at its definition.
//! - `#[gdl_message::export]` on a `pub use`: another path to a message.
//! - `#[derive(Node)]` on a type: a node, with the port types of the one
//!   role trait impl the crate has for it.
//!
//! It resolves only the paths those markers need, through `use`
//! declarations, globs, re-exports, `crate`, `self`, `super`, and
//! dependencies by the names Cargo gives them, to a marked definition. A
//! path it cannot follow is a problem that says so, never a guess. It does
//! not see items that macros make, code that build scripts generate, or
//! `cfg` that depends on compiler flags.

mod cfg;
mod markers;
mod metadata;
mod resolve;
mod tree;

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// The packages of a Cargo workspace and of all their dependencies, as
/// `cargo metadata` gives them, ready to read.
pub struct Workspace {
    graph: metadata::Graph,
    settings: cfg::Settings,
}

impl Workspace {
    /// Resolve the workspace or package of `manifest` with `cargo metadata`,
    /// which downloads what it lacks and builds nothing.
    pub fn load(manifest: &Path) -> Result<Self, Error> {
        Ok(Self {
            graph: metadata::Graph::load(manifest)?,
            settings: cfg::Settings::host()?,
        })
    }

    /// Resolve crate `name` at exactly `version`, from its registry, with a
    /// manifest of our own in `scratch` that depends on it alone. Nothing is
    /// built.
    pub fn fetch(name: &str, version: &str, scratch: &Path) -> Result<Self, Error> {
        let src = scratch.join("src");
        std::fs::create_dir_all(&src).map_err(|e| Error(format!("{}: {e}", src.display())))?;
        let manifest = format!(
            "[package]\nname = \"gdl-extract-fetch\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\
             publish = false\n\n[workspace]\n\n[dependencies]\n{name} = \"={version}\"\n"
        );
        let write = |path: &Path, text: &str| {
            std::fs::write(path, text).map_err(|e| Error(format!("{}: {e}", path.display())))
        };
        write(&scratch.join("Cargo.toml"), &manifest)?;
        write(&src.join("lib.rs"), "")?;
        Self::load(&scratch.join("Cargo.toml"))
    }

    /// What package `spec` offers, and every problem met reading it. `spec`
    /// is `name` or `name@version`: a workspace member of that name, else
    /// the one resolved package that fits.
    pub fn extract(&self, spec: &str) -> Result<Extraction, Error> {
        let package = self.graph.package(spec).map_err(Error)?;
        Ok(markers::extract(&self.graph, &self.settings, package))
    }

    /// Where package `spec` (a name, or `name@version`) is: its version,
    /// its manifest's directory, and whether that is a path package, which
    /// a dependency names by that directory rather than by version.
    pub fn locate(&self, spec: &str) -> Result<Located, Error> {
        let package = &self.graph.packages[self.graph.package(spec).map_err(Error)?];
        Ok(Located {
            name: package.name.clone(),
            version: package.version.clone(),
            dir: package.dir.clone(),
            path: package.source.starts_with("path+"),
        })
    }

    /// The name of the package whose manifest is in `dir`, if this
    /// workspace resolved it.
    pub fn package_in(&self, dir: &Path) -> Option<String> {
        self.graph
            .package_in(dir)
            .map(|i| self.graph.packages[i].name.clone())
    }
}

/// Where a resolved package is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Located {
    /// Its name.
    pub name: String,
    /// Its version.
    pub version: String,
    /// Its manifest's directory.
    pub dir: std::path::PathBuf,
    /// Whether it is a path package.
    pub path: bool,
}

/// What a crate offers, and what the reader could not read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extraction {
    /// What it offers. Entries with a problem are left out.
    pub offer: Offer,
    /// One line per problem, with its file, line, and column.
    pub problems: Vec<String>,
}

/// What one crate version offers to gdl graphs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offer {
    /// The package name.
    pub package: String,
    /// Its version.
    pub version: String,
    /// Where it comes from, as Cargo names the source: a registry, a git
    /// repository, or `path+file://` and its directory.
    pub source: String,
    /// The messages it defines, by name.
    #[serde(default)]
    pub messages: Vec<MessageEntry>,
    /// The messages it offers under its own paths, by name.
    #[serde(default)]
    pub exports: Vec<ExportEntry>,
    /// The nodes it implements, by name.
    #[serde(default)]
    pub nodes: Vec<NodeEntry>,
}

impl Offer {
    /// The offer as a working-catalogue entry: TOML.
    pub fn to_toml(&self) -> String {
        // Strings, booleans, and lists of tables, all of which TOML holds.
        toml::to_string(self).expect("an offer is representable in TOML")
    }

    /// Read an entry written by [`Offer::to_toml`].
    pub fn from_toml(text: &str) -> Result<Self, Error> {
        toml::from_str(text).map_err(|e| Error(e.to_string()))
    }
}

/// A message a crate defines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEntry {
    /// The type's name: what a graph calls it.
    pub name: String,
    /// Its definition path.
    pub path: String,
    /// Whether it implements `Serialize` and `Deserialize`, by derive or by
    /// an impl the reader found.
    pub wire: bool,
}

/// A message a crate offers under a public path of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportEntry {
    /// The name it is offered under.
    pub name: String,
    /// The public path in this crate.
    pub path: String,
    /// The message it is.
    pub origin: Origin,
    /// Whether the message is a wire type, as its definition shows.
    pub wire: bool,
}

/// A message's identity: where it is defined, wherever it is offered.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Origin {
    /// The defining package.
    pub package: String,
    /// Its version.
    pub version: String,
    /// Its source, as [`Offer::source`].
    pub source: String,
    /// The definition path.
    pub path: String,
}

/// A node a crate implements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEntry {
    /// The node's name: its type's name.
    pub name: String,
    /// `sink`, `forward`, `backward`, or `operator`.
    pub role: String,
    /// The implementing type, at the crate root.
    #[serde(rename = "type")]
    pub ty: String,
    /// Its ports, in the order the role trait declares them.
    pub ports: Vec<PortEntry>,
}

/// One port of a node: a role trait's associated type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortEntry {
    /// The associated type: `Req`, `Item`, `In`, `Out`, `UpReq`, and so on.
    pub port: String,
    /// The message, or a streamed request's head; `None` for `()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<Origin>,
    /// A streamed request's item; `None` for any other port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<Origin>,
}

/// A failure to read a workspace at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}
