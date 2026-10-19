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

    /// What package `name` offers, and every problem met reading it.
    pub fn extract(&self, name: &str) -> Result<Extraction, Error> {
        let package = self
            .graph
            .package(name)
            .ok_or_else(|| Error(format!("the workspace has no package `{name}`")))?;
        Ok(markers::extract(&self.graph, &self.settings, package))
    }
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
