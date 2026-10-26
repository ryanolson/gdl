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
//! A generic type that derives `Message` or `Node` offers nothing itself.
//! `#[export]` on a `pub type` that names an instantiation of one offers
//! that instantiation under the alias's name: a message as an export, and
//! a node with the port types of its role impl, the impl's parameters
//! bound by the alias's arguments.
//!
//! It resolves only the paths those markers need, through `use`
//! declarations, globs, re-exports, aliases, `crate`, `self`, `super`, and
//! dependencies by the names Cargo gives them, to a marked definition. An
//! instantiation is identified by its type, each argument by its
//! definition, not by how it is spelled. A path it cannot follow is a
//! problem that says so, never a guess. It does not see items that macros
//! make, code that build scripts generate, or `cfg` that depends on
//! compiler flags. Of the standard library's types, it names the
//! primitives, `()`, `String`, `Vec`, `VecDeque`, `Option`, `Box`,
//! `HashMap`, and `BTreeMap`, and no other.

mod attrs;
mod canon;
mod catalog;
mod cfg;
mod identity;
mod impls;
mod markers;
mod metadata;
mod resolve;
mod tree;
mod wire;

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

pub use catalog::{Catalog, Entry, rfc3339};

/// The packages of a Cargo workspace and of all their dependencies, as
/// `cargo metadata` gives them, ready to read.
pub struct Workspace {
    graph: metadata::Graph,
    settings: cfg::Settings,
}

/// How `cargo metadata` is run: as the user's own tool, or confined for
/// crates the process did not write.
#[derive(Debug, Clone, Copy)]
pub struct Cargo {
    deadline: Option<std::time::Duration>,
    /// Whether a confined cargo gets a process group of its own, which the
    /// deadline ends with its git; a process that is itself a confined
    /// child keeps cargo in its own group instead, so that whoever ends
    /// the child ends cargo too.
    own_group: bool,
}

impl Cargo {
    /// The user's own cargo, as the command line runs it, with every
    /// configuration it finds.
    pub fn trusted() -> Self {
        Self {
            deadline: None,
            own_group: false,
        }
    }

    /// Cargo for crates the process did not write. Git dependencies are
    /// fetched through the `git` command over HTTP(S) only, without the
    /// user's git configuration or credential helpers, and never prompt;
    /// the compiler and wrappers are fixed, whatever a crate's own
    /// `.cargo/config` says; and a run longer than `deadline` is killed,
    /// with the git it spawned, so a dependency that never answers does
    /// not hold the process or cargo's package lock. Cargo still runs from
    /// the manifest's directory: a fetch's scratch under the target
    /// directory, where the operator's own configuration (a mirror,
    /// `net.offline`) applies, or a local directory the operator chose to
    /// expose. A Git checkout is never that directory, so its
    /// configuration and toolchain file do not apply.
    pub fn confined(deadline: std::time::Duration) -> Self {
        Self {
            deadline: Some(deadline),
            own_group: true,
        }
    }

    /// [`Cargo::confined`], for a process that is itself a confined child
    /// in a process group of its own: cargo stays in that group, so that
    /// the parent ending the child ends cargo and its git at once, rather
    /// than leaving them reparented with cargo's lock. At cargo's deadline
    /// the child must end its own group after reporting, since cargo's git
    /// has no group of its own to end.
    pub fn confined_as_child(deadline: std::time::Duration) -> Self {
        Self {
            deadline: Some(deadline),
            own_group: false,
        }
    }

    pub(crate) fn deadline(&self) -> Option<std::time::Duration> {
        self.deadline
    }

    pub(crate) fn own_group(&self) -> bool {
        self.own_group
    }
}

/// The most of a subprocess's error output that is kept, by cargo's run
/// here and by a server's git: enough for any message worth relaying. A
/// Git host that streams messages without end (git relays them, asked
/// for progress or not) fills a pipe, not the process's memory.
pub const OUTPUT_LIMIT: u64 = 2 << 20;

/// The environment under which a git that touches a repository the
/// process did not write runs: HTTP(S) only, no prompt, none of the user's
/// configuration or credential helpers, and a transfer that stalls for a
/// minute given up. For cargo's own git, and for a server's.
pub fn confined_git_env() -> [(&'static str, &'static str); 16] {
    [
        ("GIT_TERMINAL_PROMPT", "0"),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
        ("GIT_CONFIG_COUNT", "6"),
        ("GIT_CONFIG_KEY_0", "protocol.allow"),
        ("GIT_CONFIG_VALUE_0", "never"),
        ("GIT_CONFIG_KEY_1", "protocol.https.allow"),
        ("GIT_CONFIG_VALUE_1", "always"),
        ("GIT_CONFIG_KEY_2", "protocol.http.allow"),
        ("GIT_CONFIG_VALUE_2", "always"),
        ("GIT_CONFIG_KEY_3", "credential.helper"),
        ("GIT_CONFIG_VALUE_3", ""),
        ("GIT_CONFIG_KEY_4", "http.lowSpeedLimit"),
        ("GIT_CONFIG_VALUE_4", "1"),
        ("GIT_CONFIG_KEY_5", "http.lowSpeedTime"),
        ("GIT_CONFIG_VALUE_5", "60"),
    ]
}

/// The root of the Git checkout cargo keeps `dir` in, if any: the nearest
/// directory above it that cargo marked complete with `.cargo-ok`, under
/// its `git/checkouts`. A `.cargo-ok` a repository commits elsewhere is
/// not under that directory, so it does not pass for the root.
pub fn checkout_root(dir: &Path) -> Option<&Path> {
    dir.ancestors().find(|candidate| {
        candidate.join(".cargo-ok").is_file()
            && candidate
                .ancestors()
                .nth(2)
                .is_some_and(|checkouts| checkouts.ends_with("git/checkouts"))
    })
}

/// End the process group led by `pid`: a command run in its own group
/// takes the children it spawned with it, where killing it alone would
/// leave them running, reparented, on a transfer no one wants.
pub fn kill_process_group(pid: u32) {
    #[cfg(unix)]
    {
        let _ = std::process::Command::new("kill")
            .args(["-9", "--", &format!("-{pid}")])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
    #[cfg(not(unix))]
    let _ = pid;
}

impl Workspace {
    /// Resolve the workspace or package of `manifest` with `cargo metadata`,
    /// which downloads what it lacks and builds nothing.
    pub fn load(manifest: &Path, cargo: &Cargo) -> Result<Self, Error> {
        Ok(Self {
            graph: metadata::Graph::load(manifest, cargo)?,
            settings: cfg::Settings::host()?,
        })
    }

    /// Resolve crate `name` at exactly `version`, from its registry, with a
    /// manifest of our own in `scratch` that depends on it alone. Nothing is
    /// built.
    pub fn fetch(name: &str, version: &str, scratch: &Path, cargo: &Cargo) -> Result<Self, Error> {
        // Both are written into a manifest, and callers name the scratch
        // directory with them: refuse anything a package name or a version
        // cannot hold before a file is written.
        let name_ok = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        let version_ok = !version.is_empty()
            && version
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+'));
        if !name_ok || !version_ok {
            return Err(Error(format!(
                "`{name}@{version}` is not a package name and a version"
            )));
        }
        let manifest = format!(
            "[package]\nname = \"gdl-extract-fetch\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\
             publish = false\n\n[workspace]\n\n[dependencies]\n{name} = \"={version}\"\n"
        );
        Self::fetch_manifest(&manifest, scratch, cargo)
    }

    /// The scratch directory a Git fetch of `name` at `rev` from `url`
    /// resolves in, under `target`.
    pub fn git_scratch(name: &str, url: &str, rev: &str, target: &Path) -> std::path::PathBuf {
        use std::hash::{Hash, Hasher};
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        (name, url, rev).hash(&mut hash);
        target
            .join("gdl-gen")
            .join(format!("git-{:016x}", hash.finish()))
    }

    /// Resolve a Git package at `rev` with Cargo, preserving Git origins for
    /// the package and its dependencies. Nothing is built.
    pub fn fetch_git(
        name: &str,
        url: &str,
        rev: &str,
        target: &Path,
        cargo: &Cargo,
    ) -> Result<Self, Error> {
        let scratch = Self::git_scratch(name, url, rev, target);
        let quote = |s: &str| toml::Value::String(s.into()).to_string();
        let manifest = format!(
            "[package]\nname = \"gdl-extract-fetch\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n\n[dependencies]\ngdl_input = {{ package = {}, git = {}, rev = {} }}\n",
            quote(name),
            quote(url),
            quote(rev)
        );
        Self::fetch_manifest(&manifest, &scratch, cargo)
    }

    fn fetch_manifest(manifest: &str, scratch: &Path, cargo: &Cargo) -> Result<Self, Error> {
        let src = scratch.join("src");
        std::fs::create_dir_all(&src).map_err(|e| Error(format!("{}: {e}", src.display())))?;
        // Adds of one crate at once share the scratch directory: each file is
        // written beside and renamed into place, so `cargo metadata` never
        // reads one half written, and left alone when it already holds the text.
        let write = |path: &Path, text: &str| {
            if std::fs::read_to_string(path).is_ok_and(|held| held == text) {
                return Ok(());
            }
            static WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let partial = path.with_extension(format!("partial-{}-{n}", std::process::id()));
            std::fs::write(&partial, text)
                .and_then(|()| std::fs::rename(&partial, path))
                .map_err(|e| Error(format!("{}: {e}", path.display())))
        };
        write(&scratch.join("Cargo.toml"), manifest)?;
        write(&src.join("lib.rs"), "")?;
        Self::load(&scratch.join("Cargo.toml"), cargo)
    }

    /// What package `spec` offers, and every problem met reading it. `spec`
    /// is `name` or `name@version`: a workspace member of that name, else
    /// the one resolved package that fits.
    pub fn extract(&self, spec: &str) -> Result<Extraction, Error> {
        let package = self.graph.package(spec).map_err(Error)?;
        Ok(markers::extract(&self.graph, &self.settings, package))
    }

    /// What the package whose manifest is in `dir` offers, if this
    /// workspace resolved it. A directory is one package, where a name and
    /// version may be two, from two sources.
    pub fn extract_in(&self, dir: &Path) -> Option<Extraction> {
        let package = self.graph.package_in(dir)?;
        Some(markers::extract(&self.graph, &self.settings, package))
    }

    /// Where package `spec` (a name, or `name@version`) is: its version,
    /// its manifest's directory, and whether that is a path package, which
    /// a dependency names by that directory rather than by version.
    pub fn locate(&self, spec: &str) -> Result<Located, Error> {
        Ok(self.located(self.graph.package(spec).map_err(Error)?))
    }

    /// The package whose manifest is in `dir`, if this workspace resolved
    /// it, as [`Workspace::locate`] gives it.
    pub fn locate_in(&self, dir: &Path) -> Option<Located> {
        Some(self.located(self.graph.package_in(dir)?))
    }

    /// Where every resolved package is, the workspace's own included.
    pub fn located_all(&self) -> impl Iterator<Item = Located> + '_ {
        (0..self.graph.packages.len()).map(|package| self.located(package))
    }

    fn located(&self, package: usize) -> Located {
        let package = &self.graph.packages[package];
        Located {
            name: package.name.clone(),
            version: package.version.clone(),
            dir: package.dir.clone(),
            path: package.source.starts_with("path+"),
            source: package.source.clone(),
            checksum: package.checksum.clone(),
            lib: package.lib.as_ref().map(|(name, _)| name.clone()),
        }
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
    /// Its source, as cargo names it: `path+`, `git+`, `registry+`, or
    /// `sparse+`, then where.
    pub source: String,
    /// The registry's checksum of its `.crate`; none for a path or git
    /// package.
    pub checksum: Option<String>,
    /// Its library's crate name, as Rust code names it; none for a package
    /// without a library.
    pub lib: Option<String>,
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
    /// The exported aliases it leaves out: their instantiations name an
    /// argument the reader cannot, such as `Arc<T>`. One is no problem of
    /// the crate's, whose other offers stand; a graph that names it is
    /// told why it is not offered.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<Skipped>,
}

/// An exported alias an offer leaves out, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Skipped {
    /// The alias's name.
    pub name: String,
    /// Why, with where.
    pub problem: String,
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
    /// The declared wire encoding, also read for messages without serde impls.
    pub encoding: gdl_model::Encoding,
    /// Whether its definition path is public: the type and every module
    /// above it are `pub`. Code outside the crate can name it there only
    /// if so; an export gives it another path.
    pub public: bool,
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
    /// The encoding declared at the message's definition.
    pub encoding: gdl_model::Encoding,
    /// Whether the public path is public: every module above the `pub use`
    /// is `pub`.
    pub public: bool,
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
    /// For an instantiation of a generic message, the package that
    /// defines each argument that is a type, nested ones included, in the
    /// order `path` writes them; none for `()`, a primitive, or a message
    /// that is not generic. `path` writes an argument by its path alone, as
    /// `core::any::type_name` does, which two versions of a package, or one
    /// version from two sources, share.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<Argument>,
}

impl Origin {
    /// The message as a problem names it: its path and its package's
    /// version, and an instantiation's arguments' packages by theirs.
    pub fn shown(&self) -> String {
        self.shown_with(false)
    }

    /// Each of `origins` as a problem that names them together names it:
    /// as [`Origin::shown`] does, and with the sources of its packages
    /// where that alone reads as another of them does, since only the
    /// sources then tell them apart.
    pub fn shown_apart(origins: &[&Origin]) -> Vec<String> {
        let plain: Vec<String> = origins.iter().map(|o| o.shown()).collect();
        origins
            .iter()
            .zip(&plain)
            .map(|(origin, shown)| {
                let alike = origins
                    .iter()
                    .zip(&plain)
                    .any(|(other, text)| text == shown && other != origin);
                if alike {
                    origin.shown_with_sources()
                } else {
                    shown.clone()
                }
            })
            .collect()
    }

    /// As [`Origin::shown`], with the source of each package.
    pub fn shown_with_sources(&self) -> String {
        self.shown_with(true)
    }

    fn shown_with(&self, sources: bool) -> String {
        let package = |name: &str, version: &str, source: &str| {
            if sources {
                format!("{name} {version} from {source}")
            } else {
                format!("{name} {version}")
            }
        };
        let own = package(&self.package, &self.version, &self.source);
        if self.arguments.is_empty() {
            return format!("`{}` ({own})", self.path);
        }
        let arguments: Vec<String> = self
            .arguments
            .iter()
            .map(|a| package(&a.package, &a.version, &a.source))
            .collect();
        format!(
            "`{}` ({own}; arguments from {})",
            self.path,
            arguments.join(", ")
        )
    }
}

/// The package that defines an argument of an instantiation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Argument {
    /// The package.
    pub package: String,
    /// Its version.
    pub version: String,
    /// Its source, as [`Offer::source`].
    pub source: String,
}

/// A node a crate implements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEntry {
    /// Typed public entries of a hosted service.
    #[serde(default)]
    pub entries: Vec<EndpointEntry>,
    /// The public client bundle type of a hosted service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients: Option<String>,
    /// Only configuration fields explicitly exposed to the graph builder.
    #[serde(default)]
    pub config: Vec<gdl_model::ConfigField>,
    /// The metrics it reports, from its `derive(Node)`'s `metrics` type;
    /// left out when it reports none, so a catalogue written before nodes
    /// had metrics reads and writes the same.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<gdl_model::MetricField>,
    /// The node's name: its type's name.
    pub name: String,
    /// Its role: a sink, a forward or backward node, an operator, or a
    /// source (a hosted frontend or service).
    pub role: gdl_model::Role,
    /// The implementing type, at the crate root.
    #[serde(rename = "type")]
    pub ty: String,
    /// Its ports, in the order the role trait declares them.
    pub ports: Vec<PortEntry>,
}

/// One independently typed public entry of a hosted service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointEntry {
    /// Client field name.
    pub name: String,
    /// Public endpoint path.
    pub path: String,
    /// Boolean graph setting that enables this entry.
    pub enabled: String,
    /// Request contract.
    pub request: PortEntry,
    /// Response item contract.
    pub response: PortEntry,
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

#[cfg(test)]
mod git_tests {
    use super::*;

    #[test]
    fn a_git_package_in_a_subdirectory_keeps_its_commit_origin() {
        use std::process::Command;
        let temp = tempfile::Builder::new()
            .prefix("gdl-git-origin-")
            .tempdir()
            .expect("test fixture");
        let dir = temp.path();
        let repo = dir.join("repo");
        std::fs::create_dir_all(repo.join("protocols/src")).expect("test fixture");
        std::fs::write(
            repo.join("Cargo.toml"),
            "[workspace]\nmembers=['protocols']\nresolver='3'\n",
        )
        .expect("test fixture");
        std::fs::write(
            repo.join("protocols/Cargo.toml"),
            "[package]\nname='protocols'\nversion='0.1.0'\nedition='2024'\n",
        )
        .expect("test fixture");
        std::fs::write(
            repo.join("protocols/src/lib.rs"),
            "#[derive(gdl_message::Message)]\npub struct Prompt;\n",
        )
        .expect("test fixture");
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
                .expect("test fixture");
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout)
                .expect("test fixture")
                .trim()
                .to_string()
        };
        git(&["init", "--quiet"]);
        git(&["add", "."]);
        git(&["commit", "--quiet", "-m", "fixture"]);
        let commit = git(&["rev-parse", "HEAD"]);
        let url = format!(
            "file://{}",
            repo.canonicalize().expect("test fixture").display()
        );
        let workspace = Workspace::fetch_git(
            "protocols",
            &url,
            &commit,
            &dir.join("target"),
            &Cargo::trusted(),
        )
        .expect("test fixture");
        let extracted = workspace.extract("protocols").expect("test fixture");
        assert!(extracted.problems.is_empty(), "{:?}", extracted.problems);
        assert_eq!(
            extracted.offer.source,
            format!("git+{url}?rev={commit}#{commit}")
        );
        assert_eq!(extracted.offer.messages[0].name, "Prompt");
        assert!(!workspace.locate("protocols").expect("test fixture").path);
    }
}
