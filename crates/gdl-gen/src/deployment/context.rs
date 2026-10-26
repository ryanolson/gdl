//! The build context of a bundle whose images build local sources
//! ([`super::Sources::Local`]): the nearest directory that holds the
//! bundle, every crate its manifests name by path, the Cargo workspace of
//! each, and the hub's crate. Paths in the bundle's files are relative to
//! it, so that no file names this machine's directories.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use super::io;
use crate::dump::toml_string;
use crate::paths::{display, normalize, relative};
use crate::{Error, Files, emit};

/// Where a local bundle's images build from.
#[derive(Clone)]
pub(super) struct Context {
    /// The context's root, absolute.
    root: PathBuf,
    /// The bundle, from the root.
    bundle: PathBuf,
    /// The hub's crate, from the root.
    hub: PathBuf,
    /// The lock file of the gdl workspace, absolute, when it has one.
    lock: Option<PathBuf>,
    /// The directory of each package of the gdl workspace, absolute, by
    /// name.
    members: BTreeMap<String, PathBuf>,
    /// The oldest Rust that `gdl-runtime` builds with, when it says.
    rust_version: Option<String>,
    /// What each crate outside the bundle and the gdl workspace that the
    /// bundle reaches by path depends on, by its directory, absolute.
    crates: BTreeMap<PathBuf, Vec<Dependency>>,
    /// Each crate by path whose workspace `cargo metadata` could not read,
    /// with the reason. The image build can still build such a crate: a
    /// dependency's own dev-dependencies, for one, need not exist.
    unread: Vec<String>,
}

/// A dependency of a crate the bundle reaches by path, as `cargo metadata`
/// gives it.
#[derive(Clone)]
struct Dependency {
    /// The package's name, which a rename does not change.
    name: String,
    /// Where the package comes from, unless it comes by path.
    source: Option<String>,
    /// The package's directory, absolute, when it comes by path.
    path: Option<PathBuf>,
}

impl Context {
    /// The context of the bundle in `out` (absolute), whose generated
    /// `files` name their dependencies relative to themselves, and whose
    /// runtime is the `gdl-runtime` crate in `runtime`. A crate named by
    /// path in a crate named by path is seen only through its workspace,
    /// when one of these directories holds it.
    pub(super) fn find(out: &Path, files: &Files, runtime: &Path) -> Result<Self, Error> {
        let gdl = gdl_workspace(runtime)?;
        let lock = Some(gdl.root.join("Cargo.lock")).filter(|lock| lock.exists());
        let mut by_path = BTreeSet::new();
        for (path, text) in files {
            if path.file_name().is_none_or(|name| name != "Cargo.toml") {
                continue;
            }
            let krate = normalize(&out.join(path.parent().unwrap_or(Path::new(""))));
            by_path.extend(path_dependencies(&krate, text));
        }
        let mut needed = BTreeSet::from([out.to_path_buf(), gdl.hub.clone(), gdl.root.clone()]);
        needed.extend(by_path.iter().cloned());
        let workspaces: Vec<PathBuf> = needed.iter().filter_map(|dir| workspace_of(dir)).collect();
        needed.extend(workspaces);
        let root = needed
            .iter()
            .map(PathBuf::as_path)
            .reduce(common)
            .unwrap_or(out)
            .to_path_buf();
        if root
            .components()
            .all(|c| matches!(c, Component::RootDir | Component::Prefix(_)))
        {
            let paths: Vec<String> = needed.iter().map(|p| display(p)).collect();
            return Err(Error::new(format!(
                "`--local`: the build context would be `{}`: the bundle and the crates it builds from by path share no other directory; write the bundle into a directory beside them, such as `target/` of their repository ({})",
                display(&root),
                paths.join(", ")
            )));
        }
        let (crates, unread) = path_crates(&by_path, out, &gdl.members, &root);
        let from = |path: &Path| relative(&root, path);
        let context = Self {
            bundle: from(out),
            hub: from(&gdl.hub),
            lock,
            members: gdl.members,
            rust_version: gdl.rust_version,
            crates,
            unread,
            root,
        };
        writable("the bundle's path", &context.bundle)?;
        writable("the hub's crate's path", &context.hub)?;
        Ok(context)
    }

    /// The context's root, absolute.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// The context from the bundle, as `compose.yaml` names it.
    pub(super) fn seen_from_bundle(&self) -> String {
        let depth = self.bundle.components().count();
        let up: PathBuf = std::iter::repeat_n("..", depth).collect();
        display(&up)
    }

    /// `path`, a path in the bundle, from the context's root.
    pub(super) fn in_bundle(&self, path: &Path) -> String {
        display(&self.bundle.join(path))
    }

    /// The hub's crate, from the context's root.
    pub(super) fn hub(&self) -> String {
        display(&self.hub)
    }

    /// The gdl workspace's lock file, absolute.
    pub(super) fn lock(&self) -> Option<&Path> {
        self.lock.as_deref()
    }

    /// Each crate by path whose dependencies could not be read, with the
    /// reason, so that a patch can miss what it takes.
    pub(super) fn unread(&self) -> &[String] {
        &self.unread
    }

    /// The oldest Rust that `gdl-runtime` builds with, when it says: the
    /// oldest Rust an image can build a segment with.
    pub(super) fn rust_version(&self) -> Option<&str> {
        self.rust_version.as_deref()
    }

    /// Whether `name` is the name of a package of the gdl workspace.
    pub(super) fn is_gdl(&self, name: &str) -> bool {
        self.members.contains_key(name)
    }

    /// The `[patch]` tables of the segment crate in `krate`, a path in the
    /// bundle, whose manifest and every other generated one `files` holds:
    /// each gdl package that a crate it reaches by path takes from another
    /// source, from the gdl workspace, by path. A segment takes gdl from
    /// `[gdl] path`, and a node crate may take it from Git: two copies,
    /// whose types differ. The segment is a workspace of its own, so its
    /// patch applies to every crate it builds. Empty when there is none.
    pub(super) fn patch(&self, files: &Files, krate: &Path) -> String {
        let out = self.root.join(&self.bundle);
        let segment = normalize(&out.join(krate));
        let mut sources: BTreeMap<String, BTreeSet<&str>> = BTreeMap::new();
        let mut seen = BTreeSet::new();
        let mut queue = vec![segment.clone()];
        while let Some(dir) = queue.pop() {
            if !seen.insert(dir.clone()) {
                continue;
            }
            if let Ok(inside) = dir.strip_prefix(&out) {
                if let Some(text) = files.get(&inside.join("Cargo.toml")) {
                    queue.extend(path_dependencies(&dir, text));
                }
                continue;
            }
            for dependency in self.crates.get(&dir).into_iter().flatten() {
                if let Some(path) = &dependency.path {
                    queue.push(path.clone());
                }
                let Some(source) = &dependency.source else {
                    continue;
                };
                if let Some(member) = self.members.get_key_value(&dependency.name) {
                    sources
                        .entry(patch_key(source))
                        .or_default()
                        .insert(member.0);
                }
            }
        }
        let mut text = String::new();
        for (source, names) in sources {
            if text.is_empty() {
                text.push_str("\n# Crates this segment reaches by path take these gdl crates from another\n# source: it builds one copy of each, from [gdl] path.\n");
            }
            text.push_str(&format!("[patch.{}]\n", toml_string(&source)));
            for name in names {
                let path = display(&relative(&segment, &self.members[name]));
                text.push_str(&format!("{name} = {{ path = {} }}\n", toml_string(&path)));
            }
        }
        text
    }

    /// The ignore file of each of the bundle's Dockerfiles: build output
    /// and git history stay out of the context, and the bundle stays in
    /// even when it is written under a `target` or `.git` directory.
    pub(super) fn ignore(&self) -> String {
        let mut text = format!(
            "# {}\n# What the build context leaves out: build output and git history.\n**/target\n**/.git\n",
            emit::MARKER
        );
        if self
            .bundle
            .components()
            .any(|c| c.as_os_str() == "target" || c.as_os_str() == ".git")
        {
            text.push_str(&format!("!{}\n", pattern(&display(&self.bundle))));
        }
        text
    }
}

/// `path` as a Docker path pattern that matches only `path`, for a `COPY`
/// source or an ignore file. Docker matches a component that holds a `*`,
/// `?`, or `[` as a Go regular expression built from it, which keeps
/// `^`, `+`, `(`, `)`, `{`, `}`, and `|` as regex syntax. It reads a
/// leading `!` as a negation and trims whitespace at the ends of a
/// pattern. Each of these characters, and
/// `*`, `?`, `[`, `]`, and `\`, becomes a class of that one character,
/// escaped: `[\*]`. Whitespace becomes a class unescaped: `[ ]`. The escape
/// alone does not work: Docker takes a component without a `*`, `?`, or
/// `[` that is not escaped as it is, backslash and all.
pub(super) fn pattern(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        if "*?[]\\^!+(){}|".contains(c) {
            out.extend(['[', '\\', c, ']']);
        } else if c.is_whitespace() {
            out.extend(['[', c, ']']);
        } else {
            out.push(c);
        }
    }
    out
}

/// Refuse `path`, which `what` names, when a bundle's files cannot name
/// it: when it is not UTF-8, which the files are, or holds a line break,
/// which ends a line of a Dockerfile or an ignore file.
fn writable(what: &str, path: &Path) -> Result<(), Error> {
    let shown = format!("{:?}", path.as_os_str());
    let Some(text) = path.to_str() else {
        return Err(Error::new(format!(
            "`--local`: {what} from its build context, {shown}, is not UTF-8, which the bundle's files cannot name; choose a directory whose path is UTF-8"
        )));
    };
    if text.contains(['\n', '\r']) {
        return Err(Error::new(format!(
            "`--local`: {what} from its build context, {shown}, holds a line break, which a Dockerfile or an ignore file cannot name; choose a directory without one"
        )));
    }
    Ok(())
}

/// The gdl workspace: the workspace of `[gdl] path`.
struct Workspace {
    /// Its root, absolute.
    root: PathBuf,
    /// Its `gdl-hub` crate, absolute: the hub is built from the same
    /// sources as the runtime the segments link.
    hub: PathBuf,
    /// The directory of each of its packages, absolute, by name.
    members: BTreeMap<String, PathBuf>,
    /// `gdl-runtime`'s `rust-version`.
    rust_version: Option<String>,
}

/// The workspace that holds the `gdl-runtime` crate in `runtime`.
fn gdl_workspace(runtime: &Path) -> Result<Workspace, Error> {
    let metadata = metadata(runtime, runtime)?;
    let root = metadata["workspace_root"]
        .as_str()
        .map(|root| normalize(Path::new(root)))
        .ok_or_else(|| Error::new("`--local`: cargo metadata names no workspace root"))?;
    let members: BTreeMap<String, PathBuf> = packages(&metadata)
        .map(|(name, dir, _)| (name.to_string(), dir))
        .collect();
    let hub = members.get("gdl-hub").cloned().ok_or_else(|| {
        Error::new(format!(
            "`--local`: the workspace of [gdl] path, {}, has no `gdl-hub` crate to build the hub from",
            root.display()
        ))
    })?;
    let rust_version = metadata["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|package| package["name"] == "gdl-runtime")
        .and_then(|package| package["rust_version"].as_str())
        .map(str::to_string);
    Ok(Workspace {
        root,
        hub,
        members,
        rust_version,
    })
}

/// What each crate outside the bundle in `out` that is not one of gdl's
/// packages, `gdl`, depends on, for the crates in `dirs` and every crate
/// they reach by path, by directory, and each crate whose workspace could
/// not be read, with the reason. One `cargo metadata --no-deps` reads a
/// workspace, and fetches nothing. It runs in the context's root, `root`,
/// as the image build does, so that both read the same Cargo
/// configuration.
fn path_crates(
    dirs: &BTreeSet<PathBuf>,
    out: &Path,
    gdl: &BTreeMap<String, PathBuf>,
    root: &Path,
) -> (BTreeMap<PathBuf, Vec<Dependency>>, Vec<String>) {
    let mut crates = BTreeMap::new();
    let mut unread = Vec::new();
    let mut queue: Vec<PathBuf> = dirs.iter().cloned().collect();
    while let Some(dir) = queue.pop() {
        // gdl's packages take each other by path.
        let member = gdl.values().any(|member| *member == dir);
        if member || dir.starts_with(out) || crates.contains_key(&dir) {
            continue;
        }
        if !dir.join("Cargo.toml").exists() {
            // The image build reports a crate that is not there.
            continue;
        }
        let metadata = match metadata(&dir, root) {
            Ok(metadata) => metadata,
            Err(error) => {
                unread.push(error.to_string());
                crates.insert(dir, Vec::new());
                continue;
            }
        };
        for (_, krate, dependencies) in packages(&metadata) {
            let dependencies: Vec<Dependency> = dependencies
                .iter()
                // A crate's tests and examples are not built for a crate
                // that depends on it.
                .filter(|d| d["kind"] != "dev")
                .filter_map(|d| {
                    Some(Dependency {
                        name: d["name"].as_str()?.to_string(),
                        source: d["source"].as_str().map(str::to_string),
                        path: d["path"].as_str().map(|p| normalize(Path::new(p))),
                    })
                })
                .collect();
            queue.extend(dependencies.iter().filter_map(|d| d.path.clone()));
            crates.insert(krate, dependencies);
        }
        // A crate that names no workspace lists none of its own.
        crates.entry(dir).or_default();
    }
    (crates, unread)
}

/// `cargo metadata --no-deps` of the crate in `dir`, run in `cwd`, which
/// reads its workspace's manifests and resolves nothing.
fn metadata(dir: &Path, cwd: &Path) -> Result<serde_json::Value, Error> {
    let output =
        std::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .current_dir(cwd)
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--manifest-path",
            ])
            .arg(dir.join("Cargo.toml"))
            .output()
            .map_err(|e| Error::new(format!("`--local`: running cargo metadata: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "`--local`: cargo metadata for {}: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(io)
}

/// Each package `metadata` lists: its name, its directory, absolute, and
/// its dependencies.
fn packages(
    metadata: &serde_json::Value,
) -> impl Iterator<Item = (&str, PathBuf, &[serde_json::Value])> {
    metadata["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|package| {
            let name = package["name"].as_str()?;
            let dir = Path::new(package["manifest_path"].as_str()?).parent()?;
            let dependencies = package["dependencies"]
                .as_array()
                .map_or(&[][..], Vec::as_slice);
            Some((name, normalize(dir), dependencies))
        })
}

/// The directory of each dependency by path that the manifest `text` of
/// the crate in `krate`, absolute, names.
fn path_dependencies(krate: &Path, text: &str) -> Vec<PathBuf> {
    let Ok(manifest) = text.parse::<toml::Table>() else {
        return Vec::new();
    };
    let deps = manifest.get("dependencies").and_then(toml::Value::as_table);
    deps.into_iter()
        .flat_map(|deps| deps.values())
        .filter_map(|dep| dep.get("path").and_then(toml::Value::as_str))
        .map(|path| normalize(&krate.join(path)))
        .collect()
}

/// The key of the `[patch]` table that replaces packages from `source`, as
/// `cargo metadata` names a source: a Git repository's URL without its
/// reference, `crates-io`, or another registry's URL. Cargo names
/// crates.io by its Git index whatever protocol it reads, so a sparse
/// URL, even crates.io's, is a registry of its own.
fn patch_key(source: &str) -> String {
    if let Some(git) = source.strip_prefix("git+") {
        return git.split(['?', '#']).next().unwrap_or(git).to_string();
    }
    match source {
        "registry+https://github.com/rust-lang/crates.io-index" => "crates-io".to_string(),
        _ => source
            .strip_prefix("registry+")
            .unwrap_or(source)
            .to_string(),
    }
}

/// The root of the Cargo workspace that holds `dir`: the nearest directory
/// at or above it whose manifest has a `[workspace]` table.
fn workspace_of(dir: &Path) -> Option<PathBuf> {
    dir.ancestors()
        .find(|dir| {
            std::fs::read_to_string(dir.join("Cargo.toml"))
                .ok()
                .and_then(|text| text.parse::<toml::Table>().ok())
                .is_some_and(|manifest| manifest.contains_key("workspace"))
        })
        .map(Path::to_path_buf)
}

/// The deepest directory that holds both `a` and `b`.
fn common<'p>(a: &'p Path, b: &Path) -> &'p Path {
    a.ancestors()
        .find(|ancestor| b.starts_with(ancestor))
        .unwrap_or(a)
}

#[cfg(test)]
mod tests {
    use super::patch_key;

    /// The key is the source as a manifest names it, so that one table
    /// patches every reference to the repository.
    #[test]
    fn a_patch_key_is_the_source_without_its_reference() {
        for (source, key) in [
            (
                "git+ssh://git@github.com/NVIDIA-dev/dynamo-gdl.git?rev=88ca8d3a#88ca8d3a1b2c",
                "ssh://git@github.com/NVIDIA-dev/dynamo-gdl.git",
            ),
            (
                "git+https://github.com/ai-dynamo/velo?branch=main#0123abcd",
                "https://github.com/ai-dynamo/velo",
            ),
            ("git+file:///work/gdl", "file:///work/gdl"),
            (
                "registry+https://github.com/rust-lang/crates.io-index",
                "crates-io",
            ),
            (
                "sparse+https://index.crates.io/",
                "sparse+https://index.crates.io/",
            ),
            (
                "registry+https://registry.example/index",
                "https://registry.example/index",
            ),
        ] {
            assert_eq!(patch_key(source), key, "{source}");
        }
    }
}
