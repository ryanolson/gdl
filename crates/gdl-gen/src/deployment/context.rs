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

/// Where a local bundle's images build from, or where `cargo gdl build`
/// builds: the crates the generated manifests reach by path, their
/// workspaces' patches, and gdl's workspace.
#[derive(Clone)]
pub(crate) struct Context {
    /// What the context is for.
    mode: Mode,
    /// The context's root, absolute.
    root: PathBuf,
    /// The bundle, from the root.
    bundle: PathBuf,
    /// The hub's crate, from the root, when gdl comes from `[gdl] path`
    /// and its workspace has one.
    hub: Option<PathBuf>,
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
    /// The `[patch]` entries of the workspace of each crate in `crates`
    /// and of each gdl package, by the crate's directory, absolute.
    patches: BTreeMap<PathBuf, Vec<Patch>>,
    /// Each crate by path whose workspace `cargo metadata` could not read,
    /// with the reason. The image build can still build such a crate: a
    /// dependency's own dev-dependencies, for one, need not exist.
    unread: Vec<String>,
}

/// What a [`Context`] is for.
#[derive(Clone, Debug)]
pub(crate) enum Mode {
    /// The images of a bundle of local sources. Docker copies the
    /// context's root into each image, so the root must be a directory
    /// that holds every crate, and the bundle's files name paths from it.
    Image,
    /// `cargo gdl build`, which builds where the crates are: there is no
    /// image context to refuse, and Cargo runs in `cwd`, the spec's
    /// directory, so that the configuration there applies.
    Build { cwd: PathBuf },
}

impl Mode {
    /// How an error names the command.
    fn prefix(&self) -> &'static str {
        match self {
            Self::Image => "`--local`",
            Self::Build { .. } => "`build`",
        }
    }
}

/// An entry of a host workspace's `[patch]` tables: Cargo applies it to
/// every crate the workspace builds.
#[derive(Clone, Debug, PartialEq)]
struct Patch {
    /// The key of its table: `crates-io`, a registry, or a URL.
    key: String,
    /// The name it patches: a package's, unless the entry has `package`.
    name: String,
    /// The entry as written, but its `path` absolute.
    value: toml::Table,
    /// The root of the workspace whose manifest holds it, absolute.
    workspace: PathBuf,
}

/// The gdl packages a segment patches: by source, each package's directory,
/// by name.
type GdlPatches = BTreeMap<String, BTreeMap<String, PathBuf>>;

/// A host entry a segment carries: the package it patches, and the root
/// of the workspace whose lock says which copy the host builds.
pub(crate) struct HostPatch {
    pub(crate) package: String,
    pub(crate) workspace: PathBuf,
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
    /// runtime is the `gdl-runtime` crate in `runtime`, when gdl comes by
    /// path. A crate named by path in a crate named by path is seen only
    /// through its workspace, when one of these directories holds it.
    pub(crate) fn find(
        out: &Path,
        files: &Files,
        runtime: Option<&Path>,
        mode: Mode,
    ) -> Result<Self, Error> {
        let prefix = mode.prefix();
        // `[gdl] path` is relative to where the generator runs, and Cargo
        // runs elsewhere.
        let gdl = match runtime {
            Some(runtime) => Some(gdl_workspace(&crate::dump::absolute(runtime)?, &mode)?),
            None => None,
        };
        let lock = gdl
            .as_ref()
            .map(|gdl| gdl.root.join("Cargo.lock"))
            .filter(|lock| lock.exists());
        let members = gdl
            .as_ref()
            .map(|gdl| gdl.members.clone())
            .unwrap_or_default();
        let mut by_path = BTreeSet::new();
        for (path, text) in files {
            if path.file_name().is_none_or(|name| name != "Cargo.toml") {
                continue;
            }
            let krate = normalize(&out.join(path.parent().unwrap_or(Path::new(""))));
            by_path.extend(path_dependencies(&krate, text));
        }
        let mut needed = BTreeSet::from([out.to_path_buf()]);
        if let Some(gdl) = &gdl {
            needed.insert(gdl.root.clone());
            needed.extend(gdl.hub.clone());
        }
        needed.extend(by_path.iter().cloned());
        let workspaces: Vec<PathBuf> = needed.iter().filter_map(|dir| workspace_of(dir)).collect();
        needed.extend(workspaces);
        let mut root = needed
            .iter()
            .map(PathBuf::as_path)
            .reduce(common)
            .unwrap_or(out)
            .to_path_buf();
        let image = matches!(mode, Mode::Image);
        let only_root = |root: &Path| {
            root.components()
                .all(|c| matches!(c, Component::RootDir | Component::Prefix(_)))
        };
        if image && only_root(&root) {
            return Err(shared_only_the_root(&root, &needed));
        }
        let cwd = |root: &Path| match &mode {
            Mode::Image => root.to_path_buf(),
            Mode::Build { cwd } => cwd.clone(),
        };
        let Walked {
            mut crates,
            mut patches,
            mut unread,
        } = path_crates(&by_path, out, &members, &cwd(&root), prefix);
        // The segments take gdl's crates by path, from its workspace.
        if let Some(gdl) = &gdl {
            let own = workspace_patches(&gdl.root);
            if !own.is_empty() {
                for dir in gdl.members.values() {
                    patches.insert(dir.clone(), own.clone());
                }
            }
        }
        // A crate that a host workspace patches in by path is built as any
        // other crate by path: the context must hold it and its workspace.
        // Each gdl package that a crate the bundle reaches by path takes
        // from another source, by source: a host entry of that name there
        // gives way ([`tables`]).
        let taken: BTreeSet<(String, &str)> = reachable(&by_path, &crates)
            .into_iter()
            .flat_map(|dir| crates.get(dir).into_iter().flatten())
            .filter(|dependency| members.contains_key(&dependency.name))
            .filter_map(|dependency| {
                let source = dependency.source.as_deref()?;
                Some((source_id(&patch_key(source)), dependency.name.as_str()))
            })
            .collect();
        let patched: BTreeSet<PathBuf> = by_path
            .iter()
            .flat_map(|dir| patches.get(dir).into_iter().flatten())
            // The segment never carries a host's patch of gdl ([`tables`]).
            .filter(|entry| !members.contains_key(entry.package()))
            .filter(|entry| !taken.contains(&(source_id(&entry.key), entry.name.as_str())))
            .filter_map(Patch::path)
            .collect();
        if !patched.is_empty() {
            // Docker copies a symlink without its target: the context
            // holds both.
            let targets: Vec<PathBuf> = patched
                .iter()
                .filter_map(|dir| crate::paths::resolve(dir).ok())
                .collect();
            needed.extend(patched.iter().chain(&targets).cloned());
            let workspaces: Vec<PathBuf> = patched
                .iter()
                .chain(&targets)
                .filter_map(|dir| workspace_of(dir))
                .collect();
            needed.extend(workspaces);
            root = needed
                .iter()
                .map(PathBuf::as_path)
                .reduce(common)
                .unwrap_or(out)
                .to_path_buf();
            let unwalked = patched
                .into_iter()
                .filter(|dir| !crates.contains_key(dir))
                .collect();
            // Cargo applies a patch only from the workspace it builds, and
            // the host builds the patched crate in the patching workspace:
            // the patched crate's own workspace patches nothing.
            let mut more = path_crates(&unwalked, out, &members, &cwd(&root), prefix);
            for (dir, dependencies) in more.crates {
                crates.entry(dir).or_insert(dependencies);
            }
            unread.append(&mut more.unread);
        }
        if image && only_root(&root) {
            return Err(shared_only_the_root(&root, &needed));
        }
        let from = |path: &Path| relative(&root, path);
        let context = Self {
            bundle: from(out),
            hub: gdl.as_ref().and_then(|gdl| gdl.hub.as_deref()).map(from),
            lock,
            members,
            rust_version: gdl.and_then(|gdl| gdl.rust_version),
            crates,
            patches,
            unread,
            root,
            mode,
        };
        if image {
            writable("the bundle's path", &context.bundle)?;
            if let Some(hub) = &context.hub {
                writable("the hub's crate's path", hub)?;
            }
        }
        Ok(context)
    }

    /// The context's root, absolute.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// Where Cargo runs: the image build's root, or the spec's directory
    /// for `cargo gdl build`.
    pub(crate) fn cwd(&self) -> &Path {
        match &self.mode {
            Mode::Image => &self.root,
            Mode::Build { cwd } => cwd,
        }
    }

    /// How an error names the command.
    pub(crate) fn prefix(&self) -> &'static str {
        self.mode.prefix()
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

    /// The hub's crate, from the context's root. An image context always
    /// has one: [`gdl_workspace`] refuses a gdl workspace without it.
    pub(super) fn hub(&self) -> String {
        self.hub.as_deref().map(display).unwrap_or_default()
    }

    /// The gdl workspace's lock file, absolute.
    pub(crate) fn lock(&self) -> Option<&Path> {
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

    /// The `[patch]` tables of the workspace in `at`, a path in the bundle,
    /// that builds the generated crates `krates`, paths in the bundle,
    /// whose manifests and every other generated one `files` holds: a
    /// segment crate of a bundle, which is its own workspace, or every
    /// crate `cargo gdl build` generates, from its workspace's root.
    ///
    /// The segment is a workspace of its own, so its patch applies to every
    /// crate it builds, and Cargo reads no other workspace's. It carries
    /// the `[patch]` entries of the workspace of each crate that a
    /// generated manifest names by path, which the host applies when it
    /// builds that crate, so that the segment builds the same copies. It
    /// also patches each gdl package that a crate it reaches by path takes
    /// from another source, from the gdl workspace, by path: a segment
    /// takes gdl from `[gdl] path`, and a node crate may take it from Git:
    /// two copies, whose types differ. A host entry that patches a gdl
    /// package, from any source, is left out. Two host workspaces
    /// that patch one name of one source differently are refused: the
    /// segment cannot build what both build. Empty when there is nothing
    /// to patch.
    pub(crate) fn patch(
        &self,
        files: &Files,
        krates: &[PathBuf],
        at: &Path,
    ) -> Result<String, Error> {
        let (gdl, host) = self.reached(files, krates);
        let segment = normalize(&self.root.join(&self.bundle).join(at));
        tables(
            &segment,
            &gdl,
            &host,
            |name| self.is_gdl(name),
            self.prefix(),
        )
    }

    /// The host entries that the generated crates `krates` carry, with
    /// the workspace of each, as [`Self::patch`] writes them, before two
    /// spellings of one entry merge.
    pub(crate) fn host_patches(&self, files: &Files, krates: &[PathBuf]) -> Vec<HostPatch> {
        let (gdl, host) = self.reached(files, krates);
        host.into_iter()
            .filter(|entry| {
                let named = gdl.iter().any(|(key, names)| {
                    source_id(key) == source_id(&entry.key) && names.contains_key(&entry.name)
                });
                !named
            })
            .map(|entry| HostPatch {
                package: entry.package().to_string(),
                workspace: entry.workspace.clone(),
            })
            .collect()
    }

    /// The gdl packages that the generated crates `krates` patch, by
    /// source, with their directories, and the host entries they carry.
    fn reached(&self, files: &Files, krates: &[PathBuf]) -> (GdlPatches, Vec<&Patch>) {
        let out = self.root.join(&self.bundle);
        let mut gdl: BTreeMap<String, BTreeSet<&str>> = BTreeMap::new();
        let mut host: Vec<&Patch> = Vec::new();
        let mut seen = BTreeSet::new();
        let mut queue: Vec<PathBuf> = krates
            .iter()
            .map(|krate| normalize(&out.join(krate)))
            .collect();
        while let Some(dir) = queue.pop() {
            if !seen.insert(dir.clone()) {
                continue;
            }
            if let Ok(inside) = dir.strip_prefix(&out) {
                if let Some(text) = files.get(&inside.join("Cargo.toml")) {
                    for named in path_dependencies(&dir, text) {
                        let entries = self.patches.get(&named).into_iter().flatten();
                        for entry in entries.filter(|entry| !self.is_gdl(entry.package())) {
                            // A crate patched in is built: it may take gdl
                            // from another source too.
                            queue.extend(entry.path());
                            host.push(entry);
                        }
                        queue.push(named);
                    }
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
                    gdl.entry(patch_key(source)).or_default().insert(member.0);
                }
            }
        }
        let gdl = gdl
            .into_iter()
            .map(|(key, names)| {
                let names = names
                    .into_iter()
                    .map(|name| (name.to_string(), self.members[name].clone()))
                    .collect();
                (key, names)
            })
            .collect();
        (gdl, host)
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

/// The refusal of a build context at `root`, the file system's root, which
/// is all that the directories `needed` share.
fn shared_only_the_root(root: &Path, needed: &BTreeSet<PathBuf>) -> Error {
    let paths: Vec<String> = needed.iter().map(|p| display(p)).collect();
    Error::new(format!(
        "`--local`: the build context would be `{}`: the bundle and the crates it builds from by path share no other directory; write the bundle into a directory beside them, such as `target/` of their repository ({})",
        display(root),
        paths.join(", ")
    ))
}

impl Patch {
    /// The package it patches: `package` when the entry renames it.
    fn package(&self) -> &str {
        self.value
            .get("package")
            .and_then(toml::Value::as_str)
            .unwrap_or(&self.name)
    }

    /// The directory of the copy it patches in, absolute, when it gives
    /// one by path.
    fn path(&self) -> Option<PathBuf> {
        self.value
            .get("path")
            .and_then(toml::Value::as_str)
            .map(PathBuf::from)
    }
}

/// The gdl workspace: the workspace of `[gdl] path`.
struct Workspace {
    /// Its root, absolute.
    root: PathBuf,
    /// Its `gdl-hub` crate, absolute: the hub is built from the same
    /// sources as the runtime the segments link. Only an image needs it.
    hub: Option<PathBuf>,
    /// The directory of each of its packages, absolute, by name.
    members: BTreeMap<String, PathBuf>,
    /// `gdl-runtime`'s `rust-version`.
    rust_version: Option<String>,
}

/// The workspace that holds the `gdl-runtime` crate in `runtime`. An
/// image context refuses one without a `gdl-hub` crate.
fn gdl_workspace(runtime: &Path, mode: &Mode) -> Result<Workspace, Error> {
    let prefix = mode.prefix();
    let cwd = match mode {
        Mode::Image => runtime,
        Mode::Build { cwd } => cwd.as_path(),
    };
    let metadata = metadata(runtime, cwd, prefix)?;
    let root = metadata["workspace_root"]
        .as_str()
        .map(|root| normalize(Path::new(root)))
        .ok_or_else(|| Error::new(format!("{prefix}: cargo metadata names no workspace root")))?;
    let members: BTreeMap<String, PathBuf> = packages(&metadata)
        .map(|(name, dir, _)| (name.to_string(), dir))
        .collect();
    let hub = members.get("gdl-hub").cloned();
    if hub.is_none() && matches!(mode, Mode::Image) {
        return Err(Error::new(format!(
            "`--local`: the workspace of [gdl] path, {}, has no `gdl-hub` crate to build the hub from",
            root.display()
        )));
    }
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
    prefix: &str,
) -> Walked {
    let mut crates = BTreeMap::new();
    let mut patches = BTreeMap::new();
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
        let metadata = match metadata(&dir, root, prefix) {
            Ok(metadata) => metadata,
            Err(error) => {
                unread.push(error.to_string());
                crates.insert(dir, Vec::new());
                continue;
            }
        };
        let workspace = metadata["workspace_root"]
            .as_str()
            .map(|root| normalize(Path::new(root)));
        let entries = workspace
            .as_deref()
            .map(workspace_patches)
            .unwrap_or_default();
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
            patches.insert(krate.clone(), entries.clone());
            crates.insert(krate, dependencies);
        }
        // A crate that names no workspace lists none of its own.
        crates.entry(dir).or_default();
    }
    Walked {
        crates,
        patches,
        unread,
    }
}

/// What [`path_crates`] reads of the crates the bundle reaches by path.
struct Walked {
    /// What each crate depends on, by its directory, absolute.
    crates: BTreeMap<PathBuf, Vec<Dependency>>,
    /// The `[patch]` entries of each crate's workspace, by its directory.
    patches: BTreeMap<PathBuf, Vec<Patch>>,
    /// Each crate whose workspace could not be read, with the reason.
    unread: Vec<String>,
}

/// The crates in `crates` that the crates in `dirs` reach by path, `dirs`
/// among them. `crates` also holds the other members of their workspaces,
/// which nothing may build.
fn reachable<'c>(
    dirs: &BTreeSet<PathBuf>,
    crates: &'c BTreeMap<PathBuf, Vec<Dependency>>,
) -> BTreeSet<&'c PathBuf> {
    let mut seen = BTreeSet::new();
    let mut queue: Vec<&PathBuf> = dirs.iter().collect();
    while let Some(dir) = queue.pop() {
        let Some((dir, dependencies)) = crates.get_key_value(dir) else {
            continue;
        };
        if seen.insert(dir) {
            queue.extend(dependencies.iter().filter_map(|d| d.path.as_ref()));
        }
    }
    seen
}

/// The `[patch]` entries of the workspace whose root is `workspace`,
/// absolute, each `path` made absolute with its `..` steps applied, as
/// Cargo applies them. A symlink stays: Cargo reads the patched crate's
/// own relative paths from the link, not from its target. An entry that is a version
/// alone is the table Cargo reads it as. `cargo metadata` has read the
/// manifest, so it parses.
fn workspace_patches(workspace: &Path) -> Vec<Patch> {
    let Ok(text) = std::fs::read_to_string(workspace.join("Cargo.toml")) else {
        return Vec::new();
    };
    let Ok(manifest) = text.parse::<toml::Table>() else {
        return Vec::new();
    };
    let tables = manifest.get("patch").and_then(toml::Value::as_table);
    let mut entries = Vec::new();
    for (key, table) in tables.into_iter().flatten() {
        for (name, value) in table.as_table().into_iter().flatten() {
            let mut value = match value {
                toml::Value::Table(table) => table.clone(),
                toml::Value::String(_) => {
                    toml::Table::from_iter([("version".into(), value.clone())])
                }
                _ => continue,
            };
            if let Some(path) = value.get("path").and_then(toml::Value::as_str) {
                let path = normalize(&workspace.join(path));
                value.insert("path".into(), toml::Value::String(display(&path)));
            }
            entries.push(Patch {
                key: key.clone(),
                name: name.clone(),
                value,
                workspace: workspace.to_path_buf(),
            });
        }
    }
    entries
}

/// `cargo metadata --no-deps` of the crate in `dir`, run in `cwd`, which
/// reads its workspace's manifests and resolves nothing.
fn metadata(dir: &Path, cwd: &Path, prefix: &str) -> Result<serde_json::Value, Error> {
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
            .map_err(|e| Error::new(format!("{prefix}: running cargo metadata: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "{prefix}: cargo metadata for {}: {}",
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

/// The `[patch]` tables of the segment crate in `segment`, absolute: the
/// gdl packages that `gdl` lists by source, with their directories, and the
/// host workspaces' entries `host`, merged one table per source.
///
/// A host entry that patches a package for which `is_gdl` holds is left
/// out, from any source: the segment builds one copy of gdl, from `[gdl]
/// path`, and gdl's own entries cover each source a crate it reaches takes
/// gdl from. So is a host entry whose name is one of gdl's entries in the
/// same table, which TOML cannot hold twice. Two host entries that patch
/// one name of one source alike are one; two that differ are refused. Each
/// path is written from the segment. With no host entry, the text is the
/// gdl patch alone, as it was before the segment carried any host's.
fn tables(
    segment: &Path,
    gdl: &GdlPatches,
    host: &[&Patch],
    is_gdl: impl Fn(&str) -> bool,
    prefix: &str,
) -> Result<String, Error> {
    // Each source's table is written as gdl names it, else as the first
    // host that patches it does: two spellings of one source are one.
    let mut spelled: BTreeMap<String, &str> = gdl
        .keys()
        .map(|key| (source_id(key), key.as_str()))
        .collect();
    let mut merged: BTreeMap<&str, BTreeMap<&str, &Patch>> = BTreeMap::new();
    for &entry in host {
        let key = *spelled.entry(source_id(&entry.key)).or_insert(&entry.key);
        let named = gdl
            .get(key)
            .is_some_and(|names| names.contains_key(&entry.name));
        if named || is_gdl(entry.package()) {
            continue;
        }
        let names = merged.entry(key).or_default();
        match names.get(entry.name.as_str()) {
            Some(first) if first.value != entry.value => {
                return Err(Error::new(format!(
                    "{prefix}: segment crate {} builds crates of two workspaces that patch `{}` from {} differently, {} with {} and {} with {}; Cargo reads only the segment's patch, which can give one: patch it alike in both",
                    display(segment),
                    entry.name,
                    toml_string(&entry.key),
                    display(&first.workspace),
                    toml::Value::Table(first.value.clone()),
                    display(&entry.workspace),
                    toml::Value::Table(entry.value.clone()),
                )));
            }
            Some(_) => {}
            None => {
                names.insert(&entry.name, entry);
            }
        }
    }
    let mut keys: BTreeSet<&str> = gdl.keys().map(String::as_str).collect();
    keys.extend(merged.keys());
    let mut text = String::new();
    for key in keys {
        if text.is_empty() {
            text.push_str(match merged.is_empty() {
                true => "\n# Crates this segment reaches by path take these gdl crates from another\n# source: it builds one copy of each, from [gdl] path.\n",
                false => "\n# The patches of the workspace of each crate this segment takes by path,\n# which Cargo reads only from the workspace it builds, so that the segment\n# builds what the host builds; and the gdl crates that crates it reaches by\n# path take from another source: it builds one copy of each, from [gdl] path.\n",
            });
        }
        text.push_str(&format!("[patch.{}]\n", toml_string(key)));
        let mut lines = BTreeMap::new();
        for (name, dir) in gdl.get(key).into_iter().flatten() {
            let path = display(&relative(segment, dir));
            lines.insert(
                name.as_str(),
                format!("{{ path = {} }}", toml_string(&path)),
            );
        }
        for (&name, entry) in merged.get(key).into_iter().flatten() {
            let mut value = entry.value.clone();
            if let Some(dir) = entry.path() {
                let path = display(&relative(segment, &dir));
                value.insert("path".into(), toml::Value::String(path));
            }
            lines.insert(name, toml::Value::Table(value).to_string());
        }
        for (name, value) in lines {
            text.push_str(&format!("{} = {value}\n", toml_key(name)));
        }
    }
    Ok(text)
}

/// The source that the `[patch]` key `key` names, as Cargo compares
/// sources: crates.io by its name or its Git index, and a URL without a
/// trailing `/` or `.git`, and on GitHub in lower case.
fn source_id(key: &str) -> String {
    let url = key.trim_end_matches('/');
    let url = url.strip_suffix(".git").unwrap_or(url);
    match url {
        "crates-io" | "https://github.com/rust-lang/crates.io-index" => "crates-io".to_string(),
        _ if url.starts_with("https://github.com/") => url.to_lowercase(),
        _ => url.to_string(),
    }
}

/// `name` as a TOML key: bare when it can be, as a crate's name can.
fn toml_key(name: &str) -> String {
    let bare = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    match bare {
        true => name.to_string(),
        false => toml_string(name),
    }
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
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use super::{Patch, patch_key, source_id, tables};

    /// A host workspace at `workspace` patching `name` from `key` with
    /// `value`, its path absolute, as [`super::workspace_patches`] reads it.
    fn entry(workspace: &str, key: &str, name: &str, value: &str) -> Patch {
        Patch {
            key: key.into(),
            name: name.into(),
            value: value.parse().expect("an entry"),
            workspace: workspace.into(),
        }
    }

    /// gdl's patch of `gdl` and `gdl-message` from `key`, in `/work/gdl`.
    fn gdl(key: &str) -> BTreeMap<String, BTreeMap<String, PathBuf>> {
        let names = ["gdl", "gdl-message"]
            .map(|name| (name.into(), Path::new("/work/gdl/crates").join(name)));
        BTreeMap::from([(key.into(), BTreeMap::from(names))])
    }

    const SEGMENT: &str = "/work/host/target/bundle/main";

    fn is_gdl(name: &str) -> bool {
        ["gdl", "gdl-message"].contains(&name)
    }
    const GDL: &str = "ssh://git@github.com/NVIDIA-dev/dynamo-gdl.git";

    /// With no host entry, the text is gdl's patch alone, byte for byte as
    /// a segment wrote it before it carried a host's.
    #[test]
    fn without_a_host_patch_the_text_is_gdls_alone() {
        let text = tables(Path::new(SEGMENT), &gdl(GDL), &[], is_gdl, "`--local`").expect("tables");
        assert_eq!(
            text,
            format!(
                "\n# Crates this segment reaches by path take these gdl crates from another\n# source: it builds one copy of each, from [gdl] path.\n[patch.\"{GDL}\"]\ngdl = {{ path = \"../../../../gdl/crates/gdl\" }}\ngdl-message = {{ path = \"../../../../gdl/crates/gdl-message\" }}\n"
            )
        );
        assert_eq!(
            tables(
                Path::new(SEGMENT),
                &BTreeMap::new(),
                &[],
                is_gdl,
                "`--local`"
            )
            .expect("tables"),
            ""
        );
    }

    /// A host entry by path is written from the segment; one from Git
    /// stays from Git, every key as written. Both share gdl's table of
    /// their source, which is written once.
    #[test]
    fn a_host_entry_keeps_its_source_and_its_path_is_written_from_the_segment() {
        let git = entry(
            "/work/host",
            "crates-io",
            "dynamo-protocols",
            "git = \"https://github.com/ai-dynamo/frontend-crates\"\nrev = \"7ff4b858\"",
        );
        let path = entry(
            "/work/host",
            "crates-io",
            "pinned",
            "path = \"/work/host/vendor/pinned\"\npackage = \"pinned-fork\"",
        );
        let text = tables(
            Path::new(SEGMENT),
            &gdl("crates-io"),
            &[&git, &path],
            is_gdl,
            "`--local`",
        )
        .expect("tables");
        assert_eq!(text.matches("[patch.").count(), 1, "{text}");
        assert!(
            text.ends_with(
                "[patch.\"crates-io\"]\n\
                 dynamo-protocols = { git = \"https://github.com/ai-dynamo/frontend-crates\", rev = \"7ff4b858\" }\n\
                 gdl = { path = \"../../../../gdl/crates/gdl\" }\n\
                 gdl-message = { path = \"../../../../gdl/crates/gdl-message\" }\n\
                 pinned = { package = \"pinned-fork\", path = \"../../../vendor/pinned\" }\n"
            ),
            "{text}"
        );
        let parsed: toml::Table = text.parse().expect("TOML");
        assert_eq!(parsed["patch"].as_table().map(toml::Table::len), Some(1));
    }

    /// A host entry that patches a gdl package gives way to gdl's, from
    /// any source and under any name: the segment builds the one copy from
    /// `[gdl] path`. An entry named as one of gdl's in the same table gives
    /// way too, whatever it patches; under another source it stays.
    #[test]
    fn gdls_entry_wins_over_a_hosts_for_the_same_package_and_source() {
        let same = entry("/work/host", GDL, "gdl", "path = \"/work/host/gdl-fork\"");
        let renamed = entry(
            "/work/host",
            "crates-io",
            "fork",
            "package = \"gdl\"\npath = \"/work/host/gdl-fork\"",
        );
        let alias = entry(
            "/work/host",
            GDL,
            "gdl-message",
            "package = \"pinned\"\npath = \"/work/host/pinned\"",
        );
        let kept = entry(
            "/work/host",
            "crates-io",
            "gdl-message",
            "package = \"pinned\"\npath = \"/work/host/pinned\"",
        );
        let other = entry(
            "/work/host",
            "crates-io",
            "gdl",
            "path = \"/work/host/gdl-fork\"",
        );
        let text = tables(
            Path::new(SEGMENT),
            &gdl(GDL),
            &[&same, &other, &renamed, &alias, &kept],
            is_gdl,
            "`--local`",
        )
        .expect("tables");
        assert_eq!(
            text.matches("gdl-fork").count(),
            0,
            "no host patch of gdl is carried: {text}"
        );
        assert!(
            text.contains(&format!(
                "[patch.\"{GDL}\"]\ngdl = {{ path = \"../../../../gdl/crates/gdl\" }}\ngdl-message = {{ path = \"../../../../gdl/crates/gdl-message\" }}\n"
            )),
            "{text}"
        );
        assert!(
            text.contains(
                "[patch.\"crates-io\"]\ngdl-message = { package = \"pinned\", path = \"../../../pinned\" }\n"
            ),
            "{text}"
        );
    }

    /// Two host workspaces that patch one name of one source alike give
    /// one entry; two that differ are refused, both named.
    #[test]
    fn host_workspaces_that_patch_a_name_differently_are_refused() {
        let a = entry(
            "/work/a",
            "crates-io",
            "pinned",
            "path = \"/work/vendor/pinned\"",
        );
        let b = entry(
            "/work/b",
            "crates-io",
            "pinned",
            "path = \"/work/vendor/pinned\"",
        );
        let text = tables(
            Path::new(SEGMENT),
            &BTreeMap::new(),
            &[&a, &b],
            is_gdl,
            "`--local`",
        )
        .expect("alike");
        assert_eq!(text.matches("pinned = ").count(), 1, "{text}");

        let c = entry("/work/c", "crates-io", "pinned", "version = \"=1.2.3\"");
        let error = tables(
            Path::new(SEGMENT),
            &BTreeMap::new(),
            &[&a, &c],
            is_gdl,
            "`--local`",
        )
        .expect_err("two patches of one name")
        .to_string();
        assert!(error.contains("`pinned`"), "{error}");
        assert!(
            error.contains("/work/a") && error.contains("/work/c"),
            "{error}"
        );
    }

    /// Two keys name one source when Cargo takes them for one: crates.io
    /// by name or by its Git index, and a URL with or without a trailing
    /// `/` or `.git`, on GitHub in any case. A sparse index is its own.
    #[test]
    fn a_source_id_is_the_source_as_cargo_compares_it() {
        for (a, b) in [
            ("crates-io", "https://github.com/rust-lang/crates.io-index"),
            ("crates-io", "https://github.com/rust-lang/crates.io-index/"),
            (
                "https://github.com/NVIDIA-dev/dynamo-gdl.git",
                "https://github.com/nvidia-dev/dynamo-gdl",
            ),
            ("file:///work/gdl/", "file:///work/gdl"),
        ] {
            assert_eq!(source_id(a), source_id(b), "{a} and {b}");
        }
        assert_ne!(
            source_id("crates-io"),
            source_id("sparse+https://index.crates.io/")
        );
        assert_ne!(source_id("file:///work/Gdl"), source_id("file:///work/gdl"));
    }

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

    /// `cargo gdl build` builds where the crates are: a target directory
    /// that shares only `/` with them is no image context to refuse. With
    /// gdl from Git or a version there is no gdl workspace, and the host's
    /// patches still apply.
    #[test]
    fn a_build_context_may_share_only_the_root_and_needs_no_gdl_path() {
        let temp = tempfile::Builder::new()
            .prefix("gdl-build-context-")
            .tempdir()
            .expect("a workspace");
        let host = temp.path().canonicalize().expect("the workspace path");
        std::fs::write(
            host.join("Cargo.toml"),
            "[workspace]\nmembers = [\"node\"]\nresolver = \"3\"\n\
             [patch.crates-io]\ngdl-host-pinned = { path = \"pinned\" }\n",
        )
        .expect("the root manifest");
        let node = host.join("node");
        std::fs::create_dir_all(node.join("src")).expect("a crate");
        std::fs::write(node.join("src/lib.rs"), "").expect("a library");
        std::fs::write(
            node.join("Cargo.toml"),
            "[package]\nname = \"node\"\nversion = \"1.0.0\"\nedition = \"2024\"\n",
        )
        .expect("a manifest");
        // Under the repository, where the target directory is: the two
        // share only `/` when the system's temporary directory is not
        // under it.
        let out = crate::dump::absolute(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/gdl-build-context/workspace"),
        )
        .expect("the workspace's path");
        let from = out.join("main");
        let files = crate::Files::from([(
            PathBuf::from("main/Cargo.toml"),
            format!(
                "[package]\nname = \"segment\"\nversion = \"1.0.0\"\n[dependencies]\nnode = {{ path = {} }}\n",
                crate::dump::toml_string(&crate::paths::display(&crate::paths::relative(
                    &from, &node
                )))
            ),
        )]);
        let shared = out
            .ancestors()
            .find(|dir| host.starts_with(dir))
            .expect("a shared directory");
        if shared.parent().is_none() {
            let refused = super::Context::find(&out, &files, None, super::Mode::Image);
            assert!(refused.is_err(), "an image context of `/`");
        }
        let context =
            super::Context::find(&out, &files, None, super::Mode::Build { cwd: host.clone() })
                .expect("the context");
        let patch = context
            .patch(&files, &[PathBuf::from("main")], Path::new(""))
            .expect("the patch");
        assert!(patch.contains("gdl-host-pinned"), "{patch}");
        assert!(
            patch.contains("path = \"../"),
            "written from the root: {patch}"
        );
    }

    /// The generated manifest takes the runtime by path, so its workspace
    /// patches apply just like those of any other direct path dependency.
    #[test]
    fn a_context_carries_the_runtime_workspaces_patch() {
        let temp = tempfile::Builder::new()
            .prefix("gdl-runtime-patch-")
            .tempdir()
            .expect("a workspace");
        let root = temp.path().canonicalize().expect("the workspace path");
        std::fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nmembers = [\"gdl-runtime\", \"gdl-hub\"]\nexclude = [\"pinned\"]\nresolver = \"3\"\n\
             [patch.crates-io]\ngdl-host-pinned = { path = \"pinned\" }\n",
        )
        .expect("the root manifest");
        for name in ["gdl-runtime", "gdl-hub", "pinned"] {
            let dir = root.join(name);
            std::fs::create_dir_all(dir.join("src")).expect("a crate");
            std::fs::write(dir.join("src/lib.rs"), "").expect("a library");
            let (package, extra) = match name {
                "gdl-runtime" => (name, "[dependencies]\ngdl-host-pinned = \"=1.0.0\"\n"),
                "pinned" => ("gdl-host-pinned", "[workspace]\n"),
                _ => (name, ""),
            };
            std::fs::write(
                dir.join("Cargo.toml"),
                format!(
                    "[package]\nname = \"{package}\"\nversion = \"1.0.0\"\nedition = \"2024\"\n{extra}"
                ),
            )
            .expect("a manifest");
        }
        let host = std::process::Command::new("timeout")
            .args([
                "30",
                env!("CARGO"),
                "metadata",
                "--offline",
                "--format-version",
                "1",
            ])
            .arg("--manifest-path")
            .arg(root.join("Cargo.toml"))
            .output()
            .expect("the runtime's patched resolve");
        assert!(
            host.status.success(),
            "{}",
            String::from_utf8_lossy(&host.stderr)
        );
        let files = crate::Files::from([(
            PathBuf::from("main/Cargo.toml"),
            "[package]\nname = \"segment\"\nversion = \"1.0.0\"\n[workspace]\n\
             [dependencies]\ngdl-runtime = { path = \"../../gdl-runtime\" }\n"
                .into(),
        )]);
        let context = super::Context::find(
            &root.join("bundle"),
            &files,
            Some(&root.join("gdl-runtime")),
            super::Mode::Image,
        )
        .expect("the context");
        let patch = context
            .patch(&files, &[PathBuf::from("main")], Path::new("main"))
            .expect("the patch");
        assert!(
            patch.contains("gdl-host-pinned"),
            "the segment loses the runtime workspace's patch: {patch:?}"
        );
    }
}
