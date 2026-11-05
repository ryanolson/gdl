//! The tree that the images of a bundle of local sources build from
//! ([`super::Sources::Local`]), for `images`, `k8s`, `deploy`, and
//! `compose`: each directory of sources as Git holds it, and the bundle at
//! its place among them, with each segment's lock settled there.
//!
//! Git decides what is in it. Each source directory is unpacked from
//! `git archive` of a commit of its repository: `HEAD`, or, when tracked
//! files have changes, the commit `git stash create` makes of them, which
//! touches neither the checkout nor any ref. So no file Git ignores or
//! does not track can reach an image, a link travels as a link, and a name
//! stays the bytes Git holds. A walk of the working tree, filtered by hand,
//! was the design before this one, and files got past its filter: through a
//! tracked directory replaced by a link, a name Git lists as bytes read as
//! UTF-8, and a parent step in an absolute path. See
//! `docs/src/guides/kubernetes.md`.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

use sha2::{Digest as _, Sha256};

use super::context::Context;
use crate::paths::normalize;
use crate::{Error, Files};

/// Build in `dir`, an empty directory, the tree of `context`'s sources,
/// with the bundle `files` at the bundle's place, and the lock of each
/// segment crate of `segments`, paths in the bundle, settled from gdl's.
/// Returns the sha256, as hex, of what the tree holds ([`digest`]).
pub(super) fn build(
    dir: &Path,
    context: &Context,
    files: &Files,
    segments: &[PathBuf],
) -> Result<String, Error> {
    let prefix = context.prefix();
    let mut entries = BTreeMap::new();
    let mut commits = BTreeMap::new();
    let (sources, unplaced) = sources(context)?;
    for source in sources {
        unpack(dir, context, &source, &mut commits, &mut entries)?;
    }
    for (path, entry) in &entries {
        if entry.kind != Kind::Generated && path.file_name() == Some(OsStr::new("Cargo.toml")) {
            manifest(dir, context, path)?;
        }
    }
    configuration(dir, context, &mut entries)?;
    // A manifest that names a directory outside the sources says why Git
    // cannot place it: that refusal comes first.
    if let Some(error) = unplaced {
        return Err(error);
    }
    let seed = seed(context, &entries)?;
    let bundle = dir.join(context.bundle());
    if let Err(link) = directories(dir, context.bundle()).map_err(super::io)? {
        return Err(Error::new(format!(
            "{prefix}: {} is on the bundle's path, {}, and is not a directory: the bundle is written through no link, which could lead outside the tree",
            context.root().join(link).display(),
            context.bundle().display()
        )));
    }
    crate::write(&bundle, files)
        .map_err(|e| Error::new(format!("{prefix}: writing the bundle: {e}")))?;
    // A link that is dangling in the archive can resolve once the bundle
    // is written: links are checked on the tree as built, before Cargo
    // reads it, and again once it is complete, before any image uses it.
    links(dir, context, &entries)?;
    for crate_dir in segments {
        let at = bundle.join(crate_dir);
        if let Some(lock) = &seed {
            std::fs::copy(dir.join(lock), at.join("Cargo.lock"))
                .map_err(|e| Error::new(format!("{prefix}: {}: {e}", lock.display())))?;
        }
        let carried = context.host_patches(files, std::slice::from_ref(crate_dir));
        // Cargo runs at the tree's root, as the image build does at
        // `/app`, so that both read the Cargo configuration the sources
        // hold, whose relative paths name the tree's copies.
        super::locks::settle(&at, dir, &carried, context)?;
    }
    generated(&bundle, context.bundle(), &mut entries)?;
    links(dir, context, &entries)?;
    Ok(digest(&entries))
}

/// Make each directory of `path`, a path in the tree in `dir`, from the
/// tree's root, never following a link: a component that exists and is
/// not a directory, such as a link, is given back, and nothing below it is
/// made.
fn directories(dir: &Path, path: &Path) -> std::io::Result<Result<(), PathBuf>> {
    let mut at = PathBuf::new();
    for part in path.components() {
        at.push(part);
        match std::fs::symlink_metadata(dir.join(&at)) {
            Ok(found) if found.is_dir() => {}
            Ok(_) => return Ok(Err(at)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(dir.join(&at))?;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(Ok(()))
}

/// gdl's lock, as a path in the tree, when the archive holds it: each
/// segment's lock starts from it. Only bytes the archive holds reach the
/// tree, so a lock that Git does not hold, such as one it ignores, seeds
/// nothing, and each lock is settled from scratch.
fn seed(context: &Context, entries: &BTreeMap<PathBuf, Entry>) -> Result<Option<PathBuf>, Error> {
    let Some(lock) = context.lock() else {
        return Ok(None);
    };
    let root = context.root().canonicalize().map_err(super::io)?;
    let real = lock.canonicalize().map_err(super::io)?;
    Ok(real
        .strip_prefix(&root)
        .ok()
        .filter(|path| {
            entries
                .get(*path)
                .is_some_and(|entry| entry.kind == Kind::File)
        })
        .map(Path::to_path_buf))
}

/// What one path of the tree is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A file from a source, executable or not.
    File,
    /// A link from a source, by its target.
    Link,
    /// A file the generator or Cargo wrote: the bundle and its locks.
    Generated,
}

impl Kind {
    fn name(self) -> &'static [u8] {
        match self {
            Self::File => b"file",
            Self::Link => b"link",
            Self::Generated => b"generated",
        }
    }
}

/// One path of the tree: its kind, its mode, and the sha256 of its
/// content, or of its target for a link.
struct Entry {
    kind: Kind,
    mode: u32,
    body: [u8; 32],
}

/// One directory of sources, as Git names it.
struct Source {
    /// The directory, absolute, as the context names it.
    dir: PathBuf,
    /// Its path in the tree: from the context's root, with every link
    /// resolved.
    at: PathBuf,
    /// Its repository's working tree, as Git names it.
    top: PathBuf,
    /// Its path in the repository, empty or ending in `/`, as bytes.
    prefix: Vec<u8>,
}

/// The directories of sources of `context`, each in the repository Git
/// finds for it, without one that another of the same repository holds,
/// and the refusal of the first directory Git cannot place, such as one in
/// no repository, naming it and Git's words: it stops the build.
fn sources(context: &Context) -> Result<(Vec<Source>, Option<Error>), Error> {
    let mut found: Vec<Source> = Vec::new();
    let mut unplaced = None;
    let root = context.root().canonicalize().map_err(super::io)?;
    for dir in context.sources() {
        // A directory reached through a link is placed where Git has it,
        // and the link is the archive's own entry.
        let real = dir
            .canonicalize()
            .map_err(|e| Error::new(format!("{}: {}: {e}", context.prefix(), dir.display())))?;
        let Ok(at) = real.strip_prefix(&root) else {
            return Err(Error::new(format!(
                "{}: {} resolves to {}, outside the context {}",
                context.prefix(),
                dir.display(),
                real.display(),
                root.display()
            )));
        };
        let placed = git(context, dir, &["rev-parse", "--show-toplevel"])
            .and_then(|top| Ok((top, git(context, dir, &["rev-parse", "--show-prefix"])?)));
        let (top, prefix) = match placed {
            Ok(placed) => placed,
            Err(error) => {
                unplaced.get_or_insert(error);
                continue;
            }
        };
        found.push(Source {
            dir: dir.clone(),
            at: at.to_path_buf(),
            top: PathBuf::from(OsStr::from_bytes(trim(&top))),
            prefix: trim(&prefix).to_vec(),
        });
    }
    found.sort_by(|a, b| (&a.top, &a.prefix).cmp(&(&b.top, &b.prefix)));
    let mut kept: Vec<Source> = Vec::new();
    for source in found {
        let held = kept
            .iter()
            .any(|outer| outer.top == source.top && source.prefix.starts_with(&outer.prefix));
        if !held {
            kept.push(source);
        }
    }
    Ok((kept, unplaced))
}

/// Unpack `source` into `dir`, at its path from the context's root, from
/// `git archive` of its repository's commit, which `commits` keeps by
/// repository, and record each path in `entries`.
fn unpack(
    dir: &Path,
    context: &Context,
    source: &Source,
    commits: &mut BTreeMap<PathBuf, OsString>,
    entries: &mut BTreeMap<PathBuf, Entry>,
) -> Result<(), Error> {
    let prefix = context.prefix();
    let at = &source.at;
    let commit = match commits.get(&source.top) {
        Some(commit) => commit.clone(),
        None => {
            let commit = commit(context, &source.top)?;
            commits.insert(source.top.clone(), commit.clone());
            commit
        }
    };
    let mut tree = commit;
    tree.push(":");
    tree.push(OsStr::from_bytes(&source.prefix));
    let tracked = std::process::Command::new("git")
        .arg("-C")
        .arg(&source.top)
        .args([OsStr::new("cat-file"), "-e".as_ref(), &tree])
        .output()
        .map_err(|e| Error::new(format!("{prefix}: running git: {e}")))?;
    if !tracked.status.success() {
        return Err(Error::new(format!(
            "{prefix}: {} holds no file Git tracks, and `--local` builds only what Git holds: `git add` its files",
            source.dir.display()
        )));
    }
    // A submodule is another repository, whose files the archive leaves
    // out: refused, rather than built without them.
    let listed = git(
        context,
        &source.top,
        &[OsStr::new("ls-tree"), "-r".as_ref(), "-z".as_ref(), &tree],
    )?;
    for line in listed
        .split(|byte| *byte == 0)
        .filter(|line| !line.is_empty())
    {
        if line.starts_with(b"160000 ") {
            let name = line
                .splitn(2, |byte| *byte == b'\t')
                .nth(1)
                .unwrap_or_default();
            return Err(Error::new(format!(
                "{prefix}: {} is a Git submodule, which `--local` does not build: build its crates from their own repository, by path",
                source.dir.join(OsStr::from_bytes(name)).display()
            )));
        }
    }
    let archive = git(
        context,
        &source.top,
        &[OsStr::new("archive"), "--format=tar".as_ref(), &tree],
    )?;
    let failed = |e: std::io::Error| {
        Error::new(format!(
            "{prefix}: reading the archive of {}: {e}",
            source.dir.display()
        ))
    };
    let mut archive = tar::Archive::new(archive.as_slice());
    for entry in archive.entries().map_err(failed)? {
        let mut entry = entry.map_err(failed)?;
        let name = PathBuf::from(OsStr::from_bytes(&entry.path_bytes()).to_os_string());
        let host = source.dir.join(&name);
        let refuse = |why: &str| {
            Err(Error::new(format!(
                "{prefix}: {} {why}, so `--local` builds no image from these sources",
                host.display()
            )))
        };
        let kind = entry.header().entry_type();
        if matches!(
            kind,
            tar::EntryType::XGlobalHeader | tar::EntryType::XHeader
        ) {
            continue;
        }
        if !name
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
        {
            return refuse("is not a path within its directory");
        }
        let path = at.join(&name);
        // The tree's root is each build's context. Docker reads its ignore
        // rules there, from `.dockerignore`, or from the ignore file beside
        // the Dockerfile, which `images` writes there: none of the
        // sources' applies, so no rule of theirs, such as `target/`, can
        // leave the bundle out.
        if [".dockerignore", "Dockerfile", "Dockerfile.dockerignore"]
            .iter()
            .any(|reserved| path == Path::new(reserved))
        {
            continue;
        }
        // Nothing is written through a link: each directory on the way is
        // made here, or is a directory already.
        let parent = match kind.is_dir() {
            true => path.as_path(),
            false => path.parent().unwrap_or(Path::new("")),
        };
        if let Err(link) = directories(dir, parent).map_err(failed)? {
            return refuse(&format!(
                "is below {}, which is not a directory",
                context.root().join(link).display()
            ));
        }
        if kind.is_dir() {
            continue;
        }
        // The bundle is written through each directory on its path, so
        // none of them may be a file or a link of the sources.
        if context.bundle().starts_with(&path) {
            return refuse(&format!(
                "is on the bundle's path, {}, and the bundle is written through no link, which could lead outside the tree",
                context.bundle().display()
            ));
        }
        if path.starts_with(context.bundle()) {
            return refuse(&format!(
                "is where the bundle goes, {}",
                context.bundle().display()
            ));
        }
        if entries.contains_key(&path) {
            return refuse("is in two of the source directories");
        }
        let to = dir.join(&path);
        let mode = entry.header().mode().map_err(failed)?;
        let recorded = if kind.is_symlink() {
            let target = entry
                .link_name_bytes()
                .map(|target| PathBuf::from(OsStr::from_bytes(&target).to_os_string()))
                .unwrap_or_default();
            if target.is_absolute() {
                return refuse(&format!(
                    "is a link to an absolute path, {}",
                    target.display()
                ));
            }
            let named = normalize(&path.parent().unwrap_or(Path::new("")).join(&target));
            if named.starts_with("..") {
                return refuse(&format!(
                    "is a link to {}, outside the sources",
                    target.display()
                ));
            }
            std::os::unix::fs::symlink(&target, &to).map_err(failed)?;
            Entry {
                kind: Kind::Link,
                mode: 0o777,
                body: Sha256::digest(target.as_os_str().as_bytes()).into(),
            }
        } else if kind.is_file() {
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut bytes).map_err(failed)?;
            // Git keeps only whether a file is executable.
            let mode = if mode & 0o111 != 0 { 0o755 } else { 0o644 };
            std::fs::write(&to, &bytes).map_err(failed)?;
            std::fs::set_permissions(&to, std::os::unix::fs::PermissionsExt::from_mode(mode))
                .map_err(failed)?;
            Entry {
                kind: Kind::File,
                mode,
                body: Sha256::digest(&bytes).into(),
            }
        } else {
            return refuse("is neither a file, a directory, nor a link");
        };
        entries.insert(path, recorded);
    }
    Ok(())
}

/// The commit of the repository in `top` that its sources are archived
/// from: the one `git stash create` makes of the changes to tracked files,
/// or `HEAD` when there are none. Untracked files are in neither: `git
/// add` a new file to build it.
fn commit(context: &Context, top: &Path) -> Result<OsString, Error> {
    // `git stash create` writes the index it refreshes, under its lock,
    // which another command, such as an editor's `git status`, can hold:
    // it runs on a copy, and the checkout's index is never written.
    let index = git(context, top, &["rev-parse", "--git-path", "index"])?;
    let index = top.join(OsStr::from_bytes(trim(&index)));
    let copy = tempfile::Builder::new()
        .prefix("gdl-index-")
        .tempfile()
        .map_err(super::io)?;
    let copied = match std::fs::copy(&index, copy.path()) {
        Ok(_) => Some(copy.path()),
        // A checkout with no index yet has nothing to copy.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            return Err(Error::new(format!(
                "{}: {}: {e}",
                context.prefix(),
                index.display()
            )));
        }
    };
    // `git stash create` first asks whether tracked files changed without
    // refreshing the index, so a file whose stat changed and whose content
    // did not, as after a copy that stamps every file with a new time,
    // counts as changed. It then refreshes, finds nothing to save, and
    // exits 1 with no word on either stream. Refreshed first, the copy
    // answers the first question as the second does. `--unmerged` leaves
    // a conflict for `stash create` to report; `-q` reports no change.
    if let Some(copied) = copied {
        run(
            context,
            top,
            Some(copied),
            &["update-index", "-q", "--unmerged", "--refresh"],
        )?;
    }
    // The commit is never stored under a ref, so whose it is and whether
    // it is signed do not matter; the host's settings for them must not
    // stop the build.
    let stash = run(
        context,
        top,
        copied,
        &[
            "-c",
            "user.name=gdl",
            "-c",
            "user.email=gdl@localhost",
            "-c",
            "commit.gpgsign=false",
            "stash",
            "create",
        ],
    )?;
    let stash = trim(&stash);
    if !stash.is_empty() {
        return Ok(OsStr::from_bytes(stash).to_os_string());
    }
    let head = git(context, top, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    Ok(OsStr::from_bytes(trim(&head)).to_os_string())
}

/// Refuse a link of the tree in `dir` that the file system resolves
/// outside it. Each link's own target is within it, but a link can reach
/// through another: in `a/b`, `x -> ../..` names the root, and `y ->
/// x/../..` reads as `a`, but resolves two levels above the root. A link
/// that names nothing reads nothing; one that names a place the bundle
/// makes resolves once it is written, so the check runs on the built tree.
/// Cargo resolves the segments' locks in this tree, so it must not reach
/// this machine's files through one.
fn links(dir: &Path, context: &Context, entries: &BTreeMap<PathBuf, Entry>) -> Result<(), Error> {
    let root = dir.canonicalize().map_err(super::io)?;
    for (path, entry) in entries {
        if entry.kind != Kind::Link {
            continue;
        }
        match dir.join(path).canonicalize() {
            Ok(real) if real.starts_with(&root) => {}
            // A link to nothing reads nothing.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                return Err(Error::new(format!(
                    "{}: {} is a link that resolves outside the sources, through another link, so `--local` builds no image from these sources",
                    context.prefix(),
                    context.root().join(path).display()
                )));
            }
        }
    }
    Ok(())
}

/// Refuse the manifest at `path` in the tree in `dir` when it names a path
/// by an absolute path, or one that leaves the tree: Cargo would read this
/// machine's sources there when it settles a lock, and the image would not
/// hold them. A manifest Cargo cannot read is left to Cargo, which refuses
/// it in the build with its own words.
fn manifest(dir: &Path, context: &Context, path: &Path) -> Result<(), Error> {
    let Some(table) = table(dir, path) else {
        return Ok(());
    };
    let mut named = Vec::new();
    paths(&table, &mut named);
    if let Some(workspace) = table
        .get("package")
        .and_then(|package| package.get("workspace"))
        .and_then(toml::Value::as_str)
    {
        named.push(workspace.to_string());
    }
    inside(context, path, path.parent().unwrap_or(Path::new("")), named)
}

/// Refuse any Cargo configuration that a build of the sources reads, but
/// one whose only key is `[net] git-fetch-with-cli`: one that the archive
/// holds, at any depth, one in a directory between a source and the
/// context's root, which Cargo on this machine reads, and one above the
/// tree in `dir`, which Cargo would read where it settles the locks. The
/// image build reads only the configuration the generator writes, which is
/// none, so a build with such a file would quietly differ from this
/// checkout's. The one key is accepted, and the file left out of the tree:
/// a local build fetches no gdl over Git, so it changes nothing an image
/// builds. Cargo's own home configuration is read in each build on this
/// machine, and in none in an image.
fn configuration(
    dir: &Path,
    context: &Context,
    entries: &mut BTreeMap<PathBuf, Entry>,
) -> Result<(), Error> {
    let named = |path: &Path| {
        path.parent().and_then(Path::file_name) == Some(OsStr::new(".cargo"))
            && CONFIG
                .iter()
                .any(|name| path.file_name() == Some(OsStr::new(name)))
    };
    let archived: Vec<PathBuf> = entries.keys().filter(|path| named(path)).cloned().collect();
    for path in &archived {
        let file = context.root().join(path);
        // Read only as a file of the tree: a link could name anything.
        if entries[path].kind != Kind::File {
            return Err(refuse_configuration(context, &file, "is a link"));
        }
        let text = std::fs::read_to_string(dir.join(path)).map_err(super::io)?;
        accepted(context, &file, &text)?;
        std::fs::remove_file(dir.join(path)).map_err(super::io)?;
        entries.remove(path);
    }
    let home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| Path::new(&home).join(".cargo")));
    let found = |above: &Path| -> Vec<PathBuf> {
        CONFIG
            .iter()
            .map(|name| above.join(".cargo").join(name))
            .filter(|file| home.as_deref() != file.parent())
            .filter(|file| std::fs::symlink_metadata(file).is_ok())
            .collect()
    };
    let mut host: Vec<PathBuf> = context
        .sources()
        .iter()
        .flat_map(|source| {
            source
                .ancestors()
                .take_while(|above| above.starts_with(context.root()))
                .flat_map(found)
                .collect::<Vec<_>>()
        })
        .chain(dir.ancestors().flat_map(found))
        .collect();
    host.sort();
    host.dedup();
    for file in host {
        let text = std::fs::read_to_string(&file)
            .map_err(|e| refuse_configuration(context, &file, &format!("cannot be read: {e}")))?;
        accepted(context, &file, &text)?;
    }
    Ok(())
}

/// Refuse the configuration `text` of `file` unless its only key is `[net]
/// git-fetch-with-cli`, a boolean, naming the first other key.
fn accepted(context: &Context, file: &Path, text: &str) -> Result<(), Error> {
    let table: toml::Table = text
        .parse()
        .map_err(|_| refuse_configuration(context, file, "does not parse as TOML"))?;
    for (key, value) in &table {
        let toml::Value::Table(net) = value else {
            return Err(refuse_configuration(
                context,
                file,
                &format!("sets `{key}`"),
            ));
        };
        if key != "net" {
            return Err(refuse_configuration(
                context,
                file,
                &format!("sets `{key}`"),
            ));
        }
        for (inner, value) in net {
            if inner != "git-fetch-with-cli" || !value.is_bool() {
                return Err(refuse_configuration(
                    context,
                    file,
                    &format!("sets `net.{inner}`"),
                ));
            }
        }
    }
    Ok(())
}

/// The refusal of the Cargo configuration in `file`, which `why`.
fn refuse_configuration(context: &Context, file: &Path, why: &str) -> Error {
    Error::new(format!(
        "{}: {} is Cargo configuration that {why}, and `--local` builds with none but `[net] git-fetch-with-cli`: the image build reads none, so the build would differ from this machine's. Remove it, or build without `--local`",
        context.prefix(),
        file.display()
    ))
}

/// The names Cargo reads its configuration from in a `.cargo` directory.
const CONFIG: [&str; 2] = ["config.toml", "config"];

/// The TOML table of the file at `path` in the tree in `dir`, when it is
/// one.
fn table(dir: &Path, path: &Path) -> Option<toml::Table> {
    std::fs::read_to_string(dir.join(path)).ok()?.parse().ok()
}

/// Refuse each of `named`, the paths the file at `path` in the tree names
/// from `here`, that is absolute or leaves the tree.
fn inside(context: &Context, path: &Path, here: &Path, named: Vec<String>) -> Result<(), Error> {
    for name in named {
        let why = if Path::new(&name).is_absolute() {
            "by an absolute path"
        } else if normalize(&here.join(&name)).starts_with("..") {
            "by a path that leaves them"
        } else {
            continue;
        };
        return Err(Error::new(format!(
            "{}: {} names {name} {why}, outside the sources the images build from: name it by a relative path within them",
            context.prefix(),
            context.root().join(path).display()
        )));
    }
    Ok(())
}

/// Each `path` string in `table`, at any depth.
fn paths(table: &toml::Table, named: &mut Vec<String>) {
    for (key, value) in table {
        match value {
            toml::Value::String(text) if key == "path" => named.push(text.clone()),
            toml::Value::Table(inner) => paths(inner, named),
            toml::Value::Array(items) => {
                for item in items {
                    if let toml::Value::Table(inner) = item {
                        paths(inner, named);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Record in `entries` each file under `dir`, the bundle, whose path in the
/// tree is `at`: what the generator and Cargo wrote.
fn generated(dir: &Path, at: &Path, entries: &mut BTreeMap<PathBuf, Entry>) -> Result<(), Error> {
    let failed = |e: std::io::Error| Error::new(format!("`--local`: {}: {e}", dir.display()));
    for found in std::fs::read_dir(dir).map_err(failed)? {
        let found = found.map_err(failed)?;
        let name = found.file_name();
        if found.file_type().map_err(failed)?.is_dir() {
            generated(&found.path(), &at.join(&name), entries)?;
        } else {
            let bytes = std::fs::read(found.path()).map_err(failed)?;
            entries.insert(
                at.join(&name),
                Entry {
                    kind: Kind::Generated,
                    mode: 0o644,
                    body: Sha256::digest(&bytes).into(),
                },
            );
        }
    }
    Ok(())
}

/// The sha256, as hex, of the tree's `entries`, in path order: for each,
/// its kind, its mode, its path's bytes, and the sha256 of its content, or
/// of its target for a link, each with its length. Every entry has these
/// four fields, so no two trees read alike: a file and a link of the same
/// bytes differ in kind. No commit's id or time is in it, so the same
/// sources give the same digest wherever they are and whenever they are
/// archived.
fn digest(entries: &BTreeMap<PathBuf, Entry>) -> String {
    let mut hash = Sha256::new();
    let mut field = |bytes: &[u8]| {
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    };
    field(b"gdl local sources");
    for (path, entry) in entries {
        field(entry.kind.name());
        field(format!("{:o}", entry.mode).as_bytes());
        field(path.as_os_str().as_bytes());
        field(&entry.body);
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// What `git <args>` prints in `dir`, as bytes. Refused, naming `dir` and
/// Git's words, when Git fails: the sources are a trust boundary, and
/// nothing falls back to a copy Git did not make.
fn git<S: AsRef<OsStr>>(context: &Context, dir: &Path, args: &[S]) -> Result<Vec<u8>, Error> {
    run(context, dir, None, args)
}

/// [`git`], with the index in `index` when it names one.
fn run<S: AsRef<OsStr>>(
    context: &Context,
    dir: &Path,
    index: Option<&Path>,
    args: &[S],
) -> Result<Vec<u8>, Error> {
    let prefix = context.prefix();
    let mut command = std::process::Command::new("git");
    command.arg("-C").arg(dir).args(args);
    if let Some(index) = index {
        command.env("GIT_INDEX_FILE", index);
    }
    let output = command
        .output()
        .map_err(|e| Error::new(format!("{prefix}: running git in {}: {e}", dir.display())))?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "{prefix}: git cannot archive the sources in {}, and `--local` builds only what Git holds: {}",
            dir.display(),
            failure(dir, index, args, &output)
        )));
    }
    Ok(output.stdout)
}

/// Which Git command failed, how it ended, and what it wrote to stderr,
/// "(empty)" when nothing: Git can fail without a word, and Git's words
/// alone then say nothing.
fn failure<S: AsRef<OsStr>>(
    dir: &Path,
    index: Option<&Path>,
    args: &[S],
    output: &std::process::Output,
) -> String {
    use std::os::unix::process::ExitStatusExt as _;
    let command = [OsStr::new("-C"), dir.as_os_str()]
        .into_iter()
        .chain(args.iter().map(AsRef::as_ref))
        .map(OsStr::to_string_lossy)
        .collect::<Vec<_>>()
        .join(" ");
    let index = index
        .map(|index| format!(" with GIT_INDEX_FILE={}", index.display()))
        .unwrap_or_default();
    let ended = match (output.status.code(), output.status.signal()) {
        (Some(code), _) => format!("exited with code {code}"),
        (None, Some(signal)) => format!("was killed by signal {signal}"),
        (None, None) => output.status.to_string(),
    };
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = match stderr.trim() {
        "" => "(empty)",
        stderr => stderr,
    };
    format!("`git {command}`{index} {ended}, stderr: {stderr}")
}

/// `bytes` without the line break Git ends its output with.
fn trim(bytes: &[u8]) -> &[u8] {
    bytes.strip_suffix(b"\n").unwrap_or(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::context::Mode;

    /// A file and a link of the same bytes, and a file whose content reads
    /// like another's path, give different digests.
    #[test]
    fn a_digest_tells_each_kind_and_field_apart() {
        let entry = |kind, body: &[u8]| Entry {
            kind,
            mode: 0o644,
            body: Sha256::digest(body).into(),
        };
        let one = |path: &str, kind, body: &[u8]| {
            digest(&BTreeMap::from([(PathBuf::from(path), entry(kind, body))]))
        };
        let file = one("a", Kind::File, b"b");
        assert_ne!(file, one("a", Kind::Link, b"b"));
        assert_ne!(file, one("a", Kind::Generated, b"b"));
        assert_ne!(file, one("ab", Kind::File, b""));
        let mut executable = entry(Kind::File, b"b");
        executable.mode = 0o755;
        assert_ne!(
            file,
            digest(&BTreeMap::from([(PathBuf::from("a"), executable)]))
        );
    }

    /// A Git checkout that Git refuses to list, here for its ownership,
    /// stops the build with Git's error: nothing falls back to a plain
    /// copy, which would admit what the checkout ignores.
    ///
    /// Git reads the ownership override from the process environment, and
    /// setting it here would reach the other tests of this binary, so the
    /// case runs again as a child that has it. The child is cheap because
    /// the test calls [`build`], where every builder reaches Git, on a
    /// fixture of one small crate: `images` and `compose` get there only
    /// after they generate and resolve a whole bundle, which costs seconds.
    #[test]
    fn a_git_error_does_not_admit_ignored_credentials() {
        const WORKER: &str = "GDL_TREE_GIT_WORKER";
        if std::env::var_os(WORKER).is_none() {
            let output = std::process::Command::new("timeout")
                .arg("10")
                .arg(std::env::current_exe().expect("the test binary"))
                .args([
                    "--exact",
                    "deployment::tree::tests::a_git_error_does_not_admit_ignored_credentials",
                    "--nocapture",
                ])
                .env(WORKER, "1")
                .env("GIT_TEST_ASSUME_DIFFERENT_OWNER", "1")
                .output()
                .expect("an isolated Git ownership check");
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            // The child ran the case itself, not none.
            assert!(stdout.contains("1 passed"), "{stdout}");
            return;
        }
        let git = |dir: &Path, args: &[&str], owned: bool| {
            let mut command = std::process::Command::new("git");
            command
                .arg("-C")
                .arg(dir)
                .args(["-c", "user.name=gdl", "-c", "user.email=gdl@example.com"])
                .args(["-c", "commit.gpgsign=false"])
                .args(args);
            if owned {
                command.env_remove("GIT_TEST_ASSUME_DIFFERENT_OWNER");
            }
            command.output().expect("git runs")
        };
        // Under one directory of the system's temporary directory, so that
        // the context's root is not `/` and Git finds no repository above.
        let temp = tempfile::Builder::new()
            .prefix("gdl-tree-git-")
            .tempdir()
            .expect("a directory");
        let root = temp.path().canonicalize().expect("the path");
        let node = root.join("node");
        std::fs::create_dir_all(node.join("src")).expect("a crate");
        std::fs::write(node.join("src/lib.rs"), "").expect("a library");
        std::fs::write(
            node.join("Cargo.toml"),
            "[package]\nname = \"node\"\nversion = \"1.0.0\"\nedition = \"2024\"\n",
        )
        .expect("a manifest");
        std::fs::write(node.join(".gitignore"), "/ignored-credentials.txt\n")
            .expect("an ignore rule");
        for args in [
            &["init", "-q"][..],
            &["add", "-A"][..],
            &["commit", "-q", "-m", "node"][..],
        ] {
            let output = git(&node, args, true);
            assert!(output.status.success(), "{output:?}");
        }
        let marker = ["ignored", "credential", "marker"].join("-");
        std::fs::write(node.join("ignored-credentials.txt"), &marker)
            .expect("an ignored synthetic credential");
        // The fixture is what the case says it is: Git refuses to list it
        // for its ownership, and ignores the file it would otherwise skip.
        let listed = git(
            &node,
            &["ls-files", "--others", "--exclude-standard"],
            false,
        );
        assert!(!listed.status.success());
        assert!(String::from_utf8_lossy(&listed.stderr).contains("dubious ownership"));
        let ignored = git(
            &node,
            &["check-ignore", "--quiet", "ignored-credentials.txt"],
            true,
        );
        assert!(ignored.status.success());

        let files = Files::from([(
            PathBuf::from("main/Cargo.toml"),
            "[package]\nname = \"segment\"\nversion = \"1.0.0\"\n\
             [dependencies]\nnode = { path = \"../../node\" }\n"
                .to_string(),
        )]);
        let context =
            Context::find(&root.join("bundle"), &files, None, Mode::Image).expect("the context");
        let tree = root.join("tree");
        std::fs::create_dir(&tree).expect("a tree");
        let built = build(&tree, &context, &files, &[]);
        // Whatever the build returned, nothing it wrote holds the secret.
        let mut found = Vec::new();
        let mut pending = vec![tree.clone()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).expect("a directory") {
                let path = entry.expect("an entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if std::fs::read(&path)
                    .expect("a file")
                    .starts_with(marker.as_bytes())
                {
                    found.push(path);
                }
            }
        }
        assert!(found.is_empty(), "{found:?} hold an ignored file");
        let error = built.expect_err("Git's refusal stops the build");
        assert!(error.to_string().contains("dubious ownership"), "{error}");
    }

    /// Git in `dir`, as the fixtures run it: with an identity, unsigned,
    /// and asserted to succeed.
    fn fixture_git(dir: &Path, args: &[&str]) -> Vec<u8> {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["-c", "user.name=gdl", "-c", "user.email=gdl@example.com"])
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("git runs");
        assert!(output.status.success(), "git {args:?}: {output:?}");
        output.stdout
    }

    /// A context in `root` whose one source is the crate `root/node`, a
    /// Git checkout of one commit, and the bundle files that name it.
    fn fixture(root: &Path) -> (Context, Files) {
        let node = root.join("node");
        std::fs::create_dir_all(node.join("src")).expect("a crate");
        std::fs::write(node.join("src/lib.rs"), "pub fn f() {}\n").expect("a library");
        std::fs::write(
            node.join("Cargo.toml"),
            "[package]\nname = \"node\"\nversion = \"1.0.0\"\nedition = \"2024\"\n",
        )
        .expect("a manifest");
        fixture_git(&node, &["init", "-q"]);
        fixture_git(&node, &["add", "-A"]);
        fixture_git(&node, &["commit", "-q", "-m", "node"]);
        let files = Files::from([(
            PathBuf::from("main/Cargo.toml"),
            "[package]\nname = \"segment\"\nversion = \"1.0.0\"\n\
             [dependencies]\nnode = { path = \"../../node\" }\n"
                .to_string(),
        )]);
        let context =
            Context::find(&root.join("bundle"), &files, None, Mode::Image).expect("the context");
        (context, files)
    }

    /// Set the modification time of every file Git tracks in `dir` to
    /// `when`, as a copy that stamps each file with the time of the copy
    /// does: no content changes, but the index's record of each file's
    /// stat no longer matches.
    fn touch(dir: &Path, when: std::time::SystemTime) {
        let listed = fixture_git(dir, &["ls-files", "-z"]);
        for name in listed.split(|b| *b == 0).filter(|n| !n.is_empty()) {
            std::fs::File::options()
                .write(true)
                .open(dir.join(OsStr::from_bytes(name)))
                .and_then(|file| file.set_modified(when))
                .expect("a tracked file");
        }
    }

    /// A git failure names the command, how it ended, and its stderr, even
    /// when Git wrote none: a bare "Git holds:" followed by nothing told no
    /// one which step failed or why.
    ///
    /// The stand-in `git` is found through `PATH`, which reaches every
    /// test of this binary if set here, so the case runs again as a child
    /// that has it.
    #[test]
    fn a_silent_git_failure_names_its_command_exit_and_empty_stderr() {
        const WORKER: &str = "GDL_TREE_SILENT_GIT_WORKER";
        let Some(stand_in) = std::env::var_os(WORKER) else {
            let temp = tempfile::Builder::new()
                .prefix("gdl-tree-silent-")
                .tempdir()
                .expect("a directory");
            let bin = temp.path().join("bin");
            std::fs::create_dir(&bin).expect("a directory");
            // Silent: no output on either stream, exit 1, or a signal when
            // asked for one.
            std::fs::write(
                bin.join("git"),
                "#!/bin/sh\ncase \"$*\" in *signal*) kill -TERM $$ ;; esac\nexit 1\n",
            )
            .expect("a stand-in git");
            std::fs::set_permissions(
                bin.join("git"),
                std::os::unix::fs::PermissionsExt::from_mode(0o755),
            )
            .expect("an executable");
            let mut path = std::ffi::OsString::from(&bin);
            path.push(":");
            path.push(std::env::var_os("PATH").unwrap_or_default());
            let output = std::process::Command::new("timeout")
                .arg("10")
                .arg(std::env::current_exe().expect("the test binary"))
                .args([
                    "--exact",
                    "deployment::tree::tests::a_silent_git_failure_names_its_command_exit_and_empty_stderr",
                    "--nocapture",
                ])
                .env(WORKER, temp.path())
                .env("PATH", path)
                .stdin(std::process::Stdio::null())
                .output()
                .expect("a child with the stand-in git");
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(stdout.contains("1 passed"), "{stdout}");
            return;
        };
        let dir = PathBuf::from(stand_in);
        let files = Files::from([(
            PathBuf::from("main/Cargo.toml"),
            "[package]\nname = \"segment\"\nversion = \"1.0.0\"\n".to_string(),
        )]);
        let context =
            Context::find(&dir.join("bundle"), &files, None, Mode::Image).expect("the context");
        let error = git(&context, &dir, &["rev-parse", "--show-toplevel"])
            .expect_err("the stand-in fails")
            .to_string();
        let command = format!("`git -C {} rev-parse --show-toplevel`", dir.display());
        assert!(error.contains(&command), "{error}");
        assert!(error.contains("exited with code 1"), "{error}");
        assert!(error.ends_with("stderr: (empty)"), "{error}");
        let error = run(&context, &dir, Some(Path::new("/i")), &["signal"])
            .expect_err("the stand-in is killed")
            .to_string();
        assert!(error.contains("with GIT_INDEX_FILE=/i"), "{error}");
        assert!(error.contains("killed by signal 15"), "{error}");
    }

    /// A checkout whose tracked files were all stamped with a new time,
    /// and none changed, archives `HEAD`; one whose file changed too
    /// archives the change. The checkout's own index is never written.
    ///
    /// Before the copy of the index was refreshed, `git stash create`
    /// failed here every time, with exit 1 and nothing on either stream:
    /// it first asks, without a refresh, whether tracked files changed,
    /// and stat alone says yes; it then refreshes and asks again, finds
    /// nothing to save, and exits 1.
    #[test]
    fn a_checkout_with_every_file_touched_archives() {
        let temp = tempfile::Builder::new()
            .prefix("gdl-tree-touched-")
            .tempdir()
            .expect("a directory");
        let root = temp.path().canonicalize().expect("the path");
        let (context, files) = fixture(&root);
        let node = root.join("node");
        let index = node.join(".git/index");
        let before = std::fs::read(&index).expect("the index");
        touch(
            &node,
            std::time::SystemTime::now() + std::time::Duration::from_secs(10),
        );
        let tree = root.join("tree");
        std::fs::create_dir(&tree).expect("a tree");
        build(&tree, &context, &files, &[]).expect("the touched checkout archives");
        assert_eq!(
            std::fs::read_to_string(tree.join("node/src/lib.rs")).expect("the library"),
            "pub fn f() {}\n"
        );
        std::fs::write(node.join("src/lib.rs"), "pub fn g() {}\n").expect("a change");
        let changed = root.join("changed");
        std::fs::create_dir(&changed).expect("a tree");
        build(&changed, &context, &files, &[]).expect("the changed checkout archives");
        assert_eq!(
            std::fs::read_to_string(changed.join("node/src/lib.rs")).expect("the library"),
            "pub fn g() {}\n"
        );
        assert_eq!(std::fs::read(&index).expect("the index"), before);
    }

    /// The capture holds while another process refreshes the checkout's
    /// index, which takes `index.lock`, as an editor's `git status` does:
    /// the capture reads the index once, through a copy, and takes no lock
    /// of the checkout's. Each round stamps every tracked file anew, so
    /// the copy is stale whenever the refresher has not caught up.
    ///
    /// Rounds run until about two seconds have passed, and at least ten:
    /// the refresher must overlap many captures for the absence of a race
    /// to mean anything.
    #[test]
    fn a_capture_holds_under_a_concurrent_index_refresh() {
        let temp = tempfile::Builder::new()
            .prefix("gdl-tree-refresh-")
            .tempdir()
            .expect("a directory");
        let root = temp.path().canonicalize().expect("the path");
        let (context, files) = fixture(&root);
        let node = root.join("node");
        let stop = std::sync::atomic::AtomicBool::new(false);
        let (rounds, refreshes) = std::thread::scope(|scope| {
            let refresher = scope.spawn(|| {
                let mut refreshes = 0u32;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let args: &[&str] = match refreshes % 2 {
                        0 => &["update-index", "-q", "--refresh"],
                        _ => &["status", "--porcelain"],
                    };
                    // Either may lose the lock to the other's last run.
                    let _ = std::process::Command::new("git")
                        .arg("-C")
                        .arg(&node)
                        .args(args)
                        .stdin(std::process::Stdio::null())
                        .output();
                    refreshes += 1;
                }
                refreshes
            });
            // Stops the refresher on any exit, a panic included, so that
            // the scope's join cannot wait on it forever.
            struct Stop<'a>(&'a std::sync::atomic::AtomicBool);
            impl Drop for Stop<'_> {
                fn drop(&mut self) {
                    self.0.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
            let guard = Stop(&stop);
            let started = std::time::Instant::now();
            let mut rounds = 0u32;
            let outcome = loop {
                if rounds >= 10 && started.elapsed() > std::time::Duration::from_secs(2) {
                    break Ok(rounds);
                }
                touch(
                    &node,
                    std::time::SystemTime::now() + std::time::Duration::from_secs(rounds.into()),
                );
                let tree = root.join(format!("tree-{rounds}"));
                std::fs::create_dir(&tree).expect("a tree");
                if let Err(error) = build(&tree, &context, &files, &[]) {
                    break Err(format!("round {rounds}: {error}"));
                }
                rounds += 1;
            };
            drop(guard);
            let refreshes = refresher.join().expect("the refresher");
            (outcome.unwrap_or_else(|e| panic!("{e}")), refreshes)
        });
        assert!(rounds >= 10 && refreshes > 0, "{rounds} {refreshes}");
    }
}
