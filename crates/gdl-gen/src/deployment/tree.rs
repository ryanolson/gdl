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
    configuration(dir, context, &entries)?;
    // A manifest that names a directory outside the sources says why Git
    // cannot place it: that refusal comes first.
    if let Some(error) = unplaced {
        return Err(error);
    }
    let seed = seed(context, &entries)?;
    let bundle = dir.join(context.bundle());
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
        if kind.is_dir() {
            std::fs::create_dir_all(dir.join(&path)).map_err(failed)?;
            continue;
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
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent).map_err(failed)?;
        }
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

/// Refuse any Cargo configuration that a build of the sources reads: one
/// that the archive holds, at any depth, one in a directory between a
/// source and the context's root, which Cargo on this machine reads, and
/// one above the tree in `dir`, which Cargo would read where it settles the
/// locks. The image build reads only the configuration the generator
/// writes, which is none, so a build with such a file would quietly differ
/// from this checkout's. Cargo's own home configuration is read in each
/// build on this machine, and in none in an image.
fn configuration(
    dir: &Path,
    context: &Context,
    entries: &BTreeMap<PathBuf, Entry>,
) -> Result<(), Error> {
    let named = |path: &Path| {
        path.parent().and_then(Path::file_name) == Some(OsStr::new(".cargo"))
            && CONFIG
                .iter()
                .any(|name| path.file_name() == Some(OsStr::new(name)))
    };
    let archived = entries
        .keys()
        .find(|path| named(path))
        .map(|path| context.root().join(path));
    let home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| Path::new(&home).join(".cargo")));
    let read = |above: &Path| {
        CONFIG
            .iter()
            .map(|name| above.join(".cargo").join(name))
            .filter(|file| home.as_deref() != file.parent())
            .find(|file| std::fs::symlink_metadata(file).is_ok())
    };
    let between = context.sources().iter().find_map(|source| {
        source
            .ancestors()
            .take_while(|above| above.starts_with(context.root()))
            .find_map(read)
    });
    let settling = dir.ancestors().find_map(read);
    match archived.or(between).or(settling) {
        None => Ok(()),
        Some(file) => Err(Error::new(format!(
            "{}: {} is Cargo configuration, which `--local` does not build with: the image build reads none, so the build would differ from this machine's. Remove it, or build without `--local`",
            context.prefix(),
            file.display()
        ))),
    }
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
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output.stdout)
}

/// `bytes` without the line break Git ends its output with.
fn trim(bytes: &[u8]) -> &[u8] {
    bytes.strip_suffix(b"\n").unwrap_or(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
