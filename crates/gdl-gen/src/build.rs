//! `cargo gdl build`: every segment of a spec generated as one Cargo
//! workspace in the target directory, built there, and its binaries copied
//! out. The source tree is never written.
//!
//! ```text
//! <target>/gdl/build.lock     held for the whole run, by each build here
//! <target>/gdl/<graph>/<spec stem>/
//!   seed.lock        the lock the workspace's lock was last seeded from
//!   lock-inputs      what the lock was last settled from, hashed
//!   binaries.toml    each binary copied out, by destination
//!   bin/<profile>/   the binaries, unless `--out` says otherwise
//!   workspace/       the generated workspace, owned by the generator
//! ```
//!
//! Every run renders every file, and writes only those whose text changed,
//! so that Cargo rebuilds and relinks only the segments a change reaches.
//! Removing every file and writing it again would relink every segment,
//! the expensive part of a release build. See
//! `docs/src/concepts/generation.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::deployment::context::{Context, HostPatch, Mode};
use crate::deployment::locks;
use crate::dump::toml_string;
use crate::emit::{Deps, MARKER};
use crate::paths::display;
use crate::spec::{GdlDep, Source};
use crate::{Error, Files, Offers, Spec};

/// The stamp that says the generator made the workspace directory.
const STAMP: &str = ".gdl-generated";

/// How to build.
#[derive(Debug, Clone, Default)]
pub struct Build {
    /// Where to copy the binaries: `bin/<profile>` in the build's directory
    /// when `None`.
    pub out: Option<PathBuf>,
    /// Build with the release profile: one codegen unit and fat LTO.
    pub release: bool,
}

/// Generate every segment of `spec` into a workspace under `target`, build
/// them with Cargo in `target`, and copy each segment's binary out. Returns
/// the copied binaries' paths, in name order.
///
/// Errors: a problem of the spec; a workspace directory that holds files
/// but not the generator's stamp; a file in a generated crate that the
/// generator did not write; a failed build.
pub fn build(spec: &Spec, target: &Path, how: &Build) -> Result<Vec<PathBuf>, Error> {
    let target = crate::dump::absolute(target)?;
    let graph = graph_name(spec, &target)?;
    let dir = build_dir(spec, &graph, &target);
    let workspace = dir.join("workspace");
    let profile = if how.release { "release" } else { "debug" };
    let default_out = dir.join("bin").join(profile);
    unlinked(&target, &workspace)?;
    unlinked(&target, &default_out)?;
    let _lock = lock(&target.join("gdl"))?;
    create_dir(&dir)?;
    owned(&workspace)?;

    let (mut files, rendered) =
        crate::render_with(spec, &workspace, &target, Deps::Build, Offers::Read)?;
    debug_assert_eq!(rendered, graph);
    let members = members(&files);
    let runtime = match &spec.gdl {
        GdlDep::Path(runtime) => Some(runtime.as_path()),
        GdlDep::Version(_) | GdlDep::Git { .. } => None,
    };
    let cwd = crate::dump::absolute(spec.dir())?;
    let context = Context::find(
        &workspace,
        &files,
        runtime,
        Mode::Build { cwd: cwd.clone() },
    )?;
    let patch = context.patch(&files, &members, Path::new(""))?;
    files.insert(
        PathBuf::from("Cargo.toml"),
        root_manifest(spec, &members, &patch),
    );
    files.insert(
        PathBuf::from(STAMP),
        format!("{MARKER}: `cargo gdl build` made this directory.\n"),
    );

    for stale in crate::write(&workspace, &files)? {
        remove_empty(&workspace, &stale);
    }
    let (seed, seeded) = seed(spec, &dir, &workspace, &context)?;
    let carried = context.host_patches(&files, &members);
    let inputs = lock_inputs(seed.as_deref(), &files, &carried);
    let settled = dir.join("lock-inputs");
    if seeded || std::fs::read_to_string(&settled).ok().as_deref() != Some(inputs.as_str()) {
        locks::settle(&workspace, &carried, &context)?;
        crate::dump::write(&settled, &inputs)?;
    }

    let packages = segment_packages(spec, &graph);
    let built = cargo_build(&workspace, &target, &cwd, how.release, &packages)?;
    let out = match &how.out {
        Some(out) => out.clone(),
        None => default_out,
    };
    copy_out(&dir.join("binaries.toml"), &out, &built)
}

/// The directory of the build of `spec`, whose graph is `graph`, in
/// `target`: by graph and by the spec's file stem, since several specs can
/// place one graph, and each would mark the other's crates stale.
pub(crate) fn build_dir(spec: &Spec, graph: &str, target: &Path) -> PathBuf {
    let stem = spec
        .file
        .file_stem()
        .map_or_else(|| "gdl".into(), |stem| stem.to_string_lossy().into_owned());
    target.join("gdl").join(graph).join(stem)
}

/// The graph's name, which names the build's directory and its packages:
/// a graph file's is read without resolving its messages; a contract
/// crate's comes from its manifest, as rendering gets it.
fn graph_name(spec: &Spec, target: &Path) -> Result<String, Error> {
    match &spec.source {
        Source::Graph(source) => {
            let file = match &source.text {
                Some(text) => crate::names::GraphFile::parse(text, &source.path)?,
                None => crate::names::GraphFile::read(&source.path)?,
            };
            Ok(file.graph.name.value())
        }
        Source::Contract(_) => Ok(
            crate::prepare(spec, None, target, Deps::Build, Offers::Read)?
                .manifest
                .name,
        ),
    }
}

/// Refuse a symbolic link at any directory of `path` below `target`: the
/// generator makes these directories, and never as links, and writing
/// through one would write wherever it points. The target directory
/// itself may be a link.
fn unlinked(target: &Path, path: &Path) -> Result<(), Error> {
    let Ok(below) = path.strip_prefix(target) else {
        return Ok(());
    };
    let mut dir = target.to_path_buf();
    for part in below.components() {
        dir.push(part);
        match std::fs::symlink_metadata(&dir) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Error::new(format!(
                    "{} is a symbolic link, which `cargo gdl build` does not make; remove it, or build into another target directory",
                    dir.display()
                )));
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io(&dir, e)),
        }
    }
    Ok(())
}

/// Refuse a workspace directory that holds files but not the stamp: it is
/// not the generator's, and a stale file there could be someone's. Refuse
/// a symbolic link in it, or the directory as one: the generator never
/// makes one, and writing through it would write wherever it points.
fn owned(workspace: &Path) -> Result<(), Error> {
    let link = |path: &Path| {
        Error::new(format!(
            "{} is a symbolic link, which `cargo gdl build` does not make; remove it, or build into another target directory",
            path.display()
        ))
    };
    match std::fs::symlink_metadata(workspace) {
        Ok(metadata) if metadata.file_type().is_symlink() => return Err(link(workspace)),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io(workspace, e)),
    }
    let mut empty = true;
    let mut dirs = vec![workspace.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).map_err(|e| io(&dir, e))? {
            let entry = entry.map_err(|e| io(&dir, e))?;
            let kind = entry.file_type().map_err(|e| io(&entry.path(), e))?;
            empty = false;
            if kind.is_symlink() {
                return Err(link(&entry.path()));
            }
            if kind.is_dir() {
                dirs.push(entry.path());
            }
        }
    }
    if empty || workspace.join(STAMP).exists() {
        return Ok(());
    }
    Err(Error::new(format!(
        "{} holds files not made by `cargo gdl build`; move them, or build into another target directory",
        workspace.display()
    )))
}

/// The directory of each generated crate, from the workspace's root: the
/// members of its workspace.
fn members(files: &Files) -> Vec<PathBuf> {
    files
        .keys()
        .filter(|path| path.file_name().is_some_and(|name| name == "Cargo.toml"))
        .filter_map(|path| path.parent())
        .filter(|dir| !dir.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .collect()
}

/// The workspace's root manifest: its members, listed so that a directory
/// left behind never joins; the profile and the patch, which Cargo reads
/// only here.
fn root_manifest(spec: &Spec, members: &[PathBuf], patch: &str) -> String {
    let file = spec.file.file_name().unwrap_or_default();
    let spec_file = crate::dump::absolute(spec.dir())
        .map(|dir| dir.join(file))
        .unwrap_or_else(|_| spec.file.clone());
    let mut text = format!(
        "# {MARKER}.\n# Regenerate with: cargo gdl build --spec \"{}\"\n\n[workspace]\nresolver = \"3\"\nmembers = [\n",
        display(&spec_file).escape_debug()
    );
    for member in members {
        text.push_str(&format!("    {},\n", toml_string(&display(member))));
    }
    text.push_str("]\n");
    text.push_str(Deps::Local.profile());
    text.push_str(patch);
    text
}

/// Remove the directories under `workspace` that removing the stale file
/// `stale` left empty.
fn remove_empty(workspace: &Path, stale: &Path) {
    for dir in stale.ancestors().skip(1) {
        if dir.as_os_str().is_empty() || std::fs::remove_dir(workspace.join(dir)).is_err() {
            break;
        }
    }
}

/// Seed the workspace's lock when it has none, or when the seed changed
/// since it was last seeded; otherwise keep what Cargo wrote. The seed is
/// the lock of the workspace that holds the spec, whose versions the
/// user's crates are built and tested with, else gdl's. Returns the seed,
/// and whether it seeded.
fn seed(
    spec: &Spec,
    dir: &Path,
    workspace: &Path,
    context: &Context,
) -> Result<(Option<String>, bool), Error> {
    // The nearest manifest at or above the spec: the spec need not sit
    // beside one.
    let spec_dir = crate::dump::absolute(spec.dir())?;
    let user = spec_dir
        .ancestors()
        .find(|dir| dir.join("Cargo.toml").exists())
        .and_then(crate::dump::workspace_lock);
    let seed = user.or_else(|| {
        context
            .lock()
            .and_then(|lock| std::fs::read_to_string(lock).ok())
    });
    let Some(seed) = seed else {
        return Ok((None, false));
    };
    let lock = workspace.join("Cargo.lock");
    let last = dir.join("seed.lock");
    let same = std::fs::read_to_string(&last).is_ok_and(|last| last == seed);
    if lock.exists() && same {
        return Ok((Some(seed), false));
    }
    crate::dump::write(&lock, &seed)?;
    crate::dump::write(&last, &seed)?;
    Ok((Some(seed), true))
}

/// What settling the lock reads, hashed: the seed, the generated
/// manifests, and the lock of each workspace whose patch the build
/// carries, which says whether the host uses that patch. When none of
/// them changed, the lock Cargo keeps is still settled.
fn lock_inputs(seed: Option<&str>, files: &Files, carried: &[HostPatch]) -> String {
    let manifests: Vec<(&PathBuf, &String)> = files
        .iter()
        .filter(|(path, _)| path.file_name().is_some_and(|name| name == "Cargo.toml"))
        .collect();
    let locks: BTreeMap<&Path, Option<String>> = carried
        .iter()
        .map(|patch| {
            let lock = std::fs::read_to_string(patch.workspace.join("Cargo.lock")).ok();
            (patch.workspace.as_path(), lock)
        })
        .collect();
    format!("{:016x}\n", crate::dump::hash(&(seed, manifests, locks)))
}

/// The package of each segment of each layout: each builds the binary of
/// its name.
fn segment_packages(spec: &Spec, graph: &str) -> BTreeSet<String> {
    spec.layouts()
        .flat_map(|laid| {
            laid.placement
                .segments()
                .map(move |(segment, _)| laid.package(graph, segment))
        })
        .collect()
}

/// A segment binary as Cargo built it.
struct Binary {
    name: String,
    path: PathBuf,
}

/// Build the workspace in `workspace` in the target directory `target`,
/// with Cargo run in `cwd`, so that its configuration there applies, and
/// return each binary of `packages`. Cargo's progress and diagnostics go to
/// stderr, as it prints them.
fn cargo_build(
    workspace: &Path,
    target: &Path,
    cwd: &Path,
    release: bool,
    packages: &BTreeSet<String>,
) -> Result<Vec<Binary>, Error> {
    let mut command = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .current_dir(cwd)
        .args([
            "build",
            "--workspace",
            "--message-format=json-render-diagnostics",
        ])
        .arg("--manifest-path")
        .arg(workspace.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if release {
        command.arg("--release");
    }
    let mut child = command
        .spawn()
        .map_err(|e| Error::new(format!("running cargo build: {e}")))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let mut binaries = BTreeMap::new();
    for line in BufReader::new(stdout).lines() {
        let line = line.map_err(|e| Error::new(format!("reading cargo build's output: {e}")))?;
        let Ok(message) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if message["reason"] != "compiler-artifact" {
            continue;
        }
        let (Some(name), Some(path)) = (
            message["target"]["name"].as_str(),
            message["executable"].as_str(),
        ) else {
            continue;
        };
        if packages.contains(name) {
            binaries.insert(name.to_string(), PathBuf::from(path));
        }
    }
    let status = child
        .wait()
        .map_err(|e| Error::new(format!("running cargo build: {e}")))?;
    if !status.success() {
        return Err(Error::new(format!(
            "cargo build of {} failed",
            workspace.display()
        )));
    }
    Ok(binaries
        .into_iter()
        .map(|(name, path)| Binary { name, path })
        .collect())
}

/// The record of what a build copied, by destination directory: each
/// binary's size and modification time after the copy. A file that still
/// matches is the build's to replace or remove.
type Record = BTreeMap<String, BTreeMap<String, Stamp>>;

/// A file's size and modification time, as the build last left it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Stamp {
    size: u64,
    seconds: u64,
    nanos: u32,
    /// The size of the copy the build is about to write, recorded before
    /// it writes, so that a build stopped between the copy and the record
    /// still owns what it wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending: Option<u64>,
}

impl Stamp {
    /// Whether the file whose stamp is `stamp` is the one the build wrote,
    /// or the copy it was about to write.
    fn owns(&self, stamp: &Stamp) -> bool {
        let last =
            (self.size, self.seconds, self.nanos) == (stamp.size, stamp.seconds, stamp.nanos);
        last || self.pending == Some(stamp.size)
    }

    /// The stamp of the file at `path`; none when there is no file.
    fn of(path: &Path) -> Result<Option<Self>, Error> {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io(path, e)),
        };
        let modified = metadata.modified().map_err(|e| io(path, e))?;
        let since = modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Ok(Some(Self {
            size: metadata.len(),
            seconds: since.as_secs(),
            nanos: since.subsec_nanos(),
            pending: None,
        }))
    }
}

/// Copy each of `built` into `out` where it differs from what is there,
/// and remove the binaries that the record in `record` says a build copied
/// into `out` and this one does not make. A file is written over or
/// removed only when the record says the build wrote it and it has not
/// changed since: a file the build did not write is refused, and one that
/// someone replaced is kept. Returns the copies' paths, in name order.
fn copy_out(record: &Path, out: &Path, built: &[Binary]) -> Result<Vec<PathBuf>, Error> {
    // One record per directory, however `out` spells it.
    let out = &crate::dump::absolute(out)?;
    let last: Record = std::fs::read_to_string(record)
        .ok()
        .and_then(|text| toml::from_str(&text).ok())
        .unwrap_or_default();
    let key = display(out);
    let wrote = last.get(&key).cloned().unwrap_or_default();
    let owned = |name: &str, stamp: Option<Stamp>| {
        stamp.is_some()
            && wrote
                .get(name)
                .zip(stamp)
                .is_some_and(|(wrote, stamp)| wrote.owns(&stamp))
    };
    let mut now = last.clone();
    let mine = now.entry(key.clone()).or_default();
    // Every refusal before any write.
    let mut copy = Vec::new();
    for binary in built {
        let path = out.join(&binary.name);
        let stamp = Stamp::of(&path)?;
        if stamp.is_none() || !same_bytes(&binary.path, &path)? {
            if stamp.is_some() && !owned(&binary.name, stamp) {
                return Err(Error::new(format!(
                    "{} was not written by `cargo gdl build`; move it, or give `--out` another directory",
                    path.display()
                )));
            }
            copy.push(binary);
        }
    }
    let made: BTreeSet<&str> = built.iter().map(|binary| binary.name.as_str()).collect();
    let dropped: Vec<String> = wrote
        .keys()
        .filter(|name| !made.contains(name.as_str()))
        .cloned()
        .collect();
    for name in dropped {
        let path = out.join(&name);
        let stamp = Stamp::of(&path)?;
        if owned(&name, stamp) {
            std::fs::remove_file(&path).map_err(|e| io(&path, e))?;
        } else if stamp.is_some() {
            eprintln!(
                "cargo gdl: kept {}: it changed after the build copied it",
                path.display()
            );
        }
        mine.remove(&name);
    }
    create_dir(out)?;
    for binary in &copy {
        let size = std::fs::metadata(&binary.path)
            .map_err(|e| io(&binary.path, e))?
            .len();
        // A stamp that no file has, when the build wrote none here before.
        let last = mine.get(&binary.name).copied().unwrap_or(Stamp {
            size: 0,
            seconds: 0,
            nanos: 0,
            pending: None,
        });
        mine.insert(
            binary.name.clone(),
            Stamp {
                pending: Some(size),
                ..last
            },
        );
    }
    if !copy.is_empty() {
        let text = toml::to_string(&now).map_err(|e| Error::new(e.to_string()))?;
        crate::dump::write(record, &text)?;
    }
    let mine = now.get_mut(&key).expect("the destination's record");
    for binary in &copy {
        let path = out.join(&binary.name);
        // A new file renamed into place: a binary that is running keeps
        // the file it started from.
        let temporary = out.join(format!(".{}.tmp-{}", binary.name, std::process::id()));
        copy_new(&binary.path, &temporary)?;
        std::fs::rename(&temporary, &path).map_err(|e| io(&path, e))?;
    }
    // Only what the build copied, or copied before and nobody changed, is
    // its own: a file that someone put there with the same bytes stays
    // theirs, to keep when its segment goes.
    let copied: BTreeSet<&str> = copy.iter().map(|binary| binary.name.as_str()).collect();
    let mut copies = Vec::new();
    for binary in built {
        let path = out.join(&binary.name);
        let stamp = Stamp::of(&path)?;
        match stamp {
            Some(stamp)
                if copied.contains(binary.name.as_str()) || owned(&binary.name, Some(stamp)) =>
            {
                mine.insert(binary.name.clone(), stamp);
            }
            _ => {
                mine.remove(&binary.name);
            }
        }
        copies.push(path);
    }
    now.retain(|_, binaries| !binaries.is_empty());
    if now != last {
        let text = toml::to_string(&now).map_err(|e| Error::new(e.to_string()))?;
        crate::dump::write(record, &text)?;
    }
    Ok(copies)
}

/// Copy `from` to `to`, a file that must not exist yet, with `from`'s
/// permissions. A file or a symbolic link already at `to` is refused, not
/// written through.
fn copy_new(from: &Path, to: &Path) -> Result<(), Error> {
    let mut source = std::fs::File::open(from).map_err(|e| io(from, e))?;
    let mut copy = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(to)
        .map_err(|e| io(to, e))?;
    std::io::copy(&mut source, &mut copy).map_err(|e| io(to, e))?;
    let permissions = source.metadata().map_err(|e| io(from, e))?.permissions();
    copy.set_permissions(permissions).map_err(|e| io(to, e))
}

/// Whether files `a` and `b` hold the same bytes; false when `b` is not
/// there.
fn same_bytes(a: &Path, b: &Path) -> Result<bool, Error> {
    let (Ok(left), Ok(right)) = (std::fs::metadata(a), std::fs::metadata(b)) else {
        return Ok(false);
    };
    if left.len() != right.len() {
        return Ok(false);
    }
    let left = std::fs::read(a).map_err(|e| io(a, e))?;
    let right = std::fs::read(b).map_err(|e| io(b, e))?;
    Ok(left == right)
}

/// Exclusive use of the builds in `dir`, `gdl` in a target directory,
/// until dropped. Two specs can build packages of one name, whose binaries
/// Cargo puts at one path, and Cargo's own lock ends with the build,
/// before the copy. Cargo builds one at a time in a target directory
/// anyway.
fn lock(dir: &Path) -> Result<std::fs::File, Error> {
    create_dir(dir)?;
    let path = dir.join("build.lock");
    // Not truncated: the file's time stays, as every other file's does.
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| io(&path, e))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            eprintln!(
                "cargo gdl: waiting for another `cargo gdl build` in {}",
                dir.parent().unwrap_or(dir).display()
            );
            file.lock().map_err(|e| io(&path, e))?;
        }
        Err(std::fs::TryLockError::Error(e)) => return Err(io(&path, e)),
    }
    Ok(file)
}

fn create_dir(dir: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(dir).map_err(|e| io(dir, e))
}

fn io(path: &Path, error: std::io::Error) -> Error {
    Error::new(format!("{}: {error}", path.display()))
}

#[cfg(test)]
mod tests;
