//! The packages and their dependencies, from `cargo metadata`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::Error;

/// Every package `cargo metadata` resolved, with each one's library, its
/// dependencies by the names its code uses for them, and its features.
pub(crate) struct Graph {
    pub(crate) packages: Vec<Package>,
    /// The workspace's own packages, by index: the ones a name selects.
    members: Vec<usize>,
    /// The workspace's root directory.
    pub(crate) workspace_root: PathBuf,
}

pub(crate) struct Package {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) source: String,
    /// The directory of its manifest.
    pub(crate) dir: PathBuf,
    /// The library target: its crate name and root file. `None` for a
    /// package without one, which offers nothing.
    pub(crate) lib: Option<(String, PathBuf)>,
    /// Normal dependencies: the name the code uses, and the package.
    pub(crate) deps: Vec<(String, usize)>,
    /// Its enabled features.
    pub(crate) features: Vec<String>,
    /// The registry's checksum of its `.crate`, from the lockfile; none for
    /// a path or git package.
    pub(crate) checksum: Option<String>,
}

impl Graph {
    pub(crate) fn load(manifest: &Path, cargo: &crate::Cargo) -> Result<Self, Error> {
        let program = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let manifest = std::path::absolute(manifest)
            .map_err(|e| Error(format!("{}: {e}", manifest.display())))?;
        let mut command = Command::new(program);
        let confinement = cargo.deadline();
        // Cargo reads its configuration from where it runs: run it where
        // the manifest is, so a `.cargo/config.toml` above a fetch's
        // scratch directory (a mirror, `net.offline`) applies to it.
        if let Some(dir) = manifest.parent() {
            command.current_dir(dir);
        }
        // Confined, Git goes through the `git` command, which honours
        // these settings: HTTP(S) only, no prompt, none of the user's
        // configuration or credential helpers, and a transfer that stalls
        // for a minute is given up. No wrapper or compiler from the crate:
        // nothing is built, but cargo asks rustc for the host's
        // configuration.
        if confinement.is_some() {
            command
                .env("CARGO_NET_GIT_FETCH_WITH_CLI", "true")
                .env("CARGO_HTTP_TIMEOUT", "60")
                .envs(crate::confined_git_env())
                .env("RUSTC", "rustc")
                .env("RUSTC_WRAPPER", "")
                .env("RUSTC_WORKSPACE_WRAPPER", "");
            // Its own process group, so that the git it spawns dies with
            // it at the deadline rather than running on, reparented; unless
            // this process is itself a confined child, whose group cargo
            // then shares.
            #[cfg(unix)]
            if cargo.own_group() {
                std::os::unix::process::CommandExt::process_group(&mut command, 0);
            }
        }
        command
            .args(["metadata", "--format-version", "1", "--manifest-path"])
            .arg(manifest)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let output = match confinement {
            None => command
                .output()
                .map_err(|e| Error(format!("running cargo: {e}")))?,
            Some(deadline) => output_within(command, deadline, cargo.own_group())?,
        };
        if !output.status.success() {
            return Err(Error(format!(
                "cargo metadata failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        let text = String::from_utf8_lossy(&output.stdout);
        Self::parse(&text)
    }

    fn parse(text: &str) -> Result<Self, Error> {
        #[derive(serde::Deserialize)]
        struct Metadata {
            packages: Vec<RawPackage>,
            workspace_members: Vec<String>,
            resolve: Resolve,
            workspace_root: PathBuf,
        }
        #[derive(serde::Deserialize)]
        struct RawPackage {
            id: String,
            name: String,
            version: String,
            source: Option<String>,
            manifest_path: PathBuf,
            targets: Vec<Target>,
        }
        #[derive(serde::Deserialize)]
        struct Target {
            name: String,
            kind: Vec<String>,
            src_path: PathBuf,
        }
        #[derive(serde::Deserialize)]
        struct Resolve {
            nodes: Vec<Node>,
        }
        #[derive(serde::Deserialize)]
        struct Node {
            id: String,
            deps: Vec<Dep>,
            #[serde(default)]
            features: Vec<String>,
        }
        #[derive(serde::Deserialize)]
        struct Dep {
            name: String,
            pkg: String,
            dep_kinds: Vec<DepKind>,
        }
        #[derive(serde::Deserialize)]
        struct DepKind {
            kind: Option<String>,
        }

        let metadata: Metadata = serde_json::from_str(text)
            .map_err(|e| Error(format!("reading cargo metadata: {e}")))?;
        let index: HashMap<&str, usize> = metadata
            .packages
            .iter()
            .enumerate()
            .map(|(i, p)| (p.id.as_str(), i))
            .collect();
        let mut packages: Vec<Package> = metadata
            .packages
            .iter()
            .map(|p| Package {
                name: p.name.clone(),
                version: p.version.clone(),
                dir: p
                    .manifest_path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_path_buf(),
                source: p.source.clone().unwrap_or_else(|| {
                    let dir = p.manifest_path.parent().unwrap_or(Path::new(""));
                    format!("path+file://{}", dir.display())
                }),
                lib: p
                    .targets
                    .iter()
                    .find(|t| t.kind.iter().any(|k| k == "lib" || k == "rlib"))
                    .map(|t| (t.name.replace('-', "_"), t.src_path.clone())),
                deps: Vec::new(),
                features: Vec::new(),
                checksum: None,
            })
            .collect();
        // `cargo metadata` resolves into the workspace's lockfile, which
        // holds each registry package's checksum.
        let checksums = checksums(&metadata.workspace_root.join("Cargo.lock"));
        for package in &mut packages {
            package.checksum = checksums
                .get(&(
                    package.name.clone(),
                    package.version.clone(),
                    package.source.clone(),
                ))
                .cloned();
        }
        for node in &metadata.resolve.nodes {
            let Some(&at) = index.get(node.id.as_str()) else {
                continue;
            };
            packages[at].features = node.features.clone();
            packages[at].deps = node
                .deps
                .iter()
                .filter(|d| d.dep_kinds.iter().any(|k| k.kind.is_none()))
                .filter_map(|d| index.get(d.pkg.as_str()).map(|&i| (d.name.clone(), i)))
                .collect();
        }
        let members = metadata
            .workspace_members
            .iter()
            .filter_map(|id| index.get(id.as_str()).copied())
            .collect();
        Ok(Self {
            packages,
            members,
            workspace_root: metadata.workspace_root,
        })
    }

    /// The package `name`, or `name@version`: a workspace member of that
    /// name first, else the one package of that name (and version) among
    /// all those resolved.
    pub(crate) fn package(&self, spec: &str) -> Result<usize, String> {
        let (name, version) = match spec.split_once('@') {
            Some((name, version)) => (name, Some(version)),
            None => (spec, None),
        };
        let fits = |&i: &usize| {
            let p = &self.packages[i];
            p.name == name && version.is_none_or(|v| p.version == v)
        };
        if let Some(member) = self.members.iter().copied().find(fits) {
            return Ok(member);
        }
        let found: Vec<usize> = (0..self.packages.len()).filter(fits).collect();
        match found.as_slice() {
            [one] => Ok(*one),
            [] => Err(format!("the workspace resolves no package `{spec}`")),
            several => Err(format!(
                "the workspace resolves {} packages `{name}` ({}); name one as `{name}@<version>`",
                several.len(),
                several
                    .iter()
                    .map(|&i| self.packages[i].version.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }

    /// The package whose manifest is in `dir`.
    pub(crate) fn package_in(&self, dir: &Path) -> Option<usize> {
        let dir = std::fs::canonicalize(dir).ok()?;
        (0..self.packages.len())
            .find(|&i| std::fs::canonicalize(&self.packages[i].dir).is_ok_and(|d| d == dir))
    }
}

/// Each locked package's checksum, by name, version, and source. A
/// lockfile that cannot be read gives none: the checksum is provenance,
/// not something reading source needs.
fn checksums(lockfile: &Path) -> HashMap<(String, String, String), String> {
    #[derive(serde::Deserialize)]
    struct Lock {
        #[serde(default)]
        package: Vec<Locked>,
    }
    #[derive(serde::Deserialize)]
    struct Locked {
        name: String,
        version: String,
        source: Option<String>,
        checksum: Option<String>,
    }
    let Ok(text) = std::fs::read_to_string(lockfile) else {
        return HashMap::new();
    };
    let Ok(lock) = toml::from_str::<Lock>(&text) else {
        return HashMap::new();
    };
    lock.package
        .into_iter()
        .filter_map(|p| Some(((p.name, p.version, p.source?), p.checksum?)))
        .collect()
}

/// The most metadata cargo may write: this workspace's own is a megabyte,
/// and a dependency's `[package.metadata]` is copied verbatim, so a
/// repository can make it anything. Past this the run is ended and
/// refused, never truncated: a cut JSON would fail to parse, less clearly.
const STDOUT_LIMIT: u64 = 64 << 20;

/// Run `command` to completion, or kill it at `deadline`. Its output is
/// drained on threads meanwhile, so a chatty command cannot fill a pipe
/// and wait on us: stdout, the metadata, up to `STDOUT_LIMIT`, past which
/// the command is killed and the run refused; and the first
/// `OUTPUT_LIMIT` of stderr, the rest discarded as it arrives, because
/// git relays a remote's messages there without end if the remote is
/// hostile.
fn output_within(
    mut command: Command,
    deadline: std::time::Duration,
    own_group: bool,
) -> Result<std::process::Output, Error> {
    use std::io::Read;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let mut child = command
        .spawn()
        .map_err(|e| Error(format!("running cargo: {e}")))?;
    let pid = child.id();
    let overflowed = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&overflowed);
    let stdout = std::thread::spawn({
        let stream = child.stdout.take();
        move || {
            let mut bytes = Vec::new();
            if let Some(mut stream) = stream {
                let _ = (&mut stream).take(STDOUT_LIMIT + 1).read_to_end(&mut bytes);
                if bytes.len() as u64 > STDOUT_LIMIT {
                    flag.store(true, Ordering::SeqCst);
                    if own_group {
                        crate::kill_process_group(pid);
                    }
                    bytes.clear();
                }
            }
            bytes
        }
    });
    let stderr = std::thread::spawn({
        let stream = child.stderr.take();
        move || {
            let mut bytes = Vec::new();
            if let Some(mut stream) = stream {
                let _ = (&mut stream)
                    .take(crate::OUTPUT_LIMIT)
                    .read_to_end(&mut bytes);
                let _ = std::io::copy(&mut stream, &mut std::io::sink());
            }
            bytes
        }
    });
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| Error(format!("waiting for cargo: {e}")))?
        {
            break status;
        }
        if overflowed.load(Ordering::SeqCst) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(too_much_metadata());
        }
        if started.elapsed() >= deadline {
            if own_group {
                crate::kill_process_group(pid);
            }
            let _ = child.kill();
            let _ = child.wait();
            return Err(Error(format!(
                "cargo metadata did not finish within {deadline:?}; a dependency may not be answering"
            )));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    let stdout = stdout.join().unwrap_or_default();
    let stderr = stderr.join().unwrap_or_default();
    if overflowed.load(Ordering::SeqCst) {
        return Err(too_much_metadata());
    }
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

fn too_much_metadata() -> Error {
    Error(format!(
        "cargo metadata wrote more than {} MiB; a dependency's manifest carries too much metadata",
        STDOUT_LIMIT >> 20
    ))
}

#[cfg(test)]
mod tests {
    /// A command's error output is kept only up to the limit, the rest
    /// read and dropped; its standard output, the metadata, whole.
    #[test]
    fn a_command_that_never_stops_talking_is_bounded() {
        let mut command = std::process::Command::new("sh");
        command
            .args([
                "-c",
                "head -c 50000000 /dev/zero | tr '\\0' x >&2; head -c 3000000 /dev/zero | tr '\\0' y",
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let output =
            super::output_within(command, std::time::Duration::from_secs(60), true).expect("ran");
        assert!(output.status.success());
        assert_eq!(output.stderr.len() as u64, crate::OUTPUT_LIMIT);
        assert_eq!(output.stdout.len(), 3_000_000);
    }

    /// Metadata past the limit ends the run and refuses it, rather than
    /// keeping it or cutting it.
    #[test]
    fn metadata_past_the_limit_is_refused() {
        let pidfile =
            std::env::temp_dir().join(format!("gdl-metadata-group-{}", std::process::id()));
        let _ = std::fs::remove_file(&pidfile);
        let mut command = std::process::Command::new("sh");
        // The group leader records itself; `sleep` is its child, and must
        // die with the group rather than with the leader alone.
        command
            .args([
                "-c",
                &format!(
                    "echo $$ > {:?}; head -c 70000000 /dev/zero; sleep 30",
                    pidfile.display()
                ),
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        #[cfg(unix)]
        std::os::unix::process::CommandExt::process_group(&mut command, 0);
        let started = std::time::Instant::now();
        let Err(error) = super::output_within(command, std::time::Duration::from_secs(60), true)
        else {
            panic!("seventy megabytes of metadata were kept");
        };
        assert!(error.to_string().contains("more than"), "{error}");
        assert!(started.elapsed() < std::time::Duration::from_secs(20));
        let leader: u32 = std::fs::read_to_string(&pidfile)
            .expect("the leader's pid")
            .trim()
            .parse()
            .expect("a pid");
        let _ = std::fs::remove_file(&pidfile);
        // Nothing of the group is left: a signal to it finds no process.
        let gone = std::process::Command::new("kill")
            .args(["-0", "--", &format!("-{leader}")])
            .stderr(std::process::Stdio::null())
            .status()
            .expect("kill");
        assert!(!gone.success(), "the group outlived the refusal");
    }
}
