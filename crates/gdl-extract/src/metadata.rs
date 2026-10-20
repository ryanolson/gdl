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
    pub(crate) fn load(manifest: &Path) -> Result<Self, Error> {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        // Cargo reads its configuration from where it runs: run it where
        // the manifest is, so a `.cargo/config.toml` above a fetch's
        // scratch directory (a mirror, `net.offline`) applies to it.
        let manifest = std::path::absolute(manifest)
            .map_err(|e| Error(format!("{}: {e}", manifest.display())))?;
        let mut command = Command::new(cargo);
        if let Some(dir) = manifest.parent() {
            command.current_dir(dir);
        }
        let output = command
            .args(["metadata", "--format-version", "1", "--manifest-path"])
            .arg(manifest)
            .output()
            .map_err(|e| Error(format!("running cargo: {e}")))?;
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
        Ok(Self { packages, members })
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
