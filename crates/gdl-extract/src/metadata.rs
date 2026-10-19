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
    /// The library target: its crate name and root file. `None` for a
    /// package without one, which offers nothing.
    pub(crate) lib: Option<(String, PathBuf)>,
    /// Normal dependencies: the name the code uses, and the package.
    pub(crate) deps: Vec<(String, usize)>,
    /// Its enabled features.
    pub(crate) features: Vec<String>,
}

impl Graph {
    pub(crate) fn load(manifest: &Path) -> Result<Self, Error> {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let output = Command::new(cargo)
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
            })
            .collect();
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

    /// The workspace member named `name`.
    pub(crate) fn package(&self, name: &str) -> Option<usize> {
        self.members
            .iter()
            .copied()
            .find(|&i| self.packages[i].name == name)
    }
}
