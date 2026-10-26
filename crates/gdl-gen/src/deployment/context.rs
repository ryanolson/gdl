//! The build context of a bundle whose images build local sources
//! ([`super::Sources::Local`]): the nearest directory that holds the
//! bundle, every crate its manifests name by path, the Cargo workspace of
//! each, and the hub's crate. Paths in the bundle's files are relative to
//! it, so that no file names this machine's directories.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use super::io;
use crate::paths::{display, normalize, relative};
use crate::{Error, Files, emit};

/// Where a local bundle's images build from.
pub(super) struct Context {
    /// The context's root, absolute.
    root: PathBuf,
    /// The bundle, from the root.
    bundle: PathBuf,
    /// The hub's crate, from the root.
    hub: PathBuf,
    /// The lock file of the gdl workspace, from the root, when it has one.
    lock: Option<PathBuf>,
}

impl Context {
    /// The context of the bundle in `out` (absolute), whose generated
    /// `files` name their dependencies relative to themselves, and whose
    /// runtime is the `gdl-runtime` crate in `runtime`. A crate named by
    /// path in a crate named by path is seen only through its workspace,
    /// when one of these directories holds it.
    pub(super) fn find(out: &Path, files: &Files, runtime: &Path) -> Result<Self, Error> {
        let (hub, workspace) = hub_crate(runtime)?;
        let lock = Some(workspace.join("Cargo.lock")).filter(|lock| lock.exists());
        let mut needed = BTreeSet::from([out.to_path_buf(), hub.clone(), workspace]);
        for (path, text) in files {
            if path.file_name().is_none_or(|name| name != "Cargo.toml") {
                continue;
            }
            let krate = normalize(&out.join(path.parent().unwrap_or(Path::new(""))));
            let Ok(manifest) = text.parse::<toml::Table>() else {
                continue;
            };
            let deps = manifest.get("dependencies").and_then(toml::Value::as_table);
            for dep in deps.into_iter().flat_map(|deps| deps.values()) {
                if let Some(path) = dep.get("path").and_then(toml::Value::as_str) {
                    needed.insert(normalize(&krate.join(path)));
                }
            }
        }
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
        let from = |path: &Path| relative(&root, path);
        Ok(Self {
            bundle: from(out),
            hub: from(&hub),
            lock: lock.as_deref().map(from),
            root,
        })
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

    /// The gdl workspace's lock file, from the context's root.
    pub(super) fn lock(&self) -> Option<String> {
        self.lock.as_deref().map(display)
    }

    /// The ignore file of each of the bundle's Dockerfiles: build output
    /// and git history stay out of the context, and the bundle stays in
    /// even when it is written under a `target` directory.
    pub(super) fn ignore(&self) -> String {
        let mut text = format!(
            "# {}\n# What the build context leaves out: build output and git history.\n**/target\n**/.git\n",
            emit::MARKER
        );
        if self.bundle.components().any(|c| c.as_os_str() == "target") {
            text.push_str(&format!("!{}\n", display(&self.bundle)));
        }
        text
    }
}

/// The `gdl-hub` crate of the workspace that holds the `gdl-runtime` crate
/// in `runtime`, and that workspace's root: the hub is built from the
/// same sources as the runtime the segments link.
fn hub_crate(runtime: &Path) -> Result<(PathBuf, PathBuf), Error> {
    let output =
        std::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--manifest-path",
            ])
            .arg(runtime.join("Cargo.toml"))
            .output()
            .map_err(|e| Error::new(format!("`--local`: running cargo metadata: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "`--local`: cargo metadata for {}: {}",
            runtime.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(io)?;
    let workspace = metadata["workspace_root"]
        .as_str()
        .map(PathBuf::from)
        .ok_or_else(|| Error::new("`--local`: cargo metadata names no workspace root"))?;
    let hub = metadata["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|package| package["name"] == "gdl-hub")
        .and_then(|package| package["manifest_path"].as_str())
        .and_then(|manifest| Path::new(manifest).parent().map(Path::to_path_buf))
        .ok_or_else(|| {
            Error::new(format!(
                "`--local`: the workspace of [gdl] path, {}, has no `gdl-hub` crate to build the hub from",
                workspace.display()
            ))
        })?;
    Ok((normalize(&hub), normalize(&workspace)))
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
