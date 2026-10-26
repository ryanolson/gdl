//! The lock file of each segment of a bundle of local sources, written on
//! the host, where every source the segment names can be fetched. The
//! image build has no SSH agent and no credentials, so it builds by this
//! lock and resolves nothing.

use std::path::Path;

use super::context::Context;
use crate::{Error, Files};

/// Write `Cargo.lock` in each segment crate of the bundle in `out`, whose
/// files are `files`: each crate a Dockerfile builds. Each starts from the
/// gdl workspace's lock, so that a dependency the runtime shares, such as
/// velo from its Git branch, stays at the version gdl was built and tested
/// with; a fresh resolve would take the newest commit. Refuses a segment
/// that still builds a second copy of a gdl crate, from a source that no
/// crate it reaches by path names, which its patch therefore misses.
pub(super) fn write(out: &Path, files: &Files, context: &Context) -> Result<(), Error> {
    let segments = files
        .keys()
        .filter(|path| path.file_name().is_some_and(|name| name == "Dockerfile"))
        .filter_map(|path| path.parent())
        .filter(|dir| files.contains_key(&dir.join("Cargo.toml")));
    for segment in segments {
        let dir = out.join(segment);
        if let Some(lock) = context.lock() {
            std::fs::copy(lock, dir.join("Cargo.lock"))
                .map_err(|e| Error::new(format!("`--local`: {}: {e}", lock.display())))?;
        }
        let metadata = resolve(&dir, context.root())?;
        let copies: Vec<String> = metadata["packages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|package| package["name"].as_str().is_some_and(|n| context.is_gdl(n)))
            .filter_map(|package| {
                let source = package["source"].as_str()?;
                Some(format!("`{}` from {source}", package["name"].as_str()?))
            })
            .collect();
        if !copies.is_empty() {
            let mut why = String::from(
                "Only a crate from Git or a registry takes gdl from that source, so the segment does not patch it. Take gdl from that source in a crate by path, or take that crate by path",
            );
            for unread in context.unread() {
                why.push_str(&format!(
                    ". The patch also skipped a crate it could not read: {unread}"
                ));
            }
            return Err(Error::new(format!(
                "`--local`: segment crate {} would build a second copy of gdl: {}. {why}",
                dir.display(),
                copies.join(", ")
            )));
        }
    }
    Ok(())
}

/// `cargo metadata` of the crate in `dir`, which resolves it and writes
/// its lock. It runs in the context's root, `root`, as the image build
/// does, so that both read the same Cargo configuration.
fn resolve(dir: &Path, root: &Path) -> Result<serde_json::Value, Error> {
    let output =
        std::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .current_dir(root)
            .args(["metadata", "--format-version", "1", "--manifest-path"])
            .arg(dir.join("Cargo.toml"))
            .output()
            .map_err(|e| Error::new(format!("`--local`: running cargo metadata: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "`--local`: resolving {} to write its lock: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(super::io)
}
