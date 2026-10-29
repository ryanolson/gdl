//! The lock file of each segment of a bundle of local sources, written on
//! the host, where every source the segment names can be fetched. The
//! image build has no SSH agent and no credentials, so it builds by this
//! lock and resolves nothing.

use std::collections::BTreeSet;
use std::path::Path;

use std::path::PathBuf;

use super::context::{Context, HostPatch};
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
        let carried = context.host_patches(files, &[PathBuf::from(segment)]);
        settle(&dir, &carried, context)?;
    }
    Ok(())
}

/// Resolve the lock of the workspace in `dir`, which builds generated
/// crates that carry the host patches `carried`: move each package the
/// lock takes in place of a host patch the host uses to the patch's
/// version, and refuse a second copy of a gdl crate, from a source that no
/// crate the workspace reaches by path names, which its patch therefore
/// misses.
pub(crate) fn settle(dir: &Path, carried: &[HostPatch], context: &Context) -> Result<(), Error> {
    let prefix = context.prefix();
    let cwd = context.cwd();
    let mut metadata = resolve(dir, cwd, prefix)?;
    // An update can move other patched packages with it, so the pins
    // are read again after each.
    let mut tried = BTreeSet::new();
    while let Some((package, locked, version)) = host_pins(dir, carried, prefix)?.into_iter().next()
    {
        if !tried.insert((package.clone(), version.clone())) {
            return Err(Error::new(format!(
                "{prefix}: segment crate {} does not build the host's patch of `{package}` {version}, which the host's lock takes: another crate it builds needs another version",
                dir.display()
            )));
        }
        update(dir, cwd, &package, &locked, &version, prefix)?;
        metadata = resolve(dir, cwd, prefix)?;
    }
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
            "{prefix}: segment crate {} would build a second copy of gdl: {}. {why}",
            dir.display(),
            copies.join(", ")
        )));
    }
    Ok(())
}

/// Each host patch that the lock of the segment crate in `dir` leaves
/// unused while the lock of the workspace that carries it uses it: its
/// package, the version the segment locks in its place, and the patch's
/// version. The segment's lock starts from gdl's, which can lock the
/// package at a version the patch does not give, and Cargo then keeps that
/// version and leaves the patch unused. A patch of a package the segment
/// does not build, such as the host's dev-dependency, is none.
fn host_pins(
    dir: &Path,
    carried: &[HostPatch],
    prefix: &str,
) -> Result<Vec<(String, String, String)>, Error> {
    let lock = dir.join("Cargo.lock");
    let segment = unused(&lock, prefix)?;
    let mut pins = Vec::new();
    for (package, version) in segment {
        let Some(locked) = locked_in_place(&lock, &package, &version, prefix)? else {
            continue;
        };
        let host = carried
            .iter()
            .filter(|patch| patch.package == package)
            .map(|patch| unused(&patch.workspace.join("Cargo.lock"), prefix))
            .collect::<Result<Vec<_>, _>>()?;
        // A host whose lock leaves the patch unused builds without it too.
        let used = host
            .iter()
            .any(|unused| !unused.contains(&(package.clone(), version.clone())));
        if used {
            pins.push((package, locked, version));
        }
    }
    Ok(pins)
}

/// The version of `package` that the lock file `lock` takes from a source
/// in place of the patch at `version`: the one Cargo takes as compatible,
/// with the same leftmost nonzero part. None when it takes none.
fn locked_in_place(
    lock: &Path,
    package: &str,
    version: &str,
    prefix: &str,
) -> Result<Option<String>, Error> {
    let text = std::fs::read_to_string(lock)
        .map_err(|e| Error::new(format!("{prefix}: {}: {e}", lock.display())))?;
    let lock: toml::Table = text
        .parse()
        .map_err(|e| Error::new(format!("{prefix}: {}: {e}", lock.display())))?;
    let packages = lock.get("package").and_then(toml::Value::as_array);
    Ok(packages
        .into_iter()
        .flatten()
        .filter(|entry| entry.get("name").and_then(toml::Value::as_str) == Some(package))
        .filter(|entry| entry.get("source").is_some())
        .filter_map(|entry| entry.get("version")?.as_str())
        .find(|locked| compatible(locked, version))
        .map(str::to_string))
}

/// Whether versions `a` and `b` agree up to their leftmost nonzero part,
/// as a caret requirement does.
fn compatible(a: &str, b: &str) -> bool {
    let parts = |v: &str| -> Vec<String> {
        v.split(['-', '+'])
            .next()
            .unwrap_or(v)
            .split('.')
            .map(str::to_string)
            .collect()
    };
    let (a, b) = (parts(a), parts(b));
    for (x, y) in a.iter().zip(&b) {
        if x != y {
            return false;
        }
        if x != "0" {
            return true;
        }
    }
    a == b
}

/// The `[[patch.unused]]` entries of the lock file `lock`, by package and
/// version: none when there is no lock.
fn unused(lock: &Path, prefix: &str) -> Result<Vec<(String, String)>, Error> {
    let Ok(text) = std::fs::read_to_string(lock) else {
        return Ok(Vec::new());
    };
    let lock: toml::Table = text
        .parse()
        .map_err(|e| Error::new(format!("{prefix}: {}: {e}", lock.display())))?;
    let entries = lock
        .get("patch")
        .and_then(|patch| patch.get("unused"))
        .and_then(toml::Value::as_array);
    Ok(entries
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let name = entry.get("name")?.as_str()?;
            let version = entry.get("version")?.as_str()?;
            Some((name.to_string(), version.to_string()))
        })
        .collect())
}

/// Move `package` at `locked` in the lock of the segment crate in `dir` to
/// `version`, which the host's patch gives, as `cargo update --precise`
/// does. The version names one package when the lock holds several.
fn update(
    dir: &Path,
    root: &Path,
    package: &str,
    locked: &str,
    version: &str,
    prefix: &str,
) -> Result<(), Error> {
    let spec = format!("{package}@{locked}");
    let output =
        std::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .current_dir(root)
            .args([
                "update",
                "--package",
                &spec,
                "--precise",
                version,
                "--manifest-path",
            ])
            .arg(dir.join("Cargo.toml"))
            .output()
            .map_err(|e| Error::new(format!("{prefix}: running cargo update: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "{prefix}: segment crate {} does not build the host's patch of `{package}` {version}, which the host's lock takes, and moving its lock to it failed: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

/// `cargo metadata` of the crate in `dir`, which resolves it and writes
/// its lock. It runs in the context's root, `root`, as the image build
/// does, so that both read the same Cargo configuration.
fn resolve(dir: &Path, root: &Path, prefix: &str) -> Result<serde_json::Value, Error> {
    let output =
        std::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .current_dir(root)
            .args(["metadata", "--format-version", "1", "--manifest-path"])
            .arg(dir.join("Cargo.toml"))
            .output()
            .map_err(|e| Error::new(format!("{prefix}: running cargo metadata: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "{prefix}: resolving {} to write its lock: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(super::io)
}

#[cfg(test)]
mod tests {
    use super::compatible;

    /// Two versions agree as a caret requirement does: up to the leftmost
    /// nonzero part, so a patch replaces only the version it can stand in
    /// for.
    #[test]
    fn versions_agree_up_to_their_leftmost_nonzero_part() {
        for (a, b, same) in [
            ("2.0.21", "2.0.20", true),
            ("1.0.69", "2.0.20", false),
            ("0.4.3", "0.4.1", true),
            ("0.4.3", "0.5.0", false),
            ("0.0.3", "0.0.3", true),
            ("0.0.3", "0.0.4", false),
            ("1.2.0-rc.1", "1.0.0", true),
        ] {
            assert_eq!(compatible(a, b), same, "{a} and {b}");
        }
    }
}
