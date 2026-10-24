//! The output directory the generator owns: writing what it renders, and
//! finding drift between the two.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::{Error, Files, MARKER};

/// Write `files` under `out`, and remove generated files there that are no
/// longer produced. Returns the removed paths. Refuses, writing nothing, if
/// a segment crate's directory holds a file the generator did not write:
/// whatever it is, it changes that crate's build.
pub fn write(out: &Path, files: &Files) -> Result<Vec<PathBuf>, Error> {
    let survey = survey(out, files)?;
    if !survey.foreign.is_empty() {
        let list: Vec<String> = survey
            .foreign
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        return Err(Error::new(format!(
            "these files in generated crates were not generated; move them out first:\n{}",
            list.join("\n")
        )));
    }
    for (path, text) in files {
        let path = out.join(path);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| io(dir, e))?;
        }
        if std::fs::read_to_string(&path).is_ok_and(|existing| &existing == text) {
            continue;
        }
        std::fs::write(&path, text).map_err(|e| io(&path, e))?;
    }
    for path in &survey.stale {
        let full = out.join(path);
        std::fs::remove_file(&full).map_err(|e| io(&full, e))?;
    }
    Ok(survey.stale)
}

/// How the files under `out` differ from `files`: one line per missing,
/// changed, stale (generated, no longer produced), or unexpected (in a
/// generated crate, not generated) file. Empty when they match.
pub fn drift(out: &Path, files: &Files) -> Result<Vec<String>, Error> {
    let mut found = Vec::new();
    for (path, text) in files {
        match std::fs::read_to_string(out.join(path)) {
            Err(_) => found.push(format!("missing: {}", path.display())),
            Ok(existing) if &existing != text => found.push(format!("differs: {}", path.display())),
            Ok(_) => {}
        }
    }
    let survey = survey(out, files)?;
    found.extend(
        survey
            .stale
            .iter()
            .map(|p| format!("stale: {}", p.display())),
    );
    found.extend(
        survey
            .foreign
            .iter()
            .map(|p| format!("unexpected: {}", p.display())),
    );
    Ok(found)
}

/// Files under `out` that `files` does not hold.
struct Survey {
    /// Marked as generated: output of an earlier generation.
    stale: Vec<PathBuf>,
    /// Unmarked, inside a generated crate's directory or at a path the
    /// generator writes.
    foreign: Vec<PathBuf>,
}

/// Walk `out`. A generated crate is a directory the generator writes a
/// `Cargo.toml` in: a segment's or the contract's at the top of the output,
/// or a segment's in its layout's directory. The one directory skipped is a
/// crate's own build directory, `<crate>/target` holding a `CACHEDIR.TAG`,
/// which is what building the crate on its own leaves. Neither the name
/// alone (a segment may be called `target`) nor a tag alone (Cargo still
/// builds a tagged `src/bin`) marks a directory as build output. Likewise a
/// crate's `Cargo.lock` is left to whoever builds the crate on its own: it
/// changes no source and pins only what that build resolved.
fn survey(out: &Path, files: &Files) -> Result<Survey, Error> {
    let crates: BTreeSet<&Path> = files
        .keys()
        .filter(|path| path.file_name().is_some_and(|name| name == "Cargo.toml"))
        .filter_map(|path| path.parent())
        .filter(|dir| !dir.as_os_str().is_empty())
        .collect();
    let in_crate = |path: &Path| path.ancestors().skip(1).any(|dir| crates.contains(dir));
    let mut survey = Survey {
        stale: Vec::new(),
        foreign: Vec::new(),
    };
    let mut dirs = vec![PathBuf::new()];
    while let Some(dir) = dirs.pop() {
        let full = out.join(&dir);
        let build_dir = dir.file_name().is_some_and(|name| name == "target")
            && dir.parent().is_some_and(|krate| crates.contains(krate));
        if build_dir && full.join("CACHEDIR.TAG").exists() {
            continue;
        }
        let entries = match std::fs::read_dir(&full) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io(&full, e)),
        };
        for entry in entries {
            let entry = entry.map_err(|e| io(&full, e))?;
            let path = dir.join(entry.file_name());
            let kind = entry.file_type().map_err(|e| io(&full, e))?;
            if kind.is_dir() {
                dirs.push(path);
                continue;
            }
            let lockfile = path.file_name().is_some_and(|name| name == "Cargo.lock")
                && crates.contains(dir.as_path());
            if lockfile {
                continue;
            }
            let text = std::fs::read_to_string(out.join(&path)).unwrap_or_default();
            let marked = text
                .lines()
                .next()
                .is_some_and(|line| line.contains(MARKER));
            // A file at a path the generator writes is the generator's only
            // if it says so; a handwritten one there is never replaced.
            if files.contains_key(&path) {
                if !marked {
                    survey.foreign.push(path);
                }
                continue;
            }
            if marked {
                survey.stale.push(path);
            } else if in_crate(&path) {
                survey.foreign.push(path);
            }
        }
    }
    survey.stale.sort();
    survey.foreign.sort();
    Ok(survey)
}

fn io(path: &Path, error: std::io::Error) -> Error {
    Error::new(format!("{}: {error}", path.display()))
}
