//! Generates one crate per segment of a gdl graph placement.
//!
//! The input is a spec ([`Spec`]): the contract crate holding the graph's
//! `graph!` invocation, the placement of nodes into segments, and the crate
//! and type implementing each node. The generator reads the contract's
//! manifest by compiling the contract alone, validates the placement and the
//! bindings against it, and writes, per segment, a crate with a library
//! (`start`, to deploy the segment inside a program) and a binary (to run
//! it on its own). Each segment crate depends only on the implementation
//! crates of the nodes it hosts.
//!
//! Every generated file is marked [`MARKER`]. Generation replaces marked
//! files and removes marked files it no longer produces; `--check` reports
//! any difference instead of writing.

mod dump;
mod emit;
mod paths;
mod spec;

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

pub use emit::{MARKER, package};
pub use spec::{Bind, Contract, GdlDep, Spec};

/// A generation failure, with everything wrong at once where possible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// Generated files, keyed by path relative to the output directory.
pub type Files = BTreeMap<PathBuf, String>;

/// Build the contract's manifest in `target` (a cargo target directory),
/// validate `spec` against it, and render every file for output directory
/// `out`.
pub fn render(spec: &Spec, out: &Path, target: &Path) -> Result<Files, Error> {
    let manifest = dump::manifest(spec, target)?;
    render_manifest(spec, &manifest, out)
}

/// [`render`] from a manifest already in hand.
pub fn render_manifest(spec: &Spec, manifest: &gdl::Manifest, out: &Path) -> Result<Files, Error> {
    let layout = validate(spec, manifest)?;
    let out = dump::absolute(out)?;
    emit::files(spec, manifest, &layout, &out)
}

/// Check the placement and the bindings against the manifest, reporting
/// every problem found.
pub fn validate(spec: &Spec, manifest: &gdl::Manifest) -> Result<gdl::Layout, Error> {
    let mut problems = Vec::new();
    let layout = match gdl::layout(manifest, &spec.placement) {
        Ok(layout) => Some(layout),
        Err(errors) => {
            problems.push(errors.to_string());
            None
        }
    };
    for (segment, _) in spec.placement.segments() {
        let valid = !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !valid {
            problems.push(format!(
                "segment `{segment}`: a segment name is letters, digits, `-`, and `_`"
            ));
        }
    }
    for (node, bind) in &spec.bind {
        match manifest.node(node) {
            None => problems.push(format!("[bind.{node}]: the graph has no node `{node}`")),
            Some(info) if info.role == gdl::Role::Source => problems.push(format!(
                "[bind.{node}]: `{node}` is the source, which takes no implementation"
            )),
            Some(_) => {}
        }
        if bind.ty.trim().is_empty() {
            problems.push(format!("[bind.{node}]: `type` is empty"));
        }
    }
    for node in &manifest.nodes {
        if node.role != gdl::Role::Source && !spec.bind.contains_key(&node.name) {
            problems.push(format!("node `{}` has no [bind.{}]", node.name, node.name));
        }
        if node.role != gdl::Role::Source && node.binding.is_none() {
            problems.push(format!(
                "node `{}` has no binding name in the manifest; rebuild the contract with this gdl",
                node.name
            ));
        }
    }
    match (layout, problems.is_empty()) {
        (Some(layout), true) => Ok(layout),
        _ => Err(Error::new(problems.join("\n"))),
    }
}

/// Write `files` under `out`, and remove generated files there that are no
/// longer produced. Returns the removed paths.
pub fn write(out: &Path, files: &Files) -> Result<Vec<PathBuf>, Error> {
    for (path, text) in files {
        let path = out.join(path);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| io(dir, e))?;
        }
        if std::fs::read_to_string(&path).is_ok_and(|old| &old == text) {
            continue;
        }
        std::fs::write(&path, text).map_err(|e| io(&path, e))?;
    }
    let stale: Vec<PathBuf> = generated_under(out)?
        .into_iter()
        .filter(|path| !files.contains_key(path))
        .collect();
    for path in &stale {
        let full = out.join(path);
        std::fs::remove_file(&full).map_err(|e| io(&full, e))?;
    }
    Ok(stale)
}

/// How the files under `out` differ from `files`: one line per missing,
/// changed, or stale generated file. Empty when they match.
pub fn drift(out: &Path, files: &Files) -> Result<Vec<String>, Error> {
    let mut found = Vec::new();
    for (path, text) in files {
        match std::fs::read_to_string(out.join(path)) {
            Err(_) => found.push(format!("missing: {}", path.display())),
            Ok(old) if &old != text => found.push(format!("differs: {}", path.display())),
            Ok(_) => {}
        }
    }
    for path in generated_under(out)? {
        if !files.contains_key(&path) {
            found.push(format!("stale: {}", path.display()));
        }
    }
    Ok(found)
}

/// Generated files under `out`, relative to it. A file is generated when its
/// first line carries the marker. Build output is skipped.
fn generated_under(out: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut found = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(dir) = dirs.pop() {
        let full = out.join(&dir);
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
                if entry.file_name() != "target" {
                    dirs.push(path);
                }
            } else if kind.is_file() {
                let text = std::fs::read_to_string(out.join(&path)).unwrap_or_default();
                if text
                    .lines()
                    .next()
                    .is_some_and(|line| line.contains(MARKER))
                {
                    found.push(path);
                }
            }
        }
    }
    found.sort();
    Ok(found)
}

fn io(path: &Path, error: std::io::Error) -> Error {
    Error::new(format!("{}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gdl::{HopInfo, Manifest, NodeInfo, Role, TypeInfo};

    /// `S => D`: a source and one sink.
    fn manifest() -> Manifest {
        let node = |name: &str, role, binding: Option<&str>| NodeInfo {
            name: name.into(),
            role,
            binding: binding.map(str::to_string),
        };
        Manifest {
            name: "tiny".into(),
            schema: 1,
            nodes: vec![
                node("S", Role::Source, None),
                node("D", Role::Sink, Some("d")),
            ],
            hops: vec![HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![TypeInfo::new("Req", true)],
                backward_types: vec![TypeInfo::new("Item", true)],
            }],
        }
    }

    fn spec(placement: gdl::Placement, bind: &[(&str, &str)]) -> Spec {
        Spec {
            file: "gdl.toml".into(),
            dir: "/work".into(),
            contract: Contract {
                package: "tiny-contract".into(),
                path: "/work/contract".into(),
                module: "tiny_contract".into(),
            },
            gdl: GdlDep::Version("0.1".into()),
            placement,
            bind: bind
                .iter()
                .map(|(node, ty)| {
                    let bind = Bind {
                        package: "tiny-nodes".into(),
                        path: "/work/nodes".into(),
                        ty: (*ty).into(),
                    };
                    ((*node).to_string(), bind)
                })
                .collect(),
        }
    }

    fn split() -> gdl::Placement {
        gdl::Placement::new()
            .segment("front", ["S"])
            .segment("engine", ["D"])
    }

    #[test]
    fn a_complete_spec_validates() {
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        assert!(validate(&spec, &manifest()).is_ok());
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let placement = gdl::Placement::new()
            .segment("front.end", ["S"])
            .segment("engine", ["D"]);
        let spec = spec(placement, &[("S", "x::S"), ("Q", "x::Q")]);
        let error = validate(&spec, &manifest())
            .expect_err("invalid")
            .to_string();
        for expected in [
            "segment `front.end`",
            "[bind.S]: `S` is the source",
            "[bind.Q]: the graph has no node `Q`",
            "node `D` has no [bind.D]",
        ] {
            assert!(
                error.contains(expected),
                "{expected:?} missing from:\n{error}"
            );
        }
    }

    #[test]
    fn a_placement_error_is_reported() {
        let placement = gdl::Placement::new().segment("front", ["S"]);
        let spec = spec(placement, &[("D", "x::D")]);
        assert!(validate(&spec, &manifest()).is_err());
    }

    #[test]
    fn drift_finds_changed_missing_and_stale_files() {
        let out = std::env::temp_dir().join(format!("gdl-gen-drift-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&out);
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        let files = render_manifest(&spec, &manifest(), &out).expect("render");
        write(&out, &files).expect("write");
        assert_eq!(drift(&out, &files).expect("drift"), Vec::<String>::new());

        let lib = out.join("engine/src/lib.rs");
        let edited = std::fs::read_to_string(&lib).expect("read") + "\n// hand edit\n";
        std::fs::write(&lib, edited).expect("edit");
        std::fs::remove_file(out.join("front/Cargo.toml")).expect("remove");
        std::fs::create_dir_all(out.join("gone/src")).expect("dir");
        std::fs::write(out.join("gone/src/lib.rs"), format!("// {MARKER}.\n")).expect("stale");
        std::fs::write(out.join("notes.txt"), "not generated\n").expect("user file");
        assert_eq!(
            drift(&out, &files).expect("drift"),
            [
                "differs: engine/src/lib.rs",
                "missing: front/Cargo.toml",
                "stale: gone/src/lib.rs",
            ]
        );

        let removed = write(&out, &files).expect("rewrite");
        assert_eq!(removed, [PathBuf::from("gone/src/lib.rs")]);
        assert_eq!(drift(&out, &files).expect("drift"), Vec::<String>::new());
        assert!(
            out.join("notes.txt").exists(),
            "a file without the marker is kept"
        );
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn only_the_source_segment_serves_and_depends_on_json() {
        let out = std::path::Path::new("/work/segments");
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        let files = render_manifest(&spec, &manifest(), out).expect("render");
        let front = &files[&PathBuf::from("front/src/main.rs")];
        let engine = &files[&PathBuf::from("engine/src/main.rs")];
        assert!(front.contains("serve(&segment).await"));
        assert!(!engine.contains("serve("));
        assert!(files[&PathBuf::from("front/Cargo.toml")].contains("serde_json"));
        assert!(!files[&PathBuf::from("engine/Cargo.toml")].contains("serde_json"));
        assert!(!files[&PathBuf::from("front/Cargo.toml")].contains("tiny-nodes"));
        assert!(files[&PathBuf::from("engine/Cargo.toml")].contains("tiny-nodes"));
    }
}
