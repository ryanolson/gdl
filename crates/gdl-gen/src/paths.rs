//! Lexical path arithmetic. Generated manifests hold paths relative to the
//! generated crate, so the output is the same wherever it is generated from;
//! the paths need not exist yet, so nothing here touches the file system.

use std::path::{Component, Path, PathBuf};

/// `path` with `.` removed and each `..` applied to the component before it.
pub fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    out
}

/// The path from directory `from` to `to`. Both must be absolute and
/// normalized.
pub fn relative(from: &Path, to: &Path) -> PathBuf {
    let from: Vec<Component<'_>> = from.components().collect();
    let to: Vec<Component<'_>> = to.components().collect();
    let common = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
    let mut out = PathBuf::new();
    for _ in common..from.len() {
        out.push("..");
    }
    for component in &to[common..] {
        out.push(component);
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    out
}

/// A path as it appears in a generated file: forward slashes on every
/// platform, so generated output does not depend on where it was made.
pub fn display(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_applies_parent_steps() {
        assert_eq!(normalize(Path::new("/a/b/../c/./d")), Path::new("/a/c/d"));
    }

    #[test]
    fn relative_walks_up_then_down() {
        let from = Path::new("/r/examples/chat/segments/front");
        assert_eq!(
            relative(from, Path::new("/r/crates/gdl")),
            Path::new("../../../../crates/gdl")
        );
        assert_eq!(
            relative(from, Path::new("/r/examples/chat/contract")),
            Path::new("../../contract")
        );
        assert_eq!(relative(from, from), Path::new("."));
    }

    #[test]
    fn display_uses_forward_slashes() {
        assert_eq!(display(Path::new("../../contract")), "../../contract");
    }
}
