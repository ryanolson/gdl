//! Path arithmetic. Generated manifests hold paths relative to the
//! generated crate, so the output is the same wherever it is generated from.
//! Paths are resolved as the file system resolves them ([`resolve`]) before
//! any comparison: lexically, `link/..` is the directory holding `link`,
//! while the file system takes it to the parent of `link`'s target.

use std::path::{Component, Path, PathBuf};

/// `path` as the file system resolves it: absolute, symlinks followed in
/// the part that exists, the rest (which cannot hold a symlink) applied
/// lexically.
pub fn resolve(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let parts: Vec<Component<'_>> = absolute.components().collect();
    for split in (1..=parts.len()).rev() {
        let existing: PathBuf = parts[..split].iter().collect();
        if let Ok(mut resolved) = existing.canonicalize() {
            resolved.extend(&parts[split..]);
            return Ok(normalize(&resolved));
        }
    }
    Ok(normalize(&absolute))
}

/// `path` with `.` removed and each `..` applied to the component before it.
pub fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                // A step up from a name removes the name.
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                // Above the root is the root.
                Some(Component::RootDir | Component::Prefix(_)) => {}
                // Nothing left to remove: the step stays.
                _ => out.push(".."),
            },
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
    let mut text = String::new();
    for component in path.components() {
        // The root is a separator of its own.
        if !text.is_empty() && !text.ends_with('/') {
            text.push('/');
        }
        match component {
            std::path::Component::RootDir => text.push('/'),
            other => text.push_str(&other.as_os_str().to_string_lossy()),
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_applies_parent_steps() {
        assert_eq!(normalize(Path::new("/a/b/../c/./d")), Path::new("/a/c/d"));
    }

    #[test]
    fn normalize_keeps_parent_steps_it_cannot_apply() {
        assert_eq!(
            normalize(Path::new("../../nodes")),
            Path::new("../../nodes")
        );
        assert_eq!(normalize(Path::new("a/../../b")), Path::new("../b"));
        assert_eq!(normalize(Path::new("/../a")), Path::new("/a"));
    }

    #[cfg(unix)]
    #[test]
    fn resolving_follows_a_symlink_before_a_parent_step() {
        let root = std::env::temp_dir().join(format!("gdl-gen-resolve-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("vendor/a")).expect("dir");
        std::fs::create_dir_all(root.join("vendor/nodes")).expect("dir");
        std::fs::create_dir_all(root.join("project")).expect("dir");
        std::os::unix::fs::symlink(root.join("vendor/a"), root.join("project/link")).expect("link");
        let root = root.canonicalize().expect("root");
        // `link/..` is `vendor`, as the file system resolves it.
        assert_eq!(
            resolve(&root.join("project/link/../nodes")).expect("resolve"),
            root.join("vendor/nodes")
        );
        // A path that does not exist yet resolves through its existing part.
        assert_eq!(
            resolve(&root.join("project/link/../out/front")).expect("resolve"),
            root.join("vendor/out/front")
        );
        let _ = std::fs::remove_dir_all(&root);
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

    /// An exported manifest names a crate outside the archive by its
    /// absolute path.
    #[test]
    fn display_writes_an_absolute_path_with_one_root() {
        assert_eq!(display(Path::new("/work/nodes")), "/work/nodes");
    }
}
