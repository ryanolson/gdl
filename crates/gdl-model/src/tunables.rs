//! The glibc tunables that every launcher gdl renders gives a segment
//! process. From glibc 2.43, malloc on AArch64 asks for transparent huge
//! pages on its heap and on each thread's arena, and each first touch of a
//! 2 MB block can then compact memory before the fault returns, which
//! stalls a start where memory is fragmented. glibc reads its tunables only
//! from the environment, before `main`, so the launcher sets them
//! (`docs/src/appendix/spec-reference.md#binary`).

/// The variable glibc reads its tunables from.
pub const GLIBC_TUNABLES: &str = "GLIBC_TUNABLES";

/// The tunable that keeps glibc's malloc from asking for transparent huge
/// pages. An older glibc ignores it, and glibc's default on x86_64 is the
/// same, so every architecture gets it.
pub const MALLOC_TUNABLE: &str = "glibc.malloc.hugetlb=0";

/// The `GLIBC_TUNABLES` value a launcher gives a segment, when the user's
/// own is `current`: [`MALLOC_TUNABLE`] when the user set none, and the
/// user's value, byte for byte, when they set one. gdl never edits a
/// user's list: glibc refuses some lists as a whole, and its loaders
/// differ in what they accept, so an edit can turn off what the user set
/// ([`tunables_warning`] says when a user's value may leave huge pages
/// on).
///
/// ```
/// use gdl_model::segment_tunables;
/// assert_eq!(segment_tunables(None), "glibc.malloc.hugetlb=0");
/// assert_eq!(
///     segment_tunables(Some("glibc.malloc.arena_max=4")),
///     "glibc.malloc.arena_max=4"
/// );
/// ```
pub fn segment_tunables(current: Option<&str>) -> String {
    current.unwrap_or(MALLOC_TUNABLE).to_string()
}

/// The warning a render prints when a user's `GLIBC_TUNABLES`, `value`,
/// set for `whom`, does not mention `glibc.malloc.hugetlb`: gdl added
/// nothing to it, and the segment's malloc may ask for huge pages. `None`
/// when it mentions the tunable. A check of the text, not glibc's parse:
/// it is advice, and the user's value is used as given either way.
pub fn tunables_warning(whom: &str, value: &str) -> Option<String> {
    let (name, _) = MALLOC_TUNABLE
        .split_once('=')
        .unwrap_or((MALLOC_TUNABLE, ""));
    (!value.contains(name)).then(|| {
        format!(
            "warning: {whom} sets {GLIBC_TUNABLES} to `{value}`, and gdl added nothing to it: include `{MALLOC_TUNABLE}` in it, or on arm64 with glibc 2.43 or later a segment's malloc asks for transparent huge pages, which can stall its start"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A user who sets the variable owns it: their value passes through
    /// byte for byte, whatever it holds, even a list glibc refuses or one
    /// that turns huge pages on.
    #[test]
    fn a_users_value_passes_through() {
        assert_eq!(segment_tunables(None), "glibc.malloc.hugetlb=0");
        let repeated = vec!["glibc.malloc.perturb=1"; 32].join(":");
        for current in [
            "",
            "glibc.malloc.arena_max=4",
            "glibc.malloc.hugetlb=1",
            "glibc.malloc.hugetlb=0:",
            "glibc.malloc.hugetlb",
            "glibc.malloc.hugetlb=+1",
            "glibc.malloc.hugetlb= 1",
            "glibc.malloc.hugetlb=1:glibc.malloc.hugetlb=",
            "x==y:",
            repeated.as_str(),
        ] {
            assert_eq!(segment_tunables(Some(current)), current);
        }
    }

    /// A value that does not mention the tunable gets a warning that names
    /// the setter, the value, and what to add. One that mentions it gets
    /// none, whatever it sets.
    #[test]
    fn a_value_without_the_tunable_is_warned_about() {
        let line =
            tunables_warning("cluster `use1`", "glibc.malloc.arena_max=4").expect("a warning");
        for part in [
            "cluster `use1` sets GLIBC_TUNABLES to `glibc.malloc.arena_max=4`",
            "gdl added nothing",
            "`glibc.malloc.hugetlb=0`",
        ] {
            assert!(line.contains(part), "{part} in {line}");
        }
        assert!(tunables_warning("x", "").is_some());
        for value in [
            "glibc.malloc.hugetlb=0",
            "glibc.malloc.hugetlb=1",
            "glibc.malloc.arena_max=4:glibc.malloc.hugetlb",
        ] {
            assert_eq!(tunables_warning("x", value), None, "{value}");
        }
    }
}
