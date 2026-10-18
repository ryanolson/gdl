//! Resolving a graph file's message names against the catalogue of the
//! crates the spec lists.

use std::collections::BTreeMap;

use gdl::{Catalogue, MessageInfo};

/// Resolve each name in `uses` (a message name and the nodes that declare
/// it) to one entry of `catalogue` from a package in `listed`. A pin maps a
/// name to the path of the entry it takes, and is how a name in two crates
/// is resolved. Every problem is reported.
pub(crate) fn resolve(
    uses: &BTreeMap<String, Vec<String>>,
    catalogue: &Catalogue,
    listed: &[String],
    pins: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, MessageInfo>, Vec<String>> {
    let offered: Vec<&MessageInfo> = catalogue
        .messages
        .iter()
        .filter(|m| listed.contains(&m.package))
        .collect();
    let mut problems = Vec::new();
    for package in listed {
        if !offered.iter().any(|m| &m.package == package) {
            problems.push(format!(
                "[messages] crate `{package}` offers no messages: derive `gdl::Message` on its message types, with the gdl that [gdl] names"
            ));
        }
    }
    for name in pins.keys() {
        if !uses.contains_key(name) {
            problems.push(format!(
                "[messages] pin `{name}`: the graph has no message `{name}`"
            ));
        }
    }

    let mut resolved = BTreeMap::new();
    for (name, users) in uses {
        let users = users.join(", ");
        if let Some(path) = pins.get(name) {
            match offered.iter().find(|m| m.path == *path) {
                Some(message) => {
                    resolved.insert(name.clone(), (*message).clone());
                }
                None => problems.push(format!(
                    "[messages] pin `{name}`: no listed crate defines `{path}`{}",
                    candidates(offered.iter().filter(|m| m.name == *name))
                        .map(|c| format!("; `{name}` is {c}"))
                        .unwrap_or_default()
                )),
            }
            continue;
        }
        let found: Vec<&MessageInfo> = offered
            .iter()
            .filter(|m| m.name == *name)
            .copied()
            .collect();
        match found.as_slice() {
            [message] => {
                resolved.insert(name.clone(), (*message).clone());
            }
            [] => problems.push(format!(
                "message `{name}` ({users}) is in no listed crate{}",
                missing_hint(name, &offered, catalogue, listed)
            )),
            several => problems.push(format!(
                "message `{name}` ({users}) is in more than one listed crate: {}; choose one under [messages] pin",
                several
                    .iter()
                    .map(|m| format!("`{}`", m.path))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
    if problems.is_empty() {
        Ok(resolved)
    } else {
        Err(problems)
    }
}

/// `` `a::X` `` or `` `a::X`, `b::X` ``, or `None` for no candidates.
fn candidates<'a>(found: impl Iterator<Item = &'a &'a MessageInfo>) -> Option<String> {
    let paths: Vec<String> = found.map(|m| format!("`{}`", m.path)).collect();
    (!paths.is_empty()).then(|| paths.join(", "))
}

/// What may help with a name that no listed crate defines: a crate that
/// defines it but is not listed, near names, or else everything offered.
fn missing_hint(
    name: &str,
    offered: &[&MessageInfo],
    catalogue: &Catalogue,
    listed: &[String],
) -> String {
    let mut unlisted: Vec<&str> = catalogue
        .messages
        .iter()
        .filter(|m| m.name == name && !listed.contains(&m.package))
        .map(|m| m.package.as_str())
        .collect();
    unlisted.dedup();
    if !unlisted.is_empty() {
        return format!(
            "; {} defines it, but is not in [messages] crates",
            unlisted
                .iter()
                .map(|p| format!("`{p}`"))
                .collect::<Vec<_>>()
                .join(" and ")
        );
    }
    let mut names: Vec<&str> = offered.iter().map(|m| m.name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    let near: Vec<&str> = names
        .iter()
        .copied()
        .filter(|candidate| is_near(name, candidate))
        .collect();
    match (near.as_slice(), names.is_empty()) {
        ([], true) => String::new(),
        ([], false) => format!("; the listed crates offer {}", quoted(&names)),
        (near, _) => format!("; did you mean {}?", quoted(near)),
    }
}

fn quoted(names: &[&str]) -> String {
    names
        .iter()
        .map(|n| format!("`{n}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A likely misspelling: the same name in another case, or a few edits
/// away (one per three characters, at least one).
fn is_near(name: &str, candidate: &str) -> bool {
    if name.eq_ignore_ascii_case(candidate) {
        return true;
    }
    let limit = (name.chars().count() / 3).max(1);
    edits(name, candidate) <= limit
}

/// Edit distance, counting a swap of two neighbouring characters as one
/// edit, as typing mistakes usually are (optimal string alignment).
fn edits(a: &str, b: &str) -> usize {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let width = b.len() + 1;
    let mut d = vec![0usize; (a.len() + 1) * width];
    for i in 0..=a.len() {
        for j in 0..=b.len() {
            d[i * width + j] = if i == 0 || j == 0 {
                i + j
            } else {
                let cost = usize::from(a[i - 1] != b[j - 1]);
                let mut best = (d[(i - 1) * width + j] + 1)
                    .min(d[i * width + j - 1] + 1)
                    .min(d[(i - 1) * width + j - 1] + cost);
                if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                    best = best.min(d[(i - 2) * width + j - 2] + 1);
                }
                best
            };
        }
    }
    d[a.len() * width + b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(package: &str, path: &str) -> MessageInfo {
        MessageInfo {
            package: package.into(),
            name: path.rsplit("::").next().expect("name").into(),
            path: path.into(),
            wire: true,
        }
    }

    fn catalogue(messages: &[(&str, &str)]) -> Catalogue {
        let mut messages: Vec<MessageInfo> =
            messages.iter().map(|(p, path)| message(p, path)).collect();
        messages.sort();
        Catalogue { messages }
    }

    fn uses(names: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        names
            .iter()
            .map(|(name, users)| {
                (
                    (*name).to_string(),
                    users.iter().map(|u| (*u).to_string()).collect(),
                )
            })
            .collect()
    }

    fn listed(packages: &[&str]) -> Vec<String> {
        packages.iter().map(|p| (*p).to_string()).collect()
    }

    fn pins(pins: &[(&str, &str)]) -> BTreeMap<String, String> {
        pins.iter()
            .map(|(n, p)| ((*n).to_string(), (*p).to_string()))
            .collect()
    }

    fn paths(resolved: &BTreeMap<String, MessageInfo>) -> Vec<(&str, &str)> {
        resolved
            .iter()
            .map(|(n, m)| (n.as_str(), m.path.as_str()))
            .collect()
    }

    /// The common case: one protocols crate holds every message, and a type
    /// in a nested module resolves by its name.
    #[test]
    fn one_protocols_crate_resolves_every_name() {
        let catalogue = catalogue(&[
            ("chat-protocols", "chat_protocols::Prompt"),
            ("chat-protocols", "chat_protocols::engine::Words"),
            ("chat-protocols", "chat_protocols::Unused"),
        ]);
        let resolved = resolve(
            &uses(&[("Prompt", &["A"]), ("Words", &["B", "C"])]),
            &catalogue,
            &listed(&["chat-protocols"]),
            &BTreeMap::new(),
        )
        .expect("resolved");
        assert_eq!(
            paths(&resolved),
            [
                ("Prompt", "chat_protocols::Prompt"),
                ("Words", "chat_protocols::engine::Words")
            ]
        );
    }

    #[test]
    fn names_resolve_across_several_crates() {
        let catalogue = catalogue(&[("front", "front::Prompt"), ("engine", "engine::Words")]);
        let resolved = resolve(
            &uses(&[("Prompt", &["A"]), ("Words", &["B"])]),
            &catalogue,
            &listed(&["front", "engine"]),
            &BTreeMap::new(),
        )
        .expect("resolved");
        assert_eq!(
            paths(&resolved),
            [("Prompt", "front::Prompt"), ("Words", "engine::Words")]
        );
    }

    #[test]
    fn a_name_in_two_crates_is_ambiguous_until_pinned() {
        let catalogue = catalogue(&[("a", "a::Chunk"), ("b", "b::inner::Chunk")]);
        let uses = uses(&[("Chunk", &["Frontend", "Number"])]);
        let both = listed(&["a", "b"]);
        let problems = resolve(&uses, &catalogue, &both, &BTreeMap::new()).expect_err("ambiguous");
        assert_eq!(
            problems,
            [
                "message `Chunk` (Frontend, Number) is in more than one listed crate: `a::Chunk`, `b::inner::Chunk`; choose one under [messages] pin"
            ]
        );
        let resolved = resolve(
            &uses,
            &catalogue,
            &both,
            &pins(&[("Chunk", "b::inner::Chunk")]),
        )
        .expect("pinned");
        assert_eq!(paths(&resolved), [("Chunk", "b::inner::Chunk")]);
    }

    /// A pin binds a name to any listed type, so a graph's name need not be
    /// the type's.
    #[test]
    fn a_pin_may_bind_a_name_to_a_type_of_another_name() {
        let catalogue = catalogue(&[("a", "a::Piece")]);
        let resolved = resolve(
            &uses(&[("Text", &["F"])]),
            &catalogue,
            &listed(&["a"]),
            &pins(&[("Text", "a::Piece")]),
        )
        .expect("pinned");
        assert_eq!(paths(&resolved), [("Text", "a::Piece")]);
    }

    #[test]
    fn a_pin_must_name_a_listed_type_and_a_used_name() {
        let catalogue = catalogue(&[("a", "a::Chunk"), ("unlisted", "unlisted::Other")]);
        let problems = resolve(
            &uses(&[("Chunk", &["A"])]),
            &catalogue,
            &listed(&["a"]),
            &pins(&[("Chunk", "a::Chunck"), ("Other", "unlisted::Other")]),
        )
        .expect_err("bad pins");
        assert_eq!(
            problems,
            [
                "[messages] pin `Other`: the graph has no message `Other`",
                "[messages] pin `Chunk`: no listed crate defines `a::Chunck`; `Chunk` is `a::Chunk`",
            ]
        );
    }

    #[test]
    fn a_missing_name_says_what_might_be_meant() {
        let catalogue = catalogue(&[
            ("asr", "asr::Audio"),
            ("asr", "asr::Session"),
            ("dsp", "dsp::Pcm"),
        ]);
        let problems = resolve(
            &uses(&[
                ("Audoi", &["Client"]),
                ("Pcm", &["Resample", "Recognizer"]),
                ("Transcript", &["Client"]),
            ]),
            &catalogue,
            &listed(&["asr"]),
            &BTreeMap::new(),
        )
        .expect_err("missing");
        assert_eq!(
            problems,
            [
                "message `Audoi` (Client) is in no listed crate; did you mean `Audio`?",
                "message `Pcm` (Resample, Recognizer) is in no listed crate; `dsp` defines it, but is not in [messages] crates",
                "message `Transcript` (Client) is in no listed crate; the listed crates offer `Audio`, `Session`",
            ]
        );
    }

    /// A listed crate that registers nothing derives nothing, or registers
    /// in another gdl's catalogue: either way the spec is wrong.
    #[test]
    fn a_listed_crate_that_offers_nothing_is_reported() {
        let catalogue = catalogue(&[("a", "a::Chunk")]);
        let problems = resolve(
            &uses(&[("Chunk", &["A"])]),
            &catalogue,
            &listed(&["a", "empty"]),
            &BTreeMap::new(),
        )
        .expect_err("empty crate");
        assert_eq!(
            problems,
            [
                "[messages] crate `empty` offers no messages: derive `gdl::Message` on its message types, with the gdl that [gdl] names"
            ]
        );
    }

    #[test]
    fn near_names_are_a_case_or_a_few_edits_away() {
        assert!(is_near("prompt", "Prompt"));
        assert!(is_near("Pcm", "Pcmm"));
        assert!(is_near("Audoi", "Audio"));
        assert!(!is_near("Pcm", "Text"));
        assert_eq!(edits("kitten", "sitting"), 3);
        assert_eq!(edits("Audoi", "Audio"), 1);
    }
}
