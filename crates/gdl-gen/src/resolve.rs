//! Resolving a graph file's message names against what the listed crates
//! offer, as the source reader found it.
//!
//! A listed crate offers the messages it defines and the messages it
//! exports. A name resolves to a message's origin, so one message offered
//! by two listed crates (its own and one that exports it) is one candidate,
//! not two.

use std::collections::BTreeMap;

use gdl_extract::{Offer, Origin};

/// A name resolved: the message it is, and the public path the contract
/// names it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// The message's identity: where it is defined.
    pub origin: Origin,
    /// A public path through a listed crate: an export's, when the crates
    /// offer one, else the definition's.
    pub path: String,
    /// Whether it is a wire type.
    pub wire: bool,
}

/// One way a listed crate offers a message.
struct Candidate {
    name: String,
    origin: Origin,
    path: String,
    wire: bool,
    /// Whether it is an export, and whether its own package exports it:
    /// what decides the path the contract names it by.
    export: Option<bool>,
}

/// Resolve each name in `uses` (a message name and the nodes that declare
/// it) to one message that `offers` hold. A pin maps a name to a public
/// path that the offers hold, and is how a name with two messages is
/// resolved, or a name given to a message of another name. Every problem
/// is reported.
pub(crate) fn resolve(
    uses: &BTreeMap<String, Vec<String>>,
    offers: &[Offer],
    pins: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, Resolved>, Vec<String>> {
    let mut problems = Vec::new();
    let mut candidates = Vec::new();
    for offer in offers {
        if offer.messages.is_empty() && offer.exports.is_empty() {
            problems.push(format!(
                "[messages] crate `{}` offers no messages: derive `Message` on its message types, or export them",
                offer.package
            ));
        }
        for m in &offer.messages {
            candidates.push(Candidate {
                name: m.name.clone(),
                origin: Origin {
                    package: offer.package.clone(),
                    version: offer.version.clone(),
                    source: offer.source.clone(),
                    path: m.path.clone(),
                },
                path: m.path.clone(),
                wire: m.wire,
                export: None,
            });
        }
        for e in &offer.exports {
            candidates.push(Candidate {
                name: e.name.clone(),
                origin: e.origin.clone(),
                path: e.path.clone(),
                wire: e.wire,
                export: Some(e.origin.package == offer.package),
            });
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
            let Some(pinned) = candidates.iter().find(|c| c.path == *path) else {
                let named: Vec<String> = candidates
                    .iter()
                    .filter(|c| c.name == *name)
                    .map(|c| format!("`{}`", c.path))
                    .collect();
                problems.push(format!(
                    "[messages] pin `{name}`: no listed crate offers `{path}`{}",
                    if named.is_empty() {
                        String::new()
                    } else {
                        format!("; `{name}` is {}", named.join(", "))
                    }
                ));
                continue;
            };
            // The pinned message, through the path the pin gives.
            resolved.insert(
                name.clone(),
                Resolved {
                    origin: pinned.origin.clone(),
                    path: pinned.path.clone(),
                    wire: pinned.wire,
                },
            );
            continue;
        }
        let found: Vec<&Candidate> = candidates.iter().filter(|c| c.name == *name).collect();
        let mut origins: Vec<&Origin> = found.iter().map(|c| &c.origin).collect();
        origins.sort();
        origins.dedup();
        match origins.as_slice() {
            [] => problems.push(format!(
                "message `{name}` ({users}) is in no listed crate{}",
                missing_hint(name, &candidates)
            )),
            [_] => {
                // An export first: its path is public, though the
                // definition's may not be; the defining package's own
                // export before another's.
                let best = found
                    .iter()
                    .min_by_key(|c| match c.export {
                        Some(true) => 0,
                        Some(false) => 1,
                        None => 2,
                    })
                    .expect("one candidate at least");
                resolved.insert(
                    name.clone(),
                    Resolved {
                        origin: best.origin.clone(),
                        path: best.path.clone(),
                        wire: best.wire,
                    },
                );
            }
            several => problems.push(format!(
                "message `{name}` ({users}) is more than one message in the listed crates: {}; choose one under [messages] pin",
                several
                    .iter()
                    .map(|o| format!("`{}` ({} {})", o.path, o.package, o.version))
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

/// What may help with a name that no listed crate offers: near names, or
/// else every name offered.
fn missing_hint(name: &str, candidates: &[Candidate]) -> String {
    let mut names: Vec<&str> = candidates.iter().map(|c| c.name.as_str()).collect();
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
pub(crate) mod tests {
    use gdl_extract::{ExportEntry, MessageEntry};

    use super::*;

    /// An offer of package `package` 0.1.0 from a path, defining the
    /// messages at `defined` and exporting `exported` (name, public path,
    /// origin package, origin path).
    pub(crate) fn offer(
        package: &str,
        defined: &[&str],
        exported: &[(&str, &str, &str, &str)],
    ) -> Offer {
        Offer {
            package: package.to_string(),
            version: "0.1.0".into(),
            source: format!("path+file:///crates/{package}"),
            messages: defined
                .iter()
                .map(|path| MessageEntry {
                    name: path.rsplit("::").next().expect("name").to_string(),
                    path: (*path).to_string(),
                    wire: true,
                })
                .collect(),
            exports: exported
                .iter()
                .map(|(name, path, from, origin)| ExportEntry {
                    name: (*name).to_string(),
                    path: (*path).to_string(),
                    origin: Origin {
                        package: (*from).to_string(),
                        version: "0.1.0".into(),
                        source: format!("path+file:///crates/{from}"),
                        path: (*origin).to_string(),
                    },
                    wire: true,
                })
                .collect(),
            nodes: Vec::new(),
        }
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

    fn pins(pins: &[(&str, &str)]) -> BTreeMap<String, String> {
        pins.iter()
            .map(|(n, p)| ((*n).to_string(), (*p).to_string()))
            .collect()
    }

    fn paths(resolved: &BTreeMap<String, Resolved>) -> Vec<(&str, &str, &str)> {
        resolved
            .iter()
            .map(|(n, r)| (n.as_str(), r.path.as_str(), r.origin.path.as_str()))
            .collect()
    }

    /// The common case: one protocols crate holds every message, and a type
    /// in a nested module resolves by its name.
    #[test]
    fn one_protocols_crate_resolves_every_name() {
        let offers = [offer(
            "chat",
            &["chat::Prompt", "chat::engine::Words", "chat::Unused"],
            &[],
        )];
        let resolved = resolve(
            &uses(&[("Prompt", &["A"]), ("Words", &["B", "C"])]),
            &offers,
            &BTreeMap::new(),
        )
        .expect("resolved");
        assert_eq!(
            paths(&resolved),
            [
                ("Prompt", "chat::Prompt", "chat::Prompt"),
                ("Words", "chat::engine::Words", "chat::engine::Words")
            ]
        );
    }

    /// An export is the message it names: a facade crate alone resolves to
    /// the origin, through its own public path, and a facade listed with
    /// the defining crate is no ambiguity.
    #[test]
    fn an_export_resolves_to_its_origin_through_a_public_path() {
        let facade = offer(
            "facade",
            &[],
            &[("Words", "facade::Words", "chat", "chat::engine::Words")],
        );
        let alone = resolve(
            &uses(&[("Words", &["A"])]),
            std::slice::from_ref(&facade),
            &BTreeMap::new(),
        )
        .expect("alone");
        assert_eq!(
            paths(&alone),
            [("Words", "facade::Words", "chat::engine::Words")]
        );
        assert_eq!(alone["Words"].origin.package, "chat");
        let both = resolve(
            &uses(&[("Words", &["A"])]),
            &[offer("chat", &["chat::engine::Words"], &[]), facade],
            &BTreeMap::new(),
        )
        .expect("both");
        assert_eq!(
            paths(&both),
            [("Words", "facade::Words", "chat::engine::Words")]
        );
    }

    /// A crate's own export comes before its private definition path, and
    /// before another crate's export.
    #[test]
    fn the_defining_crates_export_names_a_message() {
        let offers = [
            offer(
                "chat",
                &["chat::hidden::Piece"],
                &[("Piece", "chat::Piece", "chat", "chat::hidden::Piece")],
            ),
            offer(
                "facade",
                &[],
                &[("Piece", "facade::Piece", "chat", "chat::hidden::Piece")],
            ),
        ];
        let resolved =
            resolve(&uses(&[("Piece", &["A"])]), &offers, &BTreeMap::new()).expect("resolved");
        assert_eq!(resolved["Piece"].path, "chat::Piece");
    }

    #[test]
    fn a_name_of_two_messages_is_ambiguous_until_pinned() {
        let offers = [
            offer("a", &["a::Chunk"], &[]),
            offer("b", &["b::inner::Chunk"], &[]),
        ];
        let uses = uses(&[("Chunk", &["Frontend", "Number"])]);
        let problems = resolve(&uses, &offers, &BTreeMap::new()).expect_err("ambiguous");
        assert_eq!(
            problems,
            [
                "message `Chunk` (Frontend, Number) is more than one message in the listed crates: `a::Chunk` (a 0.1.0), `b::inner::Chunk` (b 0.1.0); choose one under [messages] pin"
            ]
        );
        let resolved =
            resolve(&uses, &offers, &pins(&[("Chunk", "b::inner::Chunk")])).expect("pinned");
        assert_eq!(resolved["Chunk"].origin.package, "b");
    }

    /// A pin binds a name to any offered message, so a graph's name need
    /// not be the type's.
    #[test]
    fn a_pin_may_bind_a_name_to_a_message_of_another_name() {
        let offers = [offer("a", &["a::Piece"], &[])];
        let resolved = resolve(
            &uses(&[("Text", &["F"])]),
            &offers,
            &pins(&[("Text", "a::Piece")]),
        )
        .expect("pinned");
        assert_eq!(resolved["Text"].path, "a::Piece");
    }

    #[test]
    fn a_pin_must_name_an_offered_path_and_a_used_name() {
        let offers = [offer("a", &["a::Chunk"], &[])];
        let problems = resolve(
            &uses(&[("Chunk", &["A"])]),
            &offers,
            &pins(&[("Chunk", "a::Chunck"), ("Other", "a::Chunk")]),
        )
        .expect_err("bad pins");
        assert_eq!(
            problems,
            [
                "[messages] pin `Other`: the graph has no message `Other`",
                "[messages] pin `Chunk`: no listed crate offers `a::Chunck`; `Chunk` is `a::Chunk`",
            ]
        );
    }

    #[test]
    fn a_missing_name_says_what_might_be_meant() {
        let offers = [offer("asr", &["asr::Audio", "asr::Session"], &[])];
        let problems = resolve(
            &uses(&[("Audoi", &["Client"]), ("Transcript", &["Client"])]),
            &offers,
            &BTreeMap::new(),
        )
        .expect_err("missing");
        assert_eq!(
            problems,
            [
                "message `Audoi` (Client) is in no listed crate; did you mean `Audio`?",
                "message `Transcript` (Client) is in no listed crate; the listed crates offer `Audio`, `Session`",
            ]
        );
    }

    #[test]
    fn a_listed_crate_that_offers_nothing_is_reported() {
        let offers = [offer("a", &["a::Chunk"], &[]), offer("empty", &[], &[])];
        let problems = resolve(&uses(&[("Chunk", &["A"])]), &offers, &BTreeMap::new())
            .expect_err("empty crate");
        assert_eq!(
            problems,
            [
                "[messages] crate `empty` offers no messages: derive `Message` on its message types, or export them"
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
