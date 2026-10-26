//! The code and the book point only at things that stay in the repository.
//!
//! Plans and design rulings live outside the repository, and their numbers
//! mean nothing to a reader of the code. A comment that cites "ruling 151"
//! or `agent-docs/plan.md` dangles the day the plan moves, so the reason
//! goes into the comment itself, a test's doc comment, or a book chapter
//! under `docs/src/`.

use std::fs;
use std::path::{Path, PathBuf};

/// Directories and files scanned, relative to the workspace root.
const ROOTS: &[&str] = &["crates", "examples", "docs/src", "README.md"];

/// Files that describe where text goes, and so must name `agent-docs/`.
const ALLOWED: &[&str] = &["docs/src/development/documentation.md"];

#[test]
fn no_comment_or_chapter_cites_a_plan_or_a_ruling() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = root.canonicalize().expect("workspace root");
    let mut files = Vec::new();
    for entry in ROOTS {
        collect(&root.join(entry), &mut files);
    }

    let mut hits = Vec::new();
    for file in &files {
        let relative = file.strip_prefix(&root).expect("under the root");
        let relative = relative.to_string_lossy().replace('\\', "/");
        if ALLOWED.contains(&relative.as_str()) || relative.ends_with("tests/references.rs") {
            continue;
        }
        let Ok(text) = fs::read_to_string(file) else {
            continue;
        };
        for (number, line) in text.lines().enumerate() {
            if let Some(what) = cites(line) {
                hits.push(format!(
                    "{relative}:{}: {what}: {}",
                    number + 1,
                    line.trim()
                ));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "{} references to plans or rulings; state the reason or link a book chapter:\n{}",
        hits.len(),
        hits.join("\n")
    );
}

#[test]
fn the_check_sees_each_kind_of_reference() {
    for line in [
        "// the drain waits (ruling 151).",
        "/// a trick the reader does not chase (ruling Q9), and",
        "// acknowledged while the rendezvous waits (ruling",
        "//! The design is in `agent-docs/plan.md`.",
        "/// The graph of cells.md section 3.1",
        "// ----- milestone 3 review, pass 1 -----",
        "// found in review m19-r12",
        "/// Counterexample 1 of review `cells-r2`: Route provisions Post first",
        "#[ignore = \"B-2 (review round 2): the hook names no services\"]",
        "// fixed in PR #312",
        "/// see PR 45 for the reason",
        "// moved here by pull request #7",
        "// https://github.com/NVIDIA-dev/dynamo-gdl/pull/12",
        "/// See PR `#312` for why the drain waits.",
        "The drain moved in PR **#312**.",
    ] {
        assert!(cites(line).is_some(), "missed: {line}");
    }
    for line in [
        "// the ruling class of a cell",
        "/// rulings are not cited here",
        "// a milestone of the stream",
        "See [Classes and cells](../concepts/cells.md).",
        "[Classes and cells](cells.md) covers them.",
        "let rem3 = 1;",
        "// a review of the drain, round two",
        "/// the cell-r2 table",
        "// the expr 2 form",
        "/// a pull request runs the book job",
        "// pull/push both work",
    ] {
        assert!(cites(line).is_none(), "false positive: {line}");
    }
}

/// What kind of plan reference `line` holds, if any.
fn cites(line: &str) -> Option<&'static str> {
    let lower = line.to_ascii_lowercase();
    // The book has a `concepts/cells.md` chapter; a link to it is a path
    // or a parenthesised link target, never a bare name.
    let bare = |name: &str| {
        lower
            .match_indices(name)
            .any(|(at, _)| !matches!(lower[..at].chars().last(), Some('/' | '(')))
    };
    if lower.contains("agent-docs") || bare("plan.md") || bare("cells.md") {
        return Some("plan document");
    }
    if after(&lower, "ruling ", |rest| {
        let rest = rest.strip_prefix('q').unwrap_or(rest);
        rest.starts_with(|c: char| c.is_ascii_digit())
    }) || lower.trim_end().ends_with("(ruling")
    {
        return Some("ruling number");
    }
    if after(&lower, "milestone ", |rest| {
        rest.starts_with(|c: char| c.is_ascii_digit())
    }) {
        return Some("milestone label");
    }
    if review_id(&lower) || named_review(&lower) {
        return Some("review id");
    }
    if pr_number(&lower) {
        return Some("PR number");
    }
    None
}

/// A pull request cited by number: `PR 42`, `PR #42`, `pull request #42`,
/// or a `/pull/42` link, with or without Markdown code or emphasis.
fn pr_number(line: &str) -> bool {
    let number = |rest: &str| {
        rest.trim_start_matches(['`', '*', '_', '#'])
            .starts_with(|c: char| c.is_ascii_digit())
    };
    let word = |needle: &str| {
        line.match_indices(needle).any(|(at, _)| {
            !line[..at].ends_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
                && number(&line[at + needle.len()..])
        })
    };
    word("pr ") || word("pr#") || word("pull request ") || after(line, "/pull/", number)
}

/// Whether some occurrence of `needle` in `haystack` is followed by text
/// that satisfies `then`.
fn after(haystack: &str, needle: &str, then: impl Fn(&str) -> bool) -> bool {
    haystack
        .match_indices(needle)
        .any(|(at, _)| then(&haystack[at + needle.len()..]))
}

/// A review named after "review": a round number (`review round 2`) or
/// a name ending in a round (`review cells-r2`).
fn named_review(line: &str) -> bool {
    let digit = |rest: &str| rest.starts_with(|c: char| c.is_ascii_digit());
    after(line, "review round ", digit)
        || after(line, "review ", |rest| {
            let name: String = rest
                .trim_start_matches('`')
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
                .collect();
            name.rsplit_once("-r")
                .is_some_and(|(_, round)| !round.is_empty() && digit(round))
        })
}

/// A review id such as `m19-r12` or `m10-18-r6`, standing alone as a word.
fn review_id(line: &str) -> bool {
    line.split(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
        .any(|word| {
            let Some(rest) = word.strip_prefix('m') else {
                return false;
            };
            let mut parts = rest.split('-');
            let numbered = |part: Option<&str>| {
                part.is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
            };
            if !numbered(parts.next()) {
                return false;
            }
            let rest: Vec<&str> = parts.collect();
            match rest.as_slice() {
                [review] => review.strip_prefix('r').is_some_and(|r| numbered(Some(r))),
                [range, review] => {
                    numbered(Some(range))
                        && review.strip_prefix('r').is_some_and(|r| numbered(Some(r)))
                }
                _ => false,
            }
        })
}

fn collect(path: &Path, files: &mut Vec<PathBuf>) {
    if path.is_file() {
        files.push(path.to_path_buf());
        return;
    }
    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "target" || name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            collect(&path, files);
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("rs" | "md" | "toml" | "gdl" | "js" | "cjs" | "html")
        ) {
            files.push(path);
        }
    }
}
