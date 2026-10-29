//! The code and the book point only at things that stay in the repository.
//!
//! Plans and design rulings live outside the repository, and their numbers
//! mean nothing to a reader of the code. A comment that cites "ruling 151"
//! or `agent-docs/plan.md` dangles the day the plan moves, so the reason
//! goes into the comment itself, a test's doc comment, or a book chapter
//! under `docs/src/`.
//!
//! The book's lists of `error_type` values also name exactly the error
//! kinds the code has, so that a new kind cannot leave them stale.

use std::collections::BTreeSet;
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
        "/// The graph of pools.md section 3.1",
        "// ----- milestone 3 review, pass 1 -----",
        "// found in review m19-r12",
        "/// Counterexample 1 of review `pools-r2`: Route provisions Post first",
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
        "// the ruling class of a pool",
        "/// rulings are not cited here",
        "// a milestone of the stream",
        "See [Classes and pools](../concepts/pools.md).",
        "[Classes and pools](pools.md) covers them.",
        "let rem3 = 1;",
        "// a review of the drain, round two",
        "/// the pool-r2 table",
        "// the expr 2 form",
        "/// a pull request runs the book job",
        "// pull/push both work",
    ] {
        assert!(cites(line).is_none(), "false positive: {line}");
    }
}

/// The book's lists of `error_type` values, in the limitations chapter and
/// the metrics appendix, name every `gdl::ErrorKind` but `Cancelled`, and
/// nothing else. `Cancelled` has no value: the cancellation counters count
/// it (`error_slot` in `crates/gdl/src/meter.rs`).
#[test]
fn the_book_lists_every_error_type() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let read = |path: &str| {
        fs::read_to_string(root.join(path)).unwrap_or_else(|error| panic!("{path}: {error}"))
    };
    let mut kinds = error_kinds(&read("crates/gdl/src/error.rs"))
        .unwrap_or_else(|error| panic!("crates/gdl/src/error.rs: {error}"));
    assert!(
        kinds.remove("cancelled"),
        "no `ErrorKind` variants read from crates/gdl/src/error.rs: {kinds:?}"
    );

    let mut wrong = Vec::new();
    for (path, heading) in [
        (
            "docs/src/operations/limitations.md",
            "### The runtime's `error_type` values are gdl's error kinds",
        ),
        ("docs/src/appendix/metrics.md", "## `error_type` values"),
    ] {
        let listed = listed_values(&read(path), heading).unwrap_or_else(|| {
            panic!("{path}: no list of values this check can read under `{heading}`")
        });
        let missing: Vec<_> = kinds.difference(&listed).collect();
        let extra: Vec<_> = listed.difference(&kinds).collect();
        if !missing.is_empty() || !extra.is_empty() {
            wrong.push(format!(
                "{path}: kinds it does not list {missing:?}, values that are not kinds {extra:?}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "the book's `error_type` values differ from `ErrorKind`:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn the_error_type_check_reads_variants_and_lists() {
    let source = r#"
/// What failed.
#[derive(Debug)]
pub enum ErrorKind {
    /// Node code reported a failure.
    Node,
    DeadlineExceeded,
    OutcomeUnknown,
}
pub enum Other {
    Stray,
}
"#;
    assert_eq!(
        error_kinds(source),
        Ok(set(&["node", "deadline_exceeded", "outcome_unknown"]))
    );
    let tuple = source.replace("    Node,", "    Node(u8),");
    assert!(error_kinds(&tuple).is_err(), "{:?}", error_kinds(&tuple));

    let chapter = r#"
### Values
- **What:** `errors_total{error_type}` counts by kind: `node`, `codec`, and `routing`. Others count by step (`generate`, `publish_response`).
### Next
The kinds: `stray`.
"#;
    assert_eq!(
        listed_values(chapter, "### Values"),
        Some(set(&["node", "codec", "routing"]))
    );
    assert_eq!(listed_values(chapter, "### Missing"), None);
    assert_eq!(
        listed_values(
            "### Values\nNo list.\n### Next\nKinds: `stray`.\n",
            "### Values"
        ),
        None
    );
    assert_eq!(
        listed_values("### Values\nKinds: `node` then `codec`.\n", "### Values"),
        None
    );
}

#[test]
fn an_added_error_kind_with_a_trailing_comment_is_detected() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = fs::read_to_string(root.join("crates/gdl/src/error.rs")).expect("error source");
    let mutated = source.replacen(
        "    OutcomeUnknown,\n",
        "    OutcomeUnknown,\n    Unavailable, // Retry after capacity returns.\n",
        1,
    );
    assert_ne!(mutated, source, "the fixture must add a variant");
    assert_ne!(
        error_kinds(&mutated),
        error_kinds(&source),
        "an added variant with a trailing comment must change the parsed set or fail closed"
    );
}

#[test]
fn a_documented_error_type_after_and_is_detected() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut missed = Vec::new();
    for (path, heading) in [
        (
            "docs/src/operations/limitations.md",
            "### The runtime's `error_type` values are gdl's error kinds",
        ),
        ("docs/src/appendix/metrics.md", "## `error_type` values"),
    ] {
        let text = fs::read_to_string(root.join(path)).expect("book chapter");
        let listed = listed_values(&text, heading).expect("the real chapter has a list");
        let mutated = text.replacen(
            "`outcome_unknown`.",
            "`outcome_unknown` and `unavailable`.",
            1,
        );
        assert_ne!(mutated, text, "{path}: the fixture must append a value");
        if listed_values(&mutated, heading) == Some(listed) {
            missed.push(path);
        }
    }
    assert!(
        missed.is_empty(),
        "an extra value joined with and must change the parsed set or fail closed: {missed:?}"
    );
}

fn set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| value.to_string()).collect()
}

/// The variants of `pub enum ErrorKind` in `source`, in snake case, as the
/// `error_type` label spells them. A line of the enum that is not a
/// comment, an attribute, or a unit variant is an error, so that no
/// variant goes unread.
fn error_kinds(source: &str) -> Result<BTreeSet<String>, String> {
    let (_, body) = source
        .split_once("pub enum ErrorKind {")
        .ok_or("no `pub enum ErrorKind`")?;
    let mut kinds = BTreeSet::new();
    for line in body.lines().map(str::trim).take_while(|line| *line != "}") {
        let code = line.split_once("//").map_or(line, |(code, _)| code).trim();
        if code.is_empty() || code.starts_with('#') {
            continue;
        }
        let name = code
            .strip_suffix(',')
            .filter(|name| !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric()))
            .ok_or_else(|| format!("a line of `ErrorKind` this check cannot read: {line}"))?;
        let mut snake = String::new();
        for (at, c) in name.char_indices() {
            if c.is_ascii_uppercase() && at > 0 {
                snake.push('_');
            }
            snake.push(c.to_ascii_lowercase());
        }
        kinds.insert(snake);
    }
    Ok(kinds)
}

/// The values of the first list in the section under `heading`: the run of
/// code spans after the first `: `, joined by `, `, `, and `, or ` and `,
/// that ends its sentence. A run that ends any other way is not read, so
/// that a value joined some other way is not dropped unseen.
fn listed_values(text: &str, heading: &str) -> Option<BTreeSet<String>> {
    let (_, section) = text.split_once(&format!("{heading}\n"))?;
    let section = section
        .split("\n#")
        .next()
        .expect("split yields at least one part");
    let (_, mut rest) = section.split_once(": `")?;
    let mut values = BTreeSet::new();
    loop {
        let (value, after) = rest.split_once('`')?;
        values.insert(value.to_string());
        let next = [", and `", ", `", " and `"]
            .iter()
            .find_map(|join| after.strip_prefix(join));
        match next {
            Some(next) => rest = next,
            None => return after.starts_with('.').then_some(values),
        }
    }
}

/// What kind of plan reference `line` holds, if any.
fn cites(line: &str) -> Option<&'static str> {
    let lower = line.to_ascii_lowercase();
    // The book has a `concepts/pools.md` chapter; a link to it is a path
    // or a parenthesised link target, never a bare name.
    let bare = |name: &str| {
        lower
            .match_indices(name)
            .any(|(at, _)| !matches!(lower[..at].chars().last(), Some('/' | '(')))
    };
    if lower.contains("agent-docs") || bare("plan.md") || bare("pools.md") {
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
/// a name ending in a round (`review pools-r2`).
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
