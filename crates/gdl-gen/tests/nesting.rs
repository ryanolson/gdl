//! A graph file's types nested to the depth bound and one past it, read on
//! a thread with the 2 MiB stack of a tokio blocking thread, which is where
//! the composer reads a draft's graph.

use std::path::Path;

use gdl_gen::{Format, Offers, Sources, Spec};
use gdl_syntax::MAX_DEPTH;

/// A type of each kind that syn recurses on, or nests a chain of, `depth`
/// levels deep as the bound counts them.
fn shapes(depth: usize) -> Vec<(&'static str, String)> {
    let nested =
        |open: &str, close: &str| format!("{}Prompt{}", open.repeat(depth), close.repeat(depth));
    // The array's brackets are the first level of the rest.
    let length = |prefix: &str, value: &str, suffix: &str| {
        let steps = depth - 1;
        format!(
            "[Prompt; {}{value}{}]",
            prefix.repeat(steps),
            suffix.repeat(steps)
        )
    };
    vec![
        ("type arguments", nested("Identity<", ">")),
        ("parentheses", nested("(", ")")),
        ("tuples", nested("(", ",)")),
        ("slices", nested("[", "]")),
        ("references", nested("&", "")),
        ("pointers", nested("*const ", "")),
        ("trait objects", nested("dyn A<", ">")),
        ("fn arguments", nested("fn(", ")")),
        ("fn returns", nested("fn() -> ", "")),
        ("turbofish", length("f::<", "x", ">") + "()"),
        ("negation", length("-", "1", "")),
        ("return", length("return ", "1", "")),
        ("blocks", length("{", "1", "}")),
        // A closure's `|` is a level, and the `|` that ends its
        // parameters a step.
        (
            "closures",
            format!("[Prompt; {}1]", "|a, b| ".repeat((depth - 1) * 8 / 9)),
        ),
        // The innermost `if`'s block is a level deeper than its `if`.
        (
            "conditions",
            format!(
                "[Prompt; {}a{}]",
                "if ".repeat(depth - 2),
                " {}".repeat(depth - 2)
            ),
        ),
        // syn recurses on an assignment's right side, a prefix's operand,
        // and a range's end: a level each.
        ("assignments", length("a = ", "1", "")),
        ("compound assignments", length("a += ", "1", "")),
        ("ranges", length(".. ", "1", "")),
        ("negations", length("!", "a", "")),
        ("dereferences", length("*", "a", "")),
        ("borrows", length("&", "a", "")),
        ("mutable borrows", length("&mut ", "a", "")),
        // A `let` in a condition is a level for its `=`, and its `&&` a
        // step; the block is a level deeper than the last `let`.
        (
            "let chains",
            format!(
                "[Prompt; if {}{{}}]",
                vec!["let a = 1"; (8 * depth - 23) / 9].join(" && ") + " "
            ),
        ),
        // A chain's step is an eighth of a level, inside the array's.
        (
            "casts",
            format!("[Prompt; a{}]", " as u8".repeat((depth - 1) * 8)),
        ),
        (
            "try operators",
            format!("[Prompt; a{}]", "?".repeat((depth - 1) * 8)),
        ),
        (
            "binary operators",
            format!("[Prompt; a{}]", " + a".repeat((depth - 1) * 8)),
        ),
        (
            "method calls",
            format!("[Prompt; a{}]", ".f".repeat((depth - 1) * 8)),
        ),
        // A call's own brackets are a level deeper than its steps.
        (
            "calls",
            format!("[Prompt; f{}]", "()".repeat((depth - 2) * 8)),
        ),
        // An `if` is a level, and its blocks one more.
        (
            "else-if chains",
            format!(
                "[Prompt; if a {{ 1 }} {}else {{ 0 }}]",
                "else if a { 1 } ".repeat((depth - 3) * 8)
            ),
        ),
    ]
}

/// The spec and graph of a graph whose identity node takes `ty`. Nothing
/// follows the type there: in a signature, the `->` after a type is a level
/// of its own, and would put a type of operators one level deeper.
fn spec(ty: &str, dir: &Path) -> Spec {
    let graph = format!(
        "name = \"deep\", schema = 1;\n\
         source Front: Prompt -> stream Reply;\n\
         identity Pass: request {ty};\n\
         sink Engine: Prompt -> stream Reply;\n\
         topology: Front => Pass => Engine;\n"
    );
    Spec::from_text(
        "[graph]\npath = \"deep.gdl\"\n\
         [messages]\ncrates = [\"protocols@0.1.0\"]\n\
         [gdl]\nversion = \"0.1.0\"\n\
         [segment.all]\nnodes = [\"Front\", \"Pass\", \"Engine\"]\n",
        &graph,
        dir,
    )
    .expect("spec")
}

/// `f`, run on a 2 MiB thread: a stack overflow aborts the test binary.
fn on_small_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(f)
        .expect("thread")
        .join()
        .expect("the generator returned")
}

fn temp() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("gdl-nesting-")
        .tempdir()
        .expect("dir")
}

/// At the bound, syn parses each kind of type without overflowing the
/// stack, and the generator refuses it as what it is: not a message name.
/// The deepest kinds per level (type arguments, a turbofish) overflowed a
/// debug build's 2 MiB stack past 37 levels.
#[test]
fn a_type_at_the_bound_reads_on_a_small_stack() {
    let dir = temp();
    for (kind, ty) in shapes(MAX_DEPTH) {
        let spec = spec(&ty, dir.path());
        let target = dir.path().join("target");
        let error = on_small_stack(move || gdl_gen::resolve(&spec, &target))
            .err()
            .unwrap_or_else(|| panic!("{kind}: resolved"))
            .to_string();
        assert!(!error.contains("levels deep"), "{kind}: {error}");
        // syn reads the turbofish to its end, then wants a `;` there.
        let expected = if kind == "turbofish" {
            "expected `;`"
        } else {
            "not a message name"
        };
        assert!(error.contains(expected), "{kind}: {error}");
    }
}

/// One level past the bound, each kind is refused before syn reads it, at
/// the token that goes too deep, with the file, line, and column.
#[test]
fn a_type_one_past_the_bound_is_refused_where_it_goes_too_deep() {
    let dir = temp();
    for (kind, ty) in shapes(MAX_DEPTH + 1) {
        let spec = spec(&ty, dir.path());
        let target = dir.path().join("target");
        let error = on_small_stack(move || gdl_gen::resolve(&spec, &target))
            .err()
            .unwrap_or_else(|| panic!("{kind}: resolved"))
            .to_string();
        assert!(error.contains("deep.gdl:3:"), "{kind}: {error}");
        assert!(
            error.contains(&format!("more than {MAX_DEPTH} levels deep")),
            "{kind}: {error}"
        );
    }
}

/// Every public entry point that reads a graph file refuses a type past
/// the bound on a small stack, before it reads a crate or writes a file.
#[test]
fn every_entry_point_refuses_a_type_past_the_bound() {
    let deep = format!("{}Prompt{}", "Identity<".repeat(300), ">".repeat(300));
    let dir = temp();
    let refused = |what: &str, error: String| {
        assert!(
            error.contains(&format!("more than {MAX_DEPTH} levels deep")),
            "{what}: {error}"
        );
    };
    type Entry = fn(&Spec, &Path, &Path) -> Result<(), String>;
    let entries: [(&str, Entry); 8] = [
        ("resolve", |s, _, t| {
            gdl_gen::resolve(s, t).map(drop).map_err(|e| e.to_string())
        }),
        ("check", |s, _, t| {
            gdl_gen::check(s, t, Offers::Given(&[]))
                .map(drop)
                .map_err(|e| e.to_string())
        }),
        ("render", |s, o, t| {
            gdl_gen::render(s, o, t)
                .map(drop)
                .map_err(|e| e.to_string())
        }),
        ("diagram", |s, _, t| {
            gdl_gen::diagram(s, t, Format::Mermaid)
                .map(drop)
                .map_err(|e| e.to_string())
        }),
        ("export", |s, _, t| {
            gdl_gen::export(s, t, Offers::Given(&[]))
                .map(drop)
                .map_err(|e| e.to_string())
        }),
        ("compose", |s, o, t| {
            gdl_gen::compose(s, None, o, t, Sources::Fetched)
                .map(drop)
                .map_err(|e| e.to_string())
        }),
        ("compose_files", |s, o, t| {
            gdl_gen::compose_files(s, None, o, t, Sources::Fetched)
                .map(drop)
                .map_err(|e| e.to_string())
        }),
        ("compose_bundle", |s, _, t| {
            gdl_gen::compose_bundle(s, t)
                .map(drop)
                .map_err(|e| e.to_string())
        }),
    ];
    for (what, entry) in entries {
        let spec = spec(&deep, dir.path());
        let out = dir.path().join("out");
        let target = dir.path().join("target");
        let error = on_small_stack(move || entry(&spec, &out, &target))
            .err()
            .unwrap_or_else(|| panic!("{what}: succeeded"));
        refused(what, error);
        assert!(!dir.path().join("out").exists(), "{what} wrote its output");
    }

    // `check_files` reads the spec and its graph from disk.
    let graph = format!(
        "name = \"deep\", schema = 1;\nsource Front: {deep} -> stream Reply;\n\
         sink Engine: Prompt -> stream Reply;\ntopology: Front => Engine;\n"
    );
    std::fs::write(dir.path().join("deep.gdl"), graph).expect("graph file");
    std::fs::write(
        dir.path().join("gdl.toml"),
        "[graph]\npath = \"deep.gdl\"\n[messages]\ncrates = [\"protocols@0.1.0\"]\n\
         [gdl]\nversion = \"0.1.0\"\n[segment.all]\nnodes = [\"Front\", \"Engine\"]\n",
    )
    .expect("spec file");
    let file = dir.path().join("gdl.toml");
    let target = dir.path().join("target");
    let problems = on_small_stack(move || gdl_gen::check_files(&file, None, &target).to_string());
    refused("check_files", problems);
}
