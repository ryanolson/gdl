//! The depth bound on a graph's text: what counts as a level, and what
//! does not. The bound at each kind of type, read by the generator on a
//! small stack, is in `crates/gdl-gen/tests/nesting.rs`.

use gdl_syntax::{Graph, MAX_DEPTH};

fn parse(text: &str) -> syn::Result<Graph> {
    Graph::parse(text.parse().expect("tokens"))
}

fn refused(text: &str) -> String {
    let error = parse(text).err().expect("refused");
    assert!(
        error
            .to_string()
            .contains(&format!("more than {MAX_DEPTH} levels deep")),
        "{error}"
    );
    error.to_string()
}

/// A graph of operators, each owning the next one's hop, `depth` deep.
fn operators(depth: usize) -> String {
    let mut text = String::from("name = \"g\", schema = 1;\nsource S: () -> stream ();\n");
    for i in 0..depth {
        text.push_str(&format!(
            "operator O{i}: (() -> stream ()) over (() -> stream ());\n"
        ));
    }
    text.push_str("sink E: () -> stream ();\ntopology: S => ");
    for i in 0..depth {
        text.push_str(&format!("O{i} {{ "));
    }
    text.push('E');
    text.push_str(&" }".repeat(depth));
    text.push_str(";\n");
    text
}

/// A topology's hops are brackets too: an operator's hop nests at most as
/// deep as a type, since the parser recurses into each.
#[test]
fn operator_hops_nest_to_the_bound() {
    parse(&operators(MAX_DEPTH)).expect("at the bound");
    refused(&operators(MAX_DEPTH + 1));
}

/// A long chain is no deeper than a short one: syn loops over `=>` steps,
/// `,`-separated arguments, and path segments.
#[test]
fn a_long_flat_graph_is_not_deep() {
    let n = 1_000;
    let mut text = String::from("name = \"g\", schema = 1;\nsource S: () -> stream ();\n");
    for i in 0..n {
        text.push_str(&format!(
            "forward F{i}: a::b::c::d::e::f::Req -> a::b::c::d::e::f::Req;\n"
        ));
    }
    text.push_str("sink E: a::b::c::d::e::f::Req -> stream ();\ntopology: S => ");
    for i in 0..n {
        text.push_str(&format!("F{i} => "));
    }
    text.push_str("E;\n");
    // The forward nodes' types do not match the source's; that is the
    // compiler's check, not the parser's.
    parse(&text).expect("a long graph");

    let wide = format!(
        "name = \"g\", schema = 1;\nsource S: ({}) -> stream ();\nsink E: () -> stream ();\ntopology: S => E;\n",
        "Vec<Option<u8>>, ".repeat(n)
    );
    parse(&wide).expect("a wide tuple");
}

/// A type a message really names is far inside the bound.
#[test]
fn a_real_type_is_well_inside_the_bound() {
    parse(
        "name = \"g\", schema = 1;\n\
         source S: ::std::result::Result<::std::vec::Vec<::std::collections::HashMap<String, &'static str>>, Box<dyn Fn(&[u8]) -> Option<u8> + Send>> -> stream ();\n\
         sink E: () -> stream ();\ntopology: S => E;\n",
    )
    .expect("parsed");
}

/// A graph file's deepest message position is 3 levels deep: inside an
/// operator's parentheses, after a streamed request's `+` and the `->`.
/// Each kind of position is still accepted with 13 more levels on it.
#[test]
fn a_graph_files_deepest_position_leaves_room() {
    let deep = |levels: usize| format!("{}Item{}", "A<".repeat(levels), ">".repeat(levels));
    let graph = |levels: usize| {
        let item = deep(levels);
        format!(
            "name = \"g\", schema = 1;\n\
             source S: Head + stream {item} -> stream {item};\n\
             operator O: (Head + stream Item -> stream {item}) over (Head + stream Item -> stream {item});\n\
             forward F: Head + stream Item -> Head + stream {item};\n\
             sink E: Head + stream Item -> stream {item};\n\
             topology: S => O {{ F => E }};\n"
        )
    };
    parse(&graph(MAX_DEPTH - 3)).expect("13 more levels");
    refused(&graph(MAX_DEPTH - 2));
}

/// A `>` closes only a `<` of its own bracket, and the `>` of `->` or
/// `=>` closes none: neither can make a deep type read as shallow. Each
/// case ends one level past the bound only if no `<` was closed early.
#[test]
fn an_arrow_or_an_inner_close_does_not_lower_the_depth() {
    let within = |levels: usize, inner: &str| {
        format!(
            "name = \"g\", schema = 1;\nsource S: {}{inner}{} -> stream ();\n\
             sink E: () -> stream ();\ntopology: S => E;\n",
            "A<".repeat(levels),
            ">".repeat(levels)
        )
    };
    refused(&within(MAX_DEPTH - 1, "(x > y) & & Prompt"));
    refused(&within(MAX_DEPTH - 2, "Prompt -> & & Prompt"));
    refused(&within(MAX_DEPTH - 1, "x => & & Prompt"));
    parse(&within(MAX_DEPTH - 1, "& Prompt")).expect("at the bound");
}

/// The bound is checked before syn builds its token buffer, which recurses
/// once per bracket, and the check itself does not recurse: brackets
/// nested a hundred thousand deep are refused on a 2 MiB thread.
#[test]
fn brackets_nested_without_limit_are_refused_on_a_small_stack() {
    let depth = 100_000;
    let text = format!(
        "name = \"g\", schema = 1;\nsource S: {}Prompt{} -> stream ();\n\
         sink E: () -> stream ();\ntopology: S => E;\n",
        "(".repeat(depth),
        ")".repeat(depth)
    );
    let error = std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || refused(&text))
        .expect("thread")
        .join()
        .expect("refused without overflowing");
    assert!(error.contains("levels deep"), "{error}");
}
