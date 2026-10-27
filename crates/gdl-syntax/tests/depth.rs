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

/// A graph file's deepest message position is less than 3 levels deep:
/// inside an operator's parentheses, after a streamed request's `+` (a
/// chain's step) and the `->`. Each kind of position is still accepted
/// with 13 more levels on it.
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

/// A graph whose source takes `ty`.
fn taking(ty: &str) -> String {
    format!(
        "name = \"g\", schema = 1;\nsource S: {ty} -> stream ();\n\
         sink E: () -> stream ();\ntopology: S => E;\n"
    )
}

/// `ty` inside `levels` levels of `A<…>`.
fn within(levels: usize, ty: &str) -> String {
    format!("{}{ty}{}", "A<".repeat(levels), ">".repeat(levels))
}

/// Each place where the count goes down, or does not go up, is one where
/// syn's recursion does too. Each case reaches one level past the bound
/// only if the count is right there, and would be accepted if it were not.
#[test]
fn no_reset_lowers_a_depth_that_syn_reaches() {
    let cases = [
        // A `>` closes only a `<` of its own bracket.
        ("an inner `>`", within(MAX_DEPTH - 1, "(x > y), & & Prompt")),
        // The `>` of `->` closes no `<`.
        ("an arrow", within(MAX_DEPTH - 2, "Prompt -> & & Prompt")),
        // A block right after `<` is a const argument, not a comparison's end.
        ("a const block", within(MAX_DEPTH - 1, "{1}, & & Prompt")),
        // A `,` between a closure's parameters leaves the closure open.
        (
            "closure parameters",
            format!("[Prompt; {}1]", "|a, b| ".repeat(MAX_DEPTH)),
        ),
        // `else if` goes on at its chain's level, not at the bracket's.
        (
            "an else-if chain",
            format!(
                "[Prompt; {}if a {{}} else if - - b {{}} else {{}}]",
                "- ".repeat(MAX_DEPTH - 3)
            ),
        ),
        // After a block, `*` starts a statement: it is a prefix.
        (
            "a block, then `*`",
            format!("[Prompt; {{ if a {{}} {}x }}]", "* ".repeat(MAX_DEPTH - 2)),
        ),
        // `as` after a block goes on with the same expression.
        (
            "a block, then `as`",
            format!("[Prompt; {}{{1}} as & & u8]", "- ".repeat(MAX_DEPTH - 2)),
        ),
        // Only the first `-` after an operand is binary.
        (
            "a binary `-`",
            format!("[Prompt; x {}1]", "- ".repeat(MAX_DEPTH)),
        ),
        // A macro's brackets count, though its other tokens do not.
        (
            "a macro's brackets",
            format!(
                "[Prompt; m!{}x{}]",
                "(".repeat(MAX_DEPTH),
                ")".repeat(MAX_DEPTH)
            ),
        ),
    ];
    for (case, ty) in cases {
        let error = parse(&taking(&ty))
            .err()
            .unwrap_or_else(|| panic!("{case}: accepted"));
        assert!(error.to_string().contains("levels deep"), "{case}: {error}");
    }
}

/// What syn never nests is no level: a long list, a match's arms, a block's
/// statements, a crate's items and doc comments, and a macro's body. A
/// chain, which syn nests, is an eighth of a level a step. Only the walk
/// runs here: syn's tree of a chain this long would overflow the test's
/// stack when dropped.
#[test]
fn a_long_flat_source_is_not_deep() {
    let depth = |text: String| {
        let tokens: proc_macro2::TokenStream = text.parse().expect("tokens");
        let fits = |limit| gdl_syntax::check_depth(&tokens, limit, &String::new).is_ok();
        let (mut low, mut high) = (0, 1 << 12);
        while high - low > 1 {
            let mid = (low + high) / 2;
            if fits(mid) { high = mid } else { low = mid }
        }
        high
    };
    let flat = |n: usize| {
        [
            (
                "negative numbers",
                format!("const A: [i8; {n}] = [{}];", "-1, ".repeat(n)),
            ),
            (
                "returns in arms",
                format!(
                    "fn f(c: u8) -> u8 {{ match c {{ {}_ => 0 }} }}",
                    "0 => return 1, ".repeat(n)
                ),
            ),
            (
                "statements",
                format!(
                    "fn f(a: u8, b: u8) -> u8 {{ {} 1 }}",
                    "if a < b { return 1; } ".repeat(n)
                ),
            ),
            (
                "items",
                "/// A doc.\n#[derive(Debug)]\npub struct A<T>(T);\nimpl<T> A<T> {}\n".repeat(n),
            ),
            (
                "doc comments",
                format!(
                    "{}fn f() {{}}",
                    "//! A line of the crate's doc.\n".repeat(n)
                ),
            ),
            (
                "a macro's body",
                format!("m! {{ {} }}", "& - ! * | a < b ".repeat(n)),
            ),
        ]
    };
    for ((case, long), (_, short)) in flat(2_000).into_iter().zip(flat(2)) {
        assert_eq!(depth(long), depth(short), "{case}");
    }
    let n = 2_000;
    let chain = format!(
        "fn f() {{ if a {{}} {}else {{}} }}",
        "else if a {} ".repeat(n)
    );
    let chain = depth(chain);
    assert!(
        (n / 8..=n / 8 + 8).contains(&chain),
        "an else-if chain: {chain}"
    );
    let pattern = format!(
        "fn f(c: char) -> bool {{ match c {{ {}'z' => true, _ => false }} }}",
        "'a'..='b' | ".repeat(n)
    );
    let pattern = depth(pattern);
    assert!(
        (2 * n / 8..=2 * n / 8 + 8).contains(&pattern),
        "an or-pattern: {pattern}"
    );
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
