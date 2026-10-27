//! A bound on how deeply Rust tokens nest, checked before syn reads them.
//!
//! syn recurses once per level: it builds its token buffer with one call per
//! bracket, and parses a type or an expression with one or more calls per
//! level. Text deep enough overflows the caller's stack and aborts the
//! process, and a bigger stack only moves the depth that does. So
//! [`check_depth`] walks the tokens without recursion and refuses text that
//! nests past a limit, before syn sees it. A graph's text is bounded at
//! [`MAX_DEPTH`]; the source reader bounds a source file at a limit of its
//! own.

use proc_macro2::{Delimiter, Spacing, Span, TokenStream, TokenTree, token_stream};

use crate::model::KEYWORDS;

/// The deepest a graph's text may nest, as [`check_depth`] counts.
///
/// In a debug build, through `gdl_gen::resolve` on a 2 MiB thread, syn
/// overflowed the stack past these depths, as the count counts them: 36
/// (`dyn A<…>`), 37 (`Identity<…>`, a turbofish), 49 (`fn(…)`), 52
/// (`fn() -> …`), 58 (a chain of casts or `?`), 60 (`*const`), 61 (`&`),
/// 65 (`(…)`, `[…]`), 173 (ranges), 231 (assignments), 455 (`!`, `*`, `&`
/// in an expression), 509 (`let` chains), and 1,161 (an operator's hop in a
/// topology). 16 leaves more than half of such a
/// thread to its caller: the composer checks a draft on a tokio blocking
/// thread, whose stack is 2 MiB.
///
/// A real graph file stays far below 16. Its message positions hold only
/// names and `()`, at most 2 levels deep (an operator's parentheses and a
/// `->` are a level each), and its topology reaches the bound only with
/// operators nested more than 16 deep. rustc builds much deeper types: 128
/// levels of a generic struct by default, and about 4,030 levels of a type
/// alias chain. So `graph!` refuses a port type that rustc builds when it
/// nests 17 to 128 levels deep. A contract crate can name such a type
/// through an alias, whose name is no level.
pub const MAX_DEPTH: usize = 16;

/// Refuse `tokens` if they nest deeper than `limit`, with `problem` at the
/// token that goes too deep. The walk does not recurse, so it refuses text
/// of any depth on any stack.
///
/// The count is an upper bound on how deeply syn recurses to parse the
/// tokens, and then drops or prints what it built. Each open bracket (`(`,
/// `[`, `{`, or a `<` that can open generic arguments) is one level deeper
/// than the tokens before it in its own bracket. So is each prefix operator
/// (`&`, `*`, `-`, `!`, `..`, a closure's `|`), each `->`, `@`, and
/// assignment, and each `return`, `break`, `yield`, `become`, `box`, `if`,
/// `while`, `match`, and `for` that starts an expression.
///
/// A step of a chain is an eighth of a level: a binary operator, a `.`, a
/// `?`, an `as`, a call's or an index's brackets, and an `else if`. syn
/// reads a chain in a loop, but the tree it builds nests once per step, and
/// dropping or printing that tree recurses. In a debug build, through
/// `gdl_gen::resolve` on a 2 MiB thread, each kind of chain overflowed the
/// stack past 432 to 454 steps, a twelfth of the cost of a level.
///
/// What counts lasts until the next sibling begins: at a `,`, a `;`, a
/// `=>`, or an item or a statement after a `}`. A name, a literal, and an
/// attribute's `#` are no level, and a macro's body counts only its
/// brackets, since syn does not parse it. A graph's `request`, `response`,
/// `stream`, and `over` start a type, as a keyword does. A `<<` after a
/// name counts as two `<`, since a type writes `Vec<<T as A>::B>` the same
/// way: so a chain of left shifts after names is 2 levels a step.
///
/// # Errors
///
/// A `syn::Error` with `problem` at the first token past `limit`.
pub fn check_depth(
    tokens: &TokenStream,
    limit: usize,
    problem: &dyn Fn() -> String,
) -> syn::Result<()> {
    let walk = Walk { limit, problem };
    let mut outer: Vec<Bracket> = Vec::new();
    let mut here = Bracket::new(tokens.clone(), 0, false);
    loop {
        let Some(token) = here.tokens.next() else {
            match outer.pop() {
                Some(bracket) => {
                    here = bracket;
                    continue;
                }
                None => return Ok(()),
            }
        };
        if here.opaque {
            if let TokenTree::Group(group) = token {
                let depth = walk.within(here.base + LEVEL, group.span_open())?;
                outer.push(std::mem::replace(
                    &mut here,
                    Bracket::new(group.stream(), depth, true),
                ));
            }
            continue;
        }
        if here.prev == Prev::Block && starts_sibling(&token) {
            here.sibling();
        }
        let after_else = std::mem::take(&mut here.after_else);
        let bang = std::mem::take(&mut here.bang);
        let attribute = std::mem::take(&mut here.attribute);
        match token {
            TokenTree::Group(group) => {
                // `if a < b {`: a `<` after a name, then a block after an
                // operand, was a comparison; in a type, a block can follow
                // only a `<` or a `,`.
                if group.delimiter() == Delimiter::Brace && here.prev.operand() {
                    here.close_angles();
                } else if here.prev.operand() {
                    // A call's or an index's brackets.
                    here.chain(group.span_open(), &walk)?;
                }
                let depth = walk.within(here.base + here.run + LEVEL, group.span_open())?;
                here.prev = match group.delimiter() {
                    Delimiter::Brace => Prev::Block,
                    _ => Prev::Value,
                };
                outer.push(std::mem::replace(
                    &mut here,
                    Bracket::new(group.stream(), depth, bang),
                ));
            }
            TokenTree::Punct(punct) => {
                let (op, span) = glue(&punct, &mut here.tokens);
                here.operator(op, span, attribute, &walk)?;
            }
            TokenTree::Ident(ident) => {
                // Compared in place: a word's text would cost an allocation
                // for each name in the file.
                let is = |words: &[&str]| words.iter().any(|word| ident == word);
                if is(&["self", "Self", "super", "crate", "await"]) {
                    here.prev = Prev::Name;
                } else if is(&["true", "false"]) {
                    here.prev = Prev::Value;
                } else if is(&["request", "response", "stream", "over"]) {
                    // A graph's own keywords before a type: `request &T`
                    // starts a type, it does not apply `&`.
                    here.prev = Prev::Other;
                } else if !is(&KEYWORDS) {
                    // A macro's name, between `macro_rules!` and its body.
                    here.bang = bang;
                    here.prev = Prev::Name;
                } else if ident == "if" && after_else {
                    here.elses += 1;
                    here.run = here.if_run + LEVEL + here.elses;
                    walk.within(here.base + here.run, ident.span())?;
                    here.prev = Prev::Other;
                } else if ident == "if" {
                    (here.if_run, here.elses) = (here.run, 0);
                    here.step(ident.span(), &walk)?;
                } else if ident == "as" {
                    here.chain(ident.span(), &walk)?;
                } else if is(&["return", "break", "yield", "become", "box", "while", "match"])
                    // `impl Trait for Type` names no expression.
                    || (ident == "for" && !here.prev.operand())
                {
                    here.step(ident.span(), &walk)?;
                } else {
                    here.after_else = ident == "else";
                    here.prev = Prev::Other;
                }
            }
            TokenTree::Literal(_) => here.prev = Prev::Value,
        }
    }
}

/// The limit, and the problem past it.
struct Walk<'p> {
    limit: usize,
    problem: &'p dyn Fn() -> String,
}

/// A level in the walk's units: a chain's step is one unit.
const LEVEL: usize = 8;

impl Walk<'_> {
    /// `depth` in units, if it is within the limit in levels.
    fn within(&self, depth: usize, span: Span) -> syn::Result<usize> {
        if depth > self.limit * LEVEL {
            Err(syn::Error::new(span, (self.problem)()))
        } else {
            Ok(depth)
        }
    }
}

/// The walk inside one bracket: the tokens left, and the depth so far.
struct Bracket {
    tokens: std::iter::Peekable<token_stream::IntoIter>,
    /// A macro's body: syn keeps its tokens, so only its brackets count.
    opaque: bool,
    /// The depth of the innermost open bracket or `<` here.
    base: usize,
    /// The levels counted since that bracket opened, or since the current
    /// sibling began.
    run: usize,
    /// The `run` a `,` returns to: 0, or the level of a closure's `|`,
    /// whose parameters a `,` separates while the closure stays open.
    mark: usize,
    /// The `base`, `run`, and `mark` outside each `<` open here. A `>`
    /// closes only a `<` of its own bracket.
    angles: Vec<(usize, usize, usize)>,
    prev: Prev,
    /// The `run` before the first `if` of the latest `else if` chain, and
    /// the `else if`s since.
    if_run: usize,
    elses: usize,
    after_else: bool,
    /// After a macro's `!`: the next bracket is its body.
    bang: bool,
    /// After an attribute's `#`: its `!` is no operator.
    attribute: bool,
}

/// What came before, which decides whether `-` is a prefix or a binary
/// operator, and whether `<` opens generic arguments.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Prev {
    /// A bracket's start, or a token after which an expression starts.
    Other,
    /// A name, which `<` can follow with generic arguments.
    Name,
    /// A literal, a closed `(…)` or `[…]`, a closed `<…>`, or a `?`.
    Value,
    /// A closed `{…}`: what follows is a sibling, or the rest of a
    /// block-like expression.
    Block,
}

impl Prev {
    fn operand(self) -> bool {
        matches!(self, Prev::Name | Prev::Value)
    }
}

impl Bracket {
    fn new(tokens: TokenStream, base: usize, opaque: bool) -> Self {
        Self {
            tokens: tokens.into_iter().peekable(),
            opaque,
            base,
            run: 0,
            mark: 0,
            angles: Vec::new(),
            prev: Prev::Other,
            if_run: 0,
            elses: 0,
            after_else: false,
            bang: false,
            attribute: false,
        }
    }

    /// One more level, at `span`.
    fn step(&mut self, span: Span, walk: &Walk<'_>) -> syn::Result<()> {
        self.run += LEVEL;
        walk.within(self.base + self.run, span)?;
        self.prev = Prev::Other;
        Ok(())
    }

    /// One more step of a chain, at `span`.
    fn chain(&mut self, span: Span, walk: &Walk<'_>) -> syn::Result<()> {
        self.run += 1;
        walk.within(self.base + self.run, span)?;
        self.prev = Prev::Other;
        Ok(())
    }

    /// Close every `<` open here: none was a real one.
    fn close_angles(&mut self) {
        if let Some(&(base, run, mark)) = self.angles.first() {
            (self.base, self.run, self.mark) = (base, run, mark);
            self.angles.clear();
        }
    }

    /// A new sibling begins: nothing counted since this bracket opened is
    /// still open, but a closure whose parameters this is.
    fn sibling(&mut self) {
        self.close_angles();
        self.run = self.mark;
    }

    /// A `;` or a `=>`: a statement, an item, or a match arm ends. A real
    /// `<` cannot hold either.
    fn end(&mut self) {
        self.close_angles();
        self.run = 0;
        self.mark = 0;
        self.prev = Prev::Other;
    }

    fn operator(
        &mut self,
        op: &str,
        span: Span,
        attribute: bool,
        walk: &Walk<'_>,
    ) -> syn::Result<()> {
        let operand = self.prev.operand();
        match op {
            ";" | "=>" => self.end(),
            "," => {
                self.run = self.mark;
                self.prev = Prev::Other;
            }
            "#" => {
                self.attribute = true;
                self.prev = Prev::Other;
            }
            // An inner attribute's `!`, or a macro's.
            "!" if attribute => self.attribute = true,
            "!" if operand => {
                self.bang = true;
                self.prev = Prev::Other;
            }
            "?" => {
                self.chain(span, walk)?;
                self.prev = Prev::Value;
            }
            "->" | "@" | "=" | "+=" | "-=" | "*=" | "/=" | "%=" | "^=" | "&=" | "|=" | "<<="
            | ">>=" => self.step(span, walk)?,
            // A comparison, or a shift after a value.
            "<=" => self.chain(span, walk)?,
            "<" | "<<" if self.prev == Prev::Value => self.chain(span, walk)?,
            "<" | "<<" => {
                for _ in 0..op.len() {
                    let depth = walk.within(self.base + self.run + LEVEL, span)?;
                    self.angles.push((self.base, self.run, self.mark));
                    (self.base, self.run, self.mark) = (depth, 0, 0);
                }
                self.prev = Prev::Other;
            }
            ">" | ">>" | ">=" => {
                let mut closed = false;
                for _ in 0..op.matches('>').count() {
                    if let Some((base, run, mark)) = self.angles.pop() {
                        (self.base, self.run, self.mark) = (base, run, mark);
                        closed = true;
                    }
                }
                if !closed {
                    // A comparison or a shift.
                    self.chain(span, walk)?;
                } else if op == ">=" {
                    // `A<B>= c`: the rest of the glued `>=` assigns.
                    self.step(span, walk)?;
                } else {
                    self.prev = Prev::Value;
                }
            }
            "&" | "&&" | "|" | "||" | "*" | "-" | "!" | ".." | "..=" | "..." if !operand => {
                let levels = if op.starts_with('.') { 1 } else { op.len() };
                for _ in 0..levels {
                    self.step(span, walk)?;
                }
                if op.starts_with('|') {
                    self.mark = self.run;
                }
            }
            // A path's `::`, a type's `:`, a lifetime's `'`.
            "::" | ":" | "'" | "$" => self.prev = Prev::Other,
            // A binary operator, a field's or a method's `.`, a bound's `+`.
            _ => self.chain(span, walk)?,
        }
        Ok(())
    }
}

/// An item or a statement after a `}` starts with a name, a keyword, or an
/// attribute's `#`. A block-like expression goes on only with an operator,
/// `else`, or `as`.
fn starts_sibling(token: &TokenTree) -> bool {
    match token {
        TokenTree::Ident(ident) => !matches!(ident.to_string().as_str(), "else" | "as"),
        TokenTree::Punct(punct) => punct.as_char() == '#',
        _ => false,
    }
}

/// The operator that starts with `first`, glued with the joint punctuation
/// after it as Rust's lexer glues it.
fn glue(
    first: &proc_macro2::Punct,
    tokens: &mut std::iter::Peekable<token_stream::IntoIter>,
) -> (&'static str, Span) {
    const OPERATORS: [&str; 24] = [
        "..=", "...", "<<=", ">>=", "&&", "||", "..", "::", "->", "=>", "==", "!=", "<=", ">=",
        "<<", ">>", "+=", "-=", "*=", "/=", "%=", "^=", "&=", "|=",
    ];
    // An operator is at most three characters.
    let mut text = [first.as_char(), ' ', ' '];
    let mut len = 1;
    let mut joint = first.spacing() == Spacing::Joint;
    while joint && len < text.len() {
        let Some(TokenTree::Punct(next)) = tokens.peek() else {
            break;
        };
        text[len] = next.as_char();
        let longer = &text[..=len];
        if !OPERATORS
            .iter()
            .any(|op| op.chars().zip(longer).all(|(a, b)| a == *b) && op.len() > len)
        {
            break;
        }
        joint = next.spacing() == Spacing::Joint;
        len += 1;
        tokens.next();
    }
    let glued = &text[..len];
    let op = OPERATORS
        .iter()
        .copied()
        .find(|op| op.len() == len && op.chars().zip(glued).all(|(a, b)| a == *b))
        .unwrap_or_else(|| single(first.as_char()));
    (op, first.span())
}

/// A one-character operator as a `'static` string.
fn single(c: char) -> &'static str {
    const SINGLES: &str = ";,:#!?=<>&|*-.+/%^@~$'";
    match SINGLES.find(c) {
        Some(i) => &SINGLES[i..i + 1],
        None => "",
    }
}
