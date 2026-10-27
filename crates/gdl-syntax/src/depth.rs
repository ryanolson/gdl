//! A bound on how deeply a graph's text nests, checked before syn reads it.
//!
//! syn recurses once per level: it builds its token buffer with one call per
//! bracket, and parses a type or an expression with one or more calls per
//! level. A graph's text comes from a user's file or from a draft in the
//! composer, which parses it in its own process. Text deep enough would
//! overflow the caller's stack and abort that process, and a bigger stack
//! only moves the depth that does. So [`check`] walks the tokens without
//! recursion and refuses text that nests past [`MAX_DEPTH`], before syn sees
//! it.

use proc_macro2::{Spacing, Span, TokenStream, TokenTree, token_stream};

use crate::model::is_keyword;

/// The deepest a graph's text may nest. Each open bracket (`(`, `[`, `{`, or
/// `<`) is one level deeper than the tokens before it in its own bracket.
/// Each operator and keyword token is one level too (`&`, `*const`, the
/// `->` of `fn() -> T`, `-`, `return`, `dyn`), since syn also recurses on
/// those; it counts until the next `;` or `=>`. A `,`, a `:`, a lifetime's
/// `'`, a name, and a literal are no level, so a long path or a long
/// topology costs nothing.
///
/// In a debug build, through `gdl_gen::resolve` on a 2 MiB thread, syn
/// overflowed the stack past these depths, as this count counts them:
/// 37 (`Identity<…>`, a turbofish), 61 (`&`), 65 (`(…)`, `[…]`), 72
/// (`dyn A<…>`), 98 (`fn(…)`, `A<{…}>`), 104 (`fn() -> …`), and 1,161
/// (an operator's hop in a topology). 16 leaves more than half of such a
/// thread to its caller: the composer checks a draft on a tokio blocking
/// thread, whose stack is 2 MiB.
///
/// A real graph file stays far below 16. Its message positions hold only
/// names and `()`, at most 3 levels deep (an operator's parentheses, a
/// streamed request's `+`, and a `->` are a level each), and its topology reaches the bound only with operators
/// nested more than 16 deep. rustc builds much deeper types: 128 levels of
/// a generic struct by default, and about 4,030 levels of a type alias
/// chain. So `graph!` refuses a port type that rustc builds when it nests
/// 17 to 128 levels deep. A contract crate can name such a type through an
/// alias, whose name is no level.
pub const MAX_DEPTH: usize = 16;

/// Refuse `tokens` if they nest deeper than [`MAX_DEPTH`], at the token that
/// goes too deep.
pub(crate) fn check(tokens: &TokenStream) -> syn::Result<()> {
    let mut outer: Vec<Bracket> = Vec::new();
    let mut here = Bracket::new(tokens.clone(), 0);
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
        match token {
            TokenTree::Group(group) => {
                let depth = here.deeper(group.span_open())?;
                outer.push(std::mem::replace(
                    &mut here,
                    Bracket::new(group.stream(), depth),
                ));
            }
            TokenTree::Punct(punct) => match punct.as_char() {
                ';' => here.run = 0,
                ',' | ':' | '\'' => {}
                '<' => {
                    let depth = here.deeper(punct.span())?;
                    here.angles.push((here.base, here.run));
                    here.base = depth;
                    here.run = 0;
                }
                '>' => match here.angles.pop() {
                    Some((base, run)) => (here.base, here.run) = (base, run),
                    None => here.step(punct.span())?,
                },
                // `->` and `=>` lex as two tokens; their `>` closes no `<`.
                // An arrow is one level; a `=>` ends a topology step or a
                // match arm, and what follows it is not nested in what
                // came before.
                c @ ('-' | '=')
                    if punct.spacing() == Spacing::Joint
                        && matches!(here.tokens.peek(), Some(TokenTree::Punct(p)) if p.as_char() == '>') =>
                {
                    here.tokens.next();
                    if c == '=' {
                        here.run = 0;
                    } else {
                        here.step(punct.span())?;
                    }
                }
                _ => here.step(punct.span())?,
            },
            TokenTree::Ident(ident) => {
                if is_keyword(&ident.to_string()) {
                    here.step(ident.span())?;
                }
            }
            TokenTree::Literal(_) => {}
        }
    }
}

/// The walk inside one bracket: the tokens left, and the depth so far.
struct Bracket {
    tokens: std::iter::Peekable<token_stream::IntoIter>,
    /// The depth of the innermost open bracket or `<` here.
    base: usize,
    /// The operators and keywords since that bracket opened, or since the
    /// last `;` or `=>` in it.
    run: usize,
    /// The `base` and `run` outside each `<` open here. A `>` closes only a
    /// `<` of its own bracket.
    angles: Vec<(usize, usize)>,
}

impl Bracket {
    fn new(tokens: TokenStream, base: usize) -> Self {
        Self {
            tokens: tokens.into_iter().peekable(),
            base,
            run: 0,
            angles: Vec::new(),
        }
    }

    /// The depth inside a bracket that opens here, at `span`.
    fn deeper(&self, span: Span) -> syn::Result<usize> {
        within(self.base + self.run + 1, span)
    }

    /// One more operator or keyword, at `span`.
    fn step(&mut self, span: Span) -> syn::Result<()> {
        self.run += 1;
        within(self.base + self.run, span).map(drop)
    }
}

fn within(depth: usize, span: Span) -> syn::Result<usize> {
    if depth > MAX_DEPTH {
        Err(syn::Error::new(
            span,
            format!(
                "this nests more than {MAX_DEPTH} levels deep, and a graph may not: \
                 each bracket, `<`, operator, and keyword is a level"
            ),
        ))
    } else {
        Ok(depth)
    }
}
