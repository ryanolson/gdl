//! Source text parsed only when it nests no deeper than the reader's stack
//! holds: [`READ_DEPTH`] levels, as `gdl_syntax::check_depth` counts them.

use proc_macro2::TokenStream;

/// The deepest a source file may nest for the reader to read it.
///
/// rustc 1.93.1 with default settings builds about 4,030 levels of a type
/// alias chain, and stops there with a stack overflow of its own. On its
/// 256 MiB stack ([`crate::STACK`]), the reader in a debug build overflowed
/// past 4,911 levels of `dyn A<…>` and 5,080 of `Identity<…>`, the kinds
/// it holds fewest of. So 4,096 reads every crate that rustc builds with
/// default settings, and refuses a deeper one before syn reads it, instead
/// of crashing. The deepest of 82,519 `.rs` files in the crates.io crates
/// on one machine counts 179 levels.
pub const READ_DEPTH: usize = 4_096;

/// `text` as a file, as `syn::parse_file` reads it, or a problem at the
/// token that goes deeper than [`READ_DEPTH`].
pub(crate) fn file(text: &str) -> syn::Result<syn::File> {
    let content = text.strip_prefix('\u{feff}').unwrap_or(text);
    if content.starts_with("#!") {
        // syn takes the first line as a shebang unless an attribute starts
        // there. Walk the text if it lexes; else the line is a shebang.
        let after = &content[content.find('\n').unwrap_or(content.len())..];
        if let Ok(tokens) = content.parse().or_else(|_| after.parse()) {
            within(&tokens)?;
        }
        return syn::parse_file(text);
    }
    match content.parse::<TokenStream>() {
        Ok(tokens) => {
            within(&tokens)?;
            syn::parse2(tokens)
        }
        // syn's own lexing gives the problem its usual words.
        Err(_) => syn::parse_file(text),
    }
}

/// `text` as a `T`, if it lexes, nests no deeper than [`READ_DEPTH`], and
/// parses.
pub(crate) fn parse<T: syn::parse::Parse>(text: &str) -> Option<T> {
    let tokens = text.parse().ok()?;
    within(&tokens).ok()?;
    syn::parse2(tokens).ok()
}

fn within(tokens: &TokenStream) -> syn::Result<()> {
    gdl_syntax::check_depth(tokens, READ_DEPTH, &|| {
        format!("it nests more than {READ_DEPTH} levels deep, deeper than the reader reads")
    })
}
