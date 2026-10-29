use std::path::{Path, PathBuf};

use proc_macro2::{Group, Literal, TokenStream, TokenTree};
use quote::{ToTokens, quote};

use super::*;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/segment")
}

/// Every `<segment>.thin.rs` fixture, with its `.expanded.rs` beside it.
fn cases() -> Vec<(PathBuf, PathBuf)> {
    let mut cases = Vec::new();
    for case in std::fs::read_dir(fixtures()).expect("the fixtures") {
        let case = case.expect("a case").path();
        for file in std::fs::read_dir(&case).expect("a case's files") {
            let file = file.expect("a file").path();
            let name = file.to_string_lossy().into_owned();
            if let Some(stem) = name.strip_suffix(".thin.rs") {
                cases.push((file.clone(), PathBuf::from(format!("{stem}.expanded.rs"))));
            }
        }
    }
    cases.sort();
    cases
}

/// The input of the one `segment!` call in a generated library.
fn call(file: &Path) -> TokenStream {
    let text = std::fs::read_to_string(file).expect("the library");
    let library = syn::parse_file(&text).expect("a library that parses");
    library
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Macro(item)
                if item
                    .mac
                    .path
                    .segments
                    .last()
                    .is_some_and(|s| s.ident == "segment") =>
            {
                Some(item.mac.tokens.clone())
            }
            _ => None,
        })
        .expect("a segment! call")
}

/// `tokens` with every string literal written plainly, so that a raw
/// string and an escaped one with the same value compare equal, and
/// without the trailing commas rustfmt adds before a `>` or a closing
/// delimiter, which change no meaning there.
fn plain(tokens: TokenStream) -> TokenStream {
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    let trailing = |index: usize| {
        matches!(&tokens[index], TokenTree::Punct(p) if p.as_char() == ',')
            && match tokens.get(index + 1) {
                None => true,
                Some(TokenTree::Punct(next)) => next.as_char() == '>',
                Some(_) => false,
            }
    };
    let kept: Vec<TokenTree> = (0..tokens.len())
        .filter(|&index| !trailing(index))
        .map(|index| tokens[index].clone())
        .collect();
    kept.into_iter()
        .map(|token| match token {
            TokenTree::Group(group) => {
                let mut plain_group = Group::new(group.delimiter(), plain(group.stream()));
                plain_group.set_span(group.span());
                TokenTree::Group(plain_group)
            }
            TokenTree::Literal(literal) => {
                match syn::parse_str::<syn::LitStr>(&literal.to_string()) {
                    Ok(string) => TokenTree::Literal(Literal::string(&string.value())),
                    Err(_) => TokenTree::Literal(literal),
                }
            }
            other => other,
        })
        .collect()
}

/// Each item of `file` as plain tokens: what a library holds, its crate
/// docs and comments aside.
fn items(file: &syn::File) -> Vec<String> {
    file.items
        .iter()
        .map(|item| plain(item.to_token_stream()).to_string())
        .collect()
}

/// The macro writes what the generator wrote before it held the code, item
/// for item: the calls the generator writes today for eleven specs, which
/// between them hold every form a segment takes, expand to the libraries
/// it wrote then.
#[test]
fn each_call_expands_to_the_library_the_generator_wrote() {
    let cases = cases();
    assert_eq!(cases.len(), 25, "{cases:?}");
    let krate = quote!(gdl_runtime);
    for (thin, expanded) in cases {
        let segment: Segment = syn::parse2(call(&thin)).expect("the call parses");
        let expansion: syn::File =
            syn::parse2(expand(&segment, &krate)).expect("the expansion parses");
        let text = std::fs::read_to_string(&expanded).expect("the library before");
        let before = syn::parse_file(&text).expect("the library before parses");
        let (found, wanted) = (items(&expansion), items(&before));
        for (index, (found, wanted)) in found.iter().zip(&wanted).enumerate() {
            assert_eq!(found, wanted, "{}: item {index}", thin.display());
        }
        assert_eq!(found.len(), wanted.len(), "{}: items", thin.display());
    }
}

/// A call the parser does not take says where, and what it expected.
#[test]
fn a_malformed_call_names_what_it_expected() {
    let call: TokenStream = syn::parse_str("package: \"p\";").expect("tokens");
    let error = syn::parse2::<Segment>(call).err().expect("refused");
    assert_eq!(error.to_string(), "expected `contract`");
    let call: TokenStream = syn::parse_str(
        "contract: ::c; package: \"p\"; segment: \"s\"; segments: []; placement: \"\"; \
         graph_config: \"\"; roots: []; crossings: []; feeds: []; nodes: []; policies: []; \
         serve: http;",
    )
    .expect("tokens");
    let error = syn::parse2::<Segment>(call).err().expect("refused");
    assert_eq!(
        error.to_string(),
        "expected `json_lines`, `none`, `frontend`, or `service`"
    );
}

#[test]
fn disabled_service_clients_keep_their_expansion_check() {
    let mut enabled = 0;
    let mut disabled = 0;
    for (thin, expanded) in cases() {
        let segment: Segment = syn::parse2(call(&thin)).expect("the call parses");
        let Serve::Service(service) = &segment.serve else {
            continue;
        };
        let before = syn::parse_file(&std::fs::read_to_string(expanded).expect("the library"))
            .expect("the library parses");
        let after = syn::parse2(expand(&segment, &quote!(gdl_runtime))).expect("the expansion");
        assert_eq!(items(&after), items(&before), "{}", thin.display());
        for (_, expression) in &service.entries {
            if expression.is_some() {
                enabled += 1;
            } else {
                disabled += 1;
            }
        }
    }
    assert!(
        enabled > 0,
        "the oracle must exercise enabled service clients"
    );
    assert!(
        disabled > 0,
        "no disabled service client is checked against a frozen expansion; the migrated responses: None and anthropic: None assertions only check the macro input"
    );
}
