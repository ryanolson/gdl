//! Codec declarations are parsed identically for derives and source extraction.

use gdl_model::Encoding;

fn encoding(input: &str) -> syn::Result<Encoding> {
    let input: syn::DeriveInput = syn::parse_str(input)?;
    gdl_syntax::message_encoding(
        &input
            .attrs
            .iter()
            .map(|a| a.meta.clone())
            .collect::<Vec<_>>(),
    )
}

#[test]
fn message_codecs_have_one_default_and_explicit_variants() {
    assert_eq!(
        encoding("struct A;").expect("valid codec declaration"),
        Encoding::Msgpack
    );
    for expected in Encoding::ALL {
        let name = expected.name();
        assert_eq!(
            encoding(&format!("#[gdl(codec = {name:?})] struct A;"))
                .expect("valid codec declaration"),
            expected
        );
    }
}

#[test]
fn invalid_or_duplicate_codec_declarations_are_errors() {
    for input in [
        "#[gdl(codec = \"bincode\")] struct A;",
        "#[gdl(codec = 1)] struct A;",
        "#[gdl(codec)] struct A;",
        "#[gdl(codex = \"json\")] struct A;",
        "#[gdl(codec = \"json\", codec = \"msgpack\")] struct A;",
        "#[gdl(codec = \"json\")] #[gdl(codec = \"json\")] struct A;",
    ] {
        assert!(encoding(input).is_err(), "accepted {input}");
    }
}
