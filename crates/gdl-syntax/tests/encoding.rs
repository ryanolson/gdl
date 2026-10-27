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

/// The serde key that makes each field of `input` unsafe in positional
/// MessagePack, by field, each field read on its own.
fn omissions(input: &str) -> Vec<Option<&'static str>> {
    let input: syn::DeriveInput = syn::parse_str(input).expect("a type");
    let syn::Data::Struct(data) = input.data else {
        panic!("a struct");
    };
    data.fields
        .iter()
        .map(|field| {
            let attrs = field.attrs.iter().map(|a| a.meta.clone()).collect();
            gdl_syntax::positional_omission(&[], &[attrs], false, None).map(|(_, key)| key)
        })
        .collect()
}

/// The first omission among the fields of `fields`, a struct's or a
/// variant's, as the derive reads them.
fn first(input: &str) -> Option<(usize, &'static str)> {
    let input: syn::DeriveInput = syn::parse_str(input).expect("a type");
    let metas = |attrs: &[syn::Attribute]| attrs.iter().map(|a| a.meta.clone()).collect::<Vec<_>>();
    let container = metas(&input.attrs);
    let parts: Vec<(Option<Vec<syn::Meta>>, syn::Fields)> = match input.data {
        syn::Data::Struct(data) => vec![(None, data.fields)],
        syn::Data::Enum(data) => data
            .variants
            .into_iter()
            .map(|v| (Some(metas(&v.attrs)), v.fields))
            .collect(),
        syn::Data::Union(_) => panic!("a struct or an enum"),
    };
    parts.into_iter().find_map(|(variant, fields)| {
        let attrs: Vec<_> = fields.iter().map(|f| metas(&f.attrs)).collect();
        let unnamed = matches!(fields, syn::Fields::Unnamed(_));
        gdl_syntax::positional_omission(&container, &attrs, unnamed, variant.as_deref())
    })
}

/// serde writes a newtype, or a transparent struct, as its value and
/// ignores the field's skips, and
/// refuses a variant it skips before reading its fields: neither shifts a
/// position. A tuple of two fields, or one named field, still does.
#[test]
fn a_newtype_or_a_skipped_variant_has_no_positional_omission() {
    let conditional = r#"#[serde(skip_serializing_if = "Option::is_none")]"#;
    assert_eq!(
        first(&format!("struct A({conditional} Option<u32>);")),
        None
    );
    assert_eq!(
        first(&format!(
            "#[serde(transparent)] struct A {{ {conditional} a: Option<u32>, #[serde(skip)] b: () }}"
        )),
        None
    );
    assert_eq!(
        first(&format!("enum A {{ V({conditional} Option<u32>) }}")),
        None
    );
    for skip in ["skip", "skip_serializing", "skip_deserializing"] {
        assert_eq!(
            first(&format!(
                "enum A {{ V(u32), #[serde({skip})] W {{ {conditional} a: Option<u32>, b: u32 }} }}"
            )),
            None,
            "{skip}"
        );
    }
    assert_eq!(
        first(&format!("struct A(u32, {conditional} Option<u32>);")),
        Some((1, "skip_serializing_if"))
    );
    assert_eq!(
        first(&format!("struct A {{ {conditional} a: Option<u32> }}")),
        Some((0, "skip_serializing_if"))
    );
    assert_eq!(
        first(&format!(
            "enum A {{ #[serde(rename = \"v\")] V {{ a: u32, {conditional} b: Option<u32> }} }}"
        )),
        Some((1, "skip_serializing_if"))
    );
}

/// A field that one direction writes and the other does not read shifts
/// every field after it in an array: `skip_serializing_if` in any
/// combination, or a one-sided skip alone.
#[test]
fn a_field_one_direction_can_omit_is_a_positional_omission() {
    let found = omissions(
        r#"struct A {
            #[serde(skip_serializing_if = "Option::is_none")] a: Option<u32>,
            #[serde(default, skip_serializing_if = "Option::is_none", rename = "x")] b: Option<u32>,
            #[serde(skip_serializing)] c: u32,
            #[serde(skip_deserializing)] d: u32,
            #[serde(skip_deserializing, skip_serializing_if = "is_zero")] e: u32,
            #[serde(rename = "f")] #[serde(skip_serializing)] f: u32,
        }"#,
    );
    assert_eq!(
        found,
        [
            Some("skip_serializing_if"),
            Some("skip_serializing_if"),
            Some("skip_serializing"),
            Some("skip_deserializing"),
            Some("skip_serializing_if"),
            Some("skip_serializing"),
        ]
    );
}

/// A field both directions skip, or neither, keeps every position; so do
/// serde keys that only look like a skip, and other attributes.
#[test]
fn a_field_both_directions_agree_on_is_no_positional_omission() {
    let found = omissions(
        r#"struct A {
            a: u32,
            #[serde(skip)] b: u32,
            #[serde(skip_serializing, skip_deserializing)] c: u32,
            #[serde(skip_serializing)] #[serde(skip_deserializing)] d: u32,
            #[serde(rename = "skip_serializing_if", default = "skip_serializing")] e: u32,
            #[serde(bound(serialize = "skip_serializing_if"))] f: u32,
            #[gdl(skip_serializing_if)] #[doc = "skip_serializing"] g: u32,
            #[serde(with = "skip_serializing_if")] h: u32,
        }"#,
    );
    assert_eq!(found, [None; 8]);
}

#[test]
fn a_newtype_variant_still_applies_its_one_sided_skips() {
    for skip in ["skip_serializing", "skip_deserializing"] {
        assert_eq!(
            first(&format!("struct A(#[serde({skip})] u32);")),
            None,
            "a newtype struct ignores {skip}"
        );
        assert_eq!(
            first(&format!("enum A {{ Token(#[serde({skip})] u32) }}")),
            Some((0, skip)),
            "a newtype variant applies {skip}"
        );
    }
}
