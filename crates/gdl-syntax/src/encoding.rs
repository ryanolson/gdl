use gdl_model::Encoding;
use proc_macro2::TokenTree;
use syn::Meta;

/// Read a message's encoding with the same rules in the derive and source reader.
pub fn message_encoding(attrs: &[Meta]) -> syn::Result<Encoding> {
    let mut encoding = None;
    for attr in attrs.iter().filter(|a| a.path().is_ident("gdl")) {
        let Meta::List(list) = attr else {
            return Err(syn::Error::new_spanned(
                attr,
                "expected #[gdl(codec = \"...\")]",
            ));
        };
        list.parse_nested_meta(|meta| {
            if !meta.path.is_ident("codec") {
                return Err(meta.error(format!("expected codec = \"...\", one of: {}", choices())));
            }
            if encoding.is_some() {
                return Err(meta.error("a message declares its codec only once"));
            }
            let value: syn::LitStr = meta.value()?.parse()?;
            let name = value.value();
            encoding = Some(
                Encoding::ALL
                    .into_iter()
                    .find(|encoding| encoding.name() == name)
                    .ok_or_else(|| {
                        syn::Error::new_spanned(
                            value,
                            format!("unknown codec: expected {}", choices()),
                        )
                    })?,
            );
            Ok(())
        })?;
    }
    Ok(encoding.unwrap_or_default())
}

fn choices() -> String {
    Encoding::ALL.map(Encoding::name).join(", ")
}

/// The first field of one struct or enum variant that positional
/// MessagePack (`msgpack_array`) cannot carry: its index in `fields`, and
/// the serde key that omits it. `container` is the type's attributes;
/// `fields` holds each field's; `unnamed` is whether the fields are a
/// tuple's; `variant` is the variant's attributes, none for a struct. All
/// are read with `cfg` and `cfg_attr` applied.
///
/// An array has no field names, so a field that serialization can omit
/// and deserialization reads, or the reverse, shifts every field after it
/// into the wrong one: `skip_serializing_if` in any combination, or
/// `skip_serializing` or `skip_deserializing` alone. `skip`, or both
/// one-sided skips, omits the field in both directions and keeps every
/// position. serde writes a `transparent` struct, and a newtype struct (one
/// unnamed field), as the field's value and ignores its skips; it ignores
/// `skip_serializing_if` on a newtype variant's field; and it refuses a
/// variant it skips in either direction before reading its fields. None
/// of those shifts anything. The derive and the source reader apply this
/// one rule.
pub fn positional_omission(
    container: &[Meta],
    fields: &[Vec<Meta>],
    unnamed: bool,
    variant: Option<&[Meta]>,
) -> Option<(usize, &'static str)> {
    let newtype = unnamed && fields.len() == 1;
    if variant.is_none() && (newtype || skips(container).transparent) {
        return None;
    }
    if variant.is_some_and(|attrs| {
        let skips = skips(attrs);
        skips.skip || skips.serializing || skips.deserializing
    }) {
        return None;
    }
    fields.iter().enumerate().find_map(|(index, attrs)| {
        let skips = skips(attrs);
        let key = if skips.skip || skips.serializing && skips.deserializing {
            None
        } else if skips.conditional && !newtype {
            Some("skip_serializing_if")
        } else if skips.serializing {
            Some("skip_serializing")
        } else if skips.deserializing {
            Some("skip_deserializing")
        } else {
            None
        };
        key.map(|key| (index, key))
    })
}

/// The keys among serde attributes `attrs` that decide an omission.
#[derive(Default)]
struct Skips {
    skip: bool,
    serializing: bool,
    deserializing: bool,
    conditional: bool,
    transparent: bool,
}

fn skips(attrs: &[Meta]) -> Skips {
    let mut skips = Skips::default();
    for attr in attrs {
        let Meta::List(list) = attr else {
            continue;
        };
        if !list.path.is_ident("serde") {
            continue;
        }
        // A key is the word that starts an entry. A value or a group
        // (`bound(..)`) is one token tree, so its words are never keys.
        let mut start = true;
        for token in list.tokens.clone() {
            match token {
                TokenTree::Punct(p) if p.as_char() == ',' => {
                    start = true;
                    continue;
                }
                TokenTree::Ident(ident) if start => {
                    skips.skip |= ident == "skip";
                    skips.serializing |= ident == "skip_serializing";
                    skips.deserializing |= ident == "skip_deserializing";
                    skips.conditional |= ident == "skip_serializing_if";
                    skips.transparent |= ident == "transparent";
                }
                _ => {}
            }
            start = false;
        }
    }
    skips
}

/// Why `msgpack_array` refuses field `field` of message `message`, whose
/// serde key `key` ([`positional_omission`]) omits it in one direction:
/// one text for the derive and the source reader.
pub fn positional_refusal(message: &str, field: &str, key: &str) -> String {
    format!(
        "message `{message}`: `msgpack_array` cannot carry field `{field}`: with `{key}`, the writer and the reader can disagree on whether the field is there, and an array has no field names, so the fields after it would shift; use `codec = \"msgpack\"` or `codec = \"json\"`"
    )
}
