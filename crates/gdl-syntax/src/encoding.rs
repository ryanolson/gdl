use gdl_model::Encoding;
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
                return Err(
                    meta.error("expected codec = \"msgpack\", \"msgpack_array\", or \"json\"")
                );
            }
            if encoding.is_some() {
                return Err(meta.error("a message declares its codec only once"));
            }
            let value: syn::LitStr = meta.value()?.parse()?;
            encoding = Some(match value.value().as_str() {
                "msgpack" => Encoding::Msgpack,
                "msgpack_array" => Encoding::MsgpackArray,
                "json" => Encoding::Json,
                _ => {
                    return Err(syn::Error::new_spanned(
                        value,
                        "unknown codec: expected msgpack, msgpack_array, or json",
                    ));
                }
            });
            Ok(())
        })?;
    }
    Ok(encoding.unwrap_or_default())
}
