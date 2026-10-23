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
