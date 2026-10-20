use gdl_model::{ConfigField, ConfigValue};
use syn::ext::IdentExt;

/// Read explicitly marked scalar graph fields from a configuration struct.
pub fn graph_fields(fields: &syn::Fields) -> syn::Result<Vec<ConfigField>> {
    let mut result = Vec::new();
    for field in fields {
        let mut graph = false;
        let mut default = None;
        let mut label = String::new();
        let mut subtext = String::new();
        for attr in field.attrs.iter().filter(|a| a.path().is_ident("gdl")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("graph") {
                    graph = true;
                    Ok(())
                } else if meta.path.is_ident("default") {
                    default = Some(meta.value()?.parse::<syn::Lit>()?);
                    Ok(())
                } else if meta.path.is_ident("label") {
                    label = meta.value()?.parse::<syn::LitStr>()?.value();
                    Ok(())
                } else if meta.path.is_ident("subtext") {
                    subtext = meta.value()?.parse::<syn::LitStr>()?.value();
                    Ok(())
                } else {
                    Err(meta.error("expected graph, default, label, or subtext"))
                }
            })?;
        }
        if !graph {
            continue;
        }
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "graph config requires named fields"))?;
        let mut name = ident.unraw().to_string();
        for attr in field.attrs.iter().filter(|a| a.path().is_ident("serde")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") { name = meta.value()?.parse::<syn::LitStr>()?.value(); Ok(()) }
                else { Err(meta.error("graph fields support only serde(rename); graph defaults are declared with gdl(default)")) }
            })?;
        }
        let default = default.ok_or_else(|| {
            syn::Error::new_spanned(field, "graph config requires an explicit default")
        })?;
        let type_name = if let syn::Type::Path(ty) = &field.ty {
            ty.path.segments.last().map(|s| s.ident.to_string())
        } else {
            None
        };
        let value = match (type_name.as_deref(), default) {
            (Some("bool"), syn::Lit::Bool(v)) => ConfigValue::Bool(v.value),
            (Some("String"), syn::Lit::Str(v)) => ConfigValue::Text(v.value()),
            (Some("i64"), syn::Lit::Int(v)) => ConfigValue::Integer(v.base10_parse()?),
            _ => {
                return Err(syn::Error::new_spanned(
                    field,
                    "graph config supports bool, i64, or String with a matching literal default",
                ));
            }
        };
        let description = field
            .attrs
            .iter()
            .filter_map(|a| {
                if !a.path().is_ident("doc") {
                    return None;
                }
                if let syn::Meta::NameValue(n) = &a.meta
                    && let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = &n.value
                {
                    return Some(s.value().trim().to_string());
                }
                None
            })
            .collect::<Vec<_>>()
            .join(" ");
        result.push(ConfigField {
            label,
            subtext,
            name,
            default: value,
            description,
        });
    }
    Ok(result)
}

/// Read the configuration type a node exposes to catalog discovery.
pub fn node_config(attrs: &[syn::Attribute]) -> syn::Result<Option<syn::Path>> {
    let mut config = None;
    for attr in attrs.iter().filter(|a| a.path().is_ident("gdl")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("config") {
                if config.is_some() {
                    return Err(meta.error("configuration type declared twice"));
                }
                config = Some(meta.value()?.parse()?);
                Ok(())
            } else {
                Err(meta.error("expected config"))
            }
        })?;
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    #[test]
    fn graph_fields_carry_display_metadata_separate_from_serialized_names() {
        let input: syn::DeriveInput = syn::parse_quote! {
            struct Settings {
                /// Serve the public chat protocol.
                #[gdl(graph, default = true, label = "Chat Completions", subtext = "OpenAI chat endpoint")]
                enabled_chat: bool,
            }
        };
        let syn::Data::Struct(data) = input.data else {
            panic!("struct")
        };
        let fields = super::graph_fields(&data.fields).expect("display metadata");
        assert_eq!(fields[0].name, "enabled_chat");
        assert_eq!(fields[0].label, "Chat Completions");
        assert_eq!(fields[0].subtext, "OpenAI chat endpoint");
        assert_eq!(fields[0].description, "Serve the public chat protocol.");
    }
}
