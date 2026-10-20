use syn::{GenericArgument, PathArguments, Type};

/// One field of a typed service client bundle.
pub struct Endpoint {
    /// The public client field.
    pub name: syn::Ident,
    /// The public endpoint path.
    pub path: String,
    /// The graph boolean that enables this entry.
    pub enabled: String,
    /// Request message type.
    pub request: Type,
    /// Response item type.
    pub response: Type,
}

/// Read `Option<Client<Request, Response>>` fields and their endpoint markers.
pub fn endpoints(fields: &syn::Fields) -> syn::Result<Vec<Endpoint>> {
    let mut result = Vec::new();
    let mut paths = std::collections::BTreeSet::new();
    for field in fields {
        let mut path = None;
        let mut enabled = None;
        for attr in field.attrs.iter().filter(|a| a.path().is_ident("gdl")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("endpoint") {
                    path = Some(meta.value()?.parse::<syn::LitStr>()?.value());
                    Ok(())
                } else if meta.path.is_ident("enabled") {
                    enabled = Some(meta.value()?.parse::<syn::LitStr>()?.value());
                    Ok(())
                } else {
                    Err(meta.error("expected endpoint or enabled"))
                }
            })?;
        }
        let fail = || {
            syn::Error::new_spanned(
                field,
                "an endpoint requires a public named Option<Client<Request, Response>> field and endpoint/enabled markers",
            )
        };
        let name = field.ident.clone().ok_or_else(fail)?;
        if !matches!(field.vis, syn::Visibility::Public(_)) {
            return Err(fail());
        }
        let (Some(path), Some(enabled)) = (path, enabled) else {
            return Err(fail());
        };
        if !path.starts_with('/') || !paths.insert(path.clone()) {
            return Err(syn::Error::new_spanned(
                field,
                "endpoint paths must be absolute and unique",
            ));
        }
        let optional = arguments(&field.ty, "Option").ok_or_else(fail)?;
        if optional.len() != 1 {
            return Err(fail());
        }
        let clients = arguments(&optional[0], "Client").ok_or_else(fail)?;
        if clients.len() != 2 {
            return Err(fail());
        }
        result.push(Endpoint {
            name,
            path,
            enabled,
            request: clients[0].clone(),
            response: clients[1].clone(),
        });
    }
    if result.is_empty() {
        return Err(syn::Error::new_spanned(
            fields,
            "a service requires at least one declared endpoint",
        ));
    }
    Ok(result)
}
fn arguments(ty: &Type, name: &str) -> Option<Vec<Type>> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != name {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args
        .iter()
        .map(|arg| {
            if let GenericArgument::Type(ty) = arg {
                Some(ty.clone())
            } else {
                None
            }
        })
        .collect()
}
