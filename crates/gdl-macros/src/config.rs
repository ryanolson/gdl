use proc_macro2::TokenStream;
use quote::quote;

pub(crate) fn expand(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(input, "Config requires a struct"));
    };
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("deny_unknown_fields") {
                Ok(())
            } else {
                Err(meta.error("Config supports only deny_unknown_fields at struct level"))
            }
        })?;
    }
    let fields = gdl_syntax::graph_fields(&data.fields)?;
    let values = fields.iter().map(|f| {
        let label = &f.label; let subtext = &f.subtext;
        let name = &f.name; let description = &f.description;
        let default = match &f.default {
            gdl_model::ConfigValue::Bool(v) => quote!(::gdl::ConfigValue::Bool(#v)),
            gdl_model::ConfigValue::Integer(v) => quote!(::gdl::ConfigValue::Integer(#v)),
            gdl_model::ConfigValue::Text(v) => quote!(::gdl::ConfigValue::Text(#v.to_string())),
        };
        quote!(::gdl::ConfigField {label:#label.to_string(), subtext:#subtext.to_string(), name:#name.to_string(), description:#description.to_string(), default:#default})
    });
    let ident = &input.ident;
    let (impl_generics, ty_generics, clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::gdl::ConfigSchema for #ident #ty_generics #clause {
            fn graph_fields() -> ::std::vec::Vec<::gdl::ConfigField> { ::std::vec![#(#values),*] }
        }
    })
}
