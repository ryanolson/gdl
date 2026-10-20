use proc_macro2::TokenStream;
use quote::quote;
pub(crate) fn expand(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    if !input.generics.params.is_empty() || !matches!(input.vis, syn::Visibility::Public(_)) {
        return Err(syn::Error::new_spanned(
            input,
            "Endpoints requires a public, non-generic struct",
        ));
    }
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "Endpoints requires a struct",
        ));
    };
    let entries = gdl_syntax::endpoints(&data.fields)?;
    let entries = entries.iter().map(|e| {
        let name = e.name.to_string(); let path = &e.path; let enabled = &e.enabled;
        let request = &e.request; let response = &e.response;
        quote!(::gdl::__private::Endpoint {name:#name, path:#path, enabled:#enabled, request: ::gdl::__private::port::<#request>(), response: ::gdl::__private::port::<#response>()})
    });
    let name = &input.ident;
    Ok(
        quote! {impl ::gdl::Endpoints for #name {fn entries() -> ::std::vec::Vec<::gdl::__private::Endpoint> {::std::vec![#(#entries),*]}}},
    )
}
