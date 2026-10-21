//! `#[derive(Metrics)]`: a node's metrics, from a struct of prometheus
//! metric fields. The definitions are read by `gdl_syntax::metric_fields`,
//! which the source reader shares, so the catalogue and the build agree.

use proc_macro2::TokenStream;
use quote::quote;

pub(crate) fn expand(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(input, "Metrics requires a struct"));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "a metrics type cannot be generic: a node names one",
        ));
    }
    let definitions = gdl_syntax::metric_fields(&data.fields)?;
    let values = definitions.iter().map(|d| {
        let (name, help) = (&d.name, &d.help);
        let kind = match d.kind {
            gdl_model::MetricKind::Counter => quote!(Counter),
            gdl_model::MetricKind::Gauge => quote!(Gauge),
            gdl_model::MetricKind::Histogram => quote!(Histogram),
        };
        let labels = &d.labels;
        let buckets = &d.buckets;
        quote! {
            ::gdl::MetricField {
                name: #name.to_string(),
                help: #help.to_string(),
                kind: ::gdl::MetricKind::#kind,
                labels: ::std::vec![#(#labels.to_string()),*],
                buckets: ::std::vec![#(#buckets),*],
            }
        }
    });
    let fields = data.fields.iter().enumerate().map(|(index, field)| {
        let ident = field
            .ident
            .as_ref()
            .expect("metric_fields refuses unnamed fields");
        quote!(#ident: ::gdl::__private::metric(registry, node, &definitions[#index])?)
    });
    let ident = &input.ident;
    Ok(quote! {
        impl ::gdl::__private::MetricsDerived for #ident {}
        impl ::gdl::Metrics for #ident {
            fn definitions() -> ::std::vec::Vec<::gdl::MetricField> {
                ::std::vec![#(#values),*]
            }
            fn register(
                registry: &::gdl::prometheus::Registry,
                node: &str,
            ) -> ::gdl::prometheus::Result<Self> {
                let definitions = <Self as ::gdl::Metrics>::definitions();
                ::core::result::Result::Ok(Self { #(#fields),* })
            }
        }
    })
}
