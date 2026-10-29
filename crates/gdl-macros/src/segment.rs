//! What `segment!` generates: one segment crate's library, from the data
//! `gdl-gen` computes for it.
//!
//! - `contract`, `SEGMENT`, `SEGMENTS`, and `PLACEMENT`;
//! - one type alias and one compile-time check per message the segment
//!   sends or receives across a cut, or that an anchor carries in it. Each
//!   carries the span of its alias in the call, so that a message that is
//!   not a wire type fails to build at that line;
//! - the checks of the types the binary reads or writes as JSON lines;
//! - `Nodes` and `start`, which deploy the segment inside a program;
//! - `Settings`, `NodeSettings`, and `Hosts`, and `Binary`, whose
//!   `SegmentBinary` impl is what the segment's binary runs.
//!
//! The call is written by the generator, and both come from one revision of
//! gdl: its input is not a stable interface. Paths to the runtime go through
//! the path the `macro_rules!` wrapper passes, `$crate`. A generated crate
//! also depends on `serde`, `tokio`, and `toml` itself, and the expansion
//! names them as that crate does.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::{Ident, LitStr};

mod input;

pub(crate) use input::Segment;
use input::{Crossing, Serve, Service};

pub(crate) fn expand(segment: &Segment, krate: &TokenStream) -> TokenStream {
    let consts = consts(segment);
    let crossings = crossings(segment, krate);
    let feeds = feeds(segment, krate);
    let nodes = nodes(segment);
    let start = start(segment, krate);
    let binary = binary(segment, krate);
    quote! {
        #consts
        #crossings
        #feeds
        #nodes
        #start
        #binary
    }
}

/// `" text"`, as `/// text` writes a doc attribute.
fn doc(text: &str) -> TokenStream {
    let text = format!(" {text}");
    quote!(#[doc = #text])
}

fn consts(segment: &Segment) -> TokenStream {
    let Segment {
        contract,
        segment: name,
        segments,
        placement,
        ..
    } = segment;
    let segment_doc = doc("This segment's name in the placement.");
    let segments_doc = [
        doc("Every segment of the placement. An instance waits for all of them,"),
        doc("or, in a pool, only for those of its class."),
    ];
    let placement_doc = doc("The placement this crate was generated for.");
    quote! {
        pub use #contract as contract;
        #segment_doc
        pub const SEGMENT: &str = #name;
        #(#segments_doc)*
        pub const SEGMENTS: &[&str] = &[#(#segments),*];
        #placement_doc
        pub const PLACEMENT: &str = #placement;
    }
}

/// One alias and one check per crossing, each spanned at its alias.
fn crossings(segment: &Segment, krate: &TokenStream) -> TokenStream {
    let items = segment.crossings.iter().map(|crossing| {
        let Crossing {
            check,
            alias,
            node,
            decl,
            port,
            part,
        } = crossing;
        let port = quote!(<contract::nodes::#node as #krate::#decl>::#port);
        let ty = match part {
            None => port,
            Some(part) => {
                let part = match part.to_string().as_str() {
                    "head" => quote!(Head),
                    _ => quote!(Item),
                };
                quote!(<#port as #krate::gdl::__private::Parts>::#part)
            }
        };
        quote_spanned! {alias.span()=>
            #[allow(non_camel_case_types)]
            type #alias = #ty;
            const _: () = #krate::gdl::__private::#check::<#alias>();
        }
    });
    quote!(#(#items)*)
}

fn feeds(segment: &Segment, krate: &TokenStream) -> TokenStream {
    let feeds = segment.feeds.iter().map(|feed| {
        // The error points at the type argument: spanned at the feed's name
        // in the call, as each crossing's is at its alias.
        let (check, ty) = match feed.to_string().as_str() {
            "reads_json" => (feed, quote_spanned!(feed.span()=> contract::SourceReq)),
            _ => (feed, quote_spanned!(feed.span()=> contract::SourceItem)),
        };
        quote_spanned!(feed.span()=> const _: () = #krate::#check::<#ty>();)
    });
    quote!(#(#feeds)*)
}

fn nodes(segment: &Segment) -> TokenStream {
    let nodes_doc = doc("The implementations of the nodes this segment hosts.");
    let fields = segment.nodes.iter().map(|node| {
        let field_doc = doc(&format!("Node `{}`.", node.name));
        let (binding, ty) = (&node.binding, &node.ty);
        quote! {
            #field_doc
            pub #binding: #ty,
        }
    });
    quote! {
        #nodes_doc
        pub struct Nodes { #(#fields)* }
    }
}

/// The type of what one root of the graph answers.
fn root_type(root: &Ident, krate: &TokenStream) -> TokenStream {
    quote! {
        impl #krate::gdl::Next<
            <contract::nodes::#root as #krate::SourceDecl>::Req,
            <contract::nodes::#root as #krate::SourceDecl>::Item,
        > + #krate::Register + use<>
    }
}

fn start(segment: &Segment, krate: &TokenStream) -> TokenStream {
    let roots = &segment.roots;
    let root = if roots.len() == 1 {
        root_type(&roots[0], krate)
    } else {
        roots.iter().rev().fold(quote!(()), |tail, root| {
            let head = root_type(root, krate);
            quote!((#head, #tail))
        })
    };
    let bindings = segment.nodes.iter().map(|node| {
        let binding = &node.binding;
        quote!(.#binding(nodes.#binding))
    });
    let unbound = segment
        .nodes
        .is_empty()
        .then(|| quote!(let Nodes {} = nodes;));
    let start_doc = [
        doc("Deploy this segment on `node`. Every segment's instances must be in"),
        doc("`roster`."),
    ];
    quote! {
        #(#start_doc)*
        pub fn start(
            node: #krate::velo::Velo,
            roster: &#krate::Roster,
            config: #krate::RuntimeConfig,
            nodes: Nodes,
        ) -> #krate::gdl::Result<#krate::Segment<#root>> {
            let placement = #krate::Placement::from_toml(PLACEMENT)?;
            let manifest = contract::manifest();
            let deployment =
                #krate::Deployment::new(node, &manifest, &placement, SEGMENT, roster, config)?;
            #unbound
            let root = contract::assemble(&deployment, contract::Bindings::new() #(#bindings)*)?;
            deployment.start(root)
        }
    }
}

/// What `build` returns, as [`Built`] says, and the readiness of each.
struct Built {
    /// `Hosts`' fields; none when `build` returns the nodes alone.
    fields: Vec<Ident>,
    ready: Vec<TokenStream>,
}

impl Built {
    fn new(segment: &Segment, krate: &TokenStream) -> Self {
        let (frontend, service) = segment.serve.hosts();
        let mut fields = Vec::new();
        if frontend.is_some() || service.is_some() {
            fields.push(format_ident!("nodes"));
            if frontend.is_some() {
                fields.push(format_ident!("frontend"));
            }
            if let Some(service) = service {
                fields.extend(service.adapters.iter().map(|a| a.variable.clone()));
            }
        }
        let nodes = if fields.is_empty() {
            quote!(hosts)
        } else {
            quote!(hosts.nodes)
        };
        let mut ready: Vec<TokenStream> = segment
            .nodes
            .iter()
            .map(|node| {
                let binding = &node.binding;
                quote!(#krate::gdl::Build::ready(&#nodes.#binding))
            })
            .collect();
        if frontend.is_some() {
            ready.push(quote!(#krate::gdl::Build::ready(&hosts.frontend)));
        }
        if let Some(service) = service {
            ready.extend(service.adapters.iter().map(|adapter| {
                let variable = &adapter.variable;
                quote!(#krate::gdl::Build::ready(&hosts.#variable))
            }));
        }
        Self { fields, ready }
    }

    fn extra(&self) -> bool {
        !self.fields.is_empty()
    }

    fn ty(&self) -> Ident {
        if self.extra() {
            format_ident!("Hosts")
        } else {
            format_ident!("Nodes")
        }
    }
}

fn binary(segment: &Segment, krate: &TokenStream) -> TokenStream {
    let (frontend, _) = segment.serve.hosts();
    let built = Built::new(segment, krate);
    let settings = settings_types(segment, krate);
    let hosts = built.extra().then(|| hosts_type(segment));
    let package = &segment.package;
    let binary_doc = [
        doc(&format!(
            "What the `{}` binary hosts and serves; its `main` is",
            package.value()
        )),
        doc("`gdl_runtime::segment_main::<Binary>()`."),
    ];
    let graph_config = &segment.graph_config;
    let ty = built.ty();
    let build = build_fn(segment, krate, &built);
    let ready = (!built.ready.is_empty()).then(|| {
        let ready = &built.ready;
        quote! {
            async fn ready(hosts: &#ty) -> #krate::gdl::Result<()> {
                tokio::try_join!(#(#ready),*)?;
                Ok(())
            }
        }
    });
    let start = start_fn(segment, krate, &built, frontend.is_some());
    quote! {
        #settings
        #hosts
        #(#binary_doc)*
        pub struct Binary;
        impl #krate::SegmentBinary for Binary {
            const PACKAGE: &'static str = #package;
            const SEGMENT: &'static str = SEGMENT;
            const SEGMENTS: &'static [&'static str] = SEGMENTS;
            const GRAPH_CONFIG: &'static str = #graph_config;
            type Settings = Settings;
            type Hosts = #ty;
            #build
            #ready
            fn deployment() -> #krate::gdl::Result<#krate::DeploymentSpec> {
                Ok(#krate::DeploymentSpec::new(
                    contract::manifest(),
                    #krate::Placement::from_toml(PLACEMENT)?,
                )?)
            }
            #start
        }
    }
}

/// `Settings` and `NodeSettings`: the tables this segment takes, and what
/// the runtime needs to know to address them from the environment.
fn settings_types(segment: &Segment, krate: &TokenStream) -> TokenStream {
    let (frontend, service) = segment.serve.hosts();
    let policies = !segment.policies.is_empty();
    let mut keys = vec!["node"];
    let mut fields = vec![quote!(#[serde(default)] node: NodeSettings,)];
    if frontend.is_some() {
        keys.push("frontend");
        fields.push(quote!(#[serde(default)] frontend: Option<toml::Table>,));
    }
    if service.is_some() {
        keys.push("adapter");
        fields.push(
            quote!(#[serde(default)] adapter: std::collections::BTreeMap<String, toml::Table>,),
        );
    }
    if policies {
        keys.push("pool_policy");
        fields.push(
            quote!(#[serde(default)] pool_policy: std::collections::BTreeMap<String, toml::Table>,),
        );
    }
    let names: Vec<String> = segment
        .nodes
        .iter()
        .map(|node| node.name.to_string())
        .chain(
            service
                .iter()
                .flat_map(|s| &s.adapters)
                .map(|adapter| adapter.name.value()),
        )
        .collect();
    let node_fields = segment.nodes.iter().map(|node| {
        let name = node.name.to_string();
        let binding = &node.binding;
        quote!(#[serde(rename = #name, default)] #binding: Option<toml::Table>,)
    });
    let settings_doc = doc("What this segment's binary hosts, as its settings name it.");
    quote! {
        #settings_doc
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Settings { #(#fields)* }
        impl #krate::Hosted for Settings {
            const KEYS: &'static [&'static str] = &[#(#keys),*];
            const NAMES: &'static [&'static str] = &[#(#names),*];
        }
        #[derive(Default, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct NodeSettings { #(#node_fields)* }
    }
}

/// `Hosts`: the nodes, and the source's frontend or entry adapters, which
/// the serving body takes rather than the graph.
fn hosts_type(segment: &Segment) -> TokenStream {
    let (frontend, service) = segment.serve.hosts();
    let frontend = frontend.map(|(_, ty)| quote!(frontend: #ty,));
    let adapters = service.iter().flat_map(|s| &s.adapters).map(|adapter| {
        let (variable, ty) = (&adapter.variable, &adapter.ty);
        quote!(#variable: #ty,)
    });
    let hosts_doc =
        doc("What this segment's binary builds: its nodes, and what serves the source.");
    quote! {
        #hosts_doc
        pub struct Hosts {
            nodes: Nodes,
            #frontend
            #(#adapters)*
        }
    }
}

/// `build`: each hosted implementation from its settings table, before
/// the segment meets its peers, so that a bad table fails before anyone
/// waits for it.
fn build_fn(segment: &Segment, krate: &TokenStream, built: &Built) -> TokenStream {
    let (frontend, service) = segment.serve.hosts();
    let policies = &segment.policies;
    let takes = !segment.nodes.is_empty()
        || frontend.is_some()
        || service.is_some()
        || !policies.is_empty();
    let builder = if takes {
        format_ident!("builder")
    } else {
        format_ident!("_builder")
    };
    let ty = built.ty();
    let mut fields = vec![quote!(node)];
    if frontend.is_some() {
        fields.push(quote!(frontend));
    }
    if service.is_some() {
        fields.push(quote!(mut adapter));
    }
    if !policies.is_empty() {
        fields.push(quote!(mut pool_policy));
    }
    let nodes = if segment.nodes.is_empty() {
        quote! {
            let NodeSettings {} = node;
            let nodes = Nodes {};
        }
    } else {
        let built = segment.nodes.iter().map(|node| {
            let (binding, ty) = (&node.binding, &node.ty);
            let name = node.name.to_string();
            quote!(#binding: builder.build::<#ty>(#name, node.#binding).await?,)
        });
        quote!(let nodes = Nodes { #(#built)* };)
    };
    let frontend = frontend.map(|(name, ty)| {
        let name = name.to_string();
        quote!(let frontend = builder.build::<#ty>(#name, frontend).await?;)
    });
    let adapters = service.map(|service| {
        let enabled: Vec<String> = service.adapters.iter().map(|a| a.name.value()).collect();
        let built = service.adapters.iter().map(|adapter| {
            let (variable, ty, name) = (&adapter.variable, &adapter.ty, &adapter.name);
            quote!(let #variable = builder.build::<#ty>(#name, adapter.remove(#name)).await?;)
        });
        quote! {
            let enabled: &[&str] = &[#(#enabled),*];
            if let Some(unknown) = adapter.keys().find(|key| !enabled.contains(&key.as_str())) {
                return Err(format!("[adapter.{unknown}]: no enabled entry node of that name").into());
            }
            #(#built)*
        }
    });
    let policies = (!policies.is_empty()).then(|| {
        let names: Vec<&LitStr> = policies.iter().map(|(name, _)| name).collect();
        let built = policies.iter().map(|(name, ty)| {
            quote!(builder.build_pool_policy::<#ty>(#name, pool_policy.remove(#name)).await?;)
        });
        quote! {
            let policies: &[&str] = &[#(#names),*];
            if let Some(unknown) = pool_policy.keys().find(|key| !policies.contains(&key.as_str())) {
                return Err(format!("[pool_policy.{unknown}]: this segment chooses pools by no policy of that name").into());
            }
            #(#built)*
        }
    });
    let done = if built.extra() {
        let fields = &built.fields;
        quote!(Ok(Hosts { #(#fields),* }))
    } else {
        quote!(Ok(nodes))
    };
    quote! {
        async fn build(settings: Settings, #builder: &#krate::Builder) -> Result<#ty, #krate::Failure> {
            let Settings { #(#fields),* } = settings;
            #nodes
            #frontend
            #adapters
            #policies
            #done
        }
    }
}

/// `start`: the segment's graph, and the body that serves it once the
/// segments it waits for are ready.
fn start_fn(segment: &Segment, krate: &TokenStream, built: &Built, frontend: bool) -> TokenStream {
    let serves = matches!(segment.serve, Serve::JsonLines);
    let ty = built.ty();
    let (unpack, nodes) = if built.extra() {
        let fields = &built.fields;
        (
            Some(quote!(let Hosts { #(#fields),* } = hosts;)),
            quote!(nodes),
        )
    } else {
        (None, quote!(hosts))
    };
    let segment_var = if frontend || serves {
        format_ident!("segment")
    } else {
        format_ident!("_segment")
    };
    let body = serve_body(segment, krate, serves);
    quote! {
        fn start(
            hosts: #ty,
            node: #krate::velo::Velo,
            roster: &#krate::Roster,
            runtime: #krate::RuntimeConfig,
        ) -> #krate::gdl::Result<#krate::Serving> {
            #unpack
            let segment = crate::start(node, roster, runtime, #nodes)?;
            Ok(#krate::Serving::new(segment, |#segment_var, shutdown| async move {
                #body
            }))
        }
    }
}

/// What the body does once the segments it waits for are ready: serve the
/// frontend or the service, answer JSON lines, or wait to be stopped.
fn serve_body(segment: &Segment, krate: &TokenStream, serves: bool) -> TokenStream {
    match &segment.serve {
        Serve::Service(service) => service_body(segment, krate, service),
        Serve::Frontend { .. } => quote! {
            #krate::gdl::Frontend::serve(
                frontend,
                segment.client(|root| root.clone()),
                async move { shutdown.cancelled().await },
            )
            .await
        },
        Serve::JsonLines if serves => quote! {
            let input = tokio::io::BufReader::new(tokio::io::stdin());
            let call = |request: contract::SourceReq| {
                segment.call::<_, contract::SourceItem>(#krate::gdl::Cx::new(), request)
            };
            #krate::serve_json_lines(
                input,
                tokio::io::stdout(),
                shutdown.cancelled(),
                #krate::json_codec!(contract::SourceReq, contract::SourceItem),
                call,
            )
            .await
            .map_err(|e| {
                #krate::gdl::Error::new(#krate::gdl::ErrorKind::Transport, e.to_string())
            })
        },
        Serve::JsonLines | Serve::None => quote! {
            shutdown.cancelled().await;
            Ok(())
        },
    }
}

/// The service's clients, one per entry, over the roots its entries reach.
fn service_body(segment: &Segment, krate: &TokenStream, service: &Service) -> TokenStream {
    let count = segment.roots.len();
    let pipelines = service.pipelines.iter().map(|(variable, index)| {
        let selector = if count == 1 {
            quote!(roots)
        } else {
            // `roots.1.1.0` for the third root.
            let mut selector = quote!(roots);
            for _ in 0..*index {
                selector = quote!(#selector.1);
            }
            let zero = syn::Index {
                index: 0,
                span: Span::call_site(),
            };
            quote!(#selector.#zero)
        };
        quote!(let #variable = segment.client(|roots| #selector.clone());)
    });
    let clients = &service.clients;
    let entries = service.entries.iter().map(|(field, entry)| match entry {
        Some(expression) => quote! {
            #field: Some(#krate::gdl::Client::new(#krate::gdl::Source::new(#expression))),
        },
        None => quote!(#field: None,),
    });
    quote! {
        #(#pipelines)*
        let clients = #clients { #(#entries)* };
        #krate::gdl::Service::serve(frontend, clients, async move { shutdown.cancelled().await }).await
    }
}

#[cfg(test)]
mod tests;
