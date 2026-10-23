//! A graph file's manifest, built without compiling its contract: what the
//! contract's `graph!` would emit, given where each message name resolved
//! and whether it is a wire type.

use gdl_model::{Encoding, HopInfo, ItemInfo, Manifest, NodeInfo, TypeInfo};
use syn::Type;

use crate::model::{Graph, identifier, message_name};
use crate::parse::Request;

impl Graph {
    /// The manifest of a graph file's contract. `resolve` gives each message
    /// name's path (`crate::module::Name`, without `r#`), wire capability,
    /// and encoding; the contract names the message by that path.
    ///
    /// Each type is spelled as `graph!` spells it, the compiler's printing
    /// of the rooted path (`:: chat_protocols :: Words`), because the
    /// contract hash covers the spelling. Only a graph file's positions are
    /// built, a message name or `()`; any other type is an error, as it is
    /// in a graph file.
    pub fn manifest(&self, resolve: &dyn Fn(&str) -> Option<TypeInfo>) -> Result<Manifest, String> {
        let one = |ty: &Type| -> Result<TypeInfo, String> {
            match message_name(ty) {
                // `()` has serde impls, so it is a wire type.
                Ok(None) => Ok(TypeInfo::new("()", true, Encoding::default())),
                Ok(Some(name)) => {
                    let mut info = resolve(&name)
                        .ok_or_else(|| format!("message `{name}` is not resolved"))?;
                    let spelled: Vec<String> = info.name.split("::").map(identifier).collect();
                    info.name = format!(":: {}", spelled.join(" :: "));
                    Ok(info)
                }
                Err(()) => Err("a graph file names a message, or `()`, at each position".into()),
            }
        };
        let request = |req: &Request| -> Result<TypeInfo, String> {
            let mut head = one(&req.head)?;
            if let Some(item) = &req.item {
                let item = one(item)?;
                head.stream = Some(ItemInfo {
                    name: item.name,
                    wire: item.wire,
                    encoding: item.encoding,
                });
            }
            Ok(head)
        };
        let nodes = self
            .nodes
            .iter()
            .map(|node| NodeInfo {
                name: node.ident.to_string(),
                role: node.role(),
                binding: node.binding.as_ref().map(ToString::to_string),
            })
            .collect();
        let mut hops = Vec::with_capacity(self.hops.len());
        for hop in &self.hops {
            hops.push(HopInfo {
                outer: hop.outer.to_string(),
                forward: hop.forward.iter().map(ToString::to_string).collect(),
                inner: hop.inner.to_string(),
                backward: hop.backward.iter().map(ToString::to_string).collect(),
                forward_types: hop
                    .forward_types
                    .iter()
                    .map(request)
                    .collect::<Result<_, _>>()?,
                backward_types: hop
                    .backward_types
                    .iter()
                    .map(one)
                    .collect::<Result<_, _>>()?,
            });
        }
        Ok(Manifest {
            name: self.name.value(),
            schema: self
                .schema
                .base10_parse()
                .map_err(|e| format!("the schema: {e}"))?,
            nodes,
            hops,
        })
    }
}
