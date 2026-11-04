//! A graph file's manifest, built without compiling its contract: what the
//! contract's `graph!` would emit, given where each message name resolved
//! and whether it is a wire type.

use gdl_model::{
    Encoding, HopInfo, ItemInfo, Manifest, NodeInfo, PropertyInfo, PropertyNeed, TypeInfo,
    UnaryInfo,
};
use syn::Type;

use crate::model::{Graph, identifier, message_name};
use crate::parse::{Request, Sig};

/// A message's path (`crate::module::Name`, without `r#`) as a graph
/// file's manifest spells it: `:: crate :: module :: Name`.
pub fn spelled(path: &str) -> String {
    let parts: Vec<String> = path.split("::").map(identifier).collect();
    format!(":: {}", parts.join(" :: "))
}

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
                    info.name = spelled(&info.name);
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
                    pinned: item.pinned,
                });
            }
            Ok(head)
        };
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let unary = match &node.sig {
                Sig::Unary { req, resp } => Some(UnaryInfo {
                    req: one(req)?,
                    resp: one(resp)?,
                    idempotent: node.idempotent,
                }),
                _ => None,
            };
            nodes.push(NodeInfo {
                name: node.ident.to_string(),
                role: node.role(),
                binding: node.binding.as_ref().map(ToString::to_string),
                calls: node.calls.iter().map(ToString::to_string).collect(),
                unary,
            });
        }
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
        let mut properties = Vec::with_capacity(self.properties.len());
        for property in &self.properties {
            let kind = property.kind.to_string();
            let name = match message_name(&property.ty) {
                Ok(Some(name)) => name,
                Ok(None) => {
                    return Err(format!(
                        "property `{kind}`: its type is a message, not `()`"
                    ));
                }
                Err(()) => {
                    return Err(format!(
                        "property `{kind}`: a graph file names a message as its type"
                    ));
                }
            };
            let ty = one(&property.ty)?;
            if !ty.wire {
                return Err(format!(
                    "property `{kind}`: `{name}` is not a wire type; a property's keys are carried encoded, so its type implements `Serialize` and `Deserialize`"
                ));
            }
            properties.push(self.property(&property.kind, ty));
        }
        Ok(Manifest {
            name: self.name.value(),
            schema: self
                .schema
                .base10_parse()
                .map_err(|e| format!("the schema: {e}"))?,
            nodes,
            hops,
            properties,
        })
    }

    /// The declaration of `kind`, whose type is `ty`, with the nodes that
    /// provide and need it, in declaration order: what both a graph file's
    /// manifest and `graph!`'s carry.
    pub fn property(&self, kind: &syn::Ident, ty: TypeInfo) -> PropertyInfo {
        PropertyInfo {
            kind: kind.to_string(),
            ty,
            providers: self
                .nodes
                .iter()
                .filter(|node| node.provides.contains(kind))
                .map(|node| node.ident.to_string())
                .collect(),
            needs: self
                .nodes
                .iter()
                .flat_map(|node| {
                    node.needs
                        .iter()
                        .filter(|(needed, _)| needed == kind)
                        .map(|(_, class)| PropertyNeed {
                            node: node.ident.to_string(),
                            class: class.to_string(),
                        })
                })
                .collect(),
        }
    }
}
