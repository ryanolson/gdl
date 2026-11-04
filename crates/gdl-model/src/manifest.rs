use std::fmt;

use serde::{Deserialize, Serialize};

use crate::hash::Fnv;
use crate::{Encoding, Role};

mod properties;
mod unary;

/// The shape of a graph as data: its nodes, hops, and typed edges.
///
/// `graph!` emits one per contract crate; the generator reads it to validate
/// a placement before any segment crate is compiled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The graph's name.
    pub name: String,
    /// The schema version. Bump it whenever any message type's serde representation
    /// changes: type names alone cannot fingerprint a serde representation.
    pub schema: u32,
    /// Every node, in declaration order.
    pub nodes: Vec<NodeInfo>,
    /// Each root followed by its nested hops, in topology order.
    pub hops: Vec<HopInfo>,
    /// The property kinds the graph declares (`property <kind>: <Type>;`),
    /// in declaration order, with the nodes that provide and need each.
    /// Left out when there are none, so a manifest without them reads and
    /// writes as it did before properties were declared.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub properties: Vec<PropertyInfo>,
}

/// One property kind a graph declares: its name, the message type of its
/// keys, and the nodes that provide it and that need it. A property is
/// not on any edge: the nodes of a pool's entry segments provide it, and
/// a reader of the class's pools needs it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyInfo {
    /// The kind's name, a settings key's shape
    /// ([`check_property_kind`](crate::check_property_kind)).
    pub kind: String,
    /// The type of the kind's keys: a wire type, as the type of an edge
    /// that may be cut is. Never streamed.
    #[serde(rename = "type")]
    pub ty: TypeInfo,
    /// The nodes that say their instance has properties of this kind
    /// (`provides <kind>`), in declaration order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<String>,
    /// The nodes that read this kind from a class's pools
    /// (`needs <kind> from <class>`), in declaration order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<PropertyNeed>,
}

/// A node that reads a property kind from the pools of a class.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PropertyNeed {
    /// The node.
    pub node: String,
    /// The class whose pools it reads the kind from.
    pub class: String,
}

/// One node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// The node's name.
    pub name: String,
    /// The node's role.
    pub role: Role,
    /// The node's slot in the contract's `Bindings`: the name of its setter
    /// and field, as `graph!` derives it from the node's name. `None` for the
    /// source and built-in identity nodes, which take no binding. Generated code reads it here rather
    /// than repeating the macro's naming rule.
    #[serde(default)]
    pub binding: Option<String>,
    /// The unary nodes it calls (`calls A, B`), in the order the graph
    /// writes them. Left out when there are none, so a manifest without
    /// calls reads and writes as it did before unary nodes existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub calls: Vec<String>,
    /// A unary node's request and response, and whether it is idempotent:
    /// `Some` exactly when the role is [`Role::Unary`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unary: Option<UnaryInfo>,
}

/// What a unary node takes and returns: `unary Tok: Text -> Tokens
/// idempotent;`. Neither type is streamed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnaryInfo {
    /// The request a caller sends.
    pub req: TypeInfo,
    /// The response the caller receives.
    pub resp: TypeInfo,
    /// Whether the graph marks it `idempotent`: the service's author
    /// promises that it is safe to run twice, also at the same time, so a
    /// remote call whose outcome is unknown may be sent again.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub idempotent: bool,
}

impl NodeInfo {
    /// Whether the runtime supplies this node's stateless identity implementation.
    pub fn is_identity(&self) -> bool {
        self.binding.is_none()
            && matches!(self.role, Role::Forward | Role::Backward | Role::Operator)
    }
}

/// One hop: outer end, forward chain, inner end, backward chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HopInfo {
    /// The source or operator that owns the hop.
    pub outer: String,
    /// Forward nodes in request order.
    pub forward: Vec<String>,
    /// The operator or sink at the inner end.
    pub inner: String,
    /// Backward nodes in response order.
    pub backward: Vec<String>,
    /// The type on each forward edge, from the outer end to the inner end:
    /// `forward.len() + 1` entries.
    pub forward_types: Vec<TypeInfo>,
    /// The type on each backward edge, from the inner end to the outer end:
    /// `backward.len() + 1` entries.
    pub backward_types: Vec<TypeInfo>,
}

/// A message type as the graph definition named it. For a streamed
/// request (`Head + stream Item`), `name` and `wire` describe the head and
/// `stream` the items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeInfo {
    /// The type as written in the graph definition.
    pub name: String,
    /// Whether the type has a codec, so an edge carrying it may be cut. For
    /// a streamed request the item type must have one as well.
    pub wire: bool,
    /// The encoding declared by the message, or named MessagePack by default.
    pub encoding: Encoding,
    /// The item type of a streamed request. `None` for any other type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<ItemInfo>,
}

/// The item type of a streamed request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemInfo {
    /// The type as written in the graph definition.
    pub name: String,
    /// Whether the type has a codec.
    pub wire: bool,
    /// The encoding of each input stream item.
    pub encoding: Encoding,
}

impl TypeInfo {
    /// A type entry.
    pub fn new(name: impl Into<String>, wire: bool, encoding: Encoding) -> Self {
        Self {
            name: name.into(),
            wire,
            encoding,
            stream: None,
        }
    }

    /// A streamed request's entry: its head type and its item type.
    pub fn streamed(
        head: impl Into<String>,
        head_wire: bool,
        head_encoding: Encoding,
        item: impl Into<String>,
        item_wire: bool,
        item_encoding: Encoding,
    ) -> Self {
        Self {
            name: head.into(),
            wire: head_wire,
            encoding: head_encoding,
            stream: Some(ItemInfo {
                name: item.into(),
                wire: item_wire,
                encoding: item_encoding,
            }),
        }
    }
}

impl fmt::Display for TypeInfo {
    /// The type as the graph definition writes it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.stream {
            None => f.write_str(&self.name),
            Some(item) => write!(f, "{} + stream {}", self.name, item.name),
        }
    }
}

impl HopInfo {
    /// The nodes the request visits: outer end, forward chain, inner end.
    pub fn forward_path(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.outer.as_str())
            .chain(self.forward.iter().map(String::as_str))
            .chain(std::iter::once(self.inner.as_str()))
    }

    /// The nodes the response visits: inner end, backward chain, outer end.
    pub fn backward_path(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.inner.as_str())
            .chain(self.backward.iter().map(String::as_str))
            .chain(std::iter::once(self.outer.as_str()))
    }
}

impl Manifest {
    /// The contract hash: changes when the name, schema, nodes, roles,
    /// topology, or edge types and encodings (including which requests are streamed,
    /// and their item types) change, and when a declared property kind, its
    /// type and encoding, or a node that provides or needs it changes. It
    /// changes too when a unary node, its request, response or
    /// `idempotent`, or a node's `calls` change; the order they are written
    /// in does not matter.
    /// Deployments check it so that segments built from different
    /// contracts never exchange traffic, and pools whose graphs declare
    /// different properties never follow or offer to each other.
    pub fn contract_hash(&self) -> u64 {
        let mut h = Fnv::new();
        h.str(&self.name);
        h.u64(u64::from(self.schema));
        // Unary nodes are hashed apart, in name order: they are not in the
        // topology, so where the graph declares them means nothing.
        for node in self.nodes.iter().filter(|node| node.role != Role::Unary) {
            h.str(&node.name);
            h.str(node.role.name());
        }
        for hop in &self.hops {
            h.str(&hop.outer);
            h.strs(&hop.forward);
            h.str(&hop.inner);
            h.strs(&hop.backward);
            for ty in hop.forward_types.iter().chain(&hop.backward_types) {
                hash_type(&mut h, ty);
            }
        }
        properties::hash(&mut h, &self.properties);
        unary::hash(&mut h, &self.nodes);
        h.finish()
    }

    /// The declared property kind named `kind`.
    pub fn property(&self, kind: &str) -> Option<&PropertyInfo> {
        self.properties
            .iter()
            .find(|property| property.kind == kind)
    }

    /// The node with this name.
    pub fn node(&self, name: &str) -> Option<&NodeInfo> {
        self.nodes.iter().find(|node| node.name == name)
    }

    /// The manifest as TOML, the form the generator reads it in.
    pub fn to_toml(&self) -> String {
        // Every field is a string, a number, a unit enum, or a list of
        // these, all of which TOML holds.
        toml::to_string(self).expect("a manifest is representable in TOML")
    }

    /// Read a manifest written by [`Manifest::to_toml`].
    pub fn from_toml(text: &str) -> Result<Self, crate::Error> {
        toml::from_str(text).map_err(|e| crate::Error::new(e.to_string()))
    }
}

/// A message type's part of the contract hash: its name, its encoding and
/// the encoding's wire revision, and a streamed request's item type.
fn hash_type(h: &mut Fnv, ty: &TypeInfo) {
    h.str(&ty.name);
    h.str(ty.encoding.name());
    h.u64(u64::from(ty.encoding.version()));
    match &ty.stream {
        None => h.u64(0),
        Some(item) => {
            h.u64(1);
            h.str(&item.name);
            h.str(item.encoding.name());
            h.u64(u64::from(item.encoding.version()));
        }
    }
}
