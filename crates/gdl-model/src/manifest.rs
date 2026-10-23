use std::fmt;

use serde::{Deserialize, Serialize};

use crate::hash::Fnv;
use crate::{Encoding, Role};

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
    /// and their item types) change. Deployments check it so that segments
    /// built from different contracts never exchange traffic.
    pub fn contract_hash(&self) -> u64 {
        let mut h = Fnv::new();
        h.str(&self.name);
        h.u64(u64::from(self.schema));
        for node in &self.nodes {
            h.str(&node.name);
            h.str(node.role.name());
        }
        for hop in &self.hops {
            h.str(&hop.outer);
            h.strs(&hop.forward);
            h.str(&hop.inner);
            h.strs(&hop.backward);
            for ty in hop.forward_types.iter().chain(&hop.backward_types) {
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
        }
        h.finish()
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
