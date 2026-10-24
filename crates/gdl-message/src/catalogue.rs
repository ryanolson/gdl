use serde::{Deserialize, Serialize};

/// Everything the markers register in this program, in whatever crate:
/// each type that derives [`Message`](crate::Message), each export, and
/// each node type.
///
/// `cargo gdl extract --verify` builds a program that links a crate, prints
/// [`Catalogue::linked`] as TOML, and compares it with what the source
/// reader found.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalogue {
    /// The messages, sorted.
    #[serde(default)]
    pub messages: Vec<MessageInfo>,
    /// The exports, sorted.
    #[serde(default)]
    pub exports: Vec<ExportInfo>,
    /// The node types, sorted.
    #[serde(default)]
    pub nodes: Vec<NodeInfo>,
}

/// One message type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MessageInfo {
    /// The package that defines it.
    pub package: String,
    /// That package's version.
    pub version: String,
    /// That package's directory when it was built. One program can link two
    /// packages of one name, even of one version from two sources; only a
    /// listed one offers messages, and its directory tells it apart.
    pub dir: String,
    /// The type's name: what a graph file calls it.
    pub name: String,
    /// Where it is defined: its module path and its name. A re-export is
    /// another path to the same type, never another entry.
    pub path: String,
    /// Whether the type has a codec, so an edge carrying it may cross.
    pub wire: bool,
    /// The encoding declared on the message type.
    pub encoding: gdl_model::Encoding,
}

/// A message's identity, as a catalogue holds it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Origin {
    /// The defining package.
    pub package: String,
    /// Its version.
    pub version: String,
    /// Its directory when it was built.
    pub dir: String,
    /// The definition path.
    pub path: String,
}

impl From<crate::Identity> for Origin {
    fn from(id: crate::Identity) -> Self {
        Self {
            package: id.package.to_string(),
            version: id.version.to_string(),
            dir: id.dir.to_string(),
            path: plain(id.path),
        }
    }
}

/// `path` with each segment's `r#` dropped. `module_path!` keeps the
/// spelling of a raw module (`r#type`); a catalogue holds names, as the
/// source reader and graph files write them, and code that writes a path
/// escapes each keyword again.
fn plain(path: &str) -> String {
    path.split("::")
        .map(|segment| segment.strip_prefix("r#").unwrap_or(segment))
        .collect::<Vec<_>>()
        .join("::")
}

/// A message offered under another public path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExportInfo {
    /// The package that exports it.
    pub package: String,
    /// That package's version.
    pub version: String,
    /// That package's directory when it was built.
    pub dir: String,
    /// The name it is offered under.
    pub name: String,
    /// The public path.
    pub path: String,
    /// The message it is.
    pub origin: Origin,
}

/// A type that derives `Node`, with each role trait it implements.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Graph-affecting configuration fields.
    #[serde(default)]
    pub config: Vec<gdl_model::ConfigField>,
    /// The package that defines it.
    pub package: String,
    /// That package's version.
    pub version: String,
    /// That package's directory when it was built.
    pub dir: String,
    /// The node's name: the type's name.
    pub name: String,
    /// The type's path at its crate's root.
    pub path: String,
    /// Each role trait it implements, with the messages at its ports. A
    /// node has exactly one; the reader and `--verify` report any other
    /// count.
    pub roles: Vec<RoleInfo>,
    /// The metrics its build registers; left out when it registers none.
    /// Last, so that nodes sort by name before what they report.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<gdl_model::MetricField>,
}

/// One role a node type implements.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RoleInfo {
    /// Named public entries of a hosted service.
    #[serde(default)]
    pub entries: Vec<EndpointInfo>,
    /// `sink`, `forward`, `backward`, or `operator`.
    pub role: String,
    /// Its ports, in the order the role trait declares them.
    pub ports: Vec<PortInfo>,
}

/// A typed endpoint owned by one hosted frontend.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EndpointInfo {
    /// Client field name.
    pub name: String,
    /// Public protocol endpoint.
    pub path: String,
    /// Graph configuration boolean that enables it.
    pub enabled: String,
    /// Request type.
    pub request: PortInfo,
    /// Response item type.
    pub response: PortInfo,
}

/// One port: a role trait's associated type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PortInfo {
    /// The associated type's name: `Req`, `Item`, `In`, `Out`, and so on.
    pub port: String,
    /// Whether it is a streamed request.
    pub streamed: bool,
    /// Whether a streamed head or item is itself streamed.
    #[serde(default)]
    pub nested: bool,
    /// The message, or the head; `None` for `()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<Origin>,
    /// A streamed request's item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<Origin>,
}

impl Catalogue {
    /// Every registration linked into this program, sorted, so the result
    /// does not depend on link order.
    pub fn linked() -> Self {
        let mut messages: Vec<MessageInfo> = registry::GDL_MESSAGE_CATALOGUE
            .iter()
            .map(registry::Entry::info)
            .collect();
        messages.sort();
        let mut exports: Vec<ExportInfo> = registry::GDL_EXPORT_CATALOGUE
            .iter()
            .map(registry::Export::info)
            .collect();
        exports.sort();
        let mut nodes: Vec<NodeInfo> = registry::GDL_NODE_CATALOGUE
            .iter()
            .map(registry::Node::info)
            .collect();
        nodes.sort();
        Self {
            messages,
            exports,
            nodes,
        }
    }

    /// The catalogue as TOML, the form the generator reads it in.
    pub fn to_toml(&self) -> String {
        // Strings and booleans in a list of tables, all of which TOML holds.
        toml::to_string(self).expect("a catalogue is representable in TOML")
    }

    /// Read a catalogue written by [`Catalogue::to_toml`].
    pub fn from_toml(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}

/// What the markers register. Not a stable interface.
#[doc(hidden)]
pub mod registry {
    use super::{EndpointInfo, ExportInfo, MessageInfo, NodeInfo, PortInfo, RoleInfo};
    use crate::{Identity, Shape};

    /// One entry per type that derives `Message`, collected at link time:
    /// a derive cannot add to a list any other way, and an explicit list in
    /// each protocols crate is one more place to forget a type.
    ///
    /// linkme names the linker section after this static alone, not its
    /// path, and two slices of one name in a program panic on first use.
    /// The name is gdl's own, so another crate's registry cannot take it.
    /// Two gdl versions in one program still share it: reading the slice
    /// then panics, and the generator reports that as the conflict.
    #[linkme::distributed_slice]
    pub static GDL_MESSAGE_CATALOGUE: [Entry];

    pub struct Entry {
        pub package: &'static str,
        pub version: &'static str,
        pub dir: &'static str,
        pub name: &'static str,
        pub module: &'static str,
        pub encoding: gdl_model::Encoding,
        /// Expanded where the type is concrete, as the wire probe must be.
        pub wire: fn() -> bool,
    }

    impl Entry {
        pub(super) fn info(&self) -> MessageInfo {
            MessageInfo {
                package: self.package.to_string(),
                version: self.version.to_string(),
                dir: self.dir.to_string(),
                name: self.name.to_string(),
                path: super::plain(&format!("{}::{}", self.module, self.name)),
                wire: (self.wire)(),
                encoding: self.encoding,
            }
        }
    }

    /// One entry per `pub use` name marked `#[export]`.
    #[linkme::distributed_slice]
    pub static GDL_EXPORT_CATALOGUE: [Export];

    pub struct Export {
        pub package: &'static str,
        pub version: &'static str,
        pub dir: &'static str,
        pub name: &'static str,
        pub module: &'static str,
        pub origin: Identity,
    }

    impl Export {
        pub(super) fn info(&self) -> ExportInfo {
            ExportInfo {
                package: self.package.to_string(),
                version: self.version.to_string(),
                dir: self.dir.to_string(),
                name: self.name.to_string(),
                path: super::plain(&format!("{}::{}", self.module, self.name)),
                origin: self.origin.into(),
            }
        }
    }

    /// One entry per type that derives `Node`.
    #[linkme::distributed_slice]
    pub static GDL_NODE_CATALOGUE: [Node];

    pub struct Node {
        pub config: fn() -> Vec<gdl_model::ConfigField>,
        pub metrics: fn() -> Vec<gdl_model::MetricField>,
        pub package: &'static str,
        pub version: &'static str,
        pub dir: &'static str,
        pub name: &'static str,
        pub krate: &'static str,
        /// Each role trait the type implements: probed where the type is
        /// concrete, as the wire probe is.
        pub roles: fn() -> Vec<Role>,
    }

    /// A role and its ports, as the derive's probe finds them.
    pub struct Role {
        pub entries: Vec<Endpoint>,
        pub role: &'static str,
        pub ports: Vec<(&'static str, Port)>,
    }

    pub struct Endpoint {
        pub name: &'static str,
        pub path: &'static str,
        pub enabled: &'static str,
        pub request: Port,
        pub response: Port,
    }

    impl Port {
        fn info(self, name: &str) -> PortInfo {
            PortInfo {
                port: name.to_string(),
                streamed: self.streamed,
                nested: self.nested,
                head: self.head.map(Into::into),
                item: self.item.map(Into::into),
            }
        }
    }

    /// A port's shape, read from its type's `Shape`.
    pub struct Port {
        pub streamed: bool,
        pub nested: bool,
        pub head: Option<Identity>,
        pub item: Option<Identity>,
    }

    /// The shape of port type `T`.
    pub fn port<T: Shape>() -> Port {
        Port {
            streamed: T::STREAMED,
            nested: T::NESTED,
            head: T::HEAD,
            item: T::ITEM,
        }
    }

    impl Node {
        pub(super) fn info(&self) -> NodeInfo {
            let mut roles: Vec<RoleInfo> = (self.roles)()
                .into_iter()
                .map(|role| RoleInfo {
                    entries: role
                        .entries
                        .into_iter()
                        .map(|e| EndpointInfo {
                            name: e.name.into(),
                            path: e.path.into(),
                            enabled: e.enabled.into(),
                            request: e.request.info("Req"),
                            response: e.response.info("Item"),
                        })
                        .collect(),
                    role: role.role.to_string(),
                    ports: role
                        .ports
                        .into_iter()
                        .map(|(port, shape)| PortInfo {
                            port: port.to_string(),
                            streamed: shape.streamed,
                            nested: shape.nested,
                            head: shape.head.map(Into::into),
                            item: shape.item.map(Into::into),
                        })
                        .collect(),
                })
                .collect();
            roles.sort();
            NodeInfo {
                config: (self.config)(),
                metrics: (self.metrics)(),
                package: self.package.to_string(),
                version: self.version.to_string(),
                dir: self.dir.to_string(),
                name: self.name.to_string(),
                path: format!("{}::{}", self.krate, self.name),
                roles,
            }
        }
    }
}
