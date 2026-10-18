use serde::{Deserialize, Serialize};

/// Every message type linked into this program: each type that derives
/// [`Message`](crate::Message), in whatever crate.
///
/// The generator reads the catalogue of a protocols crate by building a
/// program that links it and prints [`Catalogue::linked`] as TOML; a graph
/// file's message names resolve against it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalogue {
    /// The messages, sorted.
    pub messages: Vec<MessageInfo>,
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
}

impl Catalogue {
    /// Every message type linked into this program, sorted, so the result
    /// does not depend on link order.
    pub fn linked() -> Self {
        let mut messages: Vec<MessageInfo> = registry::GDL_MESSAGE_CATALOGUE
            .iter()
            .map(registry::Entry::info)
            .collect();
        messages.sort();
        Self { messages }
    }

    /// The catalogue as TOML, the form the generator reads it in.
    pub fn to_toml(&self) -> String {
        // Strings and booleans in a list of tables, all of which TOML holds.
        toml::to_string(self).expect("a catalogue is representable in TOML")
    }

    /// Read a catalogue written by [`Catalogue::to_toml`].
    pub fn from_toml(text: &str) -> Result<Self, crate::Error> {
        toml::from_str(text)
            .map_err(|e| crate::Error::new(crate::ErrorKind::Placement, e.to_string()))
    }
}

/// What `#[derive(Message)]` registers. Not a stable interface.
#[doc(hidden)]
pub mod registry {
    use super::MessageInfo;

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
                path: format!("{}::{}", self.module, self.name),
                wire: (self.wire)(),
            }
        }
    }
}
