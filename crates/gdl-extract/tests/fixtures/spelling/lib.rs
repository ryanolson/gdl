// The crate `tests/spelling.rs` both compiles and reads: its instantiations,
// as the derive records them and as the reader writes them. It is included
// into that test, so it holds no inner attributes and no file modules.

use std::collections::{BTreeMap, HashMap, VecDeque};

/// A carrier in a module: its definition path names the module.
pub mod carrier {
    /// The carrier.
    #[derive(gdl_message::Message)]
    pub struct Carried<P>(pub P);
}

/// Two arguments, for the separator between them.
#[derive(gdl_message::Message)]
pub struct Pair<A, B>(pub A, pub B);

/// A plain type, defined in a private module and offered at the root: a
/// build and the reader both name it by where it is defined.
mod words {
    /// The type.
    pub struct Words;
}
pub use words::Words;

use carrier::Carried;

/// An argument spelled through an alias.
pub type Bytes = Vec<u8>;

/// `()`.
#[gdl_message::export]
pub type OfUnit = Carried<()>;

/// A primitive.
#[gdl_message::export]
pub type OfPrimitive = Carried<u64>;

/// `String`, by the path the standard library defines it at.
#[gdl_message::export]
pub type OfString = Carried<String>;

/// A type of the crate's own, by its definition path, not its re-export.
#[gdl_message::export]
pub type OfWords = Carried<Words>;

/// An argument spelled through an alias: written as the type it names.
#[gdl_message::export]
pub type OfAlias = Carried<Bytes>;

/// Standard types nested in each other.
#[gdl_message::export]
pub type OfStd = Carried<Option<Vec<Box<u8>>>>;

/// `()` nested.
#[gdl_message::export]
pub type OfUnitInside = Carried<Option<()>>;

/// `VecDeque`, whose path is a module deeper than `Vec`'s.
#[gdl_message::export]
pub type OfQueue = Carried<VecDeque<Words>>;

/// `HashMap`, without the hasher it defaults.
#[gdl_message::export]
pub type OfHashMap = Carried<HashMap<String, u8>>;

/// `BTreeMap`, with an argument that has arguments.
#[gdl_message::export]
pub type OfBTreeMap = Carried<BTreeMap<String, Vec<u8>>>;

/// A generic message nested in itself.
#[gdl_message::export]
pub type OfCarried = Carried<Carried<Words>>;

/// Two arguments.
#[gdl_message::export]
pub type OfPair = Pair<Words, ()>;

/// Generic messages nested as arguments of each other.
#[gdl_message::export]
pub type OfPairs = Pair<Carried<u8>, Pair<(), String>>;

/// A reference, which the reader does not name: it leaves the alias out.
#[gdl_message::export]
pub type OfReference = Carried<&'static str>;
