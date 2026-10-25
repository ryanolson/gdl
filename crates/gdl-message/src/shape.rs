/// A message's identity: where it is defined, wherever it is offered. An
/// instantiation of a generic message is identified by its generic type's
/// package and by its type, each argument written by its definition path:
/// `protocols::Carried<protocols::Prompt, builders::Plain>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identity {
    /// The defining package.
    pub package: &'static str,
    /// Its version.
    pub version: &'static str,
    /// Its directory when it was built: with the name and version, this
    /// tells apart two packages of one name, even from two sources. For an
    /// instantiation these are the generic type's package's: `path` writes
    /// each argument by its path alone, so two packages that define one
    /// argument's path are not told apart here. The source reader's
    /// identity holds them.
    pub dir: &'static str,
    /// The definition path: the module path and the type's name, and an
    /// instantiation's arguments.
    pub path: &'static str,
}

/// A message type: what `derive(Message)` implements.
pub trait Message: Send + 'static {
    /// The name a graph uses: the type's name.
    const NAME: &'static str;
    /// The type's identity.
    const IDENTITY: Identity;
    /// The encoding of this complete value when an edge crosses segments.
    const ENCODING: gdl_model::Encoding;
}

/// The messages at a port: one message, `()`, or a streamed request's head
/// and item. A node's registration reads its ports through it.
///
/// The messages are functions, not constants: an instantiation of a
/// generic message names its type with `core::any::type_name`, which is
/// not available to a constant.
pub trait Shape {
    /// Whether the port is a streamed request.
    const STREAMED: bool;
    /// Whether a streamed request's head or item is itself streamed, which
    /// no graph can declare.
    const NESTED: bool;
    /// The message, or a streamed request's head; `None` for `()`.
    fn head() -> Option<Identity>;
    /// A streamed request's item; `None` for any other port, and for an
    /// item of `()`.
    fn item() -> Option<Identity>;
}

/// The head of a pure stream.
impl Shape for () {
    const STREAMED: bool = false;
    const NESTED: bool = false;
    fn head() -> Option<Identity> {
        None
    }
    fn item() -> Option<Identity> {
        None
    }
}
