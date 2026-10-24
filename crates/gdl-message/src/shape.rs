/// A message's identity: where it is defined, wherever it is offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identity {
    /// The defining package.
    pub package: &'static str,
    /// Its version.
    pub version: &'static str,
    /// Its directory when it was built: with the name and version, this
    /// tells apart two packages of one name, even from two sources.
    pub dir: &'static str,
    /// The definition path: the module path and the type's name.
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
pub trait Shape {
    /// Whether the port is a streamed request.
    const STREAMED: bool;
    /// Whether a streamed request's head or item is itself streamed, which
    /// no graph can declare.
    const NESTED: bool;
    /// The message, or a streamed request's head; `None` for `()`.
    const HEAD: Option<Identity>;
    /// A streamed request's item; `None` for any other port, and for an
    /// item of `()`.
    const ITEM: Option<Identity>;
}

/// The head of a pure stream.
impl Shape for () {
    const STREAMED: bool = false;
    const NESTED: bool = false;
    const HEAD: Option<Identity> = None;
    const ITEM: Option<Identity> = None;
}
