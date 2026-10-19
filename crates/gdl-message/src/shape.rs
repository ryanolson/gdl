/// A message type: what `derive(Message)` implements. Its constants are
/// the type's identity, as its catalogue entry records it.
pub trait Message: Send + 'static {
    /// The name a graph file uses: the type's name.
    const NAME: &'static str;
    /// The package that defines the type.
    const PACKAGE: &'static str;
    /// Where the type is defined: its module path and its name.
    const PATH: &'static str;
}

/// The message names at a port: a message, `()`, or a streamed request's
/// head and item. `#[gdl::node]` reads a port's names through it to check
/// an implementation against its declaration.
pub trait Shape {
    /// The value's name, or the head's for a streamed request: a message's
    /// name, or `()`.
    const HEAD: &'static str;
    /// The item's name for a streamed request; `None` for any other port.
    const ITEM: Option<&'static str>;
}

/// The head of a pure stream.
impl Shape for () {
    const HEAD: &'static str = "()";
    const ITEM: Option<&'static str> = None;
}
