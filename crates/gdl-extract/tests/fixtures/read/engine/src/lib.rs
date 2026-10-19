//! Nodes, in modules, re-exported at the root.

use read_protocols::{Words, engine::Token};

mod sink;
mod stages;

pub use sink::Engine;
pub use stages::*;

/// A node whose port is not a message.
pub struct Bad;

#[gdl::node]
impl gdl::Sink for Bad {
    type Req = String;
    type Item = Token;
}

/// A node type the crate root does not reach.
mod buried {
    pub struct Buried;

    #[gdl::node]
    impl gdl::Backward for Buried {
        type In = read_protocols::Token;
        type Out = read_protocols::Token;
    }
}

/// Not a node: no marker.
pub struct Unmarked;

impl gdl::Sink for Unmarked {
    type Req = Words;
    type Item = Token;
}
