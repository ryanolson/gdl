//! Nodes, in modules, re-exported at the root; their role impls, in the
//! same module or another.

use read_protocols::{Words, engine::Token};

mod impls;
mod sink;
mod stages;

pub use sink::Engine;
pub use stages::*;

/// A node whose port is not a message.
#[derive(gdl::Node)]
pub struct Bad;

impl gdl::Sink for Bad {
    type Req = String;
    type Item = Token;
}

/// A node type the crate root does not reach.
mod buried {
    #[derive(gdl::Node)]
    pub struct Buried;

    impl gdl::Backward for Buried {
        type In = read_protocols::Token;
        type Out = read_protocols::Token;
    }
}

/// A node with two roles.
#[derive(gdl::Node)]
pub struct Twice;

impl gdl::Sink for Twice {
    type Req = Words;
    type Item = Token;
}

impl gdl::Backward for Twice {
    type In = Token;
    type Out = Token;
}

/// A node with no role.
#[derive(gdl::Node)]
pub struct Idle;

/// Not a node: not derived.
pub struct Unmarked;

impl gdl::Sink for Unmarked {
    type Req = Words;
    type Item = Token;
}
