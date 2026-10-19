//! Nodes from a registry.

use regproto::{Token, Words};

/// An engine.
#[derive(gdl::Node)]
pub struct Engine;

impl gdl::Sink for Engine {
    type Req = Words;
    type Item = Token;
}
