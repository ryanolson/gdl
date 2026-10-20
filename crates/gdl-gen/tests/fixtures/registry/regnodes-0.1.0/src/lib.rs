//! Nodes from a registry.

use regproto::{Token, Words};

/// An engine.
#[derive(gdl::Node)]
pub struct Engine;

impl gdl::Sink for Engine {
    type Req = Words;
    type Item = Token;
}

/// A sink of a local message.
#[derive(gdl::Node)]
pub struct Quiet;

impl gdl::Sink for Quiet {
    type Req = regproto::Local;
    type Item = Token;
}
