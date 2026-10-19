use gdl::{Sink, node};

use super::{Token, Words};

/// The engine.
pub struct Engine;

#[node]
impl Sink for Engine {
    type Req = Words;
    type Item = Token;
}
