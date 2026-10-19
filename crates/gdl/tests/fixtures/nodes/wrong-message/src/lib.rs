//! A sink whose item is not the declared one.

use gdl::{Cx, Error, Items, Result, Sink};
use node_protocols::{Token, Words};

/// The engine.
#[gdl::node]
pub struct Engine;

impl Sink for Engine {
    type Req = Words;
    type Item = Token;

    async fn generate(&self, _cx: Cx, _req: Words) -> Result<Items<Token>> {
        Err(Error::node("a fixture"))
    }
}
