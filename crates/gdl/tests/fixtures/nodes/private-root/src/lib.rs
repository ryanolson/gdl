//! A node type at the crate root that other crates cannot name.

use gdl::{Cx, Error, Items, Result, Sink};
use node_protocols::{Token, Words};

/// The engine, private.
#[gdl::node]
struct Engine;

impl Sink for Engine {
    type Req = Words;
    type Item = Token;

    async fn generate(&self, _cx: Cx, _req: Words) -> Result<Items<Token>> {
        Err(Error::node("a fixture"))
    }
}
