//! Streamed requests nested in a head or an item: no declaration in graph
//! syntax describes them.

use gdl::{Cx, Error, Items, Result, Sink, Streamed};
use node_protocols::{Piece, Token, Words};

/// A stream nested in the head.
#[gdl::node]
pub struct Engine;

impl Sink for Engine {
    type Req = Streamed<Streamed<Words, Piece>, Token>;
    type Item = Token;

    async fn generate(&self, _cx: Cx, _req: Self::Req) -> Result<Items<Token>> {
        Err(Error::node("a fixture"))
    }
}

/// A stream nested in the item.
#[gdl::node]
pub struct Other;

impl Sink for Other {
    type Req = Streamed<Words, Streamed<Token, Piece>>;
    type Item = Token;

    async fn generate(&self, _cx: Cx, _req: Self::Req) -> Result<Items<Token>> {
        Err(Error::node("a fixture"))
    }
}
