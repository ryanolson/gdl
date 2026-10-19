//! Nodes that match their declarations, one in a module re-exported at the
//! crate root.

use gdl::{Backward, Cx, Error, Forward, Items, Next, Operator, Result, Sink, Streamed, items};
use node_protocols::{Pcm, Piece, Session, Token, Words};

pub use engine::Engine;

mod engine {
    use super::*;

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
}

/// The resampler: a stream with a head in, a stream without one out.
#[gdl::node]
pub struct Resample;

impl Forward for Resample {
    type In = Streamed<Session, Pcm>;
    type Out = Streamed<(), Pcm>;

    async fn forward(&self, _cx: &Cx, _req: Self::In) -> Result<Self::Out> {
        Err(Error::node("a fixture"))
    }
}

/// The detokenizer.
#[gdl::node]
pub struct Detokenize;

impl Backward for Detokenize {
    type In = Token;
    type Out = Piece;

    fn backward(&self, _cx: &Cx, _input: Items<Token>) -> Items<Piece> {
        items(futures::stream::empty())
    }
}

/// The gate.
#[gdl::node]
pub struct Gate;

impl Operator for Gate {
    type UpReq = Words;
    type UpItem = Piece;
    type DownReq = Words;
    type DownItem = Token;

    async fn generate<N>(&self, _cx: Cx, _req: Words, _next: N) -> Result<Items<Piece>>
    where
        N: Next<Words, Token>,
    {
        Err(Error::node("a fixture"))
    }
}
