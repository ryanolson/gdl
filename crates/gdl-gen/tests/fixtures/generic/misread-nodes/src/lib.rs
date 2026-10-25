//! A sink of the misread carrier.

use gdl::{Build, Cx, Error, Items, Result, Sink};
use generic_carrier::Reply;
use generic_misread::MisreadByte;

/// Takes the carrier.
#[derive(gdl::Node)]
pub struct Take;

impl Build for Take {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Sink for Take {
    type Req = MisreadByte;
    type Item = Reply;

    async fn generate(&self, _cx: Cx, _req: MisreadByte) -> Result<Items<Reply>> {
        Err(Error::node("a fixture"))
    }
}
