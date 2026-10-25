//! A response the reader takes for a wire type, which serializes but does
//! not deserialize: serde's `Serialize`, and another crate's `Deserialize`
//! under serde's name, a trick the reader does not chase, since the segment's crossing check catches it.
//! With the sink beside the source and a forward node elsewhere, it
//! returns through an anchor, which needs both.

use gdl::{Build, Cx, Error, Forward, Items, Result, Sink};

/// A request: a wire type.
#[derive(serde_derive::Serialize, serde_derive::Deserialize, gdl::Message)]
#[serde(crate = "real_serde")]
pub struct Request(pub u8);

/// A response that serializes, and does not deserialize.
#[derive(serde_derive::Serialize, gdl::Message)]
#[serde(crate = "real_serde")]
pub struct Reply<T> {
    /// What it holds.
    pub value: T,
}

impl<'de, T> serde::Deserialize<'de> for Reply<T> {}

/// The instantiation the graph carries.
#[gdl::export]
pub type ByteReply = Reply<u8>;

/// Passes the request on.
#[derive(gdl::Node)]
pub struct Relay;

impl Build for Relay {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Forward for Relay {
    type In = Request;
    type Out = Request;

    async fn forward(&self, _cx: &Cx, req: Request) -> Result<Request> {
        Ok(req)
    }
}

/// Answers it.
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
    type Req = Request;
    type Item = ByteReply;

    async fn generate(&self, _cx: Cx, _req: Request) -> Result<Items<ByteReply>> {
        Err(Error::node("a fixture"))
    }
}

/// A response that deserializes, and does not serialize: serde's
/// `Deserialize`, and the other crate's `Serialize` under serde's name.
#[derive(serde_derive::Deserialize, gdl::Message)]
#[serde(crate = "real_serde")]
pub struct Mute<T> {
    /// What it holds.
    pub value: T,
}

impl<T> serde::Serialize for Mute<T> {}

/// The instantiation the source's binary writes.
#[gdl::export]
pub type ByteMute = Mute<u8>;

/// Answers with it.
#[derive(gdl::Node)]
pub struct Hush;

impl Build for Hush {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Sink for Hush {
    type Req = Request;
    type Item = ByteMute;

    async fn generate(&self, _cx: Cx, _req: Request) -> Result<Items<ByteMute>> {
        Err(Error::node("a fixture"))
    }
}
