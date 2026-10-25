//! Instantiations that put `()` where a port names a parameter: the port
//! has no message there, as a port that writes `()` has none.

use std::marker::PhantomData;

use gdl::{Build, Cx, Error, Forward, Items, Result, Sink, Streamed};
use serde::{Deserialize, Serialize};

/// A message.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Words {
    /// The text.
    pub text: String,
}

/// A forward whose ports are its parameter.
#[derive(gdl::Node)]
pub struct Pass<T>(PhantomData<fn() -> T>);

impl<T: Send + 'static> Build for Pass<T> {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

impl<T: Send + 'static> Forward for Pass<T> {
    type In = T;
    type Out = T;

    async fn forward(&self, _cx: &Cx, req: T) -> Result<T> {
        Ok(req)
    }
}

/// Its ports carry nothing.
#[gdl::export]
pub type UnitPass = Pass<()>;

/// Its ports carry words.
#[gdl::export]
pub type WordsPass = Pass<Words>;

/// A sink of a streamed request whose head is its parameter.
#[derive(gdl::Node)]
pub struct Feed<H>(PhantomData<fn() -> H>);

impl<H: Send + 'static> Build for Feed<H> {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

impl<H: Send + 'static> Sink for Feed<H> {
    type Req = Streamed<H, Words>;
    type Item = Words;

    async fn generate(&self, _cx: Cx, _req: Streamed<H, Words>) -> Result<Items<Words>> {
        Err(Error::node("a fixture"))
    }
}

/// A pure stream: no head.
#[gdl::export]
pub type HeadlessFeed = Feed<()>;

/// A stream with a head.
#[gdl::export]
pub type HeadedFeed = Feed<Words>;

/// A sink whose request is its parameter.
#[derive(gdl::Node)]
pub struct Fed<R>(PhantomData<fn() -> R>);

impl<R: Send + 'static> Build for Fed<R> {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

impl<R: Send + 'static> Sink for Fed<R> {
    type Req = R;
    type Item = Words;

    async fn generate(&self, _cx: Cx, _req: R) -> Result<Items<Words>> {
        Err(Error::node("a fixture"))
    }
}

/// Its request bound to a pure stream: a streamed port.
#[gdl::export]
pub type StreamFed = Fed<Streamed<(), Words>>;
