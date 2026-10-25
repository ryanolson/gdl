//! Impls written for aliases: an impl for an alias is the impl for the
//! type the alias names, written out.

use std::marker::PhantomData;

use gdl::{Build, Cx, Error, Forward, Items, Result, Sink};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A message.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Words {
    /// The text.
    pub text: String,
}

/// A generic carrier.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Carried<B> {
    /// What it carries.
    pub words: Words,
    /// How it is built.
    pub builder: B,
}

/// A builder.
#[derive(Serialize, Deserialize)]
pub struct Plain;

/// Another.
#[derive(Serialize, Deserialize)]
pub struct Loud;

/// A sink with an impl for each instantiation.
#[derive(gdl::Node)]
pub struct Store<B>(PhantomData<fn() -> B>);

impl<B: Send + Sync + 'static> Build for Store<B> {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

/// Written through the alias of the instantiation.
impl Sink for PlainStore {
    type Req = Carried<Plain>;
    type Item = Words;

    async fn generate(&self, _cx: Cx, _req: Carried<Plain>) -> Result<Items<Words>> {
        Err(Error::node("a fixture"))
    }
}

/// Written out.
impl Sink for Store<Loud> {
    type Req = Carried<Loud>;
    type Item = Words;

    async fn generate(&self, _cx: Cx, _req: Carried<Loud>) -> Result<Items<Words>> {
        Err(Error::node("a fixture"))
    }
}

/// The instantiation whose impl names the alias.
#[gdl::export]
pub type PlainStore = Store<Plain>;

/// The one whose impl is written out.
#[gdl::export]
pub type LoudStore = Store<Loud>;

/// A forward with one impl for every instantiation.
#[derive(gdl::Node)]
pub struct Relay<B>(PhantomData<fn() -> B>);

/// A generic alias of it.
pub type Relayed<B> = Relay<B>;

impl<B: Send + Sync + 'static> Build for Relay<B> {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

/// Written through the generic alias.
impl<B: Send + Sync + 'static> Forward for Relayed<B> {
    type In = Carried<B>;
    type Out = Carried<B>;

    async fn forward(&self, _cx: &Cx, req: Carried<B>) -> Result<Carried<B>> {
        Ok(req)
    }
}

/// An instantiation of it.
#[gdl::export]
pub type PlainRelay = Relay<Plain>;

/// A node that is not generic, its impl written through an alias.
#[derive(gdl::Node)]
pub struct Tally;

/// The alias.
pub type Counter = Tally;

impl Build for Tally {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Sink for Counter {
    type Req = Words;
    type Item = Words;

    async fn generate(&self, _cx: Cx, _req: Words) -> Result<Items<Words>> {
        Err(Error::node("a fixture"))
    }
}

/// A generic message whose serde impls are written through an alias.
#[derive(gdl::Message)]
pub struct Narrow<T>(pub T);

impl Serialize for ByteNarrow {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ByteNarrow {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        u8::deserialize(deserializer).map(Narrow)
    }
}

/// The instantiation the impls name.
#[gdl::export]
pub type ByteNarrow = Narrow<u8>;

/// An alias that names its argument.
pub type Identity<T> = T;

/// A sink with an impl for each instantiation.
#[derive(gdl::Node)]
pub struct Kept<B>(PhantomData<fn() -> B>);

impl<B: Send + Sync + 'static> Build for Kept<B> {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

/// Written through an alias that names its argument.
impl Sink for Identity<Kept<Plain>> {
    type Req = Carried<Plain>;
    type Item = Words;

    async fn generate(&self, _cx: Cx, _req: Carried<Plain>) -> Result<Items<Words>> {
        Err(Error::node("a fixture"))
    }
}

/// Written out.
impl Sink for Kept<Loud> {
    type Req = Carried<Loud>;
    type Item = Words;

    async fn generate(&self, _cx: Cx, _req: Carried<Loud>) -> Result<Items<Words>> {
        Err(Error::node("a fixture"))
    }
}

/// The instantiation whose impl names it through `Identity`.
#[gdl::export]
pub type PlainKept = Kept<Plain>;

/// The one whose impl is written out.
#[gdl::export]
pub type LoudKept = Kept<Loud>;
