//! A generic node whose configuration and metrics are its parameters: each
//! alias's instantiation has its arguments' fields.

use std::marker::PhantomData;

use gdl::{Build, Cx, Forward, Result};
use serde::{Deserialize, Serialize};

/// A message.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Words {
    /// The text.
    pub text: String,
}

/// The settings of a fast stage.
#[derive(Deserialize, gdl::Config)]
pub struct Fast {
    /// How many at once.
    #[gdl(graph, default = 8)]
    pub batch: i64,
}

/// The settings of a slow stage.
#[derive(Deserialize, gdl::Config)]
pub struct Slow {
    /// Whether it waits.
    #[gdl(graph, default = true)]
    pub patient: bool,
}

/// What a fast stage reports.
#[derive(gdl::Metrics)]
#[expect(dead_code, reason = "registered, never counted: only the catalogue is read")]
pub struct FastMetrics {
    /// Requests batched.
    batched_total: gdl::prometheus::IntCounter,
}

/// What a slow stage reports.
#[derive(gdl::Metrics)]
#[expect(dead_code, reason = "registered, never counted: only the catalogue is read")]
pub struct SlowMetrics {
    /// Requests waited on.
    waited_total: gdl::prometheus::IntCounter,
}

/// A stage whose settings and metrics are its parameters, with no bound of
/// its own on them: the derive bounds what it reads.
#[derive(gdl::Node)]
#[gdl(config = C, metrics = M)]
pub struct Stage<C, M>(PhantomData<fn() -> (C, M)>);

impl<C, M> Build for Stage<C, M>
where
    C: serde::de::DeserializeOwned + Send + 'static,
    M: gdl::Metrics,
{
    type Config = C;
    type Metrics = M;

    async fn build(_config: C, _metrics: M) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

impl<C: Send + 'static, M: Send + 'static> Forward for Stage<C, M> {
    type In = Words;
    type Out = Words;

    async fn forward(&self, _cx: &Cx, req: Words) -> Result<Words> {
        Ok(req)
    }
}

/// The fast instantiation.
#[gdl::export]
pub type FastStage = Stage<Fast, FastMetrics>;

/// The slow instantiation.
#[gdl::export]
pub type SlowStage = Stage<Slow, SlowMetrics>;

/// Settings whose parameter has a default, as a node that is not generic
/// names them: their fields are read whatever their arguments.
#[derive(Deserialize, gdl::Config)]
pub struct Defaulted<T = String> {
    /// Whether it echoes.
    #[gdl(graph, default = true)]
    pub enabled: bool,
    /// The parameter.
    #[serde(skip)]
    pub marker: PhantomData<T>,
}

/// A node that is not generic, whose settings leave their parameter to
/// its default.
#[derive(gdl::Node)]
#[gdl(config = Defaulted)]
pub struct Echo;

impl Build for Echo {
    type Config = Defaulted;
    type Metrics = ();

    async fn build(_config: Defaulted, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Forward for Echo {
    type In = Words;
    type Out = Words;

    async fn forward(&self, _cx: &Cx, req: Words) -> Result<Words> {
        Ok(req)
    }
}

/// A node that is not generic, whose settings have an argument from the
/// standard library that the reader does not name.
#[derive(gdl::Node)]
#[gdl(config = Defaulted<std::sync::Arc<u8>>)]
pub struct Shared;

impl Build for Shared {
    type Config = Defaulted<std::sync::Arc<u8>>;
    type Metrics = ();

    async fn build(_config: Defaulted<std::sync::Arc<u8>>, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Forward for Shared {
    type In = Words;
    type Out = Words;

    async fn forward(&self, _cx: &Cx, req: Words) -> Result<Words> {
        Ok(req)
    }
}
