//! A forward node written once for every builder, the instantiations a
//! deployment names, and a sink.

use std::marker::PhantomData;

use futures::stream;
use gdl::{Build, Cx, Forward, Items, Result, Sink, items};
use generic_carrier::{Builder, Carried, PlainCarried, Prompt, Reply};
use serde::Deserialize;

/// The stamp's settings.
#[derive(Debug, Deserialize, gdl::Config)]
pub struct StampSettings {
    /// What the stamp adds after the builder's mark.
    #[gdl(graph, default = "!")]
    pub suffix: String,
}

/// What a stamp reports.
#[derive(gdl::Metrics)]
pub struct StampMetrics {
    /// Requests stamped.
    stamped_total: gdl::prometheus::IntCounter,
}

/// Marks a request with its builder's mark: one implementation for every
/// builder.
#[derive(gdl::Node)]
#[gdl(config = StampSettings, metrics = StampMetrics)]
pub struct Stamp<B> {
    suffix: String,
    metrics: StampMetrics,
    builder: PhantomData<fn() -> B>,
}

impl<B: Builder> Build for Stamp<B> {
    type Config = StampSettings;
    type Metrics = StampMetrics;

    async fn build(settings: StampSettings, metrics: StampMetrics) -> Result<Self> {
        Ok(Self {
            suffix: settings.suffix,
            metrics,
            builder: PhantomData,
        })
    }
}

impl<B: Builder> Forward for Stamp<B> {
    type In = Carried<Prompt, B>;
    type Out = Carried<Prompt, B>;

    async fn forward(&self, _cx: &Cx, mut req: Self::In) -> Result<Self::Out> {
        self.metrics.stamped_total.inc();
        let mark = format!("{}{}", req.builder.mark(), self.suffix);
        req.marks.push(mark);
        Ok(req)
    }
}

/// The stamp with the plain builder.
#[gdl::export]
pub type PlainStamp = Stamp<generic_carrier::Plain>;

/// The same implementation with the loud builder.
#[gdl::export]
pub type LoudStamp = Stamp<generic_carrier::Loud>;

/// Answers with the prompt and the marks on it.
#[derive(gdl::Node)]
pub struct Echo;

/// Echo takes no settings: its table is empty.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

impl Build for Echo {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Sink for Echo {
    type Req = PlainCarried;
    type Item = Reply;

    async fn generate(&self, _cx: Cx, req: PlainCarried) -> Result<Items<Reply>> {
        let text = format!("{} [{}]", req.payload.text, req.marks.join(" "));
        Ok(items(stream::iter([Ok(Reply { text })])))
    }
}
