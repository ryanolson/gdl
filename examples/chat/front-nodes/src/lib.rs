//! The front-end nodes: preprocessing, truncation, and numbering.

use chat_protocols::{Chunk, Prompt, Text, Words};
use futures::StreamExt;
use gdl::{Backward, Build, Cx, Forward, Items, Next, Operator, Result, items};
use serde::Deserialize;

/// Splits the prompt into words, and wraps each piece of text the engine
/// side returns as a chunk.
#[derive(gdl::Node)]
pub struct Preprocess;

impl Operator for Preprocess {
    type UpReq = Prompt;
    type UpItem = Chunk;
    type DownReq = Words;
    type DownItem = Text;

    async fn generate<N>(&self, cx: Cx, req: Prompt, next: N) -> Result<Items<Chunk>>
    where
        N: Next<Words, Text>,
    {
        let words = Words {
            words: req.text.split_whitespace().map(str::to_string).collect(),
            max: req.max_words,
        };
        let texts = next.generate(cx, words).await?;
        Ok(items(texts.map(|text| {
            text.map(|Text { text }| Chunk { index: 0, text })
        })))
    }
}

/// Keeps at most `max` words.
#[derive(gdl::Node)]
pub struct Truncate;

impl Forward for Truncate {
    type In = Words;
    type Out = Words;

    async fn forward(&self, _cx: &Cx, mut req: Words) -> Result<Words> {
        req.words.truncate(req.max);
        Ok(req)
    }
}

/// Numbers the chunks of an answer in order.
#[derive(gdl::Node)]
pub struct Number;

impl Backward for Number {
    type In = Chunk;
    type Out = Chunk;

    fn backward(&self, _cx: &Cx, input: Items<Chunk>) -> Items<Chunk> {
        items(input.enumerate().map(|(index, chunk)| {
            chunk.map(|chunk| Chunk {
                index: u32::try_from(index).unwrap_or(u32::MAX),
                ..chunk
            })
        }))
    }
}

/// None of the front-end nodes takes settings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

impl Build for Preprocess {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Build for Truncate {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Build for Number {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}
