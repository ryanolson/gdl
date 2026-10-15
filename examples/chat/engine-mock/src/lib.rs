//! The engine: answers with the prompt's words in reverse, one per item.
//!
//! A mock, but built the way a real engine is: asynchronously, from its own
//! settings, and linking a dependency (`chat-sentinel`) that no other
//! segment may link.

use std::time::Duration;

use chat_contract::{Token, Words};
use futures::StreamExt;
use gdl::{Build, Cx, Error, Items, Result, Sink, items};
use serde::Deserialize;

/// The engine.
pub struct Engine {
    model: &'static str,
    uppercase: bool,
    delay: Duration,
}

/// The engine's settings: the `[node.Engine]` table of its segment's config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// How long loading takes.
    #[serde(default)]
    pub load_ms: u64,
    /// Delay before each word.
    #[serde(default)]
    pub delay_ms: u64,
    /// Answer in upper case.
    #[serde(default)]
    pub uppercase: bool,
}

impl Build for Engine {
    type Config = Settings;

    async fn build(settings: Settings) -> Result<Self> {
        // Loading weights, compiling kernels: work that takes time and must
        // finish before the segment serves.
        tokio::time::sleep(Duration::from_millis(settings.load_ms)).await;
        Ok(Self {
            model: chat_sentinel::model(),
            uppercase: settings.uppercase,
            delay: Duration::from_millis(settings.delay_ms),
        })
    }
}

impl Sink for Engine {
    type Req = Words;
    type Item = Token;

    async fn generate(&self, _cx: Cx, req: Words) -> Result<Items<Token>> {
        if req.words.is_empty() {
            return Err(Error::node(format!("{}: nothing to answer", self.model)));
        }
        let uppercase = self.uppercase;
        let delay = self.delay;
        let words = req.words.into_iter().rev().map(move |word| {
            if uppercase {
                word.to_uppercase()
            } else {
                word
            }
        });
        Ok(items(futures::stream::iter(words).then(move |word| async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(Token { word })
        })))
    }
}
