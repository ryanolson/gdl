use crate::{Chunk, Prompt, Usage};
use futures::StreamExt;
use gdl::{Build, Cx, Items, Sink};
use serde::Deserialize;
use std::time::Duration;

/// A deterministic mock, with whitespace-word counts instead of model tokenization.
#[derive(gdl::Node)]
#[gdl(config = EngineConfig, metrics = EngineMetrics)]
pub struct MockEngine {
    config: EngineConfig,
    metrics: EngineMetrics,
}

impl Build for MockEngine {
    type Config = EngineConfig;
    type Metrics = EngineMetrics;

    async fn build(config: EngineConfig, metrics: EngineMetrics) -> gdl::Result<Self> {
        Ok(Self { config, metrics })
    }
}

impl Sink for MockEngine {
    type Req = Prompt;
    type Item = Chunk;

    async fn generate(&self, cx: Cx, request: Prompt) -> gdl::Result<Items<Chunk>> {
        cx.check()?;
        self.metrics.mock_requests_total.inc();
        let words: Vec<gdl::Result<Chunk>> = request
            .text
            .split_whitespace()
            .enumerate()
            .map(|(index, word)| {
                Ok(Chunk::Text {
                    index: index as u64,
                    text: if self.config.uppercase {
                        word.to_uppercase()
                    } else {
                        word.to_string()
                    },
                })
            })
            .collect();
        let tokens = words.len() as u64;
        let usage = Chunk::Usage(Usage {
            model: self.config.model.clone(),
            input_tokens: tokens,
            output_tokens: tokens,
        });
        let delay = Duration::from_millis(self.config.delay_ms);
        let chunks = self.metrics.mock_chunks_total.clone();
        Ok(gdl::items(futures::stream::iter(words).chain(futures::stream::once(async { Ok(usage) })).then(move |item| {
            let cx = cx.clone(); let chunks = chunks.clone();
            async move {
                cx.check()?;
                if !delay.is_zero() {
                    tokio::select! { biased; reason = cx.done() => return Err(reason), () = tokio::time::sleep(delay) => {} }
                }
                chunks.inc(); item
            }
        })))
    }
}

/// Settings captured in the graph and supplied only at runtime.
#[derive(Debug, Deserialize, gdl::Config)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// Label included in the terminal usage record; this is not a real loaded model.
    #[gdl(graph, default = "mock-echo", label = "Model label")]
    pub model: String,
    /// Convert each emitted word to uppercase.
    #[gdl(graph, default = false, label = "Uppercase response")]
    pub uppercase: bool,
    /// Runtime-only delay before each emitted chunk.
    #[serde(default)]
    pub delay_ms: u64,
}

/// Demo counters registered in the segment's metrics registry.
#[derive(gdl::Metrics)]
pub struct EngineMetrics {
    /// Requests accepted by the mock engine.
    mock_requests_total: gdl::prometheus::IntCounter,
    /// Emitted chunks, including terminal usage records.
    mock_chunks_total: gdl::prometheus::IntCounter,
}
