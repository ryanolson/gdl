//! Reference nodes for a deterministic, typed film demonstration.

mod engine;
mod frontend;

pub use engine::{EngineConfig, EngineMetrics, MockEngine};
pub use frontend::{FrontendConfig, HttpFrontend};

use serde::{Deserialize, Serialize};

/// The text-only request shared by reference and vendor nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
pub struct Prompt {
    /// Input text, split on whitespace by the mock engine.
    pub text: String,
}

/// A streamed text chunk or the mock engine's terminal usage record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Chunk {
    /// One emitted word.
    Text {
        /// Zero-based position in the answer.
        index: u64,
        /// Text returned to the caller.
        text: String,
    },
    /// Final accounting information, included in the response stream.
    Usage(Usage),
}

/// Known demonstration counts, where one whitespace-delimited word is one token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
pub struct Usage {
    /// The configured mock model label.
    pub model: String,
    /// Number of input words.
    pub input_tokens: u64,
    /// Number of output words.
    pub output_tokens: u64,
}
