//! The chat example's messages. The graph is in `chat.gdl`, which names
//! these types; the generator resolves the names here and generates the
//! contract crate. Implementation crates depend on this crate for their
//! port types.

use serde::{Deserialize, Serialize};

/// What a user asks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
pub struct Prompt {
    /// The prompt text.
    pub text: String,
    /// The most words the engine may see.
    pub max_words: usize,
}

/// One piece of the answer, as the user receives it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
pub struct Chunk {
    /// The chunk's position in the answer.
    pub index: u32,
    /// The chunk's text.
    pub text: String,
}

/// The prompt as the engine takes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
pub struct Words {
    /// The prompt's words.
    pub words: Vec<String>,
    /// The most words the engine may see.
    pub max: usize,
}

/// One word the engine produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
pub struct Token {
    /// The word.
    pub word: String,
}

/// A token turned back into text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
pub struct Piece {
    /// The text.
    pub text: String,
}

/// A piece of text ready for the user.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl::Message)]
pub struct Text {
    /// The text.
    pub text: String,
}
