//! The chat example's contract: its messages and its graph. Segment crates
//! and implementation crates depend on this; it depends on no
//! implementation.

use serde::{Deserialize, Serialize};

/// What a user asks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prompt {
    /// The prompt text.
    pub text: String,
    /// The most words the engine may see.
    pub max_words: usize,
}

/// One piece of the answer, as the user receives it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    /// The chunk's position in the answer.
    pub index: u32,
    /// The chunk's text.
    pub text: String,
}

/// The prompt as the engine takes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Words {
    /// The prompt's words.
    pub words: Vec<String>,
    /// The most words the engine may see.
    pub max: usize,
}

/// One word the engine produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Token {
    /// The word.
    pub word: String,
}

/// A token turned back into text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Piece {
    /// The text.
    pub text: String,
}

/// A piece of text ready for the user.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Text {
    /// The text.
    pub text: String,
}

gdl::graph! {
    name = "chat", schema = 1;
    source   Frontend: Prompt -> stream Chunk;
    operator Preprocess: (Prompt -> stream Chunk) over (Words -> stream Text);
    forward  Truncate: Words -> Words;
    sink     Engine: Words -> stream Token;
    backward Detokenize: Token -> Piece;
    backward Format: Piece -> Text;
    backward Number: Chunk -> Chunk;
    topology: Frontend => Preprocess { Truncate => Engine => Detokenize => Format } => Number;
}
