//! The chat graph's messages, as a protocols crate holds them: some at the
//! root, some in a module and re-exported, one that is not a wire type.

use serde::{Deserialize, Serialize};

/// What a user asks.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Prompt {
    /// The prompt text.
    pub text: String,
}

/// One piece of the answer.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Chunk {
    /// The chunk's text.
    pub text: String,
}

/// The engine's side of the protocol.
pub mod engine {
    use serde::{Deserialize, Serialize};

    /// The prompt as the engine takes it.
    #[derive(Serialize, Deserialize, gdl::Message)]
    pub struct Words {
        /// The words.
        pub words: Vec<String>,
    }

    /// One word the engine produced.
    #[derive(Serialize, Deserialize, gdl::Message)]
    pub struct Token {
        /// The word.
        pub word: String,
    }
}

pub use engine::{Token, Words};

/// A token as text: never crosses a process, so it needs no serde.
#[derive(gdl::Message)]
pub struct Piece {
    /// The text.
    pub text: std::rc::Rc<str>,
}

/// Text ready for the user.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Text {
    /// The text.
    pub text: String,
}
