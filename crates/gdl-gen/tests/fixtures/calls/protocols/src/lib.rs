//! The fixture's messages.

use serde::{Deserialize, Serialize};

/// Some text.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Text {
    /// The text.
    pub text: String,
}

/// What the tokenizer found in a text.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, gdl_message::Message)]
pub struct Tokens {
    /// How many words the text has.
    pub count: u32,
}
