//! What the front takes, what it sends to the model, and what comes back.

use serde::{Deserialize, Serialize};

/// A request.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Ask {
    /// The text.
    pub text: String,
}

/// A request as the front sends it to the model.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Prepared {
    /// The text.
    pub text: String,
    /// How many words it has.
    pub words: u32,
}

/// The answer.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Answer {
    /// The text.
    pub text: String,
}
