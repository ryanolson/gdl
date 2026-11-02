//! What the front takes and what comes back.

use serde::{Deserialize, Serialize};

/// A request.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Ask {
    /// The text.
    pub text: String,
}

/// The answer.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Answer {
    /// The text.
    pub text: String,
}
