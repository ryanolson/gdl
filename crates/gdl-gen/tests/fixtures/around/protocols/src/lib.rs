//! The fixture's one message.

use serde::{Deserialize, Serialize};

/// Some text.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Text {
    /// The text.
    pub text: String,
}
