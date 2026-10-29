//! What crosses into the model class, and what comes back.

use serde::{Deserialize, Serialize};

/// A request for a model.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Ask {
    /// The model that should answer.
    pub model: String,
    /// The text.
    pub text: String,
}

/// The answer, with the pool that gave it.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Answer {
    /// The pool whose model answered.
    pub pool: String,
    /// The text.
    pub text: String,
}
