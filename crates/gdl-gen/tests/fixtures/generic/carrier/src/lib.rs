//! A request carrier, generic over its payload and the builder an
//! endpoint chooses, and the instantiations a deployment names.

use serde::{Deserialize, Serialize};

/// How a request is built: chosen per endpoint.
pub trait Builder: Send + Sync + 'static {
    /// The mark this builder puts on a request.
    fn mark(&self) -> &str;
}

/// A builder that marks plainly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Plain;

impl Builder for Plain {
    fn mark(&self) -> &str {
        "plain"
    }
}

/// A builder with a field of its own.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Loud {
    /// How loud.
    pub volume: u8,
}

impl Builder for Loud {
    fn mark(&self) -> &str {
        "LOUD"
    }
}

/// What a user asks.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Prompt {
    /// The text.
    pub text: String,
}

/// One piece of the answer.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
pub struct Reply {
    /// The text.
    pub text: String,
}

/// A request on its way through the graph: its payload, and the builder
/// that finishes it. One definition serves every endpoint; each
/// instantiation an alias names is a message, in JSON.
#[derive(Debug, Clone, Serialize, Deserialize, gdl_message::Message)]
#[gdl(codec = "json")]
pub struct Carried<P, B> {
    /// What it carries.
    pub payload: P,
    /// The marks the nodes it passed put on it.
    #[serde(default)]
    pub marks: Vec<String>,
    /// How it is built.
    pub builder: B,
}

/// A prompt with the plain builder.
#[gdl_message::export]
pub type PlainCarried = Carried<Prompt, Plain>;

/// A prompt with the loud one.
#[gdl_message::export]
pub type LoudCarried = Carried<Prompt, Loud>;
