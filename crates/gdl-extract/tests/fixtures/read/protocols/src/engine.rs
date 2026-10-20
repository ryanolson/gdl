//! The engine's messages, in a module file.

/// One word out.
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
pub struct Token;
