//! Messages from a registry.

/// Words in.
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
pub struct Words;

/// Tokens out.
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
pub struct Token;
