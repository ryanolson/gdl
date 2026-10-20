//! A crate that defines a type but derives no message.

/// Not a message: it does not derive `gdl_message::Message`.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Prompt;
