//! A crate that defines a type but derives no message.

/// Not a message: it does not derive `gdl::Message`.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Prompt;
