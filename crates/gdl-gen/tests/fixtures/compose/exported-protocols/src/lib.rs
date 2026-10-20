//! A message defined in a private module, offered by an export: the
//! contract names it by the export's public path.

mod inner {
    /// A prompt.
    #[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
    pub struct Prompt;
}

#[gdl_message::export]
pub use inner::Prompt;
