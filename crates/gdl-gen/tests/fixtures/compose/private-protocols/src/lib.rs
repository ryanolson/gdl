//! A message defined in a private module and re-exported: the contract
//! cannot name it where it is defined.

mod inner {
    /// A prompt.
    #[derive(serde::Serialize, serde::Deserialize, gdl::Message)]
    pub struct Prompt;
}

pub use inner::Prompt;
