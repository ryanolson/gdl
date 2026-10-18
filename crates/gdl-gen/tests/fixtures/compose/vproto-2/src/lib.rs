//! The second version of a protocols package: its `Chunk` is a wire type.

/// A chunk that crosses processes.
#[derive(serde::Serialize, serde::Deserialize, gdl::Message)]
pub struct Chunk;
