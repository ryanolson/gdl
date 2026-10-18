//! The first version of a protocols package. Its `Chunk` is not a wire
//! type, and its `Hidden` is gone from the second version.

/// A chunk that never crosses a process.
#[derive(gdl::Message)]
pub struct Chunk;

/// A type only this version defines.
#[derive(serde::Serialize, serde::Deserialize, gdl::Message)]
pub struct Hidden;
