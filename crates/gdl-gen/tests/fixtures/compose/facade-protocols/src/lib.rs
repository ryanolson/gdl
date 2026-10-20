//! A crate that defines nothing and offers another crate's message under
//! its own path.

#[gdl_message::export]
pub use other_protocols::Chunk;
