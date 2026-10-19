//! A protocols crate whose build fails in a proc macro.

dual_macro::boom!();

/// A chunk.
#[derive(gdl_message::Message)]
pub struct Chunk;
