//! A protocols crate whose build fails in a proc macro.

dual_macro::boom!();

/// A chunk.
#[derive(gdl::Message)]
pub struct Chunk;
