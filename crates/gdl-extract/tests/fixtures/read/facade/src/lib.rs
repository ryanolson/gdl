//! A crate that offers another crate's messages under its own paths: one
//! through a renamed dependency, one through that crate's own re-export.

#[gdl_message::export]
pub use protocols::Words as Prompt;

#[gdl_message::export]
pub use protocols::Token;
