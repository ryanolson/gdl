//! A listed crate that depends on the first version of `vproto`, and
//! re-exports a message of it without `export`: it offers only its own.

pub use vproto::Hidden;

/// This crate's own message.
#[derive(gdl_message::Message)]
pub struct Bridge;
