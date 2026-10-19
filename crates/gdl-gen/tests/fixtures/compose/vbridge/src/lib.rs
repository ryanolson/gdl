//! A listed crate that depends on the first version of `vproto`, so both
//! versions are in the program that reads the catalogue.

pub use vproto::Hidden;

/// This crate's own message.
#[derive(gdl_message::Message)]
pub struct Bridge;
