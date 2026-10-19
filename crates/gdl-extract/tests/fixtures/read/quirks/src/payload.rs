//! A decoy: `#[path = "payload.rs"]` in `inline` does not name this file.

#[derive(gdl::Message)]
pub struct Decoy;
