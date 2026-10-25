//! The carrier, in a module: its definition path names the module.

use serde::{Deserialize, Serialize};

/// A request carrier, generic over its payload and its builder.
#[derive(Serialize, Deserialize, gdl::Message)]
#[gdl(codec = "json")]
pub struct Carried<P, B> {
    /// What it carries.
    pub payload: P,
    /// How the request is built.
    pub builder: B,
}
