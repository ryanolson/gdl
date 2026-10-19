//! A second protocols crate: it defines its own `Chunk`, and re-exports a
//! type of the first, which stays one message.

use serde::{Deserialize, Serialize};

/// Another crate's chunk: the same name, another type.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Chunk {
    /// The chunk's bytes.
    pub bytes: Vec<u8>,
}

pub use chat_protocols::Token;
