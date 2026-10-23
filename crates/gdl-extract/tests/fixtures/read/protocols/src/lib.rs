//! Messages at the root, in a module, in a private module behind an
//! export, behind a feature that is off, and behind a `cfg` that is false.

use serde::{Deserialize, Serialize};

/// A prompt as words.
#[derive(Serialize, Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub struct Words;

pub mod engine;

pub use engine::Token;

mod hidden {
    use gdl_message::Message;

    /// Text, never sent: no serde.
    #[derive(Message)]
    pub struct Piece;

    /// Serialized by hand.
    #[derive(Message)]
    #[gdl(codec = "json")]
    pub struct Session;

    impl serde::Serialize for Session {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_unit()
        }
    }

    impl<'de> serde::Deserialize<'de> for Session {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            <()>::deserialize(d).map(|()| Session)
        }
    }
}

#[gdl_message::export]
pub use hidden::{Piece, Session};

/// Only with the `extra` feature.
#[cfg(feature = "extra")]
#[derive(gdl_message::Message)]
pub struct Extra;

/// On no platform.
#[cfg(not(any(unix, windows)))]
#[derive(gdl_message::Message)]
pub struct Never;

/// Not a message: no marker.
#[derive(Serialize, Deserialize)]
pub struct Plain;
