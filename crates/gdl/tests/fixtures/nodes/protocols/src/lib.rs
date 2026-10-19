//! The fixtures' messages.

use serde::{Deserialize, Serialize};

/// A prompt as words.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Words;

/// One word out.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Token;

/// A token as text.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Piece;

/// A session's settings.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Session;

/// Audio at 16 kHz.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Pcm;
