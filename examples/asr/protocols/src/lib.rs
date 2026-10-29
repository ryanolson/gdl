//! The ASR example's messages. The graph is in `asr.gdl`, which names
//! these types; the generator resolves the names here and generates the
//! contract crate. The request is streamed: a session's settings, then its
//! audio, chunk by chunk.

use serde::{Deserialize, Serialize};

/// The settings of one session: the head of its request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl_message::Message)]
pub struct Session {
    /// The language the recognizer must expect.
    pub language: String,
    /// The audio's sample rate, in hertz: a multiple of 16,000.
    pub sample_rate: u32,
}

/// One chunk of audio at the session's sample rate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl_message::Message)]
pub struct Audio {
    /// The samples.
    pub samples: Vec<i16>,
}

/// One chunk of audio at 16 kHz, as the recognizer takes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl_message::Message)]
pub struct Pcm {
    /// The samples.
    pub samples: Vec<i16>,
}

/// What the recognizer has heard so far.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl_message::Message)]
pub struct Partial {
    /// The text so far.
    pub text: String,
    /// Whether the audio has ended and the text is final.
    pub done: bool,
}

/// What the client receives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gdl_message::Message)]
pub struct Transcript {
    /// The text so far.
    pub text: String,
    /// Whether the text is final.
    pub done: bool,
}
