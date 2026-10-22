//! The ASR example's contract: its messages and its graph. The request is
//! streamed: a session's settings, then its audio, chunk by chunk.

use serde::{Deserialize, Serialize};

/// The settings of one session: the head of its request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// The language the recognizer must expect.
    pub language: String,
    /// The audio's sample rate, in hertz: a multiple of 16,000.
    pub sample_rate: u32,
}

/// One chunk of audio at the session's sample rate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Audio {
    /// The samples.
    pub samples: Vec<i16>,
}

/// One chunk of audio at 16 kHz, as the recognizer takes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pcm {
    /// The samples.
    pub samples: Vec<i16>,
}

/// What the recognizer has heard so far.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partial {
    /// The text so far.
    pub text: String,
    /// Whether the audio has ended and the text is final.
    pub done: bool,
}

/// What the client receives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transcript {
    /// The text so far.
    pub text: String,
    /// Whether the text is final.
    pub done: bool,
}

gdl_runtime::graph! {
    name = "asr", schema = 1;
    source   Client: Session + stream Audio -> stream Transcript;
    operator Gateway: (Session + stream Audio -> stream Transcript) over (Session + stream Audio -> stream Partial);
    forward  Resample: Session + stream Audio -> Session + stream Pcm;
    sink     Recognizer: Session + stream Pcm -> stream Partial;
    backward Punctuate: Partial -> Partial;
    topology: Client => Gateway { Resample => Recognizer => Punctuate };
}
