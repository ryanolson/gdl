//! The resampler: brings each chunk of a session's audio to 16 kHz, as it
//! arrives.

use asr_contract::{Audio, Pcm, Session};
use futures::StreamExt;
use gdl::{Build, Cx, Forward, Result, Streamed, items};
use serde::Deserialize;

/// Settings for a node that takes none: an empty or missing table.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

/// The resampler. It keeps every n-th sample, a mock of a real filter.
pub struct Resample;

impl Build for Resample {
    type Config = NoSettings;

    async fn build(_: NoSettings) -> Result<Self> {
        Ok(Self)
    }
}

impl Forward for Resample {
    type In = Streamed<Session, Audio>;
    type Out = Streamed<Session, Pcm>;

    async fn forward(&self, _cx: &Cx, req: Self::In) -> Result<Self::Out> {
        let (session, audio) = req.into_parts();
        let step = (session.sample_rate / 16_000).max(1) as usize;
        let pcm = audio.map(move |chunk| {
            chunk.map(|Audio { samples }| Pcm {
                samples: samples.into_iter().step_by(step).collect(),
            })
        });
        let session = Session {
            sample_rate: 16_000,
            ..session
        };
        Ok(Streamed::new(session, items(pcm)))
    }
}
