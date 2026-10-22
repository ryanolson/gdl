//! The ASR example's three segments, each on its own velo node in one
//! process, through the libraries gdl-gen generated for them, and a client
//! that streams a session's audio to the front segment.

use asr_contract::{Audio, Session, Transcript};
use gdl::{Cx, Items, Next, Result, Streamed, items};
use gdl_runtime::{Register, Segment};

/// A running deployment of the three segments.
pub struct Demo<F> {
    front: Segment<F>,
    // Kept for their lifetime: dropping a segment stops it.
    _dsp: Box<dyn Send + Sync>,
    _engine: Box<dyn Send + Sync>,
}

/// Start every segment, each on its own velo node over loopback TCP.
pub async fn start(
    language: &str,
) -> Result<Demo<impl Next<Streamed<Session, Audio>, Transcript> + Register + use<>>> {
    let address = ([127, 0, 0, 1], 0).into();
    let nodes = [
        gdl_runtime::bind_node(address).await?,
        gdl_runtime::bind_node(address).await?,
        gdl_runtime::bind_node(address).await?,
    ];
    let mut roster = gdl_runtime::Roster::new();
    for (segment, node) in ["front", "dsp", "engine"].iter().zip(&nodes) {
        roster.add(*segment, node.peer_info());
    }
    let config = gdl_runtime::RuntimeConfig::default;
    let engine = asr_engine::start(
        nodes[2].clone(),
        &roster,
        config(),
        asr_engine::Nodes {
            recognizer: build(asr_engine_mock::Settings {
                language: language.to_string(),
            })
            .await?,
            punctuate: build(asr_engine_mock::NoSettings {}).await?,
        },
    )?;
    let dsp = asr_dsp::start(
        nodes[1].clone(),
        &roster,
        config(),
        asr_dsp::Nodes {
            resample: build(asr_dsp_nodes::NoSettings {}).await?,
        },
    )?;
    let front = asr_front::start(
        nodes[0].clone(),
        &roster,
        config(),
        asr_front::Nodes {
            gateway: build(asr_front_nodes::Settings {
                languages: vec![language.to_string()],
            })
            .await?,
        },
    )?;
    Ok(Demo {
        front,
        _dsp: Box::new(dsp),
        _engine: Box::new(engine),
    })
}

/// The demo's nodes report no metrics: it runs in one process, with no
/// registry to serve them from.
async fn build<T: gdl::Build<Metrics = ()>>(config: T::Config) -> Result<T> {
    T::build(config, ()).await
}

impl<F: Next<Streamed<Session, Audio>, Transcript>> Demo<F> {
    /// Stream `audio` for `session`; transcripts stream back while it does.
    pub async fn transcribe(
        &self,
        session: Session,
        audio: Items<Audio>,
    ) -> Result<Items<Transcript>> {
        self.front
            .call(Cx::new(), Streamed::new(session, audio))
            .await
    }
}

/// `words` as audio chunks at `sample_rate`: one chunk per word, each letter
/// a sample of its code times 100, repeated to fill the rate.
pub fn speak(words: &[&str], sample_rate: u32) -> Items<Audio> {
    let repeat = (sample_rate / 16_000).max(1) as usize;
    let chunks: Vec<Result<Audio>> = words
        .iter()
        .map(|word| {
            let samples = word
                .bytes()
                .flat_map(|letter| std::iter::repeat_n(i16::from(letter) * 100, repeat))
                .collect();
            Ok(Audio { samples })
        })
        .collect();
    items(futures::stream::iter(chunks))
}
