//! The recognizer and the punctuator. The recognizer is a mock: it hears
//! each chunk of audio as one word, spelled by its samples (each sample is a
//! letter's code times 100), and reports the text so far after each chunk.

use asr_contract::{Partial, Pcm, Session};
use futures::StreamExt;
use gdl::{Backward, Build, Cx, Error, Items, Result, Sink, Streamed, items};
use serde::Deserialize;

/// The recognizer.
pub struct Recognizer {
    language: String,
}

/// The recognizer's settings: the `[node.Recognizer]` table of its
/// segment's config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The language its model knows.
    pub language: String,
}

impl Build for Recognizer {
    type Config = Settings;
    type Metrics = ();

    async fn build(settings: Settings, (): ()) -> Result<Self> {
        Ok(Self {
            language: settings.language,
        })
    }
}

impl Sink for Recognizer {
    type Req = Streamed<Session, Pcm>;
    type Item = Partial;

    async fn generate(&self, _cx: Cx, req: Self::Req) -> Result<Items<Partial>> {
        let (session, pcm) = req.into_parts();
        if session.language != self.language {
            return Err(Error::node(format!(
                "the model knows `{}`, not `{}`",
                self.language, session.language
            )));
        }
        if session.sample_rate != 16_000 {
            return Err(Error::node("the recognizer takes 16 kHz audio"));
        }
        // After the last chunk, one more partial: the final text.
        let heard = pcm.map(Some).chain(futures::stream::iter([None]));
        let text = heard.scan(String::new(), |text, chunk| {
            let partial = match chunk {
                Some(Ok(Pcm { samples })) => {
                    if !text.is_empty() {
                        text.push(' ');
                    }
                    text.extend(samples.iter().map(|&s| char::from((s / 100) as u8)));
                    Ok(Partial {
                        text: text.clone(),
                        done: false,
                    })
                }
                Some(Err(error)) => Err(error),
                None => Ok(Partial {
                    text: text.clone(),
                    done: true,
                }),
            };
            futures::future::ready(Some(partial))
        });
        Ok(items(text))
    }
}

/// Settings for a node that takes none: an empty or missing table.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

/// The punctuator: a final text starts with a capital and ends with a
/// full stop.
pub struct Punctuate;

impl Build for Punctuate {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Backward for Punctuate {
    type In = Partial;
    type Out = Partial;

    fn backward(&self, _cx: &Cx, input: Items<Partial>) -> Items<Partial> {
        items(input.map(|partial| {
            partial.map(|Partial { text, done }| {
                if !done {
                    return Partial { text, done };
                }
                let mut letters = text.chars();
                let text = match letters.next() {
                    Some(first) => format!("{}{}.", first.to_uppercase(), letters.as_str()),
                    None => text,
                };
                Partial { text, done }
            })
        }))
    }
}
