//! The gateway: checks a session, passes its audio on, and turns what the
//! recognizer heard into transcripts.

use asr_contract::{Audio, Partial, Session, Transcript};
use futures::StreamExt;
use gdl::{Build, Cx, Error, Items, Next, Operator, Result, Streamed, items};
use serde::Deserialize;

/// The gateway.
pub struct Gateway {
    languages: Vec<String>,
}

/// The gateway's settings: the `[node.Gateway]` table of its segment's
/// config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The languages it accepts. Empty accepts any.
    #[serde(default)]
    pub languages: Vec<String>,
}

impl Build for Gateway {
    type Config = Settings;

    async fn build(settings: Settings) -> Result<Self> {
        Ok(Self {
            languages: settings.languages,
        })
    }
}

impl Operator for Gateway {
    type UpReq = Streamed<Session, Audio>;
    type UpItem = Transcript;
    type DownReq = Streamed<Session, Audio>;
    type DownItem = Partial;

    async fn generate<N>(&self, cx: Cx, req: Self::UpReq, next: N) -> Result<Items<Transcript>>
    where
        N: Next<Self::DownReq, Partial>,
    {
        let session = &req.head;
        if !self.languages.is_empty() && !self.languages.contains(&session.language) {
            return Err(Error::node(format!(
                "language `{}` is not served",
                session.language
            )));
        }
        if session.sample_rate == 0 || session.sample_rate % 16_000 != 0 {
            return Err(Error::node(format!(
                "sample rate {} is not a multiple of 16000",
                session.sample_rate
            )));
        }
        let heard = next.generate(cx, req).await?;
        Ok(items(heard.map(|partial| {
            partial.map(|Partial { text, done }| Transcript { text, done })
        })))
    }
}
