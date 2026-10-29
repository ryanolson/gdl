//! A model that answers with the name of the pool it runs in.

use futures::stream;
use gdl::{Build, Cx, Items, Result, Sink, items};
use policy_messages::{Answer, Ask};
use serde::Deserialize;

/// The model's settings: the pool it runs in, as its instance is told.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSettings {
    /// The pool's name.
    pub pool: String,
}

/// Answers each request with its pool's name.
#[derive(gdl::Node)]
pub struct Model {
    pool: String,
}

impl Build for Model {
    type Config = ModelSettings;
    type Metrics = ();

    async fn build(settings: ModelSettings, (): ()) -> Result<Self> {
        Ok(Self {
            pool: settings.pool,
        })
    }
}

impl Sink for Model {
    type Req = Ask;
    type Item = Answer;

    async fn generate(&self, _cx: Cx, req: Ask) -> Result<Items<Answer>> {
        let answer = Answer {
            pool: self.pool.clone(),
            text: req.text,
        };
        Ok(items(stream::iter([Ok(answer)])))
    }
}
