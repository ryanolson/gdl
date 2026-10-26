//! A model that answers with the name of the cell it runs in.

use futures::stream;
use gdl::{Build, Cx, Items, Result, Sink, items};
use policy_messages::{Answer, Ask};
use serde::Deserialize;

/// The model's settings: the cell it runs in, as its instance is told.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSettings {
    /// The cell's name.
    pub cell: String,
}

/// Answers each request with its cell's name.
#[derive(gdl::Node)]
pub struct Model {
    cell: String,
}

impl Build for Model {
    type Config = ModelSettings;
    type Metrics = ();

    async fn build(settings: ModelSettings, (): ()) -> Result<Self> {
        Ok(Self {
            cell: settings.cell,
        })
    }
}

impl Sink for Model {
    type Req = Ask;
    type Item = Answer;

    async fn generate(&self, _cx: Cx, req: Ask) -> Result<Items<Answer>> {
        let answer = Answer {
            cell: self.cell.clone(),
            text: req.text,
        };
        Ok(items(stream::iter([Ok(answer)])))
    }
}
