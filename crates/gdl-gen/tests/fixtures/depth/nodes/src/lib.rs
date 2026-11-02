//! A model whose build awaits a deeply nested future, as a node that
//! builds a client of a large SDK does.

use depth_messages::{Answer, Ask};
use futures::stream;
use gdl::{Build, Cx, Items, Result, Sink, items};
use serde::Deserialize;

/// An `async` block that awaits the one inside it, once per token: each
/// holds the next across its await, so laying it out lays out the next.
macro_rules! nested {
    () => {
        async {}
    };
    ($level:tt $($rest:tt)*) => {
        async { nested!($($rest)*).await }
    };
}

/// Answers each request with its text.
#[derive(gdl::Node)]
pub struct Deep;

impl Sink for Deep {
    type Req = Ask;
    type Item = Answer;

    async fn generate(&self, _cx: Cx, req: Ask) -> Result<Items<Answer>> {
        let answer = Answer { text: req.text };
        Ok(items(stream::iter([Ok(answer)])))
    }
}

/// `Deep` takes no settings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

/// 36 levels: on the pinned toolchain this crate compiles with up to 41,
/// and a segment that laid this future out inside its own build compiled
/// with only up to 27, so a few levels either way keep the test's meaning.
impl Build for Deep {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        nested!(x x x x x x x x x x x x x x x x x x x x x x x x x x x x x x x x x x x x).await;
        Ok(Self)
    }
}
