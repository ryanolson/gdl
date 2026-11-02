//! A model whose build awaits a deeply nested future, as a node that
//! builds a client of a large SDK does.

use depth_messages::{Answer, Ask};
use futures::stream;
use gdl::{Build, Cx, Frontend, Items, Next, Operator, Result, Sink, items};
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

/// Hands each request to the next stage, and its answers back. `chain.gdl`
/// places it at every level of the chain.
#[derive(gdl::Node)]
pub struct Pass;

impl Operator for Pass {
    type UpReq = Ask;
    type UpItem = Answer;
    type DownReq = Ask;
    type DownItem = Answer;

    async fn generate<N>(&self, cx: Cx, req: Ask, next: N) -> Result<Items<Answer>>
    where
        N: Next<Ask, Answer>,
    {
        next.generate(cx, req).await
    }
}

impl Build for Pass {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

/// A frontend that serves nothing, so that the depth of `chain.gdl` is the
/// request path's, as behind a frontend whose handlers are boxed.
#[derive(gdl::Node)]
pub struct Door;

impl Frontend for Door {
    type Req = Ask;
    type Item = Answer;

    async fn serve<N>(
        self,
        _next: N,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()>
    where
        N: Next<Ask, Answer>,
    {
        shutdown.await;
        Ok(())
    }
}

impl Build for Door {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}
