//! A step that counts a request's words, a model that answers it, and a
//! unary node that a test's graph calls.

use futures::stream;
use gdl::{Build, Cx, Forward, Items, Result, Sink, Unary, items};
use serde::Deserialize;
use selector_messages::{Answer, Ask, Prepared};

/// Counts the words of each request.
#[derive(gdl::Node)]
pub struct Prep;

impl Forward for Prep {
    type In = Ask;
    type Out = Prepared;

    async fn forward(&self, _cx: &Cx, req: Ask) -> Result<Prepared> {
        let words = req.text.split_whitespace().count() as u32;
        Ok(Prepared {
            text: req.text,
            words,
        })
    }
}

/// Answers each request with its text and its count of words.
#[derive(gdl::Node)]
pub struct Model;

impl Sink for Model {
    type Req = Prepared;
    type Item = Answer;

    async fn generate(&self, _cx: Cx, req: Prepared) -> Result<Items<Answer>> {
        let answer = Answer {
            text: format!("{} ({} words)", req.text, req.words),
        };
        Ok(items(stream::iter([Ok(answer)])))
    }
}

/// Neither node takes settings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

impl Build for Prep {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Build for Model {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

/// Answers with the request it was given: a unary node for the graphs
/// that route calls through a selector.
#[derive(gdl::Node)]
pub struct Count;

impl Unary for Count {
    type Req = Prepared;
    type Resp = Prepared;

    async fn call(&self, _cx: &Cx, req: Prepared) -> Result<Prepared> {
        Ok(req)
    }
}

impl Build for Count {
    type Config = NoSettings;
    type Metrics = ();
    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}
