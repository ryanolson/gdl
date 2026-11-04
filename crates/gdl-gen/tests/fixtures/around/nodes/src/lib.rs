//! An around node and a sink.

use around_protocols::Text;
use futures::StreamExt;
use gdl::{Around, Build, Cx, Items, Result, Sink, items};
use serde::Deserialize;

/// The nodes take no settings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

/// Keeps the request's text, and marks each item of the response with it.
#[derive(gdl::Node)]
pub struct Keep;

impl Build for Keep {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Around for Keep {
    type UpReq = Text;
    type UpItem = Text;
    type DownReq = Text;
    type DownItem = Text;
    type State = String;

    async fn forward(&self, _cx: &Cx, req: Text) -> Result<(Text, String)> {
        let kept = req.text.clone();
        Ok((req, kept))
    }

    fn backward(&self, _cx: &Cx, kept: String, input: Items<Text>) -> Items<Text> {
        items(input.map(move |item| {
            item.map(|text| Text {
                text: format!("{} [kept {kept}]", text.text),
            })
        }))
    }
}

/// Answers with the request's text, once.
#[derive(gdl::Node)]
pub struct Echo;

impl Build for Echo {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Sink for Echo {
    type Req = Text;
    type Item = Text;

    async fn generate(&self, _cx: Cx, req: Text) -> Result<Items<Text>> {
        Ok(items(futures::stream::iter([Ok(Text {
            text: format!("echo {}", req.text),
        })])))
    }
}

/// Answers with its request: the unary fixture's `Tok`, which no around
/// graph declares.
#[derive(gdl::Node)]
pub struct Tok;

impl Build for Tok {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl gdl::Unary for Tok {
    type Req = Text;
    type Resp = Text;

    async fn call(&self, _cx: &Cx, req: Text) -> Result<Text> {
        Ok(req)
    }
}
