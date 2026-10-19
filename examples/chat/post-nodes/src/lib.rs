//! The post-processing nodes: turn the engine's tokens into text.

use chat_protocols::{Piece, Text, Token};
use futures::StreamExt;
use gdl::{Backward, Build, Cx, Items, Result, items};
use serde::Deserialize;

/// Turns each token into a piece of text.
#[gdl::node]
pub struct Detokenize;

impl Backward for Detokenize {
    type In = Token;
    type Out = Piece;

    fn backward(&self, _cx: &Cx, input: Items<Token>) -> Items<Piece> {
        items(input.map(|token| token.map(|Token { word }| Piece { text: word })))
    }
}

/// Brackets each piece of text with the configured marks.
#[gdl::node]
pub struct Format {
    open: String,
    close: String,
}

/// `Format`'s settings: the `[node.Format]` table of its segment's config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormatSettings {
    /// Put before each piece.
    #[serde(default = "open")]
    pub open: String,
    /// Put after each piece.
    #[serde(default = "close")]
    pub close: String,
}

fn open() -> String {
    "[".to_string()
}

fn close() -> String {
    "]".to_string()
}

impl Backward for Format {
    type In = Piece;
    type Out = Text;

    fn backward(&self, _cx: &Cx, input: Items<Piece>) -> Items<Text> {
        let (open, close) = (self.open.clone(), self.close.clone());
        items(input.map(move |piece| {
            piece.map(|Piece { text }| Text {
                text: format!("{open}{text}{close}"),
            })
        }))
    }
}

/// `Detokenize` takes no settings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

impl Build for Detokenize {
    type Config = NoSettings;
    async fn build(_: NoSettings) -> Result<Self> {
        Ok(Self)
    }
}

impl Build for Format {
    type Config = FormatSettings;
    async fn build(settings: FormatSettings) -> Result<Self> {
        Ok(Self {
            open: settings.open,
            close: settings.close,
        })
    }
}
