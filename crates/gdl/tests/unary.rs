//! A unary service called from nodes in local composition: a forward node
//! awaits a call per request, and a backward node moves a clone of the
//! client into its stream and calls it per item.

use std::sync::Arc;

use futures::{StreamExt, stream};
use gdl::{
    Backward, Cx, Error, Forward, Hop, Items, Next, Result, Sink, SinkStage, Source, Unary,
    UnaryClient, items,
};

/// Spells a number as a word, and refuses those it does not know.
struct Spell;

impl Unary for Spell {
    type Req = u64;
    type Resp = String;

    async fn call(&self, cx: &Cx, n: u64) -> Result<String> {
        // A service reads the caller's context.
        let suffix = cx.meta().get("suffix").map_or("", String::as_str);
        let word = match n {
            1 => "one",
            2 => "two",
            3 => "three",
            _ => return Err(Error::node(format!("no word for {n}"))),
        };
        Ok(format!("{word}{suffix}"))
    }
}

/// Turns the request into its word, through the service.
struct Named {
    spell: UnaryClient<u64, String>,
}

impl Forward for Named {
    type In = u64;
    type Out = String;

    async fn forward(&self, cx: &Cx, n: u64) -> Result<String> {
        self.spell.call(cx, n).await
    }
}

/// Answers each number from 1 to its request's length.
struct Count;

impl Sink for Count {
    type Req = String;
    type Item = u64;

    async fn generate(&self, _cx: Cx, req: String) -> Result<Items<u64>> {
        let n = req.len() as u64;
        Ok(items(stream::iter((1..=n).map(Ok))))
    }
}

/// Spells each item, through the service.
struct Words {
    spell: UnaryClient<u64, String>,
}

impl Backward for Words {
    type In = u64;
    type Out = String;

    fn backward(&self, cx: &Cx, input: Items<u64>) -> Items<String> {
        let state = (input, self.spell.clone(), cx.clone());
        items(stream::unfold(state, |(mut input, spell, cx)| async move {
            let item = match input.next().await? {
                Ok(n) => spell.call(&cx, n).await,
                Err(e) => Err(e),
            };
            Some((item, (input, spell, cx)))
        }))
    }
}

#[tokio::test]
async fn nodes_call_a_service_per_request_and_per_item() {
    let spell = UnaryClient::local(Arc::new(Spell));
    let words = Words {
        spell: spell.clone(),
    };
    let graph = Source::new(Hop::new(
        (Arc::new(Named { spell }), ()),
        SinkStage::new(Arc::new(Count)),
        (Arc::new(words), ()),
    ));
    let mut cx = Cx::new();
    cx.insert_meta("suffix", "!");
    let answer: Vec<Result<String>> = graph
        .generate(cx, 2)
        .await
        .expect("a stream")
        .collect()
        .await;
    let answer: Vec<String> = answer
        .into_iter()
        .map(|item| match item {
            Ok(word) => word,
            Err(e) => format!("error: {e}"),
        })
        .collect();
    // "two!" has four letters: the sink counts 1 to 4, and the backward
    // node spells each, failing on the one it has no word for.
    assert_eq!(answer[..3], ["one!", "two!", "three!"]);
    assert!(answer[3].contains("no word for 4"), "{answer:?}");

    // A request the service refuses fails the call.
    let refused = graph.generate(Cx::new(), 9).await;
    assert!(refused.is_err());
}
