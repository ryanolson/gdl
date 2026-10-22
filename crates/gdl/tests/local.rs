//! Local composition with gdl alone: a node crate runs its nodes as nested
//! calls in one process, without the runtime or its transport
//! (`tests/dependencies.rs` keeps velo out of this crate's build).

use std::sync::Arc;

use futures::{StreamExt, stream};
use gdl::{Backward, Cx, Forward, Hop, Items, Join, Next, Result, Sink, SinkStage, Source, items};

/// Uppercases the request.
struct Upper;

impl Forward for Upper {
    type In = String;
    type Out = String;

    async fn forward(&self, _cx: &Cx, req: String) -> Result<String> {
        Ok(req.to_uppercase())
    }
}

/// Answers with one item per character.
struct Spell;

impl Sink for Spell {
    type Req = String;
    type Item = String;

    async fn generate(&self, _cx: Cx, req: String) -> Result<Items<String>> {
        let letters: Vec<Result<String>> = req.chars().map(|c| Ok(c.to_string())).collect();
        Ok(items(stream::iter(letters)))
    }
}

/// Marks each item.
struct Mark;

impl Backward for Mark {
    type In = String;
    type Out = String;

    fn backward(&self, _cx: &Cx, input: Items<String>) -> Items<String> {
        items(input.map(|item| item.map(|text| format!("{text}!"))))
    }
}

async fn answer<N: Next<String, String>>(graph: &N, req: &str) -> Vec<String> {
    graph
        .generate(Cx::new(), req.to_string())
        .await
        .expect("a stream")
        .map(|item| item.expect("an item"))
        .collect()
        .await
}

#[tokio::test]
async fn a_hop_runs_its_forward_sink_and_backward_nodes() {
    let graph = Source::new(Hop::new(
        (Arc::new(Upper), ()),
        SinkStage::new(Arc::new(Spell)),
        (Arc::new(Mark), ()),
    ));
    assert_eq!(answer(&graph, "ab").await, ["A!", "B!"]);
}

#[tokio::test]
async fn a_join_runs_another_graph_as_its_inner_end() {
    let inner = Source::new(Hop::new(
        (Arc::new(Upper), ()),
        SinkStage::new(Arc::new(Spell)),
        (Arc::new(Mark), ()),
    ));
    let outer = Source::new(Hop::new((), Join::new(inner), (Arc::new(Mark), ())));
    assert_eq!(answer(&outer, "ab").await, ["A!!", "B!!"]);
}
