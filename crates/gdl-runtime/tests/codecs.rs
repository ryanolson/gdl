//! Two cuts with different head, input item, and response encodings.

mod common;

use futures::StreamExt;
use gdl::{Choice, Cx, Forward, Instances, Items, Request, Selector, Sink, Streamed, items};
use gdl_runtime::{Deployment, Placement, RequestRouter, Roster, RuntimeConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, gdl::Message)]
#[gdl(codec = "json")]
/// A JSON head whose missing optional field precedes a required field.
pub struct Head {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    optional: Option<u32>,
    offset: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, gdl::Message)]
#[gdl(codec = "msgpack_array")]
/// A fixed-layout input item using positional MessagePack.
pub struct Input {
    n: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, gdl::Message)]
#[gdl(codec = "json")]
/// JSON at a later crossing exposes a router left at the default decoder.
pub struct Routed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    optional: Option<u32>,
    offset: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, gdl::Message)]
/// A response using the default named MessagePack encoding.
pub struct Output {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    optional: Option<u32>,
    n: u32,
}

mod contract {
    use super::{Head, Input, Output, Routed};
    gdl_runtime::graph! {
        name = "codecs", schema = 1;
        source A: Head + stream Input -> stream Output;
        forward F: Head + stream Input -> Routed + stream Input;
        sink B: Routed + stream Input -> stream Output;
        topology: A => F => B;
    }
}

struct Engine;

struct Relay;

impl Forward for Relay {
    type In = Streamed<Head, Input>;
    type Out = Streamed<Routed, Input>;

    async fn forward(&self, _: &Cx, request: Self::In) -> gdl::Result<Self::Out> {
        let (head, input) = request.into_parts();
        Ok(Streamed::new(
            Routed {
                optional: head.optional,
                offset: head.offset,
            },
            input,
        ))
    }
}

impl Sink for Engine {
    type Req = Streamed<Routed, Input>;
    type Item = Output;

    async fn generate(&self, cx: Cx, request: Self::Req) -> gdl::Result<Items<Output>> {
        let (head, input) = request.into_parts();
        assert_eq!(cx.meta().get("offset"), Some(&head.offset.to_string()));
        Ok(items(input.map(move |input| {
            input.map(|input| Output {
                optional: head.optional,
                n: head.offset + input.n,
            })
        })))
    }
}

struct ByHead;

impl Selector for ByHead {
    type Head = Head;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Head>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        assert_eq!(
            &serde_json::from_slice::<Head>(request.bytes()).expect("JSON"),
            request.head()
        );
        Ok(Choice::new(instances.get(0).expect("one engine"))
            .with_meta("offset", request.head().offset.to_string()))
    }
}

struct ByRouted;

impl Selector for ByRouted {
    type Head = Routed;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Routed>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        assert_eq!(
            &serde_json::from_slice::<Routed>(request.bytes()).expect("JSON"),
            request.head()
        );
        Ok(Choice::new(instances.get(0).expect("one engine")))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_codecs_cross_tcp_with_each_edges_typed_selector_and_absent_bindings() {
    let nodes = [
        common::velo_node().await,
        common::velo_node().await,
        common::velo_node().await,
    ];
    let manifest = contract::manifest();
    let placement = Placement::new()
        .segment("front", ["A"])
        .segment("middle", ["F"])
        .segment("engine", ["B"])
        .route("F", RequestRouter::Selector("head".into()))
        .route("B", RequestRouter::Selector("routed".into()));
    let mut roster = Roster::new();
    for (name, node) in ["front", "middle", "engine"].iter().zip(&nodes) {
        roster.add(*name, node.peer_info());
    }
    let mut config = RuntimeConfig::default();
    config.selectors.add("head", ByHead).expect("selector");
    let front = Deployment::new(
        nodes[0].clone(),
        &manifest,
        &placement,
        "front",
        &roster,
        config,
    )
    .expect("deployment");
    let root = contract::assemble(&front, contract::Bindings::new()).expect("absent sink");
    let front = front.start(root).expect("start");
    let mut config = RuntimeConfig::default();
    config.selectors.add("routed", ByRouted).expect("selector");
    let middle = Deployment::new(
        nodes[1].clone(),
        &manifest,
        &placement,
        "middle",
        &roster,
        config,
    )
    .expect("deployment");
    let root = contract::assemble(&middle, contract::Bindings::new().f(Relay)).expect("relay");
    let _middle = middle.start(root).expect("start");
    let engine = Deployment::new(
        nodes[2].clone(),
        &manifest,
        &placement,
        "engine",
        &roster,
        RuntimeConfig::default(),
    )
    .expect("deployment");
    let root = contract::assemble(&engine, contract::Bindings::new().b(Engine)).expect("sink");
    let _engine = engine.start(root).expect("start");

    for optional in [None, Some(5)] {
        let request = Streamed::new(
            Head {
                optional,
                offset: 10,
            },
            items(futures::stream::iter([
                Ok(Input { n: 1 }),
                Ok(Input { n: 2 }),
            ])),
        );
        let reply = tokio::time::timeout(common::WAIT, async {
            front
                .call(Cx::new(), request)
                .await
                .expect("call")
                .collect::<Vec<_>>()
                .await
        })
        .await
        .expect("bounded crossing");
        assert_eq!(
            reply,
            [
                Ok(Output { optional, n: 11 }),
                Ok(Output { optional, n: 12 })
            ]
        );
    }
}
