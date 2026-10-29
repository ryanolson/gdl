//! Projection checks at the sending segment's start.

mod common;

use gdl::{Choice, Instances, Projected, Projection, Request, Selector};
use gdl_runtime::{Deployment, Placement, RequestRouter, Roster, RuntimeConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, gdl::Message)]
#[gdl(codec = "msgpack_array")]
/// A positional head whose routing field follows its body.
pub struct Routed {
    body: u64,
    shard: u64,
}

#[derive(Deserialize, Projection)]
#[gdl(of = Routed)]
struct Hint {
    shard: u64,
}

struct ByShard;

impl Selector for ByShard {
    type Head = Projected<Hint>;

    async fn select<'a>(
        &'a self,
        _: Request<'a, Self::Head>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        Ok(Choice::new(instances.get(0).expect("one instance")))
    }
}

mod contract {
    use super::{Routed, Shard};

    // B provides the shard that the engine's want reads, so that only the
    // edge's encoding refuses the want.
    gdl_runtime::graph! {
        name = "projection", schema = 1;
        property shard: Shard;
        source A: Routed -> stream u64;
        sink B: Routed -> stream u64 provides shard;
        topology: A => B;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_projected_selector_refuses_a_positional_edge_at_start() {
    let bytes = gdl_runtime::wire_codec!(Routed)
        .expect("wire codec")
        .encode(&Routed { body: 1, shard: 7 })
        .expect("encoded");
    let decoded = gdl::__runtime::decoder::<Hint>(gdl::Encoding::MsgpackArray)(&bytes);
    assert!(
        decoded.is_err(),
        "the partial head cannot read this positional request"
    );
    let node = common::velo_node().await;
    let mut roster = Roster::new();
    roster.add("front", node.peer_info());
    let placement = Placement::new()
        .segment("front", ["A"])
        .segment("engine", ["B"])
        .route("B", RequestRouter::Selector("by_shard".into()));
    let mut runtime = RuntimeConfig::default();
    runtime
        .selectors
        .add("by_shard", ByShard)
        .expect("registered");
    let deployment = Deployment::new(
        node,
        &contract::manifest(),
        &placement,
        "front",
        &roster,
        runtime,
    )
    .expect("the selector resolves");
    let assembled = contract::assemble(&deployment, contract::Bindings::new());
    let Err(error) = assembled else {
        panic!("a projected selector accepted msgpack_array at startup");
    };
    assert_eq!(error.kind(), gdl::ErrorKind::Placement, "{error}");
    assert!(error.message().contains("msgpack_array"), "{error}");
}

/// The engine for [`contract`]: answers nothing.
struct Engine;

impl gdl::Sink for Engine {
    type Req = Routed;
    type Item = u64;

    async fn generate(&self, _: gdl::Cx, _: Routed) -> gdl::Result<gdl::Items<u64>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}

/// The shard a request wants, as a property.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Shard {
    shard: u64,
}

/// An entry segment whose nodes want a property read by a projection
/// refuses to start when the edge its offers cross on is `msgpack_array`,
/// which carries no field names: every offer would fail to read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_projected_want_refuses_a_positional_edge_at_start() {
    let node = common::velo_node().await;
    let placement = Placement::new()
        .segment("front", ["A"])
        .segment("engine", ["B"])
        .class("global", ["front"])
        .class("model_x", ["engine"]);
    let runtime = RuntimeConfig::default();
    runtime
        .properties
        .wants(
            &gdl::PropertyKind::new("shard"),
            |hint: &Projected<Hint>| Shard { shard: hint.shard },
        )
        .expect("registered");
    let deployment = Deployment::new(
        node,
        &contract::manifest(),
        &placement,
        "engine",
        &Roster::new(),
        runtime,
    )
    .expect("a deployment");
    let assembled = contract::assemble(&deployment, contract::Bindings::new().b(Engine));
    let Err(error) = assembled else {
        panic!("a projected want accepted msgpack_array at startup");
    };
    assert_eq!(error.kind(), gdl::ErrorKind::Placement, "{error}");
    assert!(
        error.message().contains("msgpack_array") && error.message().contains("`shard`"),
        "{error}"
    );
}
