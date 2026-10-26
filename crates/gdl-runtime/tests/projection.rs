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

#[derive(Deserialize)]
struct Hint {
    #[allow(dead_code, reason = "startup must refuse before a head is read")]
    shard: u64,
}

impl Projection for Hint {
    type Of = Routed;
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
    use super::Routed;

    gdl_runtime::graph! {
        name = "projection", schema = 1;
        source A: Routed -> stream u64;
        sink B: Routed -> stream u64;
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
