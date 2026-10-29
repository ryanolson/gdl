//! Property types are checked when a program assembles its graph.

use gdl::{Cx, EntryState, Items, PropertyKind, Sink};
use gdl_runtime::{Deployment, Placement, Roster, RuntimeConfig, declare_properties};

mod contract {
    gdl_runtime::graph! {
        name = "typed", schema = 1;
        property shard: super::Shard;
        source Ask: u32 -> stream u32;
        sink Serve: u32 -> stream u32 provides shard;
        topology: Ask => Serve;
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Shard {
    name: String,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Other {
    name: String,
}

struct Echo;

impl Sink for Echo {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, _: Cx, request: u32) -> gdl::Result<Items<u32>> {
        Ok(gdl::items(futures::stream::iter([Ok(request)])))
    }
}

#[tokio::test]
async fn assembling_a_graph_refuses_another_property_type() {
    let manifest = contract::manifest();
    let placement = Placement::new().segment("only", ["Ask", "Serve"]);
    let config = RuntimeConfig::default();
    config
        .properties
        .set(
            &PropertyKind::<Other>::new("shard"),
            [(Other { name: "a".into() }, EntryState::Ready)],
        )
        .expect("properties before assembly");
    let error = declare_properties(
        &manifest,
        &placement,
        "only",
        &contract::property_types(),
        &[],
        &config.properties,
        &config.followed,
    )
    .expect_err("the wrong type does not fit the contract");
    assert!(error.to_string().contains("Other`"), "{error}");

    let node = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        gdl_runtime::bind_node(([127, 0, 0, 1], 0).into()),
    )
    .await
    .expect("node starts within the bound")
    .expect("node");
    let deployment = Deployment::new(node, &manifest, &placement, "only", &Roster::new(), config)
        .expect("deployment");
    assert!(
        contract::assemble(&deployment, contract::Bindings::new().serve(Echo)).is_err(),
        "graph assembly accepts Other keys where its property declaration requires Shard"
    );
}
