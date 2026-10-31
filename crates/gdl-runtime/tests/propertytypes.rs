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

mod classed {
    gdl_runtime::graph! {
        name = "classed", schema = 1;
        property shard: super::Shard;
        source Ask: u32 -> stream u32;
        sink Serve: u32 -> stream u32;
        topology: Ask => Serve;
    }
}

/// Reads property kind `shard` of the pools its segment follows.
struct NeedsShard;

impl gdl::Selector for NeedsShard {
    type Head = gdl::Encoded;
    const NEEDS: &'static [&'static str] = &["shard"];

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, gdl::Encoded>,
        instances: gdl::Instances<'a>,
    ) -> gdl::Result<gdl::Choice<'a>> {
        Ok(gdl::Choice::new(instances.get(0).expect("never empty")))
    }
}

/// A selector registered in code says what it reads in its `NEEDS`, which
/// no source reader sees: the deployment of the segment that sends through
/// it holds the class of its crossing to a supply of each kind, as a
/// chooser's launch holds a pool policy's, and refuses it otherwise,
/// naming the kind, the selector, and the class.
#[tokio::test]
async fn a_selector_registered_in_code_requires_its_property_supply() {
    let manifest = &classed::manifest();
    let placement = |pools: &str| {
        Placement::from_toml(&format!(
            "[segment.front]\nnodes = [\"Ask\"]\n[segment.model]\nnodes = [\"Serve\"]\n\
             [routing]\nServe = {{ selector = \"by_shard\" }}\n\
             [class.global]\nsegments = [\"front\"]\n[class.model]\nsegments = [\"model\"]\n{pools}"
        ))
        .expect("a placement")
    };
    let deploy = |placement: Placement| async move {
        let node = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            gdl_runtime::bind_node(([127, 0, 0, 1], 0).into()),
        )
        .await
        .expect("node starts within the bound")
        .expect("node");
        let mut config = RuntimeConfig::default();
        config
            .selectors
            .add("by_shard", NeedsShard)
            .expect("selector");
        Deployment::new(node, manifest, &placement, "front", &Roster::new(), config).map(drop)
    };
    let refused = deploy(placement(""))
        .await
        .expect_err("model supplies no shard")
        .to_string();
    assert!(
        refused.contains("selector `by_shard` needs property `shard` from class `model`"),
        "{refused}"
    );
    deploy(placement("[pools.model]\nprovides = [\"shard\"]\n"))
        .await
        .expect("the model's pools provide it");
}
