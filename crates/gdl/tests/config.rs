//! Layered node configuration and graph contract checks.
use gdl::{
    Build,
    figment::{Figment, providers::Serialized},
};
use serde::Deserialize;

#[derive(Debug, Deserialize, gdl::Config)]
struct Settings {
    #[gdl(graph, default = true)]
    enabled_chat: bool,
    listen: String,
}

struct Node(Settings);
impl Build for Node {
    type Config = Settings;
    type Metrics = ();
    async fn build(config: Settings, (): ()) -> gdl::Result<Self> {
        Ok(Self(config))
    }
}

#[tokio::test]
async fn node_build_extracts_ordered_figment_layers_and_checks_graph_contract() {
    let config = Figment::new()
        .merge(Serialized::default("enabled_chat", true))
        .merge(Serialized::default("listen", "default"))
        .join(Serialized::default("listen", "fallback"))
        .merge(Serialized::default("listen", "override"));
    let graph = gdl::GraphConfig::from([("enabled_chat".into(), gdl::ConfigValue::Bool(true))]);
    let node = gdl::build_node::<Node>(&config, &graph, ())
        .await
        .expect("test result");
    assert!(node.0.enabled_chat);
    assert_eq!(node.0.listen, "override");
    assert!(
        gdl::build_node::<Node>(
            &config.merge(Serialized::default("enabled_chat", false)),
            &graph,
            ()
        )
        .await
        .is_err()
    );
}
