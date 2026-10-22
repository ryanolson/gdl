use crate::GraphConfig;
use figment::{Figment, providers::Serialized};
use gdl::{Build, Error, Result};

/// Build a node from ordered Figment providers, its resolved graph contract,
/// and its metrics.
///
/// Graph values fill missing fields. An explicit provider value that conflicts
/// with the checked graph is rejected before the implementation is constructed.
pub async fn build_node<T: Build>(
    providers: &Figment,
    graph: &GraphConfig,
    metrics: T::Metrics,
) -> Result<T> {
    let resolved = providers.clone().join(Serialized::defaults(graph));
    for (name, expected) in graph {
        let actual: gdl::ConfigValue = resolved
            .extract_inner(name)
            .map_err(|e| Error::node(format!("config `{name}`: {e}")))?;
        if &actual != expected {
            return Err(Error::node(format!(
                "config `{name}` conflicts with the checked graph"
            )));
        }
    }
    let config = resolved
        .extract::<T::Config>()
        .map_err(|e| Error::node(format!("node config: {e}")))?;
    T::build(config, metrics).await
}
