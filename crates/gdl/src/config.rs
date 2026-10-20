use crate::{Build, Error, GraphConfig, Result};
use figment::{Figment, providers::Serialized};

/// Build a node from ordered Figment providers and its resolved graph contract.
///
/// Graph values fill missing fields. An explicit provider value that conflicts
/// with the checked graph is rejected before the implementation is constructed.
pub async fn build_node<T: Build>(providers: &Figment, graph: &GraphConfig) -> Result<T> {
    let resolved = providers.clone().join(Serialized::defaults(graph));
    for (name, expected) in graph {
        let actual: crate::ConfigValue = resolved
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
    T::build(config).await
}
