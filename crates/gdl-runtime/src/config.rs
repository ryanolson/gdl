use crate::GraphConfig;
use figment::{Figment, providers::Serialized};
use futures::future::BoxFuture;
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
    // Boxed so that rustc lays the node's future out apart from the
    // generated `build()`, which nests it in the builder's scopes: inline,
    // the two depths add up against rustc's query depth limit, and a node
    // that compiles alone fails in its segment. One allocation per node, at
    // start (docs/src/development/lifecycle-design.md).
    let build: BoxFuture<'static, Result<T>> = Box::pin(T::build(config, metrics));
    build.await
}
