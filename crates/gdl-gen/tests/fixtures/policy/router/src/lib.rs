//! A pool policy that sends each request to the pool its settings name
//! for the request's model.

use std::collections::BTreeMap;

use gdl::{Build, Pool, PoolChoice, PoolPolicy, PoolRequest, Result};
use policy_messages::Ask;
use serde::Deserialize;

/// The policy's settings: the pool that serves each model.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ByModelSettings {
    /// Model name to pool name.
    pub models: BTreeMap<String, String>,
}

/// Chooses the pool that its settings name for the request's model, and
/// no pool for a model they do not name.
pub struct ByModel {
    models: BTreeMap<String, String>,
}

impl Build for ByModel {
    type Config = ByModelSettings;
    type Metrics = ();

    async fn build(settings: ByModelSettings, (): ()) -> Result<Self> {
        Ok(Self {
            models: settings.models,
        })
    }
}

impl PoolPolicy for ByModel {
    type Head = Ask;

    fn choose(
        &self,
        request: PoolRequest<'_, Ask>,
        pools: &[Pool],
    ) -> std::result::Result<PoolChoice, gdl::NoPool> {
        let model = &request.head().model;
        let Some(pool) = self.models.get(model) else {
            tracing::warn!(model, "no pool serves the model");
            return Err(gdl::NoPool::because(format!("no pool serves model `{model}`")));
        };
        let chosen = pools
            .iter()
            .position(|c| c.name() == pool.as_str() && c.takes_work() && !request.is_excluded(c));
        // The reason reaches only the log, as a set router's does: the
        // binary's tests read it there.
        match chosen {
            Some(index) => tracing::info!(model, pool = pools[index].name(), "chose a pool"),
            None => tracing::warn!(model, "no pool serves the model"),
        }
        chosen.map(PoolChoice::new).ok_or_else(gdl::NoPool::new)
    }
}
