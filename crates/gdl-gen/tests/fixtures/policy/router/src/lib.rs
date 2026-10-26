//! A cell policy that sends each request to the cell its settings name
//! for the request's model.

use std::collections::BTreeMap;

use gdl::{Build, Cell, CellChoice, CellPolicy, CellRequest, Result};
use policy_messages::Ask;
use serde::Deserialize;

/// The policy's settings: the cell that serves each model.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ByModelSettings {
    /// Model name to cell name.
    pub models: BTreeMap<String, String>,
}

/// Chooses the cell that its settings name for the request's model, and
/// no cell for a model they do not name.
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

impl CellPolicy for ByModel {
    type Head = Ask;

    fn choose(
        &self,
        request: CellRequest<'_, Ask>,
        cells: &[Cell],
    ) -> std::result::Result<CellChoice, gdl::NoCell> {
        let model = &request.head().model;
        let Some(cell) = self.models.get(model) else {
            tracing::warn!(model, "no cell serves the model");
            return Err(gdl::NoCell::because(format!("no cell serves model `{model}`")));
        };
        let chosen = cells
            .iter()
            .position(|c| c.name() == cell.as_str() && c.takes_work() && !request.is_excluded(c));
        // The reason reaches only the log, as a set router's does: the
        // binary's tests read it there.
        match chosen {
            Some(index) => tracing::info!(model, cell = cells[index].name(), "chose a cell"),
            None => tracing::warn!(model, "no cell serves the model"),
        }
        chosen.map(CellChoice::new).ok_or_else(gdl::NoCell::new)
    }
}
