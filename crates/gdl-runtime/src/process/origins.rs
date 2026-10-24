//! Where each `[cell]` setting was read from, a file or a `GDL_CELL__*`
//! variable, so that a problem with it names that file or that variable,
//! whether it is found as the settings are read or as the instance
//! launches against its deployment.

use std::collections::BTreeMap;

use figment::providers::Env;
use figment::value::Value;
use figment::{Figment, Provider, Source};
use gdl_model::{CellSettings, Origin, Problems, cell_variable};

/// The origin of each key of a `[cell]` table, by its path in the table
/// (`ready.e`).
#[derive(Debug, Clone, Default)]
pub(super) struct CellOrigins(BTreeMap<String, Origin>);

impl CellOrigins {
    /// Read `cell`, the `[cell]` table that `figment` merged, with where each
    /// of its keys came from. A problem in reading it names its origin.
    pub(super) fn read(figment: &Figment, cell: Value) -> Result<(CellSettings, Self), Problems> {
        let env = Env::prefixed("GDL_").metadata().name;
        let mut origins = BTreeMap::new();
        let mut note = |key: String| {
            let origin = figment
                .find_metadata(&format!("cell.{key}"))
                .and_then(|metadata| match &metadata.source {
                    Some(Source::File(path)) => Some(Origin::File(path.display().to_string())),
                    _ if metadata.name == env => Some(Origin::Variable(cell_variable(&key))),
                    _ => None,
                });
            if let Some(origin) = origin {
                origins.insert(key, origin);
            }
        };
        if let Some(table) = cell.as_dict() {
            for (key, value) in table {
                if let Some(nested) = value.as_dict() {
                    for inner in nested.keys() {
                        note(format!("{key}.{inner}"));
                    }
                }
                note(key.clone());
            }
        }
        let origins = Self(origins);
        let settings = CellSettings::read(&cell).map_err(|problems| origins.locate(problems))?;
        Ok((settings, origins))
    }

    /// `problems`, each with the origin of its key. A key that was never
    /// set (a missing one) takes the origin of the table's others, when
    /// they all came from one place.
    pub(super) fn locate(&self, problems: Problems) -> Problems {
        let shared = {
            let mut all = self.0.values();
            all.next()
                .filter(|first| all.all(|other| other == *first))
                .cloned()
        };
        Problems(
            problems
                .0
                .into_iter()
                .map(|mut problem| {
                    if problem.origin.is_none() {
                        problem.origin = match problem
                            .path()
                            .as_deref()
                            .and_then(|path| path.strip_prefix("cell."))
                        {
                            Some(key) => self.0.get(key).cloned().or_else(|| match &shared {
                                Some(Origin::Variable(_)) => {
                                    Some(Origin::Variable(cell_variable(key)))
                                }
                                other => other.clone(),
                            }),
                            None => shared.clone(),
                        };
                    }
                    problem
                })
                .collect(),
        )
    }
}
