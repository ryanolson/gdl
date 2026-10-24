//! Where each `[cell]` setting was read from, a file or a `GDL_CELL__*`
//! variable, so that a problem with it names that file or that variable,
//! whether it is found as the settings are read or as the instance
//! launches against its deployment.

use std::collections::BTreeMap;

use figment::providers::Env;
use figment::value::Value;
use figment::{Figment, Provider, Source};
use gdl_model::{CellSettings, Origin, Problems, cell_variable, cell_variable_of};

/// The origin of each key of a `[cell]` table, by its path in the table
/// (`ready.e`).
#[derive(Debug, Clone, Default)]
pub(super) struct CellOrigins(BTreeMap<String, Origin>);

impl CellOrigins {
    /// Read `cell`, the `[cell]` table that `figment` merged, with where each
    /// of its keys came from. A problem in reading it names its origin.
    pub(super) fn read(figment: &Figment, cell: Value) -> Result<(CellSettings, Self), Problems> {
        let env = Env::prefixed("GDL_").metadata().name;
        let variables: Vec<String> = std::env::vars_os()
            .filter_map(|(name, _)| name.into_string().ok())
            .collect();
        // The variable that set the key, which may have set its whole table.
        let variable = |key: &str| {
            cell_variable_of(key, variables.iter().map(String::as_str))
                .map_or_else(|| cell_variable(key), str::to_string)
        };
        let mut origins = BTreeMap::new();
        let mut note = |key: String| {
            let origin = figment
                .find_metadata(&format!("cell.{key}"))
                .and_then(|metadata| match &metadata.source {
                    Some(Source::File(path)) => Some(Origin::File(path.display().to_string())),
                    _ if metadata.name == env => Some(Origin::Variable(variable(&key))),
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
    /// set, a missing one, is the variable's to set when every other key
    /// came from a variable, and the file's when every other came from one
    /// file; otherwise it is named by its key alone.
    pub(super) fn locate(&self, problems: Problems) -> Problems {
        let variables = !self.0.is_empty()
            && self
                .0
                .values()
                .all(|origin| matches!(origin, Origin::Variable(_)));
        let file = {
            let mut all = self.0.values();
            all.next()
                .filter(|first| {
                    matches!(first, Origin::File(_)) && all.all(|other| other == *first)
                })
                .cloned()
        };
        let unset = |key: &str| match variables {
            true => Some(Origin::Variable(cell_variable(key))),
            false => file.clone(),
        };
        Problems(
            problems
                .0
                .into_iter()
                .map(|mut problem| {
                    if problem.origin.is_none() {
                        let path = problem.path();
                        problem.origin = match path.as_deref().and_then(|p| p.strip_prefix("cell."))
                        {
                            Some(key) => self.0.get(key).cloned().or_else(|| unset(key)),
                            None => file.clone(),
                        };
                    }
                    problem
                })
                .collect(),
        )
    }
}
