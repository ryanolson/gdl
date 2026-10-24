//! Where each `[cell]` setting was read from, a file or a `GDL_CELL__*`
//! variable, so that a problem with it names that file or that variable,
//! whether it is found as the settings are read or as the instance
//! launches against its deployment. Each `GDL_` variable is a provider of
//! its own, so that figment's provenance of a value names the variable
//! that gave it, whichever of several set it last.

use std::collections::BTreeMap;

use figment::value::{Dict, Map, Value};
use figment::{Figment, Metadata, Profile, Provider, Source};
use gdl_model::{CellSettings, Origin, Problems, cell_variable};

use super::{Hosted, KEYS, canonical};

/// The `GDL_` environment as a segment of `H` reads it, one provider to a
/// variable, in the environment's order, so that a later variable
/// overrides an earlier one as within one provider. A key is matched
/// without regard to case (`canonical`), and only the settings' own keys
/// are read: the build scripts' `GDL_PROFILE` and `GDL_IMAGE_PREFIX` are
/// not settings.
pub(super) fn variables<H: Hosted>() -> impl Iterator<Item = Variable> {
    const PREFIX: &str = "GDL_";
    std::env::vars_os().filter_map(|(name, value)| {
        let name = name.to_string_lossy().trim().to_string();
        let rest = name
            .get(..PREFIX.len())
            .filter(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
            .map(|_| &name[PREFIX.len()..])?;
        let key = canonical::<H>(rest);
        let first = key.split('.').next().unwrap_or_default();
        let known = KEYS.contains(&first) || H::KEYS.contains(&first);
        (known && !key.split('.').any(str::is_empty)).then(|| Variable {
            key,
            value: value.to_string_lossy().to_string(),
            name,
        })
    })
}

/// One environment variable, as a provider named for it.
pub(super) struct Variable {
    name: String,
    key: String,
    value: String,
}

/// The source a variable's metadata names, which tells it from a file's.
const VARIABLE: &str = "environment variable";

impl Provider for Variable {
    fn metadata(&self) -> Metadata {
        Metadata::from(self.name.clone(), Source::Custom(VARIABLE.into()))
    }

    fn data(&self) -> Result<Map<Profile, Dict>, figment::Error> {
        let Ok(value) = self.value.parse::<Value>();
        let dict = figment::util::nest(&self.key, value).into_dict();
        Ok(Profile::Default.collect(dict.unwrap_or_default()))
    }
}

/// The origin of each key of a `[cell]` table, by its path in the table
/// (`ready.e`).
#[derive(Debug, Clone, Default)]
pub(super) struct CellOrigins(BTreeMap<String, Origin>);

impl CellOrigins {
    /// Read `cell`, the `[cell]` table that `figment` merged, with where each
    /// of its keys came from. A problem in reading it names its origin.
    pub(super) fn read(figment: &Figment, cell: Value) -> Result<(CellSettings, Self), Problems> {
        let mut origins = BTreeMap::new();
        let mut note = |key: String| {
            let origin = figment
                .find_metadata(&format!("cell.{key}"))
                .and_then(|metadata| match &metadata.source {
                    Some(Source::File(path)) => Some(Origin::File(path.display().to_string())),
                    Some(Source::Custom(source)) if source == VARIABLE => {
                        Some(Origin::Variable(metadata.name.to_string()))
                    }
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
