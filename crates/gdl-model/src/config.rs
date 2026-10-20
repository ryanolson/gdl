use crate::Error;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A configuration field that changes the graph and is exposed by the builder.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigField {
    /// The serialized field name.
    pub name: String,
    /// Display name independent of the serialized key.
    #[serde(default)]
    pub label: String,
    /// Brief text shown below the display name.
    #[serde(default)]
    pub subtext: String,
    /// Help text from the field's documentation.
    pub description: String,
    /// The value used when no provider supplies this field.
    pub default: ConfigValue,
}

/// Scalar values supported by graph configuration controls.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue {
    /// An on/off setting.
    Bool(bool),
    /// A signed integer setting.
    Integer(i64),
    /// A text setting.
    Text(String),
}

/// Resolved graph-affecting values for one node.
pub type GraphConfig = BTreeMap<String, ConfigValue>;

/// Metadata explicitly exposed by a node's configuration type.
///
/// Implemented only by `derive(gdl::Config)`. The source reader takes a
/// config type's fields from the derive's attributes, without building the
/// crate, so it cannot run a hand-written `graph_fields`; a hand impl would
/// build and then be refused when the crate is extracted.
pub trait ConfigSchema: ConfigDerived {
    /// Only graph-affecting fields; runtime-only fields stay private to the implementation.
    fn graph_fields() -> Vec<ConfigField>;
}

/// The seal on [`ConfigSchema`], which only `derive(gdl::Config)` implements.
#[diagnostic::on_unimplemented(
    message = "`{Self}` implements `ConfigSchema` by hand",
    note = "derive it with `#[derive(gdl::Config)]`: the source reader reads only the derive"
)]
pub trait ConfigDerived {}

/// Apply declared defaults and reject unknown fields or values of the wrong type.
pub fn resolve_config(fields: &[ConfigField], values: &GraphConfig) -> Result<GraphConfig, Error> {
    let mut resolved = GraphConfig::new();
    for field in fields {
        if resolved
            .insert(field.name.clone(), field.default.clone())
            .is_some()
        {
            return Err(Error::new(format!(
                "duplicate graph config field `{}`",
                field.name
            )));
        }
    }
    for (name, value) in values {
        let expected = resolved
            .get(name)
            .ok_or_else(|| Error::new(format!("`{name}` is not a graph config field")))?;
        if std::mem::discriminant(expected) != std::mem::discriminant(value) {
            return Err(Error::new(format!(
                "graph config field `{name}` has the wrong value type"
            )));
        }
        resolved.insert(name.clone(), value.clone());
    }
    Ok(resolved)
}
