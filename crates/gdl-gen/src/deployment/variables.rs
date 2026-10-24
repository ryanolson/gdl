//! Settings as environment variables that read back as they were written.
//! A hub and a segment read each variable's value with figment's value
//! parser, which takes `true` for a boolean and `{a=1}` for a table, and
//! split its name at each `__` into nested keys.

use std::str::FromStr;

use figment::value::Value as Read;
use gdl_model::cell_variable;
use serde_json::{Map, Value};

/// `table`, a `[cell]` table, as one variable per setting:
/// `GDL_CELL__NAME`, `GDL_CELL__READY__ENGINE`. A nested table whose keys
/// a variable's name can spell is one variable per entry; one with a key
/// that holds `__`, which the name would split, is one variable holding
/// the whole table (`GDL_CELL__READY={"engine__work"=2}`).
pub(super) fn cell_variables(table: &toml::Table) -> Map<String, Value> {
    let mut variables = Map::new();
    for (key, value) in table {
        match value {
            toml::Value::Table(nested) if nested.keys().all(|name| spelled(name)) => {
                for (name, value) in nested {
                    variables.insert(
                        cell_variable(&format!("{key}.{name}")),
                        encode(value).into(),
                    );
                }
            }
            value => {
                variables.insert(cell_variable(key), encode(value).into());
            }
        }
    }
    variables
}

/// `text` as a variable's value that reads back as the string it is.
pub(super) fn string(text: &str) -> String {
    match Read::from_str(text) {
        Ok(Read::String(_, read)) if read == text => text.to_string(),
        _ => quoted(text),
    }
}

/// Whether a variable's name can spell `name` as one key: the reader splits
/// a name at each `__`, and matches it without regard to case.
fn spelled(name: &str) -> bool {
    !name.contains("__") && name.bytes().all(|b| !b.is_ascii_uppercase())
}

/// `value` as a variable's value that reads back as it is.
fn encode(value: &toml::Value) -> String {
    match value {
        toml::Value::String(text) => string(text),
        toml::Value::Table(table) => {
            let entries: Vec<String> = table
                .iter()
                .map(|(name, value)| format!("{}={}", quoted(name), encode(value)))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        value => value.to_string(),
    }
}

/// `text` in double quotes, which the reader takes as a string whatever it
/// holds, with its own quotes and backslashes escaped.
fn quoted(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each value reads back, through the reader's own parser, as the
    /// string it was; only one that would read as another value is quoted.
    #[test]
    fn a_string_reads_back_as_itself() {
        for (text, written) in [
            ("global-us", "global-us"),
            ("model-x-01:1337", "model-x-01:1337"),
            ("/var/lib/gdl/cells", "/var/lib/gdl/cells"),
            ("true", "\"true\""),
            ("false", "\"false\""),
            ("12", "\"12\""),
            (" padded ", "\" padded \""),
            ("[1]", "\"[1]\""),
            ("{a=1}", "\"{a=1}\""),
            ("a\"b\\c", "a\"b\\c"),
            ("\"q\"", "\"\\\"q\\\"\""),
        ] {
            assert_eq!(string(text), written, "{text}");
            let read = Read::from_str(&string(text)).expect("a value");
            assert!(
                matches!(&read, Read::String(_, read) if read == text),
                "{text}: {read:?}"
            );
        }
    }

    /// A table whose key holds `__` is one variable, which reads back
    /// whole; one whose keys a name can spell is a variable per entry.
    #[test]
    fn a_table_is_written_so_that_its_keys_read_back() {
        let table: toml::Table = toml::from_str(
            "name = \"true\"\nready = { engine__work = 2, front = 1 }\nrequires = { model_x = 1 }\n",
        )
        .expect("a table");
        let variables = cell_variables(&table);
        let found: Vec<(&str, &str)> = variables
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str().expect("a string")))
            .collect();
        assert_eq!(
            found,
            [
                ("GDL_CELL__NAME", "\"true\""),
                ("GDL_CELL__READY", "{\"engine__work\"=2, \"front\"=1}"),
                ("GDL_CELL__REQUIRES__MODEL_X", "1"),
            ]
        );
        let ready = Read::from_str(variables["GDL_CELL__READY"].as_str().expect("a string"))
            .expect("a value");
        let ready = ready.into_dict().expect("a table");
        assert_eq!(ready.keys().collect::<Vec<_>>(), ["engine__work", "front"]);
    }
}
