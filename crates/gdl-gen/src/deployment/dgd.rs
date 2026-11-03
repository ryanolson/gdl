//! A pool's DynamoGraphDeployment (DGD), as its fleet file names it
//! (`[pool.<name>.dgd]`): read beside the fleet file, as YAML, which
//! JSON also is, and judged as the pool's `GdlPool` judges it
//! ([`gdl_k8s::check_dgd`]), so that `check` refuses a DGD the pool could
//! not own before anything is built. See `docs/src/guides/operator.md`,
//! "A pool with a DGD".

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use gdl_model::{Fleet, PoolDgd, Problem, Problems};
use serde_json::Value;
use yaml_rust2::{Yaml, YamlLoader};

/// The DGD file of each pool of a fleet that names one, as JSON, by the
/// pool's name: what `cargo gdl k8s` and `deploy` render each such pool's
/// DGD from.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Dgds(BTreeMap<String, Value>);

impl Dgds {
    /// Read the DGD file each pool of `fleet` names, relative to the
    /// directory of the fleet file at `fleet_file`. Refused, with a
    /// problem for each, when a file cannot be read or is not one YAML
    /// document that JSON can hold.
    pub fn read(fleet: &Fleet, fleet_file: &Path) -> Result<Self, Problems> {
        let mut dgds = BTreeMap::new();
        let mut problems = Vec::new();
        for (pool, dgd) in fleet.dgds() {
            match read(dgd, fleet_file) {
                Ok(value) => {
                    dgds.insert(pool.to_string(), value);
                }
                Err(message) => problems.push(at(pool, message)),
            }
        }
        match problems.is_empty() {
            true => Ok(Self(dgds)),
            false => Err(Problems(problems)),
        }
    }

    /// The DGD of pool `pool`, if it names one.
    pub(super) fn get(&self, pool: &str) -> Option<&Value> {
        self.0.get(pool)
    }
}

/// Every problem with the DGD files of `fleet`, whose file is at
/// `fleet_file`: each that does not read, and each that its pool could
/// not own in its cluster's namespace ([`gdl_k8s::check_dgd`]). A pool
/// without a cluster has no namespace to judge its file against, and
/// [`Fleet::check`] already refuses its table.
pub(crate) fn problems(fleet: &Fleet, fleet_file: &Path) -> Vec<Problem> {
    let mut problems = Vec::new();
    for (pool, dgd) in fleet.dgds() {
        let value = match read(dgd, fleet_file) {
            Ok(value) => value,
            Err(message) => {
                problems.push(at(pool, message));
                continue;
            }
        };
        let Some((_, cluster)) = fleet.cluster_of(pool) else {
            continue;
        };
        let placed: Vec<(&str, &str)> = dgd
            .components
            .iter()
            .map(|(segment, component)| (segment.as_str(), component.as_str()))
            .collect();
        if let Err(message) = gdl_k8s::check_dgd(pool, &cluster.namespace, &value, &placed) {
            let path = path(dgd, fleet_file);
            problems.push(at(pool, format!("{}: {message}", path.display())));
        }
    }
    problems
}

/// The path of `dgd`'s file, relative to the fleet file at `fleet_file`.
fn path(dgd: &PoolDgd, fleet_file: &Path) -> PathBuf {
    match fleet_file.parent() {
        Some(dir) => dir.join(&dgd.file),
        None => PathBuf::from(&dgd.file),
    }
}

/// A problem at pool `pool`'s `dgd.file`.
fn at(pool: &str, message: String) -> Problem {
    Problem::at(
        Some(format!("pool.{pool}.dgd")),
        Some("file".into()),
        message,
    )
}

/// The DGD file of `dgd`, as JSON; refused, with why, as [`parse`] says.
fn read(dgd: &PoolDgd, fleet_file: &Path) -> Result<Value, String> {
    let path = path(dgd, fleet_file);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("{} cannot be read: {e}", path.display()))?;
    parse(&text).map_err(|why| format!("{} {why}", path.display()))
}

/// `text`, one YAML document, as JSON, with each alias read as the value
/// it names; refused, with why, unless JSON can hold it as written: its
/// keys strings and no merge key (`<<`), and its numbers finite.
fn parse(text: &str) -> Result<Value, String> {
    let documents = YamlLoader::load_from_str(text).map_err(|e| format!("is not YAML: {e}"))?;
    match documents.as_slice() {
        [document] => json(document),
        others => Err(format!(
            "holds {} YAML documents, and a DGD file holds one",
            others.len()
        )),
    }
}

/// `yaml` as JSON.
fn json(yaml: &Yaml) -> Result<Value, String> {
    Ok(match yaml {
        Yaml::Null => Value::Null,
        Yaml::Boolean(value) => Value::Bool(*value),
        Yaml::Integer(value) => Value::from(*value),
        Yaml::String(value) => Value::String(value.clone()),
        Yaml::Real(text) => yaml
            .as_f64()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| format!("holds `{text}`, a number that JSON cannot hold"))?,
        Yaml::Array(items) => Value::Array(items.iter().map(json).collect::<Result<_, _>>()?),
        Yaml::Hash(fields) => {
            let mut object = serde_json::Map::new();
            for (key, value) in fields {
                let Yaml::String(key) = key else {
                    return Err(format!(
                        "has a key that is not a string, `{key:?}`, which a Kubernetes object never has"
                    ));
                };
                // The loader keeps `<<` as a key. kubectl would merge it, and
                // the CRD's schema would prune it here, losing the fields.
                if key == "<<" {
                    return Err(
                        "has `<<`: YAML merge keys are not supported; write the fields out".into(),
                    );
                }
                object.insert(key.clone(), json(value)?);
            }
            Value::Object(object)
        }
        // The loader reads each alias as its value, and makes neither.
        Yaml::Alias(_) | Yaml::BadValue => {
            return Err("has a value that is not YAML's".into());
        }
    })
}

#[cfg(test)]
mod tests {
    use super::parse;
    use serde_json::json;

    /// YAML 1.2's scalars, as JSON holds them: a version stays a string,
    /// as do quoted numbers and booleans. An alias is its anchor's value.
    #[test]
    fn a_dgd_file_reads_as_json_with_its_scalars_kept() {
        let text = "a: 1.7.0\nb: \"1\"\nc: \"false\"\nd: 630\ne: 0.5\nf: true\ng: null\nh: yes\ni: [1, x]\nj: &k 2\nl: *k\n";
        assert_eq!(
            parse(text).expect("JSON"),
            json!({"a": "1.7.0", "b": "1", "c": "false", "d": 630, "e": 0.5, "f": true, "g": null, "h": "yes", "i": [1, "x"], "j": 2, "l": 2})
        );
    }

    #[test]
    fn what_json_cannot_hold_is_refused() {
        for (text, why) in [
            ("a: .inf\n", "a number that JSON cannot hold"),
            ("1: a\n", "a key that is not a string"),
            (
                "base: &defaults\n  replicas: 1\ncomponent:\n  <<: *defaults\n  name: Worker\n",
                "YAML merge keys are not supported; write the fields out",
            ),
        ] {
            let error = parse(text).expect_err("refused");
            assert!(error.contains(why), "{text}: {error}");
        }
    }
}
