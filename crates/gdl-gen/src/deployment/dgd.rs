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
use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser};
use yaml_rust2::scanner::{Marker, TScalarStyle};
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
/// keys strings and no merge key (`<<`), its numbers finite, and no plain
/// scalar that YAML 1.1 reads as another value ([`one_reading`]).
fn parse(text: &str) -> Result<Value, String> {
    let documents = YamlLoader::load_from_str(text).map_err(|e| format!("is not YAML: {e}"))?;
    let value = match documents.as_slice() {
        [document] => json(document)?,
        others => {
            return Err(format!(
                "holds {} YAML documents, and a DGD file holds one",
                others.len()
            ));
        }
    };
    one_reading(text)?;
    Ok(value)
}

/// Refused, with the line and the fix, when `text` has a plain scalar
/// that YAML 1.1, which kubectl reads, reads as another value than YAML
/// 1.2, which gdl reads ([`differs`]): the file would apply one way by
/// hand and another way from its `GdlPool`. A quoted or tagged scalar has
/// one reading. The tree that [`YamlLoader`] builds keeps neither a
/// scalar's style nor its text, so the parser's events are read here.
fn one_reading(text: &str) -> Result<(), String> {
    /// The first plain scalar that two readings give, and its line.
    struct Found(Option<(String, usize, &'static str)>);
    impl MarkedEventReceiver for Found {
        fn on_event(&mut self, event: Event, mark: Marker) {
            if self.0.is_none()
                && let Event::Scalar(value, TScalarStyle::Plain, _, None) = event
                && let Some(fix) = differs(&value)
            {
                self.0 = Some((value, mark.line(), fix));
            }
        }
    }
    let mut found = Found(None);
    Parser::new_from_str(text)
        .load(&mut found, true)
        .map_err(|e| format!("is not YAML: {e}"))?;
    match found.0 {
        None => Ok(()),
        Some((value, line, fix)) => Err(format!(
            "has `{value}` at line {line}: YAML 1.1, which kubectl reads, and YAML 1.2, which gdl reads, give it two values. {fix}"
        )),
    }
}

/// How to write plain scalar `plain` so that YAML 1.1 and 1.2 read it
/// alike, when they do not: a 1.1 boolean (`yes`, `no`, `on`, `off`, `y`,
/// `n`, in any case), a number with a leading zero (1.1 octal), a 1.1
/// binary number or one with `_`, a 1.2 octal (`0o`), or a 1.1 base-60
/// number (`1:30`).
fn differs(plain: &str) -> Option<&'static str> {
    const NUMBER: &str = "Quote it if it is a string, or write it in decimal";
    const BOOLEAN: &str = "Quote it if it is a string, or write `true` or `false`";
    let lower = plain.to_ascii_lowercase();
    if ["y", "n", "yes", "no", "on", "off"].contains(&lower.as_str()) {
        return Some(BOOLEAN);
    }
    let body = plain.strip_prefix(['-', '+']).unwrap_or(plain);
    let digits = |s: &str, of: &dyn Fn(u8) -> bool| !s.is_empty() && s.bytes().all(of);
    let decimal = |b: u8| b.is_ascii_digit() || b == b'_';
    let number = digits(body, &decimal)
        && body.as_bytes()[0].is_ascii_digit()
        && (body.contains('_') || (body.len() > 1 && body.starts_with('0')));
    let prefixed = |prefix: &str, of: &dyn Fn(u8) -> bool| {
        body.strip_prefix(prefix)
            .is_some_and(|rest| digits(rest, of))
    };
    let binary = prefixed("0b", &|b| b == b'0' || b == b'1' || b == b'_');
    let octal = prefixed("0o", &|b| (b'0'..=b'7').contains(&b));
    (number || binary || octal || sexagesimal(body)).then_some(NUMBER)
}

/// Whether `body`, unsigned, is a YAML 1.1 base-60 number: digits, then
/// one or more `:` and a part below 60 (`[0-5]?[0-9]`), the last of which
/// may end in a fraction.
fn sexagesimal(body: &str) -> bool {
    let mut parts = body.split(':');
    let first = parts.next().unwrap_or_default();
    let rest: Vec<&str> = parts.collect();
    let sixty = |part: &str| {
        let bytes = part.as_bytes();
        bytes.iter().all(u8::is_ascii_digit)
            && match bytes {
                [_] => true,
                [tens, _] => *tens <= b'5',
                _ => false,
            }
    };
    let Some((last, middle)) = rest.split_last() else {
        return false;
    };
    let (last, fraction) = match last.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (*last, ""),
    };
    first.as_bytes().first().is_some_and(u8::is_ascii_digit)
        && first.bytes().all(|b| b.is_ascii_digit() || b == b'_')
        && middle.iter().all(|part| sixty(part))
        && sixty(last)
        && fraction.bytes().all(|b| b.is_ascii_digit() || b == b'_')
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
        let text = "a: 1.7.0\nb: \"1\"\nc: \"false\"\nd: 630\ne: 0.5\nf: true\ng: null\nh: \"yes\"\ni: [1, x]\nj: &k 2\nl: *k\n";
        assert_eq!(
            parse(text).expect("JSON"),
            json!({"a": "1.7.0", "b": "1", "c": "false", "d": 630, "e": 0.5, "f": true, "g": null, "h": "yes", "i": [1, "x"], "j": 2, "l": 2})
        );
    }

    /// A plain scalar that YAML 1.1, which kubectl reads, reads as
    /// another value than YAML 1.2 does, which gdl reads: a number, a
    /// boolean, or a base-60 number. Each kind is refused, with the line.
    fn refused_as(text: &str, why: &str) {
        let error = parse(text).expect_err(text);
        assert!(error.contains(why), "{text}: {error}");
    }

    const NUMBER: &str = "Quote it if it is a string, or write it in decimal";
    const BOOLEAN: &str = "Quote it if it is a string, or write `true` or `false`";

    #[test]
    fn a_number_with_a_leading_zero_is_refused() {
        refused_as("a: 1\nmode: 0400\n", NUMBER);
        refused_as("mode: -0644\n", NUMBER);
        refused_as("mode: 09\n", NUMBER);
        let error = parse("a: 1\nmode: 0400\n").expect_err("refused");
        assert!(error.contains("has `0400` at line 2"), "{error}");
    }

    #[test]
    fn a_number_that_only_one_version_reads_is_refused() {
        refused_as("a: 0o400\n", NUMBER);
        refused_as("a: 0b101\n", NUMBER);
        refused_as("a: 1_000\n", NUMBER);
    }

    #[test]
    fn a_yaml_11_boolean_is_refused_in_any_case() {
        for word in ["yes", "No", "ON", "off", "y", "N", "yEs"] {
            refused_as(&format!("a: {word}\n"), BOOLEAN);
        }
        refused_as("on: x\n", BOOLEAN);
    }

    #[test]
    fn a_base_60_number_is_refused() {
        refused_as("a: 1:30\n", NUMBER);
        refused_as("a: -1:30:59\n", NUMBER);
        refused_as("a: 1:30.5\n", NUMBER);
    }

    /// Quoted, tagged, or written the one way both versions read, a
    /// value is taken.
    #[test]
    fn a_value_both_versions_read_alike_is_taken() {
        let text = "a: \"0400\"\nb: '0644'\nc: !!str 0400\nd: 256\ne: 0\nf: true\ng: \"yes\"\nh: \"1:30\"\n\
            i: 0x1F\nj: 1.7.0\nk: nvcr.io/nvidia/ai-dynamo/vllm-runtime:1.5.0\nl: 10:61\nm: 0.5\n";
        assert_eq!(
            parse(text).expect("JSON"),
            json!({"a": "0400", "b": "0644", "c": "0400", "d": 256, "e": 0, "f": true, "g": "yes", "h": "1:30",
                   "i": 31, "j": "1.7.0", "k": "nvcr.io/nvidia/ai-dynamo/vllm-runtime:1.5.0", "l": "10:61", "m": 0.5})
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
