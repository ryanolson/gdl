use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

/// One property a pool serves: its kind, and its key, an object of the
/// kind's own type, carried encoded. gdl never reads the key's fields:
/// every field is part of the property's identity, so two keys that differ
/// in any field are two properties. The type is the application's; gdl
/// decodes a key only where a node or a pool policy registered the kind
/// (`gdl::PropertyKind`).
///
/// Two keys are compared, ordered, and hashed in a canonical form, so that
/// two builds that encode one value differently name one property: an
/// object's fields are sorted by name, and a field whose value is `null`
/// is left out, as a missing optional field is. The key itself is kept,
/// and written, as it was made or read: a field that the type requires
/// and that encodes as `null` stays, so the key still decodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "KeyWire", into = "KeyWire")]
pub struct PropertyKey {
    kind: String,
    key: Value,
    /// `key` in its canonical form: what equality, order, and hashing
    /// read.
    canonical: Value,
}

impl PropertyKey {
    /// The property of `kind` whose key is `key`, kept as it is.
    pub fn new(kind: impl Into<String>, key: Value) -> Self {
        Self {
            kind: kind.into(),
            canonical: canonical(key.clone()),
            key,
        }
    }

    /// The property of `kind` whose key is `key` encoded, or why it cannot
    /// be encoded.
    pub fn encode<T: Serialize + ?Sized>(kind: impl Into<String>, key: &T) -> Result<Self, String> {
        let kind = kind.into();
        serde_json::to_value(key)
            .map(|key| Self::new(kind.clone(), key))
            .map_err(|e| format!("a key of property kind `{kind}` cannot be encoded: {e}"))
    }

    /// The property's kind.
    #[inline]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The property's key, as it was made or read.
    #[inline]
    pub fn key(&self) -> &Value {
        &self.key
    }

    /// Whether the property can be published: a kind name of its shape
    /// ([`check_property_kind`]), and a key that is an object, as the
    /// encoding of a message type is, and holds no number that is not an
    /// integer. JSON reads a float back as a near value, not always the
    /// same one, so a key with a float would not name its property after
    /// a report or a stored record.
    pub fn check(&self) -> Result<(), String> {
        check_property_kind(&self.kind)?;
        if !self.key.is_object() {
            return Err(format!(
                "a key of property kind `{}` must be an object, not `{}`",
                self.kind, self.key
            ));
        }
        if let Some(number) = fraction(&self.key) {
            return Err(format!(
                "a key of property kind `{}` holds `{number}`, a number that is not an integer: a property's numbers must be integers",
                self.kind
            ));
        }
        Ok(())
    }
}

impl PartialEq for PropertyKey {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.canonical == other.canonical
    }
}

impl Eq for PropertyKey {}

impl std::hash::Hash for PropertyKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.canonical.hash(state);
    }
}

impl PartialOrd for PropertyKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// By kind, then by key: a total order over canonical keys that agrees
/// with their equality. It is the order of a record's list, not one any
/// key's type gives.
impl Ord for PropertyKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind
            .cmp(&other.kind)
            .then_with(|| compare(&self.canonical, &other.canonical))
    }
}

/// What an entry instance tells its pool's hub of one property it has: the
/// property, and whether the instance can serve it now. A field a later
/// build adds is ignored, so builds roll one instance at a time.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "EntryWire", into = "EntryWire")]
pub struct PropertyEntry {
    property: PropertyKey,
    state: EntryState,
}

impl PropertyEntry {
    /// `property`, in `state`.
    pub fn new(property: PropertyKey, state: EntryState) -> Self {
        Self { property, state }
    }

    /// The property.
    #[inline]
    pub fn property(&self) -> &PropertyKey {
        &self.property
    }

    /// The property's kind.
    #[inline]
    pub fn kind(&self) -> &str {
        self.property.kind()
    }

    /// The property's key.
    #[inline]
    pub fn key(&self) -> &Value {
        self.property.key()
    }

    /// Whether the instance can serve the property now.
    #[inline]
    pub fn state(&self) -> EntryState {
        self.state
    }

    /// The same property, in `state`.
    pub fn with_state(self, state: EntryState) -> Self {
        Self { state, ..self }
    }
}

/// What an entry instance says of its properties: for each kind its nodes
/// set a list of, the entries of that kind. A kind it has no list of is
/// one it says nothing of, which is not a list with no entries: it serves
/// the properties of that kind that its pool's settings declare.
pub type PropertyLists = BTreeMap<String, Vec<PropertyEntry>>;

/// Whether an entry instance can serve a property it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryState {
    /// It is loading what the property names, and cannot serve it yet.
    Starting,
    /// It can serve the property now.
    Ready,
    /// It is removing the property on purpose, and takes no new work for
    /// it.
    Draining,
}

/// Whether `name` can name a property kind. A kind is a TOML key in a
/// pool's settings (`[pool.properties.<kind>]`), and part of the name of
/// the variable that sets them, which is read in lowercase, so it has a
/// settings key's shape.
pub fn check_property_kind(name: &str) -> Result<(), String> {
    crate::placement::check_name_shape("property kind", name)
}

/// The first number in `value`, at any depth, that is not an integer.
fn fraction(value: &Value) -> Option<&Number> {
    match value {
        Value::Number(number) if !number.is_i64() && !number.is_u64() => Some(number),
        Value::Array(items) => items.iter().find_map(fraction),
        Value::Object(fields) => fields.values().find_map(fraction),
        _ => None,
    }
}

/// `value` in its canonical form: each object's fields sorted by name,
/// with every `null` field left out, at any depth. An array keeps its
/// order and its `null`s, which are its own. A negative zero is zero, so
/// that equal keys order equal.
fn canonical(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut fields: Vec<(String, Value)> = map
                .into_iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(name, value)| (name, canonical(value)))
                .collect();
            fields.sort_by(|(a, _), (b, _)| a.cmp(b));
            Value::Object(fields.into_iter().collect::<Map<String, Value>>())
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonical).collect()),
        Value::Number(number) if number.as_f64() == Some(0.0) && number.is_f64() => {
            Value::Number(Number::from_f64(0.0).unwrap_or(number))
        }
        value => value,
    }
}

/// A total order over canonical values that agrees with their equality:
/// by the value's type, then by its content. Two objects compare field by
/// field, in the order of their names, which a canonical object iterates
/// in.
fn compare(a: &Value, b: &Value) -> Ordering {
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Number(_) => 2,
            Value::String(_) => 3,
            Value::Array(_) => 4,
            Value::Object(_) => 5,
        }
    }
    match (a, b) {
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Number(a), Value::Number(b)) => numbers(a, b),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Array(a), Value::Array(b)) => a
            .iter()
            .zip(b)
            .map(|(a, b)| compare(a, b))
            .find(|order| order.is_ne())
            .unwrap_or_else(|| a.len().cmp(&b.len())),
        (Value::Object(a), Value::Object(b)) => a
            .iter()
            .zip(b)
            .map(|((x, a), (y, b))| x.cmp(y).then_with(|| compare(a, b)))
            .find(|order| order.is_ne())
            .unwrap_or_else(|| a.len().cmp(&b.len())),
        _ => rank(a).cmp(&rank(b)),
    }
}

/// Integers by value, floats by value, and an integer before a float of
/// the same value, which `serde_json` holds unequal.
fn numbers(a: &Number, b: &Number) -> Ordering {
    let integer = |n: &Number| {
        n.as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
    };
    match (integer(a), integer(b)) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => {
            let (x, y) = (a.as_f64().unwrap_or(0.0), b.as_f64().unwrap_or(0.0));
            x.total_cmp(&y)
                .then_with(|| integer(b).is_some().cmp(&integer(a).is_some()))
        }
    }
}

/// A key as it is written.
#[derive(Serialize, Deserialize)]
struct KeyWire {
    kind: String,
    key: Value,
}

impl From<KeyWire> for PropertyKey {
    fn from(wire: KeyWire) -> Self {
        Self::new(wire.kind, wire.key)
    }
}

impl From<PropertyKey> for KeyWire {
    fn from(key: PropertyKey) -> Self {
        Self {
            kind: key.kind,
            key: key.key,
        }
    }
}

/// An entry as it is written: the key's fields beside its state.
#[derive(Serialize, Deserialize)]
struct EntryWire {
    kind: String,
    key: Value,
    state: EntryState,
}

impl From<EntryWire> for PropertyEntry {
    fn from(wire: EntryWire) -> Self {
        Self {
            property: PropertyKey::new(wire.kind, wire.key),
            state: wire.state,
        }
    }
}

impl From<PropertyEntry> for EntryWire {
    fn from(entry: PropertyEntry) -> Self {
        Self {
            kind: entry.property.kind,
            key: entry.property.key,
            state: entry.state,
        }
    }
}

#[cfg(test)]
mod tests;
