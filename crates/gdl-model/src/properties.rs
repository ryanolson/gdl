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
/// A key is checked when it is made and when it is read
/// ([`PropertyKey::new`]), so every key that exists can be published.
///
/// Two keys are compared, ordered, and hashed in a canonical form, so that
/// two builds that write one value's fields in another order name one
/// property: an object's fields are sorted by name. The key itself is
/// kept, and written, as it was made or read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "KeyWire", into = "KeyWire")]
pub struct PropertyKey {
    kind: String,
    key: Value,
    /// `key` in its canonical form: what equality, order, and hashing
    /// read.
    canonical: Value,
}

impl PropertyKey {
    /// The property of `kind` whose key is `key`, kept as it is, or why it
    /// cannot be one: a kind name not of a settings key's shape
    /// ([`check_property_kind`]), a key that is not an object, as the
    /// encoding of a message type is, or a key that holds a number that is
    /// not an integer. JSON reads a float back as a near value, not always
    /// the same one, so a key with a float would not name its property
    /// after a report or a stored record.
    pub fn new(kind: impl Into<String>, key: Value) -> Result<Self, String> {
        let kind = kind.into();
        check_property_kind(&kind)?;
        if !key.is_object() {
            return Err(format!(
                "a key of property kind `{kind}` must be an object, not `{key}`"
            ));
        }
        if let Some(number) = fraction(&key) {
            return Err(format!(
                "a key of property kind `{kind}` holds `{number}`, a number that is not an integer: a property's numbers must be integers"
            ));
        }
        Ok(Self {
            kind,
            canonical: canonical(key.clone()),
            key,
        })
    }

    /// The property of `kind` whose key is `key` encoded, or why it cannot
    /// be one: as [`PropertyKey::new`], or a key that does not encode.
    pub fn encode<T: Serialize + ?Sized>(kind: impl Into<String>, key: &T) -> Result<Self, String> {
        let kind = kind.into();
        let key = serde_json::to_value(key)
            .map_err(|e| format!("a key of property kind `{kind}` cannot be encoded: {e}"))?;
        Self::new(kind, key)
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
#[serde(try_from = "EntryWire", into = "EntryWire")]
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
/// order and its `null`s, which are its own.
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

/// Integers by value: a key holds no other number.
fn numbers(a: &Number, b: &Number) -> Ordering {
    let integer = |n: &Number| {
        n.as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
    };
    integer(a).cmp(&integer(b))
}

/// A key as it is written.
#[derive(Serialize, Deserialize)]
struct KeyWire {
    kind: String,
    key: Value,
}

impl TryFrom<KeyWire> for PropertyKey {
    type Error = String;

    fn try_from(wire: KeyWire) -> Result<Self, String> {
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

impl TryFrom<EntryWire> for PropertyEntry {
    type Error = String;

    fn try_from(wire: EntryWire) -> Result<Self, String> {
        Ok(Self {
            property: PropertyKey::new(wire.kind, wire.key)?,
            state: wire.state,
        })
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
