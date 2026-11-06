//! A value read whole, kept as its format wrote it, and read again as a
//! type: what a cluster table is held as until its `kind` says which type
//! it is. Every entry of a table is kept in order with its key, a key
//! written twice included, so that the type's own reader refuses it as it
//! would refuse it read directly; and every number is kept as written, so
//! that a float that is no number is refused where an integer or a string
//! goes, never read as unset. A `serde_json::Value` would do neither.

use serde::de::value::{Error, MapDeserializer, SeqDeserializer, StringDeserializer};
use serde::de::{self, Deserialize, Deserializer, IntoDeserializer, Visitor};

/// A value as its format wrote it.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Written {
    /// JSON's `null`; no TOML value reads as it.
    Unit,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
    Seq(Vec<Written>),
    /// A table's entries, in order, each key as often as it was written.
    Map(Vec<(String, Written)>),
}

impl<'de> Deserialize<'de> for Written {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(Reader)
    }
}

struct Reader;

impl<'de> Visitor<'de> for Reader {
    type Value = Written;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Written, E> {
        Ok(Written::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Written, E> {
        Ok(Written::I64(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Written, E> {
        Ok(Written::U64(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Written, E> {
        Ok(Written::F64(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Written, E> {
        Ok(Written::Str(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Written, E> {
        Ok(Written::Str(value))
    }

    fn visit_unit<E>(self) -> Result<Written, E> {
        Ok(Written::Unit)
    }

    fn visit_none<E>(self) -> Result<Written, E> {
        Ok(Written::Unit)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Written, D::Error> {
        Written::deserialize(deserializer)
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Written, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Written::Seq(items))
    }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Written, A::Error> {
        let mut entries = Vec::new();
        while let Some(entry) = map.next_entry::<String, Written>()? {
            entries.push(entry);
        }
        Ok(Written::Map(entries))
    }
}

impl<'de> Deserializer<'de> for Written {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self {
            Self::Unit => visitor.visit_unit(),
            Self::Bool(value) => visitor.visit_bool(value),
            Self::I64(value) => visitor.visit_i64(value),
            Self::U64(value) => visitor.visit_u64(value),
            Self::F64(value) => visitor.visit_f64(value),
            Self::Str(value) => visitor.visit_string(value),
            Self::Seq(items) => {
                let mut seq = SeqDeserializer::new(items.into_iter());
                let value = visitor.visit_seq(&mut seq)?;
                seq.end()?;
                Ok(value)
            }
            Self::Map(entries) => {
                let mut map = MapDeserializer::new(entries.into_iter());
                let value = visitor.visit_map(&mut map)?;
                map.end()?;
                Ok(value)
            }
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self {
            Self::Unit => visitor.visit_none(),
            some => visitor.visit_some(some),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        match self {
            Self::Str(variant) => {
                let variant: StringDeserializer<Error> = variant.into_deserializer();
                visitor.visit_enum(variant)
            }
            other => other.deserialize_any(visitor),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct map struct
        identifier ignored_any
    }
}

impl IntoDeserializer<'_, Error> for Written {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}
