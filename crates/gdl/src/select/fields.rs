//! The names a projection's fields are read by, against its edge type's.
//! `derive(gdl::Projection)` checks the fields' Rust names and types at
//! compile time, but serde's `rename` and `rename_all` can give a field
//! another name on the wire, and the derive cannot see the edge type's
//! attributes. serde's own derive passes each struct's field names, with
//! every rename and alias applied, to `deserialize_struct`; a deserializer
//! that stops there reads them without a value to decode. The list is the
//! names a type reads, flat: it does not say which field has a name, nor
//! which names the type writes.

use std::fmt;

use serde::de::{self, DeserializeOwned, Deserializer, Visitor};

/// Refuse projection `P` when a field it reads crosses under a name that
/// its edge type `Of` does not read, naming the field. A type whose
/// `Deserialize` does not read a struct (a `flatten` field, or one written
/// by hand) gives no names, and nothing is refused for it.
pub(super) fn check<P: DeserializeOwned, Of: DeserializeOwned>() -> Result<(), String> {
    let (Some(reads), Some(carried)) = (names::<P>(), names::<Of>()) else {
        return Ok(());
    };
    let missing = reads
        .iter()
        .filter(|name| !carried.contains(name))
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let carried = carried
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>();
    Err(format!(
        "reads `{}`, a projection of `{}`, whose field {} crosses under no name `{}` reads: it reads {}",
        std::any::type_name::<P>(),
        std::any::type_name::<Of>(),
        missing.join(", "),
        std::any::type_name::<Of>(),
        carried.join(", ")
    ))
}

/// The names `T`'s `Deserialize` reads its fields by, if it reads a
/// struct.
fn names<T: DeserializeOwned>() -> Option<&'static [&'static str]> {
    match T::deserialize(Names) {
        Err(Stop::Fields(fields)) => Some(fields),
        _ => None,
    }
}

/// A deserializer that reads nothing: it stops at the first call, with
/// the field names if that call reads a struct.
struct Names;

#[derive(Debug)]
enum Stop {
    Fields(&'static [&'static str]),
    Other,
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stopped before a value")
    }
}

impl std::error::Error for Stop {}

impl de::Error for Stop {
    fn custom<M: fmt::Display>(_: M) -> Self {
        Stop::Other
    }
}

impl<'de> Deserializer<'de> for Names {
    type Error = Stop;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Stop> {
        Err(Stop::Other)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Stop> {
        Err(Stop::Fields(fields))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{check, names};

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[allow(dead_code, reason = "only its names are read")]
    struct Camel {
        first_turn: bool,
        #[serde(rename = "id")]
        fingerprint: u64,
    }

    #[derive(Deserialize)]
    #[allow(dead_code, reason = "only its names are read")]
    struct Snake {
        first_turn: bool,
    }

    #[derive(Deserialize)]
    #[allow(dead_code, reason = "only its names are read")]
    struct Flattened {
        #[serde(flatten)]
        rest: Snake,
    }

    /// The names are the ones on the wire, renames applied; a type that
    /// reads no struct has none, and is not refused.
    #[test]
    fn the_names_are_the_wire_names() {
        assert_eq!(names::<Camel>(), Some(&["firstTurn", "id"][..]));
        assert_eq!(names::<Snake>(), Some(&["first_turn"][..]));
        assert_eq!(names::<Flattened>(), None);
        assert_eq!(names::<u64>(), None);
        let error = check::<Snake, Camel>().expect_err("`first_turn` is `firstTurn`");
        assert!(
            error.contains("`first_turn`") && error.contains("`firstTurn`"),
            "{error}"
        );
        assert!(check::<Snake, Snake>().is_ok());
        assert!(check::<Snake, Flattened>().is_ok());
    }
}
