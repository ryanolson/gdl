//! A placement's `[codec]` table: an edge's codec, for the messages on it
//! that pin none. See `docs/src/concepts/crossings.md`.

use std::collections::BTreeMap;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{HopLayout, Leg, PlacementError};
use crate::hash::Fnv;
use crate::manifest::{HopInfo, TypeInfo};
use crate::{Encoding, Problem};

/// A codec an edge can name: what its unpinned messages become on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EdgeCodec {
    /// Named MessagePack, then one zstd frame at level 1, for a value at
    /// least the edge's floor long.
    MsgpackZstd,
}

impl EdgeCodec {
    /// The codecs a `[codec]` entry can name.
    pub const ALL: [Self; 1] = [Self::MsgpackZstd];

    /// The floor of an entry that sets none: a value shorter than this
    /// crosses as it would without the codec.
    pub const DEFAULT_FLOOR: u32 = 16 * 1024;

    /// The name a `[codec]` entry writes.
    pub const fn name(self) -> &'static str {
        match self {
            Self::MsgpackZstd => "msgpack_zstd",
        }
    }

    /// The wire revision of the codec, in the placement hash.
    pub const fn version(self) -> u32 {
        1
    }

    fn named(name: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|codec| codec.name() == name)
            .ok_or_else(|| {
                let names: Vec<&str> = Self::ALL.map(Self::name).to_vec();
                format!("unknown codec `{name}`: expected {}", names.join(", "))
            })
    }
}

/// One `[codec]` entry: which messages of its edge take which codec, and
/// the floor below which a value crosses without it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecEntry {
    parts: Parts,
    floor: Option<u32>,
}

/// The messages of an edge an entry names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Parts {
    /// Every message the edge carries: its value, or a streamed request's
    /// head and items.
    Every(EdgeCodec),
    /// A streamed request's head, its items, or both, by name.
    Each {
        head: Option<EdgeCodec>,
        item: Option<EdgeCodec>,
    },
}

impl CodecEntry {
    /// An entry for every message on its edge.
    pub fn every(codec: EdgeCodec) -> Self {
        Self {
            parts: Parts::Every(codec),
            floor: None,
        }
    }

    /// An entry for a streamed request's head, its items, or both.
    pub fn each(head: Option<EdgeCodec>, item: Option<EdgeCodec>) -> Self {
        Self {
            parts: Parts::Each { head, item },
            floor: None,
        }
    }

    /// The entry with its own floor.
    pub fn floor(self, floor: u32) -> Self {
        Self {
            floor: Some(floor),
            ..self
        }
    }

    fn hash(&self, h: &mut Fnv) {
        let codec = |h: &mut Fnv, codec: Option<EdgeCodec>| match codec {
            None => h.u64(0),
            Some(codec) => {
                h.u64(1);
                h.str(codec.name());
                h.u64(u64::from(codec.version()));
            }
        };
        match self.parts {
            Parts::Every(every) => {
                h.str("every");
                codec(h, Some(every));
            }
            Parts::Each { head, item } => {
                h.str("each");
                codec(h, head);
                codec(h, item);
            }
        }
    }
}

/// What a layout compresses on one edge: the parts of the message it
/// carries, and the floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeCompression {
    /// The edge's value, or a streamed request's head.
    pub head: bool,
    /// A streamed request's items.
    pub item: bool,
    /// A value shorter than this, in encoded bytes, crosses uncompressed.
    pub floor: u32,
}

/// A `[codec]` key: `<from> -> <to>`, an edge in the direction its data
/// flows, spaced as the placement writes it back.
pub(super) fn edge_key(key: &str) -> Result<String, String> {
    let ends = key
        .split_once("->")
        .map(|(from, to)| (from.trim(), to.trim()))
        .filter(|(from, to)| {
            let name = |n: &str| !n.is_empty() && !n.contains(char::is_whitespace);
            name(from) && name(to) && !to.contains("->")
        });
    match ends {
        Some((from, to)) => Ok(format!("{from} -> {to}")),
        None => Err(format!(
            "`{key}` names no edge: an edge is written `<from> -> <to>`, its two nodes in the direction its data flows"
        )),
    }
}

/// Read the `[codec]` keys, each spaced as [`edge_key`] writes it.
pub(super) fn keyed(
    codecs: BTreeMap<String, CodecEntry>,
    problems: &mut Vec<Problem>,
) -> BTreeMap<String, CodecEntry> {
    let mut keyed = BTreeMap::new();
    for (key, entry) in codecs {
        match edge_key(&key) {
            Ok(edge) if keyed.contains_key(&edge) => problems.push(Problem::at(
                Some("codec".into()),
                Some(key),
                format!("names edge `{edge}`, which another key names too"),
            )),
            Ok(edge) => {
                keyed.insert(edge, entry);
            }
            Err(why) => problems.push(Problem::at(Some("codec".into()), Some(key), why)),
        }
    }
    keyed
}

/// The placement hash's part for the codecs: nothing when there are none,
/// so a placement without them keeps its hash. The floor is not hashed: a
/// receiver reads a value either way, whatever floor its sender has.
pub(super) fn hash(h: &mut Fnv, codecs: &BTreeMap<String, CodecEntry>) {
    if codecs.is_empty() {
        return;
    }
    // A marker no name's length can be, apart from the classes' own.
    h.u64(u64::MAX - 1);
    h.u64(codecs.len() as u64);
    for (edge, entry) in codecs {
        h.str(edge);
        entry.hash(h);
    }
}

/// Resolve each entry to the one edge it names, and judge it: the edge
/// must be encoded under the layout, and each part it names must carry an
/// unpinned message. Keyed by hop, path, and the position the edge enters.
pub(super) fn resolve(
    codecs: &BTreeMap<String, CodecEntry>,
    hops: &[HopInfo],
    layouts: &[HopLayout],
    errors: &mut Vec<PlacementError>,
) -> BTreeMap<(usize, Leg, usize), EdgeCompression> {
    let mut resolved = BTreeMap::new();
    for (edge, entry) in codecs {
        let mut found = Vec::new();
        let mut edges = Vec::new();
        for (index, hop) in hops.iter().enumerate() {
            for (leg, path, types) in [
                (
                    Leg::Forward,
                    hop.forward_path().collect::<Vec<_>>(),
                    &hop.forward_types,
                ),
                (
                    Leg::Backward,
                    hop.backward_path().collect::<Vec<_>>(),
                    &hop.backward_types,
                ),
            ] {
                for (at, pair) in path.windows(2).enumerate() {
                    let named = format!("{} -> {}", pair[0], pair[1]);
                    if &named == edge {
                        found.push((index, leg, at, &types[at]));
                    }
                    if !edges.contains(&named) {
                        edges.push(named);
                    }
                }
            }
        }
        let (index, leg, at, ty) = match found[..] {
            [one] => one,
            [] => {
                errors.push(PlacementError::UnknownCodecEdge {
                    edge: edge.clone(),
                    edges,
                });
                continue;
            }
            _ => {
                errors.push(PlacementError::AmbiguousCodecEdge {
                    edge: edge.clone(),
                    count: found.len(),
                });
                continue;
            }
        };
        let encoded = layouts[index]
            .wire_edges()
            .iter()
            .any(|wire| wire.leg == leg && wire.edge == at);
        if !encoded {
            errors.push(PlacementError::LocalCodecEdge { edge: edge.clone() });
            continue;
        }
        if let Some(compression) = judge(edge, entry, ty, errors) {
            resolved.insert((index, leg, at + 1), compression);
        }
    }
    resolved
}

/// The parts `entry` compresses on an edge carrying `ty`, or `None` with
/// the reason in `errors`.
fn judge(
    edge: &str,
    entry: &CodecEntry,
    ty: &TypeInfo,
    errors: &mut Vec<PlacementError>,
) -> Option<EdgeCompression> {
    let (head, item) = match (entry.parts, &ty.stream) {
        (Parts::Every(_), None) => (true, false),
        (Parts::Every(_), Some(_)) => (true, true),
        (Parts::Each { .. }, None) => {
            errors.push(PlacementError::CodecParts {
                edge: edge.to_string(),
                reason: format!(
                    "`head` and `item` name the parts of a streamed request, and `{}` is not one; name the codec with a string, or `codec = \"...\"`",
                    ty.name
                ),
            });
            return None;
        }
        (Parts::Each { head, item }, Some(_)) => (head.is_some(), item.is_some()),
    };
    let every = matches!(entry.parts, Parts::Every(_));
    let mut pinned = Vec::new();
    if head && ty.is_pinned() {
        pinned.push(("head", ty.name.as_str(), ty.encoding));
    }
    if let Some(stream) = ty.stream.as_ref().filter(|s| item && s.is_pinned()) {
        pinned.push(("item", stream.name.as_str(), stream.encoding));
    }
    if pinned.is_empty() {
        return Some(EdgeCompression {
            head,
            item,
            floor: entry.floor.unwrap_or(EdgeCodec::DEFAULT_FLOOR),
        });
    }
    for (part, message, pin) in pinned {
        // A string entry on a streamed request names both parts: the other
        // part can still be named alone, unless it is pinned too.
        let other = match (every, ty.stream.as_ref(), part) {
            (true, Some(stream), "head") if !stream.is_pinned() => Some("item"),
            (true, Some(_), "item") if !ty.is_pinned() => Some("head"),
            _ => None,
        };
        errors.push(PlacementError::PinnedCodec {
            edge: edge.to_string(),
            part: if ty.stream.is_some() { part } else { "value" },
            message: message.to_string(),
            pin,
            other,
        });
    }
    None
}

impl Serialize for CodecEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        if let (Parts::Every(codec), None) = (self.parts, self.floor) {
            return serializer.serialize_str(codec.name());
        }
        let mut map = serializer.serialize_map(None)?;
        match self.parts {
            Parts::Every(codec) => map.serialize_entry("codec", codec.name())?,
            Parts::Each { head, item } => {
                if let Some(head) = head {
                    map.serialize_entry("head", head.name())?;
                }
                if let Some(item) = item {
                    map.serialize_entry("item", item.name())?;
                }
            }
        }
        if let Some(floor) = self.floor {
            map.serialize_entry("floor", &floor)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for CodecEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Entry;
        impl<'de> Visitor<'de> for Entry {
            type Value = CodecEntry;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a codec's name, or a table of `codec`, `head`, `item`, and `floor`")
            }

            fn visit_str<E: de::Error>(self, name: &str) -> Result<CodecEntry, E> {
                EdgeCodec::named(name)
                    .map(CodecEntry::every)
                    .map_err(E::custom)
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<CodecEntry, A::Error> {
                let (mut every, mut head, mut item, mut floor) = (None, None, None, None);
                while let Some(key) = map.next_key::<String>()? {
                    let codec = |map: &mut A| -> Result<EdgeCodec, A::Error> {
                        let name: String = map.next_value()?;
                        EdgeCodec::named(&name).map_err(de::Error::custom)
                    };
                    match key.as_str() {
                        "codec" => every = Some(codec(&mut map)?),
                        "head" => head = Some(codec(&mut map)?),
                        "item" => item = Some(codec(&mut map)?),
                        "floor" => floor = Some(map.next_value::<u32>()?),
                        other => {
                            return Err(de::Error::custom(format!(
                                "unknown key `{other}`: expected `codec`, `head`, `item`, or `floor`"
                            )));
                        }
                    }
                }
                let parts = match (every, head, item) {
                    (Some(every), None, None) => Parts::Every(every),
                    (None, head, item) if head.is_some() || item.is_some() => {
                        Parts::Each { head, item }
                    }
                    (None, ..) => {
                        return Err(de::Error::custom(
                            "names no codec: write `codec = \"...\"`, or `head` and `item` for a streamed request",
                        ));
                    }
                    (Some(_), ..) => {
                        return Err(de::Error::custom(
                            "`codec` names every part, so `head` and `item` go without it",
                        ));
                    }
                };
                Ok(CodecEntry { parts, floor })
            }
        }
        deserializer.deserialize_any(Entry)
    }
}

/// The pin's attribute, as the message writes it.
pub(super) fn attribute(pin: Encoding) -> String {
    format!("#[gdl(codec = \"{}\")]", pin.name())
}
