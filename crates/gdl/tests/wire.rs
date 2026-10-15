//! Wire-capability classification. A placement may cut an edge only if the
//! edge's type has a codec; the probe decides that where the type is concrete.

use std::rc::Rc;

use gdl::{Codec, wire_codec};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Plain {
    n: u32,
    s: String,
}

type Alias = Plain;

#[derive(Serialize)]
struct SerializeOnly(u32);

struct NotSerde(#[allow(dead_code)] Rc<u32>);

#[test]
fn serde_types_get_a_codec_that_round_trips() {
    let codec = wire_codec!(Plain).expect("Plain is wire-capable");
    let value = Plain {
        n: 7,
        s: "x".into(),
    };
    let bytes = codec.encode(&value).expect("encode");
    assert_eq!(codec.decode(&bytes).expect("decode"), value);
}

#[test]
fn aliases_and_concrete_generics_are_classified_by_their_target() {
    assert!(wire_codec!(Alias).is_some());
    assert!(wire_codec!(Vec<Plain>).is_some());
    assert!(wire_codec!(Option<(u32, String)>).is_some());
}

#[test]
fn types_that_cannot_round_trip_get_no_codec() {
    assert!(wire_codec!(SerializeOnly).is_none());
    assert!(wire_codec!(NotSerde).is_none());
    // A type that deserializes only by borrowing from its input cannot be
    // classified at all: the probe fails to compile for it (see
    // `tests/ui/borrowed-message.rs`). Message types must own their data.
}

#[test]
fn a_decode_failure_is_a_codec_error() {
    let codec: Codec<Plain> = wire_codec!(Plain).expect("codec");
    let error = codec
        .decode(b"\xc1")
        .expect_err("0xc1 is never valid msgpack");
    assert_eq!(error.kind(), gdl::ErrorKind::Codec);
}

/// Why the graph macro must expand the probe per edge: inside a generic
/// function the bounded impl cannot be selected, so even a serde type falls
/// back to "no codec".
#[test]
fn the_probe_falls_back_inside_generic_code() {
    fn generic<T>() -> Option<Codec<T>> {
        wire_codec!(T)
    }
    assert!(generic::<Plain>().is_none());
}
