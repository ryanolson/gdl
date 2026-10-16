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

#[test]
fn a_stream_codec_needs_both_parts_to_be_wire() {
    assert!(gdl::stream_codec!(Plain, u32).is_some());
    assert!(gdl::stream_codec!(NotSerde, u32).is_none());
    assert!(gdl::stream_codec!(Plain, NotSerde).is_none());
    assert!(gdl::stream_codec!(SerializeOnly, u32).is_none());
}

#[tokio::test]
async fn a_stream_codec_splits_and_joins_a_streamed_request() {
    use futures::StreamExt;
    use gdl::{RequestCodec, Streamed, items};

    let codec = gdl::stream_codec!(Plain, u32).expect("both parts are wire types");
    let head = Plain {
        n: 1,
        s: "h".into(),
    };
    let request = Streamed::new(head, items(futures::stream::iter([Ok(4), Ok(5)])));
    let split = codec.split(request).expect("split");
    let joined = codec.join(&split.head, split.input).expect("join");
    let (head, input) = joined.into_parts();
    assert_eq!(
        head,
        Plain {
            n: 1,
            s: "h".into()
        }
    );
    let got: Vec<u32> = input.map(|r| r.expect("item")).collect().await;
    assert_eq!(got, [4, 5]);
}

#[tokio::test]
async fn joining_a_stream_ends_at_its_first_bad_item() {
    use futures::StreamExt;
    use gdl::{RequestCodec, items};

    let codec = gdl::stream_codec!(Plain, u32).expect("wire");
    let head = wire_codec!(Plain)
        .expect("wire")
        .encode(&Plain {
            n: 0,
            s: String::new(),
        })
        .expect("encode");
    let good = wire_codec!(u32).expect("wire").encode(&9).expect("encode");
    // 0xc1 is the one byte msgpack never uses.
    let input = items(futures::stream::iter([Ok(vec![0xc1]), Ok(good)]));
    let (_, mut input) = codec.join(&head, Some(input)).expect("join").into_parts();
    assert!(matches!(input.next().await, Some(Err(_))));
    assert!(input.next().await.is_none(), "an item followed the bad one");
}

#[test]
fn a_request_and_its_edge_must_agree_on_streaming() {
    use gdl::{RequestCodec, items};

    let value = wire_codec!(u32).expect("wire");
    let bytes = value.encode(&3).expect("encode");
    assert!(value.join(&bytes, Some(items(futures::stream::empty()))).is_err());
    let stream = gdl::stream_codec!(u32, u32).expect("wire");
    assert!(stream.join(&bytes, None).is_err());
}
