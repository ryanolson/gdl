//! Wire-capability classification. A placement may cut an edge only if the
//! edge's type has a codec; the probe decides that where the type is concrete.

use std::rc::Rc;

use gdl_runtime::{Codec, wire_codec};
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

#[derive(Debug, PartialEq, Serialize, Deserialize, gdl::Message)]
#[gdl(codec = "json")]
/// A message whose JSON codec is selected through an alias too.
pub struct Json {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    optional: Option<String>,
    text: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, gdl::Message)]
#[gdl(codec = "msgpack_array")]
/// A fixed-layout message with an explicit positional encoding.
pub struct Array {
    n: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, gdl::Message)]
#[gdl(codec = "msgpack")]
/// A message with an explicit named-field encoding.
pub struct Named {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    optional: Option<u32>,
    n: u32,
}

mod selected {
    use super::{Array, Json, Named};

    gdl_runtime::graph! {
        name = "codecs", schema = 1;
        source A: Json + stream Array -> stream Named;
        sink B: Json + stream Array -> stream Named;
        topology: A => B;
    }
}

#[test]
fn the_manifest_records_each_parts_codec_and_hashes_every_choice() {
    use gdl_runtime::Encoding;

    let manifest = selected::manifest();
    assert_eq!(manifest.hops[0].forward_types[0].encoding, Encoding::Json);
    assert_eq!(
        manifest.hops[0].forward_types[0]
            .stream
            .as_ref()
            .expect("streamed input")
            .encoding,
        Encoding::MsgpackArray
    );
    assert_eq!(
        manifest.hops[0].backward_types[0].encoding,
        Encoding::Msgpack
    );
    assert_eq!(
        gdl_runtime::Manifest::from_toml(&manifest.to_toml()).expect("parse"),
        manifest
    );
    for part in 0..3 {
        let mut changed = manifest.clone();
        let encoding = match part {
            0 => &mut changed.hops[0].forward_types[0].encoding,
            1 => {
                &mut changed.hops[0].forward_types[0]
                    .stream
                    .as_mut()
                    .expect("streamed input")
                    .encoding
            }
            _ => &mut changed.hops[0].backward_types[0].encoding,
        };
        *encoding = match encoding {
            Encoding::Msgpack => Encoding::Json,
            _ => Encoding::Msgpack,
        };
        assert_ne!(
            manifest.contract_hash(),
            changed.contract_hash(),
            "part {part}"
        );
    }
}

#[test]
fn message_annotations_select_the_codec_for_aliases_too() {
    type Alias = Json;
    let value = Json {
        optional: None,
        text: "hello".into(),
    };
    let codec = wire_codec!(Alias).expect("wire");
    let bytes = codec.encode(&value).expect("encode");
    assert_eq!(serde_json::from_slice::<Json>(&bytes).expect("JSON"), value);
    assert_eq!(codec.decode(&bytes).expect("decode"), value);

    let array = Array { n: 7 };
    let codec = wire_codec!(Array).expect("wire");
    assert_eq!(codec.encode(&array).expect("encode"), [0x91, 7]);
    assert_eq!(codec.decode(&[0x91, 7]).expect("decode"), array);

    let named = Named {
        optional: None,
        n: 7,
    };
    let codec = wire_codec!(Named).expect("wire");
    let bytes = codec.encode(&named).expect("encode");
    assert_eq!(codec.decode(&bytes).expect("decode"), named);
    assert_eq!(
        rmp_serde::from_slice::<std::collections::BTreeMap<String, u32>>(&bytes).expect("map")["n"],
        7
    );
}

#[tokio::test]
async fn a_stream_codec_uses_each_parts_declared_encoding() {
    use futures::StreamExt;
    use gdl::{Streamed, items};
    use gdl_runtime::RequestCodec;

    let codec = gdl_runtime::stream_codec!(Json, Array).expect("wire");
    let head = Json {
        optional: None,
        text: "head".into(),
    };
    let request = Streamed::new(head, items(futures::stream::iter([Ok(Array { n: 7 })])));
    let split = codec.split(request).expect("split");
    let head: Json = serde_json::from_slice(&split.head).expect("JSON head");
    assert_eq!(head.text, "head");
    let encoded: Vec<_> = split.input.expect("input").collect().await;
    assert_eq!(encoded, [Ok(vec![0x91, 7])]);
    let arrived = encoded.into_iter().map(|item| item.map(bytes::Bytes::from));
    let joined = codec
        .join(&split.head, Some(items(futures::stream::iter(arrived))))
        .expect("join");
    let (_, input) = joined.into_parts();
    assert_eq!(input.collect::<Vec<_>>().await, [Ok(Array { n: 7 })]);
}

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
fn the_default_codec_preserves_omitted_middle_fields() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Request {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        optional: Option<u8>,
        required: u8,
    }

    let codec = wire_codec!(Request).expect("wire");
    for optional in [None, Some(3)] {
        let value = Request {
            optional,
            required: 10,
        };
        let bytes = codec.encode(&value).expect("encode");
        assert_eq!(codec.decode(&bytes).expect("decode"), value);
    }
}

#[test]
fn the_default_codec_does_not_shift_one_optional_field_into_another() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Request {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        first: Option<u8>,
        #[serde(default)]
        second: Option<u8>,
    }

    let codec = wire_codec!(Request).expect("wire");
    let value = Request {
        first: None,
        second: Some(7),
    };
    let bytes = codec.encode(&value).expect("encode");
    assert_eq!(codec.decode(&bytes).expect("decode"), value);
}

#[test]
fn the_default_codec_preserves_nested_api_shapes() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Request {
        messages: Vec<Message>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        temperature: Option<f64>,
        tools: serde_json::Value,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "role", rename_all = "snake_case")]
    enum Message {
        User {
            #[serde(default, skip_serializing_if = "Option::is_none")]
            name: Option<String>,
            content: Content,
        },
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(untagged)]
    enum Content {
        Text(String),
        Parts(Vec<Part>),
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum Part {
        Text { text: String },
    }

    let codec = wire_codec!(Request).expect("wire");
    for content in [
        serde_json::json!("hello"),
        serde_json::json!([{"type": "text", "text": "hello"}]),
    ] {
        let value: Request = serde_json::from_value(serde_json::json!({
            "messages": [{"role": "user", "content": content}],
            "tools": {"type": "object", "properties": {"city": {"type": "string"}}}
        }))
        .expect("request");
        let bytes = codec.encode(&value).expect("encode");
        assert_eq!(codec.decode(&bytes).expect("decode"), value);
    }
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

#[test]
fn messagepack_bytes_and_errors_stay_the_same_across_buffer_sizes() {
    use gdl_runtime::Encoding;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Payload {
        text: String,
        numbers: Vec<u32>,
    }
    for size in [0, 1, 63, 127, 128, 129, 4096, 65536] {
        let value = Payload {
            text: "x".repeat(size),
            numbers: vec![0, 255, 65536],
        };
        for (encoding, expected) in [
            (
                Encoding::Msgpack,
                rmp_serde::to_vec_named(&value).expect("named"),
            ),
            (
                Encoding::MsgpackArray,
                rmp_serde::to_vec(&value).expect("array"),
            ),
        ] {
            let codec = Codec::<Payload>::new(encoding);
            assert_eq!(codec.encode(&value).expect("encode"), expected);
            assert_eq!(codec.decode(&expected).expect("decode"), value);
        }
    }

    #[derive(Deserialize)]
    struct Refused;
    impl Serialize for Refused {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("refused value"))
        }
    }
    for encoding in Encoding::ALL {
        let error = Codec::<Refused>::new(encoding)
            .encode(&Refused)
            .expect_err("refused");
        assert_eq!(error.kind(), gdl::ErrorKind::Codec);
        assert!(error.message().contains("refused value"));
    }
}

#[test]
fn json_limits_are_value_dependent_despite_serde_bounds() {
    use gdl_runtime::Encoding;
    use std::collections::BTreeMap;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Score {
        value: f64,
    }

    let json = Codec::<Score>::new(Encoding::Json);
    let msgpack = Codec::<Score>::new(Encoding::Msgpack);
    let finite = Score { value: 42.5 };
    assert_eq!(
        json.decode(&json.encode(&finite).expect("finite"))
            .expect("decode"),
        finite
    );
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let bytes = json.encode(&Score { value }).expect("encodes as null");
        assert_eq!(bytes, br#"{"value":null}"#);
        assert_eq!(
            json.decode(&bytes).expect_err("null is not f64").kind(),
            gdl::ErrorKind::Codec
        );
        let decoded = msgpack
            .decode(&msgpack.encode(&Score { value }).expect("encode"))
            .expect("decode");
        assert!(decoded.value == value || (value.is_nan() && decoded.value.is_nan()));
    }
    let json = Codec::<Option<f64>>::new(Encoding::Json);
    assert_eq!(
        json.decode(&json.encode(&Some(f64::NAN)).expect("null"))
            .expect("option"),
        None
    );

    let tuples = BTreeMap::from([((1_u8, 2_u8), 3_u8)]);
    let json = Codec::<BTreeMap<(u8, u8), u8>>::new(Encoding::Json);
    assert_eq!(
        json.encode(&tuples).expect_err("tuple key").kind(),
        gdl::ErrorKind::Codec
    );
    let msgpack = Codec::<BTreeMap<(u8, u8), u8>>::new(Encoding::Msgpack);
    assert_eq!(
        msgpack
            .decode(&msgpack.encode(&tuples).expect("encode"))
            .expect("decode"),
        tuples
    );

    let integers = BTreeMap::from([(1_u8, 2_u8)]);
    let json = Codec::<BTreeMap<u8, u8>>::new(Encoding::Json);
    assert_eq!(
        json.decode(&json.encode(&integers).expect("integer key"))
            .expect("decode"),
        integers
    );
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
    assert!(gdl_runtime::stream_codec!(Plain, u32).is_some());
    assert!(gdl_runtime::stream_codec!(NotSerde, u32).is_none());
    assert!(gdl_runtime::stream_codec!(Plain, NotSerde).is_none());
    assert!(gdl_runtime::stream_codec!(SerializeOnly, u32).is_none());
}

#[tokio::test]
async fn a_stream_codec_splits_and_joins_a_streamed_request() {
    use futures::StreamExt;
    use gdl::{Streamed, items};
    use gdl_runtime::RequestCodec;

    let codec = gdl_runtime::stream_codec!(Plain, u32).expect("both parts are wire types");
    let head = Plain {
        n: 1,
        s: "h".into(),
    };
    let request = Streamed::new(head, items(futures::stream::iter([Ok(4), Ok(5)])));
    let split = codec.split(request).expect("split");
    let arrived = split
        .input
        .map(|input| items(input.map(|item| item.map(bytes::Bytes::from))));
    let joined = codec.join(&split.head, arrived).expect("join");
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
    use gdl::items;
    use gdl_runtime::RequestCodec;

    let codec = gdl_runtime::stream_codec!(Plain, u32).expect("wire");
    let head = wire_codec!(Plain)
        .expect("wire")
        .encode(&Plain {
            n: 0,
            s: String::new(),
        })
        .expect("encode");
    let good = wire_codec!(u32).expect("wire").encode(&9).expect("encode");
    // 0xc1 is the one byte msgpack never uses.
    let input = items(futures::stream::iter([
        Ok(bytes::Bytes::from_static(&[0xc1])),
        Ok(bytes::Bytes::from(good)),
    ]));
    let (_, mut input) = codec.join(&head, Some(input)).expect("join").into_parts();
    assert!(matches!(input.next().await, Some(Err(_))));
    assert!(input.next().await.is_none(), "an item followed the bad one");
}

#[test]
fn a_request_and_its_edge_must_agree_on_streaming() {
    use gdl::items;
    use gdl_runtime::RequestCodec;

    let value = wire_codec!(u32).expect("wire");
    let bytes = value.encode(&3).expect("encode");
    assert!(
        value
            .join(&bytes, Some(items(futures::stream::empty())))
            .is_err()
    );
    let stream = gdl_runtime::stream_codec!(u32, u32).expect("wire");
    assert!(stream.join(&bytes, None).is_err());
}
